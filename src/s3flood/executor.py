import json, time, queue, threading, subprocess, os, socket, random, uuid, signal
from pathlib import Path
from collections import deque
from dataclasses import dataclass

from .runner import (
    _terminate_all_processes,
    aws_cp_download,
    aws_cp_upload,
    aws_list_objects,
    retry_with_backoff,
)
from .metrics import (
    MetricsCsvWriter,
    RateWindow,
    build_timeline,
    classify_error,
    summarize_latencies,
    summarize_speeds,
)

# Minimal executor with AWS CLI runner only (v1)

# Определяем, поддерживает ли терминал ANSI цвета
# На Windows проверяем переменные окружения и версию терминала
USE_COLORS = True
if os.name == "nt":  # Windows
    # Проверяем переменную NO_COLOR
    if os.environ.get("NO_COLOR") or os.environ.get("TERM") == "dumb":
        USE_COLORS = False
    else:
        # Windows 10+ поддерживает ANSI, но нужно проверить
        # Для простоты отключаем цвета на Windows, если явно не включено
        USE_COLORS = os.environ.get("FORCE_COLOR", "").lower() in ("1", "true", "yes")

ANSI_RESET = "\x1b[0m" if USE_COLORS else ""
ANSI_BOLD = "\x1b[1m" if USE_COLORS else ""

WRITE_ICON = "↑"
READ_ICON = "↓"

# Спиннер для индикации активности
SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
spinner_index = 0
spinner_lock = threading.Lock()

def get_spinner():
    """Возвращает следующий кадр спиннера с белым цветом, обновляя его при каждом вызове."""
    global spinner_index
    with spinner_lock:
        frame = SPINNER_FRAMES[spinner_index]
        spinner_index = (spinner_index + 1) % len(SPINNER_FRAMES)
        # Делаем спиннер белым и жирным для лучшей видимости
        # Добавляем пробел после спиннера и используем обрамление для лучшей видимости
        return style(frame, ANSI_BOLD) + " "


def style(text: str, *codes: str) -> str:
    if not USE_COLORS:
        return text
    prefix = "".join(codes)
    return f"{prefix}{text}{ANSI_RESET}" if codes else text


def format_bytes(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    if idx == 0:
        return f"{int(size)} {units[idx]}"
    return f"{size:.1f} {units[idx]}"


@dataclass
class Job:
    path: Path
    size: int
    group: str
    endpoint: str | None = None  # Привязанный endpoint для кластерного режима
    remote_key: str | None = None  # Последний ключ в бакете (для чтения/mixed)
def make_remote_key(filename: str, unique: bool) -> str:
    """Возвращает имя объекта для бакета, опционально добавляя уникальный постфикс."""
    if not unique:
        return filename
    stem, ext = os.path.splitext(filename)
    suffix = f"{int(time.time()*1000):x}-{uuid.uuid4().hex[:6]}"
    if ext:
        return f"{stem}.{suffix}{ext}"
    return f"{stem}.{suffix}"


class Metrics:
    def __init__(self, metrics_csv: str, report_json: str, warmup_sec: float = 0.0):
        self.csv_path = metrics_csv
        self.json_path = report_json
        self._lock = threading.Lock()
        self.ops = []  # per-op tuples: (op, start, end, nbytes, ok, lat_ms)
        self.window = RateWindow()
        self._writer = MetricsCsvWriter(metrics_csv)
        self._start = time.time()
        self.warmup_until = self._start + max(warmup_sec or 0.0, 0.0)
        self.warmup_ops = 0
        self.client_overhead_ms: float | None = None
        self.meta: dict | None = None
        self.error_counts: dict[str, int] = {}
        self.read_bytes = 0
        self.write_bytes = 0
        self.read_ops_ok = 0
        self.write_ops_ok = 0
        self.err_ops = 0
        self.write_latencies_ms: list[int] = []
        self.read_latencies_ms: list[int] = []
        self.last_upload = None
        self.last_download = None
        self.recent_ops = deque(maxlen=30)  # Буфер последних операций для дашборда
        self._active_recent_ops: dict[int, dict] = {}
        self._op_counter = 0

    def start_recent_op(self, op: str, filename: str, nbytes: int, started: float) -> int:
        """Регистрирует операцию в списке Recent ops ещё до завершения."""
        with self._lock:
            op_id = self._op_counter
            self._op_counter += 1
            entry = {
                "id": op_id,
                "op": op,
                "filename": filename,
                "bytes": nbytes,
                "speed_mbps": None,
                "latency_ms": None,
                "started": started,
                "ended": None,
                "done": False,
                "error": None,
            }
            self.recent_ops.append(entry)
            self._active_recent_ops[op_id] = entry
            return op_id

    def elapsed(self) -> float:
        return max(time.time() - self._start, 1e-6)

    def avg_write_rate(self) -> float:
        return self.write_bytes / self.elapsed()

    def avg_read_rate(self) -> float:
        return self.read_bytes / self.elapsed()

    def record(
        self, op: str, start: float, end: float, nbytes: int, ok: bool, err: str | None,
        filename: str | None = None, recent_id: int | None = None,
        endpoint: str | None = None, thread_id: int | None = None,
        attempt: int | None = None, size_group: str | None = None,
    ):
        lat_ms = int((end-start)*1000)
        is_warmup = self.warmup_until > self._start and end < self.warmup_until
        self._writer.write_row(
            ts_start=start, ts_end=end, op=op, nbytes=nbytes, ok=ok,
            latency_ms=lat_ms, error=err, endpoint=endpoint,
            thread_id=thread_id, attempt=attempt, size_group=size_group,
        )
        with self._lock:
            if not is_warmup:
                self.ops.append((op, start, end, nbytes, ok, lat_ms))
                self.window.add(ts=end, op=op, nbytes=nbytes, ok=ok)
                if ok:
                    if op == "download":
                        self.read_latencies_ms.append(lat_ms)
                    elif op == "upload":
                        self.write_latencies_ms.append(lat_ms)
            else:
                self.warmup_ops += 1
            entry = None
            if recent_id is not None:
                entry = self._active_recent_ops.pop(recent_id, None)
            if entry is None and filename:
                entry = {
                    "id": recent_id if recent_id is not None else -1,
                    "op": op,
                    "filename": filename,
                    "bytes": nbytes,
                    "speed_mbps": None,
                    "latency_ms": None,
                    "started": start,
                    "ended": None,
                    "done": False,
                    "error": None,
                }
                self.recent_ops.append(entry)
            if entry is not None:
                entry["bytes"] = nbytes
                entry["latency_ms"] = lat_ms
                entry["ended"] = end
                entry["done"] = True
                entry["error"] = err
                if lat_ms > 0:
                    entry["speed_mbps"] = (nbytes / 1024 / 1024) / (lat_ms / 1000)
                else:
                    entry["speed_mbps"] = None

            if is_warmup:
                return
            if ok:
                if op == "download":
                    self.read_ops_ok += 1
                    self.read_bytes += nbytes
                    self.last_download = {"bytes": nbytes, "lat_ms": lat_ms, "ended": end}
                elif op == "upload":
                    self.write_ops_ok += 1
                    self.write_bytes += nbytes
                    self.last_upload = {"bytes": nbytes, "lat_ms": lat_ms, "ended": end}
            else:
                self.err_ops += 1
                err_type = classify_error(err)
                self.error_counts[err_type] = self.error_counts.get(err_type, 0) + 1

    def get_recent_ops(self, count=6):
        """Возвращает последние операции для отображения в дашборде."""
        with self._lock:
            return list(self.recent_ops)[-count:]

    def current_rates(self, window_sec=5.0):
        return self.window.rates(window_sec)

    def last_latency_ms(self, op: str) -> float | None:
        data = self.last_download if op == "download" else self.last_upload
        if data:
            return data["lat_ms"]
        return None

    def get_file_stats(self, op_type="upload"):
        """Возвращает статистику по файлам: ТОП10 больших, ТОП10 маленьких, средняя скорость."""
        file_stats = {}  # key: (bytes, count, total_time_ms, speeds)
        with self._lock:
            for op, start, end, nbytes, ok, lat_ms in self.ops:
                if op == op_type and ok:
                    if nbytes not in file_stats:
                        file_stats[nbytes] = {"count": 0, "total_time_ms": 0, "speeds": []}
                    file_stats[nbytes]["count"] += 1
                    file_stats[nbytes]["total_time_ms"] += lat_ms
                    if lat_ms > 0:
                        speed_mbps = (nbytes / 1024 / 1024) / (lat_ms / 1000)
                        file_stats[nbytes]["speeds"].append(speed_mbps)
        
        if not file_stats:
            return None, None, None

        # ТОП10 маленьких/больших по уникальному размеру файла
        sorted_by_size = sorted(file_stats.items(), key=lambda x: x[0])
        total_unique_sizes = len(sorted_by_size)
        if total_unique_sizes <= 1:
            top10_small = sorted_by_size
            top10_large = sorted_by_size
        elif total_unique_sizes <= 10:
            mid = total_unique_sizes // 2
            top10_small = sorted_by_size[:mid] if mid > 0 else sorted_by_size[:1]
            top10_large = sorted_by_size[mid:] if mid < total_unique_sizes else sorted_by_size[-1:]
        else:
            top10_small = sorted_by_size[:10]
            top10_large = sorted_by_size[-10:]

        def size_entry(size_bytes, stats):
            entry = {"size_bytes": size_bytes, "count": stats["count"]}
            entry.update(summarize_speeds(stats["speeds"]))
            return entry

        small_stats = [size_entry(size, st) for size, st in top10_small]
        large_stats = [size_entry(size, st) for size, st in top10_large]

        all_speeds = []
        for stats in file_stats.values():
            all_speeds.extend(stats["speeds"])
        overall = summarize_speeds(all_speeds)

        return small_stats, large_stats, overall

    def close(self):
        self._writer.close()

    def finalize(self):
        now = time.time()
        wall_clock = max(now - self._start, 1e-6)

        # Активная длительность — по временным меткам операций, а не wall clock:
        # при простоях/паузах wall clock многократно завышал длительность прогона
        write_duration = 0.0
        read_duration = 0.0
        active_duration = 0.0

        if self.ops:
            ok_ops = [op for op in self.ops if op[4]]  # (op, start, end, nbytes, ok, lat_ms)
            span_ops = ok_ops or self.ops
            active_duration = max(
                max(op[2] for op in span_ops) - min(op[1] for op in span_ops), 1e-6
            )
            write_ops = [op for op in ok_ops if op[0] == "upload"]
            read_ops = [op for op in ok_ops if op[0] == "download"]
            if write_ops:
                write_duration = max(
                    max(op[2] for op in write_ops) - min(op[1] for op in write_ops), 1e-6
                )
            if read_ops:
                read_duration = max(
                    max(op[2] for op in read_ops) - min(op[1] for op in read_ops), 1e-6
                )

        if write_duration == 0.0 and self.write_bytes > 0:
            write_duration = wall_clock
        if read_duration == 0.0 and self.read_bytes > 0:
            read_duration = wall_clock

        write_MBps_avg = (self.write_bytes / 1024 / 1024 / write_duration) if write_duration > 0 else 0.0
        read_MBps_avg = (self.read_bytes / 1024 / 1024 / read_duration) if read_duration > 0 else 0.0

        out = {}
        if self.meta:
            out["meta"] = self.meta
        out.update({
            "duration_sec": active_duration if active_duration > 0 else wall_clock,
            "wall_clock_sec": wall_clock,
            "write_bytes": self.write_bytes,
            "read_bytes": self.read_bytes,
            "write_duration_sec": write_duration,
            "read_duration_sec": read_duration,
            "read_MBps_avg": read_MBps_avg,
            "write_MBps_avg": write_MBps_avg,
            "read_ok_ops": self.read_ops_ok,
            "write_ok_ops": self.write_ops_ok,
            "err_ops": self.err_ops,
            "warmup_ops": self.warmup_ops,
        })
        if self.client_overhead_ms is not None:
            out["client_overhead_ms"] = self.client_overhead_ms
        if self.error_counts:
            out["errors"] = dict(sorted(self.error_counts.items(), key=lambda kv: -kv[1]))
        with self._lock:
            out["timeline"] = build_timeline(self.ops)

        latency = {}
        write_lat = summarize_latencies(self.write_latencies_ms)
        read_lat = summarize_latencies(self.read_latencies_ms)
        if write_lat:
            latency["write"] = write_lat
        if read_lat:
            latency["read"] = read_lat
        if latency:
            out["latency"] = latency

        for op_type, key in (("upload", "write_file_analysis"), ("download", "read_file_analysis")):
            small_stats, large_stats, overall = self.get_file_stats(op_type)
            if small_stats or large_stats:
                out[key] = {
                    "top10_small": small_stats or [],
                    "top10_large": large_stats or [],
                    "overall": overall,
                }

        # Закрываем CSV до записи отчёта, чтобы обе выгрузки были полными
        self.close()
        with open(self.json_path, "w") as f:
            json.dump(out, f, indent=2)
        return out


def gather_files(root: Path):
    files = []
    for p in root.rglob("*"):
        name = p.name
        if name.startswith('.'):
            continue
        try:
            if p.is_file():
                files.append(p)
        except OSError:
            continue
    return files


def run_profile(args):
    profile = getattr(args, "profile", "write")
    # Поддерживаем старое имя профиля mixed-70-30 (обратная совместимость)
    if profile == "mixed-70-30":
        profile = "mixed"
    order = getattr(args, "order", "sequential")
    
    jobs: list[Job] = []
    groups: dict[str, dict[str, float]] = {}
    total_files = 0
    total_bytes = 0
    data_root = None
    
    # Для read профиля получаем список объектов из бакета
    if profile == "read":
        endpoints_list = list(getattr(args, "endpoints", []) or [])
        if not endpoints_list:
            maybe_single = getattr(args, "endpoint", None)
            if maybe_single:
                endpoints_list = [maybe_single]
        if not endpoints_list:
            print("No endpoint configured for read profile")
            return
        
        primary_endpoint = endpoints_list[0]
        print(f"Fetching object list from bucket {args.bucket}...")
        objects = aws_list_objects(
            args.bucket,
            primary_endpoint,
            getattr(args, "access_key", None),
            getattr(args, "secret_key", None),
            getattr(args, "aws_profile", None),
        )
        if not objects:
            print(f"No objects found in bucket {args.bucket}")
            return
        
        # Создаём jobs из объектов бакета
        total_files = len(objects)
        for obj in objects:
            key = obj["key"]
            size = obj["size"]
            # Определяем группу по размеру (аналогично файлам)
            if size < 100 * 1024 * 1024:  # < 100MB
                group = "small"
            elif size < 1024 * 1024 * 1024:  # < 1GB
                group = "medium"
            else:
                group = "large"
            # Для read профиля path не используется, но нужен для совместимости с Job
            fake_path = Path(key)
            jobs.append(Job(path=fake_path, size=size, group=group, remote_key=key))
            total_bytes += size
            grp = groups.setdefault(group, {"total_files": 0, "total_bytes": 0, "done_files": 0, "done_bytes": 0, "errors": 0})
            grp["total_files"] += 1
            grp["total_bytes"] += size
        
        # Сортировка по порядку
        if order == "sequential":
            jobs.sort(key=lambda j: j.size)  # Маленькие → средние → большие
        else:  # random
            random.shuffle(jobs)
        
        group_summary = ", ".join(f"{g}={info['total_files']}" for g, info in groups.items())
        print(f"Loaded {total_files} objects totalling {total_bytes/1024/1024:.1f} MB from bucket across groups: {group_summary}")
    else:
        # Для write и mixed профилей используем файлы из data_dir
        data_root = Path(args.data_dir).resolve()
        if not data_root.exists():
            print(f"Data dir not found: {data_root}")
            return

        files = gather_files(data_root)
        if not files:
            print(f"No dataset files found under {data_root}")
            return

        try:
            files.sort(key=lambda p: p.stat().st_size)
            jobs: list[Job] = []
            groups = {}
            total_files = len(files)
            total_bytes = 0
            for p in files:
                size = p.stat().st_size
                rel = p.relative_to(data_root)
                group = rel.parts[0] if rel.parts else "root"
                jobs.append(Job(path=p, size=size, group=group))
                total_bytes += size
                grp = groups.setdefault(group, {"total_files": 0, "total_bytes": 0, "done_files": 0, "done_bytes": 0, "errors": 0})
                grp["total_files"] += 1
                grp["total_bytes"] += size
            
            # Сортировка по порядку
            if order == "sequential":
                jobs.sort(key=lambda j: j.size)  # Маленькие → средние → большие
            else:  # random
                random.shuffle(jobs)
            
            group_summary = ", ".join(f"{g}={info['total_files']}" for g, info in groups.items())
            print(f"Loaded {total_files} files totalling {total_bytes/1024/1024:.1f} MB across groups: {group_summary}")
        except OSError as e:
            print(f"Failed to stat dataset files: {e}")
            return

    endpoints_list = list(getattr(args, "endpoints", []) or [])
    if not endpoints_list:
        maybe_single = getattr(args, "endpoint", None)
        if maybe_single:
            endpoints_list = [maybe_single]
    endpoint_mode = getattr(args, "endpoint_mode", "round-robin") or "round-robin"
    endpoint_mode = endpoint_mode if endpoint_mode in {"round-robin", "random"} else "round-robin"
    endpoint_lock = threading.Lock()
    endpoint_rr_index = 0

    def next_endpoint() -> str:
        nonlocal endpoint_rr_index
        if not endpoints_list:
            raise RuntimeError("No endpoints configured")
        if len(endpoints_list) == 1:
            return endpoints_list[0]
        if endpoint_mode == "random":
            return random.choice(endpoints_list)
        with endpoint_lock:
            endpoint = endpoints_list[endpoint_rr_index]
            endpoint_rr_index = (endpoint_rr_index + 1) % len(endpoints_list)
            return endpoint

    # Используем Queue с лимитом, если задан
    queue_limit = getattr(args, "queue_limit", None)
    q = queue.Queue(maxsize=queue_limit) if queue_limit else queue.Queue()
    pending_counts = {g: info["total_files"] for g, info in groups.items()}
    
    # Инициализация параметров
    mixed_read_ratio = getattr(args, "mixed_read_ratio", 0.7)
    pattern = getattr(args, "pattern", "sustained")
    burst_duration_sec = getattr(args, "burst_duration_sec", 10.0)
    burst_intensity_multiplier = getattr(args, "burst_intensity_multiplier", 10.0)
    max_retries = getattr(args, "max_retries", 3)
    retry_backoff_base = getattr(args, "retry_backoff_base", 2.0)
    unique_remote_names = bool(getattr(args, "unique_remote_names", False))
    
    # Инициализация очереди в зависимости от профиля
    if profile == "read":
        # Для read профиля сразу добавляем задачи на чтение
        for job in jobs:
            q.put(("download", job))
    elif profile == "write":
        # Для write профиля добавляем задачи на запись
        for job in jobs:
            q.put(("upload", job))
    elif profile == "mixed":
        # Для mixed профиля сначала загружаем данные
        for job in jobs:
            q.put(("upload", job))
    warmup_sec = float(getattr(args, "warmup_sec", 0.0) or 0.0)
    metrics = Metrics(args.metrics, args.report, warmup_sec=warmup_sec)
    try:
        from importlib.metadata import version as _pkg_version
        _version = _pkg_version("s3flood")
    except Exception:
        _version = "unknown"
    metrics.meta = {
        "version": _version,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "hostname": socket.gethostname(),
        "profile": profile,
        "pattern": pattern,
        "threads": args.threads,
        "endpoints": endpoints_list,
        "endpoint_mode": endpoint_mode,
        "bucket": args.bucket,
        "warmup_sec": warmup_sec,
        "infinite": bool(getattr(args, "infinite", False)),
    }
    if warmup_sec > 0:
        print(f"Warmup: первые {warmup_sec:.0f} с исключаются из статистики")

    # Базовый оверхед клиента: время холодного старта aws CLI без сетевых операций.
    # Он входит в latency каждой операции — фиксируем для честной интерпретации отчёта.
    try:
        _t0 = time.time()
        subprocess.run(["aws", "--version"], capture_output=True, timeout=30)
        metrics.client_overhead_ms = round((time.time() - _t0) * 1000, 1)
        print(f"Оверхед запуска aws CLI: ~{metrics.client_overhead_ms:.0f} ms входит в latency каждой операции")
    except Exception:
        pass

    stop = threading.Event()
    
    # Обработчик сигнала для корректного завершения всех процессов
    original_sigint = None
    interrupt_count = [0]  # Используем список для изменяемого значения в замыкании
    
    def signal_handler(signum, frame):
        """Обработчик сигнала прерывания для завершения всех активных процессов."""
        interrupt_count[0] += 1
        if interrupt_count[0] == 1:
            # Первое прерывание - корректное завершение
            print("\n[Получен сигнал прерывания, завершаем процессы...]", flush=True)
            stop.set()
            _terminate_all_processes()
        else:
            # Второе прерывание - принудительный выход
            print("\n[Принудительное завершение...]", flush=True)
            _terminate_all_processes()
            # Восстанавливаем стандартный обработчик и вызываем его
            if original_sigint is not None and original_sigint != signal.SIG_IGN:
                signal.signal(signal.SIGINT, original_sigint)
                os.kill(os.getpid(), signal.SIGINT)
    
    # Регистрируем обработчик сигнала только в главном потоке
    if threading.current_thread() is threading.main_thread():
        original_sigint = signal.signal(signal.SIGINT, signal_handler)
        # Сохраняем оригинальный обработчик для восстановления при выходе
    
    active_lock = threading.Lock()
    active_uploads = 0
    active_downloads = 0
    active_jobs: dict[int, Job] = {}
    group_lock = threading.Lock()
    pending_lock = threading.Lock()
    # Словарь: исходное имя файла -> данные о последней загрузке (remote_key + endpoint)
    uploaded_objects: dict[str, dict[str, str]] = {}
    uploaded_objects_lock = threading.Lock()
    upload_phase_done = threading.Event()
    
    # Состояние для паттернов
    burst_active = False
    burst_start_time = None
    pattern_lock = threading.Lock()
    
    # Счетчик циклов для infinite режима
    cycle_count = 0
    cycle_lock = threading.Lock()
    last_cycle_restart = 0.0  # Время последнего перезапуска цикла
    
    # Отслеживание файлов в текущем цикле для infinite режима
    files_in_current_cycle = 0
    cycle_files_lock = threading.Lock()
    
    # Для bursty режима в mixed профиле: инициализируем множество ID дополнительных потоков
    extra_thread_ids = set()

    def worker():
        nonlocal active_uploads, active_downloads, files_in_current_cycle, extra_thread_ids
        # Для bursty режима в mixed профиле: дополнительные потоки работают только во время всплеска
        current_thread_id = threading.get_ident()
        is_extra_thread = current_thread_id in extra_thread_ids if pattern == "bursty" and profile == "mixed" else False
        
        while not stop.is_set():
            # Для дополнительных потоков в bursty режиме: работаем только во время всплеска
            if is_extra_thread:
                with pattern_lock:
                    if not burst_active:
                        # Во время паузы дополнительные потоки ждут
                        time.sleep(0.1)
                        continue
            
            try:
                op, job = q.get(timeout=0.5)
            except queue.Empty:
                # Для write и read профилей: если очередь пуста, завершаем worker
                # Для mixed профиля: если upload фаза завершена и очередь пуста, завершаем worker
                if profile == "mixed":
                    if upload_phase_done.is_set() and q.empty():
                        break
                else:
                    if q.empty() and not getattr(args, "infinite", False):
                        break
                if not getattr(args, "infinite", False):
                    continue
                else:
                    continue
            start = time.time()
            remote_key = None
            if op == "upload":
                base_name = job.path.name
                remote_key = make_remote_key(base_name, unique_remote_names)
                display_name = remote_key
            elif op == "download":
                if profile == "read":
                    remote_key = job.remote_key or str(job.path)
                else:
                    remote_key = job.remote_key or job.path.name
                display_name = remote_key
            else:
                display_name = job.path.name
            recent_op_id = metrics.start_recent_op(op, display_name, job.size, start)
            with active_lock:
                if op == "upload":
                    active_uploads += 1
                elif op == "download":
                    active_downloads += 1
                active_jobs[threading.get_ident()] = job
            with pending_lock:
                if op == "upload":
                    pending_counts[job.group] -= 1
            if op == "upload":
                endpoint = next_endpoint()
                # Сохраняем endpoint в job для последующего использования
                job.endpoint = endpoint
                # Используем retry с backoff
                res, ok, err, attempts = retry_with_backoff(
                    aws_cp_upload,
                    max_retries,
                    retry_backoff_base,
                    job.path,
                    args.bucket,
                    remote_key or job.path.name,
                    endpoint,
                    getattr(args, "access_key", None),
                    getattr(args, "secret_key", None),
                    getattr(args, "aws_profile", None),
                    getattr(args, "aws_cli_multipart_threshold", None),
                    getattr(args, "aws_cli_multipart_chunksize", None),
                    getattr(args, "aws_cli_max_concurrent_requests", None),
                    stop=stop,
                )
                if not ok and res is None:
                    err = err or "retry failed"
                end = time.time()
                nbytes = job.size
                filename = job.path.name
                metrics.record(
                    "upload", start, end, nbytes, ok, err, display_name, recent_op_id,
                    endpoint=endpoint, thread_id=threading.get_ident(),
                    attempt=attempts, size_group=job.group,
                )
                if ok:
                    with group_lock:
                        grp = groups[job.group]
                        grp["done_files"] += 1
                        grp["done_bytes"] += nbytes
                    job.remote_key = display_name
                    with uploaded_objects_lock:
                        uploaded_objects[job.path.name] = {
                            "remote_key": display_name,
                            "endpoint": endpoint,
                        }
                    # Обновляем счетчик файлов в текущем цикле для infinite режима
                    if getattr(args, "infinite", False) and op == "upload":
                        with cycle_files_lock:
                            files_in_current_cycle += 1
                else:
                    with group_lock:
                        groups[job.group]["errors"] += 1
            elif op == "download":
                # Для read профиля используем key из path (который содержит имя объекта)
                # Для других профилей используем remote_key (если он есть) или имя файла
                key = remote_key or (str(job.path) if profile == "read" else job.path.name)
                # Используем endpoint из job, если он был сохранён при записи, иначе выбираем новый
                endpoint = job.endpoint if job.endpoint else next_endpoint()
                # Используем retry с backoff
                res, ok, err, attempts = retry_with_backoff(
                    aws_cp_download,
                    max_retries,
                    retry_backoff_base,
                    args.bucket,
                    key,
                    endpoint,
                    getattr(args, "access_key", None),
                    getattr(args, "secret_key", None),
                    getattr(args, "aws_profile", None),
                    getattr(args, "aws_cli_multipart_threshold", None),
                    getattr(args, "aws_cli_multipart_chunksize", None),
                    getattr(args, "aws_cli_max_concurrent_requests", None),
                    stop=stop,
                )
                end = time.time()
                if not ok and not err:
                    if res is None:
                        err = "retry failed: no result"
                    elif getattr(res, "stderr", None):
                        err = res.stderr[-300:]
                    else:
                        err = f"exit code {getattr(res, 'returncode', '?')}"
                # Для download используем размер из job.size (известен из списка объектов)
                # aws s3 cp не возвращает JSON с размером, в отличие от s3api get-object
                nbytes = job.size
                # Определяем имя файла для отображения
                filename = key
                metrics.record(
                    "download", start, end, nbytes, ok, err, filename, recent_op_id,
                    endpoint=endpoint, thread_id=threading.get_ident(),
                    attempt=attempts, size_group=job.group,
                )
            with active_lock:
                if op == "upload":
                    active_uploads -= 1
                elif op == "download":
                    active_downloads -= 1
                active_jobs.pop(threading.get_ident(), None)
            q.task_done()

    threads = []
    max_threads = args.threads
    if pattern == "bursty" and profile == "mixed":
        # Для bursty режима в mixed профиле создаем максимальное количество потоков
        max_threads = int(args.threads * burst_intensity_multiplier)
    
    # Создаем базовые потоки
    base_threads = args.threads
    for _ in range(base_threads):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)
    
    # Для bursty режима в mixed профиле создаем дополнительные потоки
    if pattern == "bursty" and profile == "mixed":
        for _ in range(max_threads - base_threads):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            extra_thread_ids.add(t.ident)
            threads.append(t)

    last_print = 0
    last_plain_log = 0.0
    download_phase_started = False
    mixed_phase_started = False
    # История RPS для спарклайнов дашборда (~24 с при тике 0.5 с)
    write_rps_history: deque = deque(maxlen=48)
    read_rps_history: deque = deque(maxlen=48)

    from rich.console import Console as _RichConsole
    from rich.live import Live as _RichLive
    from .dashboard import build_dashboard
    _console = _RichConsole()
    # Живой дашборд только в терминале; в CI/пайпе — краткая строка раз в 5 с
    live = _RichLive(console=_console, auto_refresh=False, transient=False) if _console.is_terminal else None
    # Для read профиля используем key из path, для других - path.name
    if profile == "read":
        key_to_job = {str(job.path): job for job in jobs}  # path содержит key объекта
    else:
        key_to_job = {job.path.name: job for job in jobs}
    
    def start_mixed_phase():
        """Запускает смешанную фазу для mixed профиля."""
        nonlocal mixed_phase_started
        with uploaded_objects_lock:
            if uploaded_objects:
                mixed_phase_started = True
                # Создаём список всех загруженных объектов для смешанных операций
                uploaded_list = list(uploaded_objects.items())
                random.shuffle(uploaded_list)
                for key, info in uploaded_list:
                    endpoint = info.get("endpoint")
                    remote_key_value = info.get("remote_key")
                    if key in key_to_job:
                        job = key_to_job[key]
                        job.endpoint = endpoint
                        job.remote_key = remote_key_value
                        # Решаем, что делать: чтение или запись (с учётом пропорции)
                        if random.random() < mixed_read_ratio:
                            try:
                                q.put(("download", job), block=False)
                            except queue.Full:
                                pass
                        else:
                            # Для записи создаём новый job из того же файла
                            try:
                                q.put(("upload", job), block=False)
                            except queue.Full:
                                pass
                upload_phase_done.set()
    
    def manage_burst_pattern():
        """Управляет паттерном bursty: чередует периоды высокой и низкой нагрузки."""
        nonlocal burst_active, burst_start_time
        with pattern_lock:
            now = time.time()
            if pattern == "bursty":
                if not burst_active:
                    # Начинаем всплеск
                    burst_active = True
                    burst_start_time = now
                elif burst_start_time and (now - burst_start_time) >= burst_duration_sec:
                    # Заканчиваем всплеск
                    burst_active = False
                    burst_start_time = None
            else:
                burst_active = False
    
    try:
        if live is not None:
            live.start()
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
            now = time.time()
            
            # Управление паттерном bursty
            manage_burst_pattern()
            
            # Проверяем, завершены ли все upload операции, и начинаем следующую фазу
            with pending_lock:
                upload_pending = q.qsize()
            with active_lock:
                upload_active = active_uploads
            
            # Переключение фаз в зависимости от профиля
            # Для write профиля - только запись, фаза чтения не запускается
            # Для read профиля - только чтение, фаза записи не нужна
            if not download_phase_started and not mixed_phase_started:
                # Проверяем завершение операций в зависимости от профиля
                operations_pending = 0
                operations_active = 0
                if profile == "write":
                    operations_pending = upload_pending
                    operations_active = upload_active
                elif profile == "read":
                    with active_lock:
                        operations_active = active_downloads
                    with pending_lock:
                        operations_pending = q.qsize()
                
                if operations_pending == 0 and operations_active == 0:
                    if profile == "mixed":
                        start_mixed_phase()
                    elif getattr(args, "infinite", False):
                        # Бесконечный режим: после завершения всех файлов начинаем новый цикл
                        # Защита от повторного запуска в течение 1 секунды
                        if now - last_cycle_restart > 1.0:
                            if profile == "write":
                                # Для write профиля добавляем все файлы снова в очередь
                                with cycle_lock:
                                    cycle_count += 1
                                with cycle_files_lock:
                                    files_in_current_cycle = 0  # Сбрасываем счетчик для нового цикла
                                for job in jobs:
                                    try:
                                        q.put(("upload", job), block=False)
                                    except queue.Full:
                                        pass
                                last_cycle_restart = now
                            elif profile == "read":
                                # Для read профиля добавляем все объекты снова в очередь
                                with cycle_lock:
                                    cycle_count += 1
                                with cycle_files_lock:
                                    files_in_current_cycle = 0  # Сбрасываем счетчик для нового цикла
                                for job in jobs:
                                    try:
                                        q.put(("download", job), block=False)
                                    except queue.Full:
                                        pass
                                last_cycle_restart = now
            
            # Для mixed профиля: добавляем новые задачи в смешанном режиме
            if mixed_phase_started and profile == "mixed":
                with uploaded_objects_lock:
                    if uploaded_objects and q.qsize() < (queue_limit or 1000):
                        # Добавляем новые задачи в зависимости от паттерна
                        intensity = burst_intensity_multiplier if burst_active else 1.0
                        tasks_to_add = int(args.threads * intensity) if burst_active else 1
                        uploaded_list = list(uploaded_objects.items())
                        random.shuffle(uploaded_list)
                        added = 0
                        for key, info in uploaded_list:
                            if added >= tasks_to_add:
                                break
                            if key in key_to_job:
                                job = key_to_job[key]
                                job.endpoint = info.get("endpoint")
                                job.remote_key = info.get("remote_key")
                                if random.random() < mixed_read_ratio:
                                    try:
                                        q.put(("download", job), block=False)
                                        added += 1
                                    except queue.Full:
                                        break
                                else:
                                    try:
                                        q.put(("upload", job), block=False)
                                        added += 1
                                    except queue.Full:
                                        break
            
            if now - last_print >= 0.5:  # Обновляем дашборд каждые 0.5 секунды для плавной анимации спиннера
                rbps, wbps, write_rps, read_rps = metrics.current_rates(5.0)
                files_done = metrics.write_ops_ok
                files_read = metrics.read_ops_ok
                files_err = metrics.err_ops
                avg_wbps = metrics.avg_write_rate()
                avg_rbps = metrics.avg_read_rate()
                with active_lock:
                    inflight = active_uploads + active_downloads
                    active_uploads_snap = active_uploads
                    active_downloads_snap = active_downloads
                with pending_lock:
                    pending = q.qsize()
                elapsed = metrics.elapsed()
                bytes_done = metrics.write_bytes
                bytes_read = metrics.read_bytes
                wbps_mb = wbps / 1024 / 1024
                rbps_mb = rbps / 1024 / 1024
                avg_wbps_mb = avg_wbps / 1024 / 1024
                avg_rbps_mb = avg_rbps / 1024 / 1024
                write_rps = max(write_rps, 0.0)
                read_rps = max(read_rps, 0.0)
                
                # ETA: для фазы записи или чтения
                eta_sec = None
                if profile == "read":
                    # Для read профиля: считаем ETA по оставшимся файлам для чтения
                    files_left_read = max(total_files - files_read, 0)
                    if rbps > 1 and files_left_read > 0:
                        avg_file_size = bytes_read / files_read if files_read > 0 else (total_bytes / total_files if total_files > 0 else 0)
                        bytes_left_read = avg_file_size * files_left_read
                        eta_sec = bytes_left_read / rbps
                elif download_phase_started or mixed_phase_started:
                    # В фазе чтения или mixed: считаем ETA по оставшимся файлам для чтения
                    with uploaded_objects_lock:
                        total_to_read = len(uploaded_objects)
                    files_left_read = max(total_to_read - files_read, 0)
                    if rbps > 1 and files_left_read > 0:
                        # Приблизительная оценка: средний размер файла * оставшиеся файлы / скорость чтения
                        avg_file_size = bytes_read / files_read if files_read > 0 else (total_bytes / total_files if total_files > 0 else 0)
                        bytes_left_read = avg_file_size * files_left_read
                        eta_sec = bytes_left_read / rbps
                else:
                    # В фазе записи
                    if wbps > 1 and bytes_done < total_bytes:
                        eta_sec = (total_bytes - bytes_done) / wbps
                eta_str = f"{eta_sec/60:.1f} min" if eta_sec and eta_sec > 60 else (f"{eta_sec:.0f} s" if eta_sec else "n/a")
                
                # Определяем фазу для отображения
                if profile == "read" or download_phase_started:
                    phase = "READ"
                elif mixed_phase_started:
                    phase = "MIXED"
                else:
                    phase = "WRITE"
                effective_threads = args.threads
                if pattern == "bursty" and burst_active:
                    effective_threads = int(args.threads * burst_intensity_multiplier)
                with uploaded_objects_lock:
                    total_to_read = len(uploaded_objects)
                with cycle_lock:
                    cycle_snapshot = cycle_count
                with cycle_files_lock:
                    current_cycle_files = files_in_current_cycle
                write_rps_history.append(write_rps)
                read_rps_history.append(read_rps)
                recent_per_type = max(1, min(getattr(args, "threads", 1), 15))
                recent_ops_snapshot = metrics.get_recent_ops(recent_per_type * 4)
                done_ops = [e for e in recent_ops_snapshot if e.get("done")]
                active_ops = [e for e in recent_ops_snapshot if not e.get("done")]
                display_ops = done_ops[-recent_per_type:] + active_ops[-recent_per_type:]
                endpoint_disp = ", ".join(endpoints_list[:2])
                if len(endpoints_list) > 2:
                    endpoint_disp += f" +{len(endpoints_list) - 2}"
                state = {
                    "profile": profile,
                    "pattern": pattern,
                    "version": (metrics.meta or {}).get("version"),
                    "endpoint": endpoint_disp,
                    "bucket": args.bucket,
                    "write_rps_history": list(write_rps_history),
                    "read_rps_history": list(read_rps_history),
                    "burst_active": burst_active,
                    "infinite": bool(getattr(args, "infinite", False)),
                    "cycle_count": cycle_snapshot,
                    "elapsed": elapsed,
                    "eta": eta_str if eta_sec else None,
                    "phase": phase,
                    "warmup_active": warmup_sec > 0 and now < metrics.warmup_until,
                    "total_files": total_files,
                    "files_done": files_done,
                    "files_read": files_read,
                    "files_err": files_err,
                    "total_to_read": total_to_read,
                    "current_cycle_files": current_cycle_files,
                    "bytes_done": bytes_done,
                    "bytes_read": bytes_read,
                    "total_bytes": total_bytes,
                    "write_rps": write_rps,
                    "read_rps": read_rps,
                    "wbps_mb": wbps_mb,
                    "rbps_mb": rbps_mb,
                    "avg_wbps_mb": avg_wbps_mb,
                    "avg_rbps_mb": avg_rbps_mb,
                    "inflight": inflight,
                    "threads": effective_threads,
                    "active_uploads": active_uploads_snap,
                    "active_downloads": active_downloads_snap,
                    "queue": pending,
                    "recent_ops": display_ops,
                    "now": now,
                }
                if live is not None:
                    live.update(build_dashboard(state), refresh=True)
                elif now - last_plain_log >= 5.0:
                    print(
                        f"[{elapsed:7.1f}s] {phase} W:{files_done}/{total_files} R:{files_read} "
                        f"Err:{files_err} W-RPS:{write_rps:.2f} R-RPS:{read_rps:.2f} "
                        f"Wcur:{wbps_mb:.1f}MB/s Rcur:{rbps_mb:.1f}MB/s queue:{pending}",
                        flush=True,
                    )
                    last_plain_log = now
                last_print = now
    except KeyboardInterrupt:
        # Если KeyboardInterrupt все еще произошел (например, если обработчик сигнала не сработал)
        if not stop.is_set():
            print("\n[Получен сигнал прерывания, завершаем процессы...]", flush=True)
            stop.set()
            _terminate_all_processes()
    finally:
        if live is not None:
            live.stop()

    # Восстанавливаем оригинальный обработчик сигнала
    if threading.current_thread() is threading.main_thread() and original_sigint is not None:
        signal.signal(signal.SIGINT, original_sigint)

    # Завершаем все процессы перед выходом
    _terminate_all_processes()

    for t in threads:
        t.join()

    summary = metrics.finalize()
    print_summary(summary, metrics.csv_path, metrics.json_path)


def print_summary(summary: dict, csv_path: str, json_path: str) -> None:
    """Печатает итог прогона: rich-таблицы вместо сырого JSON."""
    try:
        from rich import box
        from rich.console import Console
        from rich.table import Table
    except ImportError:
        print("SUMMARY:", json.dumps(summary, indent=2))
        return

    console = Console()
    meta = summary.get("meta", {})
    title = f"Итог прогона: {meta.get('profile', '?')}"
    if meta.get("version"):
        title += f" | s3flood {meta['version']}"
    console.print()
    console.rule(title)

    from rich.panel import Panel as RichPanel

    from .dashboard import sparkline
    from .metrics import summary_speed_stats

    stats = summary_speed_stats(summary)
    speed_lines = [
        f"[bold cyan]{stats['total_MBps']:.1f} MB/s[/bold cyan] сквозная скорость",
        f"↑ запись {stats['write_MBps']:.1f} MB/s   ↓ чтение {stats['read_MBps']:.1f} MB/s   "
        f"{stats['ops_per_sec']:.1f} оп/с   пик {stats['peak_MBps']:.1f} MB/s",
    ]
    spark = sparkline(stats["speeds"], width=48)
    if spark:
        speed_lines.append(f"[cyan]{spark}[/cyan] MB/s по времени")
    console.print(RichPanel("\n".join(speed_lines), title="Скорость",
                            title_align="left", border_style="cyan"))

    tp = Table(box=box.SIMPLE_HEAVY, title="Пропускная способность", title_justify="left")
    tp.add_column("")
    tp.add_column("операций OK", justify="right")
    tp.add_column("объём", justify="right")
    tp.add_column("средняя скорость", justify="right")
    tp.add_row(
        "Запись", str(summary.get("write_ok_ops", 0)),
        format_bytes(summary.get("write_bytes", 0)),
        f"{summary.get('write_MBps_avg', 0.0):.1f} MB/s",
    )
    tp.add_row(
        "Чтение", str(summary.get("read_ok_ops", 0)),
        format_bytes(summary.get("read_bytes", 0)),
        f"{summary.get('read_MBps_avg', 0.0):.1f} MB/s",
    )
    tp.add_row(
        "[bold]Итого[/bold]",
        str(summary.get("write_ok_ops", 0) + summary.get("read_ok_ops", 0)),
        format_bytes(summary.get("write_bytes", 0) + summary.get("read_bytes", 0)),
        f"[bold]{stats['total_MBps']:.1f} MB/s[/bold]",
    )
    console.print(tp)

    latency = summary.get("latency") or {}
    if latency:
        lt = Table(box=box.SIMPLE_HEAVY, title="Латентность, мс", title_justify="left")
        lt.add_column("")
        for col in ("p50", "p95", "p99", "avg"):
            lt.add_column(col, justify="right")
        for name, key in (("Запись", "write"), ("Чтение", "read")):
            data = latency.get(key)
            if data:
                lt.add_row(
                    name,
                    *(f"{data[k]:.0f}" for k in ("p50_ms", "p95_ms", "p99_ms", "avg_ms")),
                )
        console.print(lt)
        if summary.get("client_overhead_ms"):
            console.print(
                f"[dim]В латентность каждой операции входит ~{summary['client_overhead_ms']:.0f} мс "
                f"оверхеда запуска aws CLI[/dim]"
            )

    errors = summary.get("errors") or {}
    if errors:
        et = Table(box=box.SIMPLE_HEAVY, title="Ошибки", title_justify="left")
        et.add_column("тип")
        et.add_column("количество", justify="right")
        for err_type, count in errors.items():
            et.add_row(f"[red]{err_type}[/red]", str(count))
        console.print(et)

    duration = summary.get("duration_sec", 0.0)
    wall = summary.get("wall_clock_sec", 0.0)
    console.print(
        f"Активное время: [bold]{duration:.1f} с[/bold] (wall clock {wall:.1f} с)"
        + (f" | warmup-операций исключено: {summary['warmup_ops']}" if summary.get("warmup_ops") else "")
    )
    console.print(f"[dim]Отчёт: {json_path} | метрики: {csv_path}[/dim]")
