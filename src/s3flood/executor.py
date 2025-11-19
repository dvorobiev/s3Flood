import json, time, queue, threading, subprocess, os, csv, statistics, sys, re, random, math
from pathlib import Path
from collections import deque
from dataclasses import dataclass

# Minimal executor with AWS CLI runner only (v1)

ANSI_RESET = "\x1b[0m"
ANSI_BOLD = "\x1b[1m"
ANSI_DIM = "\x1b[2m"
ANSI_RED = "\x1b[31m"
ANSI_GREEN = "\x1b[32m"
ANSI_YELLOW = "\x1b[33m"
ANSI_BLUE = "\x1b[34m"
ANSI_MAGENTA = "\x1b[35m"
ANSI_CYAN = "\x1b[36m"

ANSI_REGEX = re.compile(r"\x1b\[[0-9;]*m")


def visible_len(s: str) -> int:
    return len(ANSI_REGEX.sub("", s))


def style(text: str, *codes: str) -> str:
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


class Metrics:
    def __init__(self, metrics_csv: str, report_json: str):
        self.csv_path = metrics_csv
        self.json_path = report_json
        self._lock = threading.Lock()
        self.ops = []  # per-op dicts
        self.window = deque(maxlen=400)
        self._start = time.time()
        self.read_bytes = 0
        self.write_bytes = 0
        self.read_ops_ok = 0
        self.write_ops_ok = 0
        self.err_ops = 0
        self.last_upload = None
        self.last_download = None
        self.upload_latencies = []
        self.download_latencies = []
        with open(self.csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["ts_start","ts_end","op","bytes","status","latency_ms","error"])
            w.writeheader()

    def elapsed(self) -> float:
        return max(time.time() - self._start, 1e-6)

    def avg_write_rate(self) -> float:
        return self.write_bytes / self.elapsed()

    def avg_read_rate(self) -> float:
        return self.read_bytes / self.elapsed()

    def record(self, op: str, start: float, end: float, nbytes: int, ok: bool, err: str|None):
        lat_ms = int((end-start)*1000)
        with self._lock:
            with open(self.csv_path, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=["ts_start","ts_end","op","bytes","status","latency_ms","error"])
                w.writerow({
                    "ts_start": start, "ts_end": end, "op": op, "bytes": nbytes,
                    "status": "ok" if ok else "err", "latency_ms": lat_ms, "error": err or ""
                })
            self.ops.append((op, start, end, nbytes, ok, lat_ms))
            self.window.append((end, op, nbytes, ok, lat_ms))
            if ok:
                if op == "download":
                    self.read_ops_ok += 1
                    self.read_bytes += nbytes
                    self.last_download = {"bytes": nbytes, "lat_ms": lat_ms, "ended": end}
                    self.download_latencies.append(lat_ms)
                elif op == "upload":
                    self.write_ops_ok += 1
                    self.write_bytes += nbytes
                    self.last_upload = {"bytes": nbytes, "lat_ms": lat_ms, "ended": end}
                    self.upload_latencies.append(lat_ms)
            else:
                self.err_ops += 1

    def current_rates(self, window_sec=5.0):
        now = time.time()
        rb = wb = 0
        ops = 0
        for t, op, nbytes, ok, lat_ms in list(self.window):
            if now - t <= window_sec and ok:
                ops += 1
                if op == "download": rb += nbytes
                elif op == "upload": wb += nbytes
        return rb/window_sec, wb/window_sec, ops/window_sec if window_sec > 0 else 0.0

    def last_latency_ms(self, op: str) -> float | None:
        data = self.last_download if op == "download" else self.last_upload
        if data:
            return data["lat_ms"]
        return None

    def latency_percentiles(self, op_type="upload"):
        latencies = self.upload_latencies if op_type == "upload" else self.download_latencies
        if not latencies:
            return None, None, None
        data = sorted(latencies)
        median = statistics.median(data)
        p90_idx = max(int(len(data) * 0.9) - 1, 0)
        p95_idx = max(int(len(data) * 0.95) - 1, 0)
        return median, data[p90_idx], data[p95_idx]

    def finalize(self):
        dur = max(time.time() - self._start, 1e-6)
        
        # Вычисляем реальное время выполнения для каждой фазы
        # Для последовательных фаз (write-heavy, read-heavy) вычисляем время каждой фазы отдельно
        # Для смешанных фаз (mixed) операции могут идти параллельно, используем общее время
        write_duration = 0.0
        read_duration = 0.0
        
        if self.ops:
            # Находим первое и последнее время для каждой операции (только успешные)
            write_ops = [op for op in self.ops if op[0] == "upload" and op[4]]  # op, start, end, nbytes, ok, lat_ms
            read_ops = [op for op in self.ops if op[0] == "download" and op[4]]
            
            if write_ops:
                write_start = min(op[1] for op in write_ops)  # start time
                write_end = max(op[2] for op in write_ops)    # end time
                write_duration = max(write_end - write_start, 1e-6)
            
            if read_ops:
                read_start = min(op[1] for op in read_ops)    # start time
                read_end = max(op[2] for op in read_ops)      # end time
                read_duration = max(read_end - read_start, 1e-6)
        
        # Если не удалось вычислить по операциям, используем общее время
        if write_duration == 0.0 and self.write_bytes > 0:
            write_duration = dur
        if read_duration == 0.0 and self.read_bytes > 0:
            read_duration = dur
        
        # Вычисляем средние скорости на основе реального времени каждой фазы
        write_MBps_avg = (self.write_bytes / 1024 / 1024 / write_duration) if write_duration > 0 else 0.0
        read_MBps_avg = (self.read_bytes / 1024 / 1024 / read_duration) if read_duration > 0 else 0.0
        
        out = {
            "duration_sec": dur,
            "write_bytes": self.write_bytes,
            "read_bytes": self.read_bytes,
            "write_duration_sec": write_duration,
            "read_duration_sec": read_duration,
            "read_MBps_avg": read_MBps_avg,
            "write_MBps_avg": write_MBps_avg,
            "read_ok_ops": self.read_ops_ok,
            "write_ok_ops": self.write_ok_ops,
            "err_ops": self.err_ops,
        }
        with open(self.json_path, "w") as f:
            json.dump(out, f, indent=2)
        return out


def _get_aws_env(access_key: str | None, secret_key: str | None, aws_profile: str | None) -> dict:
    env = os.environ.copy()
    env["AWS_EC2_METADATA_DISABLED"] = "true"
    if aws_profile:
        env["AWS_PROFILE"] = aws_profile
        env.pop("AWS_ACCESS_KEY_ID", None)
        env.pop("AWS_SECRET_ACCESS_KEY", None)
    elif access_key and secret_key:
        env["AWS_ACCESS_KEY_ID"] = access_key
        env["AWS_SECRET_ACCESS_KEY"] = secret_key
    else:
        env.pop("AWS_PROFILE", None)
        env.pop("AWS_ACCESS_KEY_ID", None)
        env.pop("AWS_SECRET_ACCESS_KEY", None)
    return env


def aws_cp_upload(
    local: Path,
    bucket: str,
    key: str,
    endpoint: str,
    access_key: str | None,
    secret_key: str | None,
    aws_profile: str | None,
):
    env = _get_aws_env(access_key, secret_key, aws_profile)
    url = f"{bucket}/{key}" if bucket.startswith("s3://") else f"s3://{bucket}/{key}"
    cmd = ["aws", "s3", "cp", str(local), url, "--endpoint-url", endpoint]
    return subprocess.run(cmd, capture_output=True, text=True, env=env)


def aws_cp_download(
    bucket: str,
    key: str,
    endpoint: str,
    access_key: str | None,
    secret_key: str | None,
    aws_profile: str | None,
):
    env = _get_aws_env(access_key, secret_key, aws_profile)
    # Используем s3api get-object вместо s3 cp, чтобы избежать ошибки обновления времени модификации /dev/null
    devnull = "NUL" if os.name == "nt" else "/dev/null"
    # Извлекаем имя бакета без префикса s3://
    bucket_name = bucket.replace("s3://", "").split("/")[0]
    cmd = ["aws", "s3api", "get-object", "--bucket", bucket_name, "--key", key, devnull, "--endpoint-url", endpoint]
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    # Если есть ошибка обновления времени модификации, но данные загружены, считаем успехом
    if res.returncode != 0 and res.stderr:
        if "Successfully Downloaded" in res.stderr and "unable to update the last modified time" in res.stderr:
            # Данные загружены успешно, просто не удалось обновить время модификации
            # Создаём фиктивный успешный результат
            class FakeResult:
                returncode = 0
                stdout = res.stdout
                stderr = ""
            return FakeResult()
    return res


def retry_with_backoff(func, max_retries: int, backoff_base: float, *args, **kwargs):
    """Выполняет функцию с повторными попытками и экспоненциальным backoff."""
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            result = func(*args, **kwargs)
            if hasattr(result, 'returncode') and result.returncode == 0:
                return result, True, None
            elif attempt < max_retries:
                wait_time = backoff_base ** attempt
                time.sleep(wait_time)
                last_error = result.stderr[-200:] if hasattr(result, 'stderr') and result.stderr else "unknown"
            else:
                last_error = result.stderr[-200:] if hasattr(result, 'stderr') and result.stderr else "unknown"
                return result, False, last_error
        except Exception as e:
            if attempt < max_retries:
                wait_time = backoff_base ** attempt
                time.sleep(wait_time)
                last_error = str(e)
            else:
                last_error = str(e)
                return None, False, last_error
    return None, False, last_error or "max retries exceeded"


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
        jobs.sort(key=lambda j: j.size)
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
    profile = getattr(args, "profile", "write-heavy")
    mixed_read_ratio = getattr(args, "mixed_read_ratio", 0.7)
    pattern = getattr(args, "pattern", "sustained")
    burst_duration_sec = getattr(args, "burst_duration_sec", 10.0)
    burst_intensity_multiplier = getattr(args, "burst_intensity_multiplier", 10.0)
    max_retries = getattr(args, "max_retries", 3)
    retry_backoff_base = getattr(args, "retry_backoff_base", 2.0)
    
    # Для всех профилей сначала загружаем данные
    for job in jobs:
        q.put(("upload", job))
    metrics = Metrics(args.metrics, args.report)

    stop = threading.Event()
    active_lock = threading.Lock()
    active_uploads = 0
    active_downloads = 0
    active_jobs: dict[int, Job] = {}
    group_lock = threading.Lock()
    pending_lock = threading.Lock()
    # Словарь: ключ объекта -> endpoint, через который он был загружен
    uploaded_objects: dict[str, str] = {}
    uploaded_objects_lock = threading.Lock()
    upload_phase_done = threading.Event()
    
    # Состояние для паттернов
    burst_active = False
    burst_start_time = None
    pattern_lock = threading.Lock()

    def worker():
        nonlocal active_uploads, active_downloads
        while not stop.is_set():
            try:
                op, job = q.get(timeout=0.5)
            except queue.Empty:
                # Если upload фаза завершена и очередь пуста, завершаем worker
                if upload_phase_done.is_set() and q.empty():
                    break
                if not getattr(args, "infinite", False):
                    continue
                else:
                    continue
            start = time.time()
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
                res, ok, err = retry_with_backoff(
                    aws_cp_upload,
                    max_retries,
                    retry_backoff_base,
                    job.path,
                    args.bucket,
                    job.path.name,
                    endpoint,
                    getattr(args, "access_key", None),
                    getattr(args, "secret_key", None),
                    getattr(args, "aws_profile", None),
                )
                if not ok and res is None:
                    err = err or "retry failed"
                end = time.time()
                nbytes = job.size
                metrics.record("upload", start, end, nbytes, ok, err)
                if ok:
                    with group_lock:
                        grp = groups[job.group]
                        grp["done_files"] += 1
                        grp["done_bytes"] += nbytes
                    with uploaded_objects_lock:
                        uploaded_objects[job.path.name] = endpoint
                else:
                    with group_lock:
                        groups[job.group]["errors"] += 1
            elif op == "download":
                # Используем endpoint из job, если он был сохранён при записи, иначе выбираем новый
                endpoint = job.endpoint if job.endpoint else next_endpoint()
                # Используем retry с backoff
                res, ok, err = retry_with_backoff(
                    aws_cp_download,
                    max_retries,
                    retry_backoff_base,
                    args.bucket,
                    job.path.name,
                    endpoint,
                    getattr(args, "access_key", None),
                    getattr(args, "secret_key", None),
                    getattr(args, "aws_profile", None),
                )
                if not ok and res is None:
                    err = err or "retry failed"
                end = time.time()
                nbytes = job.size
                metrics.record("download", start, end, nbytes, ok, err)
            with active_lock:
                if op == "upload":
                    active_uploads -= 1
                elif op == "download":
                    active_downloads -= 1
                active_jobs.pop(threading.get_ident(), None)
            q.task_done()

    threads = []
    for _ in range(args.threads):
        t = threading.Thread(target=worker, daemon=True)
        t.start(); threads.append(t)

    last_print = 0
    first_frame = True
    download_phase_started = False
    mixed_phase_started = False
    key_to_job = {job.path.name: job for job in jobs}
    
    def start_read_phase():
        """Запускает фазу чтения для read-heavy или write-heavy профилей."""
        nonlocal download_phase_started
        with uploaded_objects_lock:
            if uploaded_objects:
                download_phase_started = True
                for key, endpoint in uploaded_objects.items():
                    if key in key_to_job:
                        job = key_to_job[key]
                        job.endpoint = endpoint  # Устанавливаем endpoint из словаря
                        try:
                            q.put(("download", job), block=False)
                        except queue.Full:
                            pass  # Очередь полна, попробуем позже
                upload_phase_done.set()
    
    def start_mixed_phase():
        """Запускает смешанную фазу для mixed профиля."""
        nonlocal mixed_phase_started
        with uploaded_objects_lock:
            if uploaded_objects:
                mixed_phase_started = True
                # Создаём список всех загруженных объектов для смешанных операций
                uploaded_list = list(uploaded_objects.items())
                random.shuffle(uploaded_list)
                for key, endpoint in uploaded_list:
                    if key in key_to_job:
                        job = key_to_job[key]
                        job.endpoint = endpoint
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
            if not download_phase_started and not mixed_phase_started:
                if upload_pending == 0 and upload_active == 0:
                    if profile == "write-heavy" or profile == "read-heavy":
                        start_read_phase()
                    elif profile == "mixed-70-30":
                        start_mixed_phase()
            
            # Для mixed профиля: добавляем новые задачи в смешанном режиме
            if mixed_phase_started and profile == "mixed-70-30":
                with uploaded_objects_lock:
                    if uploaded_objects and q.qsize() < (queue_limit or 1000):
                        # Добавляем новые задачи в зависимости от паттерна
                        intensity = burst_intensity_multiplier if burst_active else 1.0
                        tasks_to_add = int(args.threads * intensity) if burst_active else 1
                        uploaded_list = list(uploaded_objects.items())
                        random.shuffle(uploaded_list)
                        added = 0
                        for key, endpoint in uploaded_list:
                            if added >= tasks_to_add:
                                break
                            if key in key_to_job:
                                job = key_to_job[key]
                                job.endpoint = endpoint
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
            
            if now - last_print >= 2.0:
                rbps, wbps, ops_per_sec = metrics.current_rates(5.0)
                files_done = metrics.write_ops_ok
                files_read = metrics.read_ops_ok
                files_err = metrics.err_ops
                files_left = max(total_files - files_done - files_err, 0)
                avg_wbps = metrics.avg_write_rate()
                avg_rbps = metrics.avg_read_rate()
                with active_lock:
                    inflight = active_uploads + active_downloads
                    active_uploads_snap = active_uploads
                    active_downloads_snap = active_downloads
                    active_snapshot = list(active_jobs.values())
                with pending_lock:
                    pending = q.qsize()
                    pending_snapshot = pending_counts.copy()
                elapsed = metrics.elapsed()
                bytes_done = metrics.write_bytes
                bytes_read = metrics.read_bytes
                pct_files = (files_done / total_files * 100) if total_files else 0.0
                pct_bytes = (bytes_done / total_bytes * 100) if total_bytes else 0.0
                wbps_mb = wbps / 1024 / 1024
                rbps_mb = rbps / 1024 / 1024
                avg_wbps_mb = avg_wbps / 1024 / 1024
                avg_rbps_mb = avg_rbps / 1024 / 1024
                ops_per_sec = max(ops_per_sec, 0.0)
                
                # ETA: для фазы записи или чтения
                eta_sec = None
                if download_phase_started or mixed_phase_started:
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
                
                # Показываем последнюю операцию (upload или download)
                last_info = "n/a"
                if download_phase_started or mixed_phase_started:
                    # В mixed фазе показываем последнюю операцию (может быть и read, и write)
                    last_download = metrics.last_download
                    last_upload = metrics.last_upload
                    if last_download and last_upload:
                        # Показываем более свежую операцию
                        if last_download.get("ended", 0) > last_upload.get("ended", 0):
                            last_size_mb = last_download["bytes"] / 1024 / 1024
                            last_dur = last_download["lat_ms"] / 1000
                            last_info = f"R:{last_size_mb:.1f}MB/{last_dur:.2f}s"
                        else:
                            last_size_mb = last_upload["bytes"] / 1024 / 1024
                            last_dur = last_upload["lat_ms"] / 1000
                            last_info = f"W:{last_size_mb:.1f}MB/{last_dur:.2f}s"
                    elif last_download:
                        last_size_mb = last_download["bytes"] / 1024 / 1024
                        last_dur = last_download["lat_ms"] / 1000
                        last_info = f"R:{last_size_mb:.1f}MB/{last_dur:.2f}s"
                    elif last_upload:
                        last_size_mb = last_upload["bytes"] / 1024 / 1024
                        last_dur = last_upload["lat_ms"] / 1000
                        last_info = f"W:{last_size_mb:.1f}MB/{last_dur:.2f}s"
                else:
                    last_upload = metrics.last_upload
                    if last_upload:
                        last_size_mb = last_upload["bytes"] / 1024 / 1024
                        last_dur = last_upload["lat_ms"] / 1000
                        last_info = f"W:{last_size_mb:.1f}MB/{last_dur:.2f}s"
                median_up, p90_up, p95_up = metrics.latency_percentiles("upload")
                median_dn, p90_dn, p95_dn = metrics.latency_percentiles("download")
                lat_line = "n/a"
                if median_up is not None or median_dn is not None:
                    parts = []
                    if median_up is not None:
                        parts.append(f"W:p50={median_up/1000:.2f}s p90={p90_up/1000:.2f}s")
                    if median_dn is not None:
                        parts.append(f"R:p50={median_dn/1000:.2f}s p90={p90_dn/1000:.2f}s")
                    lat_line = " | ".join(parts)
                plain_lines: list[str] = []
                styled_lines: list[tuple[str, tuple[str, ...], bool]] = []
                pattern_info = f" [{pattern.upper()}]" if pattern == "bursty" and burst_active else ""
                header = f"S3Flood | {profile} | t={elapsed:6.1f}s | ETA {eta_str}{pattern_info}"
                plain_lines.append(header)
                styled_lines.append((header, (ANSI_BOLD, ANSI_CYAN), False))
                # Для фазы чтения или mixed: учитываем активные операции и pending
                if download_phase_started or mixed_phase_started:
                    with uploaded_objects_lock:
                        total_to_read = len(uploaded_objects)
                    files_in_progress_read = files_read + active_downloads_snap
                    read_pct = (files_in_progress_read / total_to_read * 100) if total_to_read > 0 else 0.0
                    if mixed_phase_started:
                        phase_info = " [MIXED]"
                    else:
                        phase_info = " [READ]"
                    # В фазе чтения/mixed: подсвечиваем данные чтения (R:) зелёным, W: без стилей
                    # Разделяем на части для цветового выделения
                    w_part = f"W:{files_done}/{total_files} ({pct_files:.1f}%)"
                    r_part = f"R:{files_read}/{total_to_read} ({read_pct:.1f}%)"
                    w_bytes_part = f"W:{format_bytes(bytes_done)}"
                    r_bytes_part = f"R:{format_bytes(bytes_read)}"
                    err_part = f"Err {files_err}"
                    files_line = f"Files {w_part} {r_part}{phase_info} | Bytes {w_bytes_part} {r_bytes_part} | {err_part}"
                    # Создаём стилизованную версию: подсвечиваем R: зелёным, W: без стилей, ошибки красным
                    files_line_styled = f"Files {w_part} {style(r_part, ANSI_BOLD, ANSI_GREEN)}{phase_info} | Bytes {w_bytes_part} {style(r_bytes_part, ANSI_BOLD, ANSI_GREEN)} | {style(err_part, ANSI_RED) if files_err > 0 else err_part}"
                    files_color = ()  # Без общего цвета строки
                else:
                    read_pct = 0.0
                    phase_info = " [WRITE]"
                    # В фазе записи: подсвечиваем данные записи (W:) зелёным, R: без стилей
                    w_part = f"W:{files_done}/{total_files} ({pct_files:.1f}%)"
                    r_part = f"R:{files_read}/{total_files} ({read_pct:.1f}%)"
                    w_bytes_part = f"W:{format_bytes(bytes_done)}"
                    r_bytes_part = f"R:{format_bytes(bytes_read)}"
                    err_part = f"Err {files_err}"
                    files_line = f"Files {w_part} {r_part}{phase_info} | Bytes {w_bytes_part} {r_bytes_part} | {err_part}"
                    # Создаём стилизованную версию: подсвечиваем W: зелёным, R: без стилей, ошибки красным
                    files_line_styled = f"Files {style(w_part, ANSI_BOLD, ANSI_GREEN)} {r_part}{phase_info} | Bytes {style(w_bytes_part, ANSI_BOLD, ANSI_GREEN)} {r_bytes_part} | {style(err_part, ANSI_RED) if files_err > 0 else err_part}"
                    files_color = ()  # Без общего цвета строки
                plain_lines.append(files_line)
                styled_lines.append((files_line_styled, files_color, False))
                load_line = f"Load active {inflight}/{args.threads} (U:{active_uploads_snap} D:{active_downloads_snap}) | queue {pending} | ops {ops_per_sec:.2f}/s"
                plain_lines.append(load_line)
                styled_lines.append((load_line, (ANSI_BLUE,), False))
                
                # Цветовое выделение для rates
                if download_phase_started or mixed_phase_started:
                    # В фазе чтения/mixed: подсвечиваем R: зелёным, W: без стилей (или оба, если mixed)
                    w_rates_part = f"W:cur {wbps_mb:6.1f} MB/s avg {avg_wbps_mb:6.1f} MB/s"
                    r_rates_part = f"R:cur {rbps_mb:6.1f} MB/s avg {avg_rbps_mb:6.1f} MB/s"
                    rate_line_plain = f"Rates {w_rates_part} | {r_rates_part} | last {last_info}"
                    if mixed_phase_started:
                        # В mixed фазе подсвечиваем оба
                        rate_line_styled = f"Rates {style(w_rates_part, ANSI_BOLD, ANSI_YELLOW)} | {style(r_rates_part, ANSI_BOLD, ANSI_GREEN)} | last {style(last_info, ANSI_BOLD, ANSI_CYAN)}"
                    else:
                        rate_line_styled = f"Rates {w_rates_part} | {style(r_rates_part, ANSI_BOLD, ANSI_GREEN)} | last {style(last_info, ANSI_BOLD, ANSI_GREEN)}"
                else:
                    # В фазе записи: подсвечиваем W: зелёным, R: без стилей
                    w_rates_part = f"W:cur {wbps_mb:6.1f} MB/s avg {avg_wbps_mb:6.1f} MB/s"
                    r_rates_part = f"R:cur {rbps_mb:6.1f} MB/s avg {avg_rbps_mb:6.1f} MB/s"
                    rate_line_plain = f"Rates {w_rates_part} | {r_rates_part} | last {last_info}"
                    rate_line_styled = f"Rates {style(w_rates_part, ANSI_BOLD, ANSI_GREEN)} | {r_rates_part} | last {style(last_info, ANSI_BOLD, ANSI_GREEN)}"
                plain_lines.append(rate_line_plain)
                styled_lines.append((rate_line_styled, (ANSI_BOLD, ANSI_MAGENTA), True))
                
                # Цветовое выделение для latency
                if lat_line != "n/a":
                    if download_phase_started or mixed_phase_started:
                        # В фазе чтения/mixed: подсвечиваем R: зелёным, W: без стилей (или оба, если mixed)
                        if "W:" in lat_line and "R:" in lat_line:
                            parts = lat_line.split(" | ")
                            w_lat_part = parts[0] if parts[0].startswith("W:") else ""
                            r_lat_part = parts[1] if len(parts) > 1 and parts[1].startswith("R:") else ""
                            if w_lat_part and r_lat_part:
                                if mixed_phase_started:
                                    latency_line_styled = f"Latency {style(w_lat_part, ANSI_BOLD, ANSI_YELLOW)} | {style(r_lat_part, ANSI_BOLD, ANSI_GREEN)}"
                                else:
                                    latency_line_styled = f"Latency {w_lat_part} | {style(r_lat_part, ANSI_BOLD, ANSI_GREEN)}"
                            elif r_lat_part:
                                latency_line_styled = f"Latency {style(r_lat_part, ANSI_BOLD, ANSI_GREEN)}"
                            else:
                                latency_line_styled = f"Latency {lat_line}"
                        elif "R:" in lat_line:
                            latency_line_styled = f"Latency {style(lat_line, ANSI_BOLD, ANSI_GREEN)}"
                        else:
                            latency_line_styled = f"Latency {lat_line}"
                    else:
                        # В фазе записи: подсвечиваем W: зелёным, R: без стилей
                        if "W:" in lat_line and "R:" in lat_line:
                            parts = lat_line.split(" | ")
                            w_lat_part = parts[0] if parts[0].startswith("W:") else ""
                            r_lat_part = parts[1] if len(parts) > 1 and parts[1].startswith("R:") else ""
                            if w_lat_part and r_lat_part:
                                latency_line_styled = f"Latency {style(w_lat_part, ANSI_BOLD, ANSI_GREEN)} | {r_lat_part}"
                            elif w_lat_part:
                                latency_line_styled = f"Latency {style(w_lat_part, ANSI_BOLD, ANSI_GREEN)}"
                            else:
                                latency_line_styled = f"Latency {lat_line}"
                        elif "W:" in lat_line:
                            latency_line_styled = f"Latency {style(lat_line, ANSI_BOLD, ANSI_GREEN)}"
                        else:
                            latency_line_styled = f"Latency {lat_line}"
                else:
                    latency_line_styled = f"Latency {lat_line}"
                plain_lines.append(f"Latency {lat_line}")
                styled_lines.append((latency_line_styled, (ANSI_DIM,), False))
                groups_title = "Groups:"
                plain_lines.append(groups_title)
                styled_lines.append((groups_title, (ANSI_BOLD,), False))
                with group_lock:
                    for group, info in groups.items():
                        done = info["done_files"]
                        total = info["total_files"]
                        done_pct = (done / total * 100) if total else 0.0
                        done_bytes = info["done_bytes"]
                        total_b = info["total_bytes"]
                        bytes_pct = (done_bytes / total_b * 100) if total_b else 0.0
                        errors = info["errors"]
                        inflight_group = sum(1 for job in active_snapshot if job.group == group)
                        pending_group = pending_snapshot.get(group, 0)
                        done_bytes_str = format_bytes(done_bytes).rjust(9)
                        total_bytes_str = format_bytes(total_b).ljust(9)
                        line = (
                            f"  {group:<8} | files {done:3d}/{total:<3d} ({done_pct:5.1f}%) | bytes {done_bytes_str}/{total_bytes_str} ({bytes_pct:5.1f}%) | inflight {inflight_group:2d} | pending {pending_group:3d} | err {errors}"
                        )
                        plain_lines.append(line)
                        if errors > 0:
                            styled_lines.append((line, (ANSI_RED,), False))
                        elif done_pct >= 99.9:
                            styled_lines.append((line, (ANSI_GREEN,), False))
                        else:
                            styled_lines.append((line, (ANSI_CYAN,), False))
                interior_width = max(visible_len(line) for line in plain_lines)
                border = "+" + "-" * (interior_width + 2) + "+"
                speed_border = "|" + style("=" * (interior_width + 2), ANSI_BOLD, ANSI_MAGENTA) + "|"
                render_lines = [border]
                for plain, codes, accent in styled_lines:
                    if accent:
                        render_lines.append(speed_border)
                    colored = style(plain, *codes)
                    padding = " " * (interior_width - visible_len(plain))
                    render_lines.append(f"| {colored}{padding} |")
                    if accent:
                        render_lines.append(speed_border)
                render_lines.append(border)
                table_height = len(render_lines)
                if first_frame:
                    first_frame = False
                else:
                    sys.stdout.write(f"\x1b[{table_height}A")
                for line in render_lines:
                    sys.stdout.write("\x1b[2K")
                    sys.stdout.write(line + "\n")
                sys.stdout.flush()
                last_print = now
    except KeyboardInterrupt:
        stop.set()

    for t in threads:
        t.join()

    summary = metrics.finalize()
    print("SUMMARY:", json.dumps(summary, indent=2))
