import json, time, queue, threading, subprocess, os, csv, statistics, sys, re, random, math, uuid
from pathlib import Path
from collections import deque
from dataclasses import dataclass

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
ANSI_DIM = "\x1b[2m" if USE_COLORS else ""
ANSI_RED = "\x1b[31m" if USE_COLORS else ""
ANSI_GREEN = "\x1b[32m" if USE_COLORS else ""
ANSI_YELLOW = "\x1b[33m" if USE_COLORS else ""
ANSI_BLUE = "\x1b[34m" if USE_COLORS else ""
ANSI_MAGENTA = "\x1b[35m" if USE_COLORS else ""
ANSI_CYAN = "\x1b[36m" if USE_COLORS else ""

ANSI_REGEX = re.compile(r"\x1b\[[0-9;]*m")

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


def visible_len(s: str) -> int:
    return len(ANSI_REGEX.sub("", s))


def truncate_filename(filename: str, max_len: int) -> str:
    """Обрезает имя файла посередине, если оно слишком длинное."""
    if len(filename) <= max_len:
        return filename
    if max_len < 5:
        return filename[:max_len]
    # Оставляем начало и конец, вставляем ...
    start_len = (max_len - 3) // 2
    end_len = max_len - 3 - start_len
    return filename[:start_len] + "..." + filename[-end_len:]


def style(text: str, *codes: str) -> str:
    if not USE_COLORS:
        return text
    prefix = "".join(codes)
    return f"{prefix}{text}{ANSI_RESET}" if codes else text


def shorten_middle(text: str, max_len: int) -> str:
    """Обрезает строку, вставляя ... посередине, если она не помещается."""
    if text is None:
        return ""
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    keep = max_len - 3
    head = keep // 2
    tail = keep - head
    return f"{text[:head]}...{text[-tail:]}"


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
        self.recent_ops = deque(maxlen=30)  # Буфер последних операций для дашборда
        self._active_recent_ops: dict[int, dict] = {}
        self._op_counter = 0
        with open(self.csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["ts_start","ts_end","op","bytes","status","latency_ms","error"])
            w.writeheader()

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

    def record(self, op: str, start: float, end: float, nbytes: int, ok: bool, err: str|None, filename: str|None = None, recent_id: int | None = None):
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
    
    def get_recent_ops(self, count=6):
        """Возвращает последние операции для отображения в дашборде."""
        with self._lock:
            return list(self.recent_ops)[-count:]

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
        
        # Сортируем по размеру
        sorted_by_size = sorted(file_stats.items(), key=lambda x: x[0])
        
        # ТОП10 маленьких - берем первые 10 самых маленьких
        # ТОП10 больших - берем последние 10 самых больших
        # Если уникальных размеров меньше 10, разделяем пополам
        total_unique_sizes = len(sorted_by_size)
        if total_unique_sizes <= 1:
            # Если только один размер, оба списка одинаковые
            top10_small = sorted_by_size
            top10_large = sorted_by_size
        elif total_unique_sizes <= 10:
            # Если размеров <= 10, разделяем пополам (маленькие и большие)
            mid = total_unique_sizes // 2
            top10_small = sorted_by_size[:mid] if mid > 0 else sorted_by_size[:1]
            top10_large = sorted_by_size[mid:] if mid < total_unique_sizes else sorted_by_size[-1:]
        else:
            # Если размеров > 10, берем первые 10 и последние 10
            top10_small = sorted_by_size[:10]
            top10_large = sorted_by_size[-10:]
        
        small_stats = []
        for size_bytes, stats in top10_small:
            speeds = stats["speeds"]
            if speeds:
                avg_speed = statistics.mean(speeds)
                median_speed = statistics.median(speeds)
                min_speed = min(speeds)
                max_speed = max(speeds)
                sorted_speeds = sorted(speeds)
                p90_idx = max(int(len(sorted_speeds) * 0.9) - 1, 0)
                p95_idx = max(int(len(sorted_speeds) * 0.95) - 1, 0)
                p90_speed = sorted_speeds[p90_idx]
                p95_speed = sorted_speeds[p95_idx]
            else:
                avg_speed = median_speed = min_speed = max_speed = p90_speed = p95_speed = 0.0
            small_stats.append((size_bytes, stats["count"], avg_speed, median_speed, min_speed, max_speed, p90_speed, p95_speed))
        
        large_stats = []
        for size_bytes, stats in top10_large:
            speeds = stats["speeds"]
            if speeds:
                avg_speed = statistics.mean(speeds)
                median_speed = statistics.median(speeds)
                min_speed = min(speeds)
                max_speed = max(speeds)
                sorted_speeds = sorted(speeds)
                p90_idx = max(int(len(sorted_speeds) * 0.9) - 1, 0)
                p95_idx = max(int(len(sorted_speeds) * 0.95) - 1, 0)
                p90_speed = sorted_speeds[p90_idx]
                p95_speed = sorted_speeds[p95_idx]
            else:
                avg_speed = median_speed = min_speed = max_speed = p90_speed = p95_speed = 0.0
            large_stats.append((size_bytes, stats["count"], avg_speed, median_speed, min_speed, max_speed, p90_speed, p95_speed))
        
        # Средняя скорость по всем файлам
        all_speeds = []
        for stats in file_stats.values():
            all_speeds.extend(stats["speeds"])
        
        if all_speeds:
            avg_speed_all = statistics.mean(all_speeds)
            median_speed_all = statistics.median(all_speeds)
            min_speed_all = min(all_speeds)
            max_speed_all = max(all_speeds)
            sorted_all_speeds = sorted(all_speeds)
            p90_idx = max(int(len(sorted_all_speeds) * 0.9) - 1, 0)
            p95_idx = max(int(len(sorted_all_speeds) * 0.95) - 1, 0)
            p90_speed_all = sorted_all_speeds[p90_idx]
            p95_speed_all = sorted_all_speeds[p95_idx]
        else:
            avg_speed_all = median_speed_all = min_speed_all = max_speed_all = p90_speed_all = p95_speed_all = 0.0
        
        return small_stats, large_stats, (avg_speed_all, median_speed_all, min_speed_all, max_speed_all, p90_speed_all, p95_speed_all)

    def finalize(self):
        dur = max(time.time() - self._start, 1e-6)
        
        # Вычисляем реальное время выполнения для каждой фазы
        # Для write и read профилей - одна фаза (только запись или только чтение)
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
        
        # Получаем аналитику по файлам
        write_analysis = self.get_file_stats("upload")
        read_analysis = self.get_file_stats("download")
        
        out = {
            "duration_sec": dur,
            "write_bytes": self.write_bytes,
            "read_bytes": self.read_bytes,
            "write_duration_sec": write_duration,
            "read_duration_sec": read_duration,
            "read_MBps_avg": read_MBps_avg,
            "write_MBps_avg": write_MBps_avg,
            "read_ok_ops": self.read_ops_ok,
            "write_ok_ops": self.write_ops_ok,
            "err_ops": self.err_ops,
        }
        
        # Добавляем аналитику по файлам
        if write_analysis[0] or write_analysis[1]:  # Есть статистика по записи
            small_stats, large_stats, speed_stats = write_analysis
            avg_speed, median_speed, min_speed, max_speed, p90_speed, p95_speed = speed_stats
            write_file_analysis = {
                "top10_small": [
                    {
                        "size_bytes": s[0],
                        "count": s[1],
                        "avg_speed_mbps": s[2],
                        "median_speed_mbps": s[3],
                        "min_speed_mbps": s[4],
                        "max_speed_mbps": s[5],
                        "p90_speed_mbps": s[6],
                        "p95_speed_mbps": s[7]
                    }
                    for s in (small_stats or [])
                ],
                "top10_large": [
                    {
                        "size_bytes": s[0],
                        "count": s[1],
                        "avg_speed_mbps": s[2],
                        "median_speed_mbps": s[3],
                        "min_speed_mbps": s[4],
                        "max_speed_mbps": s[5],
                        "p90_speed_mbps": s[6],
                        "p95_speed_mbps": s[7]
                    }
                    for s in (large_stats or [])
                ],
                "overall": {
                    "avg_speed_mbps": avg_speed,
                    "median_speed_mbps": median_speed,
                    "min_speed_mbps": min_speed,
                    "max_speed_mbps": max_speed,
                    "p90_speed_mbps": p90_speed,
                    "p95_speed_mbps": p95_speed
                }
            }
            out["write_file_analysis"] = write_file_analysis
        
        if read_analysis[0] or read_analysis[1]:  # Есть статистика по чтению
            small_stats, large_stats, speed_stats = read_analysis
            avg_speed, median_speed, min_speed, max_speed, p90_speed, p95_speed = speed_stats
            read_file_analysis = {
                "top10_small": [
                    {
                        "size_bytes": s[0],
                        "count": s[1],
                        "avg_speed_mbps": s[2],
                        "median_speed_mbps": s[3],
                        "min_speed_mbps": s[4],
                        "max_speed_mbps": s[5],
                        "p90_speed_mbps": s[6],
                        "p95_speed_mbps": s[7]
                    }
                    for s in (small_stats or [])
                ],
                "top10_large": [
                    {
                        "size_bytes": s[0],
                        "count": s[1],
                        "avg_speed_mbps": s[2],
                        "median_speed_mbps": s[3],
                        "min_speed_mbps": s[4],
                        "max_speed_mbps": s[5],
                        "p90_speed_mbps": s[6],
                        "p95_speed_mbps": s[7]
                    }
                    for s in (large_stats or [])
                ],
                "overall": {
                    "avg_speed_mbps": avg_speed,
                    "median_speed_mbps": median_speed,
                    "min_speed_mbps": min_speed,
                    "max_speed_mbps": max_speed,
                    "p90_speed_mbps": p90_speed,
                    "p95_speed_mbps": p95_speed
                }
            }
            out["read_file_analysis"] = read_file_analysis
        
        with open(self.json_path, "w") as f:
            json.dump(out, f, indent=2)
        return out


def _cleanup_aws_profile_section(profile_name: str):
    """
    Удаляет все секции профиля из ~/.aws/config, чтобы избежать дубликатов.
    """
    config_path = os.path.expanduser("~/.aws/config")
    if not os.path.exists(config_path):
        return
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Удаляем все секции [profile profile_name] и все их параметры до следующей секции
        new_lines = []
        skip_section = False
        for line in lines:
            # Проверяем начало секции профиля
            if line.strip() == f"[profile {profile_name}]":
                skip_section = True
                continue
            
            # Если мы внутри секции профиля
            if skip_section:
                # Если встретили другую секцию (начинается с [), прекращаем пропуск
                if line.strip().startswith('['):
                    skip_section = False
                    new_lines.append(line)
                    continue
                # Пропускаем все строки до следующей секции (включая пустые и с отступами)
                # Параметры секции могут быть на разных уровнях вложенности
                continue
            
            new_lines.append(line)
        
        # Записываем обновленный конфиг
        with open(config_path, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
    except Exception:
        # Если не удалось очистить, продолжаем - aws configure set может перезаписать
        pass


def _configure_aws_profile_multipart(
    profile_name: str,
    multipart_threshold: int | None = None,
    multipart_chunksize: int | None = None,
    max_concurrent_requests: int | None = None,
):
    """
    Настраивает параметры multipart для указанного AWS CLI профиля.
    Использует aws configure set для динамического обновления настроек.
    Перед установкой очищает существующую секцию профиля, чтобы избежать дубликатов.
    """
    # Очищаем существующую секцию профиля перед созданием новой
    _cleanup_aws_profile_section(profile_name)
    
    # Устанавливаем параметры через aws configure set
    if multipart_threshold is not None:
        threshold_mb = int(multipart_threshold / (1024 * 1024))
        subprocess.run(
            ["aws", "configure", "set", "s3.multipart_threshold", f"{threshold_mb}MB", "--profile", profile_name],
            capture_output=True,
            timeout=5
        )
    if multipart_chunksize is not None:
        chunksize_mb = int(multipart_chunksize / (1024 * 1024))
        subprocess.run(
            ["aws", "configure", "set", "s3.multipart_chunksize", f"{chunksize_mb}MB", "--profile", profile_name],
            capture_output=True,
            timeout=5
        )
    if max_concurrent_requests is not None:
        subprocess.run(
            ["aws", "configure", "set", "s3.max_concurrent_requests", str(max_concurrent_requests), "--profile", profile_name],
            capture_output=True,
            timeout=5
        )


def _get_aws_env(
    access_key: str | None,
    secret_key: str | None,
    aws_profile: str | None,
    multipart_threshold: int | None = None,
    multipart_chunksize: int | None = None,
    max_concurrent_requests: int | None = None,
) -> tuple[dict, str | None]:
    """
    Возвращает (env, profile_name).
    profile_name - имя AWS CLI профиля с настроенными параметрами multipart (или None).
    """
    env = os.environ.copy()
    env["AWS_EC2_METADATA_DISABLED"] = "true"
    # Отключаем автоматические checksums для совместимости с S3-совместимыми бекендами
    # (начиная с boto3 1.36.0 checksums включены по умолчанию, что может вызывать BadDigest)
    env["AWS_S3_DISABLE_REQUEST_CHECKSUM"] = "true"
    
    # Определяем профиль для использования
    # Если нужно переопределить multipart настройки, используем отдельный профиль s3flood
    # и динамически обновляем его настройки
    profile_to_use = None
    if multipart_threshold is not None or multipart_chunksize is not None or max_concurrent_requests is not None:
        # Используем отдельный профиль s3flood для наших настроек
        profile_to_use = "s3flood"
        # Настраиваем параметры multipart для этого профиля
        _configure_aws_profile_multipart(
            profile_to_use,
            multipart_threshold,
            multipart_chunksize,
            max_concurrent_requests
        )
        # Если у пользователя есть aws_profile, копируем credentials из него
        if aws_profile:
            # Копируем credentials из исходного профиля в s3flood
            # (если они не заданы явно через access_key/secret_key)
            if not (access_key and secret_key):
                # Получаем credentials из исходного профиля
                try:
                    result = subprocess.run(
                        ["aws", "configure", "get", "aws_access_key_id", "--profile", aws_profile],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        access_key = result.stdout.strip()
                    result = subprocess.run(
                        ["aws", "configure", "get", "aws_secret_access_key", "--profile", aws_profile],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        secret_key = result.stdout.strip()
                except Exception:
                    pass
    elif aws_profile:
        # Если multipart настройки не заданы, используем исходный профиль
        profile_to_use = aws_profile
    
    if profile_to_use:
        env["AWS_PROFILE"] = profile_to_use
        env.pop("AWS_ACCESS_KEY_ID", None)
        env.pop("AWS_SECRET_ACCESS_KEY", None)
    elif access_key and secret_key:
        env["AWS_ACCESS_KEY_ID"] = access_key
        env["AWS_SECRET_ACCESS_KEY"] = secret_key
    else:
        env.pop("AWS_PROFILE", None)
        env.pop("AWS_ACCESS_KEY_ID", None)
        env.pop("AWS_SECRET_ACCESS_KEY", None)
    
    # Если заданы credentials явно и используется профиль s3flood, настраиваем их
    if profile_to_use == "s3flood" and access_key and secret_key:
        try:
            subprocess.run(
                ["aws", "configure", "set", "aws_access_key_id", access_key, "--profile", "s3flood"],
                capture_output=True,
                timeout=5
            )
            subprocess.run(
                ["aws", "configure", "set", "aws_secret_access_key", secret_key, "--profile", "s3flood"],
                capture_output=True,
                timeout=5
            )
        except Exception:
            pass
    
    return env, profile_to_use


def aws_cp_upload(
    local: Path,
    bucket: str,
    key: str,
    endpoint: str,
    access_key: str | None,
    secret_key: str | None,
    aws_profile: str | None,
    multipart_threshold: int | None = None,
    multipart_chunksize: int | None = None,
    max_concurrent_requests: int | None = None,
):
    """
    Загружает файл в S3 используя aws s3 cp.
    AWS CLI автоматически определяет, использовать ли multipart upload на основе
    multipart_threshold из переменной окружения AWS_CLI_FILE_TRANSFER_CONFIG.
    """
    env, profile_name = _get_aws_env(
        access_key, secret_key, aws_profile,
        multipart_threshold, multipart_chunksize, max_concurrent_requests
    )
    # Всегда используем aws s3 cp - он автоматически выберет multipart или обычный PUT
    # на основе multipart_threshold из настроек профиля
    url = f"{bucket}/{key}" if bucket.startswith("s3://") else f"s3://{bucket}/{key}"
    cmd = ["aws", "s3", "cp", str(local), url, "--endpoint-url", endpoint]
    if profile_name:
        cmd.extend(["--profile", profile_name])
    return subprocess.run(cmd, capture_output=True, text=True, env=env)


def aws_list_objects(
    bucket: str,
    endpoint: str,
    access_key: str | None,
    secret_key: str | None,
    aws_profile: str | None,
    multipart_threshold: int | None = None,
    multipart_chunksize: int | None = None,
    max_concurrent_requests: int | None = None,
):
    """Получает список объектов из бакета через s3api list-objects-v2."""
    env, profile_name = _get_aws_env(
        access_key, secret_key, aws_profile,
        multipart_threshold, multipart_chunksize, max_concurrent_requests
    )
    bucket_name = bucket.replace("s3://", "").split("/")[0]
    cmd = ["aws", "s3api", "list-objects-v2", "--bucket", bucket_name, "--endpoint-url", endpoint]
    if profile_name:
        cmd.extend(["--profile", profile_name])
    return subprocess.run(cmd, capture_output=True, text=True, env=env)
    if res.returncode != 0:
        return None
    try:
        data = json.loads(res.stdout)
        objects = []
        if "Contents" in data:
            for obj in data["Contents"]:
                objects.append({
                    "key": obj["Key"],
                    "size": obj.get("Size", 0),
                })
        return objects
    except (json.JSONDecodeError, KeyError):
        return None


def aws_check_bucket_access(
    bucket: str,
    endpoint: str,
    access_key: str | None,
    secret_key: str | None,
    aws_profile: str | None,
    multipart_threshold: int | None = None,
    multipart_chunksize: int | None = None,
    max_concurrent_requests: int | None = None,
):
    """Быстрая проверка доступа к бакету через s3api head-bucket."""
    env, profile_name = _get_aws_env(
        access_key, secret_key, aws_profile,
        multipart_threshold, multipart_chunksize, max_concurrent_requests
    )
    bucket_name = bucket.replace("s3://", "").split("/")[0]
    cmd = ["aws", "s3api", "head-bucket", "--bucket", bucket_name, "--endpoint-url", endpoint]
    if profile_name:
        cmd.extend(["--profile", profile_name])
    return subprocess.run(cmd, capture_output=True, text=True, env=env)


def aws_cp_download(
    bucket: str,
    key: str,
    endpoint: str,
    access_key: str | None,
    secret_key: str | None,
    aws_profile: str | None,
    multipart_threshold: int | None = None,
    multipart_chunksize: int | None = None,
    max_concurrent_requests: int | None = None,
):
    """
    Скачивает файл из S3 используя aws s3 cp.
    AWS CLI может использовать параллельные запросы (range requests) для больших файлов,
    что улучшает производительность. Параметр max_concurrent_requests из
    AWS_CLI_FILE_TRANSFER_CONFIG влияет на количество параллельных запросов.
    
    Примечание: multipart upload используется только для upload, не для download.
    Но aws s3 cp использует оптимизации для download через параллельные range requests.
    """
    env, profile_name = _get_aws_env(
        access_key, secret_key, aws_profile,
        multipart_threshold, multipart_chunksize, max_concurrent_requests
    )
    # Используем aws s3 cp для download - он может использовать параллельные запросы для больших файлов
    # max_concurrent_requests из настроек профиля влияет на количество параллельных запросов
    devnull = "NUL" if os.name == "nt" else "/dev/null"
    url = f"{bucket}/{key}" if bucket.startswith("s3://") else f"s3://{bucket}/{key}"
    cmd = ["aws", "s3", "cp", url, devnull, "--endpoint-url", endpoint]
    if profile_name:
        cmd.extend(["--profile", profile_name])
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    # Если команда успешна (returncode == 0), возвращаем результат как есть
    if res.returncode == 0:
        return res
    # Если есть ошибка обновления времени модификации /dev/null, но данные загружены, считаем успехом
    if res.returncode != 0 and res.stderr:
        # aws s3 cp может выдать ошибку при попытке обновить время модификации /dev/null
        # но данные уже загружены, так что это не критично
        if ("download" in res.stderr.lower() or "successfully" in res.stderr.lower()) and \
           ("unable to update" in res.stderr.lower() or "last modified" in res.stderr.lower() or "dev/null" in res.stderr.lower()):
            # Данные загружены успешно, просто не удалось обновить время модификации
            # Создаём фиктивный успешный результат
            class FakeResult:
                returncode = 0
                stdout = res.stdout
                stderr = ""
                args = res.args
            return FakeResult()
    # Для всех остальных ошибок возвращаем исходный результат
    return res


def retry_with_backoff(func, max_retries: int, backoff_base: float, *args, **kwargs):
    """Выполняет функцию с повторными попытками и экспоненциальным backoff."""
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            result = func(*args, **kwargs)
            # Проверяем успешность операции
            if result is None:
                last_error = "function returned None"
                if attempt < max_retries:
                    wait_time = backoff_base ** attempt
                    time.sleep(wait_time)
                    continue
                else:
                    return None, False, last_error
            # Проверяем наличие атрибута returncode
            if not hasattr(result, 'returncode'):
                last_error = f"result has no returncode attribute, type: {type(result)}"
                if attempt < max_retries:
                    wait_time = backoff_base ** attempt
                    time.sleep(wait_time)
                    continue
                else:
                    return result, False, last_error
            # Если returncode == 0, операция успешна
            if result.returncode == 0:
                return result, True, None
            # Если returncode != 0, это ошибка
            elif attempt < max_retries:
                wait_time = backoff_base ** attempt
                time.sleep(wait_time)
                last_error = result.stderr[-200:] if hasattr(result, 'stderr') and result.stderr else f"exit code {result.returncode}"
            else:
                last_error = result.stderr[-200:] if hasattr(result, 'stderr') and result.stderr else f"exit code {result.returncode}"
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
    metrics = Metrics(args.metrics, args.report)

    stop = threading.Event()
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
                res, ok, err = retry_with_backoff(
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
                )
                if not ok and res is None:
                    err = err or "retry failed"
                end = time.time()
                nbytes = job.size
                filename = job.path.name
                metrics.record("upload", start, end, nbytes, ok, err, display_name, recent_op_id)
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
                res, ok, err = retry_with_backoff(
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
                )
                end = time.time()
                # Детальная диагностика для отладки
                if not ok:
                    debug_info = []
                    if res is None:
                        debug_info.append("res=None")
                        err = err or "retry failed: no result"
                    else:
                        debug_info.append(f"res_type={type(res)}")
                        if hasattr(res, 'returncode'):
                            debug_info.append(f"returncode={res.returncode}")
                            if res.returncode != 0:
                                err_parts = [f"exit_code={res.returncode}"]
                                if hasattr(res, 'stderr') and res.stderr:
                                    stderr_snippet = res.stderr[-300:] if len(res.stderr) > 300 else res.stderr
                                    err_parts.append(f"stderr={stderr_snippet}")
                                    debug_info.append(f"stderr_len={len(res.stderr)}")
                                if hasattr(res, 'stdout') and res.stdout:
                                    stdout_snippet = res.stdout[-200:] if len(res.stdout) > 200 else res.stdout
                                    err_parts.append(f"stdout={stdout_snippet}")
                                    debug_info.append(f"stdout_len={len(res.stdout)}")
                                err = err or ("; ".join(err_parts) if err_parts else "unknown error")
                            else:
                                # returncode == 0, но ok == False - это странно, логируем
                                debug_info.append("WARNING: returncode=0 but ok=False")
                                err = err or f"unexpected: returncode=0 but marked as failed; debug: {'; '.join(debug_info)}"
                        else:
                            debug_info.append("no returncode attribute")
                            err = err or f"unexpected result type: {type(res)}; debug: {'; '.join(debug_info)}"
                # Для download используем размер из job.size (известен из списка объектов)
                # aws s3 cp не возвращает JSON с размером, в отличие от s3api get-object
                nbytes = job.size
                # Определяем имя файла для отображения
                filename = key
                metrics.record("download", start, end, nbytes, ok, err, filename, recent_op_id)
            with active_lock:
                if op == "upload":
                    active_uploads -= 1
                elif op == "download":
                    active_downloads -= 1
                active_jobs.pop(threading.get_ident(), None)
            q.task_done()

    threads = []
    threads_lock = threading.Lock()
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
    first_frame = True
    download_phase_started = False
    mixed_phase_started = False
    # Для read профиля используем key из path, для других - path.name
    if profile == "read":
        key_to_job = {str(job.path): job for job in jobs}  # path содержит key объекта
    else:
        key_to_job = {job.path.name: job for job in jobs}
    
    def start_read_phase():
        """Запускает фазу чтения для старых профилей (больше не используется)."""
        pass  # Удалено, так как read профиль работает напрямую
    
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
                rbps, wbps, ops_per_sec = metrics.current_rates(5.0)
                files_done = metrics.write_ops_ok
                files_read = metrics.read_ops_ok
                files_err = metrics.err_ops
                # Для бесконечного режима показываем общее количество обработанных файлов
                # Для обычного режима показываем оставшиеся файлы
                if getattr(args, "infinite", False):
                    # В бесконечном режиме показываем общее количество обработанных файлов
                    if profile == "read":
                        files_left = 0  # В бесконечном режиме не считаем оставшиеся
                    else:
                        files_left = 0  # В бесконечном режиме не считаем оставшиеся
                else:
                    # Для read профиля считаем оставшиеся файлы по чтению, для других - по записи
                    if profile == "read":
                        files_left = max(total_files - files_read - files_err, 0)
                    else:
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
                
                plain_lines: list[str] = []
                styled_lines: list[tuple[str, tuple[str, ...], bool]] = []
                pattern_info = f" [{pattern.upper()}]" if pattern == "bursty" and burst_active else ""
                with cycle_lock:
                    cycle_info = f" | cycle {cycle_count}" if getattr(args, "infinite", False) else ""
                # Спиннер крутится всегда, пока программа работает (пока есть потоки или задачи)
                spinner = get_spinner()  # Всегда крутится, пока программа работает (уже с цветом и пробелом)
                header_text = f"S3Flood | {profile} | t={elapsed:6.1f}s | ETA {eta_str}{pattern_info}{cycle_info}"
                header_plain = f"{spinner}{header_text}"  # Пробел уже в спиннере
                header_styled = f"{spinner}{style(header_text, ANSI_BOLD, ANSI_CYAN)}"  # Пробел уже в спиннере
                plain_lines.append(header_plain)
                styled_lines.append((header_styled, (), False))  # Используем стилизованную версию, спиннер уже имеет цвет
                # Для фазы чтения или mixed: учитываем активные операции и pending
                if profile == "read":
                    # Для read профиля показываем только чтение
                    phase_info = " [READ]"
                    if getattr(args, "infinite", False):
                        # В бесконечном режиме показываем общее количество прочитанных объектов
                        r_part = f"R:{files_read} objects read"
                        r_bytes_part = f"R:{format_bytes(bytes_read)}"
                        err_part = f"Err {files_err}"
                        files_line = f"Files {r_part}{phase_info} | Bytes {r_bytes_part} | {err_part} | Total in bucket: {total_files}"
                        files_line_styled = f"Files {style(r_part, ANSI_BOLD, ANSI_GREEN)}{phase_info} | Bytes {style(r_bytes_part, ANSI_BOLD, ANSI_GREEN)} | {style(err_part, ANSI_RED) if files_err > 0 else err_part} | Total in bucket: {total_files}"
                    else:
                        files_in_progress_read = files_read + active_downloads_snap
                        read_pct = (files_in_progress_read / total_files * 100) if total_files > 0 else 0.0
                        r_part = f"R:{files_read}/{total_files} ({read_pct:.1f}%)"
                        r_bytes_part = f"R:{format_bytes(bytes_read)}"
                        err_part = f"Err {files_err}"
                        files_line = f"Files {r_part}{phase_info} | Bytes {r_bytes_part} | {err_part}"
                        files_line_styled = f"Files {style(r_part, ANSI_BOLD, ANSI_GREEN)}{phase_info} | Bytes {style(r_bytes_part, ANSI_BOLD, ANSI_GREEN)} | {style(err_part, ANSI_RED) if files_err > 0 else err_part}"
                    files_color = ()  # Без общего цвета строки
                elif download_phase_started or mixed_phase_started:
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
                    if getattr(args, "infinite", False):
                        # В бесконечном режиме показываем файлы в текущем цикле и общее количество
                        with cycle_files_lock:
                            current_cycle_files = files_in_current_cycle
                        w_part = f"W:{current_cycle_files}/{total_files} (cycle) | total: {files_done}"
                        r_part = f"R:{files_read}"
                        w_bytes_part = f"W:{format_bytes(bytes_done)}"
                        r_bytes_part = f"R:{format_bytes(bytes_read)}"
                        err_part = f"Err {files_err}"
                        files_line = f"Files {w_part} {r_part}{phase_info} | Bytes {w_bytes_part} {r_bytes_part} | {err_part}"
                        files_line_styled = f"Files {style(w_part, ANSI_BOLD, ANSI_GREEN)} {r_part}{phase_info} | Bytes {style(w_bytes_part, ANSI_BOLD, ANSI_GREEN)} {r_bytes_part} | {style(err_part, ANSI_RED) if files_err > 0 else err_part}"
                    else:
                        w_part = f"W:{files_done}/{total_files} ({pct_files:.1f}%)"
                    r_part = f"R:{files_read}/{total_files} ({read_pct:.1f}%)"
                    w_bytes_part = f"W:{format_bytes(bytes_done)}"
                    r_bytes_part = f"R:{format_bytes(bytes_read)}"
                    err_part = f"Err {files_err}"
                    files_line = f"Files {w_part} {r_part}{phase_info} | Bytes {w_bytes_part} {r_bytes_part} | {err_part}"
                    files_line_styled = f"Files {style(w_part, ANSI_BOLD, ANSI_GREEN)} {r_part}{phase_info} | Bytes {style(w_bytes_part, ANSI_BOLD, ANSI_GREEN)} {r_bytes_part} | {style(err_part, ANSI_RED) if files_err > 0 else err_part}"
                    files_color = ()  # Без общего цвета строки
                plain_lines.append(files_line)
                styled_lines.append((files_line_styled, files_color, False))
                
                # Для BURSTY режима показываем эффективное количество потоков
                effective_threads = args.threads
                if pattern == "bursty" and burst_active:
                    effective_threads = int(args.threads * burst_intensity_multiplier)
                
                load_line = f"Load active {inflight}/{effective_threads} (U:{active_uploads_snap} D:{active_downloads_snap}) | queue {pending} tasks | ops {ops_per_sec:.2f} files/s"
                plain_lines.append(load_line)
                styled_lines.append((load_line, (ANSI_BLUE,), False))
                
                # Цветовое выделение для rates
                if profile == "read":
                    # Для read профиля показываем только чтение
                    r_rates_part = f"R:cur {rbps_mb:6.1f} MB/s avg {avg_rbps_mb:6.1f} MB/s"
                    rate_line_plain = f"Rates {r_rates_part}"
                    rate_line_styled = f"Rates {style(r_rates_part, ANSI_BOLD, ANSI_GREEN)}"
                elif download_phase_started or mixed_phase_started:
                    # В фазе чтения/mixed: подсвечиваем R: зелёным, W: без стилей (или оба, если mixed)
                    w_rates_part = f"W:cur {wbps_mb:6.1f} MB/s avg {avg_wbps_mb:6.1f} MB/s"
                    r_rates_part = f"R:cur {rbps_mb:6.1f} MB/s avg {avg_rbps_mb:6.1f} MB/s"
                    rate_line_plain = f"Rates {w_rates_part} | {r_rates_part}"
                    if mixed_phase_started:
                        # В mixed фазе подсвечиваем оба
                        rate_line_styled = f"Rates {style(w_rates_part, ANSI_BOLD, ANSI_YELLOW)} | {style(r_rates_part, ANSI_BOLD, ANSI_GREEN)}"
                    else:
                        rate_line_styled = f"Rates {w_rates_part} | {style(r_rates_part, ANSI_BOLD, ANSI_GREEN)}"
                else:
                    # В фазе записи: подсвечиваем W: зелёным, R: без стилей
                    w_rates_part = f"W:cur {wbps_mb:6.1f} MB/s avg {avg_wbps_mb:6.1f} MB/s"
                    r_rates_part = f"R:cur {rbps_mb:6.1f} MB/s avg {avg_rbps_mb:6.1f} MB/s"
                    rate_line_plain = f"Rates {w_rates_part} | {r_rates_part}"
                    rate_line_styled = f"Rates {style(w_rates_part, ANSI_BOLD, ANSI_GREEN)} | {r_rates_part}"
                plain_lines.append(rate_line_plain)
                styled_lines.append((rate_line_styled, (ANSI_BOLD, ANSI_CYAN), True))

                # История последних операций
                recent_per_type = max(1, min(getattr(args, "threads", 1), 15))
                history_depth = max(recent_per_type * 4, recent_per_type * 2)
                recent_ops_snapshot = metrics.get_recent_ops(history_depth)
                done_ops = [entry for entry in recent_ops_snapshot if entry.get("done")]
                active_ops = [entry for entry in recent_ops_snapshot if not entry.get("done")]
                done_section = done_ops[-recent_per_type:]
                active_section = active_ops[-recent_per_type:]
                display_ops = done_section + active_section
                if display_ops:
                    recent_header = "Recent ops (latest bottom):"
                    plain_lines.append(recent_header)
                    styled_lines.append((recent_header, (ANSI_BOLD, ANSI_BLUE), False))
                    for entry in display_ops:
                        icon = WRITE_ICON if entry["op"] == "upload" else READ_ICON
                        filename_disp = shorten_middle(entry["filename"], 32)
                        size_bytes = entry.get("bytes") or 0
                        size_gb = size_bytes / (1024 ** 3)
                        size_disp = f"{size_gb:6.2f} GB"
                        if entry.get("done"):
                            latency_ms = entry.get("latency_ms") or 0
                            latency_s = latency_ms / 1000
                            time_disp = f"{latency_s:7.2f} s"
                            speed_val = entry.get("speed_mbps")
                            if speed_val is not None:
                                speed_disp = f"{speed_val:7.1f} MB/s"
                            else:
                                speed_disp = f"{'--':>7} MB/s"
                        else:
                            elapsed_s = max(now - entry.get("started", now), 0.0)
                            time_disp = f"{elapsed_s:7.2f} s"
                            speed_disp = f"{'--':>7} MB/s"
                        line_plain = f"  {icon} {filename_disp:<32} {size_disp} {time_disp} {speed_disp}"
                        color = (ANSI_BOLD, ANSI_GREEN) if entry["op"] == "upload" else (ANSI_BOLD, ANSI_CYAN)
                        plain_lines.append(line_plain)
                        styled_lines.append((line_plain, color, False))
                
                content_width = max(visible_len(line) for line in plain_lines) if plain_lines else 0
                border = "+" + "-" * (content_width + 2) + "+"
                speed_border = "|" + style("=" * (content_width + 2), ANSI_BOLD, ANSI_CYAN) + "|"
                render_lines = [border]
                
                for plain, codes, accent in styled_lines:
                    if accent:
                        render_lines.append(speed_border)
                    colored = style(plain, *codes) if codes else plain
                    padding = " " * (content_width - visible_len(plain))
                    render_lines.append(f"| {colored}{padding} |")
                    if accent:
                        render_lines.append(speed_border)
                
                render_lines.append(border)
                table_height = len(render_lines)
                if first_frame:
                    first_frame = False
                else:
                    if USE_COLORS:
                        sys.stdout.write(f"\x1b[{table_height}A")
                    else:
                        # На Windows без поддержки ANSI просто выводим разделитель
                        sys.stdout.write("\n" + "=" * 100 + "\n")
                for line in render_lines:
                    if USE_COLORS:
                        sys.stdout.write("\x1b[2K")
                    # После очистки строки выводим актуальное содержимое и перенос
                    sys.stdout.write(line + "\n")
                sys.stdout.flush()
                last_print = now
    except KeyboardInterrupt:
        stop.set()

    for t in threads:
        t.join()

    summary = metrics.finalize()
    print("SUMMARY:", json.dumps(summary, indent=2))
