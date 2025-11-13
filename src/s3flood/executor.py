import json, time, queue, threading, subprocess, os, csv, statistics, sys, re
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

    def latency_percentiles(self):
        if not self.upload_latencies:
            return None, None, None
        data = sorted(self.upload_latencies)
        median = statistics.median(data)
        p90_idx = max(int(len(data) * 0.9) - 1, 0)
        p95_idx = max(int(len(data) * 0.95) - 1, 0)
        return median, data[p90_idx], data[p95_idx]

    def finalize(self):
        dur = max(time.time() - self._start, 1e-6)
        out = {
            "duration_sec": dur,
            "read_MBps_avg": self.read_bytes/1024/1024/dur,
            "write_MBps_avg": self.write_bytes/1024/1024/dur,
            "read_ok_ops": self.read_ops_ok,
            "write_ok_ops": self.write_ops_ok,
            "err_ops": self.err_ops,
        }
        with open(self.json_path, "w") as f:
            json.dump(out, f, indent=2)
        return out


def aws_cp_upload(
    local: Path,
    bucket: str,
    key: str,
    endpoint: str,
    access_key: str | None,
    secret_key: str | None,
    aws_profile: str | None,
):
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
    url = f"{bucket}/{key}" if bucket.startswith("s3://") else f"s3://{bucket}/{key}"
    cmd = ["aws", "s3", "cp", str(local), url, "--endpoint-url", endpoint]
    return subprocess.run(cmd, capture_output=True, text=True, env=env)


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

    q = queue.Queue()
    pending_counts = {g: info["total_files"] for g, info in groups.items()}
    for job in jobs:
        q.put(("upload", job))
    metrics = Metrics(args.metrics, args.report)

    stop = threading.Event()
    active_lock = threading.Lock()
    active_uploads = 0
    active_jobs: dict[int, Job] = {}
    group_lock = threading.Lock()
    pending_lock = threading.Lock()

    def worker():
        nonlocal active_uploads
        while not stop.is_set():
            try:
                op, job = q.get(timeout=0.5)
            except queue.Empty:
                if not args.infinite:
                    break
                else:
                    continue
            start = time.time()
            with active_lock:
                active_uploads += 1
                active_jobs[threading.get_ident()] = job
            with pending_lock:
                pending_counts[job.group] -= 1
            if op == "upload":
                res = aws_cp_upload(
                    job.path,
                    args.bucket,
                    job.path.name,
                    args.endpoint,
                    getattr(args, "access_key", None),
                    getattr(args, "secret_key", None),
                    getattr(args, "aws_profile", None),
                )
                ok = res.returncode == 0
                err = None if ok else (res.stderr[-200:] if res.stderr else "unknown")
                end = time.time()
                nbytes = job.size
                metrics.record("upload", start, end, nbytes, ok, err)
                if ok:
                    with group_lock:
                        grp = groups[job.group]
                        grp["done_files"] += 1
                        grp["done_bytes"] += nbytes
                else:
                    with group_lock:
                        groups[job.group]["errors"] += 1
            with active_lock:
                active_uploads -= 1
                active_jobs.pop(threading.get_ident(), None)
            q.task_done()

    threads = []
    for _ in range(args.threads):
        t = threading.Thread(target=worker, daemon=True)
        t.start(); threads.append(t)

    last_print = 0
    first_frame = True
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
            now = time.time()
            if now - last_print >= 2.0:
                rbps, wbps, ops_per_sec = metrics.current_rates(5.0)
                files_done = metrics.write_ops_ok
                files_err = metrics.err_ops
                files_left = max(total_files - files_done - files_err, 0)
                avg_wbps = metrics.avg_write_rate()
                avg_rbps = metrics.avg_read_rate()
                with active_lock:
                    inflight = active_uploads
                    active_snapshot = list(active_jobs.values())
                with pending_lock:
                    pending = q.qsize()
                    pending_snapshot = pending_counts.copy()
                elapsed = metrics.elapsed()
                bytes_done = metrics.write_bytes
                pct_files = (files_done / total_files * 100) if total_files else 0.0
                pct_bytes = (bytes_done / total_bytes * 100) if total_bytes else 0.0
                wbps_mb = wbps / 1024 / 1024
                rbps_mb = rbps / 1024 / 1024
                avg_wbps_mb = avg_wbps / 1024 / 1024
                avg_rbps_mb = avg_rbps / 1024 / 1024
                ops_per_sec = max(ops_per_sec, 0.0)
                eta_sec = None
                if wbps > 1 and bytes_done < total_bytes:
                    eta_sec = (total_bytes - bytes_done) / wbps
                eta_str = f"{eta_sec/60:.1f} min" if eta_sec and eta_sec > 60 else (f"{eta_sec:.0f} s" if eta_sec else "n/a")
                last_upload = metrics.last_upload
                if last_upload:
                    last_size_mb = last_upload["bytes"] / 1024 / 1024
                    last_dur = last_upload["lat_ms"] / 1000
                    last_info = f"{last_size_mb:.1f} MB in {last_dur:.2f}s"
                else:
                    last_info = "n/a"
                median, p90, p95 = metrics.latency_percentiles()
                lat_line = "n/a"
                if median is not None:
                    lat_line = f"p50={median/1000:.2f}s p90={p90/1000:.2f}s p95={p95/1000:.2f}s"
                plain_lines: list[str] = []
                styled_lines: list[tuple[str, tuple[str, ...], bool]] = []
                header = f"S3Flood | t={elapsed:6.1f}s | ETA {eta_str}"
                plain_lines.append(header)
                styled_lines.append((header, (ANSI_BOLD, ANSI_CYAN), False))
                files_line = f"Files {files_done}/{total_files} ({pct_files:.1f}%) | Bytes {format_bytes(bytes_done)} / {format_bytes(total_bytes)} ({pct_bytes:.1f}%) | Err {files_err}"
                files_color = (ANSI_RED,) if files_err > 0 else (ANSI_BOLD, ANSI_GREEN)
                plain_lines.append(files_line)
                styled_lines.append((files_line, files_color, False))
                load_line = f"Load active {inflight}/{args.threads} | queue {pending} | ops {ops_per_sec:.2f}/s"
                plain_lines.append(load_line)
                styled_lines.append((load_line, (ANSI_BLUE,), False))
                rate_line = f"Rates cur {wbps_mb:6.1f} MB/s avg {avg_wbps_mb:6.1f} MB/s | last {last_info}"
                plain_lines.append(rate_line)
                styled_lines.append((rate_line, (ANSI_BOLD, ANSI_MAGENTA), True))
                latency_line = f"Latency {lat_line}"
                plain_lines.append(latency_line)
                styled_lines.append((latency_line, (ANSI_DIM,), False))
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
