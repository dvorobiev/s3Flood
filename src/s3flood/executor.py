import json, time, queue, threading, subprocess, os, csv
from pathlib import Path
from datetime import datetime, timedelta
from collections import deque

# Minimal executor with AWS CLI runner only (v1)

class Metrics:
    def __init__(self, metrics_csv: str, report_json: str):
        self.csv_path = metrics_csv
        self.json_path = report_json
        self._lock = threading.Lock()
        self.ops = []  # per-op dicts
        self.window = deque(maxlen=200)
        self._start = time.time()
        self.read_bytes = 0
        self.write_bytes = 0
        self.read_ops_ok = 0
        self.write_ops_ok = 0
        self.err_ops = 0
        with open(self.csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["ts_start","ts_end","op","bytes","status","latency_ms","error"])
            w.writeheader()

    def record(self, op: str, start: float, end: float, nbytes: int, ok: bool, err: str|None):
        lat_ms = int((end-start)*1000)
        with self._lock:
            with open(self.csv_path, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=["ts_start","ts_end","op","bytes","status","latency_ms","error"])
                w.writerow({
                    "ts_start": start, "ts_end": end, "op": op, "bytes": nbytes,
                    "status": "ok" if ok else "err", "latency_ms": lat_ms, "error": err or ""
                })
            self.ops.append((op, start, end, nbytes, ok))
            self.window.append((end, op, nbytes, ok))
            if ok:
                if op == "download":
                    self.read_ops_ok += 1; self.read_bytes += nbytes
                elif op == "upload":
                    self.write_ops_ok += 1; self.write_bytes += nbytes
            else:
                self.err_ops += 1

    def current_rates(self, window_sec=5.0):
        now = time.time()
        rb = wb = 0
        for t, op, nbytes, ok in list(self.window):
            if now - t <= window_sec and ok:
                if op == "download": rb += nbytes
                elif op == "upload": wb += nbytes
        return rb/window_sec, wb/window_sec

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


def aws_cp_upload(local: Path, bucket: str, key: str, endpoint: str, ak: str, sk: str):
    env = os.environ.copy()
    env.update({"AWS_ACCESS_KEY_ID": ak, "AWS_SECRET_ACCESS_KEY": sk, "AWS_EC2_METADATA_DISABLED": "true"})
    url = f"{bucket}/{key}" if bucket.startswith("s3://") else f"s3://{bucket}/{key}"
    cmd = ["aws", "s3", "cp", str(local), url, "--endpoint-url", endpoint]
    return subprocess.run(cmd, capture_output=True, text=True, env=env)


def run_profile(args):
    data_dir = Path("./data") if not hasattr(args, "data_dir") else Path(args.data_dir)
    # Collect files according to 30/50/20 groups if present
    groups = [data_dir/"small", data_dir/"medium", data_dir/"large"]
    files = []
    for g in groups:
        if g.exists():
            files.extend([p for p in g.iterdir() if p.is_file()])
    if not files:
        print("No dataset files found under ./data. Run 's3flood dataset-create' first.")
        return

    q = queue.Queue()
    for p in files:
        q.put(("upload", p))
    metrics = Metrics(args.metrics, args.report)

    stop = threading.Event()

    def worker():
        while not stop.is_set():
            try:
                op, p = q.get(timeout=0.5)
            except queue.Empty:
                if not args.infinite:
                    break
                else:
                    continue
            key = p.name
            start = time.time()
            if op == "upload":
                res = aws_cp_upload(p, args.bucket, key, args.endpoint, args.access_key, args.secret_key)
                ok = res.returncode == 0
                err = None if ok else (res.stderr[-200:] if res.stderr else "unknown")
                end = time.time()
                nbytes = p.stat().st_size
                metrics.record("upload", start, end, nbytes, ok, err)
            q.task_done()

    threads = []
    for _ in range(args.threads):
        t = threading.Thread(target=worker, daemon=True)
        t.start(); threads.append(t)

    last_print = 0
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
            now = time.time()
            if now - last_print >= 2.0:
                rbps, wbps = metrics.current_rates(5.0)
                print(f"PROGRESS | succ_up={metrics.write_ops_ok} succ_dn={metrics.read_ops_ok} err={metrics.err_ops} | cur_write={wbps/1024/1024:.1f} MB/s cur_read={rbps/1024/1024:.1f} MB/s")
                last_print = now
    except KeyboardInterrupt:
        stop.set()

    for t in threads:
        t.join()

    summary = metrics.finalize()
    print("SUMMARY:", json.dumps(summary, indent=2))
