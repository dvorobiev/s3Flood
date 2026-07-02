"""Вычисление статистик и запись метрик s3flood.

Чистые функции перцентилей/сводок и вспомогательные классы:
RateWindow — скользящее окно для RPS без потери операций,
MetricsCsvWriter — буферизованная запись CSV в отдельном потоке,
чтобы дисковый I/O не сериализовал воркеров.
"""
from __future__ import annotations

import csv
import math
import queue
import re
import statistics
import threading
import time
from collections import deque

_AWS_ERROR_CODE_RE = re.compile(r"An error occurred \((\w+)\)")

CSV_FIELDS = [
    "ts_start", "ts_end", "op", "bytes", "status",
    "latency_ms", "error", "endpoint", "thread_id", "attempt", "size_group",
]


def percentile(values: list[float], p: int) -> float:
    """P-й перцентиль с линейной интерполяцией (method=inclusive)."""
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    cuts = statistics.quantiles(values, n=100, method="inclusive")
    return cuts[p - 1]


def summarize_speeds(speeds: list[float]) -> dict:
    """Сводка по скоростям (MB/s): avg/median/min/max/p90/p95."""
    if not speeds:
        return {
            "avg_speed_mbps": 0.0,
            "median_speed_mbps": 0.0,
            "min_speed_mbps": 0.0,
            "max_speed_mbps": 0.0,
            "p90_speed_mbps": 0.0,
            "p95_speed_mbps": 0.0,
        }
    return {
        "avg_speed_mbps": statistics.mean(speeds),
        "median_speed_mbps": statistics.median(speeds),
        "min_speed_mbps": min(speeds),
        "max_speed_mbps": max(speeds),
        "p90_speed_mbps": percentile(speeds, 90),
        "p95_speed_mbps": percentile(speeds, 95),
    }


def summarize_latencies(latencies_ms: list[float]) -> dict | None:
    """Сводка по латентности (мс): count/avg/min/max + p50/p90/p95/p99."""
    if not latencies_ms:
        return None
    return {
        "count": len(latencies_ms),
        "avg_ms": statistics.mean(latencies_ms),
        "min_ms": min(latencies_ms),
        "max_ms": max(latencies_ms),
        "p50_ms": percentile(latencies_ms, 50),
        "p90_ms": percentile(latencies_ms, 90),
        "p95_ms": percentile(latencies_ms, 95),
        "p99_ms": percentile(latencies_ms, 99),
    }


def classify_error(err: str | None) -> str:
    """Классифицирует текст ошибки в короткий тип для сводки отчёта."""
    if not err:
        return "unknown"
    match = _AWS_ERROR_CODE_RE.search(err)
    if match:
        return match.group(1)
    low = err.lower()
    if "timeout" in low or "timed out" in low:
        return "timeout"
    if "could not connect" in low or "connection" in low:
        return "connection"
    if "interrupted by user" in low:
        return "interrupted"
    return "other"


def build_timeline(ops, max_points: int = 300) -> list[dict]:
    """Строит посекундный таймлайн операций для графиков.

    ops — кортежи (op, start, end, nbytes, ok, lat_ms); операция относится
    к бакету по времени завершения. Длинные прогоны укрупняются так, чтобы
    точек было не больше max_points.
    """
    if not ops:
        return []
    t_first = min(op[1] for op in ops)
    t_last = max(op[2] for op in ops)
    span = max(t_last - t_first, 1e-6)
    step = max(1, math.ceil(span / max_points))
    buckets: dict[int, dict] = {}
    for op, start, end, nbytes, ok, _lat in ops:
        t_sec = int((end - t_first) // step) * step
        b = buckets.setdefault(t_sec, {
            "t_sec": t_sec,
            "write_ops": 0, "read_ops": 0, "err_ops": 0,
            "write_bytes": 0, "read_bytes": 0,
        })
        if not ok:
            b["err_ops"] += 1
        elif op == "upload":
            b["write_ops"] += 1
            b["write_bytes"] += nbytes
        elif op == "download":
            b["read_ops"] += 1
            b["read_bytes"] += nbytes
    return [buckets[k] for k in sorted(buckets)]


class RateWindow:
    """Скользящее окно операций для расчёта RPS/пропускной способности.

    Хранит операции за последние `retention_sec`, без ограничения по количеству
    (deque(maxlen=N) занижал RPS при высокой нагрузке).
    """

    def __init__(self, retention_sec: float = 60.0):
        self._retention = retention_sec
        self._lock = threading.Lock()
        self._ops: deque[tuple[float, str, int, bool]] = deque()

    def add(self, ts: float, op: str, nbytes: int, ok: bool) -> None:
        with self._lock:
            self._ops.append((ts, op, nbytes, ok))
            self._prune(time.time())

    def _prune(self, now: float) -> None:
        cutoff = now - self._retention
        while self._ops and self._ops[0][0] < cutoff:
            self._ops.popleft()

    def rates(self, window_sec: float = 5.0, now: float | None = None):
        """Возвращает (read_Bps, write_Bps, write_rps, read_rps) за окно."""
        if now is None:
            now = time.time()
        rb = wb = 0
        read_ops = write_ops = 0
        with self._lock:
            for ts, op, nbytes, ok in self._ops:
                if now - ts <= window_sec and ok:
                    if op == "download":
                        rb += nbytes
                        read_ops += 1
                    elif op == "upload":
                        wb += nbytes
                        write_ops += 1
        w = window_sec if window_sec > 0 else 1.0
        return rb / w, wb / w, write_ops / w, read_ops / w


class MetricsCsvWriter:
    """Пишет метрики в CSV одним фоновым потоком.

    Файл открывается один раз; воркеры кладут строки в очередь и не блокируются
    на дисковом I/O.
    """

    _SENTINEL = None

    def __init__(self, path: str):
        self._queue: queue.Queue = queue.Queue()
        self._file = open(path, "w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=CSV_FIELDS)
        self._writer.writeheader()
        self._closed = False
        self._thread = threading.Thread(target=self._drain, daemon=True, name="metrics-csv")
        self._thread.start()

    def write_row(
        self,
        *,
        ts_start: float,
        ts_end: float,
        op: str,
        nbytes: int,
        ok: bool,
        latency_ms: int,
        error: str | None = None,
        endpoint: str | None = None,
        thread_id: int | None = None,
        attempt: int | None = None,
        size_group: str | None = None,
    ) -> None:
        self._queue.put({
            "ts_start": ts_start,
            "ts_end": ts_end,
            "op": op,
            "bytes": nbytes,
            "status": "ok" if ok else "err",
            "latency_ms": latency_ms,
            "error": error or "",
            "endpoint": endpoint or "",
            "thread_id": "" if thread_id is None else thread_id,
            "attempt": "" if attempt is None else attempt,
            "size_group": size_group or "",
        })

    def _drain(self) -> None:
        while True:
            item = self._queue.get()
            if item is self._SENTINEL:
                break
            self._writer.writerow(item)
            # Дописываем всё, что уже накопилось, одним заходом и сбрасываем на диск
            while True:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                if item is self._SENTINEL:
                    self._file.flush()
                    return
                self._writer.writerow(item)
            self._file.flush()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._queue.put(self._SENTINEL)
        self._thread.join(timeout=10)
        self._file.close()
