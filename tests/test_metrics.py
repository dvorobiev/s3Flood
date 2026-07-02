import csv
import threading
import time

import pytest

from s3flood.metrics import (
    MetricsCsvWriter,
    RateWindow,
    percentile,
    summarize_latencies,
    summarize_speeds,
)


class TestPercentile:
    def test_p90_not_below_median(self):
        # регрессия: в старом коде p90 для двух значений возвращал минимум
        values = [0.083, 0.789]
        assert percentile(values, 90) >= percentile(values, 50)

    def test_interpolation_on_uniform_data(self):
        values = list(range(1, 11))  # 1..10
        assert percentile(values, 50) == pytest.approx(5.5)
        assert percentile(values, 90) == pytest.approx(9.1)

    def test_single_value(self):
        assert percentile([42.0], 99) == 42.0

    def test_empty_returns_zero(self):
        assert percentile([], 90) == 0.0

    def test_monotone(self):
        values = [5.0, 1.0, 3.0, 2.0, 4.0]
        ps = [percentile(values, p) for p in (50, 90, 95, 99)]
        assert ps == sorted(ps)
        assert ps[-1] <= max(values)
        assert ps[0] >= min(values)


class TestSummaries:
    def test_summarize_speeds_keys_and_order(self):
        speeds = [0.083, 0.789]
        s = summarize_speeds(speeds)
        assert s["min_speed_mbps"] == pytest.approx(0.083)
        assert s["max_speed_mbps"] == pytest.approx(0.789)
        assert s["p90_speed_mbps"] >= s["median_speed_mbps"]
        assert s["p95_speed_mbps"] >= s["p90_speed_mbps"]

    def test_summarize_latencies(self):
        lats = [10, 20, 30, 40, 50, 60, 70, 80, 90, 1000]
        s = summarize_latencies(lats)
        assert s["p50_ms"] <= s["p90_ms"] <= s["p95_ms"] <= s["p99_ms"]
        assert s["p99_ms"] <= 1000
        assert s["count"] == 10

    def test_summarize_latencies_empty(self):
        assert summarize_latencies([]) is None


class TestRateWindow:
    def test_no_loss_above_400_ops(self):
        # регрессия: deque(maxlen=400) терял операции и занижал RPS
        w = RateWindow()
        now = time.time()
        for i in range(1000):
            w.add(ts=now - 0.001 * i, op="upload", nbytes=100, ok=True)
        rb, wb, wrps, rrps = w.rates(window_sec=5.0, now=now)
        assert wrps == pytest.approx(1000 / 5.0)
        assert wb == pytest.approx(1000 * 100 / 5.0)

    def test_old_ops_expire(self):
        w = RateWindow()
        now = time.time()
        w.add(ts=now - 100.0, op="upload", nbytes=100, ok=True)
        w.add(ts=now - 1.0, op="download", nbytes=200, ok=True)
        rb, wb, wrps, rrps = w.rates(window_sec=5.0, now=now)
        assert wrps == 0.0
        assert rrps == pytest.approx(1 / 5.0)
        assert rb == pytest.approx(200 / 5.0)

    def test_failed_ops_not_counted(self):
        w = RateWindow()
        now = time.time()
        w.add(ts=now, op="upload", nbytes=100, ok=False)
        _, wb, wrps, _ = w.rates(window_sec=5.0, now=now)
        assert wrps == 0.0 and wb == 0.0


class TestMetricsCsvWriter:
    FIELDS = [
        "ts_start", "ts_end", "op", "bytes", "status",
        "latency_ms", "error", "endpoint", "thread_id", "attempt", "size_group",
    ]

    def test_writes_header_and_rows(self, tmp_path):
        path = tmp_path / "metrics.csv"
        writer = MetricsCsvWriter(str(path))
        writer.write_row(
            ts_start=1.0, ts_end=2.0, op="upload", nbytes=100, ok=True,
            latency_ms=1000, error=None, endpoint="http://e1", thread_id=3,
            attempt=1, size_group="small",
        )
        writer.close()
        with open(path) as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 1
        assert list(rows[0].keys()) == self.FIELDS
        assert rows[0]["status"] == "ok"
        assert rows[0]["endpoint"] == "http://e1"
        assert rows[0]["size_group"] == "small"

    def test_concurrent_writes_all_arrive(self, tmp_path):
        path = tmp_path / "metrics.csv"
        writer = MetricsCsvWriter(str(path))
        n_threads, per_thread = 8, 50

        def work(tid):
            for i in range(per_thread):
                writer.write_row(
                    ts_start=float(i), ts_end=float(i) + 1, op="upload",
                    nbytes=10, ok=(i % 2 == 0), latency_ms=5, error="boom",
                    endpoint="e", thread_id=tid, attempt=0, size_group="medium",
                )

        threads = [threading.Thread(target=work, args=(t,)) for t in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        writer.close()
        with open(path) as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == n_threads * per_thread

    def test_close_is_idempotent(self, tmp_path):
        writer = MetricsCsvWriter(str(tmp_path / "m.csv"))
        writer.close()
        writer.close()
