import csv
import threading
import time

import pytest

from s3flood.metrics import (
    MetricsCsvWriter,
    RateWindow,
    analyze_operations,
    percentile,
    read_ops_csv,
    summarize_latencies,
    summarize_speeds,
)


def make_op(**over):
    base = {
        "ts_start": 1000.0, "ts_end": 1001.0, "op": "upload", "bytes": 1024**2,
        "status": "ok", "latency_ms": 1000.0, "error": "",
        "endpoint": "", "attempt": "", "size_group": "",
    }
    base.update(over)
    return base


class TestAnalyzeOperations:
    def test_basic_totals_and_latency(self):
        ops = [
            make_op(ts_start=1000, ts_end=1001, latency_ms=1000),
            make_op(ts_start=1001, ts_end=1003, latency_ms=2000),
            make_op(ts_start=1003, ts_end=1004, status="err",
                    error="An error occurred (SlowDown) when calling ..."),
        ]
        r = analyze_operations(ops)
        assert r["total"] == 3 and r["ok"] == 2 and r["err"] == 1
        assert r["ok_bytes"] == 2 * 1024**2
        assert r["duration_s"] == pytest.approx(4.0)
        assert r["latency"]["p50_ms"] == pytest.approx(1500)
        assert r["errors"] == {"SlowDown": 1}
        assert r["by_op"]["upload"]["count"] == 2

    def test_size_buckets_use_size_group_when_present(self):
        ops = [make_op(size_group="small"), make_op(size_group="large", bytes=10 * 1024**2)]
        r = analyze_operations(ops)
        labels = [b["label"] for b in r["size_buckets"]]
        assert "small" in labels and "large" in labels

    def test_size_buckets_auto_without_group(self):
        ops = [make_op(bytes=100 * 1024), make_op(bytes=2 * 1024**3, ts_end=1010)]
        r = analyze_operations(ops)
        labels = [b["label"] for b in r["size_buckets"]]
        assert "<1MB" in labels and "≥1GB" in labels

    def test_endpoint_breakdown(self):
        ops = [make_op(endpoint="http://n1"), make_op(endpoint="http://n2"),
               make_op(endpoint="http://n1")]
        r = analyze_operations(ops)
        assert r["by_endpoint"]["http://n1"]["count"] == 2
        assert r["by_endpoint"]["http://n2"]["count"] == 1

    def test_retries_counted(self):
        ops = [make_op(attempt="1"), make_op(attempt="3")]
        r = analyze_operations(ops)
        assert r["retries"]["ops_with_retries"] == 1
        assert r["retries"]["max_attempt"] == 3

    def test_rps_series_for_sparkline(self):
        ops = [make_op(ts_start=1000 + i, ts_end=1000.5 + i) for i in range(10)]
        r = analyze_operations(ops)
        assert sum(r["rps_series"]) == 10

    def test_empty(self):
        assert analyze_operations([]) is None


class TestReadOpsCsv:
    def test_reads_new_format(self, tmp_path):
        p = tmp_path / "m.csv"
        w = MetricsCsvWriter(str(p))
        w.write_row(ts_start=1.0, ts_end=2.0, op="upload", nbytes=100, ok=True,
                    latency_ms=1000, endpoint="http://e", thread_id=1, attempt=2,
                    size_group="small")
        w.close()
        ops = read_ops_csv(str(p))
        assert len(ops) == 1
        assert ops[0]["size_group"] == "small"
        assert ops[0]["bytes"] == 100

    def test_reads_old_format(self, tmp_path):
        p = tmp_path / "old.csv"
        p.write_text(
            "ts_start,ts_end,op,bytes,status,latency_ms,error\n"
            "1.0,2.0,upload,100,ok,1000,\n"
        )
        ops = read_ops_csv(str(p))
        assert len(ops) == 1
        assert ops[0]["endpoint"] == ""
        assert ops[0]["status"] == "ok"


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


from s3flood.metrics import summary_speed_stats, timeline_speeds


class TestTimelineSpeeds:
    def test_empty(self):
        assert timeline_speeds([]) == []

    def test_single_bucket_step_1(self):
        tl = [{"t_sec": 0, "write_bytes": 2 * 1024 * 1024, "read_bytes": 0}]
        assert timeline_speeds(tl) == [2.0]

    def test_coarse_step_divides_by_width(self):
        # бакеты по 10 секунд: 20 MB за бакет = 2 MB/s
        tl = [
            {"t_sec": 0, "write_bytes": 20 * 1024 * 1024, "read_bytes": 0},
            {"t_sec": 10, "write_bytes": 0, "read_bytes": 20 * 1024 * 1024},
        ]
        assert timeline_speeds(tl) == [2.0, 2.0]


class TestSummarySpeedStats:
    def test_normal_summary(self):
        summary = {
            "duration_sec": 10.0,
            "write_bytes": 50 * 1024 * 1024,
            "read_bytes": 50 * 1024 * 1024,
            "write_ok_ops": 30,
            "read_ok_ops": 20,
            "write_MBps_avg": 5.0,
            "read_MBps_avg": 5.0,
            "timeline": [
                {"t_sec": 0, "write_bytes": 30 * 1024 * 1024, "read_bytes": 0},
                {"t_sec": 1, "write_bytes": 20 * 1024 * 1024,
                 "read_bytes": 50 * 1024 * 1024},
            ],
        }
        stats = summary_speed_stats(summary)
        assert stats["total_MBps"] == 10.0
        assert stats["ops_per_sec"] == 5.0
        assert stats["write_MBps"] == 5.0
        assert stats["peak_MBps"] == 70.0
        assert len(stats["speeds"]) == 2

    def test_zero_duration_and_missing_timeline(self):
        stats = summary_speed_stats({"duration_sec": 0.0})
        assert stats["total_MBps"] == 0.0
        assert stats["ops_per_sec"] == 0.0
        assert stats["peak_MBps"] == 0.0
        assert stats["speeds"] == []
