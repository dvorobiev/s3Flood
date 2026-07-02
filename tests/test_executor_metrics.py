import csv
import json
import time

import pytest

from s3flood.executor import Metrics


def make_metrics(tmp_path, **kwargs):
    return Metrics(str(tmp_path / "m.csv"), str(tmp_path / "r.json"), **kwargs)


class TestCsvOutput:
    def test_extended_columns(self, tmp_path):
        m = make_metrics(tmp_path)
        t = time.time()
        m.record(
            "upload", t - 1, t, 100, True, None,
            endpoint="http://e1", thread_id=7, attempt=2, size_group="small",
        )
        m.finalize()
        with open(tmp_path / "m.csv") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 1
        assert rows[0]["endpoint"] == "http://e1"
        assert rows[0]["thread_id"] == "7"
        assert rows[0]["attempt"] == "2"
        assert rows[0]["size_group"] == "small"


class TestWarmup:
    def test_warmup_ops_excluded_from_stats(self, tmp_path):
        m = make_metrics(tmp_path, warmup_sec=3600)
        t = time.time()
        m.record("upload", t - 1, t, 100, True, None)
        assert m.write_ops_ok == 0
        assert m.warmup_ops == 1
        # после окончания warmup операции учитываются
        m.warmup_until = time.time() - 10
        m.record("upload", t - 1, t, 100, True, None)
        assert m.write_ops_ok == 1

    def test_no_warmup_by_default(self, tmp_path):
        m = make_metrics(tmp_path)
        t = time.time()
        m.record("upload", t - 1, t, 100, True, None)
        assert m.write_ops_ok == 1
        assert m.warmup_ops == 0


class TestFinalize:
    def test_duration_is_active_span_not_wall_clock(self, tmp_path):
        m = make_metrics(tmp_path)
        t0 = time.time() - 1000
        m.record("upload", t0, t0 + 2, 100, True, None)
        m.record("upload", t0 + 3, t0 + 5, 100, True, None)
        out = m.finalize()
        assert out["duration_sec"] == pytest.approx(5.0, abs=0.01)
        assert out["wall_clock_sec"] < 100  # реальное время процесса, не 1000

    def test_latency_percentiles_present_and_monotone(self, tmp_path):
        m = make_metrics(tmp_path)
        t0 = time.time()
        for i, lat in enumerate([0.1, 0.2, 0.3, 0.4, 2.0]):
            m.record("upload", t0 + i, t0 + i + lat, 100, True, None)
        m.record("download", t0, t0 + 0.5, 100, True, None)
        out = m.finalize()
        w = out["latency"]["write"]
        assert w["count"] == 5
        assert w["p50_ms"] <= w["p90_ms"] <= w["p95_ms"] <= w["p99_ms"]
        assert out["latency"]["read"]["count"] == 1

    def test_speed_p90_not_below_median(self, tmp_path):
        # регрессия out.json: p90_speed_mbps (0.083) < median (0.436)
        m = make_metrics(tmp_path)
        t0 = time.time()
        m.record("upload", t0, t0 + 10.0, 943718, True, None)  # медленная
        m.record("upload", t0, t0 + 1.0, 943718, True, None)   # быстрая
        out = m.finalize()
        overall = out["write_file_analysis"]["overall"]
        assert overall["p90_speed_mbps"] >= overall["median_speed_mbps"]

    def test_report_written_to_json(self, tmp_path):
        m = make_metrics(tmp_path)
        t = time.time()
        m.record("upload", t - 1, t, 100, True, None)
        m.finalize()
        with open(tmp_path / "r.json") as f:
            data = json.load(f)
        assert data["write_ok_ops"] == 1
