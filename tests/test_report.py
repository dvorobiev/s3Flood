import time

from s3flood.executor import Metrics, print_summary
from s3flood.metrics import build_timeline, classify_error


def make_summary():
    return {
        "meta": {"profile": "mixed", "version": "0.13.0"},
        "duration_sec": 10.0,
        "wall_clock_sec": 11.0,
        "write_bytes": 100 * 1024 * 1024,
        "read_bytes": 50 * 1024 * 1024,
        "write_ok_ops": 40,
        "read_ok_ops": 10,
        "err_ops": 0,
        "write_MBps_avg": 10.0,
        "read_MBps_avg": 5.0,
        "timeline": [
            {"t_sec": 0, "write_bytes": 60 * 1024 * 1024, "read_bytes": 0},
            {"t_sec": 1, "write_bytes": 40 * 1024 * 1024,
             "read_bytes": 50 * 1024 * 1024},
        ],
        "latency": {
            "write": {"p50_ms": 100, "p90_ms": 150, "p95_ms": 180,
                      "p99_ms": 250, "avg_ms": 120, "max_ms": 400},
        },
    }


class TestClassifyError:
    def test_aws_error_code_extracted(self):
        err = ("An error occurred (ServiceUnavailable) when calling the "
               "PutObject operation (reached max retries: 2): Storage is offline")
        assert classify_error(err) == "ServiceUnavailable"

    def test_slowdown(self):
        assert classify_error("An error occurred (SlowDown) when calling ...") == "SlowDown"

    def test_timeout(self):
        assert classify_error("Read timeout on endpoint URL: http://x") == "timeout"

    def test_connection(self):
        assert classify_error('Could not connect to the endpoint URL: "http://x"') == "connection"

    def test_interrupted(self):
        assert classify_error("interrupted by user") == "interrupted"

    def test_unknown_and_other(self):
        assert classify_error(None) == "unknown"
        assert classify_error("") == "unknown"
        assert classify_error("что-то пошло не так") == "other"


class TestBuildTimeline:
    def test_per_second_buckets(self):
        t0 = 1000.0
        ops = [
            ("upload", t0, t0 + 0.5, 100, True, 500),
            ("upload", t0 + 0.2, t0 + 0.9, 100, True, 700),
            ("download", t0 + 1.1, t0 + 1.6, 200, True, 500),
            ("upload", t0 + 2.0, t0 + 2.5, 50, False, 500),
        ]
        tl = build_timeline(ops)
        assert tl[0]["write_ops"] == 2
        assert tl[0]["write_bytes"] == 200
        assert tl[1]["read_ops"] == 1
        assert tl[2]["err_ops"] == 1
        assert tl[0]["t_sec"] == 0

    def test_long_run_is_downsampled(self):
        t0 = 1000.0
        ops = [("upload", t0 + i, t0 + i + 0.5, 10, True, 500) for i in range(1000)]
        tl = build_timeline(ops, max_points=300)
        assert len(tl) <= 300
        assert sum(b["write_ops"] for b in tl) == 1000

    def test_empty(self):
        assert build_timeline([]) == []


class TestReportV2:
    def test_finalize_contains_meta_errors_timeline(self, tmp_path):
        m = Metrics(str(tmp_path / "m.csv"), str(tmp_path / "r.json"))
        m.meta = {"profile": "write", "endpoints": ["http://e1"]}
        t = time.time()
        m.record("upload", t - 2, t - 1, 100, True, None)
        m.record("upload", t - 1, t, 100, False,
                 "An error occurred (ServiceUnavailable) when calling the PutObject operation: x")
        out = m.finalize()
        assert out["meta"]["profile"] == "write"
        assert out["errors"] == {"ServiceUnavailable": 1}
        assert len(out["timeline"]) >= 1
        assert out["timeline"][0]["write_ops"] == 1


class TestPrintSummarySpeedFocus:
    def test_speed_block_first_and_has_throughput(self, capsys):
        print_summary(make_summary(), "m.csv", "r.json")
        out = capsys.readouterr().out
        assert "Скорость" in out
        assert "15.0 MB/s" in out          # сквозная (150 MB / 10 s)
        assert out.index("Скорость") < out.index("Латентность")

    def test_totals_row_present(self, capsys):
        print_summary(make_summary(), "m.csv", "r.json")
        out = capsys.readouterr().out
        assert "Итого" in out

    def test_latency_compact_no_max_column(self, capsys):
        print_summary(make_summary(), "m.csv", "r.json")
        out = capsys.readouterr().out
        assert "p95" in out and "p99" in out
        assert "max" not in out

    def test_works_without_timeline_and_latency(self, capsys):
        s = make_summary()
        del s["timeline"], s["latency"]
        print_summary(s, "m.csv", "r.json")
        assert "Скорость" in capsys.readouterr().out
