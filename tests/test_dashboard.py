import time

from rich.console import Console

from s3flood.dashboard import build_dashboard, sparkline


class TestSparkline:
    def test_renders_blocks(self):
        s = sparkline([0, 1, 2, 4, 8], width=5)
        assert len(s) == 5
        assert s[0] in "▁ " and s[-1] == "█"

    def test_empty(self):
        assert sparkline([], width=10) == ""

    def test_constant_values_not_empty(self):
        s = sparkline([5, 5, 5], width=3)
        assert len(s) == 3

    def test_truncates_to_width(self):
        s = sparkline(list(range(100)), width=12)
        assert len(s) == 12


def render(state) -> str:
    console = Console(record=True, width=110, force_terminal=True)
    console.print(build_dashboard(state))
    return console.export_text()


def base_state(**over):
    state = {
        "profile": "write",
        "pattern": "sustained",
        "burst_active": False,
        "infinite": False,
        "cycle_count": 0,
        "elapsed": 12.3,
        "eta": "3.2 min",
        "phase": "WRITE",
        "warmup_active": False,
        "total_files": 100,
        "files_done": 5,
        "files_read": 0,
        "files_err": 2,
        "total_to_read": 0,
        "current_cycle_files": 0,
        "bytes_done": 5 * 1024**2,
        "bytes_read": 0,
        "total_bytes": 100 * 1024**2,
        "write_rps": 3.25,
        "read_rps": 0.0,
        "wbps_mb": 120.5,
        "rbps_mb": 0.0,
        "avg_wbps_mb": 98.0,
        "avg_rbps_mb": 0.0,
        "inflight": 8,
        "threads": 8,
        "active_uploads": 8,
        "active_downloads": 0,
        "queue": 12,
        "version": "0.10.0",
        "endpoint": "http://10.1.0.5:9080",
        "bucket": "tape",
        "write_rps_history": [1.0, 2.0, 5.0, 8.0, 12.4],
        "read_rps_history": [],
        "recent_ops": [
            {
                "op": "upload", "filename": "abc.bin", "bytes": 900 * 1024**2,
                "latency_ms": 2100, "speed_mbps": 430.0, "started": time.time() - 3,
                "done": True, "error": None,
            },
            {
                "op": "download", "filename": "def.bin", "bytes": 10 * 1024**2,
                "latency_ms": None, "speed_mbps": None, "started": time.time() - 1,
                "done": False, "error": None,
            },
        ],
        "now": time.time(),
    }
    state.update(over)
    return state


class TestBuildDashboard:
    def test_contains_key_info(self):
        out = render(base_state())
        assert "s3flood" in out.lower()
        assert "W-RPS" in out
        assert "abc.bin" in out
        assert "ETA" in out
        assert "3.2 min" in out

    def test_header_shows_endpoint_bucket_and_time(self):
        out = render(base_state())
        assert "10.1.0.5:9080" in out
        assert "tape" in out
        assert "00:12" in out  # elapsed 12.3s как mm:ss

    def test_sparkline_rendered_from_history(self):
        out = render(base_state())
        assert "█" in out  # пик истории W-RPS

    def test_active_op_has_spinner(self):
        out = render(base_state())
        assert any(ch in out for ch in "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")

    def test_errors_shown(self):
        out = render(base_state(files_err=7))
        assert "7" in out

    def test_warmup_badge(self):
        out = render(base_state(warmup_active=True))
        assert "warmup" in out.lower()

    def test_burst_badge(self):
        out = render(base_state(pattern="bursty", burst_active=True))
        assert "BURST" in out

    def test_mixed_phase(self):
        out = render(base_state(profile="mixed", phase="MIXED", total_to_read=50, files_read=10))
        assert "MIXED" in out
