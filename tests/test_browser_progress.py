import asyncio
from pathlib import Path

from prompt_toolkit.input import DummyInput
from prompt_toolkit.output import DummyOutput

import s3flood.browser as browser_mod
from s3flood.browser import BucketBrowserApp, ProgressState, Row, make_bar, render_progress_lines
from s3flood.s3browser_io import OpResult


def make_app(tmp_path):
    return BucketBrowserApp(
        bucket="b", endpoint="http://h:9000", env={},
        start_dir=tmp_path, input=DummyInput(), output=DummyOutput(),
    )


class TestMakeBar:
    def test_empty_full_and_clamp(self):
        assert make_bar(0.0, 10) == "░" * 10
        assert make_bar(1.0, 10) == "█" * 10
        assert make_bar(1.7, 10) == "█" * 10
        assert make_bar(-0.5, 10) == "░" * 10

    def test_partial_length(self):
        bar = make_bar(0.5, 10)
        assert len(bar) == 10
        assert bar.count("█") == 5


class TestRenderProgressLines:
    def test_shows_current_file_and_counts(self):
        p = ProgressState(title="Копирование ↑", current="a.bin",
                          done=7, total=11, bytes_done=100, bytes_total=400)
        text = "".join(t for _s, t in render_progress_lines(p, width=50))
        assert "a.bin" in text
        assert "7/11" in text
        assert "Файлы" in text and "Объём" in text
        assert "Esc" in text

    def test_bytes_bar_hidden_when_total_zero(self):
        p = ProgressState(title="Удаление", current="a.bin", done=1, total=3)
        text = "".join(t for _s, t in render_progress_lines(p, width=50))
        assert "Объём" not in text

    def test_errors_shown(self):
        p = ProgressState(title="Копирование ↑", done=2, total=3, errors=2)
        errors_line = [t for _s, t in render_progress_lines(p, width=50)
                       if "Ошибок" in t][0]
        assert "Ошибок: 2" in errors_line

    def test_lines_fit_width(self):
        p = ProgressState(title="Копирование ↑", current="x" * 200,
                          done=1, total=2, bytes_done=1, bytes_total=2)
        for _s, t in render_progress_lines(p, width=30):
            assert len(t.rstrip("\n")) <= 30


class TestCancelBatch:
    def test_cancel_stops_remaining_files(self, tmp_path, monkeypatch):
        app = make_app(tmp_path)
        uploads = []

        async def fake_run_aws(cmd, env):
            if cmd[:3] == ["aws", "s3", "cp"]:
                uploads.append(cmd)
                app.progress.cancelled = True  # отмена после первого файла
            return OpResult(ok=True)

        async def fake_list_prefix(bucket, prefix, endpoint, env):
            return OpResult(ok=True), []

        monkeypatch.setattr(browser_mod, "run_aws", fake_run_aws)
        monkeypatch.setattr(browser_mod, "list_prefix", fake_list_prefix)

        files = []
        for name in ("a.bin", "b.bin", "c.bin"):
            f = tmp_path / name
            f.write_bytes(b"x")
            files.append(Row(name=name, size=1, payload=f))

        asyncio.run(app._op_transfer_batch(files, move=False, from_local=True))
        assert len(uploads) == 1
        assert app.progress is None
        assert "Отмен" in app.status_msg

    def test_progress_cleared_after_batch(self, tmp_path, monkeypatch):
        app = make_app(tmp_path)

        async def fake_run_aws(cmd, env):
            return OpResult(ok=True)

        async def fake_list_prefix(bucket, prefix, endpoint, env):
            return OpResult(ok=True), []

        monkeypatch.setattr(browser_mod, "run_aws", fake_run_aws)
        monkeypatch.setattr(browser_mod, "list_prefix", fake_list_prefix)
        f = tmp_path / "a.bin"
        f.write_bytes(b"x")
        asyncio.run(app._op_transfer_batch(
            [Row(name="a.bin", size=1, payload=f)], move=False, from_local=True))
        assert app.progress is None
