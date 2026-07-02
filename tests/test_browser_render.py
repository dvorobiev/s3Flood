from s3flood.browser import (
    Panel,
    Row,
    build_local_rows,
    render_panel_lines,
    rows_from_entries,
    rows_from_versions,
)
from s3flood.s3browser_io import S3Entry, S3Version


class TestBuildLocalRows:
    def test_dirs_first_with_parent(self, tmp_path):
        (tmp_path / "b.txt").write_text("x")
        (tmp_path / "adir").mkdir()
        rows = build_local_rows(tmp_path)
        assert rows[0].name == ".."
        assert rows[1].name == "adir/" and rows[1].is_dir
        assert rows[2].name == "b.txt" and not rows[2].is_dir
        assert rows[2].size == 1


class TestRowsFromEntries:
    def test_bucket_rows_with_parent_when_prefix(self):
        entries = [
            S3Entry(name="small/", key="data/small/", is_dir=True),
            S3Entry(name="a.bin", key="data/a.bin", is_dir=False, size=2048,
                    last_modified="2026-07-01T11:00:00Z"),
        ]
        rows = rows_from_entries(entries, prefix="data/")
        assert rows[0].name == ".."
        assert rows[1].name == "small/"
        assert rows[2].size == 2048

    def test_no_parent_at_root(self):
        rows = rows_from_entries([], prefix="")
        assert rows == []


class TestRowsFromVersions:
    VERSIONS = [
        S3Version("v3full-long-id", 300, "2026-07-03T10:00:00Z", True),
        S3Version("v2marker", 0, "2026-07-02T10:00:00Z", False, is_delete_marker=True),
        S3Version("v1old", 100, "2026-07-01T10:00:00Z", False),
    ]

    def test_parent_and_badges(self):
        rows = rows_from_versions(self.VERSIONS)
        assert rows[0].name == ".."
        assert "⊙ latest" in rows[1].meta
        assert "del" in rows[2].meta or "удал" in rows[2].meta

    def test_version_id_shortened(self):
        rows = rows_from_versions(self.VERSIONS)
        assert "v3full-l" in rows[1].name
        assert len(rows[1].name) < len("v3full-long-id") + 30


class TestRenderPanelLines:
    def make_panel(self, focused_rows=None):
        rows = focused_rows or [
            Row(name="..", is_dir=True),
            Row(name="file.bin", size=1024, meta="2026-07-01"),
        ]
        return Panel(title="bucket:/data/", rows=rows, selection=1)

    def test_header_and_rows(self):
        lines = render_panel_lines(self.make_panel(), width=40, focused=True)
        header_text = lines[0][1]
        assert "bucket:/data/" in header_text
        assert any("file.bin" in text for _style, text in lines)

    def test_selected_row_marked_when_focused(self):
        lines = render_panel_lines(self.make_panel(), width=40, focused=True)
        sel = [style for style, text in lines if "file.bin" in text][0]
        assert "selected" in sel

    def test_selected_row_not_marked_when_unfocused(self):
        lines = render_panel_lines(self.make_panel(), width=40, focused=False)
        sel = [style for style, text in lines if "file.bin" in text][0]
        assert "selected" not in sel

    def test_loading_placeholder(self):
        panel = Panel(title="t", rows=[], selection=0, loading=True)
        lines = render_panel_lines(panel, width=40, focused=True)
        assert any("загрузка" in text for _s, text in lines)

    def test_width_truncation(self):
        panel = Panel(title="t", rows=[Row(name="x" * 200)], selection=0)
        lines = render_panel_lines(panel, width=30, focused=True)
        assert all(len(text.rstrip("\n")) <= 30 for _s, text in lines)
