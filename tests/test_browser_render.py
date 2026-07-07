from s3flood.browser import (
    Panel,
    Row,
    build_local_rows,
    panel_summary,
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

    def test_parent_present_at_bucket_root_for_bucket_switch(self):
        # в корне бакета ".." ведёт к списку бакетов
        rows = rows_from_entries([], prefix="")
        assert rows and rows[0].name == ".."

    def test_version_badge_from_counts(self):
        entries = [
            S3Entry(name="a.bin", key="data/a.bin", is_dir=False, size=1,
                    last_modified="2026-07-01T11:00:00Z"),
            S3Entry(name="b.bin", key="data/b.bin", is_dir=False, size=1,
                    last_modified="2026-07-01T11:00:00Z"),
        ]
        counts = {"data/a.bin": 3, "data/b.bin": 1}
        rows = rows_from_entries(entries, prefix="data/", version_counts=counts)
        by_name = {r.name: r for r in rows}
        assert "⊙ 3" in by_name["a.bin"].meta
        assert "⊙" not in by_name["b.bin"].meta


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


class TestPanelSummary:
    def test_counts_files_only(self):
        panel = Panel(title="t", rows=[
            Row(name="..", is_dir=True),
            Row(name="dir/", is_dir=True),
            Row(name="a.bin", size=1024),
            Row(name="b.bin", size=2048),
        ])
        s = panel_summary(panel)
        assert "2" in s and "3.0 KB" in s

    def test_marked_selection_wins(self):
        panel = Panel(title="t", rows=[
            Row(name="a.bin", size=1024, marked=True),
            Row(name="b.bin", size=2048),
        ])
        s = panel_summary(panel)
        assert "выделено 1" in s and "1.0 KB" in s

    def test_loading_empty(self):
        assert panel_summary(Panel(title="t", loading=True)) == ""


class TestNoTitleLine:
    def test_first_line_is_column_header(self):
        panel = Panel(title="bucket:/data/",
                      rows=[Row(name="f.bin", size=1)], selection=0)
        lines = render_panel_lines(panel, width=60, focused=True)
        assert "Имя" in lines[0][1]
        assert not any("bucket:/data/" in t for _s, t in lines)


class TestColumns:
    def make_panel(self, mode="list"):
        return Panel(
            title="bucket:/data/",
            rows=[Row(name="..", is_dir=True),
                  Row(name="file.bin", size=1024, meta="2026-07-01 10:00")],
            selection=1,
            mode=mode,
        )

    def test_header_row_for_list_mode(self):
        lines = render_panel_lines(self.make_panel(), width=60, focused=True)
        style, text = lines[0]
        assert "Имя" in text and "Размер" in text and "Дата" in text
        assert "panel.columns" in style

    def test_header_row_for_versions_mode(self):
        lines = render_panel_lines(self.make_panel(mode="versions"), width=60, focused=True)
        assert "Версия" in lines[0][1]

    def test_header_row_for_buckets_mode(self):
        lines = render_panel_lines(self.make_panel(mode="buckets"), width=60, focused=True)
        assert "Бакет" in lines[0][1]

    def test_rows_have_column_separators(self):
        lines = render_panel_lines(self.make_panel(), width=60, focused=True)
        file_line = [t for _s, t in lines if "file.bin" in t][0]
        assert file_line.count("│") == 2

    def test_no_header_when_loading(self):
        panel = Panel(title="t", rows=[], selection=0, loading=True)
        lines = render_panel_lines(panel, width=40, focused=True)
        assert not any("Имя" in t for _s, t in lines)

    def test_header_and_row_columns_aligned(self):
        lines = render_panel_lines(self.make_panel(), width=60, focused=True)
        header = lines[0][1]
        file_line = [t for _s, t in lines if "file.bin" in t][0]
        assert [i for i, ch in enumerate(header) if ch == "│"] == \
               [i for i, ch in enumerate(file_line) if ch == "│"]


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
        assert "Имя" in header_text
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

    def test_marked_rows_have_star_and_style(self):
        panel = Panel(title="t", rows=[
            Row(name="a.bin", marked=True),
            Row(name="b.bin"),
        ], selection=1)
        lines = render_panel_lines(panel, width=60, focused=True)
        a_style, a_text = [(s, t) for s, t in lines if "a.bin" in t][0]
        b_style, b_text = [(s, t) for s, t in lines if "b.bin" in t][0]
        assert "*" in a_text and "marked" in a_style
        assert "*" not in b_text
