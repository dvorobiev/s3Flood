import pytest
from prompt_toolkit.data_structures import Size
from prompt_toolkit.input import DummyInput
from prompt_toolkit.layout.containers import WritePosition
from prompt_toolkit.layout.mouse_handlers import MouseHandlers
from prompt_toolkit.layout.screen import Screen
from prompt_toolkit.output import DummyOutput

from s3flood.browser import (
    BucketBrowserApp,
    Panel,
    Row,
    build_local_rows,
    panel_summary,
    render_panel_lines,
    rows_from_entries,
    rows_from_versions,
    truncate_middle,
)
from s3flood.s3browser_io import S3Entry, S3Version


class TestTruncateMiddle:
    def test_short_name_untouched(self):
        assert truncate_middle("a.bin", 20) == "a.bin"

    def test_exact_width_untouched(self):
        assert truncate_middle("12345", 5) == "12345"

    def test_long_name_has_ellipsis_and_exact_width(self):
        name = "very-long-object-name-that-does-not-fit.tar.gz"
        result = truncate_middle(name, 20)
        assert len(result) == 20
        assert "…" in result

    def test_tail_preserves_extension(self):
        name = "a" * 40 + ".tar.gz"
        result = truncate_middle(name, 20)
        assert result.endswith(".tar.gz")

    def test_width_le_1_returns_prefix(self):
        assert truncate_middle("abcdef", 1) == "a"
        assert truncate_middle("abcdef", 0) == ""

    def test_empty_name(self):
        assert truncate_middle("", 10) == ""


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


class TestBucketsPanelSummary:
    def test_counts_buckets_without_size(self):
        panel = Panel(title="t", mode="buckets", rows=[
            Row(name="bucket-a", is_dir=True),
            Row(name="bucket-b", is_dir=True),
            Row(name="bucket-c", is_dir=True),
        ])
        s = panel_summary(panel)
        assert "3" in s and "бакет" in s
        assert "B" not in s and "KB" not in s

    def test_ignores_parent_row(self):
        panel = Panel(title="t", mode="buckets", rows=[
            Row(name="..", is_dir=True),
            Row(name="bucket-a", is_dir=True),
        ])
        assert "1" in panel_summary(panel)


class _FixedSizeOutput(DummyOutput):
    """DummyOutput с настраиваемой шириной терминала (для теста реального рендера)."""

    def __init__(self, columns: int, rows: int = 40):
        self.columns = columns
        self.rows = rows

    def get_size(self) -> Size:
        return Size(rows=self.rows, columns=self.columns)


class TestPanelWidthRealRender:
    """Реальный рендер через prompt_toolkit-контейнер (не только чистая функция).

    Проверяет, что последний символ длинной строки не обрезается ни внутренней
    рамкой панели, ни внешней рамкой приложения.
    """

    @pytest.mark.parametrize("cols", [78, 80, 100, 120])
    def test_last_char_of_long_row_not_clipped(self, tmp_path, cols):
        app = BucketBrowserApp(
            bucket="b", endpoint="h", env={}, start_dir=tmp_path,
            input=DummyInput(), output=_FixedSizeOutput(cols),
        )
        app.right.loading = False
        app.right.mode = "list"
        app.right.rows = [Row(name="a.bin", size=1024, meta="2026-07-01 10:00")]
        app.right.selection = 0

        panel_width = app._panel_width()
        expected_lines = [
            text for _style, text in render_panel_lines(app.right, panel_width, True)
        ]
        expected_row = expected_lines[1].rstrip("\n")  # 0 — заголовки колонок

        screen = Screen()
        write_position = WritePosition(xpos=0, ypos=0, width=cols, height=40)
        app.app.layout.container.write_to_screen(
            screen, MouseHandlers(), write_position, "", False, 0
        )
        screen.draw_all_floats()

        # col 0 — внешняя левая "║"; inner_start = 1; левая панель занимает
        # panel_area колонок (её собственная рамка Frame внутри); правая панель
        # начинается сразу за левой (без разделителя).
        panel_area = (cols - 2) // 2
        right_content_start = 1 + panel_area + 1  # +1 внешняя рамка, +1 своя рамка Frame
        row_chars = screen.data_buffer[3]  # 0 — внешняя рамка, 1 — Frame-рамка/титул,
        # 2 — заголовки колонок, 3 — первая строка данных
        actual_row = "".join(
            row_chars[x].char for x in range(right_content_start,
                                              right_content_start + panel_width)
        )

        assert actual_row == expected_row, (
            f"cols={cols}: последний символ строки обрезан рамкой "
            f"(expected={expected_row!r}, actual={actual_row!r})"
        )


class TestOuterDoubleFrame:
    def test_top_and_bottom_borders(self, tmp_path):
        cols = 80
        app = BucketBrowserApp(
            bucket="b", endpoint="h", env={}, start_dir=tmp_path,
            input=DummyInput(), output=_FixedSizeOutput(cols),
        )
        screen = Screen()
        write_position = WritePosition(xpos=0, ypos=0, width=cols, height=40)
        app.app.layout.container.write_to_screen(
            screen, MouseHandlers(), write_position, "", False, 0
        )
        screen.draw_all_floats()

        top = screen.data_buffer[0]
        bottom = screen.data_buffer[39]
        assert top[0].char == "╔" and top[cols - 1].char == "╗"
        assert bottom[0].char == "╚" and bottom[cols - 1].char == "╝"
        assert all(top[x].char == "═" for x in range(1, cols - 1))

    def test_side_borders_present_on_body_row(self, tmp_path):
        cols = 80
        app = BucketBrowserApp(
            bucket="b", endpoint="h", env={}, start_dir=tmp_path,
            input=DummyInput(), output=_FixedSizeOutput(cols),
        )
        screen = Screen()
        write_position = WritePosition(xpos=0, ypos=0, width=cols, height=40)
        app.app.layout.container.write_to_screen(
            screen, MouseHandlers(), write_position, "", False, 0
        )
        screen.draw_all_floats()

        body_row = screen.data_buffer[1]
        assert body_row[0].char == "║"
        assert body_row[cols - 1].char == "║"


class TestFocusAccentStyle:
    def test_focused_style_brighter_than_default_frame(self, tmp_path):
        app = BucketBrowserApp(
            bucket="b", endpoint="h", env={}, start_dir=tmp_path,
            input=DummyInput(), output=DummyOutput(),
        )
        rules = dict(app.app.style.style_rules)
        assert "ansibrightcyan" in rules["panelfocus frame.border"]
        assert "ansibrightcyan" in rules["panelfocus frame.label"]
        assert "ansibrightcyan" in rules["panelfocus panel.summary"]
        # неактивная рамка/каталоги — без изменений
        assert rules["frame.border"] == "fg:#585858"
        assert rules["row.dir"] == "fg:#00d7ff"


class TestPanelsDoNotCollapse:
    """Воспроизводит баг: ширина панели не должна зависеть от того, что в ней
    временно отрендерено (loading-заглушка короче полного списка).
    """

    def _right_frame_left_border_col(self, app, cols: int) -> int:
        screen = Screen()
        write_position = WritePosition(xpos=0, ypos=0, width=cols, height=40)
        app.app.layout.container.write_to_screen(
            screen, MouseHandlers(), write_position, "", False, 0
        )
        screen.draw_all_floats()
        # строка 0 — внешняя двойная рамка приложения, строка 1 — тело
        # (VSplit с внешними "║" по бокам); col 0 — внешняя "║", col 1 —
        # "┌" левой панели, поэтому ищем правую панель начиная с col 2.
        row = screen.data_buffer[1]
        for x in range(2, cols):
            if row[x].char == "┌":
                return x
        raise AssertionError("правая рамка не найдена в верхней строке тела")

    def test_loading_and_loaded_give_same_boundary(self, tmp_path):
        cols = 100
        app = BucketBrowserApp(
            bucket="b", endpoint="h", env={}, start_dir=tmp_path,
            input=DummyInput(), output=_FixedSizeOutput(cols),
        )
        # состояние сразу после старта — правая панель ещё loading
        assert app.right.loading is True
        loading_boundary = self._right_frame_left_border_col(app, cols)

        app.right.loading = False
        app.right.mode = "list"
        app.right.rows = [Row(name="a.bin", size=1024, meta="2026-07-01 10:00")]
        app.right.selection = 0
        loaded_boundary = self._right_frame_left_border_col(app, cols)

        assert loading_boundary == loaded_boundary == cols // 2


class TestUniformRowWidth:
    def test_rows_with_different_meta_length_have_same_text_length(self):
        panel = Panel(title="t", rows=[
            Row(name="..", is_dir=True),  # meta пустая
            Row(name="a.bin", size=1024, meta="2026-07-01 10:00"),  # 16 симв.
            Row(name="b.bin", size=1, meta="⊙ latest"),  # короткая метка
        ], selection=0)
        lines = render_panel_lines(panel, width=60, focused=True)
        # первая строка результата — заголовки колонок, дальше — по одной на Row
        row_lengths = {len(text.rstrip("\n")) for _s, text in lines[1:]}
        assert row_lengths == {60}

    def test_header_row_has_same_length_as_data_rows(self):
        panel = Panel(title="t", rows=[Row(name="..", is_dir=True)], selection=0)
        lines = render_panel_lines(panel, width=50, focused=True)
        assert len(lines[0][1].rstrip("\n")) == 50
        assert len(lines[1][1].rstrip("\n")) == 50

    def test_selected_dotdot_same_width_as_selected_row_with_long_meta(self):
        panel_dotdot = Panel(title="t", rows=[Row(name="..", is_dir=True)], selection=0)
        panel_dated = Panel(title="t", rows=[
            Row(name="a.bin", size=1, meta="2026-07-01 10:00")
        ], selection=0)
        len_dotdot = len(render_panel_lines(panel_dotdot, 60, True)[1][1].rstrip("\n"))
        len_dated = len(render_panel_lines(panel_dated, 60, True)[1][1].rstrip("\n"))
        assert len_dotdot == len_dated == 60


class TestNoCursorStyleWhenUnfocused:
    def test_unfocused_selection_has_no_cursor_style(self):
        panel = Panel(title="t", rows=[
            Row(name="a.bin", size=1), Row(name="b.bin", size=1),
        ], selection=0)
        lines = render_panel_lines(panel, width=40, focused=False)
        sel_style = [s for s, t in lines if "a.bin" in t][0]
        assert "cursor" not in sel_style
        assert "selected" not in sel_style

    def test_focused_selection_still_gets_selected_style(self):
        panel = Panel(title="t", rows=[Row(name="a.bin", size=1)], selection=0)
        lines = render_panel_lines(panel, width=40, focused=True)
        sel_style = [s for s, t in lines if "a.bin" in t][0]
        assert "selected" in sel_style


class TestReloadLocalResetsSelection:
    def test_navigating_into_dir_resets_selection_to_first_row(self, tmp_path):
        app = BucketBrowserApp(
            bucket="b", endpoint="h", env={}, start_dir=tmp_path,
            input=DummyInput(), output=DummyOutput(),
        )
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "z.bin").write_text("x")
        app.left.selection = 5  # был выбран 5-й элемент в предыдущем каталоге
        app.local_path = sub
        app.reload_local()
        assert app.left.selection == 0

    def test_navigating_up_resets_selection_to_first_row(self, tmp_path):
        sub = tmp_path / "sub"
        sub.mkdir()
        app = BucketBrowserApp(
            bucket="b", endpoint="h", env={}, start_dir=sub,
            input=DummyInput(), output=DummyOutput(),
        )
        app.left.selection = 3
        app.local_path = tmp_path
        app.reload_local()
        assert app.left.selection == 0


class TestGoingUpSelectsPreviousDir:
    def test_backspace_selects_dir_we_came_from(self, tmp_path):
        (tmp_path / "aaa").mkdir()
        (tmp_path / "sub").mkdir()
        (tmp_path / "zzz").mkdir()
        app = BucketBrowserApp(
            bucket="b", endpoint="h", env={}, start_dir=tmp_path / "sub",
            input=DummyInput(), output=DummyOutput(),
        )
        app.focus_right = False
        app._key_back(None)
        assert app.local_path == tmp_path.resolve()
        selected = app.left.selected()
        assert selected is not None and selected.name == "sub/"

    def test_enter_on_dotdot_selects_dir_we_came_from(self, tmp_path):
        (tmp_path / "aaa").mkdir()
        (tmp_path / "sub").mkdir()
        app = BucketBrowserApp(
            bucket="b", endpoint="h", env={}, start_dir=tmp_path / "sub",
            input=DummyInput(), output=DummyOutput(),
        )
        app.focus_right = False
        app.left.selection = 0  # ".." — первая строка
        app._key_enter(None)
        assert app.local_path == tmp_path.resolve()
        selected = app.left.selected()
        assert selected is not None and selected.name == "sub/"

    def test_entering_subdir_still_resets_to_first_row(self, tmp_path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "inner").mkdir()
        app = BucketBrowserApp(
            bucket="b", endpoint="h", env={}, start_dir=tmp_path,
            input=DummyInput(), output=DummyOutput(),
        )
        app.focus_right = False
        # найти строку "sub/" и встать на неё
        idx = next(i for i, r in enumerate(app.left.rows) if r.name == "sub/")
        app.left.selection = idx
        app._key_enter(None)
        assert app.local_path == sub.resolve()
        assert app.left.selection == 0
