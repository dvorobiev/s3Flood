"""Двухпанельный TUI-браузер бакета (стиль Midnight Commander).

Слева — локальная файловая система, справа — S3. Из корня бакета «..» ведёт
к списку бакетов. Enter на объекте открывает его версии как «папку».
Space выделяет несколько объектов; F5 копирует, F6 перемещает (в режиме
версий — откатывает объект к версии), F8 удаляет — пакетно, с прогрессом.
Построен на prompt_toolkit по образцу ConfigEditorApp — тот же стек и
стилистика, что и остальной TUI s3flood.
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from prompt_toolkit.application import Application
from prompt_toolkit.data_structures import Point
from prompt_toolkit.filters import Condition
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import (
    ConditionalContainer,
    Float,
    FloatContainer,
    HSplit,
    Layout,
    VSplit,
    Window,
)
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.styles import Style
from prompt_toolkit.widgets import Frame

from .executor import format_bytes
from .s3browser_io import (
    S3Entry,
    S3Version,
    build_delete_cmd,
    build_get_object_cmd,
    build_list_buckets_cmd,
    build_restore_cmd,
    build_upload_cmd,
    build_versioning_status_cmd,
    build_versions_cmd,
    list_prefix,
    list_versions,
    parse_buckets,
    parse_version_counts,
    parse_versioning_enabled,
    run_aws,
)

SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


def truncate_middle(name: str, width: int) -> str:
    """Сокращает длинную строку до width символов: начало…конец.

    Хвост включает конец исходной строки — для имён файлов это естественно
    сохраняет расширение, т.к. оно всегда в конце.
    """
    if len(name) <= width:
        return name
    if width <= 1:
        return name[:width]
    avail = width - 1  # 1 символ на "…"
    tail_len = avail // 2
    head_len = avail - tail_len
    return name[:head_len] + "…" + (name[-tail_len:] if tail_len else "")


# Заголовки колонок панели по режимам (имя, размер, дата/метки)
COLUMN_HEADERS: dict[str, tuple[str, str, str]] = {
    "list": ("Имя", "Размер", "Дата"),
    "versions": ("Версия", "Размер", "Дата / метки"),
    "buckets": ("Бакет", "", ""),
}


def format_columns(name: str, size: str, meta: str, width: int) -> str:
    """Одна строка панели: имя │ размер │ мета — с фиксированной сеткой колонок."""
    name_w = max(width - 33, 12)
    display_name = truncate_middle(name, name_w)
    return f"{display_name:<{name_w}} │{size:>9} │ {meta}"


def panel_summary(panel: Panel) -> str:
    """Сводная строка панели: всего объектов/размер или выделенное."""
    if panel.loading:
        return ""
    if panel.mode == "buckets":
        count = len([r for r in panel.rows if r.name != ".."])
        return f" {count} бакетов"
    files = [r for r in panel.rows if not r.is_dir and r.name != ".."]
    marked = [r for r in files if r.marked]
    if marked:
        return f" выделено {len(marked)} · {format_bytes(sum(r.size for r in marked))}"
    return f" {len(files)} объектов · {format_bytes(sum(r.size for r in files))}"


@dataclass
class Row:
    name: str
    is_dir: bool = False
    size: int = 0
    meta: str = ""
    payload: object = None
    marked: bool = False


@dataclass
class Panel:
    title: str
    rows: list[Row] = field(default_factory=list)
    selection: int = 0
    loading: bool = False
    # bucket-панель: "list" — листинг префикса, "versions" — версии объекта,
    # "buckets" — выбор бакета
    mode: str = "list"

    def selected(self) -> Optional[Row]:
        if 0 <= self.selection < len(self.rows):
            return self.rows[self.selection]
        return None

    def clamp(self) -> None:
        self.selection = max(0, min(self.selection, len(self.rows) - 1))

    def marked_rows(self) -> list[Row]:
        return [r for r in self.rows if r.marked]

    def clear_marks(self) -> None:
        for r in self.rows:
            r.marked = False


@dataclass
class ProgressState:
    """Состояние модального окна прогресса пакетной операции."""
    title: str
    current: str = ""
    done: int = 0
    total: int = 0
    bytes_done: int = 0
    bytes_total: int = 0
    errors: int = 0
    cancelled: bool = False


def make_bar(frac: float, width: int) -> str:
    frac = max(0.0, min(1.0, frac))
    filled = int(round(frac * width))
    return "█" * filled + "░" * (width - filled)


def render_progress_lines(p: ProgressState, width: int) -> list[tuple[str, str]]:
    """Чистый рендер содержимого окна прогресса (без рамки)."""
    def cut(text: str) -> str:
        return text[:width] if len(text) > width else text

    bar_w = max(width - 26, 5)
    lines: list[tuple[str, str]] = [("class:progress.file", cut(f" {p.current}") + "\n")]
    frac_files = p.done / p.total if p.total else 0.0
    lines.append(("class:progress.bar",
                  cut(f" Файлы  {make_bar(frac_files, bar_w)}  {p.done}/{p.total}") + "\n"))
    if p.bytes_total > 0:
        frac_bytes = p.bytes_done / p.bytes_total
        lines.append(("class:progress.bar",
                      cut(f" Объём  {make_bar(frac_bytes, bar_w)}  "
                          f"{format_bytes(p.bytes_done)} / {format_bytes(p.bytes_total)}") + "\n"))
    lines.append(("class:progress.hint",
                  cut(f" Ошибок: {p.errors} · Esc — отмена") + "\n"))
    return lines


def build_local_rows(path: Path) -> list[Row]:
    """Строки локальной панели: .. , каталоги, файлы (по алфавиту)."""
    rows: list[Row] = []
    if path.parent != path:
        rows.append(Row(name="..", is_dir=True, payload=path.parent))
    dirs, files = [], []
    try:
        for item in sorted(path.iterdir(), key=lambda p: p.name.lower()):
            if item.name.startswith("."):
                continue
            try:
                if item.is_dir():
                    dirs.append(Row(name=item.name + "/", is_dir=True, payload=item))
                else:
                    st = item.stat()
                    meta = time.strftime("%Y-%m-%d %H:%M", time.localtime(st.st_mtime))
                    files.append(Row(name=item.name, size=st.st_size, meta=meta, payload=item))
            except OSError:
                continue
    except OSError:
        pass
    return rows + dirs + files


def rows_from_entries(
    entries: list[S3Entry], prefix: str, version_counts: dict[str, int] | None = None
) -> list[Row]:
    """Строки бакет-панели из листинга префикса.

    «..» есть всегда: из корня бакета она ведёт к списку бакетов.
    """
    rows: list[Row] = [Row(name="..", is_dir=True)]
    counts = version_counts or {}
    for e in entries:
        if e.is_dir:
            rows.append(Row(name=e.name, is_dir=True, payload=e))
        else:
            meta = e.last_modified[:16].replace("T", " ")
            n = counts.get(e.key, 1)
            if n > 1:
                meta += f"  ⊙ {n}"
            rows.append(Row(name=e.name, size=e.size, meta=meta, payload=e))
    return rows


def rows_from_buckets(names: list[str], current: str) -> list[Row]:
    """Строки бакет-панели в режиме выбора бакета."""
    rows: list[Row] = []
    for name in names:
        meta = "← текущий" if name == current else ""
        rows.append(Row(name=name, is_dir=True, meta=meta, payload=name))
    return rows


def rows_from_versions(versions: list[S3Version]) -> list[Row]:
    """Строки бакет-панели в режиме версий объекта."""
    rows: list[Row] = [Row(name="..", is_dir=True)]
    for v in versions:
        short = v.version_id[:8] + ("…" if len(v.version_id) > 8 else "")
        meta_parts = [v.last_modified[:16].replace("T", " ")]
        if v.is_delete_marker:
            meta_parts.append("удалена (delete marker)")
        else:
            if v.is_latest:
                meta_parts.append("⊙ latest")
        rows.append(Row(name=short, size=v.size, meta=" · ".join(meta_parts), payload=v))
    return rows


def render_panel_lines(panel: Panel, width: int, focused: bool) -> list[tuple[str, str]]:
    """Чистый рендер панели в список (style, text)-строк. Титул рисует Frame (Task 4)."""
    def cut(text: str) -> str:
        return text[:width] if len(text) > width else text

    lines: list[tuple[str, str]] = []

    if panel.loading:
        frame = SPINNER[int(time.time() * 8) % len(SPINNER)]
        lines.append(("class:loading", cut(f" {frame} загрузка…") + "\n"))
        return lines

    headers = COLUMN_HEADERS.get(panel.mode, COLUMN_HEADERS["list"])
    lines.append(("class:panel.columns", cut("   " + format_columns(*headers, width)) + "\n"))

    for idx, row in enumerate(panel.rows):
        is_sel = idx == panel.selection
        cursor = "»" if is_sel and focused else " "
        mark = "*" if row.marked else " "
        size_disp = "" if row.is_dir else format_bytes(row.size)
        text = f"{cursor}{mark} " + format_columns(row.name, size_disp, row.meta, width)
        style = "class:row.dir" if row.is_dir else "class:row"
        if row.marked:
            style = "class:row.marked"
        if is_sel and focused:
            style = "class:row.selected"
        elif is_sel:
            style = "class:row.cursor"
        lines.append((style, cut(text) + "\n"))
    if not panel.rows:
        lines.append(("class:dim", cut("  (пусто)") + "\n"))
    return lines


class BucketBrowserApp:
    def __init__(self, *, bucket: str, endpoint: str, env: dict,
                 start_dir: Path, prefix: str = "", input=None, output=None):
        self.bucket = bucket
        self.endpoint = endpoint
        self.env = env
        self.local_path = start_dir.resolve()
        self.prefix = prefix
        self.versions_key: Optional[str] = None
        # версионирование бакетов: имя -> bool (кэш)
        self._versioning: dict[str, bool] = {}
        self._load_gen = 0  # защита от устаревших async-ответов

        self.left = Panel(title=str(self.local_path), rows=build_local_rows(self.local_path))
        self.right = Panel(title=self._bucket_title(), loading=True)
        self.focus_right = True
        self.status_msg = ""
        self.status_err = ""
        # (текст подтверждения, корутина-действие) для инлайн y/n
        self.confirm: Optional[tuple[str, object]] = None
        self.progress: Optional[ProgressState] = None

        left_control = FormattedTextControl(
            lambda: self._fragments(self.left, focused=not self.focus_right),
            get_cursor_position=lambda: self._cursor_point(self.left),
        )
        right_control = FormattedTextControl(
            lambda: self._fragments(self.right, focused=self.focus_right),
            get_cursor_position=lambda: self._cursor_point(self.right),
        )
        self.left_window = Window(content=left_control, always_hide_cursor=True, wrap_lines=False)
        self.right_window = Window(content=right_control, always_hide_cursor=True, wrap_lines=False)

        def _summary_control(panel: Panel) -> FormattedTextControl:
            return FormattedTextControl(
                lambda: [("class:panel.summary", panel_summary(panel))]
            )

        left_frame = HSplit(
            [Frame(
                HSplit([
                    self.left_window,
                    Window(height=1, content=_summary_control(self.left)),
                ]),
                title=lambda: f" {self.left.title} ",
            )],
            style=lambda: "class:panelfocus" if not self.focus_right else "",
        )
        right_frame = HSplit(
            [Frame(
                HSplit([
                    self.right_window,
                    Window(height=1, content=_summary_control(self.right)),
                ]),
                title=lambda: f" {self.right.title} ",
            )],
            style=lambda: "class:panelfocus" if self.focus_right else "",
        )
        body = VSplit([left_frame, right_frame])
        status = Window(height=1, content=FormattedTextControl(self._render_status))
        keybar = Window(height=1, content=FormattedTextControl(self._render_keybar),
                        style="class:keybar")

        progress_body = Window(
            content=FormattedTextControl(self._render_progress),
            always_hide_cursor=True, width=54, height=4,
        )
        progress_float = Float(content=ConditionalContainer(
            Frame(progress_body, title=lambda: f" {self.progress.title} " if self.progress else ""),
            filter=Condition(lambda: self.progress is not None),
        ))
        root = FloatContainer(
            content=HSplit([body, status, keybar]),
            floats=[progress_float],
        )

        kb = KeyBindings()
        active = Condition(lambda: self.confirm is None and self.progress is None)
        in_confirm = Condition(lambda: self.confirm is not None)
        in_progress = Condition(lambda: self.progress is not None)
        kb.add("tab", filter=active)(self._key_tab)
        kb.add("up", filter=active)(self._key_up)
        kb.add("down", filter=active)(self._key_down)
        kb.add("pageup", filter=active)(self._key_pgup)
        kb.add("pagedown", filter=active)(self._key_pgdn)
        kb.add("enter", filter=active)(self._key_enter)
        kb.add("backspace", filter=active)(self._key_back)
        kb.add("space", filter=active)(self._key_mark)
        kb.add("f5", filter=active)(self._key_copy)
        kb.add("f6", filter=active)(self._key_move_or_restore)
        kb.add("f8", filter=active)(self._key_delete)
        kb.add("r", filter=active)(self._key_refresh)
        kb.add("q", filter=active)(self._key_quit)
        kb.add("f10")(self._key_quit)
        kb.add("c-c")(self._key_quit)
        kb.add("y", filter=in_confirm)(self._key_confirm_yes)
        kb.add("n", filter=in_confirm)(self._key_confirm_no)
        kb.add("escape", filter=in_confirm)(self._key_confirm_no)
        kb.add("escape", filter=in_progress)(self._key_cancel_op)

        self.app = Application(
            layout=Layout(root, focused_element=self.right_window),
            key_bindings=kb,
            full_screen=True,
            mouse_support=False,
            refresh_interval=0.2,
            input=input,
            output=output,
            style=Style.from_dict({
                "panel.columns": "fg:#6c6c6c",
                "row": "",
                "row.dir": "fg:#00d7ff",
                "row.selected": "reverse",
                "row.cursor": "underline",
                "row.marked": "fg:ansiyellow bold",
                "status": "",
                "status.error": "fg:ansired bold",
                "status.confirm": "fg:ansiyellow bold",
                "keybar": "reverse",
                "loading": "fg:ansiyellow",
                "dim": "fg:#6c6c6c",
                "progress.file": "bold",
                "progress.bar": "",
                "progress.hint": "fg:#6c6c6c",
                "frame.border": "fg:#585858",
                "frame.label": "fg:#6c6c6c",
                "panelfocus frame.border": "fg:#00d7ff bold",
                "panelfocus frame.label": "fg:#00d7ff bold",
                "panel.summary": "fg:#6c6c6c",
                "panelfocus panel.summary": "fg:#00d7ff",
            }),
        )

    # ---------- рендер ----------

    def _bucket_title(self) -> str:
        if self.right.mode == "buckets" if hasattr(self, "right") else False:
            return "S3: выбор бакета"
        suffix = ""
        if self._versioning.get(self.bucket) is False:
            suffix = " · versioning off"
        if self.versions_key:
            return f"{self.bucket}:/{self.versions_key} ⊙ версии{suffix}"
        return f"{self.bucket}:/{self.prefix}{suffix}"

    def _panel_width(self) -> int:
        try:
            cols = self.app.output.get_size().columns
        except Exception:
            cols = 80
        # Каждая панель обёрнута в Frame — рамка съедает по 1 колонке слева
        # и справа (2 колонки на панель). Разделителя между панелями больше
        # нет, поэтому на панель приходится ровно половина cols минус её
        # собственная рамка.
        return max(cols // 2 - 2, 20)

    def _fragments(self, panel: Panel, focused: bool):
        return render_panel_lines(panel, self._panel_width(), focused)

    @staticmethod
    def _cursor_point(panel: Panel) -> Point:
        # Курсор не должен указывать за пределы отрендеренных строк:
        # при loading/пустой панели контент короче, чем selection+1
        # Сдвиг на +1: заголовки колонок (строка 0), титул рисует Frame (Task 4)
        if panel.loading or not panel.rows:
            return Point(x=0, y=0)
        return Point(x=0, y=min(panel.selection, len(panel.rows) - 1) + 1)

    def _render_status(self):
        if self.confirm:
            return [("class:status.confirm", f" {self.confirm[0]} (y/n) ")]
        if self.status_err:
            return [("class:status.error", f" ✗ {self.status_err} ")]
        return [("class:status", f" {self.endpoint} · {self.status_msg} ")]

    def _render_progress(self):
        if self.progress is None:
            return []
        return render_progress_lines(self.progress, width=52)

    def _render_keybar(self):
        if self.right.mode == "buckets" and self.focus_right:
            keys = " Enter Выбрать бакет  Tab Панель  r Обновить  q Выход"
        elif self.right.mode == "versions" and self.focus_right:
            keys = (" Enter/.. Назад  F5 Скачать версию  F6 Откатить к версии"
                    "  F8 Удалить версию  q Выход")
        else:
            keys = (" Tab Панель  Enter Открыть  Space Выделить  F5 Копировать"
                    "  F6 Переместить  F8 Удалить  r Обновить  q Выход")
        return [("class:keybar", keys)]

    # ---------- панели/фокус ----------

    def _active(self) -> Panel:
        return self.right if self.focus_right else self.left

    def _key_tab(self, event) -> None:
        self.focus_right = not self.focus_right
        target = self.right_window if self.focus_right else self.left_window
        event.app.layout.focus(target)

    def _key_up(self, event) -> None:
        p = self._active()
        p.selection -= 1
        p.clamp()

    def _key_down(self, event) -> None:
        p = self._active()
        p.selection += 1
        p.clamp()

    def _key_pgup(self, event) -> None:
        p = self._active()
        p.selection -= 15
        p.clamp()

    def _key_pgdn(self, event) -> None:
        p = self._active()
        p.selection += 15
        p.clamp()

    def _key_quit(self, event) -> None:
        event.app.exit()

    def _key_refresh(self, event) -> None:
        self.reload_local()
        if self.right.mode == "buckets":
            self._spawn(self._load_buckets())
        elif self.right.mode == "versions" and self.versions_key:
            self._spawn(self._load_versions(self.versions_key))
        else:
            self._spawn(self._load_bucket())

    def _key_mark(self, event) -> None:
        """Space: выделить/снять выделение файла и перейти ниже (как в MC)."""
        p = self._active()
        row = p.selected()
        if row is None or row.name == ".." or p.mode in ("versions", "buckets") and self.focus_right:
            return
        if row.is_dir:
            return
        row.marked = not row.marked
        p.selection += 1
        p.clamp()

    # ---------- навигация ----------

    def _key_enter(self, event) -> None:
        row = self._active().selected()
        if row is None:
            return
        if not self.focus_right:
            if row.is_dir and isinstance(row.payload, Path):
                self.local_path = row.payload.resolve()
                self.reload_local()
            elif row.name == "..":
                self.local_path = self.local_path.parent
                self.reload_local()
            return
        # бакет-панель
        if self.right.mode == "buckets":
            if isinstance(row.payload, str):
                self.bucket = row.payload
                self.prefix = ""
                self.versions_key = None
                self._spawn(self._load_bucket())
            return
        if self.right.mode == "versions":
            if row.name == "..":
                self.versions_key = None
                self.right.mode = "list"
                self._spawn(self._load_bucket())
            return
        if row.name == "..":
            if self.prefix:
                self._go_prefix_up()
            else:
                self._spawn(self._load_buckets())
        elif row.is_dir and isinstance(row.payload, S3Entry):
            self.prefix = row.payload.key
            self._spawn(self._load_bucket())
        elif isinstance(row.payload, S3Entry):
            self.versions_key = row.payload.key
            self._spawn(self._load_versions(row.payload.key))

    def _key_back(self, event) -> None:
        if not self.focus_right:
            self.local_path = self.local_path.parent
            self.reload_local()
        elif self.right.mode == "versions":
            self.versions_key = None
            self.right.mode = "list"
            self._spawn(self._load_bucket())
        elif self.right.mode == "buckets":
            self._spawn(self._load_bucket())
        elif self.prefix:
            self._go_prefix_up()
        else:
            self._spawn(self._load_buckets())

    def _go_prefix_up(self) -> None:
        parts = self.prefix.rstrip("/").split("/")
        self.prefix = "/".join(parts[:-1]) + "/" if len(parts) > 1 else ""
        self._spawn(self._load_bucket())

    # ---------- выбор целей операций ----------

    def _targets(self, panel: Panel) -> list[Row]:
        marked = [r for r in panel.marked_rows() if not r.is_dir]
        if marked:
            return marked
        row = panel.selected()
        if row is None or row.name == ".." or row.is_dir:
            return []
        return [row]

    # ---------- операции ----------

    def _key_copy(self, event) -> None:
        if self.focus_right and self.right.mode == "buckets":
            return
        if self.focus_right and self.right.mode == "versions":
            row = self.right.selected()
            if isinstance(row.payload if row else None, S3Version) and not row.payload.is_delete_marker:
                v = row.payload
                base = Path(self.versions_key or "object").name
                target = self.local_path / f"{base}.{v.version_id[:8]}"
                self._spawn(self._op_download_version(
                    self.versions_key, target, v.version_id, size=v.size))
            return
        panel = self._active()
        targets = self._targets(panel)
        if not targets:
            self.status_err = "Нечего копировать: выберите файл (Space — несколько)"
            return
        self._spawn(self._op_transfer_batch(targets, move=False, from_local=not self.focus_right))

    def _key_move_or_restore(self, event) -> None:
        if self.focus_right and self.right.mode == "buckets":
            return
        if self.focus_right and self.right.mode == "versions":
            row = self.right.selected()
            if row is None or not isinstance(row.payload, S3Version):
                return
            v = row.payload
            if v.is_delete_marker:
                self.status_err = "Delete marker нельзя восстановить как версию"
                return
            if v.is_latest:
                self.status_msg = "Эта версия уже latest"
                return
            prompt = f"Откатить {self.versions_key} к версии {v.version_id[:8]}?"
            self.confirm = (prompt, self._op_restore(self.versions_key, v.version_id))
            return
        panel = self._active()
        targets = self._targets(panel)
        if not targets:
            self.status_err = "Нечего перемещать: выберите файл (Space — несколько)"
            return
        src = "локальные файлы будут удалены" if not self.focus_right else "объекты будут удалены из бакета"
        prompt = f"Переместить {len(targets)} объект(ов)? После копирования {src}."
        self.confirm = (
            prompt,
            self._op_transfer_batch(targets, move=True, from_local=not self.focus_right),
        )

    def _key_delete(self, event) -> None:
        if not self.focus_right:
            self.status_err = "Удаление локальных файлов из браузера отключено"
            return
        if self.right.mode == "buckets":
            return
        if self.right.mode == "versions":
            row = self.right.selected()
            if row is None or row.name == ".." or not isinstance(row.payload, S3Version):
                return
            v = row.payload
            prompt = f"Удалить версию {v.version_id[:8]} объекта {self.versions_key}?"
            self.confirm = (prompt, self._op_delete_batch([(self.versions_key, v.version_id)]))
            return
        targets = self._targets(self.right)
        if not targets:
            return
        keys = [(r.payload.key, None) for r in targets if isinstance(r.payload, S3Entry)]
        prompt = f"Удалить {len(keys)} объект(ов) из бакета?"
        self.confirm = (prompt, self._op_delete_batch(keys))

    def _key_confirm_yes(self, event) -> None:
        if self.confirm:
            _, coro = self.confirm
            self.confirm = None
            self._spawn(coro)

    def _key_confirm_no(self, event) -> None:
        if self.confirm:
            _, coro = self.confirm
            coro.close()
        self.confirm = None
        self.status_msg = "Отменено"

    def _key_cancel_op(self, event) -> None:
        if self.progress is not None:
            self.progress.cancelled = True

    # ---------- async-действия ----------

    def _spawn(self, coro) -> None:
        self.status_err = ""
        self.app.create_background_task(coro)

    def _invalidate(self) -> None:
        try:
            self.app.invalidate()
        except Exception:
            pass

    def reload_local(self) -> None:
        self.left.rows = build_local_rows(self.local_path)
        self.left.title = str(self.local_path)
        self.left.clamp()

    async def _ensure_versioning_status(self) -> None:
        if self.bucket in self._versioning:
            return
        res = await run_aws(build_versioning_status_cmd(self.bucket, self.endpoint), self.env)
        if res.ok:
            self._versioning[self.bucket] = parse_versioning_enabled(res.payload)

    async def _load_bucket(self) -> None:
        self._load_gen += 1
        gen = self._load_gen
        self.right.loading = True
        self.right.mode = "list"
        self.right.title = self._bucket_title()
        self._invalidate()
        await self._ensure_versioning_status()
        res, entries = await list_prefix(self.bucket, self.prefix, self.endpoint, self.env)
        if gen != self._load_gen:
            return
        self.right.loading = False
        if not res.ok:
            self.status_err = res.error
            self.right.rows = [Row(name="..", is_dir=True)]
        else:
            self.right.rows = rows_from_entries(entries, self.prefix)
            self.status_msg = f"{len(entries)} элементов"
            # счётчики версий подгружаем отдельно, не блокируя листинг
            self._spawn(self._load_version_counts(gen))
        self.right.title = self._bucket_title()
        self.right.selection = 0
        self._invalidate()

    async def _load_version_counts(self, gen: int) -> None:
        if self._versioning.get(self.bucket) is False:
            return
        res = await run_aws(build_versions_cmd(self.bucket, self.prefix, self.endpoint), self.env)
        if not res.ok or gen != self._load_gen:
            return
        counts = parse_version_counts(res.payload)
        for row in self.right.rows:
            if isinstance(row.payload, S3Entry) and not row.is_dir:
                n = counts.get(row.payload.key, 1)
                if n > 1 and "⊙" not in row.meta:
                    row.meta += f"  ⊙ {n}"
        self._invalidate()

    async def _load_buckets(self) -> None:
        self._load_gen += 1
        gen = self._load_gen
        self.right.loading = True
        self.right.mode = "buckets"
        self.right.title = "S3: выбор бакета"
        self._invalidate()
        res = await run_aws(build_list_buckets_cmd(self.endpoint), self.env)
        if gen != self._load_gen:
            return
        self.right.loading = False
        if not res.ok:
            self.status_err = res.error
            self.right.rows = []
        else:
            names = parse_buckets(res.payload)
            self.right.rows = rows_from_buckets(names, self.bucket)
            self.status_msg = f"{len(names)} бакетов"
        self.right.selection = 0
        self._invalidate()

    async def _load_versions(self, key: str) -> None:
        self._load_gen += 1
        gen = self._load_gen
        self.right.loading = True
        self.right.title = self._bucket_title()
        self._invalidate()
        res, versions = await list_versions(self.bucket, key, self.endpoint, self.env)
        if gen != self._load_gen:
            return
        self.right.loading = False
        if not res.ok:
            self.status_err = res.error
            self.versions_key = None
            self.right.mode = "list"
        else:
            self.right.mode = "versions"
            self.right.rows = rows_from_versions(versions)
            if len(versions) <= 1 and self._versioning.get(self.bucket) is False:
                self.status_msg = ("версий нет: на бакете выключено версионирование "
                                   "(put-bucket-versioning Status=Enabled)")
            else:
                self.status_msg = f"{len(versions)} версий"
        self.right.title = self._bucket_title()
        self.right.selection = 0
        self._invalidate()

    async def _op_transfer_batch(self, targets: list[Row], *, move: bool, from_local: bool) -> None:
        """Пакетное копирование/перемещение с модальным окном прогресса."""
        total = len(targets)
        verb = "Перемещение" if move else "Копирование"
        arrow = "↑" if from_local else "↓"
        prog = ProgressState(title=f"{verb} {arrow}", total=total,
                             bytes_total=sum(r.size for r in targets))
        self.progress = prog
        try:
            for row in targets:
                if prog.cancelled:
                    break
                prog.current = row.name
                self._invalidate()
                if from_local:
                    local: Path = row.payload
                    key = self.prefix + row.name
                    res = await run_aws(
                        build_upload_cmd(str(local), self.bucket, key, self.endpoint), self.env
                    )
                    if res.ok and move:
                        try:
                            os.remove(local)
                        except OSError as exc:
                            self.status_err = f"Не удалось удалить {local.name}: {exc}"
                            prog.errors += 1
                else:
                    entry: S3Entry = row.payload
                    target = self.local_path / Path(entry.key).name
                    res = await run_aws(
                        build_get_object_cmd(self.bucket, entry.key, str(target), self.endpoint),
                        self.env,
                    )
                    if res.ok and move:
                        res = await run_aws(
                            build_delete_cmd(self.bucket, entry.key, self.endpoint), self.env
                        )
                if not res.ok:
                    prog.errors += 1
                    self.status_err = res.error
                prog.done += 1
                prog.bytes_done += row.size
        finally:
            self.progress = None
        (self.left if from_local else self.right).clear_marks()
        self.reload_local()
        await self._load_bucket()
        if prog.cancelled:
            self.status_msg = f"{verb}: Отменено, сделано {prog.done}/{total}"
        else:
            done = total - prog.errors
            self.status_msg = f"{verb}: готово {done}/{total}" + (
                f", ошибок {prog.errors}" if prog.errors else "")
        self._invalidate()

    async def _op_download_version(
        self, key: str, target: Path, version_id: str, size: int = 0
    ) -> None:
        prog = ProgressState(title="Скачивание версии", current=target.name,
                             total=1, bytes_total=size)
        self.progress = prog
        try:
            cmd = build_get_object_cmd(self.bucket, key, str(target), self.endpoint, version_id)
            res = await run_aws(cmd, self.env)
        finally:
            self.progress = None
        if res.ok:
            self.status_msg = f"↓ сохранено: {target.name}"
            self.reload_local()
        else:
            self.status_err = res.error
        self._invalidate()

    async def _op_delete_batch(self, keys: list[tuple[str, str | None]]) -> None:
        total = len(keys)
        prog = ProgressState(title="Удаление", total=total)
        self.progress = prog
        try:
            for key, version_id in keys:
                if prog.cancelled:
                    break
                prog.current = Path(key).name
                self._invalidate()
                res = await run_aws(
                    build_delete_cmd(self.bucket, key, self.endpoint, version_id), self.env
                )
                if not res.ok:
                    prog.errors += 1
                    self.status_err = res.error
                prog.done += 1
        finally:
            self.progress = None
        if self.right.mode == "versions" and self.versions_key:
            await self._load_versions(self.versions_key)
        else:
            await self._load_bucket()
        if prog.cancelled:
            self.status_msg = f"Удаление: Отменено, сделано {prog.done}/{total}"
        else:
            self.status_msg = f"Удалено {total - prog.errors}/{total}" + (
                f", ошибок {prog.errors}" if prog.errors else "")
        self._invalidate()

    async def _op_restore(self, key: str, version_id: str) -> None:
        prog = ProgressState(title="Откат к версии", current=version_id[:8], total=1)
        self.progress = prog
        try:
            res = await run_aws(
                build_restore_cmd(self.bucket, key, version_id, self.endpoint), self.env
            )
        finally:
            self.progress = None
        if res.ok:
            self.status_msg = f"Откачено к версии {version_id[:8]} (создана новая latest-копия)"
            await self._load_versions(key)
        else:
            self.status_err = res.error
        self._invalidate()

    def run(self) -> None:
        async def _main():
            task = asyncio.ensure_future(self.app.run_async())
            self.app.create_background_task(self._load_bucket())
            await task

        asyncio.run(_main())


def browse_bucket(settings, prefix: str = "") -> None:
    """Точка входа: собирает окружение из настроек прогона и запускает браузер."""
    from .runner import _get_aws_env

    env, _profile = _get_aws_env(
        getattr(settings, "access_key", None),
        getattr(settings, "secret_key", None),
        getattr(settings, "aws_profile", None),
    )
    data_dir = Path(getattr(settings, "data_dir", ".") or ".")
    start_dir = data_dir if data_dir.exists() else Path.cwd()
    app = BucketBrowserApp(
        bucket=settings.bucket,
        endpoint=settings.endpoint,
        env=env,
        start_dir=start_dir,
        prefix=prefix,
    )
    app.run()
