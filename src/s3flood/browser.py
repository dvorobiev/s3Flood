"""Двухпанельный TUI-браузер бакета (стиль Midnight Commander).

Слева — локальная файловая система, справа — бакет из конфига.
Enter на объекте бакета открывает его версии как «папку»; F5 копирует между
панелями (upload/download, в режиме версий — конкретную версию), F6
восстанавливает версию, F8 удаляет. Построен на prompt_toolkit по образцу
ConfigEditorApp — тот же стек и стилистика, что и остальной TUI s3flood.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from prompt_toolkit.application import Application
from prompt_toolkit.data_structures import Point
from prompt_toolkit.filters import Condition
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout, VSplit, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.styles import Style

from .executor import format_bytes
from .s3browser_io import (
    S3Entry,
    S3Version,
    build_delete_cmd,
    build_get_object_cmd,
    build_restore_cmd,
    build_upload_cmd,
    list_prefix,
    list_versions,
    run_aws,
)

SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


@dataclass
class Row:
    name: str
    is_dir: bool = False
    size: int = 0
    meta: str = ""
    payload: object = None


@dataclass
class Panel:
    title: str
    rows: list[Row] = field(default_factory=list)
    selection: int = 0
    loading: bool = False
    # bucket-панель: "list" — листинг префикса, "versions" — версии объекта
    mode: str = "list"

    def selected(self) -> Optional[Row]:
        if 0 <= self.selection < len(self.rows):
            return self.rows[self.selection]
        return None

    def clamp(self) -> None:
        self.selection = max(0, min(self.selection, len(self.rows) - 1))


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


def rows_from_entries(entries: list[S3Entry], prefix: str) -> list[Row]:
    """Строки бакет-панели из листинга префикса."""
    rows: list[Row] = []
    if prefix:
        rows.append(Row(name="..", is_dir=True))
    for e in entries:
        if e.is_dir:
            rows.append(Row(name=e.name, is_dir=True, payload=e))
        else:
            rows.append(Row(
                name=e.name, size=e.size,
                meta=e.last_modified[:16].replace("T", " "),
                payload=e,
            ))
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
    """Чистый рендер панели в список (style, text)-строк."""
    def cut(text: str) -> str:
        return text[:width] if len(text) > width else text

    header_style = "class:panel.title.focused" if focused else "class:panel.title"
    lines: list[tuple[str, str]] = [(header_style, cut(f" {panel.title} ".ljust(width)) + "\n")]

    if panel.loading:
        frame = SPINNER[int(time.time() * 8) % len(SPINNER)]
        lines.append(("class:loading", cut(f" {frame} загрузка…") + "\n"))
        return lines

    for idx, row in enumerate(panel.rows):
        is_sel = idx == panel.selection
        cursor = "»" if is_sel and focused else " "
        size_disp = "" if row.is_dir else format_bytes(row.size)
        name_width = max(width - 30, 12)
        text = f"{cursor} {row.name:<{name_width}.{name_width}} {size_disp:>9} {row.meta}"
        style = "class:row.dir" if row.is_dir else "class:row"
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

        self.left = Panel(title=str(self.local_path), rows=build_local_rows(self.local_path))
        self.right = Panel(title=self._bucket_title(), loading=True)
        self.focus_right = True
        self.status_msg = ""
        self.status_err = ""
        # (текст подтверждения, корутина-действие) для инлайн y/n
        self.confirm: Optional[tuple[str, object]] = None

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

        body = VSplit([
            self.left_window,
            Window(width=1, char="│", style="class:separator"),
            self.right_window,
        ])
        status = Window(height=1, content=FormattedTextControl(self._render_status))
        keybar = Window(height=1, content=FormattedTextControl(self._render_keybar),
                        style="class:keybar")
        root = HSplit([body, status, keybar])

        kb = KeyBindings()
        no_confirm = Condition(lambda: self.confirm is None)
        in_confirm = Condition(lambda: self.confirm is not None)
        kb.add("tab", filter=no_confirm)(self._key_tab)
        kb.add("up", filter=no_confirm)(self._key_up)
        kb.add("down", filter=no_confirm)(self._key_down)
        kb.add("pageup", filter=no_confirm)(self._key_pgup)
        kb.add("pagedown", filter=no_confirm)(self._key_pgdn)
        kb.add("enter", filter=no_confirm)(self._key_enter)
        kb.add("backspace", filter=no_confirm)(self._key_back)
        kb.add("f5", filter=no_confirm)(self._key_copy)
        kb.add("f6", filter=no_confirm)(self._key_restore)
        kb.add("f8", filter=no_confirm)(self._key_delete)
        kb.add("r", filter=no_confirm)(self._key_refresh)
        kb.add("q", filter=no_confirm)(self._key_quit)
        kb.add("f10")(self._key_quit)
        kb.add("c-c")(self._key_quit)
        kb.add("y", filter=in_confirm)(self._key_confirm_yes)
        kb.add("n", filter=in_confirm)(self._key_confirm_no)
        kb.add("escape", filter=in_confirm)(self._key_confirm_no)

        self.app = Application(
            layout=Layout(root, focused_element=self.right_window),
            key_bindings=kb,
            full_screen=True,
            mouse_support=False,
            refresh_interval=0.2,
            input=input,
            output=output,
            style=Style.from_dict({
                "panel.title": "fg:#6c6c6c",
                "panel.title.focused": "reverse bold",
                "row": "",
                "row.dir": "fg:#00d7ff",
                "row.selected": "reverse",
                "row.cursor": "underline",
                "separator": "fg:#585858",
                "status": "",
                "status.error": "fg:ansired bold",
                "status.confirm": "fg:ansiyellow bold",
                "keybar": "reverse",
                "loading": "fg:ansiyellow",
                "dim": "fg:#6c6c6c",
            }),
        )

    # ---------- рендер ----------

    def _bucket_title(self) -> str:
        if self.versions_key:
            return f"{self.bucket}:/{self.versions_key} ⊙ версии"
        return f"{self.bucket}:/{self.prefix}"

    def _panel_width(self) -> int:
        try:
            cols = self.app.output.get_size().columns
        except Exception:
            cols = 80
        return max(cols // 2 - 1, 20)

    def _fragments(self, panel: Panel, focused: bool):
        return render_panel_lines(panel, self._panel_width(), focused)

    @staticmethod
    def _cursor_point(panel: Panel) -> Point:
        # Курсор не должен указывать за пределы отрендеренных строк:
        # при loading/пустой панели контент короче, чем selection+1
        if panel.loading or not panel.rows:
            return Point(x=0, y=0)
        return Point(x=0, y=min(panel.selection, len(panel.rows) - 1) + 1)

    def _render_status(self):
        if self.confirm:
            return [("class:status.confirm", f" {self.confirm[0]} (y/n) ")]
        if self.status_err:
            return [("class:status.error", f" ✗ {self.status_err} ")]
        return [("class:status", f" {self.endpoint} · {self.status_msg} ")]

    def _render_keybar(self):
        keys = " Tab Панель  Enter Открыть  F5 Копировать  F6 Восстановить  F8 Удалить  r Обновить  q Выход"
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
        self._spawn(self._load_bucket())

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
        if self.right.mode == "versions":
            if row.name == "..":
                self.versions_key = None
                self.right.mode = "list"
                self._spawn(self._load_bucket())
            return
        if row.name == "..":
            self._go_prefix_up()
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
        else:
            self._go_prefix_up()

    def _go_prefix_up(self) -> None:
        if not self.prefix:
            return
        parts = self.prefix.rstrip("/").split("/")
        self.prefix = "/".join(parts[:-1]) + "/" if len(parts) > 1 else ""
        self._spawn(self._load_bucket())

    # ---------- операции ----------

    def _key_copy(self, event) -> None:
        row = self._active().selected()
        if row is None or row.name == "..":
            return
        if not self.focus_right:
            if row.is_dir:
                self.status_err = "Копирование каталогов не поддерживается (v1)"
                return
            local = row.payload
            key = self.prefix + row.name
            self._spawn(self._op_upload(local, key))
        elif self.right.mode == "versions":
            if isinstance(row.payload, S3Version) and not row.payload.is_delete_marker:
                v = row.payload
                base = Path(self.versions_key or "object").name
                target = self.local_path / f"{base}.{v.version_id[:8]}"
                self._spawn(self._op_download(self.versions_key, target, v.version_id))
        else:
            if row.is_dir:
                self.status_err = "Копирование префиксов не поддерживается (v1)"
                return
            entry = row.payload
            target = self.local_path / Path(entry.key).name
            self._spawn(self._op_download(entry.key, target, None))

    def _key_delete(self, event) -> None:
        if not self.focus_right:
            self.status_err = "Удаление локальных файлов из браузера отключено"
            return
        row = self.right.selected()
        if row is None or row.name == "..":
            return
        if self.right.mode == "versions" and isinstance(row.payload, S3Version):
            v = row.payload
            prompt = f"Удалить версию {v.version_id[:8]} объекта {self.versions_key}?"
            self.confirm = (prompt, self._op_delete(self.versions_key, v.version_id))
        elif isinstance(row.payload, S3Entry) and not row.is_dir:
            prompt = f"Удалить объект {row.payload.key}?"
            self.confirm = (prompt, self._op_delete(row.payload.key, None))

    def _key_restore(self, event) -> None:
        if not (self.focus_right and self.right.mode == "versions"):
            self.status_err = "F6 работает в режиме версий (Enter на объекте)"
            return
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
        prompt = f"Восстановить {self.versions_key} к версии {v.version_id[:8]}?"
        self.confirm = (prompt, self._op_restore(self.versions_key, v.version_id))

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

    async def _load_bucket(self) -> None:
        self.right.loading = True
        self.right.title = self._bucket_title()
        self._invalidate()
        res, entries = await list_prefix(self.bucket, self.prefix, self.endpoint, self.env)
        self.right.loading = False
        if not res.ok:
            self.status_err = res.error
            self.right.rows = []
        else:
            self.right.mode = "list"
            self.right.rows = rows_from_entries(entries, self.prefix)
            self.status_msg = f"{len(entries)} элементов"
        self.right.title = self._bucket_title()
        self.right.selection = 0
        self._invalidate()

    async def _load_versions(self, key: str) -> None:
        self.right.loading = True
        self.right.title = self._bucket_title()
        self._invalidate()
        res, versions = await list_versions(self.bucket, key, self.endpoint, self.env)
        self.right.loading = False
        if not res.ok:
            self.status_err = res.error
            self.versions_key = None
            self.right.mode = "list"
        else:
            self.right.mode = "versions"
            self.right.rows = rows_from_versions(versions)
            self.status_msg = f"{len(versions)} версий"
        self.right.title = self._bucket_title()
        self.right.selection = 0
        self._invalidate()

    async def _op_upload(self, local: Path, key: str) -> None:
        self.status_msg = f"↑ {key}…"
        self._invalidate()
        res = await run_aws(build_upload_cmd(str(local), self.bucket, key, self.endpoint), self.env)
        if res.ok:
            self.status_msg = f"↑ загружено: {key}"
            await self._load_bucket()
        else:
            self.status_err = res.error
        self._invalidate()

    async def _op_download(self, key: str, target: Path, version_id: str | None) -> None:
        self.status_msg = f"↓ {target.name}…"
        self._invalidate()
        cmd = build_get_object_cmd(self.bucket, key, str(target), self.endpoint, version_id)
        res = await run_aws(cmd, self.env)
        if res.ok:
            self.status_msg = f"↓ сохранено: {target.name}"
            self.reload_local()
        else:
            self.status_err = res.error
        self._invalidate()

    async def _op_delete(self, key: str, version_id: str | None) -> None:
        res = await run_aws(build_delete_cmd(self.bucket, key, self.endpoint, version_id), self.env)
        if res.ok:
            self.status_msg = "Удалено"
            if self.right.mode == "versions" and self.versions_key:
                await self._load_versions(self.versions_key)
            else:
                await self._load_bucket()
        else:
            self.status_err = res.error
        self._invalidate()

    async def _op_restore(self, key: str, version_id: str) -> None:
        res = await run_aws(build_restore_cmd(self.bucket, key, version_id, self.endpoint), self.env)
        if res.ok:
            self.status_msg = f"Восстановлено к версии {version_id[:8]}"
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
