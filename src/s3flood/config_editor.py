from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from prompt_toolkit.application import Application
from prompt_toolkit.filters import Condition
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout, Window
from prompt_toolkit.layout.containers import ConditionalContainer
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.layout.dimension import Dimension
from prompt_toolkit.styles import Style
from prompt_toolkit.widgets import TextArea


def _validate_size(value: str) -> bool:
    if value is None:
        return False
    text = value.strip().lower()
    if not text:
        return False
    if text == "auto":
        return True
    units = ("kb", "mb", "gb", "tb")
    for unit in units:
        if text.endswith(unit):
            try:
                float(text[:-2])
                return True
            except ValueError:
                return False
    try:
        float(text)
        return True
    except ValueError:
        return False


def _normalize_endpoint(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if not text.startswith(("http://", "https://")):
        text = f"http://{text}"
    scheme, rest = text.split("://", 1)
    if ":" not in rest:
        rest = f"{rest}:9080"
    return f"{scheme}://{rest}"


def _normalize_endpoint_list(values: Optional[Sequence[str]]) -> List[str]:
    if not values:
        return []
    result: List[str] = []
    for item in values:
        norm = _normalize_endpoint(item)
        if norm:
            result.append(norm)
    return result


def _format_bool(value: bool) -> str:
    return "yes" if bool(value) else "no"


@dataclass
class FieldSpec:
    key: str
    label: str
    kind: str
    choices: Optional[List[str]] = None
    allow_empty: bool = True
    min_value: Optional[float] = None


FIELD_DEFS: List[FieldSpec] = [
    FieldSpec("__connection__", "Режим подключения", "connection"),
    FieldSpec("bucket", "bucket", "text", allow_empty=False),
    FieldSpec("endpoint", "endpoint (single)", "text"),
    FieldSpec("endpoints", "endpoints (через запятую)", "list"),
    FieldSpec("endpoint_mode", "endpoint_mode", "choice", choices=["round-robin", "random"]),
    FieldSpec("client", "client", "choice", choices=["awscli"]),
    FieldSpec("access_key", "access_key", "text"),
    FieldSpec("secret_key", "secret_key", "password"),
    FieldSpec("aws_profile", "aws_profile", "text"),
    FieldSpec("threads", "threads", "int", allow_empty=False, min_value=1),
    FieldSpec("data_dir", "data_dir", "text", allow_empty=False),
    FieldSpec("report", "report", "text", allow_empty=False),
    FieldSpec("metrics", "metrics", "text", allow_empty=False),
    FieldSpec("infinite", "infinite", "bool"),
    FieldSpec("unique_remote_names", "unique_remote_names", "bool"),
    FieldSpec("mixed_read_ratio", "mixed_read_ratio", "float", min_value=0.0),
    FieldSpec("pattern", "pattern", "choice", choices=["sustained", "bursty"]),
    FieldSpec("burst_duration_sec", "burst_duration_sec", "float", min_value=0.0),
    FieldSpec("burst_intensity_multiplier", "burst_intensity_multiplier", "float", min_value=1.0),
    FieldSpec("order", "order", "choice", choices=["sequential", "random"]),
    FieldSpec("aws_cli_multipart_threshold", "aws_cli_multipart_threshold", "size"),
    FieldSpec("aws_cli_multipart_chunksize", "aws_cli_multipart_chunksize", "size"),
    FieldSpec(
        "aws_cli_max_concurrent_requests",
        "aws_cli_max_concurrent_requests",
        "int",
        min_value=1,
        allow_empty=True,
    ),
]


def build_default_config() -> Dict[str, Any]:
    return {
        "client": "awscli",
        "bucket": "your-bucket-name",
        "endpoint": "http://localhost:9000:9080",
        "endpoints": [],
        "endpoint_mode": "round-robin",
        "access_key": None,
        "secret_key": None,
        "aws_profile": None,
        "threads": 8,
        "data_dir": "./loadset/data",
        "report": "out.json",
        "metrics": "out.csv",
        "infinite": False,
        "unique_remote_names": False,
        "mixed_read_ratio": 0.7,
        "pattern": "sustained",
        "burst_duration_sec": 60.0,
        "burst_intensity_multiplier": 5.0,
        "order": "random",
        "aws_cli_multipart_threshold": None,
        "aws_cli_multipart_chunksize": None,
        "aws_cli_max_concurrent_requests": None,
    }


class ConfigEditorApp:
    def __init__(self, initial: Dict[str, Any], title: str):
        self.title = title
        self.state = build_default_config()
        self.state.update(initial or {})
        self.state["endpoints"] = _normalize_endpoint_list(self.state.get("endpoints"))
        self.initial = deepcopy(self.state)
        self.fields = FIELD_DEFS
        self.selection = 0
        self.message = "↑/↓ — выбор · Enter — редактировать · S — сохранить · Esc — отмена"
        self.editing = False
        self.active_field: Optional[FieldSpec] = None
        self.input_field = TextArea(height=1, prompt="> ", multiline=False)
        self.input_field.accept_handler = self._accept_input
        self.body_control = FormattedTextControl(self._render_lines)
        self.body_window = Window(
            content=self.body_control,
            height=Dimension(preferred=20),
            wrap_lines=False,
            always_hide_cursor=True,
        )
        self.input_container = ConditionalContainer(
            HSplit(
                [
                    Window(height=1, content=FormattedTextControl(self._render_input_label)),
                    self.input_field,
                ]
            ),
            filter=Condition(lambda: self.editing),
        )
        self.footer = Window(height=1, content=FormattedTextControl(self._render_footer))
        root = HSplit(
            [
                Window(
                    content=FormattedTextControl(lambda: FormattedText([("", self.title)])),
                    height=1,
                ),
                self.body_window,
                self.input_container,
                self.footer,
            ]
        )
        self.kb = KeyBindings()
        self.kb.add("up")(self._key_up)
        self.kb.add("down")(self._key_down)
        self.kb.add("enter")(self._key_enter)
        self.kb.add("escape")(self._key_escape)
        self.kb.add("c-c")(self._key_escape)
        self.kb.add("s")(self._key_save)
        self.kb.add("q")(self._key_cancel)
        self.app = Application(
            layout=Layout(root, focused_element=self.body_window),
            key_bindings=self.kb,
            mouse_support=False,
            full_screen=True,
            style=Style.from_dict(
                {
                    "status": "reverse",
                    "changed": "fg:ansiyellow",
                    "value": "fg:ansicyan",
                    "line.selected": "reverse",
                }
            ),
        )
        self.result: Optional[Dict[str, Any]] = None

    def run(self) -> Optional[Dict[str, Any]]:
        self.app.run()
        return self.result

    def _current_field(self) -> FieldSpec:
        return self.fields[self.selection]

    def _render_lines(self):
        fragments: List[tuple[str, str]] = []
        for idx, field in enumerate(self.fields):
            if field.key == "__connection__":
                value = self._connection_value()
                changed = self._connection_changed()
            else:
                value = self._format_value(field, self.state.get(field.key))
                changed = self._is_changed(field.key)
            marker = "*" if changed else " "
            label = f"{field.label:<30}"
            style_label = "class:line.selected" if idx == self.selection else ""
            value_style = "class:changed" if changed else "class:value"
            fragments.append((style_label, f"{marker} {label} "))
            fragments.append((value_style, value))
            fragments.append(("", "\n"))
        # Разделитель и подсказки управления в стиле остального меню
        fragments.append(("", "─" * 60 + "\n"))
        fragments.append(("", "[S] Сохранить конфиг    [Q] Вернуться в меню    [Esc] Отмена ввода\n"))
        return fragments

    def _format_value(self, field: FieldSpec, value: Any) -> str:
        if field.kind == "password":
            if value:
                return "•" * len(str(value))
            return "—"
        if field.kind == "bool":
            return _format_bool(bool(value))
        if field.kind == "list":
            if not value:
                return "—"
            if isinstance(value, list):
                return ", ".join(value)
            return str(value)
        if value is None or value == "":
            return "—"
        if field.kind in {"int", "float"}:
            return str(value)
        return str(value)

    def _connection_value(self) -> str:
        endpoints = self.state.get("endpoints") or []
        endpoint = self.state.get("endpoint")
        if endpoints:
            mode = self.state.get("endpoint_mode") or "round-robin"
            return f"cluster · {len(endpoints)} host(s), mode={mode}"
        return f"single · {endpoint or 'http://localhost:9000'}"

    def _connection_changed(self) -> bool:
        initial = bool(self.initial.get("endpoints"))
        current = bool(self.state.get("endpoints"))
        return initial != current

    def _render_input_label(self):
        if not self.editing or not self.active_field:
            return FormattedText([("", "")])
        return FormattedText(
            [
                (
                    "",
                    f"Введите значение для «{self.active_field.label}» и нажмите Enter (Esc — отмена)",
                )
            ]
        )

    def _render_footer(self):
        info = self.message
        return FormattedText([("class:status", info)])

    def _is_changed(self, key: str) -> bool:
        return self.state.get(key) != self.initial.get(key)

    def _refresh(self):
        self.app.invalidate()

    def _key_up(self, event):
        if self.editing:
            return
        if self.selection > 0:
            self.selection -= 1
            self._refresh()

    def _key_down(self, event):
        if self.editing:
            return
        if self.selection < len(self.fields) - 1:
            self.selection += 1
            self._refresh()

    def _key_enter(self, event):
        if self.editing:
            self._submit_input()
        else:
            self._activate_field()

    def _key_escape(self, event):
        if self.editing:
            self._cancel_input()
        else:
            self.result = None
            event.app.exit()

    def _key_save(self, event):
        if self.editing:
            return
        self.result = self._prepare_result()
        event.app.exit()

    def _key_cancel(self, event):
        if self.editing:
            return
        self.result = None
        event.app.exit()

    def _activate_field(self):
        field = self._current_field()
        if field.key == "__connection__":
            self._toggle_connection_mode()
            return
        if field.kind == "bool":
            current = bool(self.state.get(field.key))
            self.state[field.key] = not current
            self._refresh()
            return
        if field.kind == "choice" and field.choices:
            current = self.state.get(field.key)
            if current not in field.choices:
                self.state[field.key] = field.choices[0]
            else:
                idx = field.choices.index(current)
                self.state[field.key] = field.choices[(idx + 1) % len(field.choices)]
            self._refresh()
            return
        if field.kind == "list":
            text = ", ".join(self.state.get(field.key) or [])
        else:
            value = self.state.get(field.key)
            text = "" if value is None else str(value)
        self._begin_input(field, text)

    def _begin_input(self, field: FieldSpec, text: str):
        self.editing = True
        self.active_field = field
        self.input_field.text = text or ""
        self.input_field.buffer.cursor_position = len(self.input_field.text)
        self.app.layout.focus(self.input_field)
        self._refresh()

    def _cancel_input(self):
        self.editing = False
        self.active_field = None
        self.input_field.text = ""
        self.app.layout.focus(self.body_window)
        self._refresh()

    def _submit_text(self, raw: str):
        field = self.active_field
        if not field:
            return
        text = raw.strip()
        if not text and not field.allow_empty:
            self.message = "Значение не может быть пустым."
            self._refresh()
            return
        if field.kind == "int":
            if not text:
                self.state[field.key] = None
            else:
                try:
                    value = int(float(text))
                except ValueError:
                    self.message = "Введите целое число."
                    self._refresh()
                    return
                if field.min_value is not None and value < field.min_value:
                    self.message = f"Минимум {field.min_value}"
                    self._refresh()
                    return
                self.state[field.key] = value
        elif field.kind == "float":
            if not text:
                self.state[field.key] = None
            else:
                try:
                    value = float(text)
                except ValueError:
                    self.message = "Введите число."
                    self._refresh()
                    return
                if field.min_value is not None and value < field.min_value:
                    self.message = f"Минимум {field.min_value}"
                    self._refresh()
                    return
                self.state[field.key] = value
        elif field.kind == "size":
            if text and not _validate_size(text):
                self.message = "Неверный формат (пример: 64MB, 1GB, 1024)."
                self._refresh()
                return
            self.state[field.key] = text or None
        elif field.kind == "list":
            if not text:
                self.state[field.key] = []
            else:
                parts = [p.strip() for p in text.split(",")]
                self.state[field.key] = [p for p in (_normalize_endpoint(v) for v in parts) if p]
                if not self.state[field.key]:
                    self.state[field.key] = []
        else:
            self.state[field.key] = text or None
        self.message = "Изменение сохранено."
        self.editing = False
        self.active_field = None
        self.input_field.text = ""
        self.app.layout.focus(self.body_window)
        self._refresh()

    def _submit_input(self):
        if not self.active_field:
            return
        self._submit_text(self.input_field.text)

    def _accept_input(self, buf) -> bool:
        self._submit_input()
        return False

    def _toggle_connection_mode(self):
        endpoints = self.state.get("endpoints") or []
        if endpoints:
            self.state["endpoint"] = endpoints[0] if endpoints else self.state.get("endpoint")
            self.state["endpoints"] = []
            self.state["endpoint_mode"] = None
            self.message = "Переключено на одиночный endpoint."
        else:
            base = self.state.get("endpoint") or "http://localhost:9000:9080"
            self.state["endpoints"] = [_normalize_endpoint(base) or base]
            self.state["endpoint"] = None
            if not self.state.get("endpoint_mode"):
                self.state["endpoint_mode"] = "round-robin"
            self.message = "Переключено на режим кластера."
        self._refresh()

    def _prepare_result(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for field in self.fields:
            if field.key == "__connection__":
                continue
            value = self.state.get(field.key)
            if field.kind == "list" and not value:
                result[field.key] = None
                continue
            result[field.key] = value
        if not result.get("endpoints"):
            result["endpoints"] = None
            if not result.get("endpoint"):
                result["endpoint"] = "http://localhost:9000:9080"
        if not result.get("endpoint_mode"):
            result["endpoint_mode"] = None
        return result


def edit_config_interactively(data: Dict[str, Any], title: str) -> Optional[Dict[str, Any]]:
    app = ConfigEditorApp(data, title)
    return app.run()


