# Браузер v3 (колонки NC + модальный прогресс), датасет приложения, discover_configs — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Колонки в стиле Norton Commander и модальное окно прогресса в TUI-браузере бакета; путь к датасету на уровне приложения (`.s3flood.yml`); обнаружение конфигов по содержимому вместо префикса имени.

**Architecture:** Все изменения UI — в `browser.py` (чистые функции рендера + prompt_toolkit `FloatContainer`). Настройки приложения — новый маленький модуль `app_settings.py` (плоский YAML `.s3flood.yml` в cwd). Обнаружение конфигов и приоритет `data_dir` — в `config.py`. Спека: `docs/superpowers/specs/2026-07-06-browser-columns-progress-app-dataset-configs-design.md`.

**Tech Stack:** Python 3.10+, prompt_toolkit, rich, questionary, PyYAML, pytest, ruff.

## Global Constraints

- Только существующий TUI-стек: prompt_toolkit / rich / questionary — никаких новых зависимостей.
- Ветка: `feature/bucket-browser`. Коммиты — на русском, стиль `feat:` / `test:` как в истории.
- В rich-выводе не использовать квадратные скобки в тексте (rich съедает `[текст]` как разметку).
- Все команды — из корня репозитория, venv: `.venv/bin/python -m pytest`, линт: `.venv/bin/ruff check src/`.
- Файл настроек приложения: имя строго `.s3flood.yml`, ключ `dataset_dir`.

---

### Task 1: Колонки Norton Commander в панелях браузера

**Files:**
- Modify: `src/s3flood/browser.py:154-184` (`render_panel_lines`), `src/s3flood/browser.py:259-274` (Style)
- Test: `tests/test_browser_render.py`

**Interfaces:**
- Produces: `format_columns(name: str, size: str, meta: str, width: int) -> str` и константа `COLUMN_HEADERS: dict[str, tuple[str, str, str]]` в `browser.py`. Сигнатура `render_panel_lines(panel, width, focused)` не меняется; вторая строка результата (после титула) — заголовки колонок со стилем `class:panel.columns` (кроме loading-состояния).

- [ ] **Step 1: Написать падающие тесты**

Добавить в `tests/test_browser_render.py` класс:

```python
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
        style, text = lines[1]
        assert "Имя" in text and "Размер" in text and "Дата" in text
        assert "panel.columns" in style

    def test_header_row_for_versions_mode(self):
        lines = render_panel_lines(self.make_panel(mode="versions"), width=60, focused=True)
        assert "Версия" in lines[1][1]

    def test_header_row_for_buckets_mode(self):
        lines = render_panel_lines(self.make_panel(mode="buckets"), width=60, focused=True)
        assert "Бакет" in lines[1][1]

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
        header = lines[1][1]
        file_line = [t for _s, t in lines if "file.bin" in t][0]
        assert [i for i, ch in enumerate(header) if ch == "│"] == \
               [i for i, ch in enumerate(file_line) if ch == "│"]
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `.venv/bin/python -m pytest tests/test_browser_render.py::TestColumns -v`
Expected: FAIL (нет строки заголовков, нет `│` в строках).

- [ ] **Step 3: Реализация в `browser.py`**

После определения `SPINNER` добавить:

```python
# Заголовки колонок панели по режимам (имя, размер, дата/метки)
COLUMN_HEADERS: dict[str, tuple[str, str, str]] = {
    "list": ("Имя", "Размер", "Дата"),
    "versions": ("Версия", "Размер", "Дата / метки"),
    "buckets": ("Бакет", "", ""),
}


def format_columns(name: str, size: str, meta: str, width: int) -> str:
    """Одна строка панели: имя │ размер │ мета — с фиксированной сеткой колонок."""
    name_w = max(width - 33, 12)
    return f"{name:<{name_w}.{name_w}} │{size:>9} │ {meta}"
```

В `render_panel_lines` тело после титула становится таким (полностью):

```python
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
```

(строка `name_width = max(width - 32, 12)` и старая f-строка с `{row.name:<{name_width}...}` при этом исчезают).

Важно: строка заголовков сдвигает контент на 1 строку — обновить `_cursor_point` в `BucketBrowserApp`: `return Point(x=0, y=min(panel.selection, len(panel.rows) - 1) + 2)` (было `+ 1`; титул + заголовки колонок).

В `Style.from_dict` добавить: `"panel.columns": "fg:#6c6c6c"`.

- [ ] **Step 4: Прогнать все тесты рендера**

Run: `.venv/bin/python -m pytest tests/test_browser_render.py -v`
Expected: PASS все, включая старые (`test_width_truncation`, `test_marked_rows_have_star_and_style` — они не привязаны к точному формату строки).
Если старый тест `test_header_and_rows` смотрит на `lines[0]` — он проверяет титул, индекс не сдвинулся.

- [ ] **Step 5: Commit**

```bash
git add src/s3flood/browser.py tests/test_browser_render.py
git commit -m "feat: колонки панелей браузера в стиле Norton Commander"
```

---

### Task 2: ProgressState и чистый рендер окна прогресса

**Files:**
- Modify: `src/s3flood/browser.py` (новые dataclass и функции рядом с `Row`/`render_panel_lines`)
- Test: `tests/test_browser_progress.py` (создать)

**Interfaces:**
- Produces:
  - `ProgressState` — dataclass с полями `title: str`, `current: str = ""`, `done: int = 0`, `total: int = 0`, `bytes_done: int = 0`, `bytes_total: int = 0`, `errors: int = 0`, `cancelled: bool = False`.
  - `make_bar(frac: float, width: int) -> str` — строка из `█`/`░` длиной ровно `width`.
  - `render_progress_lines(p: ProgressState, width: int) -> list[tuple[str, str]]` — содержимое окна прогресса (без рамки), формат как у `render_panel_lines`.

- [ ] **Step 1: Написать падающие тесты**

Создать `tests/test_browser_progress.py`:

```python
from s3flood.browser import ProgressState, make_bar, render_progress_lines


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
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `.venv/bin/python -m pytest tests/test_browser_progress.py -v`
Expected: FAIL с `ImportError: cannot import name 'ProgressState'`.

- [ ] **Step 3: Реализация в `browser.py`**

После dataclass `Panel` добавить:

```python
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
```

- [ ] **Step 4: Прогнать тесты**

Run: `.venv/bin/python -m pytest tests/test_browser_progress.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/s3flood/browser.py tests/test_browser_progress.py
git commit -m "feat: ProgressState и рендер окна прогресса браузера"
```

---

### Task 3: Модальное окно прогресса — интеграция в приложение и операции

**Files:**
- Modify: `src/s3flood/browser.py` (`BucketBrowserApp.__init__`, все `_op_*`, keybindings)
- Test: `tests/test_browser_progress.py` (дополнить)

**Interfaces:**
- Consumes: `ProgressState`, `render_progress_lines` из Task 2.
- Produces: `BucketBrowserApp.progress: Optional[ProgressState]` — не None, пока идёт операция; Esc во время операции ставит `progress.cancelled = True`. Сигнатуры `_op_*` не меняются, кроме `_op_download_version(key, target, version_id, size: int = 0)`.

- [ ] **Step 1: Написать падающий тест на отмену**

Дополнить `tests/test_browser_progress.py`:

```python
import asyncio
from pathlib import Path

from prompt_toolkit.input import DummyInput
from prompt_toolkit.output import DummyOutput

import s3flood.browser as browser_mod
from s3flood.browser import BucketBrowserApp, Row
from s3flood.s3browser_io import OpResult


def make_app(tmp_path):
    return BucketBrowserApp(
        bucket="b", endpoint="http://h:9000", env={},
        start_dir=tmp_path, input=DummyInput(), output=DummyOutput(),
    )


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
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `.venv/bin/python -m pytest tests/test_browser_progress.py::TestCancelBatch -v`
Expected: FAIL — `AttributeError: 'BucketBrowserApp' object has no attribute 'progress'` (или `'NoneType' has no attribute 'cancelled'`).

- [ ] **Step 3: Интеграция float-окна и блокировки клавиш**

В `browser.py`:

1. Импорты: расширить `from prompt_toolkit.layout import HSplit, Layout, VSplit, Window` → добавить `ConditionalContainer, Float, FloatContainer`; добавить `from prompt_toolkit.widgets import Frame`.

2. В `__init__` после `self.confirm = ...` добавить `self.progress: Optional[ProgressState] = None`.

3. Обернуть корневой контейнер во FloatContainer (заменить сборку `root`):

```python
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
```

и метод:

```python
    def _render_progress(self):
        if self.progress is None:
            return []
        return render_progress_lines(self.progress, width=52)
```

4. Keybindings: заменить `no_confirm = Condition(lambda: self.confirm is None)` на

```python
        active = Condition(lambda: self.confirm is None and self.progress is None)
        in_confirm = Condition(lambda: self.confirm is not None)
        in_progress = Condition(lambda: self.progress is not None)
```

во всех `kb.add(..., filter=no_confirm)` использовать `filter=active`; добавить:

```python
        kb.add("escape", filter=in_progress)(self._key_cancel_op)
```

и метод:

```python
    def _key_cancel_op(self, event) -> None:
        if self.progress is not None:
            self.progress.cancelled = True
```

5. Стили: добавить в `Style.from_dict`:
`"progress.file": "bold"`, `"progress.bar": ""`, `"progress.hint": "fg:#6c6c6c"`, `"frame.border": "fg:#00d7ff"`.

- [ ] **Step 4: Перевести операции на ProgressState**

`_op_transfer_batch` — целиком:

```python
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
```

`_op_delete_batch` — целиком:

```python
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
```

`_op_download_version` — добавить параметр `size: int = 0`; обернуть:

```python
        prog = ProgressState(title="Скачивание версии", current=target.name,
                             total=1, bytes_total=size)
        self.progress = prog
        try:
            cmd = build_get_object_cmd(self.bucket, key, str(target), self.endpoint, version_id)
            res = await run_aws(cmd, self.env)
        finally:
            self.progress = None
```

в `_key_copy` (версии) передавать `size=v.size`.

`_op_restore` — аналогично: `ProgressState(title="Откат к версии", current=version_id[:8], total=1)` вокруг существующего `run_aws`, `finally: self.progress = None`.

- [ ] **Step 5: Прогнать все тесты и линт**

Run: `.venv/bin/python -m pytest tests/ -v && .venv/bin/ruff check src/`
Expected: PASS все (112 старых + новые), ruff чистый.

- [ ] **Step 6: Ручная проверка (smoke)**

Против moto (как в memory/tasks за 2026-07-03): `.venv/bin/python -m moto.server -p 19100` в фоне, создать бакет, запустить `s3flood browse --endpoint http://127.0.0.1:19100 --bucket verbucket --access-key x --secret-key y`, скопировать 3+ файла с F5 — окно прогресса появляется по центру, Esc отменяет. Остановить moto.

- [ ] **Step 7: Commit**

```bash
git add src/s3flood/browser.py tests/test_browser_progress.py
git commit -m "feat: модальное окно прогресса файловых операций браузера (Esc — отмена)"
```

---

### Task 4: Модуль настроек приложения `.s3flood.yml`

**Files:**
- Create: `src/s3flood/app_settings.py`
- Test: `tests/test_app_settings.py` (создать)

**Interfaces:**
- Produces:
  - `APP_SETTINGS_FILE = ".s3flood.yml"`
  - `load_app_settings(cwd: Path | None = None) -> dict`
  - `save_app_settings(updates: dict, cwd: Path | None = None) -> None` — merge с существующим содержимым
  - `get_dataset_dir(cwd: Path | None = None) -> Optional[str]`

- [ ] **Step 1: Написать падающие тесты**

Создать `tests/test_app_settings.py`:

```python
from s3flood.app_settings import (
    APP_SETTINGS_FILE,
    get_dataset_dir,
    load_app_settings,
    save_app_settings,
)


class TestAppSettings:
    def test_missing_file_returns_empty(self, tmp_path):
        assert load_app_settings(tmp_path) == {}
        assert get_dataset_dir(tmp_path) is None

    def test_save_and_load_roundtrip(self, tmp_path):
        save_app_settings({"dataset_dir": "/data/set"}, tmp_path)
        assert load_app_settings(tmp_path) == {"dataset_dir": "/data/set"}
        assert get_dataset_dir(tmp_path) == "/data/set"

    def test_save_merges_existing_keys(self, tmp_path):
        save_app_settings({"dataset_dir": "/a"}, tmp_path)
        save_app_settings({"future_key": 1}, tmp_path)
        data = load_app_settings(tmp_path)
        assert data["dataset_dir"] == "/a" and data["future_key"] == 1

    def test_corrupt_file_returns_empty(self, tmp_path):
        (tmp_path / APP_SETTINGS_FILE).write_text("- just\n- a list\n")
        assert load_app_settings(tmp_path) == {}
        (tmp_path / APP_SETTINGS_FILE).write_text("{{invalid yaml")
        assert load_app_settings(tmp_path) == {}

    def test_filename_is_dotfile(self):
        assert APP_SETTINGS_FILE == ".s3flood.yml"
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `.venv/bin/python -m pytest tests/test_app_settings.py -v`
Expected: FAIL с `ModuleNotFoundError: No module named 's3flood.app_settings'`.

- [ ] **Step 3: Реализация**

Создать `src/s3flood/app_settings.py`:

```python
"""Настройки приложения: файл .s3flood.yml в рабочей папке.

Хранит параметры уровня приложения (не прогона): сейчас — путь к датасету
(dataset_dir), записываемый мастером создания датасета.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml

APP_SETTINGS_FILE = ".s3flood.yml"


def load_app_settings(cwd: Path | None = None) -> dict:
    path = (cwd or Path.cwd()) / APP_SETTINGS_FILE
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return {}
    return data if isinstance(data, dict) else {}


def save_app_settings(updates: dict, cwd: Path | None = None) -> None:
    path = (cwd or Path.cwd()) / APP_SETTINGS_FILE
    data = load_app_settings(cwd)
    data.update(updates)
    path.write_text(
        yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )


def get_dataset_dir(cwd: Path | None = None) -> Optional[str]:
    value = load_app_settings(cwd).get("dataset_dir")
    return str(value) if value else None
```

- [ ] **Step 4: Прогнать тесты**

Run: `.venv/bin/python -m pytest tests/test_app_settings.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/s3flood/app_settings.py tests/test_app_settings.py
git commit -m "feat: настройки приложения .s3flood.yml (dataset_dir)"
```

---

### Task 5: Приоритет data_dir: CLI → .s3flood.yml → ./data; предупреждение про data_dir в конфиге

**Files:**
- Modify: `src/s3flood/config.py:125-253` (`resolve_run_settings`)
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: `get_dataset_dir` из Task 4.
- Produces: `RunSettings.data_dir` резолвится по цепочке CLI → app settings → `"./data"`; `data_dir` из YAML-конфига игнорируется, при его наличии в stderr печатается предупреждение. Поле `RunConfigModel.data_dir` остаётся (нужно для детекта и предупреждения).

- [ ] **Step 1: Написать падающие тесты**

Добавить в `tests/test_config.py`:

```python
from argparse import Namespace

from s3flood.app_settings import save_app_settings
from s3flood.config import RunConfigModel, resolve_run_settings


def _min_args(**kw):
    return Namespace(profile="write", endpoint="http://h:9000", bucket="b", **kw)


class TestDataDirPriority:
    def test_default_when_no_sources(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        settings = resolve_run_settings(_min_args(), None)
        assert settings.data_dir == "./data"

    def test_app_settings_used(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        save_app_settings({"dataset_dir": "/srv/dataset"}, tmp_path)
        settings = resolve_run_settings(_min_args(), None)
        assert settings.data_dir == "/srv/dataset"

    def test_cli_overrides_app_settings(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        save_app_settings({"dataset_dir": "/srv/dataset"}, tmp_path)
        settings = resolve_run_settings(_min_args(data_dir="/cli/data"), None)
        assert settings.data_dir == "/cli/data"

    def test_config_data_dir_ignored_with_warning(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        config = RunConfigModel(data_dir="/from/config")
        settings = resolve_run_settings(_min_args(), config)
        assert settings.data_dir == "./data"
        assert "data_dir" in capsys.readouterr().err
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `.venv/bin/python -m pytest tests/test_config.py::TestDataDirPriority -v`
Expected: FAIL — `test_app_settings_used` и `test_config_data_dir_ignored_with_warning` (сейчас конфиг побеждает, app settings не читаются).

- [ ] **Step 3: Реализация в `config.py`**

Добавить импорты: `import sys` и `from .app_settings import APP_SETTINGS_FILE, get_dataset_dir`.

В `resolve_run_settings` заменить строку `data_dir = pick("data_dir", default="./data")` на:

```python
    # data_dir: датасет задаётся приложением (.s3flood.yml), не конфигом прогона
    cli_data_dir = getattr(cli_args, "data_dir", None)
    config_data_dir = config.data_dir if config is not None else None
    if config_data_dir:
        print(
            f"предупреждение: data_dir в конфиге игнорируется — датасет задаётся "
            f"приложением ({APP_SETTINGS_FILE} или --data-dir)",
            file=sys.stderr,
        )
    data_dir = cli_data_dir or get_dataset_dir() or "./data"
```

- [ ] **Step 4: Прогнать тесты**

Run: `.venv/bin/python -m pytest tests/test_config.py -v`
Expected: PASS все (старые тесты test_config.py не задают data_dir в конфиге — проверить; если какой-то задаёт, обновить его ожидание на "./data" + предупреждение).

- [ ] **Step 5: Commit**

```bash
git add src/s3flood/config.py tests/test_config.py
git commit -m "feat: data_dir из настроек приложения, data_dir в конфиге игнорируется"
```

---

### Task 6: discover_configs — конфиги по содержимому, без префикса имени

**Files:**
- Modify: `src/s3flood/config.py` (новая функция), `src/s3flood/interactive.py:167,604,674,1054` (4 glob-места), `src/s3flood/interactive.py:534` (дефолт имени)
- Test: `tests/test_config.py`

**Interfaces:**
- Produces: `discover_configs(cwd: Path) -> list[Path]` в `config.py` — отсортированный список YAML-файлов рабочей папки, похожих на конфиги прогона.

- [ ] **Step 1: Написать падающие тесты**

Добавить в `tests/test_config.py`:

```python
from pathlib import Path

from s3flood.config import discover_configs


class TestDiscoverConfigs:
    def test_finds_flat_and_run_section_configs(self, tmp_path):
        (tmp_path / "kazan.yml").write_text("endpoint: http://h:9000\nbucket: b\n")
        (tmp_path / "tape.yaml").write_text("run:\n  bucket: b\n")
        (tmp_path / "config.old.yaml").write_text("profile: write\n")
        found = {p.name for p in discover_configs(tmp_path)}
        assert found == {"kazan.yml", "tape.yaml", "config.old.yaml"}

    def test_skips_non_config_yaml(self, tmp_path):
        (tmp_path / "list.yml").write_text("- a\n- b\n")
        (tmp_path / "scalar.yml").write_text("42\n")
        (tmp_path / "other.yml").write_text("name: x\nvalue: y\n")
        (tmp_path / "broken.yml").write_text("{{not yaml")
        (tmp_path / "empty.yml").write_text("")
        assert discover_configs(tmp_path) == []

    def test_dotfile_settings_not_listed(self, tmp_path):
        (tmp_path / ".s3flood.yml").write_text("dataset_dir: /x\n")
        (tmp_path / "real.yml").write_text("bucket: b\n")
        found = [p.name for p in discover_configs(tmp_path)]
        assert found == ["real.yml"]

    def test_sorted_output(self, tmp_path):
        (tmp_path / "b.yml").write_text("bucket: b\n")
        (tmp_path / "a.yaml").write_text("bucket: b\n")
        assert [p.name for p in discover_configs(tmp_path)] == ["a.yaml", "b.yml"]
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `.venv/bin/python -m pytest tests/test_config.py::TestDiscoverConfigs -v`
Expected: FAIL с `ImportError: cannot import name 'discover_configs'`.

- [ ] **Step 3: Реализация в `config.py`**

После `load_run_config` добавить:

```python
# Ключи, по которым YAML-файл распознаётся как конфиг прогона
KNOWN_CONFIG_KEYS = {"endpoint", "endpoints", "bucket", "profile"}


def discover_configs(cwd: Path) -> list[Path]:
    """YAML-файлы рабочей папки, похожие на конфиги прогона.

    Конфиг — это маппинг с секцией run (маппингом) либо хотя бы одним из
    KNOWN_CONFIG_KEYS на верхнем уровне. Имя файла роли не играет.
    """
    found: list[Path] = []
    for path in list(cwd.glob("*.yml")) + list(cwd.glob("*.yaml")):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError):
            continue
        if not isinstance(data, dict):
            continue
        if isinstance(data.get("run"), dict) or KNOWN_CONFIG_KEYS & set(data):
            found.append(path)
    return sorted(found)
```

- [ ] **Step 4: Заменить 4 glob-места в `interactive.py`**

Импорт: в строке `from .config import load_run_config, resolve_run_settings` (найти фактическую) добавить `discover_configs`.

Во всех четырёх местах (`run_test_menu` ~167, `edit_config_menu` ~604, `validate_config_menu` ~674, `browse_bucket_menu` ~1054) заменить

```python
configs = sorted(list(cwd.glob("config*.yml")) + list(cwd.glob("config*.yaml")))
```

на

```python
configs = discover_configs(cwd)
```

(в edit_config_menu кавычки одинарные — заменить так же).

В `create_config_wizard` (~534): `default_name = "config.new.yaml"` → `default_name = "new.yml"`.

- [ ] **Step 5: Прогнать все тесты и линт**

Run: `.venv/bin/python -m pytest tests/ -v && .venv/bin/ruff check src/`
Expected: PASS, ruff чистый.

- [ ] **Step 6: Commit**

```bash
git add src/s3flood/config.py src/s3flood/interactive.py tests/test_config.py
git commit -m "feat: конфиги распознаются по содержимому, без префикса config. в имени"
```

---

### Task 7: Мастер датасета пишет .s3flood.yml; убрать data_dir из редактора и sample-конфига

**Files:**
- Modify: `src/s3flood/interactive.py:509-524` (`create_dataset_menu`), `src/s3flood/cli.py:150-159` (dataset-create), `src/s3flood/config_editor.py:94,128` (FieldSpec + default), `config.sample.yaml:20`
- Test: прогон всего пакета тестов (юнит-тест на запись — уже в Task 4; здесь интеграционные правки)

**Interfaces:**
- Consumes: `save_app_settings`, `APP_SETTINGS_FILE` из Task 4.

- [ ] **Step 1: `create_dataset_menu` — запись пути после успеха**

В `interactive.py` добавить импорт `from .app_settings import APP_SETTINGS_FILE, save_app_settings`. В `create_dataset_menu` после строки `console.print("[bold green]✅ Датасет успешно создан![/bold green]")` добавить:

```python
        dataset_path = str(Path(path).expanduser().resolve())
        save_app_settings({"dataset_dir": dataset_path})
        console.print(f"[dim]Путь к датасету записан в {APP_SETTINGS_FILE} — dataset_dir[/dim]")
```

(внутри того же `try`, чтобы при ошибке генерации путь не записывался).

- [ ] **Step 2: CLI `dataset-create` — то же самое**

В `cli.py` после вызова `plan_and_generate(...)` (ветка `args.cmd == "dataset-create"`):

```python
        from .app_settings import APP_SETTINGS_FILE, save_app_settings
        from pathlib import Path as _Path
        save_app_settings({"dataset_dir": str(_Path(args.path).expanduser().resolve())})
        print(f"Путь к датасету записан в {APP_SETTINGS_FILE} (dataset_dir)")
```

(если `Path` уже импортирован в cli.py — использовать его без алиаса).

- [ ] **Step 3: Убрать data_dir из редактора конфигов**

В `config_editor.py`:
- удалить строку `FieldSpec("data_dir", "data_dir", "text", allow_empty=False),` из `FIELD_DEFS`;
- удалить строку `"data_dir": "./loadset/data",` из `build_default_config()`.

- [ ] **Step 4: Обновить `config.sample.yaml`**

Заменить строку 20 `  data_dir: "./loadset/data"` на комментарий:

```yaml
  # Путь к датасету задаётся на уровне приложения (файл .s3flood.yml, ключ dataset_dir).
  # Он записывается автоматически при создании датасета; разово переопределить можно флагом --data-dir.
```

- [ ] **Step 5: Прогнать все тесты и линт**

Run: `.venv/bin/python -m pytest tests/ -v && .venv/bin/ruff check src/`
Expected: PASS. Отдельно проверить редактор: `.venv/bin/python -m pytest tests/test_config_editor.py -v` (там нет ссылок на data_dir — должен пройти).

- [ ] **Step 6: Ручная проверка**

`.venv/bin/python -m s3flood --interactive` → «Создать датасет» в tmp-папке маленького размера → убедиться, что появился `.s3flood.yml` с `dataset_dir`, и что «Запустить тест» видит конфиги без префикса (например, скопировать `config.kazan.yaml` как `kazan.yml`).

- [ ] **Step 7: Commit**

```bash
git add src/s3flood/interactive.py src/s3flood/cli.py src/s3flood/config_editor.py config.sample.yaml
git commit -m "feat: мастер датасета пишет dataset_dir в .s3flood.yml; data_dir убран из редактора"
```

---

## Финальная проверка (после всех задач)

- [ ] `.venv/bin/python -m pytest tests/ -v` — все зелёные
- [ ] `.venv/bin/ruff check src/` — чистый
- [ ] Обновить чекпоинт в `memory/tasks/2026-07-06-browser-v3-app-dataset-configs.md`
