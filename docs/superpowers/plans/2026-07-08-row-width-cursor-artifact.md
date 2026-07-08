# Единая ширина строки панели, убрать индикатор курсора вне фокуса — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Строки панели браузера бакета всегда одной и той же визуальной ширины (реверс-выделение/подчёркивание не «плавает» в зависимости от длины поля даты/меты); индикатор курсора в панели, потерявшей фокус, убран — как в Midnight Commander.

**Architecture:** Всё — в одной функции `render_panel_lines` (`src/s3flood/browser.py`) и в словаре стилей `Style.from_dict`. Причина обоих багов на скриншотах пользователя: поле `meta` не дополняется пробелами до конца строки, поэтому длина текста (и, следовательно, ширина применённого стиля) зависит от содержимого. Спека: `docs/superpowers/specs/2026-07-08-row-width-cursor-artifact-design.md`.

**Tech Stack:** Python 3.10+, prompt_toolkit, pytest.

## Global Constraints

- Только существующий стек — никаких новых зависимостей.
- Ветка: `feature/panel-width-double-frame` (продолжение текущей итерации). Коммиты на русском, стиль `fix:` как в git log.
- Команды из корня репо: `.venv/bin/python -m pytest tests/ -q`, `.venv/bin/ruff check src/s3flood/browser.py` — не добавлять новых lint-ошибок (существующий долг — 7 ошибок в файле).
- Не трогать чужие незакоммиченные файлы (`config.kazan.yaml`, `config.tape.yaml`, `s3flood`, `memory/`, `CLAUDE.md`, `.s3flood.yml`, посторонние `.bin`-файлы); `git add` только своих файлов явно.
- Не трогать строки `загрузка…`/`(пусто)` (вне объёма) и константы колонок в `format_columns` (сетка не меняется).

---

### Task 1: Единая ширина строки + убрать row.cursor

**Files:**
- Modify: `src/s3flood/browser.py:248-279` (`render_panel_lines`), `src/s3flood/browser.py` (Style.from_dict — убрать ключ `row.cursor`)
- Test: `tests/test_browser_render.py`

**Interfaces:** нет новых — сигнатура `render_panel_lines(panel, width, focused)` не меняется.

- [ ] **Step 1: Написать падающие тесты**

Добавить в `tests/test_browser_render.py`:

```python
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
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `.venv/bin/python -m pytest tests/test_browser_render.py::TestUniformRowWidth tests/test_browser_render.py::TestNoCursorStyleWhenUnfocused -v`
Expected: FAIL — `TestUniformRowWidth` падает (длины строк различаются, `{60}` vs например `{44, 60, 52}`), `test_unfocused_selection_has_no_cursor_style` падает (`"cursor" in "class:row.cursor"`).

- [ ] **Step 3: Реализация**

В `render_panel_lines` заменить:

```python
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
```

на:

```python
    headers = COLUMN_HEADERS.get(panel.mode, COLUMN_HEADERS["list"])
    header_text = "   " + format_columns(*headers, width)
    lines.append(("class:panel.columns", cut(header_text).ljust(width) + "\n"))

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
        lines.append((style, cut(text).ljust(width) + "\n"))
```

(убрана ветка `elif is_sel: style = "class:row.cursor"`; добавлен `.ljust(width)` после `cut(...)` в обеих строках — заголовках и данных).

В `Style.from_dict` удалить строку `"row.cursor": "underline",`.

- [ ] **Step 4: Прогнать все тесты и линт**

Run: `.venv/bin/python -m pytest tests/ -q && .venv/bin/ruff check src/s3flood/browser.py`
Expected: PASS все (168+ тестов из предыдущих итераций + новые); ruff — не больше 7 существующих ошибок.

- [ ] **Step 5: Commit**

```bash
git add src/s3flood/browser.py tests/test_browser_render.py
git commit -m "fix: единая ширина строки панели, убран индикатор курсора вне фокуса"
```

---

## Финальная проверка

- [ ] `.venv/bin/python -m pytest tests/ -q` — все зелёные
- [ ] `.venv/bin/ruff check src/s3flood/browser.py` — не больше существующего долга
- [ ] Обновить чекпоинт в `memory/tasks/`
- [ ] Ручной визуальный тест браузера — за пользователем
