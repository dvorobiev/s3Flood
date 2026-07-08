# Фикс ширины панелей, внешняя двойная рамка, акцент фокуса, усечение имён — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Панели браузера бакета всегда делят экран ровно 50/50 (не «схлопываются» при загрузке/выборе бакета); внешняя двойная рамка вокруг всего приложения; более яркий акцент только на активной панели; длинные имена файлов усекаются по центру («начало…конец.расширение»).

**Architecture:** Всё — в `src/s3flood/browser.py`. Причина схлопывания — отсутствие явной ширины у обёрток панелей: prompt_toolkit вычисляет предпочтительную ширину из контента, и она «плавает» между режимами (loading/список). Фикс — `width=Dimension(weight=1)` на обеих обёртках. Внешняя рамка — три новых окна-примитива (не встроенный `Frame`, у него зашиты одинарные символы). Спека: `docs/superpowers/specs/2026-07-08-panel-width-double-frame-truncation-design.md`.

**Tech Stack:** Python 3.10+, prompt_toolkit 3.0.52, pytest.

## Global Constraints

- Только prompt_toolkit/rich/questionary — никаких новых зависимостей.
- Ветка: `feature/panel-width-double-frame`. Коммиты на русском, стиль `feat:`/`test:`.
- Команды из корня репо: `.venv/bin/python -m pytest tests/ -q`, `.venv/bin/ruff check src/s3flood/browser.py` — не добавлять новых lint-ошибок (существующий долг — 7 ошибок в файле, не трогать).
- Не трогать чужие незакоммиченные файлы (`config.kazan.yaml`, `config.tape.yaml`, `s3flood`, `memory/`, `CLAUDE.md`, `.s3flood.yml`, посторонние `.bin`-файлы); `git add` только своих файлов явно.
- `Dimension` импортируется как `from prompt_toolkit.layout import Dimension` (уже в этом модуле импортируются `HSplit`/`VSplit`/`Window` из `prompt_toolkit.layout` — добавить `Dimension` в тот же импорт).

---

### Task 1: truncate_middle — усечение длинных имён по центру

**Files:**
- Modify: `src/s3flood/browser.py` (после `SPINNER`, перед `format_columns`)
- Test: `tests/test_browser_render.py`

**Interfaces:**
- Produces: `truncate_middle(name: str, width: int) -> str`. `format_columns` (уже существует, сигнатура не меняется) использует её для поля имени перед выравниванием.

- [ ] **Step 1: Написать падающие тесты**

Добавить в `tests/test_browser_render.py`:

```python
from s3flood.browser import truncate_middle


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
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `.venv/bin/python -m pytest tests/test_browser_render.py::TestTruncateMiddle -v`
Expected: FAIL с `ImportError: cannot import name 'truncate_middle'`.

- [ ] **Step 3: Реализация**

После `SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"` в `browser.py` добавить:

```python
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
```

Изменить `format_columns` (сейчас использует `{name:<{name_w}.{name_w}}`, что обрезает хвост
без учёта расширения):

```python
def format_columns(name: str, size: str, meta: str, width: int) -> str:
    """Одна строка панели: имя │ размер │ мета — с фиксированной сеткой колонок."""
    name_w = max(width - 33, 12)
    display_name = truncate_middle(name, name_w)
    return f"{display_name:<{name_w}} │{size:>9} │ {meta}"
```

- [ ] **Step 4: Прогнать тесты**

Run: `.venv/bin/python -m pytest tests/test_browser_render.py -q`
Expected: PASS все (включая уже существующие тесты на `format_columns`/`render_panel_lines` —
короткие имена не затронуты, поведение не меняется).

- [ ] **Step 5: Commit**

```bash
git add src/s3flood/browser.py tests/test_browser_render.py
git commit -m "feat: усечение длинных имён по центру (начало…конец.расширение)"
```

---

### Task 2: Фикс схлопывания панелей — Dimension(weight=1)

**Files:**
- Modify: `src/s3flood/browser.py:23-31` (импорт), `src/s3flood/browser.py:301-321` (`left_frame`/`right_frame`/`body`)
- Test: `tests/test_browser_render.py`

**Interfaces:**
- Consumes: ничего нового снаружи.
- Produces: `left_frame`/`right_frame` получают `width=Dimension(weight=1)` — далее (Task 3) на
  этом же принципе строится внешняя рамка.

- [ ] **Step 1: Написать падающий тест**

Добавить в `tests/test_browser_render.py` (нужны существующие `_FixedSizeOutput`, `Screen`,
`WritePosition`, `MouseHandlers`, `BucketBrowserApp`, `DummyInput` — уже импортированы для
`TestPanelWidthRealRender`):

```python
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
        row = screen.data_buffer[0]
        for x in range(1, cols):
            if row[x].char == "┌":
                return x
        raise AssertionError("правая рамка не найдена в верхней строке")

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
```

- [ ] **Step 2: Убедиться, что тест падает**

Run: `.venv/bin/python -m pytest tests/test_browser_render.py::TestPanelsDoNotCollapse -v`
Expected: FAIL — `loading_boundary != loaded_boundary` (без фикса граница «плавает» в
зависимости от контента).

- [ ] **Step 3: Реализация**

В начале файла заменить импорт:

```python
from prompt_toolkit.layout import (
    ConditionalContainer,
    Dimension,
    Float,
    FloatContainer,
    HSplit,
    Layout,
    VSplit,
    Window,
)
```

В `BucketBrowserApp.__init__` изменить `left_frame`/`right_frame` (добавить `width=`):

```python
        left_frame = HSplit(
            [Frame(
                HSplit([
                    self.left_window,
                    Window(height=1, content=_summary_control(self.left)),
                ]),
                title=lambda: f" {self.left.title} ",
            )],
            width=Dimension(weight=1),
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
            width=Dimension(weight=1),
            style=lambda: "class:panelfocus" if self.focus_right else "",
        )
```

- [ ] **Step 4: Прогнать тесты**

Run: `.venv/bin/python -m pytest tests/test_browser_render.py -q`
Expected: PASS все, включая `TestPanelWidthRealRender` (формула `_panel_width()` пока не
менялась — там нет внешней рамки, граница по-прежнему `_panel_width() + 2`, что при
`Dimension(weight=1)` совпадает с `cols // 2` при чётном `cols`).

- [ ] **Step 5: Commit**

```bash
git add src/s3flood/browser.py tests/test_browser_render.py
git commit -m "fix: панели фиксированы 50/50 через Dimension(weight=1), не зависят от контента"
```

---

### Task 3: Внешняя двойная рамка вокруг приложения

**Files:**
- Modify: `src/s3flood/browser.py:334-337` (`root`), `src/s3flood/browser.py:408-417` (`_panel_width`), `src/s3flood/browser.py:371-393` (Style)
- Test: `tests/test_browser_render.py` (обновить `TestPanelWidthRealRender`, добавить новый класс)

**Interfaces:**
- Consumes: `Dimension` из Task 2 (уже импортирован).
- Produces: `_panel_width()` учитывает внешнюю рамку (2 колонки); новый метод
  `_term_cols() -> int` (вынесенный расчёт ширины терминала, переиспользуется рендером рамки).

- [ ] **Step 1: Написать падающие тесты**

Обновить существующий класс `TestPanelWidthRealRender` в `tests/test_browser_render.py` —
из-за внешней рамки все строки/колонки сдвигаются на 1:

```python
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
```

Добавить новый класс, проверяющий саму внешнюю рамку:

```python
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
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `.venv/bin/python -m pytest tests/test_browser_render.py::TestOuterDoubleFrame tests/test_browser_render.py::TestPanelWidthRealRender -v`
Expected: FAIL — внешней рамки ещё нет (`╔`/`║`/`╚` отсутствуют в буфере), старый
`TestPanelWidthRealRender` падает на новых индексах строк/колонок.

- [ ] **Step 3: Реализация**

Добавить в `BucketBrowserApp` (рядом с `_bucket_title`, до `_panel_width`) метод для ширины
терминала и рендера линий внешней рамки:

```python
    def _term_cols(self) -> int:
        try:
            return self.app.output.get_size().columns
        except Exception:
            return 80

    def _outer_line(self, left: str, fill: str, right: str) -> list[tuple[str, str]]:
        cols = self._term_cols()
        return [("class:outer.border", left + fill * max(cols - 2, 0) + right)]
```

Изменить `_panel_width()`:

```python
    def _panel_width(self) -> int:
        cols = self._term_cols()
        inner_cols = cols - 2         # внешняя двойная рамка (левая/правая)
        panel_area = inner_cols // 2  # половина на панель (VSplit 50/50)
        return max(panel_area - 2, 20)  # минус собственная рамка панели (Frame)
```

Изменить сборку `root` (сейчас — `root = FloatContainer(content=HSplit([body, status, keybar]), floats=[progress_float])`):

```python
        inner = FloatContainer(
            content=HSplit([body, status, keybar]),
            floats=[progress_float],
        )
        root = HSplit([
            Window(height=1, content=FormattedTextControl(
                lambda: self._outer_line("╔", "═", "╗"))),
            VSplit([
                Window(width=1, char="║", style="class:outer.border"),
                inner,
                Window(width=1, char="║", style="class:outer.border"),
            ]),
            Window(height=1, content=FormattedTextControl(
                lambda: self._outer_line("╚", "═", "╝"))),
        ])
```

Добавить стиль в `Style.from_dict`:

```python
                "outer.border": "fg:ansiwhite",
```

- [ ] **Step 4: Прогнать тесты**

Run: `.venv/bin/python -m pytest tests/ -q`
Expected: PASS все (152+ тестов из предыдущих итераций + новые).

- [ ] **Step 5: Smoke конструирования приложения**

Run:
```bash
.venv/bin/python -c "
from pathlib import Path
from prompt_toolkit.input import DummyInput
from prompt_toolkit.output import DummyOutput
from s3flood.browser import BucketBrowserApp
app = BucketBrowserApp(bucket='b', endpoint='http://h:9000', env={},
                       start_dir=Path('.'), input=DummyInput(), output=DummyOutput())
print('layout OK')
"
```
Expected: `layout OK`.

- [ ] **Step 6: Commit**

```bash
git add src/s3flood/browser.py tests/test_browser_render.py
git commit -m "feat: внешняя двойная рамка вокруг приложения"
```

---

### Task 4: Яркий акцент только на фокусе

**Files:**
- Modify: `src/s3flood/browser.py:389-392` (Style — стили `panelfocus ...`)

**Interfaces:** нет новых — только значения в словаре стилей.

- [ ] **Step 1: Написать падающий тест**

Добавить в `tests/test_browser_render.py`:

```python
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
```

(`app.app.style` — это объект `Style`, собранный `Style.from_dict(...)`; его атрибут
`style_rules` — список пар `(selector, style_string)` в порядке объявления, `dict(...)`
даёт удобный доступ по ключу-селектору; проверено на установленной версии prompt_toolkit
3.0.52.)

- [ ] **Step 2: Убедиться, что тест падает**

Run: `.venv/bin/python -m pytest tests/test_browser_render.py::TestFocusAccentStyle -v`
Expected: FAIL (текущее значение `fg:#00d7ff bold`, не содержит `ansibrightcyan`).

- [ ] **Step 3: Реализация**

В `Style.from_dict` заменить три строки:

```python
                "panelfocus frame.border": "fg:ansibrightcyan bold",
                "panelfocus frame.label": "fg:ansibrightcyan bold",
                "panel.summary": "fg:#6c6c6c",
                "panelfocus panel.summary": "fg:ansibrightcyan bold",
```

Если для теста потребовался `self._style_dict` (см. Step 1) — сохранить словарь в
переменную перед вызовом `Style.from_dict(...)` и присвоить `self._style_dict = <словарь>`
рядом с созданием `self.app`.

- [ ] **Step 4: Прогнать тесты и линт**

Run: `.venv/bin/python -m pytest tests/ -q && .venv/bin/ruff check src/s3flood/browser.py`
Expected: все PASS; по ruff — не больше 7 ошибок (существующий долг).

- [ ] **Step 5: Commit**

```bash
git add src/s3flood/browser.py tests/test_browser_render.py
git commit -m "feat: ярче акцент фокуса активной панели (ansibrightcyan)"
```

---

## Финальная проверка

- [ ] `.venv/bin/python -m pytest tests/ -q` — все зелёные
- [ ] `.venv/bin/ruff check src/s3flood/browser.py` — не больше существующего долга
- [ ] Обновить чекпоинт в `memory/tasks/`
- [ ] Ручной визуальный тест браузера — за пользователем
