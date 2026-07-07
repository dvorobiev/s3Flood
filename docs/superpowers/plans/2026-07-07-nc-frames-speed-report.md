# Рамки NC в браузере + отчёт с упором на скорость — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Панели браузера бакета в полных рамках со сводной строкой (как Norton Commander); итоговый отчёт прогона начинается с блока «Скорость» со спарклайном.

**Architecture:** Рамки — prompt_toolkit `Frame` вокруг каждой панели (титул в кромке, динамический стиль фокуса), сводная строка — отдельное окно внутри рамки; из `render_panel_lines` уходит титульная строка. Скорость — две чистые функции в `metrics.py` (`timeline_speeds`, `summary_speed_stats`) + перестройка `print_summary` в `executor.py` (rich Panel + `sparkline` из dashboard.py). Спека: `docs/superpowers/specs/2026-07-07-nc-frames-speed-report-design.md`.

**Tech Stack:** Python 3.10+, prompt_toolkit, rich, pytest.

## Global Constraints

- Только существующий стек prompt_toolkit / rich / questionary — никаких новых зависимостей.
- Ветка: `feature/nc-frames-speed-report`. Коммиты на русском, стиль `feat:`/`test:`.
- В rich-выводе не использовать квадратные скобки как текст ([bold]…[/bold]-разметка — ок).
- Команды из корня репо: `.venv/bin/python -m pytest tests/ -q`, `.venv/bin/ruff check <файл>` — не добавлять новых lint-ошибок (в репо есть существовавший ранее долг).
- `report.json` не меняется — только консольный вывод.
- Не трогать чужие незакоммиченные файлы (`config.kazan.yaml`, `config.tape.yaml`, `s3flood`, `memory/`, `CLAUDE.md`, `.s3flood.yml`); `git add` только своих файлов явно.

---

### Task 1: metrics.timeline_speeds и summary_speed_stats

**Files:**
- Modify: `src/s3flood/metrics.py` (после `build_timeline`)
- Test: `tests/test_metrics.py` (дополнить)

**Interfaces:**
- Produces:
  - `timeline_speeds(timeline: list[dict]) -> list[float]` — MB/s на бакет timeline (учитывает укрупнённый шаг бакетов).
  - `summary_speed_stats(summary: dict) -> dict` с ключами `total_MBps`, `write_MBps`, `read_MBps`, `ops_per_sec`, `peak_MBps`, `speeds` (list[float]); нули при пустых/нулевых данных. Task 2 вызывает обе.

- [ ] **Step 1: Написать падающие тесты**

Добавить в `tests/test_metrics.py`:

```python
from s3flood.metrics import summary_speed_stats, timeline_speeds


class TestTimelineSpeeds:
    def test_empty(self):
        assert timeline_speeds([]) == []

    def test_single_bucket_step_1(self):
        tl = [{"t_sec": 0, "write_bytes": 2 * 1024 * 1024, "read_bytes": 0}]
        assert timeline_speeds(tl) == [2.0]

    def test_coarse_step_divides_by_width(self):
        # бакеты по 10 секунд: 20 MB за бакет = 2 MB/s
        tl = [
            {"t_sec": 0, "write_bytes": 20 * 1024 * 1024, "read_bytes": 0},
            {"t_sec": 10, "write_bytes": 0, "read_bytes": 20 * 1024 * 1024},
        ]
        assert timeline_speeds(tl) == [2.0, 2.0]


class TestSummarySpeedStats:
    def test_normal_summary(self):
        summary = {
            "duration_sec": 10.0,
            "write_bytes": 50 * 1024 * 1024,
            "read_bytes": 50 * 1024 * 1024,
            "write_ok_ops": 30,
            "read_ok_ops": 20,
            "write_MBps_avg": 5.0,
            "read_MBps_avg": 5.0,
            "timeline": [
                {"t_sec": 0, "write_bytes": 30 * 1024 * 1024, "read_bytes": 0},
                {"t_sec": 1, "write_bytes": 20 * 1024 * 1024,
                 "read_bytes": 50 * 1024 * 1024},
            ],
        }
        stats = summary_speed_stats(summary)
        assert stats["total_MBps"] == 10.0
        assert stats["ops_per_sec"] == 5.0
        assert stats["write_MBps"] == 5.0
        assert stats["peak_MBps"] == 70.0
        assert len(stats["speeds"]) == 2

    def test_zero_duration_and_missing_timeline(self):
        stats = summary_speed_stats({"duration_sec": 0.0})
        assert stats["total_MBps"] == 0.0
        assert stats["ops_per_sec"] == 0.0
        assert stats["peak_MBps"] == 0.0
        assert stats["speeds"] == []
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `.venv/bin/python -m pytest tests/test_metrics.py -q`
Expected: FAIL с `ImportError: cannot import name 'timeline_speeds'`.

- [ ] **Step 3: Реализация в `metrics.py`** (после `build_timeline`)

```python
def timeline_speeds(timeline: list[dict]) -> list[float]:
    """MB/s на бакет timeline; при укрупнённых бакетах делит на ширину шага."""
    if not timeline:
        return []
    keys = sorted(t.get("t_sec", 0) for t in timeline)
    step = 1
    if len(keys) > 1:
        diffs = [b - a for a, b in zip(keys, keys[1:]) if b > a]
        if diffs:
            step = min(diffs)
    return [
        (t.get("write_bytes", 0) + t.get("read_bytes", 0)) / 1024 / 1024 / step
        for t in timeline
    ]


def summary_speed_stats(summary: dict) -> dict:
    """Скоростные показатели прогона для консольного отчёта."""
    duration = float(summary.get("duration_sec") or 0.0)
    write_b = summary.get("write_bytes", 0)
    read_b = summary.get("read_bytes", 0)
    ops = summary.get("write_ok_ops", 0) + summary.get("read_ok_ops", 0)
    speeds = timeline_speeds(summary.get("timeline") or [])
    return {
        "total_MBps": (write_b + read_b) / 1024 / 1024 / duration if duration > 0 else 0.0,
        "write_MBps": float(summary.get("write_MBps_avg") or 0.0),
        "read_MBps": float(summary.get("read_MBps_avg") or 0.0),
        "ops_per_sec": ops / duration if duration > 0 else 0.0,
        "peak_MBps": max(speeds) if speeds else 0.0,
        "speeds": speeds,
    }
```

- [ ] **Step 4: Прогнать тесты**

Run: `.venv/bin/python -m pytest tests/test_metrics.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/s3flood/metrics.py tests/test_metrics.py
git commit -m "feat: скоростные показатели прогона (timeline_speeds, summary_speed_stats)"
```

---

### Task 2: print_summary — блок «Скорость» первым

**Files:**
- Modify: `src/s3flood/executor.py:1106-1177` (`print_summary`)
- Test: `tests/test_report.py` (дополнить)

**Interfaces:**
- Consumes: `summary_speed_stats` из Task 1, `sparkline(values, width)` из `dashboard.py`.

- [ ] **Step 1: Написать падающие тесты**

Добавить в `tests/test_report.py`:

```python
from s3flood.executor import print_summary


def make_summary():
    return {
        "meta": {"profile": "mixed", "version": "0.13.0"},
        "duration_sec": 10.0,
        "wall_clock_sec": 11.0,
        "write_bytes": 100 * 1024 * 1024,
        "read_bytes": 50 * 1024 * 1024,
        "write_ok_ops": 40,
        "read_ok_ops": 10,
        "err_ops": 0,
        "write_MBps_avg": 10.0,
        "read_MBps_avg": 5.0,
        "timeline": [
            {"t_sec": 0, "write_bytes": 60 * 1024 * 1024, "read_bytes": 0},
            {"t_sec": 1, "write_bytes": 40 * 1024 * 1024,
             "read_bytes": 50 * 1024 * 1024},
        ],
        "latency": {
            "write": {"p50_ms": 100, "p90_ms": 150, "p95_ms": 180,
                      "p99_ms": 250, "avg_ms": 120, "max_ms": 400},
        },
    }


class TestPrintSummarySpeedFocus:
    def test_speed_block_first_and_has_throughput(self, capsys):
        print_summary(make_summary(), "m.csv", "r.json")
        out = capsys.readouterr().out
        assert "Скорость" in out
        assert "15.0 MB/s" in out          # сквозная (150 MB / 10 s)
        assert out.index("Скорость") < out.index("Латентность")

    def test_totals_row_present(self, capsys):
        print_summary(make_summary(), "m.csv", "r.json")
        out = capsys.readouterr().out
        assert "Итого" in out

    def test_latency_compact_no_max_column(self, capsys):
        print_summary(make_summary(), "m.csv", "r.json")
        out = capsys.readouterr().out
        assert "p95" in out and "p99" in out
        assert "max" not in out

    def test_works_without_timeline_and_latency(self, capsys):
        s = make_summary()
        del s["timeline"], s["latency"]
        print_summary(s, "m.csv", "r.json")
        assert "Скорость" in capsys.readouterr().out
```

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `.venv/bin/python -m pytest tests/test_report.py -q`
Expected: FAIL (нет блока «Скорость», в латентности есть max).

- [ ] **Step 3: Перестроить `print_summary`**

Заменить тело между `console.rule(title)` и таблицей латентности:

```python
    console.print()
    console.rule(title)

    from rich.panel import Panel as RichPanel

    from .dashboard import sparkline
    from .metrics import summary_speed_stats

    stats = summary_speed_stats(summary)
    speed_lines = [
        f"[bold cyan]{stats['total_MBps']:.1f} MB/s[/bold cyan] сквозная скорость",
        f"↑ запись {stats['write_MBps']:.1f} MB/s   ↓ чтение {stats['read_MBps']:.1f} MB/s   "
        f"{stats['ops_per_sec']:.1f} оп/с   пик {stats['peak_MBps']:.1f} MB/s",
    ]
    spark = sparkline(stats["speeds"], width=48)
    if spark:
        speed_lines.append(f"[cyan]{spark}[/cyan] MB/s по времени")
    console.print(RichPanel("\n".join(speed_lines), title="Скорость",
                            title_align="left", border_style="cyan"))

    tp = Table(box=box.SIMPLE_HEAVY, title="Пропускная способность", title_justify="left")
    tp.add_column("")
    tp.add_column("операций OK", justify="right")
    tp.add_column("объём", justify="right")
    tp.add_column("средняя скорость", justify="right")
    tp.add_row(
        "Запись", str(summary.get("write_ok_ops", 0)),
        format_bytes(summary.get("write_bytes", 0)),
        f"{summary.get('write_MBps_avg', 0.0):.1f} MB/s",
    )
    tp.add_row(
        "Чтение", str(summary.get("read_ok_ops", 0)),
        format_bytes(summary.get("read_bytes", 0)),
        f"{summary.get('read_MBps_avg', 0.0):.1f} MB/s",
    )
    tp.add_row(
        "[bold]Итого[/bold]",
        str(summary.get("write_ok_ops", 0) + summary.get("read_ok_ops", 0)),
        format_bytes(summary.get("write_bytes", 0) + summary.get("read_bytes", 0)),
        f"[bold]{stats['total_MBps']:.1f} MB/s[/bold]",
    )
    console.print(tp)
```

В таблице латентности сократить колонки — заменить строки:

```python
        for col in ("p50", "p95", "p99", "avg"):
            lt.add_column(col, justify="right")
        for name, key in (("Запись", "write"), ("Чтение", "read")):
            data = latency.get(key)
            if data:
                lt.add_row(
                    name,
                    *(f"{data[k]:.0f}" for k in ("p50_ms", "p95_ms", "p99_ms", "avg_ms")),
                )
```

Остальное (ошибки, строка времени, пути) — без изменений.

- [ ] **Step 4: Прогнать тесты и линт**

Run: `.venv/bin/python -m pytest tests/ -q && .venv/bin/ruff check src/s3flood/executor.py`
Expected: все PASS; по ruff — не больше ошибок, чем до правки.

- [ ] **Step 5: Commit**

```bash
git add src/s3flood/executor.py tests/test_report.py
git commit -m "feat: итоговый отчёт начинается с блока скорости со спарклайном"
```

---

### Task 3: panel_summary и рендер без титульной строки

**Files:**
- Modify: `src/s3flood/browser.py` (`render_panel_lines`, новая `panel_summary`, `_cursor_point`)
- Test: `tests/test_browser_render.py`

**Interfaces:**
- Produces: `panel_summary(panel: Panel) -> str` — сводка панели; `render_panel_lines` теперь НЕ содержит титульной строки (первая строка — заголовки колонок; титул рисует Frame в Task 4). `_cursor_point` → `+1`.

- [ ] **Step 1: Написать падающие тесты**

В `tests/test_browser_render.py` добавить:

```python
from s3flood.browser import panel_summary


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
```

и обновить существующие тесты, которые опирались на титул/индексы:
- `TestRenderPanelLines.test_header_and_rows`: убрать assert про `"bucket:/data/"`, проверять `"Имя" in lines[0][1]`.
- `TestColumns.*`: заголовки колонок теперь `lines[0]`, а не `lines[1]` (во всех пяти тестах класса).
- `test_no_header_when_loading`: без изменений по смыслу (при loading первая строка — спиннер).

- [ ] **Step 2: Убедиться, что тесты падают**

Run: `.venv/bin/python -m pytest tests/test_browser_render.py -q`
Expected: FAIL (титул ещё в выводе, `panel_summary` не существует).

- [ ] **Step 3: Реализация в `browser.py`**

1. В `render_panel_lines` удалить строки:

```python
    header_style = "class:panel.title.focused" if focused else "class:panel.title"
    lines: list[tuple[str, str]] = [(header_style, cut(f" {panel.title} ".ljust(width)) + "\n")]
```

заменив на `lines: list[tuple[str, str]] = []` (параметр `focused` остаётся — используется для курсора).

2. После `format_columns` добавить:

```python
def panel_summary(panel: Panel) -> str:
    """Сводная строка панели: всего объектов/размер или выделенное."""
    if panel.loading:
        return ""
    files = [r for r in panel.rows if not r.is_dir and r.name != ".."]
    marked = [r for r in files if r.marked]
    if marked:
        return f" выделено {len(marked)} · {format_bytes(sum(r.size for r in marked))}"
    return f" {len(files)} объектов · {format_bytes(sum(r.size for r in files))}"
```

3. `_cursor_point`: вернуть `+ 1` вместо `+ 2`, обновив комментарий (титула больше нет, над строками только заголовки колонок).

- [ ] **Step 4: Прогнать тесты**

Run: `.venv/bin/python -m pytest tests/ -q`
Expected: PASS все.

- [ ] **Step 5: Commit**

```bash
git add src/s3flood/browser.py tests/test_browser_render.py
git commit -m "feat: сводная строка панели, титул уходит в рамку (подготовка к Frame)"
```

---

### Task 4: Frame-рамки вокруг панелей браузера

**Files:**
- Modify: `src/s3flood/browser.py` (`BucketBrowserApp.__init__`, Style)

**Interfaces:**
- Consumes: `panel_summary` из Task 3; `Frame` уже импортирован (окно прогресса).

- [ ] **Step 1: Перестроить компоновку панелей в `__init__`**

Заменить сборку `body`:

```python
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
```

(окно-разделитель `Window(width=1, char="│", ...)` между панелями удалить — границы даёт рамка; `self.left.title`/`self.right.title` уже обновляются в `reload_local`/`_load_*`).

- [ ] **Step 2: Стили**

В `Style.from_dict` добавить/заменить:

```python
                "frame.border": "fg:#585858",
                "frame.label": "fg:#6c6c6c",
                "panelfocus frame.border": "fg:#00d7ff bold",
                "panelfocus frame.label": "fg:#00d7ff bold",
                "panel.summary": "fg:#6c6c6c",
                "panelfocus panel.summary": "fg:#00d7ff",
```

(существующую строку `"frame.border": "fg:#00d7ff"` от окна прогресса заменить на приведённые; ключи `panel.title`/`panel.title.focused` больше не используются рендером — удалить).

- [ ] **Step 3: Прогнать все тесты и линт**

Run: `.venv/bin/python -m pytest tests/ -q && .venv/bin/ruff check src/s3flood/browser.py`
Expected: PASS; по ruff не больше ошибок, чем до правки.

- [ ] **Step 4: Smoke конструирования приложения**

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
Expected: `layout OK` (компоновка с Frame собирается без исключений).

- [ ] **Step 5: Commit**

```bash
git add src/s3flood/browser.py
git commit -m "feat: панели браузера в рамках NC с титулом и сводной строкой"
```

---

## Финальная проверка

- [ ] `.venv/bin/python -m pytest tests/ -q` — все зелёные
- [ ] Обновить чекпоинт в `memory/tasks/`
- [ ] Ручной визуальный тест браузера и отчёта — за пользователем
