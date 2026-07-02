"""Живой дашборд прогона на rich (btop-стиль).

build_dashboard(state) собирает renderable из снапшота состояния — рендеринг
полностью отделён от логики executor'а: шапка с endpoint/bucket, прогресс,
спарклайны RPS, последние операции с анимацией активных.
"""
from __future__ import annotations

from rich import box
from rich.console import Group
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text

WRITE_ICON = "↑"
READ_ICON = "↓"
SPARK_BLOCKS = " ▁▂▃▄▅▆▇█"
SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

WRITE_STYLE = "green"
READ_STYLE = "cyan"


def sparkline(values, width: int = 24) -> str:
    """Мини-график из блоковых символов по последним `width` значениям."""
    if not values:
        return ""
    tail = list(values)[-width:]
    peak = max(tail)
    if peak <= 0:
        return SPARK_BLOCKS[1] * len(tail)
    out = []
    for v in tail:
        idx = int(v / peak * (len(SPARK_BLOCKS) - 1))
        out.append(SPARK_BLOCKS[max(idx, 1)])
    return "".join(out)


def _format_bytes(num_bytes: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes or 0)
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    if idx == 0:
        return f"{int(size)} {units[idx]}"
    return f"{size:.1f} {units[idx]}"


def _format_clock(seconds: float) -> str:
    total = int(max(seconds, 0))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def _shorten_middle(text: str, max_len: int) -> str:
    if text is None:
        return ""
    if len(text) <= max_len:
        return text
    if max_len <= 3:
        return text[:max_len]
    keep = max_len - 3
    head = keep // 2
    return f"{text[:head]}...{text[-(keep - head):]}"


def _phase_badge(state: dict) -> Text:
    phase = state.get("phase", "?")
    style = {"WRITE": f"bold black on {WRITE_STYLE}",
             "READ": f"bold black on {READ_STYLE}",
             "MIXED": "bold black on yellow"}.get(phase, "bold reverse")
    return Text(f" {phase} ", style=style)


def _header(state: dict) -> Text:
    line = Text()
    line.append_text(_phase_badge(state))
    line.append(f" {state.get('pattern', 'sustained')}", style="dim")
    if state.get("pattern") == "bursty":
        line.append("  BURST" if state.get("burst_active") else "  пауза",
                    style="bold yellow" if state.get("burst_active") else "dim")
    if state.get("infinite"):
        line.append(f"  ∞ цикл {state.get('cycle_count', 0)}", style="magenta")
    if state.get("warmup_active"):
        line.append("  WARMUP", style="bold black on yellow")
        line.append(" не в статистике", style="yellow dim")
    line.append(f"   {_format_clock(state.get('elapsed', 0))}", style="bold")
    eta = state.get("eta")
    line.append(f" · ETA {eta}" if eta else " · ETA n/a", style="dim")
    return line


def _target_line(state: dict) -> Text:
    line = Text(style="dim")
    endpoint = state.get("endpoint")
    if endpoint:
        line.append(endpoint)
    bucket = state.get("bucket")
    if bucket:
        line.append(f" · {bucket}")
    line.append(f" · {state.get('threads', 0)} thr")
    return line


def _progress_row(state: dict) -> Table:
    grid = Table.grid(padding=(0, 1))
    grid.add_column(width=2)
    grid.add_column(width=26)
    grid.add_column()
    total = state.get("total_files") or 0
    done = state.get("files_done", 0)
    read = state.get("files_read", 0)
    err = state.get("files_err", 0)

    if state.get("infinite"):
        completed = state.get("current_cycle_files", 0)
        label, style = "W", WRITE_STYLE
        counts_text = f"{completed}/{total} за цикл · всего {done}"
    elif state.get("profile") == "read":
        completed, label, style = read, "R", READ_STYLE
        counts_text = f"{read}/{total}"
    else:
        completed, label, style = done, "W", WRITE_STYLE
        counts_text = f"{done}/{total}"

    pct = (completed / total * 100) if total else 0.0
    bar = ProgressBar(total=max(total, 1), completed=completed, width=26,
                      complete_style=style, finished_style=style)
    counts = Text()
    counts.append(f"{pct:3.0f}%", style=f"bold {style}")
    counts.append(f"  {counts_text}", style=style)
    total_to_read = state.get("total_to_read") or 0
    if total_to_read and state.get("profile") == "mixed":
        counts.append(f" · R {read}/{total_to_read}", style=READ_STYLE)
    counts.append(f" · {_format_bytes(state.get('bytes_done', 0))}↑", style="dim")
    if state.get("bytes_read"):
        counts.append(f" {_format_bytes(state['bytes_read'])}↓", style="dim")
    counts.append(f" · Err {err}", style="bold red" if err else "dim")
    grid.add_row(Text(label, style=f"bold {style}"), bar, counts)
    return grid


def _rates_block(state: dict) -> Group:
    profile = state.get("profile")
    rps_line = Text()
    if profile != "read":
        rps_line.append(f"W-RPS {state.get('write_rps', 0.0):6.2f} ", style=f"bold {WRITE_STYLE}")
        rps_line.append(sparkline(state.get("write_rps_history") or []), style=WRITE_STYLE)
    if profile != "write":
        if rps_line.plain:
            rps_line.append("   ")
        rps_line.append(f"R-RPS {state.get('read_rps', 0.0):6.2f} ", style=f"bold {READ_STYLE}")
        rps_line.append(sparkline(state.get("read_rps_history") or []), style=READ_STYLE)

    speed_line = Text()
    if profile != "read":
        speed_line.append(f"W {state.get('wbps_mb', 0.0):7.1f} MB/s", style=f"bold {WRITE_STYLE}")
        speed_line.append(f" (avg {state.get('avg_wbps_mb', 0.0):.1f})", style="dim")
    if profile != "write":
        if speed_line.plain:
            speed_line.append("   ")
        speed_line.append(f"R {state.get('rbps_mb', 0.0):7.1f} MB/s", style=f"bold {READ_STYLE}")
        speed_line.append(f" (avg {state.get('avg_rbps_mb', 0.0):.1f})", style="dim")
    speed_line.append(
        f"   активно {state.get('inflight', 0)}/{state.get('threads', 0)}"
        f" · очередь {state.get('queue', 0)}",
        style="dim",
    )
    return Group(rps_line, speed_line)


def _recent_ops_table(state: dict) -> Table | None:
    ops = state.get("recent_ops") or []
    if not ops:
        return None
    now = state.get("now", 0.0)
    spin = SPINNER_FRAMES[int(now * 8) % len(SPINNER_FRAMES)]
    table = Table(box=None, show_header=False, pad_edge=False, padding=(0, 1))
    table.add_column(width=1)
    table.add_column(min_width=20, max_width=40, no_wrap=True)
    table.add_column(justify="right", no_wrap=True)
    table.add_column(justify="right", no_wrap=True)
    table.add_column(justify="right", no_wrap=True)
    for entry in ops:
        is_upload = entry.get("op") == "upload"
        name = _shorten_middle(entry.get("filename") or "", 40)
        size = _format_bytes(entry.get("bytes") or 0)
        if entry.get("done"):
            icon = Text(WRITE_ICON if is_upload else READ_ICON,
                        style=WRITE_STYLE if is_upload else READ_STYLE)
            lat_ms = entry.get("latency_ms") or 0
            duration = f"{lat_ms / 1000:6.2f} с"
            speed = entry.get("speed_mbps")
            speed_disp = f"{speed:7.1f} MB/s" if speed is not None else "     --"
            row_style = "red" if entry.get("error") else None
        else:
            icon = Text(spin, style="bold yellow")
            duration = f"{max(now - entry.get('started', now), 0.0):6.2f} с"
            speed_disp = "      …"
            row_style = "dim"
        table.add_row(icon, name, size, duration, speed_disp, style=row_style)
    return table


def build_dashboard(state: dict) -> Panel:
    """Собирает панель дашборда из снапшота состояния прогона."""
    parts = [
        _header(state),
        _target_line(state),
        Text(),
        _progress_row(state),
        Text(),
        _rates_block(state),
    ]
    recent = _recent_ops_table(state)
    if recent is not None:
        rule = Text("── операции ", style="dim")
        parts.append(rule)
        parts.append(recent)
    title = Text()
    title.append("s3flood", style="bold cyan")
    version = state.get("version")
    if version:
        title.append(f" {version}", style="dim")
    return Panel(
        Group(*parts),
        title=title,
        title_align="left",
        box=box.ROUNDED,
        border_style="dim",
        padding=(0, 1),
    )
