"""Живой дашборд прогона на rich.

build_dashboard(state) собирает renderable из снапшота состояния — рендеринг
полностью отделён от логики executor'а и единообразен с остальным TUI (rich),
вместо прежней ручной перерисовки ANSI-кодами.
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


def _header(state: dict) -> Text:
    header = Text()
    header.append(f"t {state['elapsed']:.1f} с", style="bold")
    eta = state.get("eta")
    header.append(f"  ETA {eta}" if eta else "  ETA n/a", style="cyan")
    header.append(f"  фаза {state.get('phase', '?')}", style="bold cyan")
    if state.get("pattern") == "bursty":
        if state.get("burst_active"):
            header.append("  BURST", style="bold yellow")
        else:
            header.append("  пауза", style="dim")
    if state.get("infinite"):
        header.append(f"  цикл {state.get('cycle_count', 0)}", style="magenta")
    if state.get("warmup_active"):
        header.append("  warmup — не в статистике", style="bold yellow")
    return header


def _progress_row(state: dict) -> Table:
    grid = Table.grid(padding=(0, 1))
    grid.add_column(width=30)
    grid.add_column()
    total = state.get("total_files") or 0
    done = state.get("files_done", 0)
    read = state.get("files_read", 0)
    err = state.get("files_err", 0)

    if state.get("infinite"):
        bar = ProgressBar(total=max(total, 1), completed=state.get("current_cycle_files", 0), width=30)
        counts = Text()
        counts.append(f"W {state.get('current_cycle_files', 0)}/{total} за цикл, всего {done}", style="green")
    elif state.get("profile") == "read":
        bar = ProgressBar(total=max(total, 1), completed=read, width=30)
        counts = Text()
        counts.append(f"R {read}/{total}", style="green")
    else:
        bar = ProgressBar(total=max(total, 1), completed=done, width=30)
        counts = Text()
        counts.append(f"W {done}/{total}", style="green")
        total_to_read = state.get("total_to_read") or 0
        if total_to_read:
            counts.append(f"  R {read}/{total_to_read}", style="cyan")

    counts.append(f"  {_format_bytes(state.get('bytes_done', 0))}↑", style="dim")
    if state.get("bytes_read"):
        counts.append(f" {_format_bytes(state['bytes_read'])}↓", style="dim")
    err_style = "bold red" if err else "dim"
    counts.append(f"  Err {err}", style=err_style)
    grid.add_row(bar, counts)
    return grid


def _load_line(state: dict) -> Text:
    line = Text()
    line.append(
        f"Активно {state.get('inflight', 0)}/{state.get('threads', 0)} "
        f"(U:{state.get('active_uploads', 0)} D:{state.get('active_downloads', 0)})",
        style="blue",
    )
    line.append(f"  очередь {state.get('queue', 0)}", style="blue")
    profile = state.get("profile")
    if profile != "read":
        line.append(f"  W-RPS {state.get('write_rps', 0.0):.2f}", style="bold green")
    if profile != "write":
        line.append(f"  R-RPS {state.get('read_rps', 0.0):.2f}", style="bold cyan")
    return line


def _rates_line(state: dict) -> Text:
    line = Text()
    profile = state.get("profile")
    if profile != "read":
        line.append(
            f"W cur {state.get('wbps_mb', 0.0):6.1f} MB/s avg {state.get('avg_wbps_mb', 0.0):6.1f}",
            style="bold green",
        )
    if profile != "write":
        if line.plain:
            line.append("  |  ", style="dim")
        line.append(
            f"R cur {state.get('rbps_mb', 0.0):6.1f} MB/s avg {state.get('avg_rbps_mb', 0.0):6.1f}",
            style="bold cyan",
        )
    return line


def _recent_ops_table(state: dict) -> Table | None:
    ops = state.get("recent_ops") or []
    if not ops:
        return None
    now = state.get("now", 0.0)
    table = Table(box=box.SIMPLE, show_header=False, pad_edge=False, padding=(0, 1))
    table.add_column(width=1)
    table.add_column(min_width=20, max_width=40, no_wrap=True)
    table.add_column(justify="right", no_wrap=True)
    table.add_column(justify="right", no_wrap=True)
    table.add_column(justify="right", no_wrap=True)
    for entry in ops:
        is_upload = entry.get("op") == "upload"
        icon = Text(WRITE_ICON if is_upload else READ_ICON, style="green" if is_upload else "cyan")
        name = _shorten_middle(entry.get("filename") or "", 40)
        size = _format_bytes(entry.get("bytes") or 0)
        if entry.get("done"):
            lat_ms = entry.get("latency_ms") or 0
            duration = f"{lat_ms / 1000:6.2f} с"
            speed = entry.get("speed_mbps")
            speed_disp = f"{speed:7.1f} MB/s" if speed is not None else "     --"
            row_style = "red" if entry.get("error") else None
        else:
            duration = f"{max(now - entry.get('started', now), 0.0):6.2f} с"
            speed_disp = "     …"
            row_style = "dim"
        table.add_row(icon, name, size, duration, speed_disp, style=row_style)
    return table


def build_dashboard(state: dict) -> Panel:
    """Собирает панель дашборда из снапшота состояния прогона."""
    parts = [_header(state), _progress_row(state), _load_line(state), _rates_line(state)]
    recent = _recent_ops_table(state)
    if recent is not None:
        parts.append(Text("Последние операции", style="bold blue"))
        parts.append(recent)
    title = f"s3flood ▪ {state.get('profile', '?')} ▪ {state.get('pattern', 'sustained')}"
    return Panel(Group(*parts), title=title, title_align="left", box=box.ROUNDED, padding=(0, 1))
