"""Генерация датасета для нагрузочного тестирования.

Файлы получают случайные размеры внутри диапазона своей группы и по умолчанию
заполняются несжимаемыми данными: датасет из нулей одного размера давал
ложно-высокие результаты на хранилищах с компрессией/дедупликацией.
"""
import os
import random
import shutil
import uuid
from pathlib import Path
from typing import Tuple

# Utilities to parse sizes like "100MB", "1GB"
UNITS = {"kb": 1024, "mb": 1024**2, "gb": 1024**3, "tb": 1024**4}

GROUP_NAMES = ["small", "medium", "large"]
# Нижняя граница размеров первой группы — доля от её верхней границы
SMALL_GROUP_LOWER_RATIO = 100
_RANDOM_CHUNK = 1024 * 1024


def parse_size(s: str) -> int:
    s = str(s).strip().lower()
    for u, mul in UNITS.items():
        if s.endswith(u):
            return int(float(s[:-len(u)]) * mul)
    return int(s)


def plan_file_sizes(lower: int, upper: int, count: int, rng: random.Random) -> list[int]:
    """Равномерно распределённые размеры файлов в диапазоне [lower, upper]."""
    if upper <= lower:
        return [lower] * count
    return [rng.randint(lower, upper) for _ in range(count)]


def group_bounds(limits: Tuple[int, ...]) -> list[tuple[int, int]]:
    """Диапазоны размеров (lower, upper) для групп small/medium/large."""
    bounds = []
    for i, upper in enumerate(limits):
        if i == 0:
            lower = max(1, upper // SMALL_GROUP_LOWER_RATIO)
        else:
            lower = limits[i - 1] + 1
        bounds.append((lower, upper))
    return bounds


def write_random_file(path: Path, size: int) -> None:
    """Создаёт файл заданного размера с несжимаемым содержимым."""
    with open(path, "wb") as fh:
        remaining = size
        while remaining > 0:
            chunk = os.urandom(min(_RANDOM_CHUNK, remaining))
            fh.write(chunk)
            remaining -= len(chunk)


def write_zero_file(path: Path, size: int) -> None:
    """Создаёт sparse-файл из нулей (быстро, но сжимаемо) — режим совместимости."""
    with open(path, "wb") as fh:
        if size > 0:
            fh.seek(size - 1)
            fh.write(b"\0")


def plan_groups(target_bytes: int, min_counts: Tuple[int, int, int]):
    shares = [0.3, 0.5, 0.2]
    base = [max(min_counts[0], 1), max(min_counts[1], 1), max(min_counts[2], 1)]
    alloc = [int(target_bytes * p) for p in shares]
    return alloc, base


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def make_seed_files(base_dir: Path, bounds, rng: random.Random, fill: str, seeds_per_group: int = 8):
    """Создаёт несколько seed-файлов разных размеров на группу (для symlink-режима)."""
    write_file = write_random_file if fill == "random" else write_zero_file
    seeds: list[list[Path]] = []
    for gi, (lower, upper) in enumerate(bounds):
        gdir = base_dir / f"seed_g{gi}"
        ensure_dir(gdir)
        group_seeds = []
        for si, size in enumerate(plan_file_sizes(lower, upper, seeds_per_group, rng)):
            f = gdir / f"seed_{si}.bin"
            if not f.exists() or f.stat().st_size != size:
                write_file(f, size)
            group_seeds.append(f)
        seeds.append(group_seeds)
    return seeds


def plan_and_generate(
    path: str,
    target_bytes: str,
    use_symlinks: bool,
    min_counts: str,
    group_limits: str,
    safety_ratio: float,
    fill: str = "random",
):
    base = Path(path).expanduser()
    ensure_dir(base)
    base = base.resolve()

    mins = tuple(int(x) for x in min_counts.split(","))
    limits = tuple(parse_size(x) for x in group_limits.split(","))
    bounds = group_bounds(limits)
    rng = random.Random()
    write_file = write_random_file if fill == "random" else write_zero_file

    if target_bytes == "auto":
        st = shutil.disk_usage(base)
        target = int(st.free * safety_ratio)
    else:
        target = parse_size(target_bytes)

    alloc, _ = plan_groups(target, mins)
    seeds = make_seed_files(base / "seeds", bounds, rng, fill) if use_symlinks else None

    data_dir = base / "data"
    ensure_dir(data_dir)

    for gi, (bytes_target, minc, (lower, upper)) in enumerate(zip(alloc, mins, bounds)):
        gdir = data_dir / GROUP_NAMES[gi]
        ensure_dir(gdir)
        total_bytes = 0
        count = 0

        def add_file() -> int:
            key = gdir / f"{uuid.uuid4()}.bin"
            if use_symlinks:
                seed = rng.choice(seeds[gi])
                os.symlink(os.path.relpath(seed, start=gdir), key)
                return seed.stat().st_size
            size = plan_file_sizes(lower, upper, 1, rng)[0]
            write_file(key, size)
            return size

        # минимальное количество файлов, затем добор до целевого объёма группы
        for _ in range(minc):
            total_bytes += add_file()
            count += 1
        while total_bytes + upper <= bytes_target:
            total_bytes += add_file()
            count += 1

    print(f"Dataset prepared under {data_dir}")
