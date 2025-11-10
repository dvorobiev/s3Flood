import os, shutil, uuid, math
from pathlib import Path
from typing import Tuple

# Utilities to parse sizes like "100MB", "1GB"
UNITS = {"kb": 1024, "mb": 1024**2, "gb": 1024**3}

def parse_size(s: str) -> int:
    s = s.strip().lower()
    for u, mul in UNITS.items():
        if s.endswith(u):
            return int(float(s[:-len(u)]) * mul)
    return int(s)


def plan_groups(target_bytes: int, min_counts: Tuple[int,int,int]):
    shares = [0.3, 0.5, 0.2]
    base = [max(min_counts[0], 1), max(min_counts[1], 1), max(min_counts[2], 1)]
    alloc = [int(target_bytes * p) for p in shares]
    return alloc, base


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def make_seed_files(base_dir: Path, limits):
    # create a few seed files per group close to the upper bound to maximize re-use
    seeds = []
    for i, lim in enumerate(limits):
        gdir = base_dir / f"seed_g{i}"
        ensure_dir(gdir)
        size = int(lim * 0.9)
        f = gdir / "seed.bin"
        if not f.exists() or f.stat().st_size != size:
            with open(f, "wb") as fh:
                fh.seek(size-1)
                fh.write(b"\0")
        seeds.append(f)
    return seeds


def plan_and_generate(path: str, target_bytes: str, use_symlinks: bool, min_counts: str, group_limits: str, safety_ratio: float):
    base = Path(path)
    ensure_dir(base)

    mins = tuple(int(x) for x in min_counts.split(","))
    limits = tuple(parse_size(x) for x in group_limits.split(","))

    if target_bytes == "auto":
        st = shutil.disk_usage(base)
        target = int(st.free * safety_ratio)
    else:
        target = parse_size(target_bytes)

    alloc, base_counts = plan_groups(target, mins)
    seeds = make_seed_files(base / "seeds", limits)

    # create symlinked dataset structure
    data_dir = base / "data"
    ensure_dir(data_dir)

    for gi, (bytes_target, minc, lim) in enumerate(zip(alloc, mins, limits)):
        gdir = data_dir / ["small","medium","large"][gi]
        ensure_dir(gdir)
        seed = seeds[gi]
        # minimal count, then fill by symlinks until bytes_target
        total_bytes = 0
        count = 0
        # create minimal real files first (one real seed already)
        for _ in range(minc):
            key = gdir / f"{uuid.uuid4()}.bin"
            if use_symlinks:
                if not key.exists():
                    os.symlink(seed, key)
                sz = seed.stat().st_size
            else:
                with open(key, "wb") as fh:
                    fh.seek(int(lim*0.9)-1); fh.write(b"\0")
                sz = key.stat().st_size
            total_bytes += sz
            count += 1
        # fill up to bytes_target
        while total_bytes + seed.stat().st_size <= bytes_target:
            key = gdir / f"{uuid.uuid4()}.bin"
            if use_symlinks:
                os.symlink(seed, key)
                sz = seed.stat().st_size
            else:
                with open(key, "wb") as fh:
                    fh.seek(int(lim*0.9)-1); fh.write(b"\0")
                sz = key.stat().st_size
            total_bytes += sz
            count += 1

    print(f"Dataset prepared under {data_dir}")
