#!/usr/bin/env python3
from pathlib import Path
import uuid

def ensure_file_size(path: Path, size: int):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size != size:
        with open(path, "wb") as fh:
            fh.seek(size - 1)
            fh.write(b"\0")

def main():
    base = Path("loadset")
    seeds_dir = base / "seeds"
    data_dir = base / "data"
    seeds_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    # sizes: small≈100MB, medium≈1GB, large≈10GB (по 90% лимита, чтобы не упираться)
    sizes = [
        int(100 * 1024**2 * 0.9),
        int(1 * 1024**3 * 0.9),
        int(10 * 1024**3 * 0.9),
    ]
    groups = ["small", "medium", "large"]
    per_group_links = [100, 50, 20]  # можно увеличить по необходимости

    seed_paths = []
    for i, size in enumerate(sizes):
        seed = seeds_dir / f"seed_g{i}" / "seed.bin"
        ensure_file_size(seed, size)
        seed_paths.append(seed.resolve())

    for i, group in enumerate(groups):
        gdir = data_dir / group
        gdir.mkdir(parents=True, exist_ok=True)
        seed_abs = seed_paths[i]
        need = per_group_links[i]
        created = 0
        while created < need:
            key = gdir / f"{uuid.uuid4()}.bin"
            if key.exists():
                continue
            # Абсолютная цель, чтобы ссылки были устойчивыми при любом cwd
            key.symlink_to(seed_abs)
            created += 1

    print(f"dataset ok: {data_dir}")

if __name__ == "__main__":
    main()
    
