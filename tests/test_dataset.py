import random
import zlib

import pytest

from s3flood.dataset import parse_size, plan_file_sizes, plan_and_generate, write_random_file


class TestParseSize:
    def test_units(self):
        assert parse_size("1kb") == 1024
        assert parse_size("2MB") == 2 * 1024**2
        assert parse_size("1.5GB") == int(1.5 * 1024**3)
        assert parse_size("100") == 100


class TestPlanFileSizes:
    def test_sizes_within_bounds(self):
        rng = random.Random(42)
        sizes = plan_file_sizes(lower=1024, upper=10240, count=100, rng=rng)
        assert len(sizes) == 100
        assert all(1024 <= s <= 10240 for s in sizes)

    def test_sizes_are_varied(self):
        # регрессия: раньше все файлы группы были ровно одного размера (0.9*limit)
        rng = random.Random(42)
        sizes = plan_file_sizes(lower=1024, upper=1024**2, count=50, rng=rng)
        assert len(set(sizes)) > 10

    def test_degenerate_range(self):
        rng = random.Random(1)
        sizes = plan_file_sizes(lower=500, upper=500, count=3, rng=rng)
        assert sizes == [500, 500, 500]


class TestWriteRandomFile:
    def test_exact_size(self, tmp_path):
        f = tmp_path / "x.bin"
        write_random_file(f, 123_456)
        assert f.stat().st_size == 123_456

    def test_content_incompressible(self, tmp_path):
        # регрессия: раньше файлы были из нулей — компрессия/дедуп на стороне
        # хранилища давали ложно-высокие результаты
        f = tmp_path / "x.bin"
        write_random_file(f, 256 * 1024)
        data = f.read_bytes()
        ratio = len(zlib.compress(data)) / len(data)
        assert ratio > 0.95


class TestPlanAndGenerate:
    def test_random_fill_with_varied_sizes(self, tmp_path):
        plan_and_generate(
            path=str(tmp_path),
            target_bytes="600KB",
            use_symlinks=False,
            min_counts="3,2,1",
            group_limits="10KB,50KB,200KB",
            safety_ratio=0.8,
        )
        data_dir = tmp_path / "data"
        small_files = list((data_dir / "small").glob("*.bin"))
        assert len(small_files) >= 3
        sizes = {f.stat().st_size for f in small_files}
        assert all(s <= 10 * 1024 for s in sizes)
        # содержимое не нулевое
        sample = small_files[0].read_bytes()
        assert any(b != 0 for b in sample)

    def test_zero_fill_compat(self, tmp_path):
        plan_and_generate(
            path=str(tmp_path),
            target_bytes="100KB",
            use_symlinks=False,
            min_counts="2,1,1",
            group_limits="5KB,20KB,50KB",
            safety_ratio=0.8,
            fill="zero",
        )
        small_files = list((tmp_path / "data" / "small").glob("*.bin"))
        assert small_files
        assert all(b == 0 for b in small_files[0].read_bytes())

    def test_symlink_mode_has_multiple_seed_sizes(self, tmp_path):
        plan_and_generate(
            path=str(tmp_path),
            target_bytes="2MB",
            use_symlinks=True,
            min_counts="5,2,1",
            group_limits="10KB,50KB,100KB",
            safety_ratio=0.8,
        )
        small_files = list((tmp_path / "data" / "small").glob("*.bin"))
        assert all(f.is_symlink() for f in small_files)
        sizes = {f.stat().st_size for f in small_files}
        # symlink-режим должен давать хотя бы несколько разных размеров
        assert len(sizes) >= 2
