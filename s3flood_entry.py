#!/usr/bin/env python3
"""
Entry point для PyInstaller.
Этот файл используется только для сборки standalone exe.
"""
import sys
from pathlib import Path

# Добавляем src в путь для импорта модулей
src_path = Path(__file__).parent / "src"
if src_path.exists():
    sys.path.insert(0, str(src_path))

from s3flood.cli import main

if __name__ == "__main__":
    main()

