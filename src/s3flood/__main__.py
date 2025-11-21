import sys
import os

# Для PyInstaller: добавляем путь к модулю в sys.path
if getattr(sys, 'frozen', False):
    # Если запущено как exe (PyInstaller)
    base_path = sys._MEIPASS
    sys.path.insert(0, base_path)
    from s3flood.cli import main
else:
    # Если запущено как модуль
    from .cli import main

if __name__ == "__main__":
    main()
