"""Настройки приложения: файл .s3flood.yml в рабочей папке.

Хранит параметры уровня приложения (не прогона): сейчас — путь к датасету
(dataset_dir), записываемый мастером создания датасета.
"""
from __future__ import annotations

from pathlib import Path

import yaml

APP_SETTINGS_FILE = ".s3flood.yml"


def load_app_settings(cwd: Path | None = None) -> dict:
    path = (cwd or Path.cwd()) / APP_SETTINGS_FILE
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return {}
    return data if isinstance(data, dict) else {}


def save_app_settings(updates: dict, cwd: Path | None = None) -> None:
    path = (cwd or Path.cwd()) / APP_SETTINGS_FILE
    data = load_app_settings(cwd)
    data.update(updates)
    path.write_text(
        yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8"
    )


def get_dataset_dir(cwd: Path | None = None) -> str | None:
    value = load_app_settings(cwd).get("dataset_dir")
    return str(value) if value else None
