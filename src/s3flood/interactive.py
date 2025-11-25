"""
Интерактивное меню для s3flood с использованием rich и questionary.
"""
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich.text import Text
from pathlib import Path
import yaml
import questionary
import shutil
from prompt_toolkit.completion import PathCompleter
from typing import Optional

from .config import load_run_config, RunConfigModel
from .dataset import plan_and_generate
from .executor import run_profile
from .config import resolve_run_settings


console = Console()
path_completer = PathCompleter(expanduser=True, only_directories=True)


def run_test_menu():
    """Меню запуска теста (заглушка)."""
    console.rule("[bold yellow]🚀 Запустить тест[/bold yellow]")
    console.print("[dim]Функция в разработке...[/dim]\n")
    questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()


def validate_size_format(value: str) -> bool:
    """Валидация формата размера (auto, 1GB, 500MB и т.д.)."""
    if value.lower() == "auto":
        return True
    value_lower = value.lower().strip()
    units = ["kb", "mb", "gb", "tb"]
    for unit in units:
        if value_lower.endswith(unit):
            try:
                float(value_lower[:-len(unit)])
                return True
            except ValueError:
                return False
    try:
        int(value_lower)
        return True
    except ValueError:
        return False


def validate_counts_format(value: str) -> bool:
    """Валидация формата min_counts (100,50,20)."""
    try:
        parts = value.split(",")
        if len(parts) != 3:
            return False
        for part in parts:
            int(part.strip())
        return True
    except ValueError:
        return False


def validate_group_limits_format(value: str) -> bool:
    """Валидация формата group_limits (100MB,1GB,10GB)."""
    try:
        parts = value.split(",")
        if len(parts) != 3:
            return False
        for part in parts:
            part_lower = part.strip().lower()
            units = ["kb", "mb", "gb", "tb"]
            found = False
            for unit in units:
                if part_lower.endswith(unit):
                    float(part_lower[:-len(unit)])
                    found = True
                    break
            if not found:
                return False
        return True
    except ValueError:
        return False


def create_dataset_menu():
    """Мастер создания датасета."""
    console.rule("[bold yellow]📦 Создать датасет[/bold yellow]")
    
    # Путь к датасету
    path = questionary.path(
        "Укажите путь к директории для датасета:",
        completer=path_completer,
        validate=lambda p: Path(p).expanduser().parent.exists() or "Родительская директория не найдена"
    ).ask()
    if not path:
        return
    
    # Размер датасета
    target_bytes_choice = questionary.select(
        "Размер датасета:",
        choices=[
            "auto (использовать 80% свободного места)",
            "Указать вручную (например, 1GB, 500MB)"
        ]
    ).ask()
    
    if target_bytes_choice is None:
        return
    
    if "auto" in target_bytes_choice.lower():
        target_bytes = "auto"
        safety_ratio = questionary.text(
            "Доля свободного места для использования (0.1-1.0):",
            default="0.8",
            validate=lambda v: (v.replace(".", "").isdigit() and 0.1 <= float(v) <= 1.0) or "Введите число от 0.1 до 1.0"
        ).ask()
        if not safety_ratio:
            return
        safety_ratio = float(safety_ratio)
    else:
        target_bytes = questionary.text(
            "Размер датасета (например, 1GB, 500MB, 10GB):",
            default="1GB",
            validate=lambda v: validate_size_format(v) or "Неверный формат. Используйте: auto, 1GB, 500MB и т.д."
        ).ask()
        if not target_bytes:
            return
        safety_ratio = 0.8  # По умолчанию, не используется при ручном размере
    
    # Использовать симлинки
    use_symlinks = questionary.confirm(
        "Использовать символические ссылки? (экономит место, но не работает на Windows)",
        default=False
    ).ask()
    
    # Минимальные количества файлов
    min_counts = questionary.text(
        "Минимальные количества файлов для групп small,medium,large (через запятую):",
        default="100,50,20",
        validate=lambda v: validate_counts_format(v) or "Неверный формат. Используйте: 100,50,20"
    ).ask()
    if not min_counts:
        return
    
    # Лимиты размеров файлов
    group_limits = questionary.text(
        "Максимальные размеры файлов для групп small,medium,large (через запятую):",
        default="100MB,1GB,10GB",
        validate=lambda v: validate_group_limits_format(v) or "Неверный формат. Используйте: 100MB,1GB,10GB"
    ).ask()
    if not group_limits:
        return
    
    # Подтверждение
    console.print("\n[bold]Параметры датасета:[/bold]")
    summary_table = Table(show_header=False, box=None)
    summary_table.add_column(style="cyan")
    summary_table.add_column(style="white")
    summary_table.add_row("Путь:", path)
    summary_table.add_row("Размер:", target_bytes if target_bytes != "auto" else f"auto (safety_ratio={safety_ratio})")
    summary_table.add_row("Симлинки:", "Да" if use_symlinks else "Нет")
    summary_table.add_row("Минимум файлов:", min_counts)
    summary_table.add_row("Лимиты размеров:", group_limits)
    console.print(summary_table)
    
    if not questionary.confirm("\nСоздать датасет с этими параметрами?", default=True).ask():
        console.print("[yellow]Создание отменено.[/yellow]")
        return
    
    # Создание датасета
    try:
        console.print("\n[bold green]Создание датасета...[/bold green]")
        plan_and_generate(
            path=path,
            target_bytes=target_bytes,
            use_symlinks=use_symlinks,
            min_counts=min_counts,
            group_limits=group_limits,
            safety_ratio=safety_ratio
        )
        console.print("[bold green]✅ Датасет успешно создан![/bold green]")
    except Exception as e:
        console.print(f"[bold red]Ошибка при создании датасета: {e}[/bold red]")
    
    questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()


def manage_configs_menu():
    """Меню управления конфигами (заглушка)."""
    console.rule("[bold yellow]⚙️  Управление конфигами[/bold yellow]")
    console.print("[dim]Функция в разработке...[/dim]\n")
    questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()


def validate_config_menu():
    """Меню проверки конфига (заглушка)."""
    console.rule("[bold yellow]🔍 Проверить конфиг[/bold yellow]")
    console.print("[dim]Функция в разработке...[/dim]\n")
    questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()


def view_metrics_menu():
    """Меню просмотра метрик (заглушка)."""
    console.rule("[bold yellow]📋 Просмотр метрик[/bold yellow]")
    console.print("[dim]Функция в разработке...[/dim]\n")
    questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()


def run_interactive():
    """Запуск интерактивного меню."""
    while True:
        console.rule("[bold]Меню s3flood[/bold]")
        choice = questionary.select(
            "Выберите действие:",
            choices=[
                "1. 🚀 Запустить тест",
                "2. 📦 Создать датасет",
                "3. ⚙️  Управление конфигами",
                "4. 🔍 Проверить конфиг",
                "5. 📋 Просмотр метрик",
                questionary.Separator(),
                "Выход"
            ],
            use_indicator=True
        ).ask()

        if choice is None or choice == "Выход":
            console.print("[bold green]До свидания![/bold green]")
            break

        console.clear()

        if "1." in choice:
            run_test_menu()
        elif "2." in choice:
            create_dataset_menu()
        elif "3." in choice:
            manage_configs_menu()
        elif "4." in choice:
            validate_config_menu()
        elif "5." in choice:
            view_metrics_menu()

        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        console.clear()


if __name__ == "__main__":
    try:
        run_interactive()
    except (KeyboardInterrupt, EOFError):
        console.print("\n[bold yellow]Выход по запросу пользователя.[/bold yellow]")
