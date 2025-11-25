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
from typing import Optional

from .config import load_run_config, RunConfigModel
from .dataset import plan_and_generate
from .executor import run_profile
from .config import resolve_run_settings


console = Console()


def run_test_menu():
    """Меню запуска теста (заглушка)."""
    console.rule("[bold yellow]🚀 Запустить тест[/bold yellow]")
    console.print("[dim]Функция в разработке...[/dim]\n")
    questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()


def create_dataset_menu():
    """Меню создания датасета (заглушка)."""
    console.rule("[bold yellow]📦 Создать датасет[/bold yellow]")
    console.print("[dim]Функция в разработке...[/dim]\n")
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
