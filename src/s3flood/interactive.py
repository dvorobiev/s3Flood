"""
Интерактивное меню для s3flood с использованием rich.
"""
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich.text import Text
from pathlib import Path
import yaml
from typing import Optional

from .config import load_run_config, RunConfigModel
from .dataset import plan_and_generate
from .executor import run_profile
from .config import resolve_run_settings


console = Console()


def show_main_menu():
    """Показывает главное меню и возвращает выбранный пункт."""
    menu_text = Text()
    menu_text.append("1. ", style="cyan")
    menu_text.append("🚀 Запустить тест\n", style="white")
    menu_text.append("2. ", style="cyan")
    menu_text.append("📦 Создать датасет\n", style="white")
    menu_text.append("3. ", style="cyan")
    menu_text.append("⚙️  Управление конфигами\n", style="white")
    menu_text.append("4. ", style="cyan")
    menu_text.append("🔍 Проверить конфиг\n", style="white")
    menu_text.append("5. ", style="cyan")
    menu_text.append("📋 Просмотр метрик\n", style="white")
    menu_text.append("6. ", style="cyan")
    menu_text.append("❌ Выход\n", style="red")
    
    panel = Panel(menu_text, title="[bold cyan]s3flood — Interactive Menu[/bold cyan]", border_style="cyan")
    console.print(panel)
    
    choice = Prompt.ask(
        "\n[cyan]Выберите пункт[/cyan]",
        choices=["1", "2", "3", "4", "5", "6"],
        default="6"
    )
    return choice


def run_test_menu():
    """Меню запуска теста (заглушка)."""
    console.print("\n[bold yellow]Запуск теста[/bold yellow]")
    console.print("[dim]Функция в разработке...[/dim]\n")
    input("Нажмите Enter для возврата в главное меню...")


def create_dataset_menu():
    """Меню создания датасета (заглушка)."""
    console.print("\n[bold yellow]Создание датасета[/bold yellow]")
    console.print("[dim]Функция в разработке...[/dim]\n")
    input("Нажмите Enter для возврата в главное меню...")


def manage_configs_menu():
    """Меню управления конфигами (заглушка)."""
    console.print("\n[bold yellow]Управление конфигами[/bold yellow]")
    console.print("[dim]Функция в разработке...[/dim]\n")
    input("Нажмите Enter для возврата в главное меню...")


def validate_config_menu():
    """Меню проверки конфига (заглушка)."""
    console.print("\n[bold yellow]Проверка конфига[/bold yellow]")
    console.print("[dim]Функция в разработке...[/dim]\n")
    input("Нажмите Enter для возврата в главное меню...")


def view_metrics_menu():
    """Меню просмотра метрик (заглушка)."""
    console.print("\n[bold yellow]Просмотр метрик[/bold yellow]")
    console.print("[dim]Функция в разработке...[/dim]\n")
    input("Нажмите Enter для возврата в главное меню...")


def run_interactive():
    """Запуск интерактивного меню."""
    while True:
        console.clear()
        choice = show_main_menu()
        
        if choice == "1":
            run_test_menu()
        elif choice == "2":
            create_dataset_menu()
        elif choice == "3":
            manage_configs_menu()
        elif choice == "4":
            validate_config_menu()
        elif choice == "5":
            view_metrics_menu()
        elif choice == "6":
            console.print("\n[green]До свидания![/green]\n")
            break


if __name__ == "__main__":
    run_interactive()
