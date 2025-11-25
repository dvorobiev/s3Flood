"""
Интерактивное меню для s3flood с использованием textual.
"""
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import Button, Header, Footer, Static, Input, Select, Checkbox, RadioSet, RadioButton
from textual.binding import Binding
from pathlib import Path
import yaml
from typing import Optional

from .config import load_run_config, RunConfigModel
from .dataset import plan_and_generate
from .executor import run_profile
from .config import resolve_run_settings


class MainMenu(App):
    """Главное меню приложения."""
    
    CSS = """
    Screen {
        align: center middle;
    }
    
    .menu-container {
        width: 60;
        height: auto;
        border: solid $primary;
        padding: 1;
    }
    
    .menu-title {
        text-align: center;
        text-style: bold;
        margin: 1;
    }
    
    .menu-button {
        width: 100%;
        margin: 1;
    }
    """
    
    BINDINGS = [
        Binding("q", "quit", "Выход", priority=True),
    ]
    
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Container(classes="menu-container"):
            yield Static("s3flood — Interactive Menu", classes="menu-title")
            with Vertical():
                yield Button("🚀 Запустить тест", id="run-test", classes="menu-button")
                yield Button("📦 Создать датасет", id="create-dataset", classes="menu-button")
                yield Button("⚙️  Управление конфигами", id="manage-configs", classes="menu-button")
                yield Button("🔍 Проверить конфиг", id="validate-config", classes="menu-button")
                yield Button("📋 Просмотр метрик", id="view-metrics", classes="menu-button")
                yield Button("❌ Выход", id="exit", classes="menu-button", variant="error")
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id
        if button_id == "run-test":
            self.push_screen(RunTestScreen())
        elif button_id == "create-dataset":
            self.push_screen(DatasetWizardScreen())
        elif button_id == "manage-configs":
            self.push_screen(ConfigManagerScreen())
        elif button_id == "validate-config":
            self.push_screen(ConfigValidatorScreen())
        elif button_id == "view-metrics":
            self.push_screen(MetricsViewerScreen())
        elif button_id == "exit":
            self.exit()
    
    def action_quit(self) -> None:
        self.exit()


class RunTestScreen(App):
    """Экран запуска теста."""
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Запуск теста (заглушка)", id="title")
        yield Button("Назад", id="back")
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back":
            self.dismiss()


class DatasetWizardScreen(App):
    """Мастер создания датасета."""
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Создание датасета (заглушка)", id="title")
        yield Button("Назад", id="back")
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back":
            self.dismiss()


class ConfigManagerScreen(App):
    """Управление конфигами."""
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Управление конфигами (заглушка)", id="title")
        yield Button("Назад", id="back")
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back":
            self.dismiss()


class ConfigValidatorScreen(App):
    """Проверка конфига."""
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Проверка конфига (заглушка)", id="title")
        yield Button("Назад", id="back")
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back":
            self.dismiss()


class MetricsViewerScreen(App):
    """Просмотр метрик."""
    
    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Просмотр метрик (заглушка)", id="title")
        yield Button("Назад", id="back")
        yield Footer()
    
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back":
            self.dismiss()


def run_interactive():
    """Запуск интерактивного меню."""
    app = MainMenu()
    app.run()


if __name__ == "__main__":
    run_interactive()

