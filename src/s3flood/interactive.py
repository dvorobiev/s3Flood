"""
Интерактивное меню для s3flood с использованием rich и questionary.
"""
from rich.console import Console
from rich.table import Table
from pathlib import Path
import argparse
import os
import subprocess
import time
import threading
import yaml

from .defaults import DEFAULT_S3_PORT
import questionary
import csv
import statistics
from typing import Optional
from urllib.parse import urlparse, urlunparse
from prompt_toolkit import prompt as pt_prompt
from prompt_toolkit.completion import PathCompleter
from prompt_toolkit.formatted_text import HTML

from .config import load_run_config, resolve_run_settings
from .config_editor import build_default_config, edit_config_interactively
from .dataset import plan_and_generate
from .executor import run_profile, get_spinner
from .runner import _get_aws_env, aws_check_bucket_access, aws_list_objects

PROJECT_ROOT = Path(__file__).resolve().parents[2]


console = Console()
path_completer = PathCompleter(expanduser=True, only_directories=True)
ANSI_RESET = "\x1b[0m"
ANSI_BOLD = "\x1b[1m"
ANSI_CYAN = "\x1b[36m"
ANSI_YELLOW = "\x1b[33m"


def supports_emoji() -> bool:
    """Проверяет, поддерживает ли терминал эмодзи."""
    try:
        # Если терминал поддерживает UTF-8, скорее всего поддерживает эмодзи
        if "UTF" in os.environ.get("LANG", "").upper() or "UTF" in os.environ.get("LC_ALL", "").upper():
            return True
        # Проверяем через попытку вывода эмодзи
        import sys
        if hasattr(sys.stdout, "encoding") and sys.stdout.encoding:
            return "utf" in sys.stdout.encoding.lower()
        return False
    except Exception:
        return False


def get_menu_emoji(emoji: str, fallback: str = "") -> str:
    """Возвращает эмодзи или fallback в зависимости от поддержки."""
    return emoji if supports_emoji() else fallback


def format_bytes_to_readable(bytes_val: Optional[int]) -> str:
    """Конвертирует байты в читаемый формат (MB или GB)."""
    if bytes_val is None:
        return "не задано"
    mb = bytes_val / (1024 * 1024)
    if mb >= 1024:
        return f"{mb / 1024:.1f} GB"
    return f"{int(mb)} MB"


class DotSpinner:
    """Простой спиннер для индикации длительных операций."""

    def __init__(self):
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def __enter__(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        # Очищаем строку со спиннером
        console.file.write("\r" + " " * 80 + "\r")
        console.file.flush()

    def _run(self):
        while not self._stop.is_set():
            # Используем тот же спиннер, что и в дашборде
            frame = get_spinner()
            console.file.write(f"\r{frame}")
            console.file.flush()
            time.sleep(0.1)


def prompt_inline(message: str, default_value: str = "", allow_empty: bool = True) -> Optional[str]:
    prompt_msg = HTML(f"<ansiyellow>{message}</ansiyellow> ")
    try:
        answer = pt_prompt(prompt_msg, default=default_value or "")
    except (KeyboardInterrupt, EOFError):
        return None
    if not allow_empty and not (answer or "").strip():
        console.print("[red]Значение не может быть пустым.[/red]")
        return prompt_inline(message, default_value, allow_empty)
    return answer


def normalize_endpoint_url(value: str) -> str:
    if not value:
        return value
    raw = value.strip()
    if not raw:
        return raw
    if not raw.startswith(("http://", "https://")):
        raw = f"http://{raw}"
    parsed = urlparse(raw)
    netloc = parsed.netloc or parsed.path
    path = parsed.path if parsed.netloc else ""
    if ":" not in netloc:
        netloc = f"{netloc}:{DEFAULT_S3_PORT}"
    parsed = parsed._replace(
        scheme=parsed.scheme or "http",
        netloc=netloc,
        path=path,
    )
    return urlunparse(parsed)


def run_test_menu():
    """Меню запуска теста с выбором конфига и профиля."""
    console.clear()
    console.rule("[bold yellow]🚀 Запустить тест[/bold yellow]")

    # Ищем YAML-конфиги в текущей директории
    cwd = Path(".").resolve()
    configs = sorted(list(cwd.glob("config*.yml")) + list(cwd.glob("config*.yaml")))
    choices = [str(cfg.name) for cfg in configs]
    choices.append(f"{get_menu_emoji('📂', '[+]')} Ввести путь вручную")
    # Разделяем список конфигов и пункт возврата в меню, как в других подменю
    choices.append(questionary.Separator())
    choices.append(f"{get_menu_emoji('⬅️', '[0]')} Вернуться в главное меню")

    console.print(
        "[dim]Стрелки — выбрать конфиг, Enter — продолжить, ⬅️ Вернуться в главное меню — выйти без запуска.[/dim]\n"
    )

    choice = questionary.select(
        "Выберите конфиг:",
        choices=choices,
        use_indicator=False,
    ).ask()
    if not choice or choice.startswith("⬅️"):
        return

    if choice.startswith("📂"):
        config_path = questionary.path(
            "Укажите путь к YAML-конфигу (например, config.yaml):",
            completer=path_completer,
            validate=lambda p: Path(p).expanduser().exists() or "Файл не найден",
        ).ask()
        if not config_path:
            return
    else:
        config_path = str(cwd / choice)

    # Выбор профиля нагрузки
    profile = questionary.select(
        "Выберите профиль нагрузки:",
        choices=[
            f"{get_menu_emoji('🔺', 'W')} write — только запись",
            f"{get_menu_emoji('🔻', 'R')} read  — только чтение",
            f"{get_menu_emoji('🔀', 'M')} mixed — смешанный профиль",
        ],
        use_indicator=False
    ).ask()
    if not profile:
        return

    if profile.startswith("🔺"):
        profile_value = "write"
    elif profile.startswith("🔻"):
        profile_value = "read"
    else:
        profile_value = "mixed"

    # Загружаем конфиг
    try:
        config_model = load_run_config(config_path)
    except (OSError, ValueError) as exc:
        console.print(f"[bold red]Не удалось прочитать конфиг: {exc}[/bold red]")
        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        return

    # Псевдо-CLI аргументы: всё берём из конфига, кроме profile
    # (resolve_run_settings читает атрибуты через getattr с дефолтом None)
    cli_args = argparse.Namespace(profile=profile_value)

    try:
        settings = resolve_run_settings(cli_args, config_model)
    except SystemExit as exc:
        # Ошибка валидации настроек (например, не указан bucket)
        console.print(f"[bold red]Ошибка конфигурации: {exc}[/bold red]")
        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        return

    console.print(
        f"\n[bold]Запуск профиля[/bold] [cyan]{settings.profile}[/cyan] "
        f"с конфигом [magenta]{config_path}[/magenta]"
    )

    # Краткое резюме перед запуском
    console.print("\n[bold]Параметры запуска:[/bold]")
    summary_table = Table(show_header=False, box=None)
    summary_table.add_column(style="cyan")
    summary_table.add_column(style="white")

    # Эндпоинты
    if settings.endpoints:
        endpoints_str = "\n".join(str(ep) for ep in settings.endpoints)
    else:
        endpoints_str = str(settings.endpoint)
    summary_table.add_row("Endpoint(ы):", endpoints_str)

    # Бакет и профиль
    summary_table.add_row("Bucket:", settings.bucket)
    summary_table.add_row("Профиль:", settings.profile)
    summary_table.add_row("Data_dir:", settings.data_dir)
    summary_table.add_row("Threads:", str(settings.threads))
    summary_table.add_row("Infinite:", "yes" if settings.infinite else "no")
    summary_table.add_row("unique_remote_names:", "yes" if settings.unique_remote_names else "no")
    if settings.profile == "mixed":
        summary_table.add_row("mixed_read_ratio:", str(settings.mixed_read_ratio))
    # AWS CLI параметры
    if settings.aws_cli_multipart_threshold is not None or settings.aws_cli_multipart_chunksize is not None or settings.aws_cli_max_concurrent_requests is not None:
        summary_table.add_row("", "")  # Пустая строка для разделения
        summary_table.add_row("AWS CLI параметры:", "")
        if settings.aws_cli_multipart_threshold is not None:
            summary_table.add_row("  multipart_threshold:", format_bytes_to_readable(settings.aws_cli_multipart_threshold))
        if settings.aws_cli_multipart_chunksize is not None:
            summary_table.add_row("  multipart_chunksize:", format_bytes_to_readable(settings.aws_cli_multipart_chunksize))
        if settings.aws_cli_max_concurrent_requests is not None:
            summary_table.add_row("  max_concurrent_requests:", str(settings.aws_cli_max_concurrent_requests))
    console.print(summary_table)

    # Возможность переопределить ключевые параметры перед запуском
    params_changed = False
    if questionary.confirm("Изменить параметры перед запуском?", default=False).ask():
        params_changed = True
        # data_dir
        data_dir_new = questionary.path(
            "Каталог датасета (data_dir):",
            default=str(settings.data_dir),
            completer=path_completer,
            validate=lambda p: Path(p).expanduser().is_dir() or "Каталог не существует",
        ).ask()
        if data_dir_new:
            settings.data_dir = str(Path(data_dir_new).expanduser())

        # threads
        threads_str = questionary.text(
            "Число потоков (threads):",
            default=str(settings.threads),
            validate=lambda v: (v.isdigit() and int(v) > 0) or "Введите целое число > 0",
        ).ask()
        if threads_str:
            settings.threads = int(threads_str)

        # infinite
        infinite_new = questionary.confirm(
            "Бесконечный режим (infinite)?",
            default=bool(settings.infinite),
        ).ask()
        settings.infinite = bool(infinite_new)

        # unique_remote_names
        urn_new = questionary.confirm(
            "Уникальные имена объектов (unique_remote_names)?",
            default=bool(settings.unique_remote_names),
        ).ask()
        settings.unique_remote_names = bool(urn_new)

        # mixed_read_ratio только для mixed
        if settings.profile == "mixed":
            mrr_default = settings.mixed_read_ratio if settings.mixed_read_ratio is not None else 0.7
            mrr_str = questionary.text(
                "mixed_read_ratio (0.0 - 1.0):",
                default=str(mrr_default),
                validate=lambda v: (
                    v.strip() == ""
                    or (v.replace(".", "", 1).isdigit() and 0.0 <= float(v) <= 1.0)
                    or "Введите число от 0.0 до 1.0 или оставьте пустым"
                ),
            ).ask() or str(mrr_default)
            if mrr_str.strip() != "":
                settings.mixed_read_ratio = float(mrr_str)

        console.print("\n[bold]Итоговые параметры запуска:[/bold]")
        final_table = Table(show_header=False, box=None)
        final_table.add_column(style="cyan")
        final_table.add_column(style="white")
        final_table.add_row("Bucket:", settings.bucket)
        final_table.add_row("Профиль:", settings.profile)
        final_table.add_row("Data_dir:", settings.data_dir)
        final_table.add_row("Threads:", str(settings.threads))
        final_table.add_row("Infinite:", "yes" if settings.infinite else "no")
        final_table.add_row("unique_remote_names:", "yes" if settings.unique_remote_names else "no")
        if settings.profile == "mixed":
            final_table.add_row("mixed_read_ratio:", str(settings.mixed_read_ratio))
        # AWS CLI параметры
        if settings.aws_cli_multipart_threshold is not None or settings.aws_cli_multipart_chunksize is not None or settings.aws_cli_max_concurrent_requests is not None:
            final_table.add_row("", "")  # Пустая строка для разделения
            final_table.add_row("AWS CLI параметры:", "")
            if settings.aws_cli_multipart_threshold is not None:
                final_table.add_row("  multipart_threshold:", format_bytes_to_readable(settings.aws_cli_multipart_threshold))
            if settings.aws_cli_multipart_chunksize is not None:
                final_table.add_row("  multipart_chunksize:", format_bytes_to_readable(settings.aws_cli_multipart_chunksize))
            if settings.aws_cli_max_concurrent_requests is not None:
                final_table.add_row("  max_concurrent_requests:", str(settings.aws_cli_max_concurrent_requests))
        console.print(final_table)
    else:
        # Если параметры не меняли, сразу запускаем без дополнительного промпта
        console.print()

    # Показываем промпт только если меняли параметры
    if params_changed:
        questionary.press_any_key_to_continue("Нажмите любую клавишу для запуска...").ask()

    # Запуск профиля (у самого теста уже есть свой спиннер в дашборде)
    try:
        run_profile(settings.to_namespace())
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Остановка по запросу пользователя.[/bold yellow]")
    except Exception as exc:
        console.print(f"[bold red]Ошибка во время выполнения профиля: {exc}[/bold red]")

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
    console.clear()
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
        console.print()
        console.print("[dim]Создание датасета...[/dim]")
        with DotSpinner():
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


def create_config_wizard():
    """Создание нового конфига через inline-редактор."""
    console.clear()
    console.rule(f"[bold yellow]{get_menu_emoji('📝', '')} Создать новый конфиг[/bold yellow]")

    default_name = "config.new.yaml"
    target_path = questionary.text(
        "Имя файла конфига:",
        default=default_name,
    ).ask()
    if not target_path:
        return
    target_path = str(Path(target_path).expanduser())

    if Path(target_path).exists():
        overwrite = questionary.confirm(
            f"Файл {target_path} уже существует. Перезаписать?", default=False
        ).ask()
        if not overwrite:
            console.print("[yellow]Создание конфига отменено.[/yellow]")
            return

    base_cfg = build_default_config()
    edited = edit_config_interactively(base_cfg, f"Создать конфиг: {target_path}")
    if edited is None:
        console.print("[yellow]Создание конфига отменено.[/yellow]")
        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        return

    config_obj = {"run": edited}
    try:
        with open(target_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(config_obj, f, sort_keys=False, allow_unicode=True)
        console.print(f"[bold green]✅ Конфиг сохранён в {target_path}[/bold green]")
    except Exception as exc:
        console.print(f"[bold red]Ошибка при сохранении конфига: {exc}[/bold red]")

    questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()


def manage_configs_menu():
    """Меню управления и проверки конфигов."""
    while True:
        console.clear()
        console.rule("[bold yellow]🧩 Конфиги и проверка[/bold yellow]")
        choice = questionary.select(
            "Выберите действие:",
            choices=[
                f"{get_menu_emoji('📝', '[1]')} Создать новый конфиг",
                f"{get_menu_emoji('🔍', '[2]')} Проверить / управлять конфигом",
                f"{get_menu_emoji('✏️', '[3]')} Редактировать существующий конфиг",
                questionary.Separator(),
                f"{get_menu_emoji('⬅️', '[0]')} Вернуться в главное меню",
            ],
            use_indicator=False,
        ).ask()

        if not choice or get_menu_emoji("⬅️", "[0]") in choice or choice.startswith("⬅️"):
            return

        if get_menu_emoji("📝", "[1]") in choice or "Создать новый конфиг" in choice:
            create_config_wizard()
        elif get_menu_emoji("🔍", "[2]") in choice or "Проверить / управлять конфигом" in choice:
            validate_config_menu()
        elif get_menu_emoji("✏️", "[3]") in choice or "Редактировать существующий конфиг" in choice:
            edit_config_menu()
        else:
            return


def edit_config_menu():
    """Редактирование существующего конфига через inline-редактор (prompt_toolkit)."""
    console.clear()
    console.rule(f"[bold yellow]{get_menu_emoji('✏️', '')} Редактировать конфиг[/bold yellow]")

    cwd = Path('.').resolve()
    configs = sorted(list(cwd.glob('config*.yml')) + list(cwd.glob('config*.yaml')))
    choices = [str(cfg.name) for cfg in configs]
    choices.append(f"{get_menu_emoji('📂', '[+]' )} Ввести путь вручную")
    choices.append(f"{get_menu_emoji('⬅️', '[0]')} Вернуться в главное меню")

    selected = questionary.select(
        "Выберите конфиг для редактирования:",
        choices=choices,
        use_indicator=False,
    ).ask()
    if not selected or selected.startswith('⬅️'):
        return

    if selected.startswith('📂'):
        path_str = questionary.path(
            "Путь к YAML-конфигу:",
            completer=path_completer,
            validate=lambda p: Path(p).is_file() or 'Файл не найден',
        ).ask()
        if not path_str:
            return
        cfg_path = Path(path_str).expanduser()
    else:
        cfg_path = cwd / selected

    # Отрисовываем заголовок и подсказку в едином стиле, как в остальных меню
    console.clear()
    console.rule(f"[bold yellow]{get_menu_emoji('✏️', '')} Редактировать конфиг[/bold yellow]")
    console.print(f"[bold]Файл:[/bold] [cyan]{cfg_path}[/cyan]")
    console.print(
        "[dim]Enter — изменить значение. Для переключаемых полей (Yes/No, порядок) переключение происходит сразу.[/dim]\n"
    )

    # Читаем YAML как словарь, чтобы сохранить структуру файла
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            raw_cfg = yaml.safe_load(f) or {}
    except Exception as exc:
        console.print(f"[bold red]Не удалось прочитать конфиг: {exc}[/bold red]")
        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        return

    run_section = raw_cfg.get("run") or {}
    if isinstance(run_section, list):
        # Секция run записана как список — берём первый dict-элемент
        run_section = next((item for item in run_section if isinstance(item, dict)), {})
    # Внутри редактора показываем только список параметров; заголовок уже нарисован выше.
    edited, cancelled_with_changes = edit_config_interactively(run_section, str(cfg_path))
    if edited is None:
        if cancelled_with_changes:
            console.print("[yellow]Изменения отменены.[/yellow]")
            questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        return

    raw_cfg["run"] = edited
    try:
        with open(cfg_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(raw_cfg, f, sort_keys=False, allow_unicode=True)
        console.print(f"[bold green]✅ Конфиг сохранён: {cfg_path}[/bold green]")
    except Exception as exc:
        console.print(f"[bold red]Ошибка при сохранении конфига: {exc}[/bold red]")

    questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()

def validate_config_menu():
    """Меню проверки конфига: базовая валидация и работа с бакетом."""
    console.clear()
    console.rule(f"[bold yellow]{get_menu_emoji('🔍', '')} Проверить конфиг[/bold yellow]")

    # Выбор конфига (список config*.yml/yaml + ручной ввод)
    cwd = Path(".").resolve()
    configs = sorted(list(cwd.glob("config*.yml")) + list(cwd.glob("config*.yaml")))
    choices = [str(cfg.name) for cfg in configs]
    choices.append(f"{get_menu_emoji('📂', '[+]')} Ввести путь вручную")
    choices.append(f"{get_menu_emoji('⬅️', '[0]')} Вернуться в главное меню")

    choice = questionary.select(
        "Выберите конфиг:",
        choices=choices,
        use_indicator=False,
    ).ask()
    if not choice or choice.startswith("⬅️"):
        return

    if choice.startswith("📂"):
        config_path = questionary.path(
            "Укажите путь к YAML-конфигу (например, config.yaml):",
            completer=path_completer,
            validate=lambda p: Path(p).expanduser().exists() or "Файл не найден",
        ).ask()
        if not config_path:
            return
    else:
        config_path = str(cwd / choice)

    # Загрузка конфига
    try:
        config_model = load_run_config(config_path)
    except (OSError, ValueError) as exc:
        console.print(f"[bold red]Не удалось прочитать конфиг: {exc}[/bold red]")
        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        return

    # Собираем настройки (write-профиль по умолчанию — профиль здесь не важен)
    cli_args = argparse.Namespace(profile="write")

    try:
        settings = resolve_run_settings(cli_args, config_model)
    except SystemExit as exc:
        console.print(f"[bold red]Ошибка конфигурации: {exc}[/bold red]")
        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        return

    endpoints_list = list(settings.endpoints or [])
    primary_endpoint = endpoints_list[0] if endpoints_list else settings.endpoint

    console.print("\n[bold]Базовая информация:[/bold]")
    info_table = Table(show_header=False, box=None)
    info_table.add_column(style="cyan")
    info_table.add_column(style="white")
    info_table.add_row("Bucket:", settings.bucket)
    info_table.add_row("Endpoint:", primary_endpoint or "<не задан>")
    info_table.add_row("Threads:", str(settings.threads))
    info_table.add_row("Data_dir:", str(settings.data_dir))
    console.print(info_table)

    # Проверка конфигурации endpoint'а
    if not primary_endpoint:
        console.print("[bold red]Endpoint не настроен — подключение к S3 невозможно.[/bold red]")
        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        return

    # Подключение к S3 и вывод первых 5 объектов
    # Сначала отдельный быстрый тест доступа
    console.print("\n[bold]Проверка доступа к бакету (head-bucket)...[/bold]")
    try:
        head_res = aws_check_bucket_access(
            settings.bucket,
            primary_endpoint,
            settings.access_key,
            settings.secret_key,
            settings.aws_profile,
            settings.aws_cli_multipart_threshold,
            settings.aws_cli_multipart_chunksize,
            settings.aws_cli_max_concurrent_requests,
        )
    except Exception as exc:
        console.print(f"[bold red]Ошибка при проверке доступа: {exc}[/bold red]")
        head_res = None

    if head_res is not None and head_res.returncode == 0:
        console.print("[bold green]Доступ к бакету подтверждён (head-bucket успешен).[/bold green]")
    else:
        # Если команда отработала, но вернула ошибку — показываем stderr
        msg = head_res.stderr.strip() if head_res is not None else ""
        console.print(
            "[bold red]Не удалось подтвердить доступ к бакету (head-bucket завершился с ошибкой).[/bold red]"
        )
        if msg:
            console.print(f"[red]{msg}[/red]")
        questionary.press_any_key_to_continue(
            "Нажмите любую клавишу для возврата в меню..."
        ).ask()
        return

    # Проверка локального датасета
    console.print("\n[bold]Проверка датасета (data_dir)...[/bold]")
    data_root = Path(settings.data_dir).expanduser()
    if not data_root.exists():
        console.print(f"[bold red]Каталог датасета не найден:[/bold red] [cyan]{data_root}[/cyan]")
    else:
        # Подсчёт файлов и общего объёма
        total_files = 0
        total_bytes = 0
        try:
            for p in data_root.rglob("*"):
                if p.is_file():
                    total_files += 1
                    try:
                        total_bytes += p.stat().st_size
                    except OSError:
                        continue
            size_gb = total_bytes / 1024 / 1024 / 1024 if total_bytes > 0 else 0.0
            console.print(
                f"[green]Каталог датасета найден:[/green] [cyan]{data_root}[/cyan] "
                f"(файлов: {total_files}, объём: {size_gb:.2f} GB)"
            )
        except OSError as exc:
            console.print(f"[bold red]Ошибка при обходе датасета: {exc}[/bold red]")

    console.print("\n[bold]Пробуем получить список объектов из бакета...[/bold]")
    try:
        objects = aws_list_objects(
            settings.bucket,
            primary_endpoint,
            settings.access_key,
            settings.secret_key,
            settings.aws_profile,
            settings.aws_cli_multipart_threshold,
            settings.aws_cli_multipart_chunksize,
            settings.aws_cli_max_concurrent_requests,
        )
    except Exception as exc:
        console.print(f"[bold red]Ошибка при запросе списка объектов: {exc}[/bold red]")
        objects = None

    # Разделяем ситуации: ошибка / пустой список / есть объекты
    if objects is None:
        console.print(
            "[yellow]Не удалось получить список объектов (возможна проблема с доступом или AWS CLI).[/yellow]"
        )
        questionary.press_any_key_to_continue(
            "Нажмите любую клавишу для возврата в меню..."
        ).ask()
        return

    if len(objects) == 0:
        console.print(
            "[green]Подключение к бакету и endpoint успешно,[/green] "
            "[yellow]но в бакете сейчас нет ни одного объекта.[/yellow]"
        )
        questionary.press_any_key_to_continue(
            "Нажмите любую клавишу для возврата в меню..."
        ).ask()
        return

    console.print(f"[green]Найдено объектов:[/green] {len(objects)}")
    preview_count = min(len(objects), 5)
    preview = objects[:preview_count]
    table = Table(title=f"Первые {preview_count} объектов", box=None)
    table.add_column("Key", style="cyan")
    table.add_column("Size (MB)", style="white", justify="right")
    for obj in preview:
        size_mb = obj.get("size", 0) / 1024 / 1024
        table.add_row(obj.get("key", ""), f"{size_mb:.2f}")
    console.print(table)

    # Дополнительные действия
    action = questionary.select(
        "\nДополнительные действия:",
        choices=[
            f"{get_menu_emoji('⬅️', '[0]')} Вернуться в главное меню",
            "Удалить ВСЕ объекты из бакета",
        ],
    ).ask()

    if not action or action.startswith("⬅️"):
        # Просто выходим без лишнего подтверждения
        return

    if action == "Удалить ВСЕ объекты из бакета":
        console.print(
            f"\n[bold red]ВНИМАНИЕ: будет выполнено полное удаление всех объектов из бакета "
            f"[cyan]{settings.bucket}[/cyan] через endpoint [magenta]{primary_endpoint}[/magenta].[/bold red]"
        )
        if not questionary.confirm("Вы уверены, что хотите продолжить?", default=False).ask():
            console.print("[yellow]Удаление отменено.[/yellow]")
            questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
            return
        if not questionary.confirm("Это удалит ВСЕ данные в бакете. Продолжить?", default=False).ask():
            console.print("[yellow]Удаление отменено.[/yellow]")
            questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
            return

        env, profile_name = _get_aws_env(
            settings.access_key,
            settings.secret_key,
            settings.aws_profile,
            getattr(settings, "aws_cli_multipart_threshold", None),
            getattr(settings, "aws_cli_multipart_chunksize", None),
            getattr(settings, "aws_cli_max_concurrent_requests", None),
        )
        bucket_name = settings.bucket.replace("s3://", "").split("/")[0]
        url = f"s3://{bucket_name}"
        cmd = [
            "aws",
            "s3",
            "rm",
            url,
            "--recursive",
            "--endpoint-url",
            primary_endpoint,
        ]
        if profile_name:
            cmd.extend(["--profile", profile_name])
        console.print(f"\n[bold]Выполняем:[/bold] {' '.join(cmd)}\n")
        try:
            with DotSpinner():
                res = subprocess.run(cmd, env=env, capture_output=True, text=True)
            if res.returncode == 0:
                console.print("[bold green]✅ Все объекты в бакете удалены (команда aws s3 rm вернула 0).[/bold green]")
            else:
                console.print(
                    f"[bold red]Команда завершилась с кодом {res.returncode}[/bold red]\n"
                    f"stdout:\n{res.stdout}\n\nstderr:\n{res.stderr}"
                )
        except Exception as exc:
            console.print(f"[bold red]Ошибка при выполнении удаления: {exc}[/bold red]")

        # После операции удаления даём пользователю прочитать вывод
        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()


def view_metrics_menu():
    """Меню просмотра метрик: базовый анализ CSV."""
    console.clear()
    console.rule("[bold yellow]📊 Просмотр метрик[/bold yellow]")

    cwd = Path(".").resolve()
    csv_files = sorted(cwd.glob("*.csv"))
    if not csv_files:
        console.print("[yellow]В текущем каталоге нет CSV-файлов с метриками.[/yellow]\n")
        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        return

    choices = [f.name for f in csv_files]
    choices.append(f"{get_menu_emoji('⬅️', '[0]')} Вернуться в главное меню")

    choice = questionary.select(
        "Выберите CSV с метриками:",
        choices=choices,
        use_indicator=False,
    ).ask()
    if not choice or choice.startswith("⬅️"):
        return

    metrics_path = cwd / choice

    # Читаем CSV и считаем базовую статистику
    ops = []
    try:
        with metrics_path.open("r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                try:
                    ts_start = float(row.get("ts_start", "0") or 0.0)
                    ts_end = float(row.get("ts_end", "0") or 0.0)
                    op = row.get("op") or ""
                    bytes_v = int(row.get("bytes", "0") or 0)
                    status = row.get("status") or ""
                    latency_ms = float(row.get("latency_ms", "0") or 0.0)
                    error = row.get("error") or ""
                except ValueError:
                    continue
                duration_s = max(ts_end - ts_start, 0.0)
                speed_MBps = (bytes_v / 1024 / 1024) / duration_s if duration_s > 0 else 0.0
                ops.append(
                    {
                        "ts_start": ts_start,
                        "ts_end": ts_end,
                        "op": op,
                        "bytes": bytes_v,
                        "status": status,
                        "latency_ms": latency_ms,
                        "error": error,
                        "duration_s": duration_s,
                        "speed_MBps": speed_MBps,
                    }
                )
    except OSError as exc:
        console.print(f"[bold red]Не удалось прочитать файл метрик: {exc}[/bold red]")
        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        return

    if not ops:
        console.print("[yellow]В файле не найдено ни одной операции.[/yellow]\n")
        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        return

    ts_min = min(o["ts_start"] for o in ops)
    ts_max = max(o["ts_end"] for o in ops)
    total_duration = max(ts_max - ts_min, 0.0)

    ok_ops = [o for o in ops if o["status"] == "ok"]
    err_ops = [o for o in ops if o["status"] != "ok"]

    ok_bytes = sum(o["bytes"] for o in ok_ops)

    speeds = [o["speed_MBps"] for o in ok_ops if o["speed_MBps"] > 0]
    avg_speed = sum(speeds) / len(speeds) if speeds else 0.0
    median_speed = statistics.median(speeds) if speeds else 0.0
    p90_speed = statistics.quantiles(speeds, n=10)[-1] if len(speeds) >= 10 else 0.0

    console.print(f"\n[bold]Файл метрик:[/bold] [cyan]{metrics_path}[/cyan]\n")

    summary = Table(show_header=False, box=None)
    summary.add_column(style="cyan")
    summary.add_column(style="white")
    summary.add_row("Всего операций:", str(len(ops)))
    summary.add_row("Успешных:", str(len(ok_ops)))
    summary.add_row("С ошибкой:", str(len(err_ops)))
    summary.add_row("Всего байт (успешные):", f"{ok_bytes / 1024 / 1024 / 1024:.2f} GB")
    summary.add_row("Общая длительность:", f"{total_duration:.2f} s")
    summary.add_row("Средняя скорость (по операциям):", f"{avg_speed:.1f} MB/s")
    summary.add_row("Медиана по скорости:", f"{median_speed:.1f} MB/s")
    summary.add_row("P90 по скорости:", f"{p90_speed:.1f} MB/s")
    console.print(summary)

    # Покажем топ-10 самых быстрых операций
    top_n = 10
    fast_ops = sorted(ok_ops, key=lambda o: o["speed_MBps"], reverse=True)[:top_n]
    if fast_ops:
        table = Table(title=f"Топ-{top_n} по скорости", box=None)
        table.add_column("op", style="cyan")
        table.add_column("size (GB)", justify="right")
        table.add_column("duration (s)", justify="right")
        table.add_column("speed (MB/s)", justify="right")
        for o in fast_ops:
            table.add_row(
                o["op"],
                f"{o['bytes'] / 1024 / 1024 / 1024:.2f}",
                f"{o['duration_s']:.2f}",
                f"{o['speed_MBps']:.1f}",
            )
        console.print()
        console.print(table)

    questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()


def run_interactive():
    """Запуск интерактивного меню."""
    while True:
        console.clear()
        console.rule("[bold]Меню s3flood[/bold]")
        choice = questionary.select(
            "Выберите действие:",
            choices=[
                f"{get_menu_emoji('🚀', '[1]')} Запустить тест",
                f"{get_menu_emoji('📦', '[2]')} Создать датасет",
                f"{get_menu_emoji('🧩', '[3]')} Конфиги и проверка",
                f"{get_menu_emoji('📊', '[4]')} Просмотр метрик",
                questionary.Separator(),
                f"{get_menu_emoji('⬅️', '[0]')} Выход"
            ],
            use_indicator=False
        ).ask()

        if choice is None or choice.startswith("⬅️"):
            break

        console.clear()

        if choice.startswith("🚀"):
            run_test_menu()
        elif choice.startswith("📦"):
            create_dataset_menu()
        elif choice.startswith("🧩"):
            manage_configs_menu()
        elif choice.startswith("📊"):
            view_metrics_menu()


if __name__ == "__main__":
    try:
        run_interactive()
    except (KeyboardInterrupt, EOFError):
        console.print("\n[bold yellow]Выход по запросу пользователя.[/bold yellow]")
