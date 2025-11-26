"""
Интерактивное меню для s3flood с использованием rich и questionary.
"""
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich.text import Text
from pathlib import Path
import argparse
import subprocess
import time
import threading
import yaml
import questionary
import shutil
import csv
import statistics
from prompt_toolkit.completion import PathCompleter
from typing import Optional, Union

from .config import load_run_config, RunConfigModel, resolve_run_settings
from .dataset import plan_and_generate
from .executor import run_profile, aws_list_objects, aws_check_bucket_access, _get_aws_env, get_spinner


console = Console()
path_completer = PathCompleter(expanduser=True, only_directories=True)


def format_bytes_to_readable(bytes_val: Optional[int]) -> str:
    """Конвертирует байты в читаемый формат (MB или GB)."""
    if bytes_val is None:
        return "не задано"
    mb = bytes_val / (1024 * 1024)
    if mb >= 1024:
        return f"{mb / 1024:.1f} GB"
    return f"{int(mb)} MB"


class DotSpinner:
    """Простой спиннер из точек для индикации длительных операций."""

    def __init__(self, message: str = ""):
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._message = message

    def __enter__(self):
        if self._message:
            # Сообщение — отдельной строкой, чтобы не мешать другим выводам
            console.print(self._message, style="dim")
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        # Перенос строки после спиннера
        console.file.write("\n")
        console.file.flush()

    def _run(self):
        while not self._stop.is_set():
            # Используем тот же спиннер, что и в дашборде
            frame = get_spinner()
            console.print(frame, end="\r", soft_wrap=False)
            console.file.flush()
            time.sleep(0.1)


def run_test_menu():
    """Меню запуска теста с выбором конфига и профиля."""
    console.clear()
    console.rule("[bold yellow]🚀 Запустить тест[/bold yellow]")

    # Ищем YAML-конфиги в текущей директории
    cwd = Path(".").resolve()
    configs = sorted(list(cwd.glob("config*.yml")) + list(cwd.glob("config*.yaml")))
    choices = [str(cfg.name) for cfg in configs]
    choices.append("Ввести путь вручную")
    choices.append("Вернуться в главное меню")

    choice = questionary.select(
        "Выберите конфиг:",
        choices=choices,
        use_indicator=True,
    ).ask()
    if not choice or choice == "Вернуться в главное меню":
        return

    if choice == "Ввести путь вручную":
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
            "🔺 write — только запись",
            "🔻 read  — только чтение",
            "🔀 mixed — смешанный профиль",
        ],
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

    # Готовим псевдо-CLI аргументы: все берём из конфига, кроме profile
    cli_args = argparse.Namespace(
        profile=profile_value,
        client=None,
        endpoint=None,
        endpoints=None,
        endpoint_mode=None,
        bucket=None,
        access_key=None,
        secret_key=None,
        aws_profile=None,
        threads=None,
        infinite=None,
        report=None,
        metrics=None,
        data_dir=None,
        mixed_read_ratio=None,
        pattern=None,
        burst_duration_sec=None,
        burst_intensity_multiplier=None,
        queue_limit=None,
        max_retries=None,
        retry_backoff_base=None,
        order=None,
        unique_remote_names=None,
    )

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
    if questionary.confirm("Изменить параметры перед запуском?", default=False).ask():
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
        with DotSpinner("Создание датасета"):
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
    """Мастер создания нового конфигурационного файла."""
    console.clear()
    console.rule("[bold yellow]📝 Создать новый конфиг[/bold yellow]")

    # Имя файла
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

    # Endpoint / endpoints
    mode = questionary.select(
        "Режим подключения:",
        choices=[
            "Один endpoint",
            "Кластер (несколько endpoints)",
        ],
        use_indicator=True,
    ).ask()
    if not mode:
        return

    if mode.startswith("Один"):
        endpoint = questionary.text(
            "Endpoint (например, http://localhost:9000):",
            default="http://localhost:9000",
        ).ask()
        endpoints = None
        endpoint_mode = None
    else:
        raw_eps = questionary.text(
            "Список endpoints через запятую (http://node1:9000,http://node2:9000):",
            default="http://node1:9000,http://node2:9000",
        ).ask()
        endpoints = [e.strip() for e in (raw_eps or "").split(",") if e.strip()]
        endpoint = None
        endpoint_mode = questionary.select(
            "Режим выбора endpoint:",
            choices=["round-robin", "random"],
            default="round-robin",
        ).ask()

    # Bucket
    bucket = questionary.text(
        "Имя S3 бакета:",
        default="your-bucket-name",
    ).ask()
    if not bucket:
        console.print("[red]Бакет обязателен.[/red]")
        return

    # Аутентификация
    auth_mode = questionary.select(
        "Способ аутентификации:",
        choices=[
            "AWS профиль (aws_profile)",
            "Access/Secret ключи",
            "Без явных учётных данных",
        ],
        use_indicator=True,
    ).ask()

    access_key = secret_key = aws_profile = None
    if auth_mode.startswith("AWS профиль"):
        aws_profile = questionary.text(
            "Имя AWS профиля (из ~/.aws/credentials):",
            default="default",
        ).ask()
    elif auth_mode.startswith("Access/Secret"):
        access_key = questionary.text("AWS Access Key ID:", default="YOUR_ACCESS_KEY").ask()
        secret_key = questionary.text("AWS Secret Access Key:", default="YOUR_SECRET_KEY").ask()

    # Базовые параметры
    threads = questionary.text(
        "Количество потоков:",
        default="8",
        validate=lambda v: (v.isdigit() and int(v) > 0) or "Введите целое число > 0",
    ).ask()
    threads = int(threads) if threads else 8

    data_dir = questionary.text(
        "Путь к датасету (data_dir):",
        default="./loadset/data",
    ).ask()
    report = questionary.text(
        "Имя JSON отчёта (report):",
        default="out.json",
    ).ask()
    metrics = questionary.text(
        "Имя CSV с метриками (metrics):",
        default="out.csv",
    ).ask()

    # Дополнительные опции
    infinite = questionary.confirm(
        "Бесконечный режим (infinite)?", default=False
    ).ask()

    order = questionary.select(
        "Порядок обработки файлов (order):",
        choices=["sequential", "random"],
        default="random",
    ).ask()

    unique_remote_names = questionary.confirm(
        "Добавлять уникальный постфикс к именам объектов (unique_remote_names)?",
        default=False,
    ).ask()

    # mixed_read_ratio и pattern оставляем по умолчанию, при необходимости пользователь добавит руками

    run_cfg: dict[str, object] = {
        "profile": "write",  # по умолчанию; может быть переопределён через CLI/меню
        "client": "awscli",
        "bucket": bucket,
        "threads": threads,
        "data_dir": data_dir,
        "report": report,
        "metrics": metrics,
        "infinite": bool(infinite),
        "order": order,
        "unique_remote_names": bool(unique_remote_names),
    }

    if endpoint:
        run_cfg["endpoint"] = endpoint
    if endpoints:
        run_cfg["endpoints"] = endpoints
        if endpoint_mode:
            run_cfg["endpoint_mode"] = endpoint_mode
    if access_key and secret_key:
        run_cfg["access_key"] = access_key
        run_cfg["secret_key"] = secret_key
    if aws_profile:
        run_cfg["aws_profile"] = aws_profile

    config_obj = {"run": run_cfg}

    console.print("\n[bold]Итоговый конфиг:[/bold]")
    console.print(Panel(yaml.safe_dump(config_obj, sort_keys=False, allow_unicode=True), title=target_path))

    if not questionary.confirm("Сохранить этот конфиг?", default=True).ask():
        console.print("[yellow]Сохранение отменено.[/yellow]")
        return

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
                "📝 Создать новый конфиг",
                "🔍 Проверить / управлять конфигом",
                "✏️ Редактировать существующий конфиг",
                questionary.Separator(),
                "⬅️ Вернуться в главное меню",
            ],
            use_indicator=True,
        ).ask()

        if not choice or choice.startswith("⬅️"):
            return

        if choice.startswith("📝"):
            create_config_wizard()
        elif choice.startswith("🔍"):
            validate_config_menu()
        elif choice.startswith("✏️"):
            edit_config_menu()
        else:
            return


def edit_config_menu():
    """Интерактивное редактирование существующего конфига (основные поля)."""
    console.clear()
    console.rule("[bold yellow]✏️ Редактировать конфиг[/bold yellow]")

    # Выбор конфига
    cwd = Path(".").resolve()
    configs = sorted(list(cwd.glob("config*.yml")) + list(cwd.glob("config*.yaml")))
    choices = [str(cfg.name) for cfg in configs]
    choices.append("📂 Ввести путь вручную")
    choices.append("⬅️ Вернуться в главное меню")

    choice = questionary.select(
        "Выберите конфиг для редактирования:",
        choices=choices,
        use_indicator=True,
    ).ask()
    if not choice or choice.startswith("⬅️"):
        return

    if choice.startswith("📂"):
        path_str = questionary.path(
            "Путь к YAML-конфигу:",
            completer=path_completer,
            validate=lambda p: Path(p).is_file() or "Файл не найден",
        ).ask()
        if not path_str:
            return
        cfg_path = Path(path_str).expanduser()
    else:
        cfg_path = cwd / choice

    # Загрузка и преобразование в dict
    try:
        cfg_model = load_run_config(str(cfg_path))
    except Exception as e:
        console.print(f"[bold red]Не удалось прочитать или разобрать конфиг: {e}[/bold red]")
        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        return

    data = cfg_model.model_dump()

    console.print(f"[bold]Редактирование конфига:[/bold] [cyan]{cfg_path}[/cyan]\n")

    # Сводка текущих настроек (без профиля нагрузки — он выбирается при запуске)
    summary = Table(show_header=False, box=None)
    summary.add_column(style="cyan")
    summary.add_column(style="white")
    summary.add_row("bucket", str(data.get("bucket") or ""))
    summary.add_row("endpoint", str(data.get("endpoint") or ""))
    endpoints_list = data.get("endpoints") or []
    summary.add_row("endpoints", ", ".join(str(e) for e in endpoints_list))
    summary.add_row("endpoint_mode", str(data.get("endpoint_mode") or ""))
    summary.add_row("threads", str(data.get("threads") or ""))
    summary.add_row("data_dir", str(data.get("data_dir") or ""))
    summary.add_row("report", str(data.get("report") or ""))
    summary.add_row("metrics", str(data.get("metrics") or ""))
    summary.add_row("infinite", str(data.get("infinite") or "False"))
    summary.add_row("mixed_read_ratio", str(data.get("mixed_read_ratio") or ""))
    summary.add_row("unique_remote_names", str(data.get("unique_remote_names") or "False"))
    summary.add_row("pattern", str(data.get("pattern") or "sustained"))
    summary.add_row("burst_duration_sec", str(data.get("burst_duration_sec") or ""))
    summary.add_row("burst_intensity_multiplier", str(data.get("burst_intensity_multiplier") or ""))
    summary.add_row("order", str(data.get("order") or "sequential"))
    if data.get("aws_cli_multipart_threshold") is not None:
        summary.add_row("aws_cli_multipart_threshold", str(data.get("aws_cli_multipart_threshold")))
    if data.get("aws_cli_multipart_chunksize") is not None:
        summary.add_row("aws_cli_multipart_chunksize", str(data.get("aws_cli_multipart_chunksize")))
    if data.get("aws_cli_max_concurrent_requests") is not None:
        summary.add_row("aws_cli_max_concurrent_requests", str(data.get("aws_cli_max_concurrent_requests")))
    console.print(summary)
    console.print()  # пустая строка перед вопросами

    # Endpoint / endpoints
    endpoint_default = data.get("endpoint") or ""
    endpoints_default_list = data.get("endpoints") or []
    endpoints_default = ",".join(endpoints_default_list)
    endpoint_mode_default = data.get("endpoint_mode") or "round-robin"

    mode = questionary.select(
        "Режим подключения:",
        choices=["Один endpoint", "Кластер (несколько endpoints)"],
        default="Кластер (несколько endpoints)" if endpoints_default_list else "Один endpoint",
    ).ask()
    if not mode:
        return

    if mode.startswith("Кластер"):
        endpoints_str = questionary.text(
            "Endpoints (через запятую):",
            default=endpoints_default or "http://localhost:9000",
        ).ask() or endpoints_default
        endpoints = [e.strip() for e in endpoints_str.split(",") if e.strip()]
        endpoint = None
        endpoint_mode = questionary.select(
            "Стратегия выбора endpoint:",
            choices=["round-robin", "random"],
            default=endpoint_mode_default if endpoint_mode_default in ["round-robin", "random"] else "round-robin",
        ).ask() or endpoint_mode_default
    else:
        endpoint = questionary.text(
            "Endpoint (например, http://localhost:9000):",
            default=endpoint_default or "http://localhost:9000",
        ).ask() or endpoint_default
        # если один endpoint — список endpoints и режим очищаем
        endpoints = []
        endpoint_mode = None

    # Бакет и базовые параметры
    bucket = questionary.text(
        "Bucket:",
        default=data.get("bucket") or "",
    ).ask() or data.get("bucket") or ""

    threads_str = questionary.text(
        "Число потоков:",
        default=str(data.get("threads") or 8),
        validate=lambda v: (v.isdigit() and int(v) > 0) or "Введите целое число > 0",
    ).ask()
    threads_int = int(threads_str) if threads_str else (data.get("threads") or 8)

    data_dir = questionary.text(
        "Каталог датасета (data_dir):",
        default=data.get("data_dir") or "./loadset/data",
    ).ask() or data.get("data_dir") or "./loadset/data"

    infinite = questionary.confirm(
        "Бесконечный режим (infinite):",
        default=bool(data.get("infinite")),
    ).ask()

    mixed_read_ratio_default = data.get("mixed_read_ratio")
    if mixed_read_ratio_default is None:
        mixed_read_ratio_default = 0.7
    mixed_ratio_str = questionary.text(
        "mixed_read_ratio (0.0 - 1.0):",
        default=str(mixed_read_ratio_default),
        validate=lambda v: (
            v.strip() == ""
            or (v.replace(".", "", 1).isdigit() and 0.0 <= float(v) <= 1.0)
            or "Введите число от 0.0 до 1.0 или оставьте пустым"
        ),
    ).ask() or str(mixed_read_ratio_default)
    mixed_ratio = mixed_read_ratio_default if mixed_ratio_str.strip() == "" else float(mixed_ratio_str)

    unique_remote_names = questionary.confirm(
        "unique_remote_names (уникальные имена объектов):",
        default=bool(data.get("unique_remote_names")),
    ).ask()

    # Настройки AWS CLI (переопределяют настройки из ~/.aws/config)
    console.print("\n[bold]Настройки AWS CLI (опционально, переопределяют ~/.aws/config):[/bold]")
    
    # Функция для форматирования байтов в читаемый формат (MB или GB)
    def _format_size_for_display(bytes_val: Union[int, str, None]) -> str:
        if bytes_val is None:
            return ""
        if isinstance(bytes_val, str):
            return bytes_val
        # Конвертируем байты в MB или GB
        mb = bytes_val / (1024 * 1024)
        if mb >= 1024:
            return f"{mb / 1024:.1f}GB"
        return f"{int(mb)}MB"
    
    multipart_threshold_val = data.get("aws_cli_multipart_threshold")
    multipart_threshold_display = _format_size_for_display(multipart_threshold_val)
    multipart_threshold_str = questionary.text(
        "aws_cli_multipart_threshold (порог для multipart, MB или строка типа '5GB', '8MB'):",
        default=multipart_threshold_display,
        validate=lambda v: (
            v.strip() == "" or validate_size_format(v)
        ) or "Введите размер в формате MB (например, 5120) или строку типа '5GB', '8MB', или оставьте пустым",
    ).ask() or ""
    aws_cli_multipart_threshold = multipart_threshold_str.strip() if multipart_threshold_str.strip() else None

    multipart_chunksize_val = data.get("aws_cli_multipart_chunksize")
    multipart_chunksize_display = _format_size_for_display(multipart_chunksize_val)
    multipart_chunksize_str = questionary.text(
        "aws_cli_multipart_chunksize (размер чанка, MB или строка типа '8MB', '16MB'):",
        default=multipart_chunksize_display,
        validate=lambda v: (
            v.strip() == "" or validate_size_format(v)
        ) or "Введите размер в формате MB (например, 8) или строку типа '8MB', '16MB', или оставьте пустым",
    ).ask() or ""
    aws_cli_multipart_chunksize = multipart_chunksize_str.strip() if multipart_chunksize_str.strip() else None

    max_concurrent_val = data.get("aws_cli_max_concurrent_requests")
    max_concurrent_str = questionary.text(
        "aws_cli_max_concurrent_requests (макс. параллельных запросов, например 10):",
        default=str(max_concurrent_val) if max_concurrent_val is not None else "",
        validate=lambda v: (
            v.strip() == "" or (v.isdigit() and int(v) > 0)
        ) or "Введите целое число > 0 или оставьте пустым",
    ).ask() or ""
    aws_cli_max_concurrent_requests = int(max_concurrent_str) if max_concurrent_str.strip() else None

    # Паттерн нагрузки
    pattern_default = data.get("pattern") or "sustained"
    pattern = questionary.select(
        "Паттерн нагрузки (pattern):",
        choices=["sustained", "bursty"],
        default=pattern_default if pattern_default in ["sustained", "bursty"] else "sustained",
    ).ask() or pattern_default

    burst_duration_val = data.get("burst_duration_sec")
    burst_intensity_val = data.get("burst_intensity_multiplier")

    if pattern == "bursty":
        # Длительность всплеска
        burst_duration_str = questionary.text(
            "burst_duration_sec (длительность всплеска, сек):",
            default=str(burst_duration_val if burst_duration_val is not None else 60.0),
            validate=lambda v: (
                v.replace(".", "", 1).isdigit() and float(v) > 0.0
            ) or "Введите число > 0",
        ).ask()
        burst_duration_sec = float(burst_duration_str) if burst_duration_str else (burst_duration_val or 60.0)

        # Множитель интенсивности
        burst_intensity_str = questionary.text(
            "burst_intensity_multiplier (множитель интенсивности):",
            default=str(burst_intensity_val if burst_intensity_val is not None else 5.0),
            validate=lambda v: (
                v.replace(".", "", 1).isdigit() and float(v) > 1.0
            ) or "Введите число > 1.0",
        ).ask()
        burst_intensity_multiplier = (
            float(burst_intensity_str) if burst_intensity_str else (burst_intensity_val or 5.0)
        )
    else:
        # Для sustained не трогаем существующие значения (если были заданы в конфиге)
        burst_duration_sec = burst_duration_val
        burst_intensity_multiplier = burst_intensity_val

    # Обновляем структуру (профиль нагрузки из конфига игнорируем, он выбирается при запуске)
    updated = dict(data)
    updated.pop("profile", None)
    updated["bucket"] = bucket
    updated["threads"] = threads_int
    updated["data_dir"] = data_dir
    updated["infinite"] = bool(infinite)
    updated["mixed_read_ratio"] = mixed_ratio
    updated["unique_remote_names"] = bool(unique_remote_names)
    updated["pattern"] = pattern
    updated["burst_duration_sec"] = burst_duration_sec
    updated["burst_intensity_multiplier"] = burst_intensity_multiplier
    if aws_cli_multipart_threshold is not None:
        updated["aws_cli_multipart_threshold"] = aws_cli_multipart_threshold
    else:
        updated.pop("aws_cli_multipart_threshold", None)
    if aws_cli_multipart_chunksize is not None:
        updated["aws_cli_multipart_chunksize"] = aws_cli_multipart_chunksize
    else:
        updated.pop("aws_cli_multipart_chunksize", None)
    if aws_cli_max_concurrent_requests is not None:
        updated["aws_cli_max_concurrent_requests"] = aws_cli_max_concurrent_requests
    else:
        updated.pop("aws_cli_max_concurrent_requests", None)

    if endpoint:
        updated["endpoint"] = endpoint
        updated["endpoints"] = None
        updated["endpoint_mode"] = None
    else:
        updated["endpoint"] = None
        updated["endpoints"] = endpoints
        updated["endpoint_mode"] = endpoint_mode

    # Итоговая сводка
    console.print("\n[bold]Обновлённые параметры:[/bold]")
    table = Table(show_header=False, box=None)
    table.add_column(style="cyan")
    table.add_column(style="white")
    table.add_row("bucket", str(updated.get("bucket")))
    table.add_row("endpoint", str(updated.get("endpoint") or ""))
    table.add_row("endpoints", ", ".join(updated.get("endpoints") or []))
    table.add_row("endpoint_mode", str(updated.get("endpoint_mode") or ""))
    table.add_row("threads", str(updated.get("threads")))
    table.add_row("data_dir", str(updated.get("data_dir")))
    table.add_row("infinite", str(updated.get("infinite")))
    table.add_row("mixed_read_ratio", str(updated.get("mixed_read_ratio")))
    table.add_row("unique_remote_names", str(updated.get("unique_remote_names")))
    table.add_row("pattern", str(updated.get("pattern")))
    table.add_row("burst_duration_sec", str(updated.get("burst_duration_sec")))
    table.add_row("burst_intensity_multiplier", str(updated.get("burst_intensity_multiplier")))
    if updated.get("aws_cli_multipart_threshold") is not None:
        table.add_row("aws_cli_multipart_threshold", str(updated.get("aws_cli_multipart_threshold")))
    if updated.get("aws_cli_multipart_chunksize") is not None:
        table.add_row("aws_cli_multipart_chunksize", str(updated.get("aws_cli_multipart_chunksize")))
    if updated.get("aws_cli_max_concurrent_requests") is not None:
        table.add_row("aws_cli_max_concurrent_requests", str(updated.get("aws_cli_max_concurrent_requests")))
    console.print(table)

    if not questionary.confirm("\nСохранить изменения в этом конфиге?", default=True).ask():
        console.print("[yellow]Изменения отменены.[/yellow]")
        return

    try:
        out = {"run": {k: v for k, v in updated.items() if v is not None}}
        with open(cfg_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(out, f, sort_keys=False, allow_unicode=True)
        console.print(f"[bold green]✅ Конфиг обновлён: {cfg_path}[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Ошибка при сохранении конфига: {e}[/bold red]")

    questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()


def validate_config_menu():
    """Меню проверки конфига: базовая валидация и работа с бакетом."""
    console.clear()
    console.rule("[bold yellow]🔍 Проверить конфиг[/bold yellow]")

    # Выбор конфига (список config*.yml/yaml + ручной ввод)
    cwd = Path(".").resolve()
    configs = sorted(list(cwd.glob("config*.yml")) + list(cwd.glob("config*.yaml")))
    choices = [str(cfg.name) for cfg in configs]
    choices.append("📂 Ввести путь вручную")
    choices.append("⬅️ Вернуться в главное меню")

    choice = questionary.select(
        "Выберите конфиг:",
        choices=choices,
        use_indicator=True,
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

    # Собираем настройки (используем write-профиль по умолчанию — профиль здесь не важен)
    cli_args = argparse.Namespace(
        profile="write",
        client=None,
        endpoint=None,
        endpoints=None,
        endpoint_mode=None,
        bucket=None,
        access_key=None,
        secret_key=None,
        aws_profile=None,
        threads=None,
        infinite=None,
        report=None,
        metrics=None,
        data_dir=None,
        mixed_read_ratio=None,
        pattern=None,
        burst_duration_sec=None,
        burst_intensity_multiplier=None,
        queue_limit=None,
        max_retries=None,
        retry_backoff_base=None,
        order=None,
        unique_remote_names=None,
    )

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
    preview = objects[:5]
    table = Table(title="Первые 5 объектов", box=None)
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
            "Вернуться в главное меню",
            "Удалить ВСЕ объекты из бакета",
        ],
    ).ask()

    if action == "Вернуться в главное меню":
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

        env = _get_aws_env(settings.access_key, settings.secret_key, settings.aws_profile)
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
        console.print(f"\n[bold red]Выполняем:[/bold red] {' '.join(cmd)}")
        try:
            with DotSpinner("Удаление объектов из бакета"):
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
    choices.append("⬅️ Вернуться в главное меню")

    choice = questionary.select(
        "Выберите CSV с метриками:",
        choices=choices,
        use_indicator=True,
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

    total_bytes = sum(o["bytes"] for o in ops)
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
                "🚀 Запустить тест",
                "📦 Создать датасет",
                "🧩 Конфиги и проверка",
                "📊 Просмотр метрик",
                questionary.Separator(),
                "⬅️ Выход"
            ],
            use_indicator=True
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
