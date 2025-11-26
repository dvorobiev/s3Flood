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
from prompt_toolkit.completion import PathCompleter
from typing import Optional

from .config import load_run_config, RunConfigModel, resolve_run_settings
from .dataset import plan_and_generate
from .executor import run_profile, aws_list_objects, _get_aws_env


console = Console()
path_completer = PathCompleter(expanduser=True, only_directories=True)


class DotSpinner:
    """Простой спиннер из точек для индикации длительных операций."""

    def __init__(self, message: str = ""):
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._message = message

    def __enter__(self):
        if self._message:
            console.print(self._message, end="", style="dim")
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        # Перенос строки после спиннера
        console.print()

    def _run(self):
        while not self._stop.is_set():
            console.print(".", end="", style="dim", soft_wrap=False)
            console.file.flush()
            # Небольшая пауза между точками
            time.sleep(0.3)


def run_test_menu():
    """Меню запуска теста с выбором конфига и профиля."""
    console.rule("[bold yellow]Запустить тест[/bold yellow]")

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
            "write — только запись",
            "read  — только чтение",
            "mixed — смешанный профиль",
        ],
    ).ask()
    if not profile:
        return

    if profile.startswith("write"):
        profile_value = "write"
    elif profile.startswith("read"):
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
    console.print(summary_table)

    questionary.press_any_key_to_continue("Нажмите любую клавишу для запуска...").ask()

    # Запуск профиля
    try:
        with DotSpinner("Выполнение профиля"):
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
    console.rule("[bold yellow]Создать датасет[/bold yellow]")
    
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
    console.rule("[bold yellow]Создать новый конфиг[/bold yellow]")

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
    console.rule("[bold yellow]Конфиги и проверка[/bold yellow]")

    while True:
        choice = questionary.select(
            "Выберите действие:",
            choices=[
                "Создать новый конфиг",
                "Проверить / управлять конфигом",
                "Редактировать существующий конфиг",
                questionary.Separator(),
                "Вернуться в главное меню",
            ],
            use_indicator=True,
        ).ask()

        if not choice or choice == "Вернуться в главное меню":
            return

        if choice.startswith("Создать"):
            create_config_wizard()
        elif choice.startswith("Проверить"):
            validate_config_menu()
        elif choice.startswith("Редактировать"):
            edit_config_menu()


def validate_config_menu():
    """Меню проверки конфига: базовая валидация и работа с бакетом."""
    console.rule("[bold yellow]Проверить конфиг[/bold yellow]")

    # Выбор конфига (список config*.yml/yaml + ручной ввод)
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
    console.print(info_table)

    if not primary_endpoint:
        console.print("[bold red]Endpoint не настроен — подключение к S3 невозможно.[/bold red]")
        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
        return

    # Подключение к S3 и вывод первых 5 объектов
    console.print("\n[bold]Пробуем получить список объектов из бакета...[/bold]")
    try:
        objects = aws_list_objects(
            settings.bucket,
            primary_endpoint,
            settings.access_key,
            settings.secret_key,
            settings.aws_profile,
        )
    except Exception as exc:
        console.print(f"[bold red]Ошибка при запросе списка объектов: {exc}[/bold red]")
        objects = None

    if not objects:
        console.print("[yellow]Объекты не найдены или не удалось получить список.[/yellow]")
        # Нечего удалять или нет уверенности в списке — не предлагаем destructive-опции
        questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()
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
            "Ничего не делать",
            "Удалить ВСЕ объекты из бакета",
        ],
    ).ask()

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

    questionary.press_any_key_to_continue("Нажмите любую клавишу для возврата в меню...").ask()


def view_metrics_menu():
    """Меню просмотра метрик (заглушка)."""
    console.rule("[bold yellow]Просмотр метрик[/bold yellow]")
    console.print("[dim]Функция в разработке...[/dim]\n")
    # Пока просто возвращаемся в главное меню
    return


def run_interactive():
    """Запуск интерактивного меню."""
    while True:
        console.rule("[bold]Меню s3flood[/bold]")
        choice = questionary.select(
            "Выберите действие:",
            choices=[
                "1. Запустить тест",
                "2. Создать датасет",
                "3. Конфиги и проверка",
                "4. Просмотр метрик",
                questionary.Separator(),
                "Выход"
            ],
            use_indicator=True
        ).ask()

        if choice is None or choice == "Выход":
            break

        console.clear()

        if "1." in choice:
            run_test_menu()
        elif "2." in choice:
            create_dataset_menu()
        elif "3." in choice:
            manage_configs_menu()
        elif "4." in choice:
            view_metrics_menu()


if __name__ == "__main__":
    try:
        run_interactive()
    except (KeyboardInterrupt, EOFError):
        console.print("\n[bold yellow]Выход по запросу пользователя.[/bold yellow]")
