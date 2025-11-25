import argparse
import sys
from .config import load_run_config, resolve_run_settings
from .dataset import plan_and_generate
from .executor import run_profile
from .interactive import run_interactive


def main():
    top_level_epilog = """
Быстрые примеры:

  # Создать дефолтный датасет (авторазмер)
  s3flood dataset-create --path ./loadset

  # Запустить тест с конфигом
  s3flood run --config config.yaml

  # Запустить write-профиль (MinIO)
  s3flood run --profile write \\
    --endpoint http://localhost:9000 --bucket test \\
    --access-key minioadmin --secret-key minioadmin \\
    --data-dir ./loadset/data

  # Запустить read-профиль
  s3flood run --profile read --config config.yaml
"""
    parser = argparse.ArgumentParser(
        prog="s3flood",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Минималистичный клиент для нагрузочного тестирования S3",
        epilog=top_level_epilog
    )
    parser.add_argument("--interactive", "-i", action="store_true", help="Запустить интерактивное меню")
    sub = parser.add_subparsers(dest="cmd", required=False)

    dataset_create_epilog = """
Примеры создания датасета:

  # Создать датасет с дефолтными настройками (автоматический размер)
  s3flood dataset-create --path ./loadset

  # Создать датасет размером 5GB
  s3flood dataset-create --path ./loadset --target-bytes 5GB

  # Создать датасет с символическими ссылками (экономит место)
  s3flood dataset-create --path ./loadset --use-symlinks

  # Создать датасет с кастомными параметрами
  s3flood dataset-create --path ./loadset --target-bytes 10GB \\
    --min-counts 200,100,50 --group-limits 50MB,500MB,5GB
"""
    dcreate = sub.add_parser(
        "dataset-create",
        help="Создать датасет для нагрузочного тестирования",
        epilog=dataset_create_epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    dcreate.add_argument("--path", required=True, help="Путь к каталогу, где будет создан датасет")
    dcreate.add_argument("--target-bytes", type=str, default="auto", help="Целевой размер датасета (например, '5GB', '1TB') или 'auto' для использования 80%% свободного места")
    dcreate.add_argument("--use-symlinks", action="store_true", help="Использовать символические ссылки вместо реальных файлов (экономит место, но не работает при копировании на Windows)")
    dcreate.add_argument("--min-counts", type=str, default="100,50,20", help="Минимальное количество файлов для групп small,medium,large (формат: '100,50,20')")
    dcreate.add_argument("--group-limits", type=str, default="100MB,1GB,10GB", help="Максимальные размеры файлов для групп small,medium,large (формат: '100MB,1GB,10GB'). Совет: ≤5GB для защиты от BadDigest")
    dcreate.add_argument("--safety-ratio", type=float, default=0.8, help="Доля свободного места для использования при --target-bytes auto (по умолчанию 0.8 = 80%%)")

    run_epilog = """
Примеры запуска тестирования:

  # Запуск с конфигурационным файлом
  s3flood run --config config.yaml

  # Запуск с параметрами через CLI (MinIO)
  s3flood run --profile write \\
    --endpoint http://localhost:9000 \\
    --bucket test \\
    --access-key minioadmin \\
    --secret-key minioadmin \\
    --data-dir ./loadset/data

  # Запуск профиля чтения
  s3flood run --profile read \\
    --endpoint http://localhost:9000 \\
    --bucket test \\
    --access-key minioadmin \\
    --secret-key minioadmin

  # Запуск mixed профиля с bursty паттерном
  s3flood run --config config.yaml \\
    --profile mixed-70-30 \\
    --pattern bursty \\
    --threads 16 \\
    --infinite

  # Запуск с кластером (несколько endpoints)
  s3flood run --config config.yaml \\
    --endpoints http://node1:9000 http://node2:9000 \\
    --endpoint-mode round-robin
"""
    runp = sub.add_parser(
        "run",
        help="Запустить нагрузочный тест S3",
        epilog=run_epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    runp.add_argument("--config", help="YAML-файл с параметрами запуска (endpoint, bucket, креденшлы). Все параметры из конфига можно переопределить через CLI")
    runp.add_argument("--profile", choices=["write","read","mixed-70-30"], default=None, help="Профиль нагрузки: write (только запись), read (только чтение из бакета), mixed-70-30 (смешанные операции)")
    runp.add_argument("--client", choices=["awscli","rclone","s3cmd"], default=None, help="S3 клиент для использования (по умолчанию: awscli)")
    runp.add_argument("--endpoint", default=None, help="URL S3 endpoint (например, http://localhost:9000 для MinIO)")
    runp.add_argument("--endpoints", nargs="+", default=None, help="Список endpoint'ов для кластерного режима (например: http://node1:9000 http://node2:9000)")
    runp.add_argument("--endpoint-mode", choices=["round-robin","random"], default=None, help="Стратегия выбора endpoint'а при кластерном режиме: round-robin (по кругу) или random (случайно)")
    runp.add_argument("--bucket", default=None, help="Имя S3 бакета для тестирования")
    runp.add_argument("--access-key", dest="access_key", default=None, help="AWS Access Key ID (или S3-совместимый ключ доступа)")
    runp.add_argument("--secret-key", dest="secret_key", default=None, help="AWS Secret Access Key (или S3-совместимый секретный ключ)")
    runp.add_argument("--aws-profile", dest="aws_profile", default=None, help="Имя профиля AWS CLI (из ~/.aws/credentials). Альтернатива --access-key/--secret-key")
    runp.add_argument("--threads", type=int, default=None, help="Количество параллельных потоков для операций (по умолчанию: 8)")
    runp.add_argument("--infinite", action="store_true", default=None, help="Бесконечный режим: после завершения всех файлов начинать заново")
    runp.add_argument("--report", default=None, help="Путь к JSON файлу с итоговым отчётом (по умолчанию: report.json)")
    runp.add_argument("--metrics", default=None, help="Путь к CSV файлу с детальными метриками по каждой операции (по умолчанию: metrics.csv)")
    runp.add_argument("--data-dir", dest="data_dir", default=None, help="Путь к корню датасета (сканируется рекурсивно, по умолчанию: ./data)")
    runp.add_argument("--mixed-read-ratio", type=float, dest="mixed_read_ratio", default=None, help="Доля операций чтения для mixed профиля (0.0-1.0, по умолчанию для mixed-70-30: 0.7)")
    runp.add_argument("--pattern", choices=["sustained","bursty"], default=None, help="Паттерн нагрузки: sustained (ровная постоянная) или bursty (чередование всплесков и пауз)")
    runp.add_argument("--burst-duration-sec", type=float, dest="burst_duration_sec", default=None, help="Длительность всплеска в секундах для bursty паттерна (по умолчанию: 10.0)")
    runp.add_argument("--burst-intensity-multiplier", type=float, dest="burst_intensity_multiplier", default=None, help="Множитель интенсивности во время всплеска для bursty паттерна (по умолчанию: 10.0)")
    runp.add_argument("--queue-limit", type=int, dest="queue_limit", default=None, help="Максимальный размер очереди операций (по умолчанию: без ограничений)")
    runp.add_argument("--max-retries", type=int, dest="max_retries", default=None, help="Максимальное количество повторов при ошибке (по умолчанию: 3)")
    runp.add_argument("--retry-backoff-base", type=float, dest="retry_backoff_base", default=None, help="Базовый множитель для экспоненциального backoff при повторах (по умолчанию: 2.0, т.е. задержки: 1s, 2s, 4s)")
    runp.add_argument("--order", choices=["sequential","random"], default=None, help="Порядок обработки файлов: sequential (сначала маленькие, потом средние, потом большие) или random (случайный порядок)")
    runp.add_argument("--unique-remote-names", dest="unique_remote_names", action="store_true", default=None, help="Добавлять уникальный постфикс к имени объекта при загрузке (полезно для бесконечных прогонов, чтобы не перезаписывать предыдущие файлы)")

    args = parser.parse_args()

    # Запуск интерактивного меню, если указан флаг или нет команды
    if args.interactive or args.cmd is None:
        run_interactive()
        return

    if args.cmd == "dataset-create":
        plan_and_generate(
            path=args.path,
            target_bytes=args.target_bytes,
            use_symlinks=args.use_symlinks,
            min_counts=args.min_counts,
            group_limits=args.group_limits,
            safety_ratio=args.safety_ratio,
        )
    elif args.cmd == "run":
        config_model = None
        if args.config:
            try:
                config_model = load_run_config(args.config)
            except (OSError, ValueError) as exc:
                raise SystemExit(f"Не удалось прочитать конфиг: {exc}") from exc
        settings = resolve_run_settings(args, config_model)
        run_profile(settings.to_namespace())
