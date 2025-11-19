import argparse
from .config import load_run_config, resolve_run_settings
from .dataset import plan_and_generate
from .executor import run_profile


def main():
    parser = argparse.ArgumentParser(prog="s3flood")
    sub = parser.add_subparsers(dest="cmd", required=True)

    dcreate = sub.add_parser("dataset-create")
    dcreate.add_argument("--path", required=True)
    dcreate.add_argument("--target-bytes", type=str, default="auto")
    dcreate.add_argument("--use-symlinks", action="store_true")
    dcreate.add_argument("--min-counts", type=str, default="100,50,20")
    dcreate.add_argument("--group-limits", type=str, default="100MB,1GB,10GB")
    dcreate.add_argument("--safety-ratio", type=float, default=0.8)

    runp = sub.add_parser("run")
    runp.add_argument("--config", help="YAML-файл с параметрами запуска (endpoint, bucket, креденшлы)")
    runp.add_argument("--profile", choices=["write-heavy","read-heavy","mixed-70-30"], default=None)
    runp.add_argument("--client", choices=["awscli","rclone","s3cmd"], default=None)
    runp.add_argument("--endpoint", default=None)
    runp.add_argument("--endpoints", nargs="+", default=None, help="Список endpoint'ов для кластерного режима")
    runp.add_argument("--endpoint-mode", choices=["round-robin","random"], default=None, help="Стратегия выбора endpoint'а при кластерном режиме")
    runp.add_argument("--bucket", default=None)
    runp.add_argument("--access-key", dest="access_key", default=None)
    runp.add_argument("--secret-key", dest="secret_key", default=None)
    runp.add_argument("--aws-profile", dest="aws_profile", default=None)
    runp.add_argument("--threads", type=int, default=None)
    runp.add_argument("--infinite", action="store_true", default=None)
    runp.add_argument("--report", default=None)
    runp.add_argument("--metrics", default=None)
    runp.add_argument("--data-dir", dest="data_dir", default=None, help="Путь к корню датасета (сканируется рекурсивно)")
    runp.add_argument("--mixed-read-ratio", type=float, dest="mixed_read_ratio", default=None, help="Доля операций чтения для mixed профиля (0.0-1.0)")
    runp.add_argument("--pattern", choices=["sustained","bursty"], default=None, help="Паттерн нагрузки: sustained (ровная) или bursty (всплески)")
    runp.add_argument("--burst-duration-sec", type=float, dest="burst_duration_sec", default=None, help="Длительность всплеска в секундах для bursty паттерна")
    runp.add_argument("--burst-intensity-multiplier", type=float, dest="burst_intensity_multiplier", default=None, help="Множитель интенсивности для bursty паттерна")
    runp.add_argument("--queue-limit", type=int, dest="queue_limit", default=None, help="Максимальный размер очереди операций")
    runp.add_argument("--max-retries", type=int, dest="max_retries", default=None, help="Максимальное количество повторов при ошибке")
    runp.add_argument("--retry-backoff-base", type=float, dest="retry_backoff_base", default=None, help="Базовый множитель для экспоненциального backoff")

    args = parser.parse_args()

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
