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
    runp.add_argument("--bucket", default=None)
    runp.add_argument("--access-key", dest="access_key", default=None)
    runp.add_argument("--secret-key", dest="secret_key", default=None)
    runp.add_argument("--aws-profile", dest="aws_profile", default=None)
    runp.add_argument("--threads", type=int, default=None)
    runp.add_argument("--infinite", action="store_true", default=None)
    runp.add_argument("--report", default=None)
    runp.add_argument("--metrics", default=None)
    runp.add_argument("--data-dir", dest="data_dir", default=None, help="Путь к корню датасета (сканируется рекурсивно)")

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
