import argparse
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
    runp.add_argument("--profile", choices=["write-heavy","read-heavy","mixed-70-30"], required=True)
    runp.add_argument("--client", choices=["awscli","rclone","s3cmd"], default="awscli")
    runp.add_argument("--endpoint", required=True)
    runp.add_argument("--bucket", required=True)
    runp.add_argument("--access-key", required=True)
    runp.add_argument("--secret-key", required=True)
    runp.add_argument("--threads", type=int, default=8)
    runp.add_argument("--infinite", action="store_true")
    runp.add_argument("--report", default="report.json")
    runp.add_argument("--metrics", default="metrics.csv")
    runp.add_argument("--data-dir", default="./data", help="Path to dataset root (will scan recursively)")

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
        run_profile(args)
