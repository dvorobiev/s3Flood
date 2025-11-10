# s3Flood (CLI minimal)

Lightweight CLI for S3 backend load testing: dataset generator with symlinks, profiles (write/read/mixed), progress logs, and metrics export. Default client: AWS CLI. Windows build not required; provide install scripts.

Key defaults:
- Size groups: small ≤100MB, medium ≤1GB, large ≤10GB.
- Byte share: small 30%, medium 50%, large 20%.
- Minimum objects: small ≥100, medium ≥50, large ≥20.
- Disk safety ratio: 0.8 of free space when auto-planning dataset.

See docs/CLI.md for usage until README is rewritten fully.
