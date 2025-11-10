#!/usr/bin/env bash
set -euo pipefail

# Minimal install for Linux
# - Python 3.10+
# - awscli via pipx or pip if not present

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required" >&2; exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  if command -v pipx >/dev/null 2>&1; then
    pipx install awscli || true
  else
    python3 -m pip install --user awscli
  fi
fi

python3 -m pip install --user -e .

echo "Install complete. Use: s3flood dataset create ... or python -m s3flood.cli ..."
