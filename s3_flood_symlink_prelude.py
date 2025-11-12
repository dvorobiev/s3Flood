#!/usr/bin/env python3
"""
S3 Flood - TUI application for S3 backend testing using s5cmd or rclone
"""

from pathlib import Path
import os
import platform

# New: robust Path import and symlink handling helpers for macOS

def resolve_real_file(p: Path) -> Path:
    """Return real file path, resolving symlinks on macOS; leave regular files unchanged."""
    try:
        if p.is_symlink():
            target = p.resolve(strict=False)
            return target
    except Exception:
        pass
    return p

# Rest of original file will be appended below at build time
