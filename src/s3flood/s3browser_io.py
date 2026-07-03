"""Слой данных TUI-браузера бакета: команды aws CLI и парсеры ответов.

Парсеры и сборщики команд — чистые функции (тестируются на фикстурах);
асинхронные обёртки запускают aws CLI без блокировки интерфейса.
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from urllib.parse import quote

from .metrics import classify_error


@dataclass
class S3Entry:
    """Элемент листинга: объект или «папка»-префикс."""
    name: str
    key: str
    is_dir: bool
    size: int = 0
    last_modified: str = ""


@dataclass
class S3Version:
    version_id: str
    size: int
    last_modified: str
    is_latest: bool
    is_delete_marker: bool = False


@dataclass
class OpResult:
    ok: bool
    error: str = ""
    payload: dict = field(default_factory=dict)


def parse_list_objects(payload: dict, prefix: str) -> list[S3Entry]:
    """Разбирает ответ list-objects-v2 (--delimiter /) в записи панели."""
    dirs = []
    for cp in payload.get("CommonPrefixes") or []:
        full = cp.get("Prefix") or ""
        dirs.append(S3Entry(name=full[len(prefix):], key=full, is_dir=True))
    files = []
    for obj in payload.get("Contents") or []:
        key = obj.get("Key") or ""
        if key == prefix:  # placeholder-объект самой «папки»
            continue
        files.append(S3Entry(
            name=key[len(prefix):],
            key=key,
            is_dir=False,
            size=int(obj.get("Size") or 0),
            last_modified=str(obj.get("LastModified") or ""),
        ))
    dirs.sort(key=lambda e: e.name)
    files.sort(key=lambda e: e.name)
    return dirs + files


def parse_list_versions(payload: dict, key: str) -> list[S3Version]:
    """Разбирает list-object-versions, оставляя только версии точного ключа."""
    versions = []
    for v in payload.get("Versions") or []:
        if v.get("Key") != key:
            continue
        versions.append(S3Version(
            version_id=str(v.get("VersionId") or ""),
            size=int(v.get("Size") or 0),
            last_modified=str(v.get("LastModified") or ""),
            is_latest=bool(v.get("IsLatest")),
        ))
    for m in payload.get("DeleteMarkers") or []:
        if m.get("Key") != key:
            continue
        versions.append(S3Version(
            version_id=str(m.get("VersionId") or ""),
            size=0,
            last_modified=str(m.get("LastModified") or ""),
            is_latest=bool(m.get("IsLatest")),
            is_delete_marker=True,
        ))
    versions.sort(key=lambda v: v.last_modified, reverse=True)
    return versions


def parse_buckets(payload: dict) -> list[str]:
    """Список имён бакетов из ответа list-buckets."""
    return sorted(b.get("Name") or "" for b in payload.get("Buckets") or [])


def parse_versioning_enabled(payload: dict) -> bool:
    """True, если на бакете включено версионирование."""
    return payload.get("Status") == "Enabled"


def parse_version_counts(payload: dict) -> dict[str, int]:
    """Счётчик версий по ключам (включая delete markers) из list-object-versions."""
    counts: dict[str, int] = {}
    for section in ("Versions", "DeleteMarkers"):
        for v in payload.get(section) or []:
            key = v.get("Key") or ""
            counts[key] = counts.get(key, 0) + 1
    return counts


def _bucket_name(bucket: str) -> str:
    return bucket.replace("s3://", "").split("/")[0]


def build_list_buckets_cmd(endpoint: str) -> list[str]:
    return ["aws", "s3api", "list-buckets", "--endpoint-url", endpoint]


def build_versioning_status_cmd(bucket: str, endpoint: str) -> list[str]:
    return [
        "aws", "s3api", "get-bucket-versioning",
        "--bucket", _bucket_name(bucket),
        "--endpoint-url", endpoint,
    ]


def build_list_cmd(bucket: str, prefix: str, endpoint: str) -> list[str]:
    cmd = [
        "aws", "s3api", "list-objects-v2",
        "--bucket", _bucket_name(bucket),
        "--delimiter", "/",
        "--endpoint-url", endpoint,
    ]
    if prefix:
        cmd += ["--prefix", prefix]
    return cmd


def build_versions_cmd(bucket: str, key: str, endpoint: str) -> list[str]:
    return [
        "aws", "s3api", "list-object-versions",
        "--bucket", _bucket_name(bucket),
        "--prefix", key,
        "--endpoint-url", endpoint,
    ]


def build_get_object_cmd(
    bucket: str, key: str, local_path: str, endpoint: str, version_id: str | None = None
) -> list[str]:
    cmd = [
        "aws", "s3api", "get-object",
        "--bucket", _bucket_name(bucket),
        "--key", key,
        "--endpoint-url", endpoint,
    ]
    if version_id:
        cmd += ["--version-id", version_id]
    cmd.append(local_path)
    return cmd


def build_upload_cmd(local: str, bucket: str, key: str, endpoint: str) -> list[str]:
    return ["aws", "s3", "cp", local, f"s3://{_bucket_name(bucket)}/{key}",
            "--endpoint-url", endpoint]


def build_delete_cmd(
    bucket: str, key: str, endpoint: str, version_id: str | None = None
) -> list[str]:
    cmd = [
        "aws", "s3api", "delete-object",
        "--bucket", _bucket_name(bucket),
        "--key", key,
        "--endpoint-url", endpoint,
    ]
    if version_id:
        cmd += ["--version-id", version_id]
    return cmd


def build_restore_cmd(bucket: str, key: str, version_id: str, endpoint: str) -> list[str]:
    """Server-side откат объекта к версии: copy-object версии поверх latest."""
    name = _bucket_name(bucket)
    source = f"{name}/{quote(key)}?versionId={version_id}"
    return [
        "aws", "s3api", "copy-object",
        "--bucket", name,
        "--key", key,
        "--copy-source", source,
        "--endpoint-url", endpoint,
    ]


async def run_aws(cmd: list[str], env: dict) -> OpResult:
    """Запускает aws CLI асинхронно; ошибки — в человекочитаемом виде."""
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await proc.communicate()
    except OSError as exc:
        return OpResult(ok=False, error=str(exc))
    if proc.returncode != 0:
        err = (stderr or b"").decode(errors="replace").strip()
        short = err.splitlines()[-1][-200:] if err else f"exit code {proc.returncode}"
        return OpResult(ok=False, error=f"{classify_error(err)}: {short}")
    payload = {}
    text = (stdout or b"").decode(errors="replace").strip()
    if text:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            payload = {}
    return OpResult(ok=True, payload=payload)


async def list_prefix(bucket: str, prefix: str, endpoint: str, env: dict):
    res = await run_aws(build_list_cmd(bucket, prefix, endpoint), env)
    if not res.ok:
        return res, []
    return res, parse_list_objects(res.payload, prefix)


async def list_versions(bucket: str, key: str, endpoint: str, env: dict):
    res = await run_aws(build_versions_cmd(bucket, key, endpoint), env)
    if not res.ok:
        return res, []
    return res, parse_list_versions(res.payload, key)
