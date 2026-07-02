"""Запуск S3-операций через субпроцессы aws CLI.

Изолирует всю работу с aws CLI: окружение/профили, запуск процессов
с возможностью прерывания, ретраи с backoff и реестр активных процессов
для корректного завершения по Ctrl+C.
"""
from __future__ import annotations

import inspect
import json
import os
import subprocess
import threading
import time
from pathlib import Path

CONFIG_HOME = Path.home()
CUSTOM_AWS_PROFILE = "s3flood-temp"
CUSTOM_AWS_CONFIG_PATH = Path(
    os.environ.get("S3FLOOD_CUSTOM_AWS_CONFIG") or (CONFIG_HOME / ".aws" / "s3flood-config")
).expanduser()
_custom_config_signature: tuple | None = None
_custom_config_lock = threading.Lock()

# Механизм отслеживания активных процессов для корректного завершения при прерывании
_active_processes: set[subprocess.Popen] = set()
_active_processes_lock = threading.Lock()


def _format_cli_size(value: int) -> str:
    """Преобразует размер в строку формата AWS CLI (MB/GB)."""
    gb = 1024 * 1024 * 1024
    mb = 1024 * 1024
    if value % gb == 0:
        return f"{value // gb}GB"
    if value % mb == 0:
        return f"{value // mb}MB"
    return f"{value}B"


def _ensure_custom_aws_config(
    source_profile: str | None,
    s3_settings: dict[str, str],
    access_key: str | None,
    secret_key: str | None,
) -> Path:
    """
    Создаёт отдельный AWS config с нужными параметрами S3 и (опционально) source_profile.
    """
    global _custom_config_signature
    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
    signature = (
        tuple(sorted(s3_settings.items())),
        source_profile,
        region,
        access_key,
        secret_key,
    )
    with _custom_config_lock:
        if _custom_config_signature == signature and CUSTOM_AWS_CONFIG_PATH.exists():
            return CUSTOM_AWS_CONFIG_PATH

        block_lines = [f"[profile {CUSTOM_AWS_PROFILE}]"]
        if source_profile:
            block_lines.append(f"source_profile = {source_profile}")
        if region:
            block_lines.append(f"region = {region}")
        if s3_settings:
            block_lines.append("s3 =")
            for key, value in s3_settings.items():
                block_lines.append(f"    {key} = {value}")
        if access_key and secret_key:
            block_lines.append(f"aws_access_key_id = {access_key}")
            block_lines.append(f"aws_secret_access_key = {secret_key}")

        config_content = "\n".join(block_lines).strip() + "\n"
        CUSTOM_AWS_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(CUSTOM_AWS_CONFIG_PATH, "w", encoding="utf-8") as cfg:
            cfg.write(config_content)
        try:
            os.chmod(CUSTOM_AWS_CONFIG_PATH, 0o600)
        except OSError:
            pass
        _custom_config_signature = signature
        return CUSTOM_AWS_CONFIG_PATH


def _register_process(proc: subprocess.Popen) -> None:
    """Регистрирует процесс в списке активных для возможности прерывания."""
    with _active_processes_lock:
        _active_processes.add(proc)


def _unregister_process(proc: subprocess.Popen) -> None:
    """Удаляет процесс из списка активных."""
    with _active_processes_lock:
        _active_processes.discard(proc)


def _terminate_all_processes() -> None:
    """Завершает все активные процессы при прерывании."""
    with _active_processes_lock:
        processes_to_terminate = list(_active_processes)
    for proc in processes_to_terminate:
        try:
            if proc.poll() is None:  # Процесс еще работает
                proc.terminate()
                # Даем процессу время на корректное завершение
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    # Если не завершился за 2 секунды, принудительно убиваем
                    proc.kill()
                    proc.wait()
        except Exception:
            # Игнорируем ошибки при завершении процессов
            pass
    with _active_processes_lock:
        _active_processes.clear()


def _get_aws_env(
    access_key: str | None,
    secret_key: str | None,
    aws_profile: str | None,
    multipart_threshold: int | None = None,
    multipart_chunksize: int | None = None,
    max_concurrent_requests: int | None = None,
) -> tuple[dict, str | None]:
    """
    Возвращает (env, profile_name) для запуска AWS CLI.
    profile_name — имя профиля, который нужно передать в aws ... --profile.
    """
    env = os.environ.copy()
    env["AWS_EC2_METADATA_DISABLED"] = "true"
    # Отключаем автоматические checksums для совместимости с S3-совместимыми бекендами
    # (начиная с boto3 1.36.0 checksums включены по умолчанию, что может вызывать BadDigest)
    env["AWS_S3_DISABLE_REQUEST_CHECKSUM"] = "true"

    s3_settings: dict[str, str] = {}
    if multipart_threshold is not None:
        s3_settings["multipart_threshold"] = _format_cli_size(multipart_threshold)
    if multipart_chunksize is not None:
        s3_settings["multipart_chunksize"] = _format_cli_size(multipart_chunksize)
    if max_concurrent_requests is not None:
        s3_settings["max_concurrent_requests"] = str(max_concurrent_requests)

    custom_path = _ensure_custom_aws_config(aws_profile, s3_settings, access_key, secret_key)
    env["AWS_CONFIG_FILE"] = str(custom_path)
    profile_to_use: str | None = CUSTOM_AWS_PROFILE

    if access_key and secret_key:
        env["AWS_ACCESS_KEY_ID"] = access_key
        env["AWS_SECRET_ACCESS_KEY"] = secret_key
    else:
        env.pop("AWS_ACCESS_KEY_ID", None)
        env.pop("AWS_SECRET_ACCESS_KEY", None)

    env["AWS_PROFILE"] = profile_to_use

    # Больше не используем AWS_CLI_FILE_TRANSFER_CONFIG
    env.pop("AWS_CLI_FILE_TRANSFER_CONFIG", None)

    return env, profile_to_use


def _run_interruptible(cmd: list[str], env: dict, stop: threading.Event | None = None):
    """Запускает процесс и ждёт завершения с возможностью прерывания по stop.

    communicate(timeout=...) возвращается в момент фактического выхода процесса,
    поэтому время завершения не квантуется интервалом опроса (старый цикл
    poll()+sleep(0.1) добавлял к latency до 100 мс).
    """
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    _register_process(proc)
    try:
        while True:
            try:
                stdout, stderr = proc.communicate(timeout=0.25)
                break
            except subprocess.TimeoutExpired:
                if stop and stop.is_set():
                    proc.terminate()
                    try:
                        stdout, stderr = proc.communicate(timeout=2)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        stdout, stderr = proc.communicate()
                    break
        return subprocess.CompletedProcess(proc.args, proc.returncode, stdout, stderr)
    finally:
        _unregister_process(proc)


def aws_cp_upload(
    local: Path,
    bucket: str,
    key: str,
    endpoint: str,
    access_key: str | None,
    secret_key: str | None,
    aws_profile: str | None,
    multipart_threshold: int | None = None,
    multipart_chunksize: int | None = None,
    max_concurrent_requests: int | None = None,
    stop: threading.Event | None = None,
):
    """Загружает файл в S3 через aws s3 cp (multipart выбирается CLI автоматически)."""
    env, profile_name = _get_aws_env(
        access_key, secret_key, aws_profile,
        multipart_threshold, multipart_chunksize, max_concurrent_requests
    )
    url = f"{bucket}/{key}" if bucket.startswith("s3://") else f"s3://{bucket}/{key}"
    cmd = ["aws", "s3", "cp", str(local), url, "--endpoint-url", endpoint]
    if profile_name:
        cmd.extend(["--profile", profile_name])
    return _run_interruptible(cmd, env, stop)


def aws_list_objects(
    bucket: str,
    endpoint: str,
    access_key: str | None,
    secret_key: str | None,
    aws_profile: str | None,
    multipart_threshold: int | None = None,
    multipart_chunksize: int | None = None,
    max_concurrent_requests: int | None = None,
):
    """Получает список объектов из бакета через s3api list-objects-v2."""
    env, profile_name = _get_aws_env(
        access_key, secret_key, aws_profile,
        multipart_threshold, multipart_chunksize, max_concurrent_requests
    )
    bucket_name = bucket.replace("s3://", "").split("/")[0]
    cmd = ["aws", "s3api", "list-objects-v2", "--bucket", bucket_name, "--endpoint-url", endpoint]
    if profile_name:
        cmd.extend(["--profile", profile_name])
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if res.returncode != 0:
        return None
    try:
        data = json.loads(res.stdout)
        objects = []
        if "Contents" in data:
            for obj in data["Contents"]:
                objects.append({
                    "key": obj["Key"],
                    "size": obj.get("Size", 0),
                })
        return objects
    except (json.JSONDecodeError, KeyError):
        return None


def aws_check_bucket_access(
    bucket: str,
    endpoint: str,
    access_key: str | None,
    secret_key: str | None,
    aws_profile: str | None,
    multipart_threshold: int | None = None,
    multipart_chunksize: int | None = None,
    max_concurrent_requests: int | None = None,
):
    """Быстрая проверка доступа к бакету через s3api head-bucket."""
    env, profile_name = _get_aws_env(
        access_key, secret_key, aws_profile,
        multipart_threshold, multipart_chunksize, max_concurrent_requests
    )
    bucket_name = bucket.replace("s3://", "").split("/")[0]
    cmd = ["aws", "s3api", "head-bucket", "--bucket", bucket_name, "--endpoint-url", endpoint]
    if profile_name:
        cmd.extend(["--profile", profile_name])
    return subprocess.run(cmd, capture_output=True, text=True, env=env)


def aws_cp_download(
    bucket: str,
    key: str,
    endpoint: str,
    access_key: str | None,
    secret_key: str | None,
    aws_profile: str | None,
    multipart_threshold: int | None = None,
    multipart_chunksize: int | None = None,
    max_concurrent_requests: int | None = None,
    stop: threading.Event | None = None,
):
    """
    Скачивает файл из S3 используя aws s3 cp.
    AWS CLI может использовать параллельные запросы (range requests) для больших файлов,
    что улучшает производительность. Параметр max_concurrent_requests из
    AWS_CLI_FILE_TRANSFER_CONFIG влияет на количество параллельных запросов.
    
    Примечание: multipart upload используется только для upload, не для download.
    Но aws s3 cp использует оптимизации для download через параллельные range requests.
    
    Args:
        stop: Event для проверки прерывания. Если установлен, процесс будет прерван.
    """
    env, profile_name = _get_aws_env(
        access_key, secret_key, aws_profile,
        multipart_threshold, multipart_chunksize, max_concurrent_requests
    )
    devnull = "NUL" if os.name == "nt" else "/dev/null"
    url = f"{bucket}/{key}" if bucket.startswith("s3://") else f"s3://{bucket}/{key}"
    cmd = ["aws", "s3", "cp", url, devnull, "--endpoint-url", endpoint]
    if profile_name:
        cmd.extend(["--profile", profile_name])
    res = _run_interruptible(cmd, env, stop)

    # aws s3 cp не может обновить mtime у /dev/null и возвращает ошибку,
    # хотя данные уже скачаны — считаем такую операцию успешной
    if res.returncode != 0 and res.stderr:
        stderr_low = res.stderr.lower()
        if ("download" in stderr_low or "successfully" in stderr_low) and \
           ("unable to update" in stderr_low or "last modified" in stderr_low or "dev/null" in stderr_low):
            return subprocess.CompletedProcess(res.args, 0, res.stdout, "")
    return res


def retry_with_backoff(func, max_retries: int, backoff_base: float, *args, stop: threading.Event | None = None, **kwargs):
    """Выполняет функцию с повторами и экспоненциальным backoff.

    Возвращает (result, ok, error, attempts) — attempts нужен для метрик.
    """
    last_error = None
    last_result = None
    accepts_stop = "stop" in inspect.signature(func).parameters

    def wait_or_abort(attempt: int) -> bool:
        """Ждёт backoff-паузу; False — если во время ожидания пришёл stop."""
        wait_time = backoff_base ** attempt
        for _ in range(int(wait_time * 10)):
            if stop and stop.is_set():
                return False
            time.sleep(0.1)
        return True

    for attempt in range(max_retries + 1):
        attempts = attempt + 1
        if stop and stop.is_set():
            return None, False, "interrupted by user", attempts
        try:
            if accepts_stop:
                result = func(*args, stop=stop, **kwargs)
            else:
                result = func(*args, **kwargs)
        except Exception as e:
            result = None
            last_error = str(e)
        else:
            if result is None:
                last_error = "function returned None"
            elif not hasattr(result, "returncode"):
                last_error = f"result has no returncode attribute, type: {type(result)}"
                last_result = result
            elif result.returncode == 0:
                return result, True, None, attempts
            else:
                last_result = result
                stderr = getattr(result, "stderr", None)
                last_error = stderr[-200:] if stderr else f"exit code {result.returncode}"
        if attempt < max_retries:
            if not wait_or_abort(attempt):
                return None, False, "interrupted by user", attempts
            continue
        return last_result, False, last_error or "max retries exceeded", attempts
    return last_result, False, last_error or "max retries exceeded", max_retries + 1


