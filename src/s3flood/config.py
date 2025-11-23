from __future__ import annotations

from argparse import Namespace
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationError


class RunConfigModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    profile: Optional[str] = None
    client: Optional[str] = None
    endpoint: Optional[str] = None
    endpoints: Optional[List[str]] = Field(
        default=None,
        validation_alias=AliasChoices("endpoints", "endpoint_list"),
    )
    endpoint_mode: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("endpoint_mode", "endpoint-mode"),
    )
    bucket: Optional[str] = None
    access_key: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("access_key", "access-key"),
    )
    secret_key: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("secret_key", "secret-key"),
    )
    aws_profile: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("aws_profile", "aws-profile"),
    )
    threads: Optional[int] = None
    data_dir: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("data_dir", "data-dir"),
    )
    report: Optional[str] = None
    metrics: Optional[str] = None
    infinite: Optional[bool] = None
    # Параметры для mixed профиля
    mixed_read_ratio: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    # Паттерны нагрузки
    pattern: Optional[str] = None  # sustained | bursty
    burst_duration_sec: Optional[float] = Field(default=None, gt=0.0)
    burst_intensity_multiplier: Optional[float] = Field(default=None, gt=1.0)
    # Управление очередью
    queue_limit: Optional[int] = Field(default=None, gt=0)
    max_retries: Optional[int] = Field(default=None, ge=0)
    retry_backoff_base: Optional[float] = Field(default=None, gt=1.0)
    # Порядок обработки файлов
    order: Optional[str] = None  # sequential | random


@dataclass
class RunSettings:
    profile: str
    client: str
    endpoint: str
    endpoints: List[str]
    endpoint_mode: str
    bucket: str
    access_key: Optional[str]
    secret_key: Optional[str]
    aws_profile: Optional[str]
    threads: int
    infinite: bool
    report: str
    metrics: str
    data_dir: str
    mixed_read_ratio: Optional[float]
    pattern: Optional[str]
    burst_duration_sec: Optional[float]
    burst_intensity_multiplier: Optional[float]
    queue_limit: Optional[int]
    max_retries: Optional[int]
    retry_backoff_base: Optional[float]
    order: Optional[str]

    def to_namespace(self) -> Namespace:
        return Namespace(**asdict(self))


def load_run_config(path: str) -> RunConfigModel:
    config_path = Path(path).expanduser()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as fh:
        parsed = yaml.safe_load(fh) or {}
    if isinstance(parsed, dict) and "run" in parsed and isinstance(parsed["run"], dict):
        parsed = parsed["run"]
    if not isinstance(parsed, dict):
        raise ValueError(f"Config file {config_path} must contain a mapping at the top level.")
    try:
        return RunConfigModel(**parsed)
    except ValidationError as exc:
        raise ValueError(f"Invalid run configuration in {config_path}: {exc}") from exc


def resolve_run_settings(cli_args: Namespace, config: Optional[RunConfigModel]) -> RunSettings:
    def pick(name: str, default=None):
        cli_value = getattr(cli_args, name, None)
        if cli_value is not None:
            return cli_value
        if config is not None:
            conf_value = getattr(config, name)
            if conf_value is not None:
                return conf_value
        return default

    profile = pick("profile")
    if profile is None:
        raise SystemExit("run: missing profile (use --profile or set in config file)")

    client = pick("client", default="awscli")
    endpoint_mode = pick("endpoint_mode", default="round-robin")
    endpoints = pick("endpoints")
    endpoint = pick("endpoint")
    bucket = pick("bucket")
    if endpoints:
        endpoints = [str(ep) for ep in endpoints if ep]
    if endpoints:
        primary_endpoint = endpoints[0]
    else:
        primary_endpoint = endpoint
    if not primary_endpoint:
        raise SystemExit("run: missing endpoint(s) (use --endpoint/--endpoints or set in config file)")
    if not endpoints:
        endpoints = [primary_endpoint]
    if endpoint_mode not in {"round-robin", "random"}:
        endpoint_mode = "round-robin"
    if bucket is None:
        raise SystemExit("run: missing bucket (use --bucket or set in config file)")

    threads = pick("threads", default=8)
    data_dir = pick("data_dir", default="./data")
    report = pick("report", default="report.json")
    metrics = pick("metrics", default="metrics.csv")

    infinite = pick("infinite", default=False)
    if infinite is None:
        infinite = False

    access_key = pick("access_key")
    secret_key = pick("secret_key")
    aws_profile = pick("aws_profile")

    # Параметры для mixed профиля (по умолчанию 70% чтения, 30% записи)
    mixed_read_ratio = pick("mixed_read_ratio")
    if profile == "mixed-70-30" and mixed_read_ratio is None:
        mixed_read_ratio = 0.7

    # Паттерны нагрузки
    pattern = pick("pattern", default="sustained")
    burst_duration_sec = pick("burst_duration_sec")
    burst_intensity_multiplier = pick("burst_intensity_multiplier", default=10.0)

    # Управление очередью
    queue_limit = pick("queue_limit")
    max_retries = pick("max_retries", default=3)
    retry_backoff_base = pick("retry_backoff_base", default=2.0)
    
    # Порядок обработки файлов
    order = pick("order", default="sequential")

    return RunSettings(
        profile=profile,
        client=client,
        endpoint=primary_endpoint,
        endpoints=endpoints,
        endpoint_mode=endpoint_mode,
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
        aws_profile=aws_profile,
        threads=threads,
        infinite=bool(infinite),
        report=report,
        metrics=metrics,
        data_dir=data_dir,
        mixed_read_ratio=mixed_read_ratio,
        pattern=pattern,
        burst_duration_sec=burst_duration_sec,
        burst_intensity_multiplier=burst_intensity_multiplier,
        queue_limit=queue_limit,
        max_retries=max_retries,
        retry_backoff_base=retry_backoff_base,
        order=order,
    )

