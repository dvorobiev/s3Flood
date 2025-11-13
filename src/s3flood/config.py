from __future__ import annotations

from argparse import Namespace
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import yaml
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationError


class RunConfigModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    profile: Optional[str] = None
    client: Optional[str] = None
    endpoint: Optional[str] = None
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


@dataclass
class RunSettings:
    profile: str
    client: str
    endpoint: str
    bucket: str
    access_key: Optional[str]
    secret_key: Optional[str]
    aws_profile: Optional[str]
    threads: int
    infinite: bool
    report: str
    metrics: str
    data_dir: str

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
    endpoint = pick("endpoint")
    bucket = pick("bucket")
    if endpoint is None:
        raise SystemExit("run: missing endpoint (use --endpoint or set in config file)")
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

    return RunSettings(
        profile=profile,
        client=client,
        endpoint=endpoint,
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
        aws_profile=aws_profile,
        threads=threads,
        infinite=bool(infinite),
        report=report,
        metrics=metrics,
        data_dir=data_dir,
    )

