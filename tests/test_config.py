from argparse import Namespace

import pytest

from s3flood.config import RunConfigModel, resolve_run_settings


def make_config(**over):
    base = {"endpoint": "http://cfg:9000", "bucket": "cfg-bucket"}
    base.update(over)
    return RunConfigModel(**base)


class TestResolveRunSettings:
    def test_minimal_namespace_enough(self):
        # resolve использует getattr(..., None): достаточно Namespace(profile=...)
        s = resolve_run_settings(Namespace(profile="write"), make_config())
        assert s.endpoint == "http://cfg:9000"
        assert s.bucket == "cfg-bucket"
        assert s.threads == 8  # дефолт

    def test_cli_overrides_config(self):
        s = resolve_run_settings(
            Namespace(profile="write", threads=4, bucket="cli-bucket"),
            make_config(threads=16),
        )
        assert s.threads == 4
        assert s.bucket == "cli-bucket"

    def test_mixed_defaults_ratio(self):
        s = resolve_run_settings(Namespace(profile="mixed"), make_config())
        assert s.mixed_read_ratio == 0.7

    def test_legacy_mixed_70_30_profile(self):
        s = resolve_run_settings(Namespace(profile="mixed-70-30"), make_config())
        assert s.profile == "mixed"

    def test_missing_bucket_raises(self):
        with pytest.raises(SystemExit):
            resolve_run_settings(
                Namespace(profile="write"), RunConfigModel(endpoint="http://x")
            )

    def test_endpoints_list_takes_priority(self):
        s = resolve_run_settings(
            Namespace(profile="write"),
            make_config(endpoints=["http://n1:9000", "http://n2:9000"]),
        )
        assert s.endpoint == "http://n1:9000"
        assert s.endpoints == ["http://n1:9000", "http://n2:9000"]
