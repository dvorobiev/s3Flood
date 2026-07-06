from argparse import Namespace

import pytest

from s3flood.app_settings import save_app_settings
from s3flood.config import RunConfigModel, discover_configs, resolve_run_settings


def make_config(**over):
    base = {"endpoint": "http://cfg:9000", "bucket": "cfg-bucket"}
    base.update(over)
    return RunConfigModel(**base)


def _min_args(**kw):
    return Namespace(profile="write", endpoint="http://h:9000", bucket="b", **kw)


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


class TestDataDirPriority:
    def test_default_when_no_sources(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        settings = resolve_run_settings(_min_args(), None)
        assert settings.data_dir == "./data"

    def test_app_settings_used(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        save_app_settings({"dataset_dir": "/srv/dataset"}, tmp_path)
        settings = resolve_run_settings(_min_args(), None)
        assert settings.data_dir == "/srv/dataset"

    def test_cli_overrides_app_settings(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        save_app_settings({"dataset_dir": "/srv/dataset"}, tmp_path)
        settings = resolve_run_settings(_min_args(data_dir="/cli/data"), None)
        assert settings.data_dir == "/cli/data"

    def test_config_data_dir_ignored_with_warning(self, tmp_path, monkeypatch, capsys):
        monkeypatch.chdir(tmp_path)
        config = RunConfigModel(data_dir="/from/config")
        settings = resolve_run_settings(_min_args(), config)
        assert settings.data_dir == "./data"
        assert "data_dir" in capsys.readouterr().err


class TestDiscoverConfigs:
    def test_finds_flat_and_run_section_configs(self, tmp_path):
        (tmp_path / "kazan.yml").write_text("endpoint: http://h:9000\nbucket: b\n")
        (tmp_path / "tape.yaml").write_text("run:\n  bucket: b\n")
        (tmp_path / "config.old.yaml").write_text("profile: write\n")
        found = {p.name for p in discover_configs(tmp_path)}
        assert found == {"kazan.yml", "tape.yaml", "config.old.yaml"}

    def test_skips_non_config_yaml(self, tmp_path):
        (tmp_path / "list.yml").write_text("- a\n- b\n")
        (tmp_path / "scalar.yml").write_text("42\n")
        (tmp_path / "other.yml").write_text("name: x\nvalue: y\n")
        (tmp_path / "broken.yml").write_text("{{not yaml")
        (tmp_path / "empty.yml").write_text("")
        assert discover_configs(tmp_path) == []

    def test_dotfile_settings_not_listed(self, tmp_path):
        # даже если содержимое дотфайла похоже на конфиг — он скрытый, в список не попадает
        (tmp_path / ".s3flood.yml").write_text("bucket: b\ndataset_dir: /x\n")
        (tmp_path / "real.yml").write_text("bucket: b\n")
        found = [p.name for p in discover_configs(tmp_path)]
        assert found == ["real.yml"]

    def test_sorted_output(self, tmp_path):
        (tmp_path / "b.yml").write_text("bucket: b\n")
        (tmp_path / "a.yaml").write_text("bucket: b\n")
        assert [p.name for p in discover_configs(tmp_path)] == ["a.yaml", "b.yml"]
