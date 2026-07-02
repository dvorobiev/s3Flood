from s3flood.s3browser_io import (
    build_delete_cmd,
    build_get_object_cmd,
    build_list_cmd,
    build_restore_cmd,
    build_upload_cmd,
    build_versions_cmd,
    parse_list_objects,
    parse_list_versions,
)


class TestParseListObjects:
    PAYLOAD = {
        "CommonPrefixes": [{"Prefix": "data/small/"}, {"Prefix": "data/large/"}],
        "Contents": [
            {"Key": "data/readme.txt", "Size": 100, "LastModified": "2026-07-01T10:00:00Z"},
            {"Key": "data/a.bin", "Size": 2048, "LastModified": "2026-07-01T11:00:00Z"},
            {"Key": "data/", "Size": 0, "LastModified": "2026-07-01T09:00:00Z"},
        ],
    }

    def test_dirs_first_then_files_sorted(self):
        entries = parse_list_objects(self.PAYLOAD, prefix="data/")
        names = [e.name for e in entries]
        assert names == ["large/", "small/", "a.bin", "readme.txt"]
        assert entries[0].is_dir and entries[1].is_dir
        assert not entries[2].is_dir

    def test_placeholder_of_prefix_itself_skipped(self):
        entries = parse_list_objects(self.PAYLOAD, prefix="data/")
        assert all(e.key != "data/" for e in entries)

    def test_keys_and_sizes(self):
        entries = parse_list_objects(self.PAYLOAD, prefix="data/")
        by_name = {e.name: e for e in entries}
        assert by_name["a.bin"].key == "data/a.bin"
        assert by_name["a.bin"].size == 2048
        assert by_name["small/"].key == "data/small/"

    def test_empty_payload(self):
        assert parse_list_objects({}, prefix="") == []


class TestParseListVersions:
    PAYLOAD = {
        "Versions": [
            {"Key": "data/a.bin", "VersionId": "v3", "Size": 30,
             "LastModified": "2026-07-03T10:00:00Z", "IsLatest": True},
            {"Key": "data/a.bin", "VersionId": "v1", "Size": 10,
             "LastModified": "2026-07-01T10:00:00Z", "IsLatest": False},
            {"Key": "data/a.bin.bak", "VersionId": "x1", "Size": 99,
             "LastModified": "2026-07-02T10:00:00Z", "IsLatest": True},
        ],
        "DeleteMarkers": [
            {"Key": "data/a.bin", "VersionId": "v2", "IsLatest": False,
             "LastModified": "2026-07-02T10:00:00Z"},
        ],
    }

    def test_exact_key_only(self):
        versions = parse_list_versions(self.PAYLOAD, key="data/a.bin")
        assert all(v.version_id in {"v1", "v2", "v3"} for v in versions)
        assert len(versions) == 3

    def test_sorted_newest_first(self):
        versions = parse_list_versions(self.PAYLOAD, key="data/a.bin")
        assert [v.version_id for v in versions] == ["v3", "v2", "v1"]

    def test_latest_and_delete_marker_flags(self):
        versions = {v.version_id: v for v in parse_list_versions(self.PAYLOAD, key="data/a.bin")}
        assert versions["v3"].is_latest
        assert versions["v2"].is_delete_marker
        assert not versions["v1"].is_latest

    def test_empty(self):
        assert parse_list_versions({}, key="x") == []


class TestCommandBuilders:
    EP = "http://127.0.0.1:9000"

    def test_list_cmd_uses_delimiter_and_prefix(self):
        cmd = build_list_cmd("b", "data/", self.EP)
        assert "list-objects-v2" in cmd
        assert "--delimiter" in cmd and "/" in cmd
        i = cmd.index("--prefix")
        assert cmd[i + 1] == "data/"
        assert "--endpoint-url" in cmd

    def test_list_cmd_empty_prefix_omits_prefix_flag(self):
        cmd = build_list_cmd("b", "", self.EP)
        assert "--prefix" not in cmd

    def test_versions_cmd(self):
        cmd = build_versions_cmd("b", "data/a.bin", self.EP)
        assert "list-object-versions" in cmd
        assert "data/a.bin" in cmd

    def test_get_object_with_version(self):
        cmd = build_get_object_cmd("b", "k.bin", "/tmp/x", self.EP, version_id="v7")
        assert "get-object" in cmd
        i = cmd.index("--version-id")
        assert cmd[i + 1] == "v7"
        assert cmd[-1] == "/tmp/x" or "/tmp/x" in cmd

    def test_get_object_without_version(self):
        cmd = build_get_object_cmd("b", "k.bin", "/tmp/x", self.EP)
        assert "--version-id" not in cmd

    def test_delete_with_version(self):
        cmd = build_delete_cmd("b", "k.bin", self.EP, version_id="v7")
        assert "delete-object" in cmd and "v7" in cmd

    def test_restore_builds_copy_source_with_version(self):
        cmd = build_restore_cmd("b", "data/a.bin", "v7", self.EP)
        assert "copy-object" in cmd
        i = cmd.index("--copy-source")
        assert cmd[i + 1] == "b/data/a.bin?versionId=v7"
        i = cmd.index("--key")
        assert cmd[i + 1] == "data/a.bin"

    def test_restore_quotes_special_chars_in_copy_source(self):
        cmd = build_restore_cmd("b", "папка/файл 1.bin", "v7", self.EP)
        i = cmd.index("--copy-source")
        src = cmd[i + 1]
        assert " " not in src and "?versionId=v7" in src

    def test_upload_cmd(self):
        cmd = build_upload_cmd("/tmp/f.bin", "b", "data/f.bin", self.EP)
        assert cmd[:3] == ["aws", "s3", "cp"]
        assert "s3://b/data/f.bin" in cmd
