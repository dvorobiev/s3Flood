from s3flood.app_settings import (
    APP_SETTINGS_FILE,
    get_dataset_dir,
    load_app_settings,
    save_app_settings,
)


class TestAppSettings:
    def test_missing_file_returns_empty(self, tmp_path):
        assert load_app_settings(tmp_path) == {}
        assert get_dataset_dir(tmp_path) is None

    def test_save_and_load_roundtrip(self, tmp_path):
        save_app_settings({"dataset_dir": "/data/set"}, tmp_path)
        assert load_app_settings(tmp_path) == {"dataset_dir": "/data/set"}
        assert get_dataset_dir(tmp_path) == "/data/set"

    def test_save_merges_existing_keys(self, tmp_path):
        save_app_settings({"dataset_dir": "/a"}, tmp_path)
        save_app_settings({"future_key": 1}, tmp_path)
        data = load_app_settings(tmp_path)
        assert data["dataset_dir"] == "/a" and data["future_key"] == 1

    def test_corrupt_file_returns_empty(self, tmp_path):
        (tmp_path / APP_SETTINGS_FILE).write_text("- just\n- a list\n")
        assert load_app_settings(tmp_path) == {}
        (tmp_path / APP_SETTINGS_FILE).write_text("{{invalid yaml")
        assert load_app_settings(tmp_path) == {}

    def test_filename_is_dotfile(self):
        assert APP_SETTINGS_FILE == ".s3flood.yml"
