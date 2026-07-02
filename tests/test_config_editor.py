import re

from s3flood.config_editor import _normalize_endpoint
from s3flood.defaults import DEFAULT_ENDPOINT

VALID_ENDPOINT = re.compile(r"^https?://[^:/]+(:\d+)?$")


class TestNormalizeEndpoint:
    def test_adds_scheme_and_port(self):
        assert _normalize_endpoint("myhost") == "http://myhost:9080"

    def test_keeps_existing_port(self):
        assert _normalize_endpoint("http://h:9000") == "http://h:9000"

    def test_empty(self):
        assert _normalize_endpoint("") is None
        assert _normalize_endpoint(None) is None


class TestDefaultEndpoint:
    def test_default_endpoint_is_valid_url(self):
        # регрессия: дефолт был "http://localhost:9000:9080" — двойной порт
        assert VALID_ENDPOINT.match(DEFAULT_ENDPOINT)

    def test_no_double_port_literal_in_sources(self):
        import s3flood.config_editor as ce
        import inspect
        src = inspect.getsource(ce)
        assert "9000:9080" not in src
