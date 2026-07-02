from types import SimpleNamespace

from s3flood.executor import retry_with_backoff


def result(returncode, stderr=""):
    return SimpleNamespace(returncode=returncode, stderr=stderr, stdout="")


class TestRetryWithBackoff:
    def test_success_first_try_reports_one_attempt(self):
        res, ok, err, attempts = retry_with_backoff(lambda: result(0), 3, 2.0)
        assert ok is True
        assert err is None
        assert attempts == 1

    def test_failure_then_success_counts_attempts(self):
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            return result(1, "boom") if calls["n"] < 2 else result(0)

        res, ok, err, attempts = retry_with_backoff(flaky, 3, 1.0)
        assert ok is True
        assert attempts == 2

    def test_exhausted_retries(self):
        res, ok, err, attempts = retry_with_backoff(lambda: result(1, "err!"), 1, 1.0)
        assert ok is False
        assert "err!" in err
        assert attempts == 2
