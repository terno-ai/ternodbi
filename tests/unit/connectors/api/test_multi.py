"""Partial-success fan-out across accounts."""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.sources._multi import gather_accounts


def test_good_accounts_survive_a_bad_one():
    def fetch(account):
        if account == "bad":
            raise ApiError(ErrorCode.UPSTREAM_ERROR, "not enabled")
        return [{"_account": account, "v": 1}]

    rows, warnings = gather_accounts(["a", "bad", "b"], fetch)
    assert [r["_account"] for r in rows] == ["a", "b"]
    assert len(warnings) == 1
    assert "bad" in warnings[0] and "not enabled" in warnings[0]


def test_all_failing_reraises_the_first_error():
    def fetch(account):
        raise ApiError(ErrorCode.UPSTREAM_ERROR, f"fail {account}")

    with pytest.raises(ApiError) as exc:
        gather_accounts(["a", "b"], fetch)
    assert exc.value.message == "fail a"     # the first, not a swallowed generic


def test_all_succeeding_has_no_warnings():
    rows, warnings = gather_accounts(["a", "b"], lambda a: [{"_account": a}])
    assert len(rows) == 2
    assert warnings == []


def test_non_api_errors_are_not_swallowed():
    # A bug (KeyError, etc.) must surface, not be hidden as a per-account warning.
    def fetch(account):
        raise KeyError("bug")

    with pytest.raises(KeyError):
        gather_accounts(["a"], fetch)


class TestParallel:
    def test_runs_accounts_concurrently(self):
        import threading
        barrier = threading.Barrier(3, timeout=5)

        def fetch(account):
            barrier.wait()          # only passes if all three run at once
            return [{"_account": account}]

        rows, warnings = gather_accounts(["a", "b", "c"], fetch, max_workers=3)
        assert [r["_account"] for r in rows] == ["a", "b", "c"]
        assert warnings == []

    def test_keeps_account_order_and_partial_success(self):
        import time

        def fetch(account):
            if account == "bad":
                raise ApiError(ErrorCode.UPSTREAM_ERROR, "not enabled")
            time.sleep(0.02 if account == "a" else 0)   # finish out of order
            return [{"_account": account}]

        rows, warnings = gather_accounts(["a", "bad", "b"], fetch, max_workers=4)
        assert [r["_account"] for r in rows] == ["a", "b"]
        assert warnings == ["Account bad skipped: not enabled"]

    def test_all_failing_reraises_the_first_error(self):
        def fetch(account):
            raise ApiError(ErrorCode.UPSTREAM_ERROR, f"fail {account}")

        with pytest.raises(ApiError) as exc:
            gather_accounts(["a", "b"], fetch, max_workers=2)
        assert exc.value.message == "fail a"

    def test_non_api_errors_are_not_swallowed(self):
        def fetch(account):
            raise KeyError("bug")

        with pytest.raises(KeyError):
            gather_accounts(["a", "b"], fetch, max_workers=2)
