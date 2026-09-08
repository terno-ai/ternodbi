"""Token refresh (§6.5): the *when* (expiry) and the *once* (single refresh)."""

import time

import pytest
from django.core.cache import cache

from terno_dbi.connectors.api.auth.tokens import (
    ensure_fresh_token,
    token_needs_refresh,
)


@pytest.fixture(autouse=True)
def clear_cache():
    cache.clear()
    yield
    cache.clear()


class _DS:
    id = 42


class TestExpiry:
    def test_absent_access_token_always_needs_refresh(self):
        assert token_needs_refresh({}) is True

    def test_no_expiry_recorded_is_not_refreshed(self):
        # Some providers issue long-lived tokens with no expiry.
        assert token_needs_refresh({"ACCESS_TOKEN": "t"}) is False

    def test_expired_needs_refresh(self):
        past = time.time() - 10
        assert token_needs_refresh(
            {"ACCESS_TOKEN": "t", "TOKEN_EXPIRES_AT": str(past)}
        ) is True

    def test_within_skew_needs_refresh(self):
        soon = time.time() + 60          # inside the 300s skew
        assert token_needs_refresh(
            {"ACCESS_TOKEN": "t", "TOKEN_EXPIRES_AT": str(soon)}
        ) is True

    def test_well_ahead_does_not(self):
        later = time.time() + 3600
        assert token_needs_refresh(
            {"ACCESS_TOKEN": "t", "TOKEN_EXPIRES_AT": str(later)}
        ) is False


class TestEnsureFreshToken:
    def test_fresh_token_is_returned_without_refreshing(self):
        fresh = {"ACCESS_TOKEN": "t", "TOKEN_EXPIRES_AT": str(time.time() + 3600)}
        calls = []

        def refresh():
            calls.append(1)
            return fresh

        out = ensure_fresh_token(_DS(), refresh, read_tokens=lambda: fresh)
        assert out == fresh
        assert calls == []          # never called

    def test_stale_token_triggers_a_refresh(self):
        stale = {"ACCESS_TOKEN": "old", "TOKEN_EXPIRES_AT": str(time.time() - 1)}
        new = {"ACCESS_TOKEN": "new", "TOKEN_EXPIRES_AT": str(time.time() + 3600)}
        calls = []

        def refresh():
            calls.append(1)
            return new

        out = ensure_fresh_token(_DS(), refresh, read_tokens=lambda: stale)
        assert out["ACCESS_TOKEN"] == "new"
        assert calls == [1]

    def test_loser_of_the_lock_does_not_refresh(self):
        # The token is stale on first read (so we enter the refresh path), but
        # another worker holds the lock and has already stored a fresh token.
        # The loser must re-read and take that, not refresh again.
        cache.add("api_token_refresh:42", "1", timeout=30)
        stale = {"ACCESS_TOKEN": "old", "TOKEN_EXPIRES_AT": str(time.time() - 1)}
        won = {"ACCESS_TOKEN": "won", "TOKEN_EXPIRES_AT": str(time.time() + 3600)}
        reads = iter([stale, won, won])   # first stale, then the winner's token
        calls = []

        def read_tokens():
            return next(reads)

        def refresh():
            calls.append(1)
            return {"ACCESS_TOKEN": "should-not-happen"}

        out = ensure_fresh_token(
            _DS(), refresh, read_tokens=read_tokens, wait_timeout=0.2,
        )
        assert out["ACCESS_TOKEN"] == "won"
        assert calls == []
