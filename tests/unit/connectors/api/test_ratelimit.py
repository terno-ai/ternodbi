"""Rate limiting (§6.2) — the guard against retry storms and quota burn."""

import pytest
from django.core.cache import cache

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.pipeline.ratelimit import RateLimit, check_rate_limit


@pytest.fixture(autouse=True)
def clear_cache():
    cache.clear()
    yield
    cache.clear()


class TestPerSecond:
    def test_allows_up_to_the_limit(self):
        limit = RateLimit(per_second=3)
        for _ in range(3):
            check_rate_limit("google_ads", 1, limit)   # no raise

    def test_rejects_over_the_limit(self):
        limit = RateLimit(per_second=2)
        check_rate_limit("google_ads", 1, limit)
        check_rate_limit("google_ads", 1, limit)
        with pytest.raises(ApiError) as exc:
            check_rate_limit("google_ads", 1, limit)
        assert exc.value.code == ErrorCode.RATE_LIMITED
        assert exc.value.retriable is True

    def test_counts_are_per_org(self):
        limit = RateLimit(per_second=1)
        check_rate_limit("google_ads", 1, limit)
        # A different org has its own budget.
        check_rate_limit("google_ads", 2, limit)   # no raise

    def test_counts_are_per_source(self):
        limit = RateLimit(per_second=1)
        check_rate_limit("google_ads", 1, limit)
        check_rate_limit("meta_ads", 1, limit)      # different source, no raise


class TestPerDay:
    def test_quota_exceeded_is_its_own_code(self):
        limit = RateLimit(per_day=2)
        check_rate_limit("google_ads", 1, limit)
        check_rate_limit("google_ads", 1, limit)
        with pytest.raises(ApiError) as exc:
            check_rate_limit("google_ads", 1, limit)
        assert exc.value.code == ErrorCode.QUOTA_EXCEEDED
        # Tells the agent when to come back.
        assert exc.value.retry_after_seconds is not None


class TestNoLimit:
    def test_absent_windows_are_not_enforced(self):
        limit = RateLimit()   # nothing declared
        for _ in range(1000):
            check_rate_limit("x", 1, limit)   # never raises
