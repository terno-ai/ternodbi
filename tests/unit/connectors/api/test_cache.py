"""Result caching (§6.3): key construction and the closed/open TTL policy."""

import pytest
from django.core.cache import cache

from terno_dbi.connectors.api.pipeline.cache import (
    CLOSED_RANGE_TTL,
    OPEN_RANGE_TTL,
    cache_key,
    get_cached,
    set_cached,
)
from terno_dbi.connectors.api.model.types import Compare, DateRange, QuerySpec


@pytest.fixture(autouse=True)
def clear_cache():
    cache.clear()
    yield
    cache.clear()


def _spec(**kw):
    base = dict(
        accounts=["1"],
        fields=["date", "sessions"],
        date_range=DateRange("2020-01-01", "2020-01-31"),
    )
    base.update(kw)
    return QuerySpec(**base)


class TestKey:
    def test_field_order_does_not_fork_the_key(self):
        a = cache_key("ga4", 1, _spec(fields=["date", "sessions"]))
        b = cache_key("ga4", 1, _spec(fields=["sessions", "date"]))
        assert a == b

    def test_account_order_does_not_fork_the_key(self):
        a = cache_key("ga4", 1, _spec(accounts=["1", "2"]))
        b = cache_key("ga4", 1, _spec(accounts=["2", "1"]))
        assert a == b

    def test_different_org_is_a_different_key(self):
        assert cache_key("ga4", 1, _spec()) != cache_key("ga4", 2, _spec())

    def test_filters_change_the_key(self):
        assert cache_key("ga4", 1, _spec(filters="clicks > 1")) != \
               cache_key("ga4", 1, _spec())

    def test_compare_changes_the_key(self):
        assert cache_key("ga4", 1, _spec(compare=Compare("prev_year"))) != \
               cache_key("ga4", 1, _spec())


class TestTtlPolicy:
    def test_closed_range_is_cached_hard(self):
        # ends in 2020 — long closed, whatever "today" is.
        ttl = set_cached("ga4", 1, _spec(), {"x": 1})
        assert ttl == CLOSED_RANGE_TTL

    def test_range_touching_today_is_cached_briefly(self):
        spec = _spec(date_range=DateRange("2020-01-01", "2999-01-01"))
        ttl = set_cached("ga4", 1, spec, {"x": 1})
        assert ttl == OPEN_RANGE_TTL

    def test_inclusive_of_today_is_always_open(self):
        spec = _spec(date_range=DateRange("2020-01-01", "2020-01-31",
                                          inclusive_of_today=True))
        ttl = set_cached("ga4", 1, spec, {"x": 1})
        assert ttl == OPEN_RANGE_TTL


class TestRoundTrip:
    def test_store_then_get(self):
        spec = _spec()
        assert get_cached("ga4", 1, spec) is None
        set_cached("ga4", 1, spec, {"rows": [1, 2]})
        assert get_cached("ga4", 1, spec) == {"rows": [1, 2]}

    def test_a_different_org_gets_no_hit(self):
        spec = _spec()
        set_cached("ga4", 1, spec, {"rows": [1]})
        assert get_cached("ga4", 2, spec) is None
