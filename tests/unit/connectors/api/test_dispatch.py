"""The dispatch pipeline (§6, §7): order, authorisation, caching, rate limit."""

import pytest
from django.core.cache import cache

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.pipeline.dispatch import run_query
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.pipeline.ratelimit import RateLimit
from terno_dbi.connectors.api.model.types import (
    Account,
    DateRange,
    Field,
    QueryResult,
    QuerySpec,
)


class _Catalog:
    def __init__(self):
        self.key = "fake"
        self.report_types = [{"id": "Default", "settings": []}]
        self.has_report_types = True
        self.default_report_type = ""


class _DS:
    type = "fake"
    connection_json = {"ACCESS_TOKEN": "t"}

    def __init__(self):
        self.catalog = _Catalog()   # a fresh catalog per connector, no leakage


class CountingConnector(ApiConnector):
    """Counts how often the provider is actually hit, to prove cache/authz."""

    def __init__(self):
        super().__init__(_DS())
        self.runs = 0

    def list_accounts(self):
        return [Account("1", "One")]

    def list_fields(self, report_type=None):
        return [Field("sessions", "Sessions", "metric")]

    def _run(self, spec):
        self.runs += 1
        return QueryResult(
            requested_field_ids=list(spec.fields),
            rows=[{"sessions": 10}],
            row_count=1,
        )


def _spec(accounts=None, end="2020-01-31"):
    return QuerySpec(
        accounts=accounts or ["1"],
        fields=["sessions"],
        date_range=DateRange("2020-01-01", end),
        report_type="Default",
    )


@pytest.fixture(autouse=True)
def clear_cache():
    cache.clear()
    yield
    cache.clear()


class TestDefaultReportType:
    """A connector may declare a default report type; the pipeline applies it
    when a query omits one — but only when the connector declared it."""

    def _capture_run_spec(self, conn):
        seen = {}
        orig = conn._run

        def spy(spec):
            seen["report_type"] = spec.report_type
            return orig(spec)

        conn._run = spy
        return seen

    def test_default_is_applied_when_omitted(self):
        conn = CountingConnector()
        conn.catalog.default_report_type = "Default"
        seen = self._capture_run_spec(conn)

        spec = QuerySpec(accounts=["1"], fields=["sessions"],
                         date_range=DateRange("2020-01-01", "2020-01-31"))  # no report_type
        run_query(conn, spec, org_id=1, permitted_accounts=["1"])
        assert seen["report_type"] == "Default"

    def test_explicit_report_type_is_untouched(self):
        conn = CountingConnector()
        conn.catalog.default_report_type = "Default"
        seen = self._capture_run_spec(conn)
        run_query(conn, _spec(), org_id=1, permitted_accounts=["1"])  # report_type="Default"
        assert seen["report_type"] == "Default"

    def test_no_declared_default_leaves_it_unset(self):
        conn = CountingConnector()
        conn.catalog.default_report_type = ""      # connector declares no default
        # Its catalog has report types, so an omitted report_type must fail
        # validation rather than being silently defaulted.
        spec = QuerySpec(accounts=["1"], fields=["sessions"],
                         date_range=DateRange("2020-01-01", "2020-01-31"))
        with pytest.raises(ApiError) as exc:
            run_query(conn, spec, org_id=1, permitted_accounts=["1"])
        assert exc.value.code == ErrorCode.INVALID_REPORT_TYPE


class TestAuthorisation:
    def test_permitted_account_passes(self):
        conn = CountingConnector()
        out = run_query(conn, _spec(["1"]), org_id=1, permitted_accounts=["1"])
        assert out["success"] is True

    def test_forbidden_account_is_rejected_before_the_provider(self):
        conn = CountingConnector()
        with pytest.raises(ApiError) as exc:
            run_query(conn, _spec(["2"]), org_id=1, permitted_accounts=["1"])
        assert exc.value.code == ErrorCode.ACCOUNT_FORBIDDEN
        assert conn.runs == 0            # never reached the provider

    def test_empty_allowlist_denies_everything(self):
        # The classic inversion bug: empty must not mean "all".
        conn = CountingConnector()
        with pytest.raises(ApiError) as exc:
            run_query(conn, _spec(["1"]), org_id=1, permitted_accounts=[])
        assert exc.value.code == ErrorCode.ACCOUNT_FORBIDDEN

    def test_none_is_unrestricted(self):
        conn = CountingConnector()
        out = run_query(conn, _spec(["9"]), org_id=1, permitted_accounts=None)
        assert out["success"] is True


class TestCaching:
    def test_second_identical_query_is_served_from_cache(self):
        conn = CountingConnector()
        first = run_query(conn, _spec(), org_id=1, permitted_accounts=["1"])
        second = run_query(conn, _spec(), org_id=1, permitted_accounts=["1"])
        assert first["cache_hit"] is False
        assert second["cache_hit"] is True
        assert conn.runs == 1            # provider hit once

    def test_different_org_does_not_share_the_cache(self):
        conn = CountingConnector()
        run_query(conn, _spec(), org_id=1, permitted_accounts=["1"])
        run_query(conn, _spec(), org_id=2, permitted_accounts=["1"])
        assert conn.runs == 2

    def test_use_cache_false_always_hits_the_provider(self):
        conn = CountingConnector()
        run_query(conn, _spec(), org_id=1, permitted_accounts=["1"], use_cache=False)
        run_query(conn, _spec(), org_id=1, permitted_accounts=["1"], use_cache=False)
        assert conn.runs == 2


class TestRateLimit:
    def test_rate_limit_rejects_before_cache_or_provider(self):
        conn = CountingConnector()
        limit = RateLimit(per_second=1)
        run_query(conn, _spec(), org_id=1, permitted_accounts=["1"], rate_limit=limit)
        with pytest.raises(ApiError) as exc:
            run_query(conn, _spec(end="2020-02-28"), org_id=1,
                      permitted_accounts=["1"], rate_limit=limit)
        assert exc.value.code == ErrorCode.RATE_LIMITED


# --- write-action dispatch (per-account write gate) ------------------------

class _WriteConnector:
    """Minimal connector for run_write_action: only what dispatch touches."""

    key = "fake"

    def __init__(self, datasource):
        self.datasource = datasource
        self.executed = []

    def execute_action(self, action_id, account, params=None, dry_run=False):
        from terno_dbi.connectors.api.model.types import ActionResult
        self.executed.append((action_id, account, params))
        return ActionResult(action=action_id, account=account, summary="done")


@pytest.fixture
def real_ds(db):
    from django.contrib.auth import get_user_model
    from terno_dbi.core.models import CoreOrganisation, DataSource
    User = get_user_model()
    owner = User.objects.create(username="wa-owner")
    org = CoreOrganisation.objects.create(
        name="WA", subdomain="wa", owner=owner)
    return DataSource.objects.create(
        display_name="wa-ds", type="google_ads", connection_str="",
        organisation=org)


@pytest.mark.django_db
def test_run_write_action_denies_until_account_opted_in(real_ds):
    from terno_dbi.connectors.api.pipeline.dispatch import run_write_action
    from terno_dbi.connectors.api.auth import account_selection as sel

    sel.sync_account_selections(real_ds, [Account("1", "One")])
    conn = _WriteConnector(real_ds)

    # writes off by default -> denied, and the connector is never called
    with pytest.raises(ApiError) as e:
        run_write_action(conn, "pause_campaign", "1", {"campaign_id": "5"})
    assert e.value.code == ErrorCode.ACCOUNT_FORBIDDEN
    assert conn.executed == []

    # opt the account into writes -> the action runs
    sel.set_writes_enabled_accounts(real_ds, ["1"])
    payload = run_write_action(conn, "pause_campaign", "1", {"campaign_id": "5"})
    assert payload["success"] is True
    assert conn.executed == [("pause_campaign", "1", {"campaign_id": "5"})]


@pytest.mark.django_db
def test_run_write_action_denies_a_different_account(real_ds):
    from terno_dbi.connectors.api.pipeline.dispatch import run_write_action
    from terno_dbi.connectors.api.auth import account_selection as sel

    sel.sync_account_selections(real_ds, [Account("1", "One"), Account("2", "Two")])
    sel.set_writes_enabled_accounts(real_ds, ["1"])   # only account 1
    conn = _WriteConnector(real_ds)

    with pytest.raises(ApiError) as e:
        run_write_action(conn, "pause_campaign", "2", {"campaign_id": "5"})
    assert e.value.code == ErrorCode.ACCOUNT_FORBIDDEN
    assert conn.executed == []
