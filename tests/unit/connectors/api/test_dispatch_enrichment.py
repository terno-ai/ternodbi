"""Dispatch enrichment: comparison, aggregation notes, currency guard (§6.7, §6.9)."""

import pytest
from django.core.cache import cache

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.pipeline.dispatch import run_query
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import (
    Account,
    Compare,
    DateRange,
    Field,
    QueryResult,
    QuerySpec,
)


class _Catalog:
    key = "fake"
    report_types = [{"id": "Default", "settings": []}]
    has_report_types = True


class _DS:
    type = "fake"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "t"}


class Connector(ApiConnector):
    """Returns date-keyed rows; the metric value encodes the period so the two
    periods are distinguishable after alignment."""

    def __init__(self, accounts=None, fields=None):
        super().__init__(_DS())
        self._accounts = accounts or [Account("1", "One", currency="USD")]
        self._fields = fields or [
            Field("date", "Date", "dimension", data_type="date"),
            Field("sessions", "Sessions", "metric"),
        ]

    def list_accounts(self):
        return self._accounts

    def list_fields(self, report_type=None):
        return self._fields

    def _run(self, spec):
        # value = 100 for the base month (August), 80 for the prior (July).
        val = 100 if spec.date_range.start.startswith("2026-08") else 80
        return QueryResult(
            requested_field_ids=list(spec.fields),
            rows=[{"date": spec.date_range.start, "sessions": val}],
            row_count=1,
        )


@pytest.fixture(autouse=True)
def clear_cache():
    cache.clear()
    yield
    cache.clear()


def _spec(**kw):
    base = dict(
        accounts=["1"], fields=["date", "sessions"],
        date_range=DateRange("2026-08-01", "2026-08-31"),
        report_type="Default",
    )
    base.update(kw)
    return QuerySpec(**base)


class TestComparison:
    def test_comparison_attaches_delta_columns(self):
        conn = Connector()
        spec = _spec(compare=Compare("prev_range", show="perc_change"))
        out = run_query(conn, spec, org_id=1, permitted_accounts=["1"])
        row = out["rows"][0]
        assert row["sessions"] == 100
        assert row["sessions__compare"] == 80
        assert row["sessions__delta"] == pytest.approx(25.0)
        assert out["compare"]["type"] == "prev_range"

    def test_partial_period_is_flagged(self):
        conn = Connector()
        spec = _spec(
            date_range=DateRange("2026-08-01", "2026-08-31",
                                 inclusive_of_today=True),
            compare=Compare("prev_range"),
        )
        out = run_query(conn, spec, org_id=1, permitted_accounts=["1"])
        assert out["partial_period"] is True
        assert any("partial" in w.lower() for w in out["warnings"])


class TestAggregationNotes:
    def test_non_aggregatable_metric_is_called_out(self):
        conn = Connector(fields=[
            Field("date", "Date", "dimension", data_type="date"),
            Field("totalUsers", "Total users", "metric",
                  is_non_aggregatable=True),
        ])
        spec = _spec(fields=["date", "totalUsers"])
        out = run_query(conn, spec, org_id=1, permitted_accounts=["1"])
        assert any("must not be summed" in n for n in out["notes"])

    def test_plain_metric_produces_no_such_note(self):
        conn = Connector()
        out = run_query(conn, _spec(), org_id=1, permitted_accounts=["1"])
        assert not any("must not be summed" in n for n in out["notes"])


class TestCurrencyGuard:
    def test_mixed_currency_monetary_query_is_refused(self):
        conn = Connector(
            accounts=[
                Account("1", "US", currency="USD"),
                Account("2", "UK", currency="GBP"),
            ],
            fields=[Field("spend", "Spend", "metric", is_monetary=True)],
        )
        spec = _spec(accounts=["1", "2"], fields=["spend"])
        with pytest.raises(ApiError) as exc:
            run_query(conn, spec, org_id=1, permitted_accounts=["1", "2"])
        assert exc.value.code == ErrorCode.MIXED_CURRENCY

    def test_same_currency_is_fine(self):
        conn = Connector(
            accounts=[
                Account("1", "US", currency="USD"),
                Account("2", "US2", currency="USD"),
            ],
            fields=[Field("spend", "Spend", "metric", is_monetary=True)],
        )
        spec = _spec(accounts=["1", "2"], fields=["spend"])
        out = run_query(conn, spec, org_id=1, permitted_accounts=["1", "2"])
        assert out["success"] is True

    def test_non_monetary_mixed_currency_is_fine(self):
        # Sessions have no currency, so mixed-currency accounts are irrelevant.
        conn = Connector(accounts=[
            Account("1", "US", currency="USD"),
            Account("2", "UK", currency="GBP"),
        ])
        spec = _spec(accounts=["1", "2"], fields=["date", "sessions"])
        out = run_query(conn, spec, org_id=1, permitted_accounts=["1", "2"])
        assert out["success"] is True
