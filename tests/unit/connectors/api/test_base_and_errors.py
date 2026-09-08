"""The ApiConnector base class, the error taxonomy, and get_today.

The `FakeGA4` below doubles as the reference for how a real connector extends the
base: implement list_accounts / list_fields / _run, and everything shared —
settings validation, token access — comes from the base.
"""

import pytest

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.dates import get_today
from terno_dbi.connectors.api.model.errors import (
    ApiError,
    ErrorCode,
    invalid_field,
    missing_setting,
)
from terno_dbi.connectors.api.model.types import (
    Account,
    DateRange,
    Field,
    QueryResult,
    QuerySpec,
)


# --------------------------------------------------------------------------
# A reference connector: this is the whole surface a real source implements.
# --------------------------------------------------------------------------


class _Catalog:
    key = "fake_ga4"
    report_types = [{"id": "Default", "settings": []}]
    has_report_types = True


class _DataSource:
    def __init__(self, connection_json):
        self.type = "fake_ga4"
        self.catalog = _Catalog()
        self.connection_json = connection_json


class FakeGA4(ApiConnector):
    def list_accounts(self):
        return [Account(id="440705731", name="PyQuest", currency="USD")]

    def list_fields(self, report_type=None):
        return [
            Field("date", "Date", "dimension", data_type="date"),
            Field("sessions", "Sessions", "metric", data_type="number"),
            Field("totalUsers", "Total users", "metric",
                  is_non_aggregatable=True),
        ]

    def _run(self, spec: QuerySpec) -> QueryResult:
        return QueryResult(
            requested_field_ids=list(spec.fields),
            rows=[{"date": "2026-08-27", "sessions": 31}],
            row_count=1,
        )


@pytest.fixture
def connector():
    return FakeGA4(_DataSource({"ACCESS_TOKEN": "tok-123"}))


class TestContract:
    def test_key_comes_from_catalog(self, connector):
        assert connector.key == "fake_ga4"

    def test_list_accounts(self, connector):
        acc = connector.list_accounts()[0]
        assert acc.id == "440705731"
        assert acc.as_dict()["currency"] == "USD"

    def test_non_aggregatable_flag_is_carried(self, connector):
        by_id = {f.id: f for f in connector.list_fields()}
        assert by_id["totalUsers"].is_non_aggregatable is True
        assert by_id["totalUsers"].as_dict()["is_non_aggregatable"] is True
        # A plain metric does not carry the flag at all.
        assert "is_non_aggregatable" not in by_id["sessions"].as_dict()

    def test_query_validates_then_runs(self, connector):
        spec = QuerySpec(
            accounts=["440705731"],
            fields=["date", "sessions"],
            date_range=DateRange("2026-08-01", "2026-08-27"),
            report_type="Default",
        )
        result = connector.query(spec)
        assert result.row_count == 1
        assert result.requested_field_ids == ["date", "sessions"]

    def test_query_rejects_bad_report_type_before_running(self):
        # Validation happens in the base's query(), so _run is never reached.
        conn = FakeGA4(_DataSource({"ACCESS_TOKEN": "t"}))
        spec = QuerySpec(
            accounts=["1"], fields=["sessions"],
            date_range=DateRange("2026-08-01", "2026-08-27"),
            report_type="NoSuchReport",
        )
        with pytest.raises(ApiError) as exc:
            conn.query(spec)
        assert exc.value.code == ErrorCode.INVALID_REPORT_TYPE


class TestTokenAccess:
    def test_reads_access_token(self, connector):
        assert connector.access_token() == "tok-123"

    def test_lowercase_access_token_is_accepted(self):
        conn = FakeGA4(_DataSource({"access_token": "lower"}))
        assert conn.access_token() == "lower"

    def test_missing_credentials_is_auth_expired(self):
        conn = FakeGA4(_DataSource(None))
        with pytest.raises(ApiError) as exc:
            conn.access_token()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED

    def test_legacy_string_credentials_are_rejected(self):
        # API sources always store an encrypted dict envelope; a raw string is
        # not a valid credential bundle, so it reads as "reconnect", not a crash.
        conn = FakeGA4(_DataSource('{"ACCESS_TOKEN": "from-string"}'))
        with pytest.raises(ApiError) as exc:
            conn.access_token()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestTokenFreshness:
    """The connector's invariant: every provider call reads a fresh token.

    Token refresh lives here — in `access_token()` — not at the call sites, so
    discovery, query and async jobs all get it without remembering to. These
    pin that the injected refresher runs on token access, and only there.
    """

    def test_access_token_runs_the_refresher_first(self):
        calls = []
        conn = FakeGA4(
            _DataSource({"ACCESS_TOKEN": "tok"}),
            token_refresher=lambda: calls.append(1),
        )
        assert conn.access_token() == "tok"
        assert calls == [1]

    def test_no_refresher_is_a_plain_read(self):
        conn = FakeGA4(_DataSource({"ACCESS_TOKEN": "tok"}))
        assert conn.access_token() == "tok"     # no refresher, no error

    def test_refresher_can_swap_in_a_new_token(self):
        # Simulate a refresh: the refresher rewrites the datasource credentials,
        # and access_token then returns the new value.
        ds = _DataSource({"ACCESS_TOKEN": "old"})

        def refresh():
            ds.connection_json = {"ACCESS_TOKEN": "new"}

        conn = FakeGA4(ds, token_refresher=refresh)
        assert conn.access_token() == "new"


class TestErrorTaxonomy:
    def test_payload_shape(self):
        err = ApiError(ErrorCode.RATE_LIMITED, "slow down",
                       retry_after_seconds=60)
        payload = err.to_payload()
        assert payload["success"] is False
        assert payload["error"]["code"] == "RATE_LIMITED"
        assert payload["error"]["retriable"] is True      # inferred
        assert payload["error"]["retry_after_seconds"] == 60

    def test_retriability_is_inferred_from_code(self):
        assert ApiError(ErrorCode.TIMEOUT, "x").retriable is True
        assert ApiError(ErrorCode.INVALID_FIELD, "x").retriable is False

    def test_invalid_field_suggests_near_matches(self):
        err = invalid_field("screenpageviews",
                            ["screenPageViews", "sessions", "totalUsers"])
        assert err.code == ErrorCode.INVALID_FIELD
        assert "screenPageViews" in err.message
        assert err.details["suggestions"] == ["screenPageViews"]

    def test_invalid_field_without_a_match_points_at_list_fields(self):
        err = invalid_field("zzz", ["sessions"])
        assert "list_fields" in err.message

    def test_missing_setting_names_the_setting(self):
        err = missing_setting("video_id", "Video ID", "VideoTotals")
        assert err.code == ErrorCode.MISSING_SETTING
        assert "Video ID" in err.message


class TestGetToday:
    def test_utc_shape(self):
        today = get_today()
        assert len(today["utc_date"]) == 10          # YYYY-MM-DD
        assert today["utc_datetime"].endswith("Z")

    def test_named_timezone_adds_local_fields(self):
        today = get_today("America/New_York")
        assert today["timezone"] == "America/New_York"
        assert "local_date" in today

    def test_unknown_timezone_degrades_to_utc(self):
        today = get_today("Not/AZone")
        assert "timezone_error" in today
        assert "utc_date" in today
