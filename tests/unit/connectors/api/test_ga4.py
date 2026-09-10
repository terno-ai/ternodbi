"""The GA4 connector (Phase 4), against mocked Google API responses.

The mock returns the real response shapes from the GA4 Admin and Data APIs, so
the parsing and request-building are exercised without a live provider.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.sources.ga4 import GA4Connector
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec


class _Catalog:
    key = "googleanalytics4"
    report_types = [
        {"id": "Default", "settings": []},
        {"id": "CohortWeekly", "settings": []},
        {"id": "Realtime", "settings": []},
        {"id": "Funnel", "settings": [
            {"setting_id": "funnel_steps", "required": True, "label": "Funnel steps"},
        ]},
    ]
    has_report_types = True


class _DS:
    type = "googleanalytics4"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok"}


# --- canned Google responses ------------------------------------------------

ACCOUNT_SUMMARIES = {
    "accountSummaries": [{
        "displayName": "Acme Inc",
        "propertySummaries": [
            {"property": "properties/440705731", "displayName": "PyQuest"},
            {"property": "properties/111222333", "displayName": "Terno AI"},
        ],
    }],
}

METADATA = {
    "dimensions": [
        {"apiName": "date", "uiName": "Date", "category": "Time"},
        {"apiName": "country", "uiName": "Country", "category": "Geography"},
    ],
    "metrics": [
        {"apiName": "sessions", "uiName": "Sessions", "type": "TYPE_INTEGER",
         "category": "Session"},
        {"apiName": "totalUsers", "uiName": "Total users", "type": "TYPE_INTEGER",
         "category": "User"},
        {"apiName": "totalRevenue", "uiName": "Total revenue",
         "type": "TYPE_CURRENCY", "category": "Revenue"},
    ],
}

REPORT = {
    "rows": [
        {"dimensionValues": [{"value": "2026-08-01"}],
         "metricValues": [{"value": "31"}]},
        {"dimensionValues": [{"value": "2026-08-02"}],
         "metricValues": [{"value": "27"}]},
    ],
}


def _mock_http(routes):
    """Build an http callable dispatching on (method, url-substring)."""
    def http(method, url, token, body=None):
        assert token == "tok"
        for (m, needle), response in routes.items():
            if method == m and needle in url:
                return response
        raise AssertionError(f"unexpected call: {method} {url}")
    return http


def _connector(routes):
    return GA4Connector(_DS(), http=_mock_http(routes))


class TestListAccounts:
    def test_maps_properties_to_accounts(self):
        conn = _connector({("GET", "accountSummaries"): ACCOUNT_SUMMARIES})
        accounts = conn.list_accounts()
        assert {a.id for a in accounts} == {"440705731", "111222333"}
        assert accounts[0].name == "PyQuest"
        assert accounts[0].extra["account"] == "Acme Inc"


class TestListFields:
    def test_maps_dimensions_and_metrics(self):
        conn = _connector({
            ("GET", "accountSummaries"): ACCOUNT_SUMMARIES,
            ("GET", "/metadata"): METADATA,
        })
        by_id = {f.id: f for f in conn.list_fields()}
        assert by_id["date"].kind == "dimension"
        assert by_id["date"].data_type == "date"
        assert by_id["sessions"].kind == "metric"
        assert by_id["totalRevenue"].is_monetary is True

    def test_curated_non_aggregatable_flag(self):
        conn = _connector({
            ("GET", "accountSummaries"): ACCOUNT_SUMMARIES,
            ("GET", "/metadata"): METADATA,
        })
        by_id = {f.id: f for f in conn.list_fields()}
        # GA4 metadata never says this; we curate it.
        assert by_id["totalUsers"].is_non_aggregatable is True
        assert by_id["sessions"].is_non_aggregatable is False


class TestRunReport:
    def _spec(self, fields=("date", "sessions"), accounts=("440705731",)):
        return QuerySpec(
            accounts=list(accounts), fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="Default",
        )

    def test_parses_rows_by_field_id(self):
        conn = _connector({
            ("GET", "accountSummaries"): ACCOUNT_SUMMARIES,
            ("GET", "/metadata"): METADATA,
            ("POST", ":runReport"): REPORT,
        })
        result = conn.query(self._spec())
        assert result.row_count == 2
        # GA4's raw '20260801' is normalised to ISO on a date-typed dimension.
        assert result.rows[0] == {"date": "2026-08-01", "sessions": 31}
        assert result.requested_field_ids == ["date", "sessions"]

    def test_time_series_is_ordered_by_date(self):
        captured = {}

        def http(method, url, token, body=None):
            if "accountSummaries" in url:
                return ACCOUNT_SUMMARIES
            if "/metadata" in url:
                return METADATA
            captured["body"] = body
            return REPORT

        conn = GA4Connector(_DS(), http=http)
        conn.query(self._spec(fields=("date", "sessions")))
        # GA4 returns rows unsorted; we ask it to order by the date dimension.
        assert captured["body"]["orderBys"] == [
            {"dimension": {"dimensionName": "date"}}]

    def test_breakdown_without_date_orders_by_first_metric_desc(self):
        captured = {}

        def http(method, url, token, body=None):
            if "accountSummaries" in url:
                return ACCOUNT_SUMMARIES
            if "/metadata" in url:
                return METADATA
            captured["body"] = body
            return {"rows": []}

        conn = GA4Connector(_DS(), http=http)
        conn.query(self._spec(fields=("country", "sessions")))
        assert captured["body"]["orderBys"] == [
            {"metric": {"metricName": "sessions"}, "desc": True}]

    def test_date_dimension_is_normalised_to_iso(self):
        conn = _connector({
            ("GET", "accountSummaries"): ACCOUNT_SUMMARIES,
            ("GET", "/metadata"): METADATA,
            ("POST", ":runReport"): {
                "rows": [{"dimensionValues": [{"value": "20260819"}],
                          "metricValues": [{"value": "45"}]}],
            },
        })
        result = conn.query(self._spec())
        assert result.rows[0]["date"] == "2026-08-19"

    def test_unknown_field_is_rejected_with_a_suggestion(self):
        conn = _connector({
            ("GET", "accountSummaries"): ACCOUNT_SUMMARIES,
            ("GET", "/metadata"): METADATA,
        })
        spec = self._spec(fields=("date", "sessionz"))
        with pytest.raises(ApiError) as exc:
            conn.query(spec)
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "sessions" in exc.value.message      # near-match suggested

    def test_multi_account_merges_and_tags_rows(self):
        conn = _connector({
            ("GET", "accountSummaries"): ACCOUNT_SUMMARIES,
            ("GET", "/metadata"): METADATA,
            ("POST", ":runReport"): REPORT,
        })
        result = conn.query(self._spec(accounts=("440705731", "111222333")))
        # Two accounts x two rows, each tagged with its account.
        assert result.row_count == 4
        assert all("_account" in r for r in result.rows)

    def test_property_id_accepts_either_form(self):
        seen = {}

        def http(method, url, token, body=None):
            if "accountSummaries" in url:
                return ACCOUNT_SUMMARIES
            if "/metadata" in url:
                return METADATA
            seen["url"] = url
            return REPORT

        conn = GA4Connector(_DS(), http=http)
        conn.query(self._spec(accounts=("properties/440705731",)))
        assert "properties/440705731:runReport" in seen["url"]


class TestAuthMapping:
    def test_401_becomes_auth_expired(self):
        from terno_dbi.connectors.api.sources.ga4 import _AuthError

        def http(method, url, token, body=None):
            raise _AuthError()

        conn = GA4Connector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestTokenRefreshOnProviderCall:
    """A real provider call must refresh the token (via access_token), so a
    stale-since-connect token self-heals on any call — discovery or query."""

    def test_list_accounts_refreshes_the_token(self):
        calls = []

        def http(method, url, token, body=None):
            return ACCOUNT_SUMMARIES

        conn = GA4Connector(
            _DS(), http=http, token_refresher=lambda: calls.append(1),
        )
        conn.list_accounts()
        assert calls == [1]        # the provider call went through access_token

    def test_run_report_refreshes_the_token(self):
        calls = []

        def http(method, url, token, body=None):
            if "accountSummaries" in url:
                return ACCOUNT_SUMMARIES
            if "/metadata" in url:
                return METADATA
            return REPORT

        conn = GA4Connector(
            _DS(), http=http, token_refresher=lambda: calls.append(1),
        )
        conn.query(QuerySpec(
            accounts=["440705731"], fields=["date", "sessions"],
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="Default",
        ))
        assert calls   # refreshed at least once across the calls it made


class TestRegistration:
    def test_ga4_is_registered_at_startup(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("googleanalytics4")

    def test_registered_factory_binds_a_token_refresher(self):
        # The factory must wire refresh in, or the invariant above never fires
        # in production.
        from terno_dbi.connectors.api.sources.ga4 import make_ga4_connector

        class _Cat:
            key = "googleanalytics4"
            report_types = []
            has_report_types = False

        class _D:
            type = "googleanalytics4"
            catalog = _Cat()
            connection_json = {"ACCESS_TOKEN": "t"}

        conn = make_ga4_connector(_D())
        assert conn._token_refresher is not None


REALTIME_REPORT = {
    "rows": [
        {"dimensionValues": [{"value": "US"}], "metricValues": [{"value": "12"}]},
        {"dimensionValues": [{"value": "IN"}], "metricValues": [{"value": "7"}]},
    ],
}

FUNNEL_REPORT = {
    "funnelTable": {
        "dimensionHeaders": [{"name": "funnelStepName"}],
        "metricHeaders": [{"name": "activeUsers"}, {"name": "completionRate"}],
        "rows": [
            {"dimensionValues": [{"value": "View"}],
             "metricValues": [{"value": "1000"}, {"value": "1"}]},
            {"dimensionValues": [{"value": "Purchase"}],
             "metricValues": [{"value": "150"}, {"value": "0.15"}]},
        ],
    },
}


class TestRealtimeReport:
    def _spec(self, fields=("country", "activeUsers"), accounts=("440705731",)):
        from terno_dbi.connectors.api.model.types import DateRange
        return QuerySpec(
            accounts=list(accounts), fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-01"),
            report_type="Realtime",
        )

    def test_uses_the_curated_realtime_catalogue(self):
        conn = GA4Connector(_DS())
        ids = {f.id for f in conn.list_fields("Realtime")}
        assert "minutesAgo" in ids
        assert "activeUsers" in ids
        # Standard-report-only fields must not leak in.
        assert "sessions" not in ids

    def test_calls_run_realtime_report_endpoint(self):
        seen = {}

        def http(method, url, token, body=None):
            seen["url"] = url
            seen["body"] = body
            return REALTIME_REPORT

        conn = GA4Connector(_DS(), http=http)
        result = conn.query(self._spec())
        assert ":runRealtimeReport" in seen["url"]
        assert "dateRanges" not in seen["body"]     # realtime has no date range
        assert result.row_count == 2
        assert result.rows[0] == {"country": "US", "activeUsers": 12}

    def test_unknown_realtime_field_is_rejected(self):
        conn = GA4Connector(_DS(), http=lambda *a, **k: REALTIME_REPORT)
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec(fields=("sessions",)))
        assert exc.value.code == ErrorCode.INVALID_FIELD

    def test_no_metric_defaults_to_active_users(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["body"] = body
            return REALTIME_REPORT

        conn = GA4Connector(_DS(), http=http)
        conn.query(self._spec(fields=("country",)))
        assert captured["body"]["metrics"] == [{"name": "activeUsers"}]


class TestFunnelReport:
    def _spec(self, steps, accounts=("440705731",)):
        from terno_dbi.connectors.api.model.types import DateRange
        return QuerySpec(
            accounts=list(accounts), fields=[],
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="Funnel",
            settings={"funnel_steps": steps},
        )

    def test_calls_the_v1alpha_funnel_endpoint_with_event_filters(self):
        seen = {}

        def http(method, url, token, body=None):
            seen["url"] = url
            seen["body"] = body
            return FUNNEL_REPORT

        conn = GA4Connector(_DS(), http=http)
        steps = [
            {"name": "View", "event_name": "page_view"},
            {"name": "Purchase", "event_name": "purchase"},
        ]
        conn.query(self._spec(steps))
        assert "v1alpha" in seen["url"] and ":runFunnelReport" in seen["url"]
        step_bodies = seen["body"]["funnel"]["steps"]
        assert step_bodies[0]["name"] == "View"
        assert (step_bodies[0]["filterExpression"]["funnelEventFilter"]["eventName"]
                == "page_view")
        assert step_bodies[1]["name"] == "Purchase"

    def test_parses_rows_from_response_headers(self):
        conn = GA4Connector(_DS(), http=lambda *a, **k: FUNNEL_REPORT)
        steps = [
            {"name": "View", "event_name": "page_view"},
            {"name": "Purchase", "event_name": "purchase"},
        ]
        result = conn.query(self._spec(steps))
        assert result.row_count == 2
        assert result.rows[0] == {
            "funnelStepName": "View", "activeUsers": 1000, "completionRate": 1,
        }
        assert result.requested_field_ids == [
            "funnelStepName", "activeUsers", "completionRate"]

    def test_missing_funnel_steps_is_rejected(self):
        # Caught generically by settings validation (before the connector
        # runs), since the catalog declares funnel_steps as required.
        conn = GA4Connector(_DS(), http=lambda *a, **k: FUNNEL_REPORT)
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec(None))
        assert exc.value.code == ErrorCode.MISSING_SETTING

    def test_single_step_is_rejected(self):
        conn = GA4Connector(_DS(), http=lambda *a, **k: FUNNEL_REPORT)
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec([{"name": "View", "event_name": "page_view"}]))
        assert exc.value.code == ErrorCode.INVALID_SETTING

    def test_step_missing_event_name_is_rejected(self):
        conn = GA4Connector(_DS(), http=lambda *a, **k: FUNNEL_REPORT)
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec([{"name": "View"}, {"name": "Buy"}]))
        assert exc.value.code == ErrorCode.INVALID_SETTING


class TestPerPropertyMetadata:
    """Metadata is fetched and cached per property, so custom dimensions defined
    on one property are discoverable and valid there — and only there."""

    A, B = "440705731", "111222333"

    def _http(self):
        meta_a = {
            "dimensions": [
                {"apiName": "date", "uiName": "Date", "category": "Time"},
                {"apiName": "customEvent:foo", "uiName": "Foo", "category": "Custom"},
            ],
            "metrics": [
                {"apiName": "sessions", "uiName": "Sessions",
                 "type": "TYPE_INTEGER", "category": "Session"},
            ],
        }
        meta_b = {
            "dimensions": [
                {"apiName": "date", "uiName": "Date", "category": "Time"},
                {"apiName": "customEvent:bar", "uiName": "Bar", "category": "Custom"},
            ],
            "metrics": [
                {"apiName": "sessions", "uiName": "Sessions",
                 "type": "TYPE_INTEGER", "category": "Session"},
            ],
        }

        def http(method, url, token, body=None):
            assert token == "tok"
            if "accountSummaries" in url:
                return ACCOUNT_SUMMARIES
            if "/metadata" in url:
                return meta_a if self.A in url else meta_b
            if ":runReport" in url:
                return REPORT
            raise AssertionError(f"unexpected call: {method} {url}")
        return http

    def _spec(self, account, fields):
        return QuerySpec(
            accounts=[account], fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="Default",
        )

    def test_list_fields_unions_custom_dimensions_across_properties(self):
        conn = GA4Connector(_DS(), http=self._http())
        ids = {f.id for f in conn.list_fields()}
        # Every property's custom dimensions are surfaced, not just the first's.
        assert "customEvent:foo" in ids
        assert "customEvent:bar" in ids

    def test_custom_dimension_valid_on_its_own_property(self):
        conn = GA4Connector(_DS(), http=self._http())
        result = conn.query(self._spec(self.A, ["customEvent:foo", "sessions"]))
        assert result.row_count == 2      # accepted — no false rejection

    def test_custom_dimension_rejected_on_a_property_that_lacks_it(self):
        conn = GA4Connector(_DS(), http=self._http())
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec(self.B, ["customEvent:foo", "sessions"]))
        assert exc.value.code == ErrorCode.INVALID_FIELD
