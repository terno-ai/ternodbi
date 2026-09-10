"""The Google Search Console connector, against mocked Search Console API v3
responses.

The mock returns the real response shapes from `sites.list` and
`searchAnalytics.query`, so parsing and request-building are exercised without a
live provider — the same approach as the GA4 tests.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.sources.gsc import GSCConnector
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec


class _Catalog:
    key = "google_search_console"
    report_types = [{"id": "SearchAnalytics", "settings": []}]
    has_report_types = True


class _DS:
    type = "google_search_console"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok"}


# --- canned Search Console responses ---------------------------------------

SITES = {
    "siteEntry": [
        {"siteUrl": "https://example.com/", "permissionLevel": "siteOwner"},
        {"siteUrl": "sc-domain:example.com", "permissionLevel": "siteFullUser"},
        # Visible but not queryable — must be filtered out.
        {"siteUrl": "https://other.com/", "permissionLevel": "siteUnverifiedUser"},
    ],
}

# Two rows keyed by [date, query], with all four metrics present as the API
# always returns them.
REPORT = {
    "rows": [
        {"keys": ["2026-08-01", "shoes"],
         "clicks": 10, "impressions": 100, "ctr": 0.1, "position": 3.2},
        {"keys": ["2026-08-02", "boots"],
         "clicks": 5, "impressions": 80, "ctr": 0.0625, "position": 7.5},
    ],
}


def _mock_http(routes):
    def http(method, url, token, body=None):
        assert token == "tok"
        for (m, needle), response in routes.items():
            if method == m and needle in url:
                return response
        raise AssertionError(f"unexpected call: {method} {url}")
    return http


def _connector(routes):
    return GSCConnector(_DS(), http=_mock_http(routes))


class TestListAccounts:
    def test_maps_verified_sites_and_filters_unverified(self):
        conn = _connector({("GET", "/sites"): SITES})
        accounts = conn.list_accounts()
        ids = {a.id for a in accounts}
        assert ids == {"https://example.com/", "sc-domain:example.com"}
        assert "https://other.com/" not in ids   # unverified dropped
        assert accounts[0].extra["permission_level"] == "siteOwner"


class TestListFields:
    def test_exposes_the_fixed_catalogue(self):
        conn = _connector({})
        by_id = {f.id: f for f in conn.list_fields()}
        assert by_id["date"].kind == "dimension"
        assert by_id["date"].data_type == "date"
        assert by_id["clicks"].kind == "metric"
        assert by_id["clicks"].data_type == "integer"
        # Ratios/averages must never be summed across rows.
        assert by_id["ctr"].is_non_aggregatable is True
        assert by_id["position"].is_non_aggregatable is True
        assert by_id["clicks"].is_non_aggregatable is False


class TestRunReport:
    def _spec(self, fields=("date", "query", "clicks"),
              accounts=("https://example.com/",)):
        return QuerySpec(
            accounts=list(accounts), fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="SearchAnalytics",
        )

    def test_parses_rows_by_field_id(self):
        conn = _connector({
            ("GET", "/sites"): SITES,
            ("POST", "searchAnalytics/query"): REPORT,
        })
        result = conn.query(self._spec())
        assert result.row_count == 2
        assert result.rows[0] == {"date": "2026-08-01", "query": "shoes", "clicks": 10}
        assert result.requested_field_ids == ["date", "query", "clicks"]

    def test_request_body_sends_only_dimensions(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["body"] = body
            return REPORT

        conn = GSCConnector(_DS(), http=http)
        conn.query(self._spec(fields=("date", "query", "clicks", "impressions")))
        # Metrics are not sent as dimensions; the API returns them implicitly.
        assert captured["body"]["dimensions"] == ["date", "query"]
        assert captured["body"]["startDate"] == "2026-08-01"
        assert captured["body"]["endDate"] == "2026-08-31"

    def test_no_metric_requested_returns_all_four(self):
        conn = _connector({
            ("GET", "/sites"): SITES,
            ("POST", "searchAnalytics/query"): REPORT,
        })
        result = conn.query(self._spec(fields=("date", "query")))
        # The caller named no metric, so all four are surfaced from the row.
        assert result.rows[0]["clicks"] == 10
        assert result.rows[0]["impressions"] == 100
        assert "ctr" in result.rows[0]
        assert "position" in result.rows[0]

    def test_siteurl_is_percent_encoded_in_the_path(self):
        seen = {}

        def http(method, url, token, body=None):
            if "/sites/" in url and "searchAnalytics" in url:
                seen["url"] = url
            return REPORT

        conn = GSCConnector(_DS(), http=http)
        conn.query(self._spec(accounts=("https://example.com/",)))
        # Slashes and colon must be encoded, or the API 404s.
        assert "https%3A%2F%2Fexample.com%2F" in seen["url"]

    def test_domain_property_is_percent_encoded(self):
        seen = {}

        def http(method, url, token, body=None):
            if "searchAnalytics" in url:
                seen["url"] = url
            return REPORT

        conn = GSCConnector(_DS(), http=http)
        conn.query(self._spec(accounts=("sc-domain:example.com",)))
        assert "sc-domain%3Aexample.com" in seen["url"]

    def test_unknown_field_is_rejected_with_a_suggestion(self):
        conn = _connector({("POST", "searchAnalytics/query"): REPORT})
        spec = self._spec(fields=("date", "clickz"))
        with pytest.raises(ApiError) as exc:
            conn.query(spec)
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "clicks" in exc.value.message    # near-match suggested

    def test_multi_account_merges_and_tags_rows(self):
        conn = _connector({
            ("GET", "/sites"): SITES,
            ("POST", "searchAnalytics/query"): REPORT,
        })
        result = conn.query(self._spec(
            accounts=("https://example.com/", "sc-domain:example.com")))
        assert result.row_count == 4
        assert all("_account" in r for r in result.rows)


class TestAuthMapping:
    def test_401_becomes_auth_expired(self):
        from terno_dbi.connectors.api.sources.gsc import _AuthError

        def http(method, url, token, body=None):
            raise _AuthError()

        conn = GSCConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestTokenRefreshOnProviderCall:
    def test_list_accounts_refreshes_the_token(self):
        calls = []
        conn = GSCConnector(
            _DS(), http=lambda *a, **k: SITES,
            token_refresher=lambda: calls.append(1),
        )
        conn.list_accounts()
        assert calls == [1]


class TestRegistration:
    def test_gsc_is_registered_at_startup(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("google_search_console")

    def test_registered_factory_binds_a_token_refresher(self):
        from terno_dbi.connectors.api.sources.gsc import make_gsc_connector
        conn = make_gsc_connector(_DS())
        assert conn._token_refresher is not None
