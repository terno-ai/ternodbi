"""The HubSpot CRM connector, against mocked CRM API v3 responses.

The mock returns the real response shapes from token introspection and
`objects/{type}/search`, so parsing, date-filter building and paging are
exercised without a live provider — the same approach as the GSC tests.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.sources.hubspot import HubSpotConnector, _day_bounds_ms
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec


class _Catalog:
    key = "hubspot"
    report_types = [
        {"id": "Contacts", "settings": []},
        {"id": "Companies", "settings": []},
        {"id": "Deals", "settings": []},
    ]
    has_report_types = True


class _DS:
    type = "hubspot"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok"}


# --- canned HubSpot responses ----------------------------------------------

TOKEN_INFO = {"hub_id": 12345, "hub_domain": "acme.com", "scopes": []}

DEALS = {
    "results": [
        {"id": "1", "properties": {
            "dealname": "Big deal", "dealstage": "closedwon",
            "amount": "1000.5", "createdate": "2026-08-02T10:00:00Z"}},
        {"id": "2", "properties": {
            "dealname": "Small deal", "dealstage": "qualifiedtobuy",
            "amount": "250", "createdate": "2026-08-01T09:00:00Z"}},
    ],
    "paging": {},
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
    return HubSpotConnector(_DS(), http=_mock_http(routes))


class TestDayBounds:
    def test_inclusive_millisecond_window(self):
        start_ms, end_ms = _day_bounds_ms("2026-08-01", "2026-08-01")
        # 2026-08-01T00:00:00Z in epoch ms, to the last millisecond of the day.
        assert start_ms == 1785542400000
        assert end_ms == start_ms + 86400000 - 1


class TestListAccounts:
    def test_returns_the_single_portal(self):
        conn = _connector({("GET", "access-tokens"): TOKEN_INFO})
        accounts = conn.list_accounts()
        assert len(accounts) == 1
        assert accounts[0].id == "12345"
        assert accounts[0].name == "acme.com"


class TestListFields:
    def test_curated_catalogue_per_report(self):
        conn = _connector({})
        deal_fields = {f.id: f for f in conn.list_fields("Deals")}
        assert deal_fields["amount"].kind == "metric"
        assert deal_fields["amount"].is_monetary is True
        assert deal_fields["dealname"].kind == "dimension"
        # A different report exposes a different property set.
        contact_fields = {f.id: f for f in conn.list_fields("Contacts")}
        assert "email" in contact_fields
        assert "amount" not in contact_fields


class TestRunReport:
    def _spec(self, fields=("dealname", "amount"), report_type="Deals"):
        return QuerySpec(
            accounts=["12345"], fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type=report_type,
        )

    def test_parses_rows_and_coerces_money(self):
        conn = _connector({("POST", "objects/deals/search"): DEALS})
        result = conn.query(self._spec())
        assert result.row_count == 2
        assert result.rows[0] == {"dealname": "Big deal", "amount": 1000.5}
        assert result.requested_field_ids == ["dealname", "amount"]

    def test_date_filter_uses_millisecond_bounds(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["body"] = body
            return DEALS

        conn = HubSpotConnector(_DS(), http=http)
        conn.query(self._spec())
        flt = captured["body"]["filterGroups"][0]["filters"][0]
        assert flt["propertyName"] == "createdate"
        assert flt["operator"] == "BETWEEN"
        assert flt["value"] == _day_bounds_ms("2026-08-01", "2026-08-31")[0]
        assert flt["highValue"] == _day_bounds_ms("2026-08-01", "2026-08-31")[1]

    def test_unknown_field_is_rejected_with_a_suggestion(self):
        conn = _connector({("POST", "objects/deals/search"): DEALS})
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec(fields=("dealname", "amountt")))
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "amount" in exc.value.message

    def test_paging_follows_the_cursor_until_exhausted(self):
        page1 = {
            "results": [{"id": "1", "properties": {"dealname": "A"}}],
            "paging": {"next": {"after": "100"}},
        }
        page2 = {
            "results": [{"id": "2", "properties": {"dealname": "B"}}],
            "paging": {},
        }
        seq = [page1, page2]

        def http(method, url, token, body=None):
            return seq.pop(0)

        conn = HubSpotConnector(_DS(), http=http)
        result = conn.query(self._spec(fields=("dealname",)))
        assert [r["dealname"] for r in result.rows] == ["A", "B"]


class TestAuthMapping:
    def test_401_becomes_auth_expired(self):
        from terno_dbi.connectors.api.sources.hubspot import _AuthError

        def http(method, url, token, body=None):
            raise _AuthError()

        conn = HubSpotConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestRegistration:
    def test_hubspot_is_registered_at_startup(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("hubspot")

    def test_registered_factory_binds_a_token_refresher(self):
        from terno_dbi.connectors.api.sources.hubspot import make_hubspot_connector
        conn = make_hubspot_connector(_DS())
        assert conn._token_refresher is not None
