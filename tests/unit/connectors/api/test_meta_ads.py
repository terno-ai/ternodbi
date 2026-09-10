"""The Meta Ads connector, against mocked Graph API responses.

Exercises the two request shapes without a live provider or app approval: the
insights metric report (with breakdowns, level inference, daily split and money)
and the entity reports (campaigns/adsets/ads).
"""

import json
import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.sources.meta_ads import MetaAdsConnector
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec


class _Catalog:
    key = "meta_ads"
    report_types = [
        {"id": "Insights", "settings": []},
        {"id": "Campaigns", "settings": []},
    ]
    has_report_types = True


class _DS:
    type = "meta_ads"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok"}


ADACCOUNTS = {
    "data": [
        {"id": "act_111", "name": "Acme", "account_id": "111",
         "currency": "USD", "timezone_name": "America/New_York"},
        {"id": "act_222", "name": "Beta", "account_id": "222",
         "currency": "EUR", "timezone_name": "Europe/Berlin"},
    ],
}

INSIGHTS = {
    "data": [
        {"date_start": "2026-08-01", "date_stop": "2026-08-01",
         "campaign_name": "Brand", "impressions": "1000", "clicks": "40",
         "spend": "12.34"},
        {"date_start": "2026-08-02", "date_stop": "2026-08-02",
         "campaign_name": "Brand", "impressions": "800", "clicks": "25",
         "spend": "9.10"},
    ],
}

CAMPAIGNS = {
    "data": [
        {"id": "c1", "name": "Brand", "status": "ACTIVE", "objective": "SALES"},
        {"id": "c2", "name": "Promo", "status": "PAUSED", "objective": "REACH"},
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
    return MetaAdsConnector(_DS(), http=_mock_http(routes))


class TestListAccounts:
    def test_maps_ad_accounts_with_currency_and_tz(self):
        conn = _connector({("GET", "/me/adaccounts"): ADACCOUNTS})
        accounts = conn.list_accounts()
        assert {a.id for a in accounts} == {"act_111", "act_222"}
        assert accounts[0].currency == "USD"
        assert accounts[0].timezone == "America/New_York"


class TestListFields:
    def test_insights_vs_entity_catalogues(self):
        conn = _connector({})
        insights = {f.id for f in conn.list_fields("Insights")}
        campaigns = {f.id for f in conn.list_fields("Campaigns")}
        assert "spend" in insights and "age" in insights
        assert "objective" in campaigns
        assert "spend" not in campaigns

    def test_money_and_ratio_flags(self):
        conn = _connector({})
        by_id = {f.id: f for f in conn.list_fields("Insights")}
        assert by_id["spend"].is_monetary is True
        assert by_id["cpc"].is_non_aggregatable is True
        assert by_id["impressions"].is_non_aggregatable is False


class TestInsights:
    def _spec(self, fields, accounts=("act_111",)):
        return QuerySpec(
            accounts=list(accounts), fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="Insights",
        )

    def test_params_split_fields_breakdowns_and_date(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["url"] = url
            captured["params"] = body
            return INSIGHTS

        conn = MetaAdsConnector(_DS(), http=http)
        conn.query(self._spec(("date", "campaign_name", "age", "impressions", "spend")))
        p = captured["params"]
        # Metrics + entity name go in fields; age is a breakdown; date drives the split.
        assert set(p["fields"].split(",")) == {"impressions", "spend", "campaign_name"}
        assert p["breakdowns"] == "age"
        assert p["time_increment"] == 1
        assert p["level"] == "campaign"          # inferred from campaign_name
        assert json.loads(p["time_range"]) == {"since": "2026-08-01", "until": "2026-08-31"}
        assert "act_111/insights" in captured["url"]

    def test_level_defaults_to_account_without_entity_field(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"data": []}

        conn = MetaAdsConnector(_DS(), http=http)
        conn.query(self._spec(("impressions",)))
        assert captured["params"]["level"] == "account"

    def test_finest_level_wins(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"data": []}

        conn = MetaAdsConnector(_DS(), http=http)
        conn.query(self._spec(("campaign_name", "ad_name", "impressions")))
        assert captured["params"]["level"] == "ad"

    def test_parses_rows_with_date_remap_and_money(self):
        conn = _connector({
            ("GET", "/me/adaccounts"): ADACCOUNTS,
            ("GET", "act_111/insights"): INSIGHTS,
        })
        result = conn.query(self._spec(
            ("date", "campaign_name", "impressions", "spend")))
        assert result.row_count == 2
        row = result.rows[0]
        assert row["date"] == "2026-08-01"          # from date_start
        assert row["campaign_name"] == "Brand"
        assert row["impressions"] == 1000            # coerced to int
        assert row["spend"] == 12.34                 # money keeps decimals

    def test_unknown_field_is_rejected_with_a_suggestion(self):
        conn = _connector({("GET", "act_111/insights"): INSIGHTS})
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec(("impressionz",)))
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "impressions" in exc.value.message

    def test_bare_account_id_gets_act_prefix(self):
        seen = {}

        def http(method, url, token, body=None):
            seen["url"] = url
            return {"data": []}

        conn = MetaAdsConnector(_DS(), http=http)
        conn.query(self._spec(("impressions",), accounts=("111",)))
        assert "act_111/insights" in seen["url"]

    def test_multi_account_tags_rows(self):
        conn = _connector({
            ("GET", "/me/adaccounts"): ADACCOUNTS,
            ("GET", "/insights"): INSIGHTS,
        })
        result = conn.query(self._spec(
            ("date", "impressions"), accounts=("act_111", "act_222")))
        assert result.row_count == 4
        assert all("_account" in r for r in result.rows)


class TestEntities:
    def _spec(self, fields, report_type="Campaigns"):
        return QuerySpec(
            accounts=["act_111"], fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type=report_type,
        )

    def test_lists_campaigns_from_the_entity_endpoint(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["url"] = url
            captured["params"] = body
            return CAMPAIGNS

        conn = MetaAdsConnector(_DS(), http=http)
        result = conn.query(self._spec(("id", "name", "status")))
        assert "act_111/campaigns" in captured["url"]
        assert set(captured["params"]["fields"].split(",")) == {"id", "name", "status"}
        assert result.row_count == 2
        assert result.rows[0] == {"id": "c1", "name": "Brand", "status": "ACTIVE"}


class TestAuthMapping:
    def test_401_becomes_auth_expired(self):
        from terno_dbi.connectors.api.sources.meta_ads import _AuthError

        def http(method, url, token, body=None):
            raise _AuthError()

        conn = MetaAdsConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestRegistration:
    def test_meta_ads_is_registered_at_startup(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("meta_ads")

    def test_registered_factory_binds_a_token_refresher(self):
        from terno_dbi.connectors.api.sources.meta_ads import make_meta_ads_connector
        conn = make_meta_ads_connector(_DS())
        assert conn._token_refresher is not None
