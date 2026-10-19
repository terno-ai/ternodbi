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
        {"id": "ActionTypes", "settings": []},
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


class TestConversionMetrics:
    def _spec(self, fields):
        return QuerySpec(
            accounts=["act_111"], fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="Insights",
        )

    def test_action_metrics_request_backing_arrays_not_derived_ids(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"data": []}

        conn = MetaAdsConnector(_DS(), http=http)
        conn.query(self._spec(
            ("spend", "purchases", "purchase_value", "purchase_roas", "leads")))
        fields = set(captured["params"]["fields"].split(","))
        # Flat metric stays; derived ids are replaced by their backing arrays, deduped.
        assert "spend" in fields
        assert {"actions", "action_values", "purchase_roas"} <= fields
        assert "purchases" not in fields and "leads" not in fields

    def test_parses_action_values_with_omni_fallback_and_roas(self):
        row = {"data": [{
            "spend": "100",
            "actions": [
                {"action_type": "omni_purchase", "value": "7"},
                {"action_type": "lead", "value": "3"},
            ],
            "action_values": [{"action_type": "purchase", "value": "250.5"}],
            "purchase_roas": [{"action_type": "omni_purchase", "value": "2.5"}],
        }]}
        conn = MetaAdsConnector(_DS(), http=_mock_http({
            ("GET", "act_111/insights"): row}))
        result = conn.query(self._spec(
            ("spend", "purchases", "leads", "purchase_value", "purchase_roas")))
        r = result.rows[0]
        assert r["purchases"] == 7            # omni_purchase
        assert r["leads"] == 3
        assert r["purchase_value"] == 250.5   # falls back to bare "purchase"
        assert r["purchase_roas"] == 2.5

    def test_missing_action_type_yields_none(self):
        row = {"data": [{"spend": "10", "actions": [
            {"action_type": "landing_page_view", "value": "5"}]}]}
        conn = MetaAdsConnector(_DS(), http=_mock_http({
            ("GET", "act_111/insights"): row}))
        r = conn.query(self._spec(("purchases", "landing_page_views"))).rows[0]
        assert r["landing_page_views"] == 5
        assert r["purchases"] is None

    def test_meta_error_message_prefers_user_message(self):
        from terno_dbi.connectors.api.sources.meta_ads import _meta_error_message

        class _Resp:
            reason = "Bad Request"
            def __init__(self, body): self._body = body
            def json(self): return self._body

        assert _meta_error_message(_Resp(
            {"error": {"error_user_msg": "Pick a valid date range",
                       "message": "generic"}})) == "Pick a valid date range"
        assert _meta_error_message(_Resp(
            {"error": {"message": "Unsupported breakdown"}})) == "Unsupported breakdown"

        class _Broken:
            reason = "Bad Request"
            def json(self): raise ValueError("not json")
        assert _meta_error_message(_Broken()) == "Bad Request"

    def test_omni_wins_over_variants_no_double_count(self):
        # Both omni_purchase and web_in_store_purchase present with the same 67;
        # first-match must take omni and report 67, not 134.
        row = {"data": [{"actions": [
            {"action_type": "web_in_store_purchase", "value": "67"},
            {"action_type": "omni_purchase", "value": "67"},
        ]}]}
        conn = MetaAdsConnector(_DS(), http=_mock_http({
            ("GET", "act_111/insights"): row}))
        assert conn.query(self._spec(("purchases",))).rows[0]["purchases"] == 67

    def test_variant_resolves_when_omni_absent(self):
        # Account lacks omni_purchase -> falls back to web_in_store_purchase.
        row = {"data": [{"actions": [
            {"action_type": "web_in_store_purchase", "value": "42"}]}]}
        conn = MetaAdsConnector(_DS(), http=_mock_http({
            ("GET", "act_111/insights"): row}))
        assert conn.query(self._spec(("purchases",))).rows[0]["purchases"] == 42

    def test_action_metric_dict_and_field_ids_match(self):
        from terno_dbi.connectors.api.sources.meta_ads import (
            _ACTION_METRICS, _ACTION_METRIC_FIELDS)
        assert set(_ACTION_METRICS) == {f.id for f in _ACTION_METRIC_FIELDS}

    def test_cost_per_result_and_video_and_outbound_srcs(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"data": []}

        conn = MetaAdsConnector(_DS(), http=http)
        conn.query(self._spec((
            "cost_per_purchase", "cost_per_lead", "thruplays", "video_p75",
            "outbound_clicks", "cost_per_outbound_click")))
        fields = set(captured["params"]["fields"].split(","))
        assert "cost_per_action_type" in fields          # shared by both cost metrics
        assert {"video_thruplay_watched_actions", "video_p75_watched_actions",
                "outbound_clicks", "cost_per_outbound_click"} <= fields
        # derived ids never sent to Meta
        assert not ({"cost_per_purchase", "thruplays"} & fields)

    def test_parses_cost_per_result_and_thruplays(self):
        row = {"data": [{
            "cost_per_action_type": [
                {"action_type": "omni_purchase", "value": "4.25"},
                {"action_type": "lead", "value": "1.10"}],
            "video_thruplay_watched_actions": [
                {"action_type": "video_view", "value": "42"}],
        }]}
        conn = MetaAdsConnector(_DS(), http=_mock_http({
            ("GET", "act_111/insights"): row}))
        r = conn.query(self._spec(
            ("cost_per_purchase", "cost_per_lead", "thruplays"))).rows[0]
        assert r["cost_per_purchase"] == 4.25     # monetary, decimals kept
        assert r["cost_per_lead"] == 1.10
        assert r["thruplays"] == 42                # count, int


class TestBreakdownsAndFields:
    def _spec(self, fields):
        return QuerySpec(
            accounts=["act_111"], fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="Insights",
        )

    def test_hour_breakdown_maps_to_api_name_and_parses(self):
        captured = {}
        api_key = "hourly_stats_aggregated_by_advertiser_time_zone"

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"data": [{api_key: "06:00:00 - 06:59:59", "impressions": "12"}]}

        conn = MetaAdsConnector(_DS(), http=http)
        r = conn.query(self._spec(("hour", "impressions"))).rows[0]
        assert captured["params"]["breakdowns"] == api_key
        assert r["hour"] == "06:00:00 - 06:59:59"
        assert r["impressions"] == 12

    def test_delivery_breakdowns_pass_through(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"data": []}

        conn = MetaAdsConnector(_DS(), http=http)
        conn.query(self._spec(("platform_position", "device_platform", "region", "spend")))
        assert set(captured["params"]["breakdowns"].split(",")) == {
            "platform_position", "device_platform", "region"}

    def test_link_click_metrics_are_flat_fields(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"data": []}

        conn = MetaAdsConnector(_DS(), http=http)
        conn.query(self._spec(
            ("inline_link_clicks", "inline_link_click_ctr", "cost_per_inline_link_click")))
        assert set(captured["params"]["fields"].split(",")) == {
            "inline_link_clicks", "inline_link_click_ctr", "cost_per_inline_link_click"}

    def test_entity_catalogue_exposes_budget_and_status_fields(self):
        conn = _connector({})
        campaigns = {f.id for f in conn.list_fields("Campaigns")}
        adsets = {f.id for f in conn.list_fields("AdSets")}
        assert {"effective_status", "objective", "daily_budget",
                "bid_strategy", "start_time"} <= campaigns
        assert {"optimization_goal", "bid_amount", "billing_event"} <= adsets
        budget = next(f for f in conn.list_fields("Campaigns")
                      if f.id == "daily_budget")
        assert budget.is_monetary is True

    def test_audience_timezone_hour_breakdown_maps(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"data": []}

        conn = MetaAdsConnector(_DS(), http=http)
        conn.query(self._spec(("hour_audience", "impressions")))
        assert captured["params"]["breakdowns"] == \
            "hourly_stats_aggregated_by_audience_time_zone"

    def test_impression_device_and_dma_pass_through(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"data": []}

        conn = MetaAdsConnector(_DS(), http=http)
        conn.query(self._spec(("impression_device", "dma", "spend")))
        assert set(captured["params"]["breakdowns"].split(",")) == {
            "impression_device", "dma"}

    def test_insights_requests_unified_attribution(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"data": []}

        conn = MetaAdsConnector(_DS(), http=http)
        conn.query(self._spec(("spend",)))
        assert captured["params"]["use_unified_attribution_setting"] == "true"

    def test_video_metric_with_hourly_breakdown_is_rejected(self):
        conn = _connector({})
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec(("hour", "thruplays")))
        assert exc.value.code == ErrorCode.INVALID_FILTER
        assert "hour" in exc.value.message

    def test_video_quartile_with_region_is_rejected(self):
        conn = _connector({})
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec(("region", "video_p50")))
        assert exc.value.code == ErrorCode.INVALID_FILTER

    def test_thruplays_with_dma_is_rejected(self):
        conn = _connector({})
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec(("dma", "thruplays")))
        assert exc.value.code == ErrorCode.INVALID_FILTER

    def test_reach_with_hourly_is_rejected(self):
        conn = _connector({})
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec(("hour", "reach")))
        assert exc.value.code == ErrorCode.INVALID_FILTER

    def test_video_views_action_metric_allowed_under_hourly(self):
        # video_views is backed by `actions`, not a video_* field -> allowed.
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"data": []}

        conn = MetaAdsConnector(_DS(), http=http)
        conn.query(self._spec(("hour", "video_views")))   # must not raise
        assert "hourly_stats_aggregated_by_advertiser_time_zone" in \
            captured["params"]["breakdowns"]

    def test_quality_rankings_and_unique_metrics_are_flat(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"data": [{"quality_ranking": "ABOVE_AVERAGE",
                              "unique_clicks": "9", "cpp": "3.5"}]}

        conn = MetaAdsConnector(_DS(), http=http)
        r = conn.query(self._spec(
            ("quality_ranking", "unique_clicks", "cpp"))).rows[0]
        assert set(captured["params"]["fields"].split(",")) == {
            "quality_ranking", "unique_clicks", "cpp"}
        assert r["quality_ranking"] == "ABOVE_AVERAGE"   # categorical string kept
        assert r["unique_clicks"] == 9
        assert r["cpp"] == 3.5


class TestEntityBudgets:
    def _spec(self, fields):
        return QuerySpec(
            accounts=["act_111"], fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="Campaigns",
        )

    def _http(self, currency, campaigns):
        def http(method, url, token, body=None):
            if body and body.get("fields") == "currency":
                if currency is None:
                    raise AssertionError("no currency")   # simulate lookup failure
                return {"currency": currency}
            if "/campaigns" in url:
                return {"data": campaigns}
            raise AssertionError(f"unexpected: {url}")
        return http

    def test_budget_converted_to_major_units_by_currency(self):
        conn = MetaAdsConnector(_DS(), http=self._http(
            "USD", [{"name": "Brand", "daily_budget": "50000"}]))
        row = conn.query(self._spec(("name", "daily_budget"))).rows[0]
        assert row["daily_budget"] == 500.0                # 50000 cents /100

    def test_zero_decimal_currency_not_divided(self):
        conn = MetaAdsConnector(_DS(), http=self._http(
            "JPY", [{"name": "JP", "daily_budget": "50000"}]))
        row = conn.query(self._spec(("name", "daily_budget"))).rows[0]
        assert row["daily_budget"] == 50000.0              # offset 1

    def test_currency_lookup_skipped_when_no_money_field(self):
        # currency http branch would raise if hit; it must not be called here.
        def http(method, url, token, body=None):
            assert not (body and body.get("fields") == "currency")
            return {"data": [{"name": "Brand", "status": "ACTIVE"}]}
        conn = MetaAdsConnector(_DS(), http=http)
        row = conn.query(self._spec(("name", "status"))).rows[0]
        assert row["status"] == "ACTIVE"

    def test_currency_lookup_failure_falls_back_to_cents(self):
        conn = MetaAdsConnector(_DS(), http=self._http(
            None, [{"name": "Brand", "daily_budget": "12300"}]))
        row = conn.query(self._spec(("name", "daily_budget"))).rows[0]
        assert row["daily_budget"] == 123.0                # default offset 100


class TestActionTypesDiagnostic:
    def _spec(self, accounts=("act_111",)):
        return QuerySpec(
            accounts=list(accounts), fields=[],
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="ActionTypes",
        )

    def test_flattens_action_types_with_count_and_value(self):
        row = {"data": [{
            "actions": [
                {"action_type": "omni_purchase", "value": "7"},
                {"action_type": "offsite_conversion.fb_pixel_lead", "value": "3"},
            ],
            "action_values": [
                {"action_type": "omni_purchase", "value": "250.5"}],
        }]}
        conn = MetaAdsConnector(_DS(), http=_mock_http({
            ("GET", "act_111/insights"): row}))
        rows = conn.query(self._spec()).rows
        by_type = {r["action_type"]: r for r in rows}
        assert by_type["omni_purchase"]["count"] == 7
        assert by_type["omni_purchase"]["value"] == 250.5          # money kept
        assert by_type["offsite_conversion.fb_pixel_lead"]["count"] == 3
        assert by_type["offsite_conversion.fb_pixel_lead"]["value"] is None

    def test_requests_actions_arrays_at_account_level(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"data": []}

        conn = MetaAdsConnector(_DS(), http=http)
        conn.query(self._spec())
        assert captured["params"]["level"] == "account"
        assert set(captured["params"]["fields"].split(",")) == {
            "actions", "action_values"}

    def test_action_types_report_in_catalogue(self):
        conn = _connector({})
        ids = {f.id for f in conn.list_fields("ActionTypes")}
        assert ids == {"action_type", "count", "value"}


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
