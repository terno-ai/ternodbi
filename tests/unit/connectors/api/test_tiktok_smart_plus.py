"""TikTok Upgraded Smart+ actions and reports, against mocked responses.

Every Smart+ action is driven by the official request spec in
`smart_plus_spec.SPEC` (from TikTok's API reference), so these tests
pin the engine's guarantees: only official fields go out, required ones are
enforced, the connector owns advertiser/request_id/paused-on-create, and
entities are read before they are written.
"""

import json
import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
from terno_dbi.connectors.api.sources.tiktok.ads import connector as tiktok_ads
from terno_dbi.connectors.api.sources.tiktok import _api as tt_api
from terno_dbi.connectors.api.sources.tiktok.ads import actions as tt_actions
from terno_dbi.connectors.api.sources.tiktok.ads import fields as tt_fields
from terno_dbi.connectors.api.sources.tiktok.ads import gmv_max_spec as tt_gmv
from terno_dbi.connectors.api.sources.tiktok.ads import listings as tt_listings
from terno_dbi.connectors.api.sources.tiktok.ads import reports as tt_reports
from terno_dbi.connectors.api.sources.tiktok.ads import smart_plus as tt_smart_plus
from terno_dbi.connectors.api.sources.tiktok.ads import smart_plus as sp
from terno_dbi.connectors.api.sources.tiktok.ads.connector import TikTokAdsConnector
from terno_dbi.connectors.api.sources.tiktok.ads.smart_plus_spec import (
    SMART_PLUS_REPORT_DIMENSIONS, SPEC,
)


class _Catalog:
    key = "tiktok_ads"
    from terno_dbi.catalog.declarations import get_spec as _get_spec
    report_types = [r.as_dict() for r in _get_spec("tiktok_ads").report_types]
    has_report_types = True


class _DS:
    type = "tiktok_ads"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok"}


def _ok(data):
    return {"code": 0, "message": "OK", "request_id": "r", "data": data}


class _Http:
    def __init__(self, reads=None, posts=None):
        self.reads = reads or {}
        self.posts_reply = posts or {}
        self.gets, self.posts = [], []

    def __call__(self, method, url, token, params=None, *, body=None,
                 files=None):
        path = url.split("/open_api/v1.3/", 1)[1]
        if method == "GET":
            self.gets.append((path, dict(params or {})))
            return _ok(self.reads.get(path, {"list": []}))
        self.posts.append((path, body))
        return _ok(self.posts_reply.get(path, {}))


_CAMPAIGN = {"campaign_id": "11", "campaign_name": "Sales+",
             "operation_status": "ENABLE", "budget": 100.0}
_ADGROUP = {"adgroup_id": "21", "adgroup_name": "SP group",
            "campaign_id": "11", "operation_status": "ENABLE"}
_AD = {"smart_plus_ad_id": "31", "ad_name": "SP ad", "adgroup_id": "21",
       "operation_status": "ENABLE"}
_READS = {"smart_plus/campaign/get/": {"list": [_CAMPAIGN]},
          "smart_plus/adgroup/get/": {"list": [_ADGROUP]},
          "smart_plus/ad/get/": {"list": [_AD]}}


def _conn(reads=_READS, posts=None):
    http = _Http(reads=reads, posts=posts)
    return TikTokAdsConnector(_DS(), http=http), http


_ADGROUP_PARAMS = {
    "campaign_id": "11", "adgroup_name": "g", "promotion_type": "WEBSITE",
    "optimization_goal": "CONVERT", "bid_type": "BID_TYPE_NO_BID",
    "billing_event": "OCPM", "schedule_type": "SCHEDULE_FROM_NOW",
    "schedule_start_time": "2026-12-01 00:00:00",
    "targeting_spec": {"location_ids": ["6252001"], "age_groups": ["AGE_25_34"]},
    "pixel_id": "61", "optimization_event": "ON_WEB_ORDER",
}


def _walk(fields, prefix=""):
    for name, spec in fields.items():
        yield prefix + name
        yield from _walk(spec.get("f") or {}, prefix + name + ".")


class TestRegistry:
    def test_every_spec_action_targets_an_official_endpoint(self):
        for action in sp.SPEC_ACTIONS:
            assert action.path in SPEC, action.id
            assert SPEC[action.path]["m"] == "POST"

    def test_listed_with_schemas_built_from_the_official_fields(self):
        conn, _ = _conn()
        actions = {a.id: a for a in conn.list_actions()}
        for action in sp.SPEC_ACTIONS:
            schema = actions[action.id].schema
            official = set(SPEC[action.path]["f"])
            hidden = {"advertiser_id"} | {k for k, _ in action.forced}
            assert set(schema["properties"]) == official - hidden
            assert schema["additionalProperties"] is False

    def test_nested_schema_follows_the_spec(self):
        conn, _ = _conn()
        schema = {a.id: a for a in conn.list_actions()}[
            "smart_plus_create_ad_group"].schema
        targeting = schema["properties"]["targeting_spec"]
        assert set(targeting["properties"]) == set(
            SPEC["smart_plus/adgroup/create/"]["f"]["targeting_spec"]["f"])
        assert "targeting_spec" in schema["required"]
        assert "request_id" not in schema["required"]   # generated

    def test_spec_names_are_identifiers(self):
        import re
        for path, endpoint in SPEC.items():
            for name in _walk(endpoint["f"]):
                assert re.fullmatch(r"[a-z][a-z0-9_.]*", name), (path, name)


class TestCreate:
    def test_campaign_is_created_paused_with_a_request_id(self):
        conn, http = _conn(posts={"smart_plus/campaign/create/": {
            "campaign_id": "77"}})
        result = conn.execute_action("smart_plus_create_campaign", "7001", {
            "campaign_name": "Sales+", "objective_type": "WEB_CONVERSIONS",
            "sales_destination": "WEBSITE", "budget_mode":
            "BUDGET_MODE_DYNAMIC_DAILY_BUDGET", "budget": 50})
        path, body = http.posts[0]
        assert path == "smart_plus/campaign/create/"
        assert body["operation_status"] == "DISABLE"
        assert body["advertiser_id"] == "7001"
        assert body["request_id"].isdigit()
        assert result.details == {"campaign_id": "77"}
        assert "paused" in result.summary

    def test_a_retry_can_reuse_its_request_id(self):
        conn, http = _conn()
        conn.execute_action("smart_plus_create_campaign", "7001", {
            "campaign_name": "x", "objective_type": "LEAD_GENERATION",
            "request_id": "123456789"})
        assert http.posts[0][1]["request_id"] == "123456789"

    def test_the_caller_cannot_create_it_enabled(self):
        conn, http = _conn()
        with pytest.raises(ApiError) as exc:
            conn.execute_action("smart_plus_create_campaign", "7001", {
                "campaign_name": "x", "objective_type": "LEAD_GENERATION",
                "operation_status": "ENABLE"})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert http.posts == []

    def test_ad_group_reads_its_campaign_then_creates_paused(self):
        conn, http = _conn(posts={"smart_plus/adgroup/create/": {
            "adgroup_id": "88"}})
        result = conn.execute_action("smart_plus_create_ad_group", "7001",
                                     dict(_ADGROUP_PARAMS))
        assert http.gets[0][0] == "smart_plus/campaign/get/"
        body = http.posts[0][1]
        assert body["operation_status"] == "DISABLE"
        assert body["targeting_spec"]["location_ids"] == ["6252001"]
        assert result.details == {"adgroup_id": "88"}

    def test_ad_is_created_paused(self):
        conn, http = _conn(posts={"smart_plus/ad/create/": {
            "smart_plus_ad_id": "99"}})
        conn.execute_action("smart_plus_create_ad", "7001", {
            "adgroup_id": "21", "ad_name": "a",
            "creative_list": [{"creative_info": {
                "ad_format": "SINGLE_VIDEO",
                "video_info": {"video_id": "v1"},
                "identity_type": "CUSTOMIZED_USER", "identity_id": "id1"}}],
            "ad_text_list": [{"ad_text": "Try it"}],
            "call_to_action_list": [{"call_to_action": "SHOP_NOW"}],
            "landing_page_url_list": [{"landing_page_url": "https://a.test"}]})
        assert http.posts[0][1]["operation_status"] == "DISABLE"


class TestOfficialFieldValidation:
    def test_unknown_top_level_field_is_refused(self):
        conn, http = _conn()
        with pytest.raises(ApiError) as exc:
            conn.execute_action("smart_plus_create_campaign", "7001", {
                "campaign_name": "x", "objective_type": "LEAD_GENERATION",
                "made_up_field": 1})
        assert "made_up_field" in exc.value.message
        assert http.posts == []

    def test_unknown_nested_field_is_refused_with_its_path(self):
        conn, http = _conn()
        params = dict(_ADGROUP_PARAMS)
        params["targeting_spec"] = {"location_ids": ["1"], "planet": "Mars"}
        with pytest.raises(ApiError) as exc:
            conn.execute_action("smart_plus_create_ad_group", "7001", params)
        assert "targeting_spec.planet" in str(exc.value.details)
        assert http.posts == []

    @pytest.mark.parametrize("missing", [
        "adgroup_name", "promotion_type", "optimization_goal", "bid_type",
        "billing_event", "schedule_type", "schedule_start_time",
        "targeting_spec", "campaign_id"])
    def test_officially_required_fields_are_enforced(self, missing):
        conn, http = _conn()
        params = dict(_ADGROUP_PARAMS)
        params.pop(missing)
        with pytest.raises(ApiError) as exc:
            conn.execute_action("smart_plus_create_ad_group", "7001", params)
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert http.posts == []

    def test_types_are_checked(self):
        conn, _ = _conn()
        with pytest.raises(ApiError) as exc:
            conn.execute_action("smart_plus_update_campaign", "7001", {
                "campaign_id": "11", "budget": "lots"})
        assert "budget" in exc.value.message

    def test_list_of_objects_items_are_checked(self):
        conn, _ = _conn()
        with pytest.raises(ApiError) as exc:
            conn.execute_action("smart_plus_create_ad", "7001", {
                "adgroup_id": "21",
                "ad_text_list": [{"ad_text": "ok"}, {"text": "wrong key"}]})
        assert "ad_text_list[1]" in str(exc.value.details)


class TestReadBeforeWrite:
    @pytest.mark.parametrize("action,ids_key,getter,op", [
        ("smart_plus_pause_campaign", "campaign_ids",
         "smart_plus/campaign/get/", "DISABLE"),
        ("smart_plus_enable_campaign", "campaign_ids",
         "smart_plus/campaign/get/", "ENABLE"),
        ("smart_plus_delete_campaign", "campaign_ids",
         "smart_plus/campaign/get/", "DELETE"),
        ("smart_plus_pause_ad_group", "adgroup_ids",
         "smart_plus/adgroup/get/", "DISABLE"),
        ("smart_plus_enable_ad_group", "adgroup_ids",
         "smart_plus/adgroup/get/", "ENABLE"),
        ("smart_plus_delete_ad_group", "adgroup_ids",
         "smart_plus/adgroup/get/", "DELETE"),
        ("smart_plus_pause_ad", "smart_plus_ad_ids", "smart_plus/ad/get/",
         "DISABLE"),
        ("smart_plus_enable_ad", "smart_plus_ad_ids", "smart_plus/ad/get/",
         "ENABLE"),
        ("smart_plus_delete_ad", "smart_plus_ad_ids", "smart_plus/ad/get/",
         "DELETE"),
    ])
    def test_status_actions(self, action, ids_key, getter, op):
        entity_id = {"campaign_ids": "11", "adgroup_ids": "21",
                     "smart_plus_ad_ids": "31"}[ids_key]
        conn, http = _conn()
        result = conn.execute_action(action, "7001", {ids_key: [entity_id]})
        path, params = http.gets[0]
        assert path == getter
        assert json.loads(params["filtering"]) == {ids_key: [entity_id]}
        assert http.posts[0][1][ids_key] == [entity_id]
        assert http.posts[0][1]["operation_status"] == op
        assert result.before["operation_status"] == "ENABLE"

    def test_missing_entity_is_refused_without_writing(self):
        conn, http = _conn(reads={"smart_plus/campaign/get/": {"list": []}})
        with pytest.raises(ApiError):
            conn.execute_action("smart_plus_pause_campaign", "7001",
                                {"campaign_ids": ["99"]})
        assert http.posts == []

    def test_update_reports_before(self):
        conn, http = _conn()
        result = conn.execute_action("smart_plus_update_campaign", "7001", {
            "campaign_id": "11", "budget": 150})
        assert result.before["budget"] == 100.0
        assert http.posts[0] == ("smart_plus/campaign/update/", {
            "campaign_id": "11", "budget": 150, "advertiser_id": "7001"})

    def test_budget_ids_are_read_from_the_nested_lists(self):
        conn, http = _conn()
        conn.execute_action("smart_plus_set_ad_group_budgets", "7001", {
            "budget": [{"adgroup_id": "21", "budget": 40}]})
        assert json.loads(http.gets[0][1]["filtering"]) == {
            "adgroup_ids": ["21"]}

    def test_more_than_20_ids_is_refused(self):
        conn, _ = _conn()
        with pytest.raises(ApiError):
            conn.execute_action("smart_plus_pause_ad", "7001", {
                "smart_plus_ad_ids": [str(i) for i in range(1, 22)]})

    def test_creative_status_and_appeal(self):
        conn, http = _conn()
        conn.execute_action("smart_plus_pause_creatives", "7001", {
            "smart_plus_ad_id": "31", "ad_material_ids": ["m1"]})
        conn.execute_action("smart_plus_appeal_ad", "7001", {
            "smart_plus_ad_id": "31", "appeal_reason": "Compliant"})
        assert http.posts[0][1]["operation_status"] == "DISABLE"
        assert http.posts[1][0] == "smart_plus/ad/appeal/"


class TestOtherActions:
    def test_copy_is_paused_and_returns_a_task(self):
        conn, http = _conn(posts={"smart_plus/campaign/copy/task/create/": {
            "task_id": "t1"}})
        result = conn.execute_action("smart_plus_copy_campaign", "7001", {
            "campaign_id": "11", "campaign_name": "Copy"})
        body = http.posts[0][1]
        assert body["operation_status"] == "DISABLE"
        assert body["request_id"].isdigit()
        assert result.details["task_id"] == "t1"

    def test_value_rule_set_operation_is_owned_by_the_action(self):
        conn, http = _conn(posts={
            "smart_plus/value_rule_set/create_or_update/": {
                "value_rule_set_id": "v1"}})
        conn.execute_action("smart_plus_create_value_rule_set", "7001", {
            "value_rule_set": {"rule_set_name": "Boost iOS", "rules": [
                {"order": 1, "rule_name": "iOS", "adjust_percentage": 20,
                 "criteria": [{"criteria_type": "OS",
                               "value": {"operating_systems": ["IOS"]}}]}]}})
        conn.execute_action("smart_plus_delete_value_rule_set", "7001", {
            "value_rule_set": {"value_rule_set_id": "v1"}})
        assert [b["operation_type"] for _, b in http.posts] == [
            "CREATE", "DELETE"]

    def test_preview_changes_nothing(self):
        conn, http = _conn(posts={"smart_plus/ad/preview/": {
            "preview_link": "https://p.test"}})
        actions = {a.id: a for a in conn.list_actions()}
        assert actions["smart_plus_preview_ad"].destructive is False
        result = conn.execute_action("smart_plus_preview_ad", "7001", {
            "preview_type": "AD", "smart_plus_ad_id": "31"})
        assert result.details["preview_link"] == "https://p.test"

    def test_dry_run_validates_reads_and_sends_nothing(self):
        conn, http = _conn()
        result = conn.execute_action("smart_plus_enable_campaign", "7001",
                                     {"campaign_ids": ["11"]}, dry_run=True)
        assert http.gets and http.posts == []
        assert result.details["dry_run"] is True
        assert result.details["request"]["operation_status"] == "ENABLE"


class TestReports:
    def _query(self, conn, report_type, fields, settings=None, dated=False):
        return conn.query(QuerySpec(
            accounts=["7001"], fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-07"),
            report_type=report_type, settings=settings or {}))

    def test_campaign_listing_uses_official_fields(self):
        conn, http = _conn(reads={"smart_plus/campaign/get/": {
            "list": [_CAMPAIGN], "page_info": {"total_page": 1}}})
        result = self._query(conn, "SmartPlusCampaigns",
                             ("campaign_id", "campaign_name", "budget"))
        assert json.loads(http.gets[0][1]["fields"]) == [
            "campaign_id", "campaign_name", "budget"]
        assert result.rows == [{"campaign_id": "11", "campaign_name": "Sales+",
                                "budget": 100.0}]

    def test_ad_review_needs_ids_and_sends_them_as_a_list(self):
        conn, http = _conn(reads={"smart_plus/ad/review_info/": {
            "smart_plus_ad_review_infos": [
                {"smart_plus_ad_id": "31", "review_status": "APPROVED"}]}})
        result = self._query(conn, "SmartPlusAdReview",
                             ("smart_plus_ad_id", "review_status"),
                             settings={"smart_plus_ad_ids": "31, 32"})
        params = http.gets[0][1]
        assert json.loads(params["smart_plus_ad_ids"]) == ["31", "32"]
        assert "page" not in params
        assert result.rows[0]["review_status"] == "APPROVED"

    def test_copy_task_is_a_single_record(self):
        conn, http = _conn(reads={"smart_plus/campaign/copy/task/check/": {
            "task_status": "SUCCESS", "task_result": {"campaign_id": "12"}}})
        result = self._query(conn, "SmartPlusCopyTask",
                             ("task_status", "task_result"),
                             settings={"task_id": "t1"})
        assert http.gets[0][1]["task_id"] == "t1"
        assert result.rows == [{"task_status": "SUCCESS",
                                "task_result": {"campaign_id": "12"}}]

    def test_creative_overview_report(self):
        conn, http = _conn(reads={"smart_plus/material_report/overview/": {
            "list": [{"dimensions": {"main_material_id": "m1"},
                      "metrics": {"spend": "12.5", "impressions": "1000",
                                  "campaign_name": "Sales+"}}],
            "page_info": {"total_page": 1}}})
        result = self._query(conn, "SmartPlusCreativeOverview",
                             ("main_material_id", "spend", "impressions",
                              "campaign_name"),
                             settings={"campaign_ids": "11"})
        params = http.gets[0][1]
        assert json.loads(params["dimensions"]) == ["main_material_id"]
        assert json.loads(params["metrics"]) == ["spend", "impressions",
                                                 "campaign_name"]
        assert json.loads(params["filtering"]) == {"campaign_ids": ["11"]}
        assert params["page_size"] == 100
        assert result.rows == [{"main_material_id": "m1", "spend": 12.5,
                                "impressions": 1000,
                                "campaign_name": "Sales+"}]

    def test_report_dimensions_are_the_official_ones(self):
        overview = set(tt_fields._fields_for("SmartPlusCreativeOverview"))
        breakdown = set(tt_fields._fields_for("SmartPlusCreativeBreakdown"))
        assert set(SMART_PLUS_REPORT_DIMENSIONS[
            "smart_plus/material_report/overview/"]) <= overview
        assert "stat_time_day" in breakdown
        assert "stat_time_day" not in overview

    def test_a_report_needs_a_dimension(self):
        conn, _ = _conn()
        with pytest.raises(ApiError):
            self._query(conn, "SmartPlusCreativeBreakdown", ("spend",))
