"""The Google Ads connector, against mocked Google Ads REST responses.

The mock returns the real shapes from `customers:listAccessibleCustomers` and
`googleAds:search` (nested, camelCased GoogleAdsRow), so GAQL generation, the
camelCase path mapping, and micros conversion are exercised without a live
provider or a developer token.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.sources.google_ads import GoogleAdsConnector
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec


class _Catalog:
    key = "google_ads"
    report_types = [
        {"id": "Campaign", "settings": []},
        {"id": "Keyword", "settings": []},
    ]
    has_report_types = True


class _DS:
    type = "google_ads"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok"}


ACCESSIBLE = {"resourceNames": ["customers/1112223333", "customers/4445556666"]}

# A GoogleAdsRow as REST returns it: nested by resource, camelCased leaves,
# money in micros as strings.
SEARCH = {
    "results": [
        {"campaign": {"id": "1", "name": "Brand"},
         "metrics": {"clicks": "40", "costMicros": "12500000", "ctr": 0.05},
         "segments": {"date": "2026-08-01"}},
        {"campaign": {"id": "2", "name": "Generic"},
         "metrics": {"clicks": "10", "costMicros": "3000000", "ctr": 0.02},
         "segments": {"date": "2026-08-02"}},
    ],
}


# A GoogleAdsField RESOURCE row for `campaign`, as the field service returns it.
FIELDS_CAMPAIGN = {
    "results": [{
        "name": "campaign",
        "metrics": ["metrics.clicks", "metrics.cost_micros", "metrics.ctr"],
        "segments": ["segments.date"],
    }],
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
    return GoogleAdsConnector(_DS(), http=_mock_http(routes))


class TestDynamicFieldDiscovery:
    # A GoogleAdsField RESOURCE row: metrics/segments list the compatible fields.
    FIELDS = {
        "results": [{
            "name": "campaign",
            "metrics": ["metrics.clicks", "metrics.video_view_rate",
                        "metrics.average_cpv", "metrics.cost_micros"],
            "segments": ["segments.date", "segments.ad_network_type"],
        }],
    }

    def test_list_fields_uses_field_service_and_covers_everything(self):
        conn = _connector({("POST", "googleAdsFields:search"): self.FIELDS})
        by_id = {f.id: f for f in conn.list_fields("Campaign")}
        # Discovered metrics/segments are present...
        assert "metrics.video_view_rate" in by_id
        assert "segments.ad_network_type" in by_id
        # ...curated attributes still there...
        assert "campaign.name" in by_id
        # ...video_view_rate flagged non-aggregatable, average_cpv monetary...
        assert by_id["metrics.video_view_rate"].is_non_aggregatable is True
        assert by_id["metrics.average_cpv"].is_monetary is True
        # ...and a metric the field service did not list is absent.
        assert "metrics.impressions" not in by_id

    def test_falls_back_to_curated_when_discovery_fails(self):
        # No googleAdsFields route -> the mock raises -> curated fallback.
        conn = _connector({})
        ids = {f.id for f in conn.list_fields("Campaign")}
        assert "metrics.impressions" in ids   # from the curated set
        assert "campaign.name" in ids

    def test_discovered_micros_metric_is_divided_on_query(self):
        conn = _connector({
            ("POST", "googleAdsFields:search"): self.FIELDS,
            ("POST", "googleAds:search"): {"results": [
                {"campaign": {"name": "V"},
                 "metrics": {"averageCpv": "250000"}}]},
        })
        spec = QuerySpec(
            accounts=["1112223333"],
            fields=["campaign.name", "metrics.average_cpv"],
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="Campaign",
        )
        result = conn.query(spec)
        assert result.rows[0]["metrics.average_cpv"] == 0.25   # micros -> currency


class TestDateValidation:
    def _spec(self, start, end):
        return QuerySpec(
            accounts=["1112223333"],
            fields=["campaign.name", "metrics.clicks"],
            date_range=DateRange(start, end),
            report_type="Campaign",
        )

    def test_relative_date_token_is_rejected_with_guidance(self):
        conn = _connector({("POST", "googleAdsFields:search"): FIELDS_CAMPAIGN})
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec("2026-01-01", "today"))
        assert exc.value.code == ErrorCode.INVALID_FILTER
        assert "get_today" in exc.value.message
        assert exc.value.retriable is False

    def test_start_after_end_is_rejected(self):
        conn = _connector({("POST", "googleAdsFields:search"): FIELDS_CAMPAIGN})
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec("2026-09-01", "2026-08-01"))
        assert exc.value.code == ErrorCode.INVALID_FILTER


class TestSegmentMetricCompatibility:
    # metrics.in_feed can only be selected with metrics.clicks, NOT segments.date.
    COMPAT = {
        "results": [
            {"name": "segments.date",
             "selectableWith": ["metrics.clicks", "metrics.cost_micros"]},
            {"name": "metrics.clicks",
             "selectableWith": ["segments.date", "metrics.video_view_rate_in_feed"]},
            {"name": "metrics.video_view_rate_in_feed",
             "selectableWith": ["metrics.clicks"]},
        ],
    }
    DISCOVER = {
        "results": [{
            "name": "campaign",
            "metrics": ["metrics.clicks", "metrics.video_view_rate_in_feed"],
            "segments": ["segments.date"],
        }],
    }

    def _http(self):
        # The field service is called twice: resource discovery, then
        # selectable_with. Distinguish by the query body.
        def http(method, url, token, body=None):
            if "googleAdsFields:search" in url:
                q = (body or {}).get("query", "")
                return self.COMPAT if "selectable_with" in q else self.DISCOVER
            return {"results": []}
        return http

    def _spec(self, fields):
        return QuerySpec(
            accounts=["1112223333"], fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="Campaign",
        )

    def test_incompatible_metric_and_segment_is_caught_before_the_api(self):
        conn = GoogleAdsConnector(_DS(), http=self._http())
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec(
                ["segments.date", "metrics.video_view_rate_in_feed"]))
        assert exc.value.code == ErrorCode.INVALID_FILTER
        assert "metrics.video_view_rate_in_feed" in exc.value.message
        assert "segments.date" in exc.value.message

    def test_compatible_selection_passes(self):
        # clicks IS selectable with segments.date -> no error, query proceeds.
        calls = []

        def http(method, url, token, body=None):
            if "googleAdsFields:search" in url:
                q = (body or {}).get("query", "")
                return self.COMPAT if "selectable_with" in q else self.DISCOVER
            calls.append(url)
            return {"results": []}

        conn = GoogleAdsConnector(_DS(), http=http)
        conn.query(self._spec(["segments.date", "metrics.clicks"]))
        assert any("googleAds:search" in u for u in calls)   # reached the query

    def test_metric_metric_pairs_are_never_flagged(self):
        # A metric's selectable_with lists segments/attributes, NOT other metrics,
        # so two metrics must never be judged incompatible (would false-positive
        # on ordinary combos like cost + a video rate). No segment selected here.
        compat = {"results": [
            {"name": "metrics.cost_micros", "selectableWith": ["segments.date"]},
            {"name": "metrics.video_view_rate",
             "selectableWith": ["segments.device"]},
        ]}
        discover = {"results": [{
            "name": "campaign",
            "metrics": ["metrics.cost_micros", "metrics.video_view_rate"],
            "segments": [],
        }]}
        reached = []

        def http(method, url, token, body=None):
            if "googleAdsFields:search" in url:
                q = (body or {}).get("query", "")
                return compat if "selectable_with" in q else discover
            reached.append(url)
            return {"results": []}

        conn = GoogleAdsConnector(_DS(), http=http)
        # cost + a video rate together, no segment -> must NOT be blocked.
        conn.query(self._spec(
            ["metrics.cost_micros", "metrics.video_view_rate"]))
        assert any("googleAds:search" in u for u in reached)

    def test_common_metric_pair_with_unknown_metadata_is_not_flagged(self):
        # Field service returns nothing for these -> unknown -> must NOT block.
        discover = {"results": [{
            "name": "campaign",
            "metrics": ["metrics.clicks", "metrics.cost_micros"],
            "segments": [],
        }]}
        reached = []

        def http(method, url, token, body=None):
            if "googleAdsFields:search" in url:
                q = (body or {}).get("query", "")
                return {"results": []} if "selectable_with" in q else discover
            reached.append(url)
            return {"results": []}

        conn = GoogleAdsConnector(_DS(), http=http)
        conn.query(self._spec(["metrics.clicks", "metrics.cost_micros"]))
        assert any("googleAds:search" in u for u in reached)   # not blocked


class TestListAccounts:
    def test_maps_accessible_customers(self):
        conn = _connector({("GET", "listAccessibleCustomers"): ACCESSIBLE})
        ids = {a.id for a in conn.list_accounts()}
        assert ids == {"1112223333", "4445556666"}


class TestListFields:
    def test_fields_are_scoped_to_the_report_type(self):
        conn = _connector({})
        campaign = {f.id for f in conn.list_fields("Campaign")}
        keyword = {f.id for f in conn.list_fields("Keyword")}
        assert "campaign.name" in campaign
        assert "ad_group_criterion.keyword.text" in keyword
        assert "ad_group_criterion.keyword.text" not in campaign
        # Shared metrics appear on every report.
        assert "metrics.clicks" in campaign and "metrics.clicks" in keyword

    def test_money_and_ratio_flags(self):
        conn = _connector({})
        by_id = {f.id: f for f in conn.list_fields("Campaign")}
        assert by_id["metrics.cost_micros"].is_monetary is True
        assert by_id["metrics.ctr"].is_non_aggregatable is True
        assert by_id["metrics.clicks"].is_non_aggregatable is False

    def test_unknown_report_type_falls_back_to_default(self):
        conn = _connector({})
        # An unrecognised report must not explode; it defaults to Campaign.
        ids = {f.id for f in conn.list_fields("NotAReport")}
        assert "campaign.name" in ids

    def test_video_and_impression_share_metrics_are_covered(self):
        conn = _connector({})
        by_id = {f.id: f for f in conn.list_fields("Campaign")}
        # TrueView / video coverage that was previously missing.
        assert "metrics.video_view_rate" in by_id
        assert by_id["metrics.video_view_rate"].is_non_aggregatable is True
        assert "metrics.video_views" in by_id
        assert "metrics.average_cpv" in by_id
        # Impression share, conversion rate, view-through.
        assert "metrics.search_impression_share" in by_id
        assert "metrics.conversions_from_interactions_rate" in by_id
        assert "metrics.view_through_conversions" in by_id

    def test_non_suffixed_micros_metrics_are_flagged_monetary(self):
        conn = _connector({})
        by_id = {f.id: f for f in conn.list_fields("Campaign")}
        # These are in micros despite having no _micros suffix — must be monetary.
        for fid in ("metrics.average_cpv", "metrics.cost_per_conversion",
                    "metrics.average_cpm"):
            assert by_id[fid].is_monetary is True
        # conversions_value is already in currency and must NOT be micros-divided.
        from terno_dbi.connectors.api.sources.google_ads import (
            _MICROS_FIELDS, _is_micros, _dynamic_metric_field)
        assert "metrics.conversions_value" not in _MICROS_FIELDS
        assert "metrics.average_cpv" in _MICROS_FIELDS
        # A dynamically-discovered CPV variant the curated set never listed must
        # still be detected as micros and flagged monetary (the live-test bug).
        assert _is_micros("metrics.trueview_average_cpv") is True
        assert _dynamic_metric_field("metrics.trueview_average_cpv").is_monetary is True
        # value_per_* is currency, not micros — must not be divided.
        assert _is_micros("metrics.value_per_conversion") is False


class TestRunReport:
    def _spec(self, fields=("segments.date", "campaign.name", "metrics.clicks"),
              accounts=("1112223333",), report_type="Campaign"):
        return QuerySpec(
            accounts=list(accounts), fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type=report_type,
        )

    def test_builds_gaql_with_resource_dates_and_order(self):
        captured = {}

        def http(method, url, token, body=None):
            if "listAccessibleCustomers" in url:
                return ACCESSIBLE
            if "googleAdsFields:search" in url:
                return FIELDS_CAMPAIGN
            captured["body"] = body
            captured["url"] = url
            return SEARCH

        conn = GoogleAdsConnector(_DS(), http=http)
        conn.query(self._spec())
        q = captured["body"]["query"]
        assert "FROM campaign" in q
        assert "WHERE segments.date BETWEEN '2026-08-01' AND '2026-08-31'" in q
        assert "ORDER BY segments.date ASC" in q          # time series
        assert "customers/1112223333/googleAds:search" in captured["url"]

    def test_breakdown_orders_by_first_metric_desc(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["body"] = body
            return {"results": []}

        conn = GoogleAdsConnector(_DS(), http=http)
        conn.query(self._spec(fields=("campaign.name", "metrics.clicks")))
        assert "ORDER BY metrics.clicks DESC" in captured["body"]["query"]

    def test_parses_nested_camelcase_and_converts_micros(self):
        conn = _connector({
            ("GET", "listAccessibleCustomers"): ACCESSIBLE,
            ("POST", "googleAds:search"): SEARCH,
        })
        result = conn.query(self._spec(
            fields=("segments.date", "campaign.name",
                    "metrics.clicks", "metrics.cost_micros")))
        assert result.row_count == 2
        row = result.rows[0]
        assert row["segments.date"] == "2026-08-01"
        assert row["campaign.name"] == "Brand"
        assert row["metrics.clicks"] == 40           # string coerced to int
        assert row["metrics.cost_micros"] == 12.5     # 12_500_000 micros -> 12.5

    def test_unknown_field_is_rejected_with_a_suggestion(self):
        conn = _connector({("POST", "googleAds:search"): SEARCH})
        spec = self._spec(fields=("campaign.name", "metrics.clickz"))
        with pytest.raises(ApiError) as exc:
            conn.query(spec)
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "metrics.clicks" in exc.value.message

    def test_multi_account_merges_and_tags_rows(self):
        conn = _connector({
            ("GET", "listAccessibleCustomers"): ACCESSIBLE,
            ("POST", "googleAds:search"): SEARCH,
        })
        result = conn.query(self._spec(accounts=("1112223333", "4445556666")))
        assert result.row_count == 4
        assert all("_account" in r for r in result.rows)

    def test_customer_id_is_normalised(self):
        seen = {}

        def http(method, url, token, body=None):
            if "googleAdsFields:search" in url:
                return FIELDS_CAMPAIGN
            seen["url"] = url
            return SEARCH

        conn = GoogleAdsConnector(_DS(), http=http)
        conn.query(self._spec(accounts=("111-222-3333",)))
        assert "customers/1112223333/googleAds:search" in seen["url"]


class TestAuthMapping:
    def test_401_becomes_auth_expired(self):
        from terno_dbi.connectors.api.sources.google_ads import _AuthError

        def http(method, url, token, body=None):
            raise _AuthError()

        conn = GoogleAdsConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestPartialSuccess:
    def test_one_bad_account_does_not_sink_the_query(self):
        good = "1112223333"
        bad = "4445556666"

        def http(method, url, token, body=None):
            if "listAccessibleCustomers" in url:
                return ACCESSIBLE
            if "googleAdsFields:search" in url:
                return FIELDS_CAMPAIGN
            if f"customers/{bad}/" in url:
                # Simulate CUSTOMER_NOT_ENABLED (a manager/deactivated account).
                raise ApiError(ErrorCode.UPSTREAM_ERROR,
                               "The customer account can't be accessed",
                               retriable=False)
            return SEARCH

        conn = GoogleAdsConnector(_DS(), http=http)
        spec = QuerySpec(
            accounts=[good, bad],
            fields=["segments.date", "campaign.name", "metrics.clicks"],
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="Campaign",
        )
        result = conn.query(spec)
        # Good account's rows come back; the bad account becomes a warning.
        assert result.row_count == 2
        assert any(bad in w for w in result.warnings)


class TestErrorSurfacing:
    class _Resp:
        def __init__(self, status, payload=None, text=""):
            self.status_code = status
            self._payload = payload
            self.text = text

        def json(self):
            if self._payload is None:
                raise ValueError("no json")
            return self._payload

    def test_google_ads_failure_is_surfaced_and_non_retriable(self):
        from terno_dbi.connectors.api.sources.google_ads import _ads_error
        resp = self._Resp(403, {
            "error": {
                "code": 403, "message": "The caller does not have permission",
                "details": [{
                    "errors": [{
                        "errorCode": {"authorizationError": "DEVELOPER_TOKEN_PROHIBITED"},
                        "message": "Developer token is not allowed with project '123'.",
                    }],
                }],
            },
        })
        err = _ads_error(resp)
        assert err.code == ErrorCode.UPSTREAM_ERROR
        assert err.retriable is False
        assert "DEVELOPER_TOKEN_PROHIBITED" in err.message
        assert "project '123'" in err.message

    def test_404_hints_at_a_sunset_api_version(self):
        from terno_dbi.connectors.api.sources.google_ads import _ads_error, _API_VERSION
        err = _ads_error(self._Resp(404, None, text="<html>Not Found</html>"))
        assert _API_VERSION in err.message      # points the operator at the version
        assert err.retriable is False

    def test_server_errors_stay_retriable(self):
        from terno_dbi.connectors.api.sources.google_ads import _ads_error
        err = _ads_error(self._Resp(503, {"error": {"message": "backend"}}))
        assert err.retriable is True

    def test_missing_developer_token_is_a_clear_config_error(self, monkeypatch):
        from terno_dbi.connectors.api.sources import google_ads as ga
        monkeypatch.delenv(ga._DEVELOPER_TOKEN_ENV, raising=False)
        with pytest.raises(ApiError) as exc:
            ga._default_http("GET", "https://x", "tok")
        assert exc.value.retriable is False
        assert ga._DEVELOPER_TOKEN_ENV in exc.value.message


class TestRegistration:
    def test_google_ads_is_registered_at_startup(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("google_ads")

    def test_registered_factory_binds_a_token_refresher(self):
        from terno_dbi.connectors.api.sources.google_ads import (
            make_google_ads_connector,
        )
        conn = make_google_ads_connector(_DS())
        assert conn._token_refresher is not None


def _recording_http(routes):
    """Like _mock_http but records every (method, url, body) for assertions."""
    calls = []

    def http(method, url, token, body=None):
        assert token == "tok"
        calls.append({"method": method, "url": url, "body": body})
        for (m, needle), response in routes.items():
            if method == m and needle in url:
                return response
        raise AssertionError(f"unexpected call: {method} {url}")

    return http, calls


class TestWriteActions:
    def test_list_actions_exposes_pause_enable_and_budget(self):
        conn = _connector({})
        by_id = {a.id: a for a in conn.list_actions()}
        assert {"pause_campaign", "enable_campaign", "set_campaign_budget",
                "pause_ad_group", "enable_ad_group"} <= set(by_id)
        # every action carries a JSON schema and is marked destructive
        assert all(a.schema.get("type") == "object" for a in conn.list_actions())
        assert all(a.destructive for a in conn.list_actions())

    def test_pause_campaign_reads_then_mutates_status(self):
        search = {"results": [{"campaign": {
            "id": "1", "name": "Brand", "status": "ENABLED"}}]}
        http, calls = _recording_http({
            ("POST", "googleAds:search"): search,
            ("POST", "campaigns:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("pause_campaign", "1112223333",
                                  {"campaign_id": "1"})
        d = res.as_dict()
        assert d["before"]["status"] == "ENABLED"      # read-before-write
        assert d["after"]["status"] == "PAUSED"
        # the mutate body sets status=PAUSED on the right resource, never REMOVED
        mutate = [c for c in calls if "campaigns:mutate" in c["url"]][0]
        op = mutate["body"]["operations"][0]
        assert op["update"]["status"] == "PAUSED"
        assert op["update"]["resourceName"].endswith("/campaigns/1")
        assert op["updateMask"] == "status"

    def test_enable_campaign_sets_enabled(self):
        search = {"results": [{"campaign": {
            "id": "9", "name": "X", "status": "PAUSED"}}]}
        http, calls = _recording_http({
            ("POST", "googleAds:search"): search,
            ("POST", "campaigns:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("enable_campaign", "1112223333",
                                  {"campaign_id": "9"})
        assert res.as_dict()["after"]["status"] == "ENABLED"

    def test_set_campaign_budget_converts_to_micros_on_the_budget_resource(self):
        search = {"results": [{
            "campaign": {"id": "1", "name": "Brand",
                         "campaignBudget": "customers/1112223333/campaignBudgets/55"},
            "campaignBudget": {"amountMicros": "10000000"},
        }]}
        http, calls = _recording_http({
            ("POST", "googleAds:search"): search,
            ("POST", "campaignBudgets:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("set_campaign_budget", "1112223333",
                                  {"campaign_id": "1", "amount": 50})
        d = res.as_dict()
        assert d["before"]["amount"] == 10.0     # old micros -> currency units
        assert d["after"]["amount"] == 50
        mutate = [c for c in calls if "campaignBudgets:mutate" in c["url"]][0]
        op = mutate["body"]["operations"][0]
        assert op["update"]["amountMicros"] == 50_000_000
        assert op["update"]["resourceName"].endswith("/campaignBudgets/55")

    def test_unknown_action_is_rejected(self):
        conn = _connector({})
        with pytest.raises(ApiError) as e:
            conn.execute_action("delete_campaign", "1112223333", {"campaign_id": "1"})
        assert e.value.code == ErrorCode.UNKNOWN_ACTION

    def test_missing_or_nonnumeric_id_is_rejected(self):
        conn = _connector({})
        with pytest.raises(ApiError) as e1:
            conn.execute_action("pause_campaign", "1112223333", {})
        assert e1.value.code == ErrorCode.INVALID_ACTION_PARAMS
        with pytest.raises(ApiError) as e2:
            conn.execute_action("pause_campaign", "1112223333",
                                {"campaign_id": "abc"})
        assert e2.value.code == ErrorCode.INVALID_ACTION_PARAMS

    def test_nonpositive_budget_is_rejected_before_any_call(self):
        conn = _connector({})   # no routes: a provider call would raise
        with pytest.raises(ApiError) as e:
            conn.execute_action("set_campaign_budget", "1112223333",
                                {"campaign_id": "1", "amount": 0})
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS

    def test_missing_campaign_is_reported_not_mutated(self):
        http, calls = _recording_http({("POST", "googleAds:search"): {"results": []}})
        conn = GoogleAdsConnector(_DS(), http=http)
        with pytest.raises(ApiError) as e:
            conn.execute_action("pause_campaign", "1112223333", {"campaign_id": "7"})
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert not any("mutate" in c["url"] for c in calls)   # never mutated

    def test_base_connector_is_read_only_by_default(self):
        from terno_dbi.connectors.api.model.base import ApiConnector
        # A minimal concrete connector that does not override write methods.
        class _RO(ApiConnector):
            def list_accounts(self): return []
            def list_fields(self, report_type=None): return []
            def _run(self, spec): raise AssertionError
        ro = _RO(_DS())
        assert ro.list_actions() == []
        with pytest.raises(ApiError) as e:
            ro.execute_action("anything", "acct", {})
        assert e.value.code == ErrorCode.UNKNOWN_ACTION


class TestWriteActionsExtended:
    def test_list_actions_now_includes_create_and_keywords(self):
        conn = _connector({})
        ids = {a.id for a in conn.list_actions()}
        assert {"create_campaign", "add_keywords", "add_negative_keywords",
                "remove_keyword"} <= ids

    def test_create_campaign_makes_budget_then_paused_campaign(self):
        http, calls = _recording_http({
            ("POST", "campaignBudgets:mutate"):
                {"results": [{"resourceName": "customers/1112223333/campaignBudgets/77"}]},
            ("POST", "campaigns:mutate"):
                {"results": [{"resourceName": "customers/1112223333/campaigns/999"}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("create_campaign", "1112223333",
                                  {"name": "Brand", "daily_budget": 50})
        d = res.as_dict()
        assert d["after"]["status"] == "PAUSED"        # create-paused
        assert d["after"]["id"] == "999"
        assert d["after"]["daily_budget"] == 50
        # budget created first, referenced by the campaign create
        budget_call = [c for c in calls if "campaignBudgets:mutate" in c["url"]][0]
        assert budget_call["body"]["operations"][0]["create"]["amountMicros"] == 50_000_000
        camp_op = [c for c in calls if "campaigns:mutate" in c["url"]][0]["body"]["operations"][0]["create"]
        assert camp_op["status"] == "PAUSED"
        assert camp_op["advertisingChannelType"] == "SEARCH"
        assert camp_op["campaignBudget"] == "customers/1112223333/campaignBudgets/77"

    def test_create_campaign_rejects_nonpositive_budget(self):
        conn = _connector({})   # no routes -> a provider call would blow up
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_campaign", "1112223333",
                                {"name": "X", "daily_budget": 0})
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS

    def test_add_keywords_creates_enabled_criteria_with_match_type(self):
        http, calls = _recording_http({
            ("POST", "adGroupCriteria:mutate"): {"results": [{}, {}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("add_keywords", "1112223333",
                                  {"ad_group_id": "55",
                                   "keywords": ["running shoes", "trainers"],
                                   "match_type": "exact"})
        assert res.as_dict()["after"]["match_type"] == "EXACT"
        ops = [c for c in calls if "adGroupCriteria:mutate" in c["url"]][0]["body"]["operations"]
        assert len(ops) == 2
        assert ops[0]["create"]["keyword"] == {"text": "running shoes", "matchType": "EXACT"}
        assert ops[0]["create"]["adGroup"].endswith("/adGroups/55")
        assert ops[0]["create"]["status"] == "ENABLED"

    def test_add_keywords_defaults_to_phrase(self):
        http, _ = _recording_http({("POST", "adGroupCriteria:mutate"): {"results": [{}]}})
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("add_keywords", "1112223333",
                                  {"ad_group_id": "55", "keywords": ["shoes"]})
        assert res.as_dict()["after"]["match_type"] == "PHRASE"

    def test_add_negative_keywords_marks_negative_on_campaign(self):
        http, calls = _recording_http({
            ("POST", "campaignCriteria:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        conn.execute_action("add_negative_keywords", "1112223333",
                            {"campaign_id": "12", "keywords": ["free"]})
        op = [c for c in calls if "campaignCriteria:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["create"]["negative"] is True
        assert op["create"]["campaign"].endswith("/campaigns/12")
        assert op["create"]["keyword"]["text"] == "free"

    def test_add_keywords_rejects_empty_list(self):
        conn = _connector({})
        with pytest.raises(ApiError) as e:
            conn.execute_action("add_keywords", "1112223333",
                                {"ad_group_id": "55", "keywords": []})
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS

    def test_add_keywords_rejects_bad_match_type(self):
        conn = _connector({})
        with pytest.raises(ApiError) as e:
            conn.execute_action("add_keywords", "1112223333",
                                {"ad_group_id": "55", "keywords": ["x"],
                                 "match_type": "SORTA"})
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS

    def test_remove_keyword_targets_the_composite_resource(self):
        http, calls = _recording_http({
            ("POST", "adGroupCriteria:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        conn.execute_action("remove_keyword", "1112223333",
                            {"ad_group_id": "55", "criterion_id": "888"})
        op = [c for c in calls if "adGroupCriteria:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["remove"].endswith("/adGroupCriteria/55~888")


class TestBiddingAndAds:
    def test_list_actions_includes_bidding_and_rsa(self):
        ids = {a.id for a in _connector({}).list_actions()}
        assert {"set_target_cpa", "set_target_roas", "set_max_cpc",
                "create_responsive_search_ad"} <= ids

    def test_set_target_cpa_switches_strategy_with_micros(self):
        search = {"results": [{"campaign": {
            "id": "1", "name": "Brand", "biddingStrategyType": "MANUAL_CPC"}}]}
        http, calls = _recording_http({
            ("POST", "googleAds:search"): search,
            ("POST", "campaigns:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("set_target_cpa", "1112223333",
                                  {"campaign_id": "1", "target_cpa": 25})
        d = res.as_dict()
        assert d["before"]["bidding_strategy_type"] == "MANUAL_CPC"
        assert d["after"]["bidding_strategy_type"] == "TARGET_CPA"
        op = [c for c in calls if "campaigns:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["update"]["targetCpa"]["targetCpaMicros"] == 25_000_000
        assert op["updateMask"] == "target_cpa.target_cpa_micros"

    def test_set_target_roas_uses_ratio(self):
        search = {"results": [{"campaign": {"id": "1", "name": "B",
                                            "biddingStrategyType": "MANUAL_CPC"}}]}
        http, calls = _recording_http({
            ("POST", "googleAds:search"): search,
            ("POST", "campaigns:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        conn.execute_action("set_target_roas", "1112223333",
                            {"campaign_id": "1", "target_roas": 4})
        op = [c for c in calls if "campaigns:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["update"]["targetRoas"]["targetRoas"] == 4
        assert op["updateMask"] == "target_roas.target_roas"

    def test_set_max_cpc_on_ad_group(self):
        search = {"results": [{"adGroup": {
            "id": "55", "name": "AG", "cpcBidMicros": "1000000"}}]}
        http, calls = _recording_http({
            ("POST", "googleAds:search"): search,
            ("POST", "adGroups:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("set_max_cpc", "1112223333",
                                  {"ad_group_id": "55", "max_cpc": 1.5})
        d = res.as_dict()
        assert d["before"]["max_cpc"] == 1.0
        assert d["after"]["max_cpc"] == 1.5
        op = [c for c in calls if "adGroups:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["update"]["cpcBidMicros"] == 1_500_000
        assert op["updateMask"] == "cpc_bid_micros"

    def test_create_rsa_builds_headlines_and_descriptions(self):
        http, calls = _recording_http({
            ("POST", "adGroupAds:mutate"):
                {"results": [{"resourceName": "customers/1112223333/adGroupAds/55~9"}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("create_responsive_search_ad", "1112223333", {
            "ad_group_id": "55",
            "final_url": "https://example.com",
            "headlines": ["H1", "H2", "H3"],
            "descriptions": ["D1", "D2"],
        })
        assert res.as_dict()["after"]["status"] == "ENABLED"
        create = [c for c in calls if "adGroupAds:mutate" in c["url"]][0]["body"]["operations"][0]["create"]
        rsa = create["ad"]["responsiveSearchAd"]
        assert [h["text"] for h in rsa["headlines"]] == ["H1", "H2", "H3"]
        assert [d["text"] for d in rsa["descriptions"]] == ["D1", "D2"]
        assert create["ad"]["finalUrls"] == ["https://example.com"]

    def test_create_rsa_requires_min_headlines(self):
        conn = _connector({})
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_responsive_search_ad", "1112223333", {
                "ad_group_id": "55", "final_url": "https://x.com",
                "headlines": ["only one"], "descriptions": ["D1", "D2"],
            })
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS

    def test_create_rsa_rejects_overlong_headline(self):
        conn = _connector({})
        with pytest.raises(ApiError) as e:
            conn.execute_action("create_responsive_search_ad", "1112223333", {
                "ad_group_id": "55", "final_url": "https://x.com",
                "headlines": ["x" * 31, "H2", "H3"], "descriptions": ["D1", "D2"],
            })
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS

    def test_bidding_rejects_nonpositive(self):
        conn = _connector({})
        with pytest.raises(ApiError) as e:
            conn.execute_action("set_target_cpa", "1112223333",
                                {"campaign_id": "1", "target_cpa": 0})
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS


class TestLifecycleCompletion:
    def test_list_actions_now_covers_full_lifecycle(self):
        ids = {a.id for a in _connector({}).list_actions()}
        assert {"create_ad_group", "remove_ad_group", "pause_ad", "enable_ad",
                "remove_ad", "update_keyword", "remove_campaign"} <= ids
        assert len(ids) == 33   # complete registry (incl. advanced surface)

    def test_create_ad_group_is_paused_with_optional_max_cpc(self):
        http, calls = _recording_http({
            ("POST", "adGroups:mutate"):
                {"results": [{"resourceName": "customers/1112223333/adGroups/60"}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("create_ad_group", "1112223333",
                                  {"campaign_id": "1", "name": "AG1", "max_cpc": 2})
        d = res.as_dict()
        assert d["after"]["status"] == "PAUSED" and d["after"]["id"] == "60"
        op = [c for c in calls if "adGroups:mutate" in c["url"]][0]["body"]["operations"][0]["create"]
        assert op["status"] == "PAUSED"
        assert op["campaign"].endswith("/campaigns/1")
        assert op["cpcBidMicros"] == 2_000_000

    def test_remove_ad_group_reads_then_removes(self):
        search = {"results": [{"adGroup": {"id": "55", "name": "AG"}}]}
        http, calls = _recording_http({
            ("POST", "googleAds:search"): search,
            ("POST", "adGroups:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("remove_ad_group", "1112223333", {"ad_group_id": "55"})
        assert res.as_dict()["after"]["removed"] is True
        op = [c for c in calls if "adGroups:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["remove"].endswith("/adGroups/55")

    def test_pause_ad_reads_status_then_mutates(self):
        search = {"results": [{"adGroupAd": {"status": "ENABLED", "ad": {"id": "9"}}}]}
        http, calls = _recording_http({
            ("POST", "googleAds:search"): search,
            ("POST", "adGroupAds:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("pause_ad", "1112223333",
                                  {"ad_group_id": "55", "ad_id": "9"})
        d = res.as_dict()
        assert d["before"]["status"] == "ENABLED"
        assert d["after"]["status"] == "PAUSED"
        op = [c for c in calls if "adGroupAds:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["update"]["resourceName"].endswith("/adGroupAds/55~9")
        assert op["update"]["status"] == "PAUSED"

    def test_remove_ad_targets_composite(self):
        http, calls = _recording_http({("POST", "adGroupAds:mutate"): {"results": [{}]}})
        conn = GoogleAdsConnector(_DS(), http=http)
        conn.execute_action("remove_ad", "1112223333",
                            {"ad_group_id": "55", "ad_id": "9"})
        op = [c for c in calls if "adGroupAds:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["remove"].endswith("/adGroupAds/55~9")

    def test_update_keyword_status_and_bid_builds_mask(self):
        http, calls = _recording_http({("POST", "adGroupCriteria:mutate"): {"results": [{}]}})
        conn = GoogleAdsConnector(_DS(), http=http)
        conn.execute_action("update_keyword", "1112223333",
                            {"ad_group_id": "55", "criterion_id": "888",
                             "status": "paused", "max_cpc": 3})
        body = [c for c in calls if "adGroupCriteria:mutate" in c["url"]][0]["body"]["operations"][0]
        assert body["update"]["status"] == "PAUSED"
        assert body["update"]["cpcBidMicros"] == 3_000_000
        assert set(body["updateMask"].split(",")) == {"status", "cpc_bid_micros"}
        assert body["update"]["resourceName"].endswith("/adGroupCriteria/55~888")

    def test_update_keyword_requires_a_field(self):
        conn = _connector({})
        with pytest.raises(ApiError) as e:
            conn.execute_action("update_keyword", "1112223333",
                                {"ad_group_id": "55", "criterion_id": "888"})
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS

    def test_remove_campaign_reads_then_removes(self):
        search = {"results": [{"campaign": {"id": "1", "name": "Brand", "status": "PAUSED"}}]}
        http, calls = _recording_http({
            ("POST", "googleAds:search"): search,
            ("POST", "campaigns:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("remove_campaign", "1112223333", {"campaign_id": "1"})
        d = res.as_dict()
        assert d["before"]["name"] == "Brand"
        assert d["after"]["status"] == "REMOVED"
        op = [c for c in calls if "campaigns:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["remove"].endswith("/campaigns/1")


class TestAdvancedSurface:
    def test_registry_totals_33(self):
        ids = {a.id for a in _connector({}).list_actions()}
        assert {"set_maximize_conversions", "set_maximize_conversion_value",
                "set_manual_cpc", "set_target_impression_share",
                "create_portfolio_bid_strategy", "attach_campaign_to_portfolio",
                "add_sitelink", "add_callout", "add_structured_snippet",
                "create_customer_list", "add_customer_list_members",
                "attach_audience", "remove_audience"} <= ids

    def _camp_search(self):
        return {"results": [{"campaign": {"id": "1", "name": "B",
                                          "biddingStrategyType": "MANUAL_CPC"}}]}

    def test_maximize_conversions_with_target_cpa(self):
        http, calls = _recording_http({
            ("POST", "googleAds:search"): self._camp_search(),
            ("POST", "campaigns:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        conn.execute_action("set_maximize_conversions", "1112223333",
                            {"campaign_id": "1", "target_cpa": 20})
        op = [c for c in calls if "campaigns:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["update"]["maximizeConversions"]["targetCpaMicros"] == 20_000_000
        assert op["updateMask"] == "maximize_conversions.target_cpa_micros"

    def test_maximize_conversions_without_target(self):
        http, calls = _recording_http({
            ("POST", "googleAds:search"): self._camp_search(),
            ("POST", "campaigns:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        conn.execute_action("set_maximize_conversions", "1112223333", {"campaign_id": "1"})
        op = [c for c in calls if "campaigns:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["updateMask"] == "maximize_conversions"
        assert op["update"]["maximizeConversions"] == {}

    def test_manual_cpc_enhanced(self):
        http, calls = _recording_http({
            ("POST", "googleAds:search"): self._camp_search(),
            ("POST", "campaigns:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        conn.execute_action("set_manual_cpc", "1112223333",
                            {"campaign_id": "1", "enhanced": True})
        op = [c for c in calls if "campaigns:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["update"]["manualCpc"]["enhancedCpcEnabled"] is True

    def test_target_impression_share_converts_percentage(self):
        http, calls = _recording_http({
            ("POST", "googleAds:search"): self._camp_search(),
            ("POST", "campaigns:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        conn.execute_action("set_target_impression_share", "1112223333",
                            {"campaign_id": "1", "location": "top_of_page",
                             "target_percentage": 65, "cpc_bid_ceiling": 2})
        op = [c for c in calls if "campaigns:mutate" in c["url"]][0]["body"]["operations"][0]
        tis = op["update"]["targetImpressionShare"]
        assert tis["location"] == "TOP_OF_PAGE"
        assert tis["locationFractionMicros"] == 650_000   # 65%
        assert tis["cpcBidCeilingMicros"] == 2_000_000
        assert "target_impression_share.cpc_bid_ceiling_micros" in op["updateMask"]

    def test_create_portfolio_target_roas(self):
        http, calls = _recording_http({
            ("POST", "biddingStrategies:mutate"):
                {"results": [{"resourceName": "customers/1112223333/biddingStrategies/5"}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("create_portfolio_bid_strategy", "1112223333",
                                  {"name": "P1", "type": "TARGET_ROAS", "target": 4})
        assert res.as_dict()["after"]["id"] == "5"
        op = [c for c in calls if "biddingStrategies:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["create"]["targetRoas"]["targetRoas"] == 4

    def test_attach_campaign_to_portfolio(self):
        http, calls = _recording_http({
            ("POST", "googleAds:search"): self._camp_search(),
            ("POST", "campaigns:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        conn.execute_action("attach_campaign_to_portfolio", "1112223333",
                            {"campaign_id": "1", "bidding_strategy_id": "5"})
        op = [c for c in calls if "campaigns:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["update"]["biddingStrategy"].endswith("/biddingStrategies/5")
        assert op["updateMask"] == "bidding_strategy"

    def test_add_sitelink_creates_asset_then_links(self):
        http, calls = _recording_http({
            ("POST", "assets:mutate"):
                {"results": [{"resourceName": "customers/1112223333/assets/900"}]},
            ("POST", "campaignAssets:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        conn.execute_action("add_sitelink", "1112223333",
                            {"campaign_id": "1", "link_text": "Shop",
                             "final_url": "https://e.com"})
        asset_op = [c for c in calls if "assets:mutate" in c["url"] and "campaignAssets" not in c["url"]][0]["body"]["operations"][0]
        assert asset_op["create"]["sitelinkAsset"]["linkText"] == "Shop"
        link_op = [c for c in calls if "campaignAssets:mutate" in c["url"]][0]["body"]["operations"][0]
        assert link_op["create"]["fieldType"] == "SITELINK"
        assert link_op["create"]["asset"].endswith("/assets/900")

    def test_add_callout(self):
        http, calls = _recording_http({
            ("POST", "assets:mutate"):
                {"results": [{"resourceName": "customers/1112223333/assets/901"}]},
            ("POST", "campaignAssets:mutate"): {"results": [{}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        conn.execute_action("add_callout", "1112223333",
                            {"campaign_id": "1", "text": "Free shipping"})
        link_op = [c for c in calls if "campaignAssets:mutate" in c["url"]][0]["body"]["operations"][0]
        assert link_op["create"]["fieldType"] == "CALLOUT"

    def test_create_customer_list(self):
        http, calls = _recording_http({
            ("POST", "userLists:mutate"):
                {"results": [{"resourceName": "customers/1112223333/userLists/321"}]},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("create_customer_list", "1112223333", {"name": "VIPs"})
        assert res.as_dict()["after"]["id"] == "321"
        op = [c for c in calls if "userLists:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["create"]["crmBasedUserList"]["uploadKeyType"] == "CONTACT_INFO"

    def test_add_members_hashes_pii_and_runs_job(self):
        import hashlib
        http, calls = _recording_http({
            ("POST", "offlineUserDataJobs:create"):
                {"resourceName": "customers/1112223333/offlineUserDataJobs/77"},
            ("POST", "offlineUserDataJobs/77:addOperations"): {},
            ("POST", "offlineUserDataJobs/77:run"): {},
        })
        conn = GoogleAdsConnector(_DS(), http=http)
        res = conn.execute_action("add_customer_list_members", "1112223333",
                                  {"user_list_id": "321",
                                   "emails": ["  Alice@Example.com "]})
        assert res.as_dict()["after"]["member_count"] == 1
        add_op = [c for c in calls if "addOperations" in c["url"]][0]["body"]["operations"][0]
        expected = hashlib.sha256("alice@example.com".encode()).hexdigest()
        assert add_op["create"]["userIdentifiers"][0]["hashedEmail"] == expected
        # the job is actually run
        assert any(":run" in c["url"] for c in calls)

    def test_add_members_requires_some_identifier(self):
        conn = _connector({})
        with pytest.raises(ApiError) as e:
            conn.execute_action("add_customer_list_members", "1112223333",
                                {"user_list_id": "321", "emails": [], "phones": []})
        assert e.value.code == ErrorCode.INVALID_ACTION_PARAMS

    def test_attach_audience(self):
        http, calls = _recording_http({("POST", "adGroupCriteria:mutate"): {"results": [{}]}})
        conn = GoogleAdsConnector(_DS(), http=http)
        conn.execute_action("attach_audience", "1112223333",
                            {"ad_group_id": "55", "user_list_id": "321"})
        op = [c for c in calls if "adGroupCriteria:mutate" in c["url"]][0]["body"]["operations"][0]
        assert op["create"]["userList"]["userList"].endswith("/userLists/321")
        assert op["create"]["adGroup"].endswith("/adGroups/55")
