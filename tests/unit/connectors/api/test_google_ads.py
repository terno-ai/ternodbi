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

    def test_incompatible_metric_metric_pair_is_caught(self):
        # cost_micros and video_view_rate_in_feed don't list each other.
        compat = {"results": [
            {"name": "metrics.cost_micros", "selectableWith": ["metrics.clicks"]},
            {"name": "metrics.video_view_rate_in_feed",
             "selectableWith": ["metrics.clicks"]},
        ]}
        discover = {"results": [{
            "name": "campaign",
            "metrics": ["metrics.cost_micros", "metrics.video_view_rate_in_feed"],
            "segments": [],
        }]}

        def http(method, url, token, body=None):
            if "googleAdsFields:search" in url:
                q = (body or {}).get("query", "")
                return compat if "selectable_with" in q else discover
            return {"results": []}

        conn = GoogleAdsConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec(
                ["metrics.cost_micros", "metrics.video_view_rate_in_feed"]))
        assert exc.value.code == ErrorCode.INVALID_FILTER

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
