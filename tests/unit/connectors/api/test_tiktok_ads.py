"""The TikTok Ads connector, against mocked Business API responses.

Exercises the two request shapes without a live app: the integrated report
(data level from the finest entity column, BASIC vs AUDIENCE, date windows,
attribute metrics) and the entity listings — plus the `{code, message, data}`
envelope, whose failures arrive as HTTP 200.
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
from terno_dbi.connectors.api.sources.tiktok.ads.connector import TikTokAdsConnector


class _Catalog:
    key = "tiktok_ads"
    # The declared report types, so tests see what production validates.
    from terno_dbi.catalog.declarations import get_spec as _get_spec
    report_types = [r.as_dict() for r in _get_spec("tiktok_ads").report_types]
    has_report_types = True


class _DS:
    type = "tiktok_ads"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok"}


def _ok(data):
    return {"code": 0, "message": "OK", "request_id": "r", "data": data}


def _report(rows, page=1, total_page=1):
    return _ok({"list": rows, "page_info": {
        "page": page, "page_size": 1000, "total_number": len(rows),
        "total_page": total_page}})


class _Recorder:
    """Answers each call from `respond(path, params)` and records it."""

    def __init__(self, respond):
        self.respond = respond
        self.calls = []

    def __call__(self, method, url, token, params=None):
        assert method == "GET"
        assert token == "tok"
        path = url.split("/open_api/v1.3/", 1)[1]
        self.calls.append((path, dict(params or {})))
        return self.respond(path, params or {})


def _connector(respond):
    rec = _Recorder(respond)
    return TikTokAdsConnector(_DS(), http=rec), rec


def _spec(fields, report_type="Performance", start="2026-08-01",
          end="2026-08-07", accounts=("7001",), max_rows=1000):
    return QuerySpec(
        accounts=list(accounts), fields=list(fields),
        date_range=DateRange(start, end), report_type=report_type,
        max_rows=max_rows,
    )


@pytest.fixture
def app_creds(monkeypatch):
    monkeypatch.setenv("TERNO_TIKTOK_APP_ID", "app-1")
    monkeypatch.setenv("TERNO_TIKTOK_APP_SECRET", "app-secret")


class TestListAccounts:
    def test_lists_authorised_advertisers_with_currency_and_tz(self, app_creds):
        def respond(path, params):
            if path == "oauth2/advertiser/get/":
                return _ok({"list": [
                    {"advertiser_id": 7001, "advertiser_name": "Acme"},
                    {"advertiser_id": "7002", "advertiser_name": "Beta"},
                ]})
            assert path == "advertiser/info/"
            assert json.loads(params["advertiser_ids"]) == ["7001", "7002"]
            return _ok({"list": [
                {"advertiser_id": "7001", "name": "Acme Inc", "currency": "USD",
                 "timezone": "Etc/GMT+5", "display_timezone": "America/New_York"},
            ]})

        conn, rec = _connector(respond)
        accounts = conn.list_accounts()
        # The listing authenticates the app as well as the token.
        assert rec.calls[0][1] == {"app_id": "app-1", "secret": "app-secret"}
        assert [a.id for a in accounts] == ["7001", "7002"]
        assert accounts[0].name == "Acme Inc"
        assert accounts[0].currency == "USD"
        assert accounts[0].timezone == "America/New_York"
        # No info row: the authorisation's own name, no currency.
        assert accounts[1].name == "Beta"
        assert accounts[1].currency is None

    def test_info_failure_still_lists_the_advertisers(self, app_creds):
        def respond(path, params):
            if path == "oauth2/advertiser/get/":
                return _ok({"list": [{"advertiser_id": "7001",
                                      "advertiser_name": "Acme"}]})
            return {"code": 40001, "message": "No permission"}

        conn, _ = _connector(respond)
        accounts = conn.list_accounts()
        assert [(a.id, a.name) for a in accounts] == [("7001", "Acme")]

    def test_info_is_batched_by_100(self, app_creds):
        ids = [str(9000 + i) for i in range(150)]

        def respond(path, params):
            if path == "oauth2/advertiser/get/":
                return _ok({"list": [{"advertiser_id": i} for i in ids]})
            return _ok({"list": []})

        conn, rec = _connector(respond)
        conn.list_accounts()
        batches = [json.loads(p["advertiser_ids"]) for path, p in rec.calls
                   if path == "advertiser/info/"]
        assert [len(b) for b in batches] == [100, 50]

    def test_unconfigured_app_is_refused(self, monkeypatch):
        monkeypatch.delenv("TERNO_TIKTOK_APP_ID", raising=False)
        monkeypatch.delenv("TERNO_TIKTOK_APP_SECRET", raising=False)
        conn, rec = _connector(lambda p, q: pytest.fail("no call expected"))
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert "TERNO_TIKTOK_APP_ID" in exc.value.message
        assert rec.calls == []


class TestListFields:
    def test_report_vs_entity_catalogues(self):
        conn, _ = _connector(None)
        report = {f.id for f in conn.list_fields("Performance")}
        campaigns = {f.id for f in conn.list_fields("Campaigns")}
        assert {"spend", "date", "age", "campaign_name"} <= report
        assert {"objective_type", "budget"} <= campaigns
        assert "spend" not in campaigns

    def test_default_report_is_performance(self):
        conn, _ = _connector(None)
        assert ({f.id for f in conn.list_fields()}
                == {f.id for f in conn.list_fields("Performance")})

    def test_money_and_ratio_flags(self):
        conn, _ = _connector(None)
        by_id = {f.id: f for f in conn.list_fields("Performance")}
        assert by_id["spend"].is_monetary is True
        assert by_id["cpc"].is_non_aggregatable is True
        assert by_id["reach"].is_non_aggregatable is True
        assert by_id["impressions"].is_non_aggregatable is False


class TestReportRequest:
    def _params(self, fields, **kw):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_spec(fields, **kw))
        assert all(path == "report/integrated/get/" for path, _ in rec.calls)
        return rec.calls[0][1], rec

    def test_advertiser_level_without_an_entity_column(self):
        p, _ = self._params(("spend", "impressions"))
        assert p["data_level"] == "AUCTION_ADVERTISER"
        assert json.loads(p["dimensions"]) == ["advertiser_id"]
        assert json.loads(p["metrics"]) == ["spend", "impressions"]
        assert p["report_type"] == "BASIC"
        assert p["service_type"] == "AUCTION"
        assert p["advertiser_id"] == "7001"
        assert (p["start_date"], p["end_date"]) == ("2026-08-01", "2026-08-07")

    def test_finest_entity_sets_the_level_and_names_ride_as_metrics(self):
        p, _ = self._params(("campaign_name", "adgroup_name", "date", "clicks"))
        assert p["data_level"] == "AUCTION_ADGROUP"
        assert json.loads(p["dimensions"]) == ["adgroup_id", "stat_time_day"]
        assert json.loads(p["metrics"]) == [
            "clicks", "campaign_name", "adgroup_name"]

    def test_coarser_ids_are_attribute_metrics(self):
        p, _ = self._params(("ad_id", "campaign_id", "spend"))
        assert p["data_level"] == "AUCTION_AD"
        assert json.loads(p["dimensions"]) == ["ad_id"]
        assert json.loads(p["metrics"]) == ["spend", "campaign_id"]

    def test_audience_split_switches_the_report_type(self):
        p, _ = self._params(("campaign_id", "age", "gender", "spend"))
        assert p["report_type"] == "AUDIENCE"
        assert json.loads(p["dimensions"]) == ["campaign_id", "age", "gender"]

    def test_dimensions_only_still_sends_a_metric(self):
        p, _ = self._params(("campaign_id", "campaign_name"))
        assert json.loads(p["metrics"]) == ["campaign_name"]
        p, _ = self._params(("campaign_id",))
        assert json.loads(p["metrics"]) == ["spend"]

    def test_daily_split_is_fetched_in_30_day_windows(self):
        _, rec = self._params(("date", "spend"), start="2026-01-01",
                              end="2026-03-15")
        windows = [(q["start_date"], q["end_date"]) for _, q in rec.calls]
        assert windows == [("2026-01-01", "2026-01-30"),
                           ("2026-01-31", "2026-03-01"),
                           ("2026-03-02", "2026-03-15")]

    def test_hourly_split_is_fetched_a_day_at_a_time(self):
        p, rec = self._params(("hour", "spend"), start="2026-08-01",
                              end="2026-08-03")
        assert json.loads(p["dimensions"]) == ["advertiser_id", "stat_time_hour"]
        assert [q["start_date"] for _, q in rec.calls] == [
            "2026-08-01", "2026-08-02", "2026-08-03"]
        assert all(q["start_date"] == q["end_date"] for _, q in rec.calls)

    def test_totals_are_one_request_up_to_365_days(self):
        _, rec = self._params(("spend",), start="2026-01-01", end="2026-12-31")
        assert len(rec.calls) == 1

    def test_totals_beyond_365_days_are_refused(self):
        conn, rec = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(("spend",), start="2025-01-01", end="2026-06-30"))
        assert exc.value.code == ErrorCode.INVALID_FILTER
        assert "date dimension" in exc.value.message
        assert rec.calls == []

    def test_date_and_hour_together_are_refused(self):
        conn, _ = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(("date", "hour", "spend")))
        assert exc.value.code == ErrorCode.INVALID_FILTER

    def test_unknown_field_is_rejected_with_a_suggestion(self):
        conn, _ = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(("spendd",)))
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "spend" in exc.value.details["suggestions"]

    def test_attribute_metrics_set_the_data_level(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_spec(("objective_type", "spend")))
        p = rec.calls[0][1]
        # objective_type is supported from campaign level (official note).
        assert p["data_level"] == "AUCTION_CAMPAIGN"
        assert json.loads(p["dimensions"]) == ["campaign_id"]
        assert json.loads(p["metrics"]) == ["spend", "objective_type"]

    def test_unknown_field_is_still_rejected(self):
        conn, _ = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(("made_up_metric",)))
        assert exc.value.code == ErrorCode.INVALID_FIELD


class TestReportRows:
    ROWS = [
        {"dimensions": {"campaign_id": "11", "stat_time_day": "2026-08-01 00:00:00"},
         "metrics": {"campaign_name": "Brand", "spend": "12.50",
                     "impressions": "1000", "ctr": "1.25", "conversion": "-"}},
        {"dimensions": {"campaign_id": "11", "stat_time_day": "2026-08-02 00:00:00"},
         "metrics": {"campaign_name": "Brand", "spend": "9",
                     "impressions": "800", "ctr": "0.90", "conversion": "3"}},
    ]

    def test_parses_dimensions_attribute_metrics_and_numbers(self):
        conn, _ = _connector(lambda p, q: _report(self.ROWS))
        fields = ("date", "campaign_id", "campaign_name", "spend",
                  "impressions", "ctr", "conversion")
        result = conn.query(_spec(fields))
        assert result.requested_field_ids == list(fields)
        assert result.rows[0] == {
            "date": "2026-08-01", "campaign_id": "11", "campaign_name": "Brand",
            "spend": 12.5, "impressions": 1000, "ctr": 1.25, "conversion": None,
        }
        # Money stays a float even when whole; counts collapse to int.
        assert result.rows[1]["spend"] == 9.0
        assert isinstance(result.rows[1]["spend"], float)
        assert result.rows[1]["conversion"] == 3

    def test_hour_keeps_its_timestamp(self):
        row = {"dimensions": {"advertiser_id": "7001",
                              "stat_time_hour": "2026-08-01 13:00:00"},
               "metrics": {"spend": "1.5"}}
        conn, _ = _connector(lambda p, q: _report([row]))
        result = conn.query(_spec(("hour", "spend"), end="2026-08-01"))
        assert result.rows == [{"hour": "2026-08-01 13:00:00", "spend": 1.5}]

    def test_no_fields_defaults_to_core_metrics(self):
        row = {"dimensions": {"advertiser_id": "7001"},
               "metrics": {"spend": "5", "impressions": "50", "clicks": "2"}}
        conn, rec = _connector(lambda p, q: _report([row]))
        result = conn.query(_spec(()))
        assert json.loads(rec.calls[0][1]["metrics"]) == [
            "spend", "impressions", "clicks"]
        assert result.requested_field_ids == ["spend", "impressions", "clicks"]
        assert result.rows == [{"spend": 5.0, "impressions": 50, "clicks": 2}]

    def test_follows_pages(self):
        def respond(path, params):
            page = params["page"]
            assert params["page_size"] == 1000
            return _report([{"dimensions": {"advertiser_id": "7001"},
                             "metrics": {"spend": str(page)}}],
                           page=page, total_page=3)

        conn, rec = _connector(respond)
        result = conn.query(_spec(("spend",)))
        assert [r["spend"] for r in result.rows] == [1.0, 2.0, 3.0]
        assert [q["page"] for _, q in rec.calls] == [1, 2, 3]

    def test_max_rows_stops_paging_and_windowing(self):
        def respond(path, params):
            return _report([{"dimensions": {"stat_time_day": params["start_date"]},
                             "metrics": {"spend": "1"}}] * 2,
                           page=params["page"], total_page=5)

        conn, rec = _connector(respond)
        result = conn.query(_spec(("date", "spend"), start="2026-01-01",
                                  end="2026-03-31", max_rows=3))
        assert result.row_count == 3
        assert len(rec.calls) == 2   # two pages of the first window, then stop

    def test_multi_account_tags_rows_and_keeps_partial_success(self):
        def respond(path, params):
            if params["advertiser_id"] == "7002":
                return {"code": 40002, "message": "No access to advertiser"}
            return _report([{"dimensions": {"advertiser_id": "7001"},
                             "metrics": {"spend": "4"}}])

        conn, _ = _connector(respond)
        result = conn.query(_spec(("spend",), accounts=("7001", "7002")))
        assert result.rows == [{"_account": "7001", "spend": 4.0}]
        assert len(result.warnings) == 1
        assert "7002" in result.warnings[0]
        assert "No access to advertiser" in result.warnings[0]


class TestEntities:
    def test_lists_campaigns_with_json_fields(self):
        def respond(path, params):
            assert path == "campaign/get/"
            return _ok({"list": [
                {"campaign_id": "11", "campaign_name": "Brand", "budget": 50.0,
                 "operation_status": "ENABLE", "extra": "x"},
            ], "page_info": {"page": 1, "total_page": 1}})

        conn, rec = _connector(respond)
        result = conn.query(_spec(("campaign_id", "campaign_name", "budget"),
                                  report_type="Campaigns"))
        params = rec.calls[0][1]
        assert params["advertiser_id"] == "7001"
        assert json.loads(params["fields"]) == [
            "campaign_id", "campaign_name", "budget"]
        assert "start_date" not in params
        assert result.rows == [
            {"campaign_id": "11", "campaign_name": "Brand", "budget": 50.0}]

    @pytest.mark.parametrize("report_type,path", [
        ("AdGroups", "adgroup/get/"), ("Ads", "ad/get/")])
    def test_each_entity_report_has_its_endpoint(self, report_type, path):
        conn, rec = _connector(lambda p, q: _ok({"list": []}))
        result = conn.query(_spec((), report_type=report_type))
        assert rec.calls[0][0] == path
        # No fields asked: the id and the name.
        assert len(result.requested_field_ids) == 2


class TestErrors:
    def _raised(self, envelope):
        conn, _ = _connector(lambda p, q: envelope)
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(("spend",)))
        return exc.value

    def test_revoked_token_becomes_auth_expired(self):
        err = self._raised({"code": 40105, "message": "Access token is incorrect"})
        assert err.code == ErrorCode.AUTH_EXPIRED

    def test_http_401_becomes_auth_expired(self):
        def http(method, url, token, params=None):
            raise tt_api._AuthError()

        conn = TikTokAdsConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(("spend",)))
        assert exc.value.code == ErrorCode.AUTH_EXPIRED

    @pytest.mark.parametrize("code", [40016, 40100, 40133])
    def test_throttling_is_rate_limited(self, code):
        err = self._raised({"code": code, "message": "Too many requests"})
        assert err.code == ErrorCode.RATE_LIMITED
        assert err.retriable is True

    def test_service_hiccup_is_retriable(self):
        err = self._raised({"code": 51001, "message": "Internal service timeout"})
        assert err.code == ErrorCode.UPSTREAM_ERROR
        assert err.retriable is True

    def test_refusal_carries_tiktoks_message_and_is_final(self):
        err = self._raised({"code": 40002,
                            "message": "metrics: invalid metric 'foo'"})
        assert err.code == ErrorCode.UPSTREAM_ERROR
        assert err.retriable is False
        assert "40002" in err.message
        assert "invalid metric 'foo'" in err.message

    def test_transport_failure_is_wrapped(self):
        def http(method, url, token, params=None):
            raise ConnectionError("boom")

        conn = TikTokAdsConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(("spend",)))
        assert exc.value.code == ErrorCode.UPSTREAM_ERROR

    def test_default_http_sends_the_access_token_header(self, monkeypatch):
        import requests
        seen = {}

        class _Resp:
            status_code = 200
            reason = "OK"
            def json(self): return _ok({})

        def fake_request(method, url, headers=None, params=None, timeout=None):
            seen.update(method=method, url=url, headers=headers, params=params)
            return _Resp()

        monkeypatch.setattr(requests, "request", fake_request)
        tt_api._default_http("GET", "https://x/y", "tok", {"a": 1})
        assert seen["headers"] == {"Access-Token": "tok"}
        assert seen["params"] == {"a": 1}


class TestCatalogSpec:
    def test_declared_report_types_match_the_connector(self):
        from terno_dbi.catalog.declarations import get_spec
        spec = get_spec("tiktok_ads")
        declared = {r.id for r in spec.report_types}
        assert declared == {tt_fields._DEFAULT_REPORT, *tt_listings._ENTITY_ENDPOINT,
                            *tt_fields._SP_REPORTS, *tt_fields._TYPED_REPORTS,
                            *tt_gmv.REPORTS}
        assert spec.default_report_type == tt_fields._DEFAULT_REPORT

    def test_provider_is_tiktoks_portal(self):
        from terno_dbi.connectors.api.auth.providers import get_provider
        provider = get_provider("tiktok_ads")
        assert provider.client_id_param == "app_id"
        assert provider.token_json_envelope is True
        assert provider.use_pkce is False


class TestRegistration:
    def test_tiktok_ads_is_registered_at_startup(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("tiktok_ads")

    def test_registered_factory_binds_a_token_refresher(self):
        from terno_dbi.connectors.api.sources.tiktok.ads.connector import (
            make_tiktok_ads_connector,
        )
        conn = make_tiktok_ads_connector(_DS())
        assert conn._token_refresher is not None


# --- new entity listings ---------------------------------------------------


class TestMoreListings:
    def test_audiences_page_by_100_without_a_fields_projection(self):
        def respond(path, params):
            assert path == "dmp/custom_audience/list/"
            return _ok({"list": [{"audience_id": "55", "name": "Buyers",
                                  "cover_num": 1200}],
                        "page_info": {"page": 1, "total_page": 1}})

        conn, rec = _connector(respond)
        result = conn.query(_spec(("custom_audience_id", "name", "cover_num"),
                                  report_type="Audiences"))
        params = rec.calls[0][1]
        assert params["page_size"] == 100
        assert "fields" not in params
        # TikTok's own key is aliased to the declared column.
        assert result.rows == [{"custom_audience_id": "55", "name": "Buyers",
                                "cover_num": 1200}]

    def test_identities_read_their_own_list_key(self):
        conn, _ = _connector(lambda p, q: _ok({"identity_list": [
            {"identity_id": "i1", "identity_type": "CUSTOMIZED_USER",
             "display_name": "Acme"}]}))
        result = conn.query(_spec(("identity_id", "display_name"),
                                  report_type="Identities"))
        assert result.rows == [{"identity_id": "i1", "display_name": "Acme"}]

    def test_spark_posts_flatten_nested_info(self):
        conn, _ = _connector(lambda p, q: _ok({"list": [
            {"item_info": {"item_id": "7300", "text": "hi"},
             "user_info": {"identity_id": "u1", "tiktok_name": "Creator"},
             "auth_info": {"ad_auth_status": "AUTHORIZED"}}]}))
        result = conn.query(_spec(("item_id", "tiktok_name", "ad_auth_status"),
                                  report_type="SparkPosts"))
        assert result.rows == [{"item_id": "7300", "tiktok_name": "Creator",
                                "ad_auth_status": "AUTHORIZED"}]

    def test_comments_carry_the_date_range_and_ad_group_filter(self):
        conn, rec = _connector(lambda p, q: _ok({"comments": [
            {"comment_id": "c1", "content": "nice",
             "comment_status": "PUBLIC"}]}))
        spec = QuerySpec(accounts=["7001"],
                         fields=["comment_id", "content", "comment_status"],
                         date_range=DateRange("2026-08-01", "2026-08-07"),
                         report_type="Comments", settings={"adgroup_id": "21"})
        result = conn.query(spec)
        path, params = rec.calls[0]
        assert path == "comment/list/"
        assert (params["start_time"], params["end_time"]) == (
            "2026-08-01", "2026-08-07")
        assert (params["search_field"], params["search_value"]) == (
            "ADGROUP_ID", "21")
        assert result.rows == [{"comment_id": "c1", "content": "nice",
                                "comment_status": "PUBLIC"}]


# --- write actions ---------------------------------------------------------


class _Writer:
    """Answers GETs from `reads` (path -> data) and records every POST."""

    def __init__(self, reads=None, posts=None):
        self.reads = reads or {}
        self.posts_reply = posts or {}
        self.gets = []
        self.posts = []

    def __call__(self, method, url, token, params=None, *, body=None,
                 files=None):
        assert token == "tok"
        path = url.split("/open_api/v1.3/", 1)[1]
        if method == "GET":
            self.gets.append((path, dict(params or {})))
            data = self.reads.get(path, {"list": []})
            return _ok(data(params) if callable(data) else data)
        assert method == "POST"
        self.posts.append((path, body, files))
        reply = self.posts_reply.get(path, {})
        if isinstance(reply, dict) and "code" in reply:
            return reply
        return _ok(reply)


def _writer(**kw):
    w = _Writer(**kw)
    return TikTokAdsConnector(_DS(), http=w), w


_CAMPAIGN = {"campaign_id": "11", "campaign_name": "Brand",
             "operation_status": "ENABLE", "budget_mode": "BUDGET_MODE_DAY",
             "budget": 100.0}
_ADGROUP = {"adgroup_id": "21", "adgroup_name": "US 18-34", "campaign_id": "11",
            "operation_status": "ENABLE", "budget_mode": "BUDGET_MODE_DAY",
            "budget": 40.0, "bid_price": 1.5}
_AD = {"ad_id": "31", "ad_name": "Hook v1", "adgroup_id": "21",
       "campaign_id": "11", "operation_status": "ENABLE",
       "ad_format": "SINGLE_VIDEO", "identity_type": "CUSTOMIZED_USER",
       "identity_id": "id1", "video_id": "v1", "image_ids": ["img1"],
       "ad_text": "Old text", "call_to_action": "LEARN_MORE",
       "landing_page_url": "https://acme.test", "secondary_status": "X"}
_ENTITY_READS = {"campaign/get/": {"list": [_CAMPAIGN]},
                 "adgroup/get/": {"list": [_ADGROUP]},
                 "ad/get/": {"list": [_AD]}}


class TestActionRegistry:
    def test_full_surface_is_listed_with_schemas(self):
        conn, _ = _writer()
        actions = {a.id: a for a in conn.list_actions()}
        assert len(actions) == 46 + len(tt_smart_plus.SPEC_ACTIONS)
        for expected in (
                "create_campaign", "update_campaign", "set_campaign_budget",
                "pause_campaign", "enable_campaign", "delete_campaign",
                "create_ad_group", "update_ad_group", "set_ad_group_budget",
                "set_ad_group_bid", "set_ad_group_schedule",
                "set_ad_group_targeting", "set_ad_group_audiences",
                "pause_ad_group", "enable_ad_group", "delete_ad_group",
                "create_ad", "update_ad", "pause_ad", "enable_ad", "delete_ad",
                "upload_image", "upload_video", "create_identity",
                "authorize_spark_post", "create_customer_file_audience",
                "add_audience_members", "remove_audience_members",
                "rename_audience", "create_lookalike_audience",
                "create_rule_audience", "delete_audience", "share_audience",
                "reply_to_comment", "hide_comment", "unhide_comment",
                "delete_comment", "add_blocked_words", "remove_blocked_words",
                "create_automated_rule", "update_automated_rule",
                "enable_automated_rule", "disable_automated_rule",
                "delete_automated_rule", "create_pixel", "update_pixel"):
            assert expected in actions, expected
            schema = actions[expected].schema
            assert schema["type"] == "object"
            assert schema["additionalProperties"] is False

    def test_unknown_action(self):
        conn, _ = _writer()
        with pytest.raises(ApiError) as exc:
            conn.execute_action("launch_rocket", "7001", {})
        assert exc.value.code == ErrorCode.UNKNOWN_ACTION

    def test_unknown_param_is_refused_before_any_call(self):
        conn, w = _writer()
        with pytest.raises(ApiError) as exc:
            conn.execute_action("pause_campaign", "7001",
                                {"campaign_id": "11", "oops": 1})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert w.gets == [] and w.posts == []

    @pytest.mark.parametrize("params", [{"campaign_id": "abc"},
                                        {"campaign_id": ""}, {}])
    def test_bad_ids_are_refused(self, params):
        conn, _ = _writer()
        with pytest.raises(ApiError) as exc:
            conn.execute_action("pause_campaign", "7001", params)
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS


class TestStatusActions:
    @pytest.mark.parametrize("action,path,id_key,status", [
        ("pause_campaign", "campaign/status/update/", "campaign_id", "DISABLE"),
        ("enable_campaign", "campaign/status/update/", "campaign_id", "ENABLE"),
        ("delete_campaign", "campaign/status/update/", "campaign_id", "DELETE"),
        ("pause_ad_group", "adgroup/status/update/", "adgroup_id", "DISABLE"),
        ("enable_ad_group", "adgroup/status/update/", "adgroup_id", "ENABLE"),
        ("delete_ad_group", "adgroup/status/update/", "adgroup_id", "DELETE"),
        ("pause_ad", "ad/status/update/", "ad_id", "DISABLE"),
        ("enable_ad", "ad/status/update/", "ad_id", "ENABLE"),
        ("delete_ad", "ad/status/update/", "ad_id", "DELETE"),
    ])
    def test_reads_then_posts_status(self, action, path, id_key, status):
        entity_id = {"campaign_id": "11", "adgroup_id": "21", "ad_id": "31"}[id_key]
        conn, w = _writer(reads=_ENTITY_READS)
        result = conn.execute_action(action, "7001", {id_key: entity_id})
        kind = id_key[:-3]
        get_path, get_params = w.gets[0]
        assert get_path == f"{kind}/get/"
        assert json.loads(get_params["filtering"]) == {f"{kind}_ids": [entity_id]}
        assert w.posts == [(path, {"advertiser_id": "7001",
                                   f"{kind}_ids": [entity_id],
                                   "operation_status": status}, None)]
        assert result.before["operation_status"] == "ENABLE"
        assert result.after["operation_status"] == status

    def test_bulk_ids(self):
        reads = {"campaign/get/": {"list": [
            {**_CAMPAIGN, "campaign_id": "11"},
            {**_CAMPAIGN, "campaign_id": "12", "campaign_name": "B"}]}}
        conn, w = _writer(reads=reads)
        result = conn.execute_action("pause_campaign", "7001",
                                     {"campaign_ids": ["11", "12"]})
        assert w.posts[0][1]["campaign_ids"] == ["11", "12"]
        assert set(result.after) == {"11", "12"}
        assert "2 campaigns" in result.summary

    def test_missing_entity_is_a_param_error_and_nothing_is_posted(self):
        conn, w = _writer(reads={"campaign/get/": {"list": []}})
        with pytest.raises(ApiError) as exc:
            conn.execute_action("enable_campaign", "7001", {"campaign_id": "99"})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert w.posts == []

    def test_dry_run_reads_but_never_posts(self):
        conn, w = _writer(reads=_ENTITY_READS)
        result = conn.execute_action("delete_ad", "7001", {"ad_id": "31"},
                                     dry_run=True)
        assert w.gets and w.posts == []
        assert result.details == {"dry_run": True, "applied": False,
                                  "request": {"advertiser_id": "7001",
                                              "ad_ids": ["31"],
                                              "operation_status": "DELETE"}}

    def test_more_than_20_ids_is_refused(self):
        conn, _ = _writer()
        with pytest.raises(ApiError):
            conn.execute_action("pause_ad", "7001",
                                {"ad_ids": [str(i) for i in range(1, 22)]})


class TestCampaignActions:
    def test_create_campaign_is_paused(self):
        conn, w = _writer(posts={"campaign/create/": {"campaign_id": "77"}})
        result = conn.execute_action("create_campaign", "7001", {
            "campaign_name": "Launch", "objective_type": "traffic",
            "budget_mode": "BUDGET_MODE_DAY", "budget": 60})
        path, body, _ = w.posts[0]
        assert path == "campaign/create/"
        assert body["operation_status"] == "DISABLE"
        assert body["objective_type"] == "TRAFFIC"
        assert body["budget"] == 60.0
        assert result.after["campaign_id"] == "77"
        assert "paused" in result.summary

    def test_capped_budget_mode_needs_a_budget(self):
        conn, _ = _writer()
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_campaign", "7001", {
                "campaign_name": "x", "objective_type": "REACH",
                "budget_mode": "BUDGET_MODE_TOTAL"})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS

    def test_extra_cannot_override_status(self):
        conn, _ = _writer()
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_campaign", "7001", {
                "campaign_name": "x", "objective_type": "REACH",
                "extra": {"operation_status": "ENABLE"}})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS

    def test_set_campaign_budget_reports_before_and_after(self):
        conn, w = _writer(reads=_ENTITY_READS)
        result = conn.execute_action("set_campaign_budget", "7001",
                                     {"campaign_id": "11", "budget": 150})
        assert w.posts == [("campaign/update/", {
            "advertiser_id": "7001", "campaign_id": "11", "budget": 150.0},
            None)]
        assert result.before["budget"] == 100.0
        assert result.after["budget"] == 150.0

    def test_uncapped_campaign_budget_points_to_ad_groups(self):
        reads = {"campaign/get/": {"list": [
            {**_CAMPAIGN, "budget_mode": "BUDGET_MODE_INFINITE"}]}}
        conn, w = _writer(reads=reads)
        with pytest.raises(ApiError) as exc:
            conn.execute_action("set_campaign_budget", "7001",
                                {"campaign_id": "11", "budget": 150})
        assert "set_ad_group_budget" in exc.value.message
        assert w.posts == []

    def test_update_campaign_renames(self):
        conn, w = _writer(reads=_ENTITY_READS)
        result = conn.execute_action("update_campaign", "7001",
                                     {"campaign_id": "11",
                                      "campaign_name": "Brand 2"})
        assert w.posts[0][1]["campaign_name"] == "Brand 2"
        assert result.before == {"campaign_id": "11", "campaign_name": "Brand"}


class TestAdGroupActions:
    def test_create_ad_group_applies_safe_defaults(self):
        conn, w = _writer(reads=_ENTITY_READS,
                          posts={"adgroup/create/": {"adgroup_id": "88"}})
        result = conn.execute_action("create_ad_group", "7001", {
            "campaign_id": "11", "adgroup_name": "New", "budget": 30,
            "budget_mode": "BUDGET_MODE_DAY", "billing_event": "CPC",
            "promotion_type": "WEBSITE",
            "location_ids": ["6252001"], "optimization_goal": "CLICK",
            "age_groups": ["AGE_18_24"]})
        body = w.posts[0][1]
        assert body["operation_status"] == "DISABLE"
        assert body["budget_mode"] == "BUDGET_MODE_DAY"
        assert body["placement_type"] == "PLACEMENT_TYPE_AUTOMATIC"
        assert body["schedule_type"] == "SCHEDULE_FROM_NOW"
        assert body["bid_type"] == "BID_TYPE_NO_BID"
        assert body["schedule_start_time"]
        assert result.after["adgroup_id"] == "88"

    _GROUP = {"campaign_id": "11", "adgroup_name": "New", "budget": 30,
              "budget_mode": "BUDGET_MODE_DAY", "billing_event": "CPC",
              "location_ids": ["6252001"], "optimization_goal": "CLICK"}

    @pytest.mark.parametrize("missing", ["billing_event", "budget_mode",
                                         "budget", "optimization_goal",
                                         "location_ids"])
    def test_officially_required_fields_are_required(self, missing):
        conn, w = _writer(reads=_ENTITY_READS)
        params = {**self._GROUP, "promotion_type": "WEBSITE"}
        params.pop(missing)
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_ad_group", "7001", params)
        assert missing in exc.value.message
        assert w.posts == []

    def test_promotion_type_needed_unless_reach_video_or_engagement(self):
        conn, _ = _writer(reads=_ENTITY_READS)   # campaign has no objective
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_ad_group", "7001", dict(self._GROUP))
        assert "promotion_type" in exc.value.message
        reads = {"campaign/get/": {"list": [
            {**_CAMPAIGN, "objective_type": "REACH"}]}}
        conn, w = _writer(reads=reads,
                          posts={"adgroup/create/": {"adgroup_id": "88"}})
        conn.execute_action("create_ad_group", "7001", dict(self._GROUP))
        assert "promotion_type" not in w.posts[0][1]

    def test_total_budget_needs_a_start_end_schedule(self):
        conn, _ = _writer(reads=_ENTITY_READS)
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_ad_group", "7001", {
                **self._GROUP, "promotion_type": "WEBSITE",
                "budget_mode": "BUDGET_MODE_TOTAL"})
        assert "SCHEDULE_START_END" in exc.value.message

    def test_frequency_cap_is_reach_only(self):
        reads = {"campaign/get/": {"list": [
            {**_CAMPAIGN, "objective_type": "TRAFFIC"}]}}
        conn, _ = _writer(reads=reads)
        with pytest.raises(ApiError):
            conn.execute_action("create_ad_group", "7001", {
                **self._GROUP, "promotion_type": "WEBSITE", "frequency": 2,
                "frequency_schedule": 3})

    def test_values_outside_the_official_enums_are_refused(self):
        conn, w = _writer(reads=_ENTITY_READS)
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_ad_group", "7001", {
                **self._GROUP, "promotion_type": "WEBSITE",
                "billing_event": "OCPC"})   # deprecated, not in the enum
        assert "billing_event" in exc.value.message
        assert w.posts == []

    def test_custom_bid_needs_a_bid(self):
        conn, _ = _writer(reads=_ENTITY_READS)
        with pytest.raises(ApiError):
            conn.execute_action("create_ad_group", "7001", {
                **self._GROUP, "promotion_type": "WEBSITE",
                "bid_type": "BID_TYPE_CUSTOM"})

    def test_set_budget_goes_through_adgroup_update(self):
        conn, w = _writer(reads=_ENTITY_READS)
        result = conn.execute_action("set_ad_group_budget", "7001",
                                     {"adgroup_id": "21", "budget": 55})
        assert w.posts == [("adgroup/update/", {
            "advertiser_id": "7001", "adgroup_id": "21", "budget": 55.0},
            None)]
        assert (result.before["budget"], result.after["budget"]) == (40.0, 55.0)

    def test_set_bid_sends_only_bid_fields(self):
        conn, w = _writer(reads=_ENTITY_READS)
        result = conn.execute_action("set_ad_group_bid", "7001",
                                     {"adgroup_id": "21", "bid_price": 2})
        assert w.posts[0] == ("adgroup/update/", {
            "advertiser_id": "7001", "adgroup_id": "21", "bid_price": 2.0}, None)
        assert result.before["bid_price"] == 1.5

    def test_setter_rejects_fields_outside_its_scope(self):
        conn, _ = _writer(reads=_ENTITY_READS)
        with pytest.raises(ApiError) as exc:
            conn.execute_action("set_ad_group_bid", "7001",
                                {"adgroup_id": "21", "budget": 5})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS

    def test_targeting_and_audiences(self):
        conn, w = _writer(reads=_ENTITY_READS)
        conn.execute_action("set_ad_group_targeting", "7001", {
            "adgroup_id": "21", "gender": "GENDER_FEMALE",
            "location_ids": ["6252001", "2635167"]})
        conn.execute_action("set_ad_group_audiences", "7001", {
            "adgroup_id": "21", "audience_ids": ["55"],
            "excluded_audience_ids": ["56"]})
        assert w.posts[0][1]["location_ids"] == ["6252001", "2635167"]
        assert w.posts[1][1]["excluded_audience_ids"] == ["56"]

    def test_bad_datetime_is_refused(self):
        conn, _ = _writer(reads=_ENTITY_READS)
        with pytest.raises(ApiError):
            conn.execute_action("set_ad_group_schedule", "7001", {
                "adgroup_id": "21", "schedule_end_time": "2026-12-01"})


class TestAdActions:
    def test_create_ad_is_paused_in_the_same_call(self):
        conn, w = _writer(reads=_ENTITY_READS,
                          posts={"ad/create/": {"ad_ids": ["901"]}})
        result = conn.execute_action("create_ad", "7001", {
            "adgroup_id": "21", "ad_name": "Hook v2",
            "identity_type": "CUSTOMIZED_USER", "identity_id": "id1",
            "video_id": "v9", "image_ids": ["img9"], "ad_text": "Try it",
            "call_to_action": "SHOP_NOW",
            "landing_page_url": "https://acme.test/x"})
        assert len(w.posts) == 1
        path, body, _ = w.posts[0]
        assert path == "ad/create/"
        creative = body["creatives"][0]
        assert creative["ad_format"] == "SINGLE_VIDEO"
        assert creative["operation_status"] == "DISABLE"
        assert result.details["ad_ids"] == ["901"]
        assert "paused" in result.summary

    def test_image_ad_needs_images(self):
        conn, _ = _writer(reads=_ENTITY_READS)
        with pytest.raises(ApiError):
            conn.execute_action("create_ad", "7001", {
                "adgroup_id": "21", "ad_name": "x", "ad_format": "SINGLE_IMAGE",
                "identity_type": "CUSTOMIZED_USER", "identity_id": "id1",
                "ad_text": "t"})

    def test_spark_ad_needs_no_text(self):
        conn, w = _writer(reads=_ENTITY_READS,
                          posts={"ad/create/": {"ad_ids": ["902"]}})
        conn.execute_action("create_ad", "7001", {
            "adgroup_id": "21", "ad_name": "Spark",
            "identity_type": "AUTH_CODE", "identity_id": "u1",
            "tiktok_item_id": "7300"})
        assert w.posts[0][1]["creatives"][0]["tiktok_item_id"] == "7300"

    def test_update_ad_carries_unchanged_creative_fields(self):
        conn, w = _writer(reads=_ENTITY_READS)
        result = conn.execute_action("update_ad", "7001",
                                     {"ad_id": "31", "ad_text": "New text"})
        path, body, _ = w.posts[0]
        assert path == "ad/update/"
        assert body["adgroup_id"] == "21"
        creative = body["creatives"][0]
        assert creative["ad_id"] == "31"
        assert creative["ad_text"] == "New text"
        assert creative["video_id"] == "v1"           # carried over
        assert "secondary_status" not in creative      # read-only, not sent
        assert result.before == {"ad_id": "31", "ad_text": "Old text"}


class TestCreativeActions:
    def test_upload_image_by_url(self):
        conn, w = _writer(posts={"file/image/ad/upload/": {"image_id": "im1"}})
        result = conn.execute_action("upload_image", "7001",
                                     {"image_url": "https://cdn.test/a.jpg"})
        path, body, files = w.posts[0]
        assert body == {"advertiser_id": "7001",
                        "upload_type": "UPLOAD_BY_URL",
                        "image_url": "https://cdn.test/a.jpg"}
        # Official docs: UPLOAD_BY_URL is sent as application/json.
        assert files is None
        assert result.details["image_id"] == "im1"

    def test_upload_video_reads_a_list_reply(self):
        conn, _ = _writer(posts={"file/video/ad/upload/": [{"video_id": "v5"}]})
        result = conn.execute_action("upload_video", "7001",
                                     {"video_url": "https://cdn.test/a.mp4"})
        assert result.details["video_id"] == "v5"

    def test_non_http_url_is_refused(self):
        conn, _ = _writer()
        with pytest.raises(ApiError):
            conn.execute_action("upload_image", "7001",
                                {"image_url": "file:///etc/passwd"})

    def test_identity_and_spark_authorisation(self):
        conn, w = _writer(posts={"identity/create/": {"identity_id": "id7"}})
        r1 = conn.execute_action("create_identity", "7001",
                                 {"display_name": "Acme", "image_uri": "im1"})
        conn.execute_action("authorize_spark_post", "7001",
                            {"auth_code": "#abc"})
        assert r1.details["identity_id"] == "id7"
        assert w.posts[1] == ("tt_video/authorize/",
                              {"advertiser_id": "7001", "auth_code": "#abc"},
                              None)

    def test_spark_dry_run_redacts_the_code(self):
        conn, w = _writer()
        result = conn.execute_action("authorize_spark_post", "7001",
                                     {"auth_code": "secret"}, dry_run=True)
        assert w.posts == []
        assert "secret" not in json.dumps(result.as_dict())


class TestAudienceActions:
    def test_customer_file_is_hashed_uploaded_then_created(self):
        import hashlib
        conn, w = _writer(posts={
            "dmp/custom_audience/file/upload/": {"file_path": "fp/1"},
            "dmp/custom_audience/create/": {"custom_audience_id": "555"}})
        result = conn.execute_action("create_customer_file_audience", "7001", {
            "name": "Buyers", "identifier_type": "email",
            "members": [" Jane@Acme.test ", "jane@acme.test", "bob@acme.test"]})
        (up_path, up_body, files), (cr_path, cr_body, _) = w.posts
        assert up_path == "dmp/custom_audience/file/upload/"
        assert up_body["calculate_type"] == "EMAIL_SHA256"
        content = files["file"][1].decode()
        expected = hashlib.sha256(b"jane@acme.test").hexdigest()
        assert content.splitlines()[0] == expected
        assert len(content.splitlines()) == 2           # deduplicated
        assert "jane@acme.test" not in content           # no raw PII
        assert up_body["file_signature"] == hashlib.md5(
            files["file"][1]).hexdigest()
        assert cr_body["file_paths"] == ["fp/1"]
        assert result.details["custom_audience_id"] == "555"

    def test_phone_needs_a_country_code(self):
        conn, _ = _writer()
        with pytest.raises(ApiError):
            conn.execute_action("create_customer_file_audience", "7001", {
                "name": "x", "identifier_type": "phone",
                "members": ["4155550100"]})

    def test_dry_run_uploads_nothing(self):
        conn, w = _writer()
        result = conn.execute_action("create_customer_file_audience", "7001", {
            "name": "x", "identifier_type": "phone",
            "members": ["+1 (415) 555-0100"]}, dry_run=True)
        assert w.posts == []
        assert "1 hashed" in result.summary

    def test_add_and_remove_members(self):
        reads = {"dmp/custom_audience/get/": {"list": [{"audience_details": {
            "audience_id": "555", "name": "Buyers", "cover_num": 10}}]}}
        conn, w = _writer(reads=reads, posts={
            "dmp/custom_audience/file/upload/": {"file_path": "fp/2"}})
        conn.execute_action("add_audience_members", "7001", {
            "custom_audience_id": "555", "identifier_type": "gaid",
            "members": ["ABC-123"]})
        conn.execute_action("remove_audience_members", "7001", {
            "custom_audience_id": "555", "identifier_type": "gaid",
            "members": ["ABC-123"]})
        updates = [b for p, b, _ in w.posts if p == "dmp/custom_audience/update/"]
        assert [u["action"] for u in updates] == ["APPEND", "REMOVE"]

    def test_lookalike_reads_the_seed(self):
        reads = {"dmp/custom_audience/get/": {"list": [
            {"custom_audience_id": "555", "name": "Buyers"}]}}
        conn, w = _writer(reads=reads, posts={
            "dmp/custom_audience/lookalike/create/": {"custom_audience_id": "556"}})
        conn.execute_action("create_lookalike_audience", "7001", {
            "name": "LAL", "source_audience_id": "555",
            "location_ids": ["6252001"], "audience_size": "NARROW",
            "mobile_os": "ALL", "placements": ["TikTok", "Pangle"],
            "include_source": False})
        spec = w.posts[0][1]["lookalike_spec"]
        assert spec["source_audience_id"] == "555"
        assert spec["audience_size"] == "NARROW"
        assert spec["mobile_os"] == "ALL"
        assert spec["placements"] == ["TikTok", "Pangle"]
        assert spec["include_source"] is False

    def test_lookalike_placements_use_tiktoks_exact_values(self):
        reads = {"dmp/custom_audience/get/": {"list": [
            {"custom_audience_id": "555", "name": "Buyers"}]}}
        conn, _ = _writer(reads=reads)
        with pytest.raises(ApiError):
            conn.execute_action("create_lookalike_audience", "7001", {
                "name": "LAL", "source_audience_id": "555",
                "location_ids": ["6252001"], "audience_size": "NARROW",
                "mobile_os": "ALL", "placements": ["PLACEMENT_TIKTOK"],
                "include_source": False})

    def test_delete_and_share(self):
        reads = {"dmp/custom_audience/get/": {"list": [
            {"custom_audience_id": "555", "name": "Buyers"}]}}
        conn, w = _writer(reads=reads)
        conn.execute_action("delete_audience", "7001",
                            {"custom_audience_id": "555"})
        conn.execute_action("share_audience", "7001", {
            "custom_audience_ids": ["555"], "shared_advertiser_ids": ["7002"]})
        assert [p for p, _, _ in w.posts] == ["dmp/custom_audience/delete/",
                                              "dmp/custom_audience/share/"]


class TestCommentRuleAndPixelActions:
    def test_reply_hide_unhide_delete(self):
        conn, w = _writer(posts={"comment/post/": {"comment_id": "r1"}})
        conn.execute_action("reply_to_comment", "7001", {
            "comment_id": "1", "ad_id": "31", "tiktok_item_id": "7300",
            "text": "Thanks!", "identity_id": "id1",
            "identity_type": "CUSTOMIZED_USER"})
        conn.execute_action("hide_comment", "7001", {"comment_ids": ["1", "2"]})
        conn.execute_action("unhide_comment", "7001", {"comment_id": "1"})
        conn.execute_action("delete_comment", "7001", {
            "comment_id": "5", "ad_id": "31", "tiktok_item_id": "7300",
            "identity_id": "id1", "identity_type": "CUSTOMIZED_USER"})
        paths = [p for p, _, _ in w.posts]
        assert paths == ["comment/post/", "comment/status/update/",
                         "comment/status/update/", "comment/delete/"]
        assert w.posts[1][1]["operation"] == "HIDDEN"
        assert w.posts[2][1]["operation"] == "PUBLIC"

    def test_reply_is_always_comment_type_reply(self):
        conn, w = _writer(posts={"comment/post/": {"comment_id": "r1"}})
        conn.execute_action("reply_to_comment", "7001", {
            "comment_id": "1", "ad_id": "31", "tiktok_item_id": "7300",
            "text": "x" * 300, "identity_id": "id1",
            "identity_type": "TT_USER"})
        assert w.posts[0][1]["comment_type"] == "REPLY"

    def test_reply_identity_must_be_custom_or_tiktok_user(self):
        conn, _ = _writer()
        with pytest.raises(ApiError):
            conn.execute_action("reply_to_comment", "7001", {
                "comment_id": "1", "ad_id": "31", "tiktok_item_id": "7300",
                "text": "hi", "identity_id": "id1",
                "identity_type": "AUTH_CODE"})

    def test_blocked_words(self):
        conn, w = _writer()
        conn.execute_action("add_blocked_words", "7001", {"words": ["scam"]})
        conn.execute_action("remove_blocked_words", "7001", {"words": ["scam"]})
        assert [p for p, _, _ in w.posts] == ["blockedword/create/",
                                              "blockedword/delete/"]

    def test_rules_lifecycle(self):
        conn, w = _writer(posts={"optimizer/rule/create/": {"rule_ids": ["41"]}})
        r = conn.execute_action("create_automated_rule", "7001",
                                {"rule": dict(_RULE)})
        conn.execute_action("update_automated_rule", "7001",
                            {"rule_id": "41",
                             "rule": {**_RULE, "name": "Renamed"}})
        for action, op in (("enable_automated_rule", "TURN_ON"),
                           ("disable_automated_rule", "TURN_OFF"),
                           ("delete_automated_rule", "DELETE")):
            conn.execute_action(action, "7001", {"rule_id": "41"})
            assert w.posts[-1][1]["operate_type"] == op
        assert r.details["rule_id"] == "41"
        assert w.posts[1][1]["rules"][0]["rule_id"] == "41"

    def test_rule_needs_a_name(self):
        conn, _ = _writer()
        with pytest.raises(ApiError):
            conn.execute_action("create_automated_rule", "7001",
                                {"rule": {"name": "only a name"}})

    def test_pixels(self):
        conn, w = _writer(posts={"pixel/create/": {"pixel_id": "61",
                                                   "pixel_code": "CABC"}})
        r = conn.execute_action("create_pixel", "7001", {"pixel_name": "Site"})
        conn.execute_action("update_pixel", "7001",
                            {"pixel_id": "61", "pixel_name": "Site 2"})
        assert "CABC" in r.summary
        assert w.posts[1] == ("pixel/update/", {"advertiser_id": "7001",
                                                "pixel_id": "61",
                                                "pixel_name": "Site 2"}, None)


class TestWriteErrors:
    def _refused(self, envelope):
        conn, _ = _writer(reads=_ENTITY_READS,
                          posts={"campaign/status/update/": envelope})
        with pytest.raises(ApiError) as exc:
            conn.execute_action("pause_campaign", "7001", {"campaign_id": "11"})
        return exc.value

    def test_missing_permission_explains_the_app_setup(self):
        err = self._refused({"code": 40001, "message": "No permission"})
        assert err.code == ErrorCode.ACCOUNT_FORBIDDEN
        assert "permission" in err.message

    def test_rejected_value_is_a_param_error(self):
        err = self._refused({"code": 40002, "message": "budget too low"})
        assert err.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "budget too low" in err.message

    def test_rate_limit_is_still_retriable(self):
        err = self._refused({"code": 40100, "message": "slow down"})
        assert err.code == ErrorCode.RATE_LIMITED

    def test_default_http_posts_json(self, monkeypatch):
        import requests
        seen = {}

        class _Resp:
            status_code = 200
            reason = "OK"
            def json(self): return _ok({})

        def fake_request(method, url, **kw):
            seen.update(method=method, **kw)
            return _Resp()

        monkeypatch.setattr(requests, "request", fake_request)
        tt_api._default_http("POST", "https://x/y", "tok", None,
                                 body={"a": 1})
        assert seen["json"] == {"a": 1}
        assert seen["headers"]["Access-Token"] == "tok"
        tt_api._default_http("POST", "https://x/y", "tok", None,
                                 body={"a": "1"}, files={"file": ("f", b"x")})
        assert seen["files"]["a"] == (None, "1")
        assert seen["files"]["file"] == ("f", b"x")
        # Upload by URL: multipart with no file part (TikTok's upload
        # endpoints take multipart/form-data).
        tt_api._default_http("POST", "https://x/y", "tok", None,
                                 body={"upload_type": "UPLOAD_BY_URL"},
                                 files={})
        assert seen["files"] == {"upload_type": (None, "UPLOAD_BY_URL")}


class TestCommentsReportNeedsAFilter:
    def test_without_ad_or_adgroup_it_is_refused(self):
        conn, rec = _connector(lambda p, q: _ok({"comments": []}))
        with pytest.raises(ApiError) as exc:
            conn.query(QuerySpec(accounts=["7001"], fields=["comment_id"],
                                 date_range=DateRange("2026-08-01", "2026-08-07"),
                                 report_type="Comments"))
        assert exc.value.code == ErrorCode.MISSING_SETTING
        assert rec.calls == []


class TestAdGroupUpdatableFields:
    @pytest.mark.parametrize("field,value", [
        ("placement_type", "PLACEMENT_TYPE_NORMAL"),
        ("optimization_goal", "CLICK"), ("pixel_id", "61"),
        ("billing_event", "CPC"), ("promotion_type", "WEBSITE")])
    def test_creation_only_fields_cannot_be_updated(self, field, value):
        conn, w = _writer(reads=_ENTITY_READS)
        with pytest.raises(ApiError) as exc:
            conn.execute_action("update_ad_group", "7001",
                                {"adgroup_id": "21", field: value})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert w.posts == []


# --- conformance with TikTok's official API reference -----------------------
#
# fixtures/tiktok_ads_official_v1_3.json lists, per endpoint, the request
# fields and required flags from TikTok's official API for Business v1.3
# reference (business-api.tiktok.com/portal/docs). Every request this
# connector sends must use only those fields and include the required ones,
# so a typo or an invented field fails here, not in production.

import os as _os

_FIXTURE = _os.path.join(_os.path.dirname(__file__), "fixtures",
                         "tiktok_ads_official_v1_3.json")
with open(_FIXTURE) as _f:
    _OFFICIAL = json.load(_f)["endpoints"]


_RULE = {"name": "Pause high CPA",
         "apply_objects": [{"dimension": "CAMPAIGN", "dimension_ids": ["11"],
                            "pre_condition_type": "SELECTED"}],
         "conditions": [{"subject_type": "CPA", "range_type": "TODAY",
                         "match_type": "GT", "values": ["20"]}],
         "actions": [{"subject_type": "TURN_OFF"}],
         "notification": {"notification_type": "NOT_NOTIFY"},
         "rule_exec_info": {"exec_time_type": "PER_HALF_HOUR"}}


def _every_action_call():
    """(action, params, reads, posts) exercising every write action."""
    aud_reads = {"dmp/custom_audience/get/": {"list": [
        {"custom_audience_id": "555", "name": "Buyers"}]}}
    reads = {**_ENTITY_READS, **aud_reads}
    posts = {"dmp/custom_audience/file/upload/": {"file_path": "fp/1"}}
    member = {"identifier_type": "email", "members": ["a@b.test"]}
    calls = [
        ("create_campaign", {"campaign_name": "c",
                             "objective_type": "APP_PROMOTION",
                             "budget_mode": "BUDGET_MODE_DAY", "budget": 60,
                             "special_industries": ["HOUSING"],
                             "budget_optimize_on": False,
                             "app_promotion_type": "APP_INSTALL",
                             "campaign_type": "REGULAR_CAMPAIGN"}),
        ("update_campaign", {"campaign_id": "11", "campaign_name": "x",
                             "budget": 70, "special_industries": ["CREDIT"]}),
        ("set_campaign_budget", {"campaign_id": "11", "budget": 80}),
        *[(a, {f"{k}_id": i}) for a, (k, i) in {
            "pause_campaign": ("campaign", "11"),
            "enable_campaign": ("campaign", "11"),
            "delete_campaign": ("campaign", "11"),
            "pause_ad_group": ("adgroup", "21"),
            "enable_ad_group": ("adgroup", "21"),
            "delete_ad_group": ("adgroup", "21"),
            "pause_ad": ("ad", "31"), "enable_ad": ("ad", "31"),
            "delete_ad": ("ad", "31")}.items()],
        ("create_ad_group", {
            "campaign_id": "11", "adgroup_name": "g", "budget": 30,
            "budget_mode": "BUDGET_MODE_TOTAL",
            "location_ids": ["6252001"], "optimization_goal": "CONVERT",
            "promotion_type": "WEBSITE", "placement_type": "PLACEMENT_TYPE_NORMAL",
            "placements": ["PLACEMENT_TIKTOK"], "schedule_type":
            "SCHEDULE_START_END", "schedule_start_time": "2026-12-01 00:00:00",
            "schedule_end_time": "2026-12-31 00:00:00", "dayparting": "1" * 336,
            "optimization_event": "ON_WEB_ORDER", "billing_event": "OCPM",
            "bid_type": "BID_TYPE_CUSTOM", "conversion_bid_price": 9,
            "roas_bid": 2, "bid_price": 1,
            "pacing": "PACING_MODE_FAST", "pixel_id": "61",
            "gender": "GENDER_FEMALE", "age_groups": ["AGE_18_24"],
            "languages": ["en"], "operating_systems": ["IOS"],
            "network_types": ["WIFI"], "interest_category_ids": ["1"],
            "interest_keyword_ids": ["2"], "audience_ids": ["555"],
            "excluded_audience_ids": ["556"]}),
        ("update_ad_group", {
            "adgroup_id": "21", "adgroup_name": "g2", "budget": 31,
            "location_ids": ["6252001"], "schedule_type": "SCHEDULE_FROM_NOW",
            "schedule_start_time": "2026-12-01 00:00:00",
            "schedule_end_time": "2026-12-31 00:00:00", "dayparting": "1" * 336,
            "bid_type": "BID_TYPE_CUSTOM", "bid_price": 2,
            "conversion_bid_price": 9, "deep_cpa_bid": 20, "roas_bid": 2,
            "pacing": "PACING_MODE_SMOOTH", "gender": "GENDER_MALE",
            "age_groups": ["AGE_25_34"], "languages": ["es"],
            "operating_systems": ["ANDROID"], "network_types": ["4G"],
            "interest_category_ids": ["1"], "interest_keyword_ids": ["2"],
            "audience_ids": ["555"], "excluded_audience_ids": ["556"],
            "frequency": 2, "frequency_schedule": 7}),
        ("set_ad_group_budget", {"adgroup_id": "21", "budget": 40}),
        ("set_ad_group_bid", {"adgroup_id": "21", "bid_price": 2}),
        ("set_ad_group_schedule", {"adgroup_id": "21",
                                   "schedule_end_time": "2026-12-31 00:00:00"}),
        ("set_ad_group_targeting", {"adgroup_id": "21", "gender": "GENDER_MALE"}),
        ("set_ad_group_audiences", {"adgroup_id": "21", "audience_ids": ["555"]}),
        ("create_ad", {
            "adgroup_id": "21", "ad_name": "a", "identity_type":
            "CUSTOMIZED_USER", "identity_id": "id1", "video_id": "v1",
            "image_ids": ["i1"], "ad_text": "t", "call_to_action": "SHOP_NOW",
            "landing_page_url": "https://a.test", "display_name": "Acme",
            "deeplink": "app://x", "deeplink_type": "NORMAL",
            "tracking_pixel_id": "61",
            "impression_tracking_url": "https://t.test/i",
            "click_tracking_url": "https://t.test/c", "card_id": "9",
            "page_id": "8"}),
        ("update_ad", {"ad_id": "31", "ad_text": "new"}),
        ("upload_image", {"image_url": "https://cdn.test/a.jpg",
                          "file_name": "a"}),
        ("upload_video", {"video_url": "https://cdn.test/a.mp4",
                          "file_name": "v"}),
        ("create_identity", {"display_name": "Acme", "image_uri": "i1"}),
        ("authorize_spark_post", {"auth_code": "#abc"}),
        ("create_customer_file_audience", {"name": "n", **member,
                                           "retention_in_days": 30}),
        ("add_audience_members", {"custom_audience_id": "555", **member}),
        ("remove_audience_members", {"custom_audience_id": "555", **member}),
        ("rename_audience", {"custom_audience_id": "555", "name": "m"}),
        ("create_lookalike_audience", {"name": "l", "source_audience_id": "555",
                                       "location_ids": ["6252001"],
                                       "audience_size": "BALANCED",
                                       "mobile_os": "ALL",
                                       "placements": ["TikTok"],
                                       "include_source": True}),
        ("create_rule_audience", {"name": "r", "audience_type": "ENGAGEMENT",
                                  "rule_spec": {"inclusion_rule_set": {}}}),
        ("delete_audience", {"custom_audience_id": "555"}),
        ("share_audience", {"custom_audience_ids": ["555"],
                            "shared_advertiser_ids": ["7002"]}),
        ("reply_to_comment", {"comment_id": "1", "ad_id": "31",
                              "tiktok_item_id": "7300", "text": "hi",
                              "identity_id": "id1",
                              "identity_type": "CUSTOMIZED_USER"}),
        ("hide_comment", {"comment_id": "1"}),
        ("unhide_comment", {"comment_id": "1"}),
        ("delete_comment", {"comment_id": "1", "ad_id": "31",
                            "tiktok_item_id": "7300", "identity_id": "id1",
                            "identity_type": "CUSTOMIZED_USER"}),
        ("add_blocked_words", {"words": ["x"]}),
        ("remove_blocked_words", {"words": ["x"]}),
        ("create_automated_rule", {"rule": dict(_RULE)}),
        ("update_automated_rule", {"rule_id": "41", "rule": dict(_RULE)}),
        ("enable_automated_rule", {"rule_id": "41"}),
        ("disable_automated_rule", {"rule_id": "41"}),
        ("delete_automated_rule", {"rule_id": "41"}),
        ("create_pixel", {"pixel_name": "p"}),
        ("update_pixel", {"pixel_id": "61", "pixel_name": "p"}),
    ]
    return calls, reads, posts


class TestOfficialSpecConformance:
    def test_every_action_is_exercised(self):
        calls, _, _ = _every_action_call()  # noqa: F841
        assert {a for a, _ in calls} == set(tt_actions._ACTIONS_BY_ID)

    def _run_all(self):
        calls, reads, posts = _every_action_call()
        conn, w = _writer(reads=reads, posts=posts)
        for action, params in calls:
            conn.execute_action(action, "7001", params)
        return w

    def test_every_endpoint_is_official_with_its_method(self):
        w = self._run_all()
        used = {(p, "POST") for p, _, _ in w.posts} | {(p, "GET")
                                                        for p, _ in w.gets}
        for path, method in used:
            assert path in _OFFICIAL, f"{path} is not an official endpoint"
            assert _OFFICIAL[path]["method"] == method, path

    def test_every_request_uses_only_official_fields(self):
        w = self._run_all()
        sent = [(p, {**b, **(f or {})}) for p, b, f in w.posts] + list(w.gets)
        for path, body in sent:
            spec = _OFFICIAL[path]
            extra = set(body) - set(spec["fields"])
            assert not extra, f"{path}: not in the official docs: {extra}"
            for key, nested in (spec.get("nested") or {}).items():
                value = body.get(key)
                if isinstance(value, str) and key == "filtering":
                    value = json.loads(value)
                items = value if isinstance(value, list) else [value]
                for item in items:
                    if isinstance(item, dict):
                        bad = set(item) - set(nested["fields"])
                        assert not bad, f"{path}.{key}: not official: {bad}"

    def test_every_request_sends_the_official_required_fields(self):
        w = self._run_all()
        sent = [(p, {**b, **(f or {})}) for p, b, f in w.posts] + list(w.gets)
        for path, body in sent:
            spec = _OFFICIAL[path]
            missing = set(spec["required"]) - set(body)
            assert not missing, f"{path}: missing required {missing}"
            for key, nested in (spec.get("nested") or {}).items():
                value = body.get(key)
                if isinstance(value, str) and key == "filtering":
                    continue
                items = value if isinstance(value, list) else [value]
                for item in items:
                    if isinstance(item, dict):
                        # On a creative update only ad_id is required.
                        lack = set(nested["required"]) - set(item)
                        assert not lack, f"{path}.{key}: missing {lack}"

    def test_enum_values_sent_are_official(self):
        w = self._run_all()
        for path, body, _ in w.posts:
            for key, value in body.items():
                allowed = tt_actions._ENUMS.get(key)
                if not allowed:
                    continue
                for v in (value if isinstance(value, list) else [value]):
                    assert v in allowed, (path, key, v)

    def test_audience_update_actions_use_official_enum(self):
        calls, reads, posts = _every_action_call()
        conn, w = _writer(reads=reads, posts=posts)
        for action, params in calls:
            if action.endswith("_audience_members"):
                conn.execute_action(action, "7001", params)
        actions = {b.get("action") for p, b, _ in w.posts
                   if p == "dmp/custom_audience/update/"}
        assert actions <= {"APPEND", "REMOVE", "REPLACE"}

    def test_nothing_is_left_unverified(self):
        assert tt_actions._UNVERIFIED_ENDPOINTS == frozenset()



class TestOfficialReportingCatalogue:
    def test_every_official_basic_metric_is_offered(self):
        from terno_dbi.connectors.api.sources.tiktok.ads import report_spec as rs
        fields = tt_fields._fields_for("Performance")
        for fid, _label, _group, _flags in rs.BASIC:
            assert fid in fields, fid
        assert len(rs.BASIC) == 1024
        for fid in ("onsite_shopping_roas", "offline_total_crm_events",
                    "skan_app_install", "native_growth_purchase_roas",
                    "engaged_view_15s", "vta_purchase", "billed_cost"):
            assert fields[fid].kind == "metric"

    def test_metric_flags_follow_official_ids(self):
        fields = tt_fields._fields_for("Performance")
        assert fields["spend"].is_monetary
        assert fields["cost_per_conversion"].is_monetary
        assert fields["cost_per_conversion"].is_non_aggregatable
        assert fields["ctr"].is_non_aggregatable
        assert not fields["impressions"].is_monetary
        assert not fields["impressions"].is_non_aggregatable
        assert not fields["total_active_pay_roas"].is_monetary

    def test_ad_level_attribute_uses_the_ad_id(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_spec(("ad_text", "campaign_name", "clicks")))
        p = rec.calls[0][1]
        assert p["data_level"] == "AUCTION_AD"
        assert json.loads(p["dimensions"]) == ["ad_id"]
        assert set(json.loads(p["metrics"])) == {"clicks", "ad_text",
                                                 "campaign_name"}

    def test_ad_id_v2_can_be_the_grouping_id(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_spec(("ad_id_v2", "spend")))
        p = rec.calls[0][1]
        assert json.loads(p["dimensions"]) == ["ad_id_v2"]
        assert p["data_level"] == "AUCTION_AD"

    def test_ad_id_and_ad_id_v2_together_are_refused(self):
        conn, _ = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError):
            conn.query(_spec(("ad_id", "ad_id_v2", "spend")))

    def test_single_level_attribute_is_enforced(self):
        conn, _ = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(("is_aco", "ad_name", "spend")))
        assert "AUCTION_ADGROUP" in exc.value.message

    def test_country_is_a_basic_dimension_now(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_spec(("country_code", "spend")))
        assert rec.calls[0][1]["report_type"] == "BASIC"

    def test_audience_only_dimension_switches_and_checks_metrics(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_spec(("interest_category", "spend", "impressions")))
        assert rec.calls[0][1]["report_type"] == "AUDIENCE"
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(("gender", "onsite_shopping_roas")))
        assert "onsite_shopping_roas" in exc.value.message

    def test_basic_only_dimensions_pass_through(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_spec(("search_terms", "campaign_id", "spend")))
        p = rec.calls[0][1]
        assert json.loads(p["dimensions"]) == ["campaign_id", "search_terms"]
        assert p["report_type"] == "BASIC"

    def test_include_deleted_lifts_the_level_status_filter(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(QuerySpec(accounts=["7001"], fields=["adgroup_id", "spend"],
                             date_range=DateRange("2026-08-01", "2026-08-07"),
                             report_type="Performance",
                             settings={"include_deleted": "true"}))
        f = json.loads(rec.calls[0][1]["filtering"])
        assert f == [{"field_name": "adgroup_status", "filter_type": "IN",
                      "filter_value": json.dumps(["STATUS_ALL"])}]

    def test_lifetime_ignores_dates(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(QuerySpec(accounts=["7001"], fields=["campaign_id", "spend"],
                             date_range=DateRange("2020-01-01", "2026-08-07"),
                             report_type="Performance",
                             settings={"query_lifetime": "true"}))
        p = rec.calls[0][1]
        assert p["query_lifetime"] == "true"
        assert "start_date" not in p and len(rec.calls) == 1

    def test_lifetime_cannot_be_split_by_date(self):
        conn, _ = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError):
            conn.query(QuerySpec(accounts=["7001"], fields=["date", "spend"],
                                 date_range=DateRange("2026-08-01",
                                                      "2026-08-07"),
                                 report_type="Performance",
                                 settings={"query_lifetime": "true"}))


class TestTypedReports:
    def test_playable_report(self):
        conn, rec = _connector(lambda p, q: _report([
            {"dimensions": {"playable_id": "pl1"},
             "metrics": {"playable_cta_click": "5"}}]))
        result = conn.query(_spec(("playable_id", "playable_cta_click"),
                                  report_type="PlayableAds"))
        p = rec.calls[0][1]
        assert p["report_type"] == "PLAYABLE_MATERIAL"
        assert "data_level" not in p
        assert result.rows == [{"playable_id": "pl1",
                                "playable_cta_click": 5}]

    def test_dsa_report_takes_a_data_level(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_spec(("campaign_id", "product_id", "spend"),
                         report_type="DSA"))
        p = rec.calls[0][1]
        assert p["report_type"] == "CATALOG"
        assert p["data_level"] == "AUCTION_CAMPAIGN"

    def test_bc_report_uses_bc_id_and_no_service_type(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(QuerySpec(accounts=["7001"],
                             fields=["advertiser_id", "spend"],
                             date_range=DateRange("2026-08-01", "2026-08-07"),
                             report_type="BusinessCenter",
                             settings={"bc_id": "bc9"}))
        p = rec.calls[0][1]
        assert p["report_type"] == "BC" and p["bc_id"] == "bc9"
        assert "service_type" not in p and "advertiser_id" not in p

    def test_typed_report_needs_a_dimension(self):
        conn, _ = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError):
            conn.query(_spec(("spend",), report_type="PlayableAds"))


class TestOfficialGroupingRules:
    """TikTok's dimension-grouping and metric-compatibility rules
    (report_rules) are enforced before any call."""

    def test_every_rule_metric_is_an_official_metric(self):
        from terno_dbi.connectors.api.sources.tiktok.ads import report_rules as rr
        from terno_dbi.connectors.api.sources.tiktok.ads import report_spec as rs
        basic = {b[0] for b in rs.BASIC}
        aud = {e if isinstance(e, str) else e[0] for e in rs.AUDIENCE}
        bc = {e if isinstance(e, str) else e[0] for e in rs.BC}
        for table in (*rr.BASIC_DIM_METRICS.values(),
                      *rr.BASIC_DIM_EXCLUDED.values()):
            assert table <= basic, sorted(table - basic)
        assert set(rr.BASIC_METRIC_RULES) <= basic
        for table in rr.AUDIENCE_DIM_METRICS.values():
            assert table <= aud, sorted(table - aud)
        for table in rr.BC_DIM_METRICS.values():
            assert table <= bc, sorted(table - bc)
        for d in (*rr.BASIC_GROUPINGS, *rr.AUDIENCE_GROUPINGS):
            assert d in rs.DIMENSIONS["BASIC"] or d in rs.DIMENSIONS["AUDIENCE"]

    def test_official_examples(self):
        from terno_dbi.connectors.api.sources.tiktok.ads import report_rules as rr
        for ok in (["campaign_id"], ["stat_time_day"],
                   ["campaign_id", "stat_time_day"],
                   ["campaign_id", "stat_time_hour"],
                   ["campaign_id", "country_code"],
                   ["ad_id", "stat_time_day", "country_code"]):
            assert rr.grouping_error("BASIC", ok) is None, ok
        assert rr.grouping_error("BASIC", ["stat_time_hour", "country_code"])
        assert rr.grouping_error("AUDIENCE",
                                 ["stat_time_day", "age", "gender"]) is None
        assert rr.grouping_error("AUDIENCE", ["age", "language"])
        assert rr.grouping_error("AUDIENCE", ["advertiser_id", "ad_type"])
        assert rr.grouping_error("AUDIENCE",
                                 ["interest_category", "stat_time_day"])
        assert rr.grouping_error("BC", ["advertiser_id", "placement"]) is None
        assert rr.grouping_error("BC", ["stat_time_day", "placement"])

    def test_hourly_country_split_is_refused(self):
        conn, rec = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(("hour", "country_code", "spend")))
        assert "date" in exc.value.message and not rec.calls

    def test_asset_dimension_drops_the_id_column(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_spec(("page_id", "component_name", "clicks")))
        assert json.loads(rec.calls[0][1]["dimensions"]) == [
            "page_id", "component_name"]

    def test_asset_dimension_with_attributes_is_refused(self):
        conn, rec = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError):
            conn.query(_spec(("room_id", "campaign_name", "spend")))
        assert not rec.calls

    def test_search_terms_not_at_ad_level(self):
        conn, _ = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError):
            conn.query(_spec(("search_terms", "ad_name", "clicks")))

    def test_dimension_metric_table(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_spec(("image_id", "clicks", "single_image_ctr")))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(("image_id", "spend")))
        assert "spend" in exc.value.message
        with pytest.raises(ApiError):
            conn.query(_spec(("video_material_id", "reach")))
        conn.query(_spec(("video_material_id", "spend")))

    def test_category_rule_offline_needs_an_id_and_no_hour(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_spec(("date", "offline_total_crm_events")))
        assert json.loads(rec.calls[-1][1]["dimensions"]) == [
            "advertiser_id", "stat_time_day"]
        with pytest.raises(ApiError):
            conn.query(_spec(("hour", "offline_total_crm_events")))
        with pytest.raises(ApiError):
            conn.query(_spec(("ad_type", "offline_total_crm_events")))

    def test_dimension_table_overrides_conflicting_metric_note(self):
        # country_code's table lists purchase; the Purchase note alone would
        # refuse it — conflicting official statements are allowed.
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_spec(("country_code", "purchase")))
        assert rec.calls

    def test_audience_reach_only_with_listed_dimensions(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_spec(("age", "reach")))
        with pytest.raises(ApiError):
            conn.query(_spec(("platform", "reach")))

    def test_bc_metric_table(self):
        conn, rec = _connector(lambda p, q: _report([]))
        spec = lambda f: QuerySpec(  # noqa: E731
            accounts=["7001"], fields=list(f),
            date_range=DateRange("2026-08-01", "2026-08-07"),
            report_type="BusinessCenter", settings={"bc_id": "bc9"})
        conn.query(spec(("advertiser_id", "placement", "spend")))
        with pytest.raises(ApiError):
            conn.query(spec(("advertiser_id", "placement", "reach")))
        with pytest.raises(ApiError):
            conn.query(spec(("stat_time_day", "placement", "spend")))


def _gmv_spec(report_type, fields, start="2026-08-01", end="2026-08-07",
              **settings):
    return QuerySpec(accounts=["7001"], fields=list(fields),
                     date_range=DateRange(start, end),
                     report_type=report_type,
                     settings={"store_id": "s1", **settings})


class TestGmvMaxReports:
    """gmv_max/report/get/ — official levels, groupings and filters."""

    def test_every_level_is_declared_with_its_official_settings(self):
        from terno_dbi.catalog.declarations import get_spec
        from terno_dbi.connectors.api.sources.tiktok.ads import gmv_max_spec as g
        types = {r.id: r for r in get_spec("tiktok_ads").report_types}
        for rid, rep in g.REPORTS.items():
            settings = {s.setting_id: s.required for s in types[rid].settings}
            assert settings["store_id"] is True
            for k in rep.required:
                assert settings[k] is True, (rid, k)
            for k in rep.optional:
                assert settings[k] is False, (rid, k)
            assert ("sort_field" in settings) == bool(rep.sort_fields)

    def test_product_campaign_request(self):
        conn, rec = _connector(lambda p, q: _report([
            {"dimensions": {"campaign_id": "c1",
                            "stat_time_day": "2026-08-01 00:00:00"},
             "metrics": {"campaign_name": "Shop", "cost": "12.5",
                         "roi": "3.2"}}]))
        result = conn.query(_gmv_spec(
            "GmvMaxProductCampaigns",
            ("campaign_id", "date", "campaign_name", "cost", "roi")))
        path, p = rec.calls[0]
        assert path == "gmv_max/report/get/"
        assert json.loads(p["store_ids"]) == ["s1"]
        assert json.loads(p["dimensions"]) == ["campaign_id", "stat_time_day"]
        assert json.loads(p["metrics"]) == ["campaign_name", "cost", "roi"]
        assert json.loads(p["filtering"]) == {
            "gmv_max_promotion_types": ["PRODUCT"]}
        assert p["page_size"] == 1000
        assert result.rows == [{"campaign_id": "c1", "date": "2026-08-01",
                                "campaign_name": "Shop", "cost": 12.5,
                                "roi": 3.2}]

    def test_store_id_is_required(self):
        conn, _ = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError) as exc:
            conn.query(QuerySpec(accounts=["7001"], fields=["cost"],
                                 date_range=DateRange("2026-08-01",
                                                      "2026-08-07"),
                                 report_type="GmvMaxOverview"))
        assert exc.value.code == ErrorCode.MISSING_SETTING

    def test_required_filters_and_default_grouping(self):
        conn, rec = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError) as exc:
            conn.query(_gmv_spec("GmvMaxProducts", ("cost",)))
        assert exc.value.code == ErrorCode.MISSING_SETTING
        conn.query(_gmv_spec("GmvMaxProducts", ("cost",),
                             campaign_ids="c1,c2"))
        p = rec.calls[-1][1]
        assert json.loads(p["dimensions"]) == ["item_group_id"]
        assert json.loads(p["filtering"]) == {"campaign_ids": ["c1", "c2"]}

    def test_creative_groupings(self):
        conn, rec = _connector(lambda p, q: _report([]))
        ok = dict(campaign_ids="c1", item_group_ids="g1")
        conn.query(_gmv_spec("GmvMaxCreatives",
                             ("item_id", "date", "cost"), **ok))
        # ["item_id"] alone is the endpoint's own official example.
        conn.query(_gmv_spec("GmvMaxCreatives", ("item_id", "cost"), **ok))
        with pytest.raises(ApiError):   # item_id + campaign_id isn't listed
            conn.query(_gmv_spec("GmvMaxCreatives",
                                 ("item_id", "campaign_id", "cost"), **ok))
        with pytest.raises(ApiError):   # no hourly creative split
            conn.query(_gmv_spec("GmvMaxCreatives",
                                 ("item_id", "hour", "cost"), **ok))

    def test_filter_limits_and_enums(self):
        conn, _ = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError):
            conn.query(_gmv_spec("GmvMaxProducts", ("cost",),
                                 campaign_ids=",".join(map(str, range(101)))))
        with pytest.raises(ApiError):
            conn.query(_gmv_spec("GmvMaxLiveCampaigns", ("cost",),
                                 campaign_statuses="RUNNING"))
        with pytest.raises(ApiError):
            conn.query(_gmv_spec("GmvMaxLiveCampaigns", ("cost",),
                                 sort_field="product_clicks"))

    def test_attributes_below_campaign_level_need_one_id(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_gmv_spec("GmvMaxProducts", ("product_name", "cost"),
                             campaign_ids="c1"))
        with pytest.raises(ApiError):
            conn.query(_gmv_spec("GmvMaxProducts", ("product_name", "cost"),
                                 campaign_ids="c1,c2"))
        with pytest.raises(ApiError):
            conn.query(_gmv_spec("GmvMaxProducts",
                                 ("item_group_id", "campaign_id",
                                  "product_name"), campaign_ids="c1"))

    def test_date_spans(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_gmv_spec("GmvMaxOverview", ("date", "cost"),
                             start="2026-06-01", end="2026-08-09"))
        assert len(rec.calls) == 3          # 30-day windows
        with pytest.raises(ApiError):
            conn.query(_gmv_spec("GmvMaxOverview", ("cost",),
                                 start="2025-01-01", end="2026-08-09"))

    def test_store_and_campaign_listings(self):
        conn, rec = _connector(lambda p, q: _ok({"store_list": [
            {"store_id": "s1", "is_gmv_max_available": True,
             "store_authorized_bc_info": {"bc_name": "BC"},
             "exclusive_authorized_advertiser_info": {
                 "advertiser_id": "7001"}}]}) if p.startswith("gmv_max/store")
            else _report([{"campaign_id": "c1"}]))
        r = conn.query(QuerySpec(
            accounts=["7001"], report_type="GmvMaxStores", date_range=None,
            fields=["store_id", "is_gmv_max_available", "bc_name",
                    "exclusive_advertiser_id"]))
        assert r.rows == [{"store_id": "s1", "is_gmv_max_available": True,
                           "bc_name": "BC", "exclusive_advertiser_id": "7001"}]
        with pytest.raises(ApiError) as exc:
            conn.query(QuerySpec(accounts=["7001"], date_range=None,
                                 report_type="GmvMaxCampaigns",
                                 fields=["campaign_id"]))
        assert exc.value.code == ErrorCode.MISSING_SETTING
        conn.query(QuerySpec(accounts=["7001"], report_type="GmvMaxCampaigns",
                             date_range=None,
                             fields=["campaign_id"], settings={
                                 "gmv_max_promotion_types": "LIVE_GMV_MAX"}))
        path, p = rec.calls[-1]
        assert path == "gmv_max/campaign/get/"
        assert json.loads(p["filtering"]) == {
            "gmv_max_promotion_types": ["LIVE_GMV_MAX"]}


class _AsyncTikTok:
    """Fakes report/task/create|check|download|cancel."""

    def __init__(self, statuses, csv_text="campaign_id,spend\n11,4.5\n"):
        self.statuses = list(statuses)
        self.csv_text = csv_text
        self.calls = []

    def __call__(self, method, url, token, params=None, *, body=None,
                 files=None):
        path = url.split("/open_api/v1.3/", 1)[1]
        self.calls.append((method, path, body if method == "POST"
                           else dict(params or {})))
        if path == "report/task/create/":
            return _ok({"task_id": "t1"})
        if path == "report/task/check/":
            return _ok({"status": self.statuses.pop(0)})
        if path == "report/task/cancel/":
            return _ok({"status": "CANCELED"})
        if path == "report/task/download/":
            return {"code": 0, "message": "OK",
                    "data": {tt_api._RAW_TEXT: self.csv_text}}
        raise AssertionError(path)


class TestAsyncReports:
    def _conn(self, fake):
        conn = TikTokAdsConnector(_DS(), http=fake)
        conn._sleep = lambda s: None
        return conn

    def _spec(self, fields, **settings):
        return QuerySpec(accounts=["7001"], fields=list(fields),
                         date_range=DateRange("2024-01-01", "2026-08-07"),
                         report_type="Performance",
                         settings={"async_report": "true", **settings})

    def test_create_check_download(self):
        fake = _AsyncTikTok(["QUEUING", "PROCESSING", "SUCCESS"])
        result = self._conn(fake).query(
            self._spec(("campaign_name", "spend")))
        create = fake.calls[0]
        assert create[:2] == ("POST", "report/task/create/")
        body = create[2]
        assert body["advertiser_id"] == "7001"
        assert body["report_type"] == "BASIC"
        assert body["data_level"] == "AUCTION_CAMPAIGN"
        assert body["dimensions"] == ["campaign_id"]
        assert body["start_date"] == "2024-01-01"   # no 365-day limit
        assert body["enable_report_title_translation"] is False
        assert body["output_format"] == "CSV_STRING"
        assert [c[1] for c in fake.calls].count("report/task/check/") == 3
        assert fake.calls[-1][1] == "report/task/download/"
        assert result.rows == [{"campaign_name": None, "spend": 4.5}]

    def test_failed_task(self):
        fake = _AsyncTikTok(["FAILED"])
        with pytest.raises(ApiError) as exc:
            self._conn(fake).query(self._spec(("spend",)))
        assert "t1" in exc.value.message

    def test_timeout_cancels_the_task(self, monkeypatch):
        monkeypatch.setattr(tt_reports, "_ASYNC_MAX_WAIT", 3.0)
        fake = _AsyncTikTok(["PROCESSING"] * 10)
        with pytest.raises(ApiError) as exc:
            self._conn(fake).query(self._spec(("spend",)))
        assert exc.value.retriable
        assert ("POST", "report/task/cancel/",
                {"advertiser_id": "7001", "task_id": "t1"}) in fake.calls

    def test_official_async_limits(self):
        conn = self._conn(_AsyncTikTok(["SUCCESS"]))
        for fields, settings in (
                (("billed_cost",), {}),              # sync-only metric
                (("spend",), {"include_deleted": "true",
                              "query_lifetime": "false"}),
                (("gender", "reach"), {}),           # UV x audience
                (("minis_id", "spend"), {})):        # sync-only dimension
            with pytest.raises(ApiError):
                conn.query(self._spec(fields, **settings))

    def test_country_name_known_issue(self):
        fake = _AsyncTikTok(["SUCCESS"],
                            csv_text="country_name,spend\nUnited States,2\n")
        result = self._conn(fake).query(self._spec(("country_code", "spend")))
        assert result.rows[0]["country_code"] == "United States"
        assert any("country names" in w for w in result.warnings)

    def test_advertiser_only_metric_refused_below_advertiser(self):
        conn, _ = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError):
            conn.query(_spec(("campaign_name", "cash_spend")))



class TestReviewFixes:
    def test_text_metrics_are_not_attributes(self):
        conn, rec = _connector(lambda p, q: _report([]))
        conn.query(_gmv_spec("GmvMaxCreatives",
                             ("item_id", "creative_delivery_status", "cost"),
                             campaign_ids="c1", item_group_ids="g1,g2"))
        assert json.loads(rec.calls[-1][1]["metrics"]) == [
            "creative_delivery_status", "cost"]

    def test_non_json_body_is_an_error_except_async_download(self, monkeypatch):
        import requests

        class _Resp:
            status_code, reason, text = 200, "OK", "<html>maintenance</html>"

            def json(self):
                raise ValueError("no json")

        monkeypatch.setattr(requests, "request", lambda *a, **k: _Resp())
        with pytest.raises(ApiError):
            tt_api._default_http(
                "GET", "https://x/open_api/v1.3/campaign/get/", "tok", {})
        out = tt_api._default_http(
            "GET", "https://x/open_api/v1.3/report/task/download/", "tok", {})
        assert out["data"][tt_api._RAW_TEXT].startswith("<html>")

    def test_string_filter_given_a_list(self):
        assert tt_listings._filter_from_settings(
            "R", {"campaign_name": ["Spring"]},
            [("campaign_name", "str", None, (), False)]) == {
                "campaign_name": "Spring"}
        with pytest.raises(ApiError):
            tt_listings._filter_from_settings(
                "R", {"campaign_name": ["a", "b"]},
                [("campaign_name", "str", None, (), False)])
