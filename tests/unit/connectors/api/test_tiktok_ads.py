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
from terno_dbi.connectors.api.sources import tiktok_ads
from terno_dbi.connectors.api.sources.tiktok_ads import TikTokAdsConnector


class _Catalog:
    key = "tiktok_ads"
    report_types = [
        {"id": "Performance", "settings": []},
        {"id": "Campaigns", "settings": []},
        {"id": "AdGroups", "settings": []},
        {"id": "Ads", "settings": []},
    ]
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

    def test_entity_field_is_not_valid_on_the_report(self):
        conn, _ = _connector(lambda p, q: _report([]))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(("objective_type",)))
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
            raise tiktok_ads._AuthError()

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
        tiktok_ads._default_http("GET", "https://x/y", "tok", {"a": 1})
        assert seen["headers"] == {"Access-Token": "tok"}
        assert seen["params"] == {"a": 1}


class TestCatalogSpec:
    def test_declared_report_types_match_the_connector(self):
        from terno_dbi.catalog.declarations import get_spec
        spec = get_spec("tiktok_ads")
        declared = {r.id for r in spec.report_types}
        assert declared == {tiktok_ads._DEFAULT_REPORT, *tiktok_ads._ENTITY_ENDPOINT}
        assert spec.default_report_type == tiktok_ads._DEFAULT_REPORT

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
        from terno_dbi.connectors.api.sources.tiktok_ads import (
            make_tiktok_ads_connector,
        )
        conn = make_tiktok_ads_connector(_DS())
        assert conn._token_refresher is not None
