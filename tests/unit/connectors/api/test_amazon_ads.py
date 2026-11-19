"""The Amazon Ads connector, against mocked profiles + async reporting v3.

The mocks mirror the real shapes — `GET /v2/profiles` per region, the async
`POST /reporting/reports` → poll → gzipped-JSON download — so parsing, region
routing, and the async lifecycle are exercised without a live provider.
"""

import gzip
import json

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.sources.amazon_ads import AmazonAdsConnector
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec


class _Catalog:
    key = "amazon_ads"
    report_types = [{"id": "spCampaigns", "settings": []}]
    has_report_types = True


class _DS:
    type = "amazon_ads"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok"}


PROFILES_NA = [
    {"profileId": 111, "countryCode": "US", "currencyCode": "USD",
     "accountInfo": {"name": "Acme US", "marketplaceStringId": "ATVPDKIKX0DER",
                     "type": "seller"}},
]
PROFILES_EU = [
    {"profileId": 222, "countryCode": "DE", "currencyCode": "EUR",
     "accountInfo": {"name": "Acme DE", "type": "seller"}},
]

REPORT_ROWS = [
    {"campaignId": "c1", "campaignName": "Brand", "impressions": 1000,
     "clicks": 50, "cost": 25.5, "sales7d": 300.0, "purchases7d": 12,
     "date": "2026-08-01"},
    {"campaignId": "c2", "campaignName": "Generic", "impressions": 800,
     "clicks": 20, "cost": 10.0, "sales7d": 90.0, "purchases7d": 3,
     "date": "2026-08-01"},
]


@pytest.fixture(autouse=True)
def _client_id(monkeypatch):
    monkeypatch.setenv("TERNO_AMAZON_ADS_CLIENT_ID", "amzn-client")


@pytest.fixture(autouse=True)
def _fresh_cache():
    from django.core.cache import cache
    cache.clear()
    yield
    cache.clear()


def _gzip_rows(rows):
    return gzip.compress(json.dumps(rows).encode("utf-8"))


def _connector(*, profiles=None, create=None, status_seq=None, rows=REPORT_ROWS,
               capture=None):
    """Build a connector with a scripted HTTP + download."""
    profiles = profiles or {"advertising-api.amazon.com": PROFILES_NA,
                            "advertising-api-eu.amazon.com": PROFILES_EU,
                            "advertising-api-fe.amazon.com": []}
    status_seq = list(status_seq or [{"status": "COMPLETED",
                                      "url": "https://s3/report.gz"}])

    def http(method, url, headers, body=None):
        assert headers["Amazon-Advertising-API-ClientId"] == "amzn-client"
        assert headers["Authorization"] == "Bearer tok"
        if "/v2/profiles" in url:
            for host, profs in profiles.items():
                if host in url:
                    return profs
            return []
        if method == "POST" and url.endswith("/reporting/reports"):
            if capture is not None:
                capture["body"] = body
                capture["scope"] = headers.get("Amazon-Advertising-API-Scope")
                capture["host"] = url
            return create or {"reportId": "r1", "status": "PENDING"}
        if "/reporting/reports/" in url:
            return status_seq.pop(0) if len(status_seq) > 1 else status_seq[0]
        raise AssertionError(f"unexpected call {method} {url}")

    def download(url):
        return _gzip_rows(rows)

    return AmazonAdsConnector(_DS(), http=http, download=download,
                              sleep=lambda *_a: None)


class TestListAccounts:
    def test_profiles_across_regions_with_region_tagged(self):
        conn = _connector()
        accounts = conn.list_accounts()
        by_id = {a.id: a for a in accounts}
        assert set(by_id) == {"111", "222"}
        assert by_id["111"].extra["region"] == "NA"
        assert by_id["222"].extra["region"] == "EU"
        assert by_id["111"].currency == "USD"

    def test_unauthorized_region_is_skipped(self):
        def http(method, url, headers, body=None):
            if "advertising-api-eu" in url:
                raise ApiError(ErrorCode.UPSTREAM_ERROR, "no access")
            if "/v2/profiles" in url:
                return PROFILES_NA if "advertising-api.amazon" in url else []
            raise AssertionError(url)
        conn = AmazonAdsConnector(_DS(), http=http, download=lambda u: b"",
                                  sleep=lambda *_a: None)
        assert {a.id for a in conn.list_accounts()} == {"111"}


class TestListFields:
    def test_catalogue_per_report_type(self):
        conn = _connector()
        camp = {f.id: f for f in conn.list_fields("spCampaigns")}
        assert camp["cost"].is_monetary is True
        assert camp["acosClicks14d"].is_non_aggregatable is True
        assert camp["clicks"].kind == "metric"
        st = {f.id for f in conn.list_fields("spSearchTerm")}
        assert "searchTerm" in st and "searchTerm" not in camp
        assert {"acosClicks7d", "acosClicks14d"} <= st

    def test_campaign_columns_are_ones_amazon_accepts(self):
        # From Amazon's 400 reply listing the allowed spCampaigns columns.
        allowed = set("""
            attributedSalesSameSku1d date campaignBiddingStrategy roasClicks14d
            unitsSoldClicks1d endDate attributedSalesSameSku7d
            attributedSalesSameSku14d royaltyQualifiedBorrows sales1d sales7d
            addToList attributedSalesSameSku30d purchasesSameSku14d
            kindleEditionNormalizedPagesRoyalties14d purchasesSameSku1d spend
            unitsSoldSameSku1d purchases1d purchasesSameSku7d unitsSoldSameSku7d
            purchases7d unitsSoldSameSku30d cost costPerClick unitsSoldClicks14d
            retailer sales14d sales30d clickThroughRate impressions
            kindleEditionNormalizedPagesRead14d purchasesSameSku30d purchases14d
            unitsSoldClicks30d qualifiedBorrows acosClicks14d purchases30d clicks
            startDate unitsSoldClicks7d unitsSoldSameSku14d
            campaignRuleBasedBudgetAmount campaignBudgetCurrencyCode campaignId
            campaignApplicableBudgetRuleId campaignBudgetType
            topOfSearchImpressionShare campaignStatus campaignName
            campaignApplicableBudgetRuleName campaignBudgetAmount
        """.split())
        ids = {f.id for f in _connector().list_fields("spCampaigns")}
        assert ids <= allowed, ids - allowed


class TestRunReport:
    def _spec(self, fields=("date", "campaignName", "cost", "sales7d"),
              accounts=("111",)):
        return QuerySpec(
            accounts=list(accounts), fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="spCampaigns",
        )

    def test_async_lifecycle_and_parse(self):
        conn = _connector(status_seq=[
            {"status": "PROCESSING"},
            {"status": "COMPLETED", "url": "https://s3/report.gz"},
        ])
        result = conn.query(self._spec())
        assert result.row_count == 2
        assert result.rows[0] == {"date": "2026-08-01", "campaignName": "Brand",
                                  "cost": 25.5, "sales7d": 300.0}

    def test_request_body_shape(self):
        cap = {}
        conn = _connector(capture=cap)
        conn.query(self._spec(fields=("date", "campaignName", "cost")))
        cfg = cap["body"]["configuration"]
        assert cfg["adProduct"] == "SPONSORED_PRODUCTS"
        assert cfg["reportTypeId"] == "spCampaigns"
        assert cfg["timeUnit"] == "DAILY"          # date requested
        assert cfg["format"] == "GZIP_JSON"
        assert cap["scope"] == "111"               # profile id in scope header

    def test_summary_when_no_date_requested(self):
        cap = {}
        conn = _connector(capture=cap)
        conn.query(self._spec(fields=("campaignName", "cost")))
        assert cap["body"]["configuration"]["timeUnit"] == "SUMMARY"
        assert "date" not in cap["body"]["configuration"]["columns"]

    def test_query_routes_to_the_profiles_region(self):
        cap = {}
        conn = _connector(capture=cap)
        conn.query(self._spec(accounts=("222",)))   # EU profile
        assert "advertising-api-eu.amazon.com" in cap["host"]

    def test_unknown_field_rejected(self):
        conn = _connector()
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec(fields=("campaignName", "coooost")))
        assert exc.value.code == ErrorCode.INVALID_FIELD

    def test_failed_report_surfaces_error(self):
        conn = _connector(status_seq=[{"status": "FAILURE",
                                       "statusDetails": "bad range"}])
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec())
        assert "Amazon report failed" in exc.value.message


class _Clock:
    """A fake monotonic clock that only moves when the connector sleeps."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def _resp(status, payload=None, headers=None):
    class _R:
        status_code = status
        text = json.dumps(payload or {})
        content = text.encode()

        def json(self):
            return payload or {}
    r = _R()
    r.headers = headers or {}
    return r


class TestReportWaiting:
    def _spec(self, accounts=("111",)):
        return QuerySpec(
            accounts=list(accounts), fields=["campaignName", "clicks"],
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="spCampaigns",
        )

    def _connector(self, http, clock):
        return AmazonAdsConnector(_DS(), http=http,
                                  download=lambda u: _gzip_rows(REPORT_ROWS),
                                  sleep=clock.sleep, clock=clock)

    def _http(self, *, on_status, on_create=None):
        def http(method, url, headers, body=None):
            if "/v2/profiles" in url:
                if "advertising-api.amazon.com" in url:
                    return PROFILES_NA
                if "advertising-api-eu" in url:
                    return PROFILES_EU
                return []
            if method == "POST":
                if on_create:
                    return on_create(headers["Amazon-Advertising-API-Scope"])
                return {"reportId": f"r-{headers['Amazon-Advertising-API-Scope']}"}
            return on_status(url.rsplit("/", 1)[-1])
        return http

    def test_times_out_with_a_retriable_error_naming_the_report(self, monkeypatch):
        monkeypatch.setenv("TERNO_AMAZON_ADS_REPORT_WAIT_SECONDS", "60")
        clock = _Clock()
        conn = self._connector(self._http(on_status=lambda rid: {"status": "PENDING"}),
                               clock)
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec())
        err = exc.value
        assert err.code == ErrorCode.TIMEOUT and err.retriable
        assert err.retry_after_seconds == 120
        assert err.details == {"profile": "111", "report_id": "r-111"}
        assert clock.now == pytest.approx(60)       # never sleeps past the budget

    def test_polling_backs_off_to_a_cap(self, monkeypatch):
        monkeypatch.setenv("TERNO_AMAZON_ADS_REPORT_WAIT_SECONDS", "120")
        clock = _Clock()
        conn = self._connector(self._http(on_status=lambda rid: {"status": "PENDING"}),
                               clock)
        with pytest.raises(ApiError):
            conn.query(self._spec())
        assert clock.sleeps[:5] == pytest.approx([2.0, 3.0, 4.5, 6.75, 10.125])
        assert max(clock.sleeps) == 15.0

    def test_duplicate_request_resumes_the_existing_report(self):
        from terno_dbi.connectors.api.sources.amazon_ads import _DuplicateReport
        clock = _Clock()
        polled = []

        def on_create(scope):
            raise _DuplicateReport("existing-report")

        def on_status(report_id):
            polled.append(report_id)
            return {"status": "COMPLETED", "url": "https://s3/r.gz"}

        conn = self._connector(self._http(on_status=on_status, on_create=on_create),
                               clock)
        result = conn.query(self._spec())
        assert polled == ["existing-report"]
        assert result.row_count == 2

    def test_throttled_status_checks_keep_waiting(self):
        clock = _Clock()
        calls = []

        def on_status(report_id):
            calls.append(report_id)
            if len(calls) == 1:
                raise ApiError(ErrorCode.RATE_LIMITED, "slow down")
            return {"status": "COMPLETED", "url": "https://s3/r.gz"}

        conn = self._connector(self._http(on_status=on_status), clock)
        assert conn.query(self._spec()).row_count == 2
        assert len(calls) == 2

    def test_slow_profile_is_a_warning_when_others_finish(self, monkeypatch):
        monkeypatch.setenv("TERNO_AMAZON_ADS_REPORT_WAIT_SECONDS", "30")
        clock = _Clock()

        def on_status(report_id):
            if report_id == "r-111":
                return {"status": "COMPLETED", "url": "https://s3/r.gz"}
            return {"status": "PROCESSING"}

        conn = self._connector(self._http(on_status=on_status), clock)
        result = conn.query(self._spec(accounts=("111", "222")))
        assert {r["_account"] for r in result.rows} == {"111"}
        assert len(result.warnings) == 1
        assert "222" in result.warnings[0] and "still generating" in result.warnings[0]

    def test_profiles_are_reported_on_concurrently(self):
        import threading
        barrier = threading.Barrier(2, timeout=5)

        def on_create(scope):
            barrier.wait()          # deadlocks if profiles ran one by one
            return {"reportId": f"r-{scope}"}

        conn = AmazonAdsConnector(
            _DS(), http=self._http(
                on_status=lambda rid: {"status": "COMPLETED", "url": "u"},
                on_create=on_create),
            download=lambda u: _gzip_rows(REPORT_ROWS), sleep=lambda *_a: None)
        result = conn.query(self._spec(accounts=("111", "222")))
        assert result.row_count == 4


class TestReportWaitSetting:
    def test_default_override_floor_and_garbage(self, monkeypatch):
        from terno_dbi.connectors.api.sources.amazon_ads import _report_wait
        monkeypatch.delenv("TERNO_AMAZON_ADS_REPORT_WAIT_SECONDS", raising=False)
        assert _report_wait() == 180.0
        monkeypatch.setenv("TERNO_AMAZON_ADS_REPORT_WAIT_SECONDS", "240")
        assert _report_wait() == 240.0
        monkeypatch.setenv("TERNO_AMAZON_ADS_REPORT_WAIT_SECONDS", "1")
        assert _report_wait() == 10.0
        monkeypatch.setenv("TERNO_AMAZON_ADS_REPORT_WAIT_SECONDS", "soon")
        assert _report_wait() == 180.0


class TestHttpErrors:
    def test_duplicate_report_id_is_read_from_a_425(self):
        from terno_dbi.connectors.api.sources.amazon_ads import _duplicate_report_id
        rid = "0e8c1f5c-1d2b-4c3a-9f7e-123456789abc"
        resp = _resp(425, {"code": "425",
                           "detail": f"The Request is a duplicate of : {rid}"})
        assert _duplicate_report_id(resp) == rid
        assert _duplicate_report_id(_resp(425, {"detail": "Too early"})) is None

    def test_default_http_raises_duplicate_on_425(self, monkeypatch):
        import requests
        from terno_dbi.connectors.api.sources.amazon_ads import (
            _DuplicateReport, _default_http)
        rid = "0e8c1f5c-1d2b-4c3a-9f7e-123456789abc"
        monkeypatch.setattr(requests, "request", lambda *a, **k: _resp(
            425, {"detail": f"The Request is a duplicate of : {rid}"}))
        with pytest.raises(_DuplicateReport) as exc:
            _default_http("POST", "https://x/reporting/reports", {}, {})
        assert exc.value.report_id == rid

    def test_429_is_rate_limited_with_retry_after(self):
        from terno_dbi.connectors.api.sources.amazon_ads import _amazon_error
        err = _amazon_error(_resp(429, {"message": "throttled"},
                                  headers={"Retry-After": "7"}))
        assert err.code == ErrorCode.RATE_LIMITED and err.retriable
        assert err.retry_after_seconds == 7

    def test_4xx_is_not_retriable_and_5xx_is(self):
        from terno_dbi.connectors.api.sources.amazon_ads import _amazon_error
        assert _amazon_error(_resp(400, {"details": "bad"})).retriable is False
        assert _amazon_error(_resp(503, {})).retriable is True


class TestAuthAndConfig:
    def test_missing_client_id_is_a_clear_config_error(self, monkeypatch):
        monkeypatch.delenv("TERNO_AMAZON_ADS_CLIENT_ID", raising=False)
        conn = _connector()
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert "TERNO_AMAZON_ADS_CLIENT_ID" in exc.value.message

    def test_401_becomes_auth_expired(self):
        from terno_dbi.connectors.api.sources.amazon_ads import _AuthError

        def http(method, url, headers, body=None):
            raise _AuthError()
        conn = AmazonAdsConnector(_DS(), http=http, download=lambda u: b"",
                                  sleep=lambda *_a: None)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestRegistration:
    def test_amazon_ads_is_registered(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("amazon_ads")

    def test_factory_binds_token_refresher(self):
        from terno_dbi.connectors.api.sources.amazon_ads import make_amazon_ads_connector
        conn = make_amazon_ads_connector(_DS())
        assert conn._token_refresher is not None


class TestReportReuse:
    """Re-running a query picks up the report it already requested."""

    def _spec(self, end="2026-08-31"):
        return QuerySpec(
            accounts=["111"], fields=["campaignName", "clicks"],
            date_range=DateRange("2026-08-01", end),
            report_type="spCampaigns",
        )

    def _connector(self, statuses, created, ds=None, monkeypatch=None):
        """`statuses` maps report id -> status payload; POSTs mint r1, r2, ..."""
        def http(method, url, headers, body=None):
            if "/v2/profiles" in url:
                return PROFILES_NA if "advertising-api.amazon.com" in url else []
            if method == "POST":
                created.append(f"r{len(created) + 1}")
                return {"reportId": created[-1]}
            return statuses(url.rsplit("/", 1)[-1])
        clock = _Clock()
        return AmazonAdsConnector(ds or _DS(), http=http,
                                  download=lambda u: _gzip_rows(REPORT_ROWS),
                                  sleep=clock.sleep, clock=clock)

    def test_rerun_after_timeout_reuses_the_report(self, monkeypatch):
        monkeypatch.setenv("TERNO_AMAZON_ADS_REPORT_WAIT_SECONDS", "30")
        created, ready = [], {"now": False}

        def statuses(rid):
            if ready["now"]:
                return {"status": "COMPLETED", "url": "https://s3/r.gz"}
            return {"status": "PENDING"}

        with pytest.raises(ApiError) as exc:
            self._connector(statuses, created).query(self._spec())
        assert exc.value.code == ErrorCode.TIMEOUT

        ready["now"] = True
        result = self._connector(statuses, created).query(self._spec())
        assert result.row_count == 2
        assert created == ["r1"]            # no second report requested

    def test_finished_report_is_reused(self):
        created = []
        done = lambda rid: {"status": "COMPLETED", "url": "https://s3/r.gz"}  # noqa: E731
        self._connector(done, created).query(self._spec())
        self._connector(done, created).query(self._spec())
        assert created == ["r1"]

    def test_failed_report_is_replaced(self, monkeypatch):
        monkeypatch.setenv("TERNO_AMAZON_ADS_REPORT_WAIT_SECONDS", "30")
        created = []
        with pytest.raises(ApiError):
            self._connector(lambda rid: {"status": "PENDING"}, created).query(
                self._spec())

        def statuses(rid):
            if rid == "r1":
                return {"status": "FAILED", "failureReason": "expired"}
            return {"status": "COMPLETED", "url": "https://s3/r.gz"}

        result = self._connector(statuses, created).query(self._spec())
        assert result.row_count == 2
        assert created == ["r1", "r2"]

    def test_reports_are_not_shared_between_datasources(self):
        class _DS2(_DS):
            pk = 2
        created = []
        done = lambda rid: {"status": "COMPLETED", "url": "https://s3/r.gz"}  # noqa: E731
        self._connector(done, created).query(self._spec())
        self._connector(done, created, ds=_DS2()).query(self._spec())
        assert created == ["r1", "r2"]

    def test_reuse_window_is_shorter_when_the_range_includes_today(self):
        from datetime import date, timedelta

        from terno_dbi.connectors.api.sources.amazon_ads import _reuse_seconds
        today = date.today()
        assert _reuse_seconds({"endDate": today.isoformat()}) == 3600
        past = (today - timedelta(days=1)).isoformat()
        assert _reuse_seconds({"endDate": past}) == 12 * 3600
