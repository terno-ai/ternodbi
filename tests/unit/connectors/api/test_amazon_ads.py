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
        assert camp["acosClicks7d"].is_non_aggregatable is True
        assert camp["clicks"].kind == "metric"
        st = {f.id for f in conn.list_fields("spSearchTerm")}
        assert "searchTerm" in st and "searchTerm" not in camp


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
