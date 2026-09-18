"""Amazon Ads connector (Sponsored Products / Brands / Display).

Implements the `ApiConnector` interface against the Amazon Ads API. Amazon differs
from every other connector here:

- **Accounts are profiles, and profiles are per-region.** `GET /v2/profiles` on
  each regional host (NA/EU/FE) lists that region's advertising profiles. A query
  for a profile must go to that profile's region, and set the profile id in the
  `Amazon-Advertising-API-Scope` header.
- **Reporting v3 is asynchronous.** `list_fields` is a curated catalogue per report
  type (Amazon has no field-metadata endpoint). `_run` creates a report
  (`POST /reporting/reports`), polls `GET /reporting/reports/{id}` until it is
  COMPLETED, downloads the gzipped-JSON result from a presigned URL, and maps it
  back to the requested columns.
- **Money is already in the account currency** (a float), not micros.
- Every call carries the Login-with-Amazon client id in
  `Amazon-Advertising-API-ClientId`, alongside the OAuth bearer token.

The single OAuth scope (`advertising::campaign_management`) grants all of this;
DSP and Amazon Marketing Cloud are separate APIs (separate access) and are not
covered here.
"""

from __future__ import annotations
import gzip
import json
import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

_CLIENT_ID_ENV = "TERNO_AMAZON_ADS_CLIENT_ID"
_V3_REPORT_CT = "application/vnd.createasyncreportrequest.v3+json"

# Profiles and queries must hit the host for the profile's region.
_REGIONS: Dict[str, str] = {
    "NA": "https://advertising-api.amazon.com",
    "EU": "https://advertising-api-eu.amazon.com",
    "FE": "https://advertising-api-fe.amazon.com",
}

# Async report polling budget. Amazon reports usually complete in well under a
# minute; bounded so a stuck report surfaces a retriable timeout, not a hang.
_POLL_INTERVAL_S = 5
_POLL_ATTEMPTS = 24   # ~2 minutes


# --------------------------------------------------------------------------
# Report catalogues — one per report type. Amazon has no field-metadata
# endpoint, so columns are curated from the Reporting v3 documentation. Each
# report fixes its `ad_product`, `report_type_id` and `group_by`; the Field ids
# are the exact Amazon column names. Money columns are flagged monetary (already
# in account currency — no conversion); ratios are non-aggregatable.
# --------------------------------------------------------------------------

def _m(cid, name, **kw):   # metric shorthand
    return Field(cid, name, "metric", kw.pop("desc", ""), **kw)


def _d(cid, name, **kw):   # dimension shorthand
    return Field(cid, name, "dimension", kw.pop("desc", ""), **kw)


_DATE = _d("date", "Date", data_type="date",
           desc="Day of the stat (omitted for summary totals).")

# Shared Sponsored Products performance metrics (7-day attribution shown; other
# windows are available as their own columns).
_SP_CORE_METRICS: List[Field] = [
    _m("impressions", "Impressions", data_type="integer"),
    _m("clicks", "Clicks", data_type="integer"),
    _m("cost", "Spend", data_type="number", is_monetary=True),
    _m("costPerClick", "CPC", data_type="number", is_monetary=True,
       is_non_aggregatable=True),
    _m("clickThroughRate", "CTR", data_type="number", is_non_aggregatable=True),
    _m("purchases7d", "Purchases (7d)", data_type="integer"),
    _m("sales7d", "Sales (7d)", data_type="number", is_monetary=True),
    _m("unitsSoldClicks7d", "Units sold (7d)", data_type="integer"),
    _m("acosClicks7d", "ACOS (7d)", data_type="number", is_non_aggregatable=True),
    _m("roasClicks7d", "ROAS (7d)", data_type="number", is_non_aggregatable=True),
]
# Extra attribution windows, offered for completeness.
_SP_WINDOWS: List[Field] = [
    _m("purchases1d", "Purchases (1d)", data_type="integer"),
    _m("purchases14d", "Purchases (14d)", data_type="integer"),
    _m("purchases30d", "Purchases (30d)", data_type="integer"),
    _m("sales1d", "Sales (1d)", data_type="number", is_monetary=True),
    _m("sales14d", "Sales (14d)", data_type="number", is_monetary=True),
    _m("sales30d", "Sales (30d)", data_type="number", is_monetary=True),
    _m("unitsSoldClicks1d", "Units sold (1d)", data_type="integer"),
    _m("unitsSoldClicks14d", "Units sold (14d)", data_type="integer"),
    _m("unitsSoldClicks30d", "Units sold (30d)", data_type="integer"),
]


class _Report:
    def __init__(self, ad_product, report_type_id, group_by, fields):
        self.ad_product = ad_product
        self.report_type_id = report_type_id
        self.group_by = group_by
        self.fields = fields
        self.catalogue = {f.id: f for f in fields}


_REPORTS: Dict[str, _Report] = {
    "spCampaigns": _Report(
        "SPONSORED_PRODUCTS", "spCampaigns", ["campaign"],
        [_DATE,
         _d("campaignId", "Campaign ID", data_type="string"),
         _d("campaignName", "Campaign"),
         _d("campaignStatus", "Campaign status"),
         _m("campaignBudgetAmount", "Budget", data_type="number", is_monetary=True,
            is_non_aggregatable=True),
         *_SP_CORE_METRICS, *_SP_WINDOWS]),
    "spTargeting": _Report(
        "SPONSORED_PRODUCTS", "spTargeting", ["targeting"],
        [_DATE,
         _d("campaignId", "Campaign ID", data_type="string"),
         _d("campaignName", "Campaign"),
         _d("adGroupId", "Ad group ID", data_type="string"),
         _d("adGroupName", "Ad group"),
         _d("keywordId", "Keyword ID", data_type="string"),
         _d("keyword", "Keyword / target"),
         _d("keywordType", "Keyword type"),
         _d("matchType", "Match type"),
         _d("targeting", "Targeting expression"),
         _m("keywordBid", "Bid", data_type="number", is_monetary=True,
            is_non_aggregatable=True),
         *_SP_CORE_METRICS]),
    "spSearchTerm": _Report(
        "SPONSORED_PRODUCTS", "spSearchTerm", ["searchTerm"],
        [_DATE,
         _d("campaignId", "Campaign ID", data_type="string"),
         _d("campaignName", "Campaign"),
         _d("adGroupId", "Ad group ID", data_type="string"),
         _d("keywordId", "Keyword ID", data_type="string"),
         _d("keyword", "Keyword"),
         _d("matchType", "Match type"),
         _d("searchTerm", "Search term"),
         *_SP_CORE_METRICS]),
    "spAdvertisedProduct": _Report(
        "SPONSORED_PRODUCTS", "spAdvertisedProduct", ["advertiser"],
        [_DATE,
         _d("campaignId", "Campaign ID", data_type="string"),
         _d("campaignName", "Campaign"),
         _d("adGroupId", "Ad group ID", data_type="string"),
         _d("advertisedAsin", "Advertised ASIN", data_type="string"),
         _d("advertisedSku", "Advertised SKU", data_type="string"),
         *_SP_CORE_METRICS]),
    "spPurchasedProduct": _Report(
        "SPONSORED_PRODUCTS", "spPurchasedProduct", ["asin"],
        [_DATE,
         _d("campaignId", "Campaign ID", data_type="string"),
         _d("adGroupId", "Ad group ID", data_type="string"),
         _d("keywordId", "Keyword ID", data_type="string"),
         _d("keyword", "Keyword"),
         _d("purchasedAsin", "Purchased ASIN", data_type="string"),
         _m("purchases7d", "Purchases (7d)", data_type="integer"),
         _m("sales7d", "Sales (7d)", data_type="number", is_monetary=True),
         _m("unitsSoldClicks7d", "Units sold (7d)", data_type="integer")]),
    # Sponsored Brands / Display campaign performance. Column names differ from
    # SP (non-windowed purchases/sales); curated conservatively and marked for
    # live validation before enabling those report types in production.
    "sbCampaigns": _Report(
        "SPONSORED_BRANDS", "sbCampaigns", ["campaign"],
        [_DATE,
         _d("campaignId", "Campaign ID", data_type="string"),
         _d("campaignName", "Campaign"),
         _m("impressions", "Impressions", data_type="integer"),
         _m("clicks", "Clicks", data_type="integer"),
         _m("cost", "Spend", data_type="number", is_monetary=True),
         _m("purchases", "Purchases", data_type="integer"),
         _m("sales", "Sales", data_type="number", is_monetary=True),
         _m("newToBrandPurchases", "New-to-brand purchases", data_type="integer"),
         _m("newToBrandSales", "New-to-brand sales", data_type="number",
            is_monetary=True)]),
    "sdCampaigns": _Report(
        "SPONSORED_DISPLAY", "sdCampaigns", ["campaign"],
        [_DATE,
         _d("campaignId", "Campaign ID", data_type="string"),
         _d("campaignName", "Campaign"),
         _m("impressions", "Impressions", data_type="integer"),
         _m("clicks", "Clicks", data_type="integer"),
         _m("cost", "Spend", data_type="number", is_monetary=True),
         _m("purchases", "Purchases", data_type="integer"),
         _m("sales", "Sales", data_type="number", is_monetary=True),
         _m("detailPageViews", "Detail page views", data_type="integer")]),
}
_DEFAULT_REPORT = "spCampaigns"


def _report_for(report_type: Optional[str]) -> _Report:
    return _REPORTS.get(report_type or _DEFAULT_REPORT, _REPORTS[_DEFAULT_REPORT])


# --------------------------------------------------------------------------
# Transport
# --------------------------------------------------------------------------

def _default_http(method: str, url: str, headers: Dict[str, str],
                  json_body: Optional[Dict] = None) -> Any:
    import requests
    resp = requests.request(method, url, headers=headers, json=json_body,
                            timeout=60)
    if resp.status_code == 401:
        raise _AuthError()
    if resp.status_code >= 400:
        raise _amazon_error(resp)
    return resp.json() if resp.content else {}


def _default_download(url: str) -> bytes:
    """Fetch a completed report from its presigned URL — no Amazon auth headers."""
    import requests
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    return resp.content


def _amazon_error(resp) -> ApiError:
    try:
        body = resp.json() or {}
        msg = (body.get("details") or body.get("message")
               or body.get("code") or "")
    except ValueError:
        msg = (resp.text or "")[:200]
    return ApiError(
        ErrorCode.UPSTREAM_ERROR,
        f"Amazon Ads API error ({resp.status_code}): {msg or 'unknown error'}",
        retriable=resp.status_code == 429 or resp.status_code >= 500,
    )


class _AuthError(Exception):
    """Internal marker for a 401 from Amazon, mapped to AUTH_EXPIRED."""


class AmazonAdsConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 download: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None,
                 sleep: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http
        self._download = download or _default_download
        self._sleep = sleep or time.sleep
        self._profile_region: Dict[str, str] = {}   # profileId -> base URL

    # -- transport ----------------------------------------------------------

    def _client_id(self) -> str:
        cid = os.getenv(_CLIENT_ID_ENV, "").strip()
        if not cid:
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                f"Amazon Ads is not configured on the server: {_CLIENT_ID_ENV} "
                "is unset. Set the Login-with-Amazon client id and restart.",
                retriable=False,
            )
        return cid

    def _headers(self, scope: Optional[str] = None,
                 content_type: Optional[str] = None) -> Dict[str, str]:
        h = {
            "Authorization": f"Bearer {self.access_token()}",
            "Amazon-Advertising-API-ClientId": self._client_id(),
        }
        if scope:
            h["Amazon-Advertising-API-Scope"] = str(scope)
        if content_type:
            h["Content-Type"] = content_type
        return h

    def _call(self, method: str, url: str, *, scope: Optional[str] = None,
              content_type: Optional[str] = None,
              body: Optional[Dict] = None) -> Any:
        try:
            return self._http(method, url, self._headers(scope, content_type), body)
        except _AuthError:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} access was rejected; reconnect the source.",
            )
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("Amazon Ads request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Amazon Ads returned an error. Try again.",
            )

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        """Profiles across all regions. A profile the credential can't see in a
        region simply doesn't appear; an unauthorized region is skipped."""
        self._client_id()   # surface a missing-config error before the loop
        accounts: List[Account] = []
        region_map: Dict[str, str] = {}
        for region, base in _REGIONS.items():
            try:
                data = self._call("GET", f"{base}/v2/profiles")
            except ApiError as exc:
                # A rejected token means reconnect — surface it. Other per-region
                # failures (no access in that region) are skipped so the regions
                # the credential *can* see still come back.
                if exc.code == ErrorCode.AUTH_EXPIRED:
                    raise
                logger.info("Amazon profiles skipped for %s: %s", region, exc.message)
                continue
            for prof in data or []:
                pid = prof.get("profileId")
                if pid is None:
                    continue
                pid = str(pid)
                info = prof.get("accountInfo") or {}
                country = prof.get("countryCode") or ""
                label = info.get("name") or country or pid
                region_map[pid] = base
                accounts.append(Account(
                    id=pid,
                    name=f"{label} ({country})" if country else label,
                    currency=prof.get("currencyCode"),
                    extra={k: v for k, v in {
                        "region": region,
                        "country": country,
                        "marketplace": info.get("marketplaceStringId"),
                        "account_type": info.get("type"),
                    }.items() if v},
                ))
        self._profile_region = region_map
        return accounts

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(_report_for(report_type).fields)

    def _base_for(self, profile: str) -> str:
        if profile not in self._profile_region:
            self.list_accounts()                       # populate the region map
        return self._profile_region.get(profile, _REGIONS["NA"])

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = spec.report_type if spec.report_type in _REPORTS else _DEFAULT_REPORT
        report = _report_for(report_type)
        catalogue = report.catalogue

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        requested = list(spec.fields) or list(catalogue.keys())
        want_date = "date" in requested
        # Columns sent to Amazon: the requested set minus `date` when this is a
        # summary (no daily breakdown) query.
        columns = [c for c in requested if not (c == "date" and not want_date)]

        configuration = {
            "adProduct": report.ad_product,
            "groupBy": report.group_by,
            "columns": columns,
            "reportTypeId": report.report_type_id,
            "timeUnit": "DAILY" if want_date else "SUMMARY",
            "format": "GZIP_JSON",
        }
        body = {
            "name": f"terno {report.report_type_id} {spec.date_range.start}..{spec.date_range.end}",
            "startDate": spec.date_range.start,
            "endDate": spec.date_range.end,
            "configuration": configuration,
        }

        multi = len(spec.accounts) > 1

        def fetch(account):
            base = self._base_for(account)
            created = self._call(
                "POST", f"{base}/reporting/reports",
                scope=account, content_type=_V3_REPORT_CT, body=body)
            report_id = created.get("reportId") or created.get("reportId".lower())
            if not report_id:
                raise ApiError(ErrorCode.UPSTREAM_ERROR,
                               "Amazon did not return a report id.")
            url = self._poll(base, account, report_id)
            raw = self._download(url)
            return _parse_report(raw, requested, catalogue, account, multi=multi)

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )

    def _poll(self, base: str, scope: str, report_id: str) -> str:
        """Poll a report to completion and return its download URL."""
        for _ in range(_POLL_ATTEMPTS):
            st = self._call(
                "GET", f"{base}/reporting/reports/{report_id}", scope=scope)
            status = str(st.get("status") or "").upper()
            if status in ("COMPLETED", "SUCCESS"):
                url = st.get("url") or st.get("location")
                if url:
                    return url
                raise ApiError(ErrorCode.UPSTREAM_ERROR,
                               "Amazon report completed without a download URL.")
            if status in ("FAILED", "FAILURE", "CANCELLED"):
                detail = st.get("statusDetails") or status
                raise ApiError(ErrorCode.UPSTREAM_ERROR,
                               f"Amazon report failed: {detail}", retriable=True)
            self._sleep(_POLL_INTERVAL_S)
        raise ApiError(ErrorCode.TIMEOUT,
                       "Amazon report did not finish in time; retry the query.",
                       retriable=True)


def _parse_report(raw: bytes, requested, catalogue, account, *,
                  multi=False) -> List[Dict[str, Any]]:
    """Gunzip the report and map each row to the requested column ids."""
    try:
        payload = gzip.decompress(raw)
    except (OSError, EOFError):
        payload = raw   # some transports hand back already-decompressed bytes
    data = json.loads(payload.decode("utf-8"))
    if not isinstance(data, list):
        data = data.get("rows", []) if isinstance(data, dict) else []
    out: List[Dict[str, Any]] = []
    for row in data:
        record: Dict[str, Any] = {}
        if multi:
            record["_account"] = account
        for col in requested:
            record[col] = _coerce(col, row.get(col), catalogue)
        out.append(record)
    return out


def _coerce(field_id, raw, catalogue):
    if raw is None:
        return None
    field = catalogue.get(field_id)
    if field and field.kind == "metric":
        try:
            f = float(raw)
            return int(f) if f.is_integer() else f
        except (TypeError, ValueError):
            return raw
    return raw


def make_amazon_ads_connector(datasource) -> AmazonAdsConnector:
    """Build an Amazon Ads connector wired to refresh its own OAuth token."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return AmazonAdsConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["AmazonAdsConnector", "make_amazon_ads_connector"]
