"""Google Ads connector.

Implements the `ApiConnector` interface against the Google Ads REST API using
GAQL (the Google Ads Query Language) directly:

- `list_accounts()` — `customers:listAccessibleCustomers`
- `list_fields()`   — a *curated* catalogue per report type. The Ads API exposes
  thousands of fields via GoogleAdsFieldService; a hand-picked, useful subset per
  report is what an agent actually needs, and keeps discovery legible.
- `_run()`          — `customers/{id}/googleAds:search` with a generated GAQL query

Unlike GA4/GSC, Google Ads:
  * namespaces fields (`campaign.name`, `metrics.clicks`, `segments.date`) and
    each report type selects `FROM` a different resource;
  * returns money as integer *micros* (`cost_micros` = currency * 1e6);
  * requires a developer token header in addition to the OAuth bearer.
"""

from __future__ import annotations
import logging
import os
from typing import Any, Callable, Dict, List, Optional
from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

_API_VERSION = "v22"
_BASE = f"https://googleads.googleapis.com/{_API_VERSION}"
_DEVELOPER_TOKEN_ENV = "TERNO_GOOGLE_ADS_DEVELOPER_TOKEN"

_RESOURCE: Dict[str, str] = {
    "Campaign": "campaign",
    "AdGroup": "ad_group",
    "Keyword": "keyword_view",
    "SearchTerm": "search_term_view",
}
_DEFAULT_REPORT = "Campaign"

_SHARED_METRICS: List[Field] = [
    Field("metrics.impressions", "Impressions", "metric",
          "Times an ad was shown.", data_type="integer"),
    Field("metrics.clicks", "Clicks", "metric", "Ad clicks.",
          data_type="integer"),
    Field("metrics.cost_micros", "Cost", "metric",
          "Spend for the row (converted from micros).", data_type="number",
          is_monetary=True),
    Field("metrics.conversions", "Conversions", "metric",
          "Attributed conversions.", data_type="number"),
    Field("metrics.conversions_value", "Conversion value", "metric",
          "Total value of conversions.", data_type="number", is_monetary=True),
    Field("metrics.ctr", "CTR", "metric",
          "Click-through rate (clicks / impressions).", data_type="number",
          is_non_aggregatable=True),
    Field("metrics.average_cpc", "Avg. CPC", "metric",
          "Average cost per click (converted from micros).", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
]

_SHARED_SEGMENTS: List[Field] = [
    Field("segments.date", "Date", "dimension", "Day the stat occurred.",
          data_type="date"),
    Field("segments.device", "Device", "dimension",
          "Device class: MOBILE, DESKTOP, TABLET, CONNECTED_TV."),
]

_REPORT_DIMENSIONS: Dict[str, List[Field]] = {
    "Campaign": [
        Field("campaign.id", "Campaign ID", "dimension", data_type="string"),
        Field("campaign.name", "Campaign", "dimension"),
        Field("campaign.status", "Campaign status", "dimension"),
        Field("campaign.advertising_channel_type", "Channel", "dimension"),
    ],
    "AdGroup": [
        Field("ad_group.id", "Ad group ID", "dimension", data_type="string"),
        Field("ad_group.name", "Ad group", "dimension"),
        Field("ad_group.status", "Ad group status", "dimension"),
        Field("campaign.name", "Campaign", "dimension"),
    ],
    "Keyword": [
        Field("ad_group_criterion.keyword.text", "Keyword", "dimension"),
        Field("ad_group_criterion.keyword.match_type", "Match type", "dimension"),
        Field("ad_group.name", "Ad group", "dimension"),
        Field("campaign.name", "Campaign", "dimension"),
    ],
    "SearchTerm": [
        Field("search_term_view.search_term", "Search term", "dimension"),
        Field("ad_group.name", "Ad group", "dimension"),
        Field("campaign.name", "Campaign", "dimension"),
    ],
}

# Fields delivered in micros (currency * 1e6); divided back to currency on parse.
_MICROS_FIELDS = frozenset({"metrics.cost_micros", "metrics.average_cpc"})


def _fields_for(report_type: Optional[str]) -> Dict[str, Field]:
    rt = report_type if report_type in _RESOURCE else _DEFAULT_REPORT
    fields = [*_REPORT_DIMENSIONS[rt], *_SHARED_SEGMENTS, *_SHARED_METRICS]
    return {f.id: f for f in fields}


def _default_http(method: str, url: str, token: str,
                  json_body: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    dev_token = os.getenv(_DEVELOPER_TOKEN_ENV, "").strip()
    if not dev_token:
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"Google Ads is not configured on the server: {_DEVELOPER_TOKEN_ENV} "
            "is unset. Set the developer token and restart.",
            retriable=False,
        )
    headers = {"Authorization": f"Bearer {token}", "developer-token": dev_token}
    resp = requests.request(method, url, headers=headers, json=json_body, timeout=30)
    if resp.status_code == 401:
        raise _AuthError()
    if resp.status_code >= 400:
        raise _ads_error(resp)
    return resp.json()


def _ads_error(resp) -> ApiError:
    """Turn a Google Ads error response into an actionable `ApiError`.

    Google Ads wraps the real cause in a `GoogleAdsFailure` with a typed
    `errorCode` and message; surfacing those (rather than a generic "try again")
    is what lets an operator see e.g. DEVELOPER_TOKEN_PROHIBITED or a sunset API
    version. Only 429/5xx are retriable; a 4xx here is a config/permission issue
    that retrying will not fix.
    """
    status = resp.status_code
    code_name, message = "", ""
    try:
        err = (resp.json() or {}).get("error", {})
        message = err.get("message", "")
        for detail in err.get("details", []):
            for e in detail.get("errors", []):
                ec = e.get("errorCode", {})
                if isinstance(ec, dict) and ec:
                    code_name = next(iter(ec.values()))
                message = e.get("message", message)
                break
            if code_name:
                break
    except ValueError:
        message = (resp.text or "")[:200]   # non-JSON (e.g. a 404 HTML page)

    if status == 404:
        message = (message or "Not found") + (
            f" (is API version {_API_VERSION} still supported?)")
    label = f"{status} {code_name}".strip()
    return ApiError(
        ErrorCode.UPSTREAM_ERROR,
        f"Google Ads API error ({label}): {message or 'unknown error'}",
        retriable=status == 429 or status >= 500,
    )


class _AuthError(Exception):
    """Internal marker for a 401 from Google, mapped to AUTH_EXPIRED."""


class GoogleAdsConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http

    # -- transport ----------------------------------------------------------

    def _call(self, method: str, url: str, body: Optional[Dict] = None) -> Dict[str, Any]:
        try:
            return self._http(method, url, self.access_token(), body)
        except _AuthError:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} access was rejected; reconnect the source.",
            )
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("Google Ads request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Google Ads returned an error. Try again.",
            )

    @staticmethod
    def _customer_id(account_id: str) -> str:
        """Accept '123-456-7890' or 'customers/1234567890'; return bare digits."""
        aid = str(account_id).split("/")[-1]
        return aid.replace("-", "")

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        data = self._call(
            "GET", f"{_BASE}/customers:listAccessibleCustomers")
        accounts: List[Account] = []
        for name in data.get("resourceNames", []):
            cid = name.split("/")[-1]
            if cid:
                accounts.append(Account(id=cid, name=cid))
        return accounts

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(_fields_for(report_type).values())

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = spec.report_type if spec.report_type in _RESOURCE else _DEFAULT_REPORT
        catalogue = _fields_for(report_type)

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        dimensions = [f for f in spec.fields if catalogue[f].kind == "dimension"]
        metrics = [f for f in spec.fields if catalogue[f].kind == "metric"]
        if not (dimensions or metrics):
            # A report with no selected fields is a dead end; default to the
            # report's core metrics.
            metrics = [m.id for m in _SHARED_METRICS[:3]]

        gaql = _build_gaql(
            _RESOURCE[report_type], dimensions, metrics,
            spec.date_range.start, spec.date_range.end, spec.max_rows,
        )

        multi = len(spec.accounts) > 1

        def fetch(account):
            cid = self._customer_id(account)
            url = f"{_BASE}/customers/{cid}/googleAds:search"
            data = self._call("POST", url, {"query": gaql})
            return _parse_results(data, dimensions, metrics, catalogue,
                                  account, multi=multi)

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=list(spec.fields) or [*dimensions, *metrics],
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )


def _build_gaql(resource, dimensions, metrics, start, end, limit) -> str:
    """Compose a GAQL query. Every report resource supports `segments.date`, so
    the date filter is always valid; ordering mirrors GA4's default (time series
    ascending, otherwise largest metric first)."""
    select = ", ".join([*dimensions, *metrics])
    q = (f"SELECT {select} FROM {resource} "
         f"WHERE segments.date BETWEEN '{start}' AND '{end}'")
    if "segments.date" in dimensions:
        q += " ORDER BY segments.date ASC"
    elif metrics:
        q += f" ORDER BY {metrics[0]} DESC"
    q += f" LIMIT {int(limit)}"
    return q


def _camel(segment: str) -> str:
    """snake_case -> camelCase for one GAQL path segment."""
    head, *tail = segment.split("_")
    return head + "".join(w[:1].upper() + w[1:] for w in tail)


def _json_path(field_id: str) -> List[str]:
    """'metrics.cost_micros' -> ['metrics', 'costMicros']; the REST response
    nests by resource and camelCases every leaf."""
    return [_camel(p) for p in field_id.split(".")]


def _dig(obj: Dict[str, Any], path: List[str]):
    cur: Any = obj
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _parse_results(data, dimensions, metrics, catalogue, account, *,
                   multi=False) -> List[Dict[str, Any]]:
    """Map GAQL `search` results back to field ids.

    Each result is a nested GoogleAdsRow; a field id is a dotted path that,
    camelCased, walks straight into it. Money fields arrive in micros and are
    divided back to currency here so a consumer never sees a raw micro value.
    """
    result: List[Dict[str, Any]] = []
    for row in data.get("results", []):
        record: Dict[str, Any] = {}
        if multi:
            record["_account"] = account
        for name in [*dimensions, *metrics]:
            value = _dig(row, _json_path(name))
            record[name] = _coerce(name, value, catalogue)
        result.append(record)
    return result


def _coerce(field_id, raw, catalogue):
    if raw is None:
        return None
    if field_id in _MICROS_FIELDS:
        try:
            return float(raw) / 1_000_000
        except (TypeError, ValueError):
            return raw
    field = catalogue.get(field_id)
    if field and field.kind == "metric":
        try:
            f = float(raw)
            return int(f) if f.is_integer() else f
        except (TypeError, ValueError):
            return raw
    return raw


def make_google_ads_connector(datasource) -> GoogleAdsConnector:
    """Build a Google Ads connector wired to refresh its own OAuth token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return GoogleAdsConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["GoogleAdsConnector", "make_google_ads_connector"]
