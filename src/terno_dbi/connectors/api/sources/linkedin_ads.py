"""LinkedIn Ads connector.

Implements the `ApiConnector` interface against the LinkedIn Marketing API
directly:

- `list_accounts()` — `GET /rest/adAccounts?q=search` (the sponsored accounts
  the member can administer)
- `list_fields()`   — a *curated* catalogue per report type. LinkedIn's
  analytics finder accepts a fixed metric vocabulary and one `pivot` at a time,
  so — like YouTube rather than GA4 — the report type fixes the breakdown and
  the caller picks metrics from the valid set.
- `_run()`          — `GET /rest/adAnalytics?q=analytics`

Three things make LinkedIn different from the other ad connectors:

  * every request carries a `LinkedIn-Version` header (YYYYMM) alongside the
    bearer token, and versions are sunset on a rolling schedule;
  * Rest.li parameters are a syntax, not JSON — a date range is written
    `(start:(year:2026,month:8,day:1),...)` and an account list is
    `List(urn:li:sponsoredAccount:123)`;
  * analytics rows identify their pivot only by URN, so a campaign *name*
    costs a second call — made only when a name field was actually requested.

LinkedIn issues refresh tokens only to approved applications; without one an
access token simply expires after ~60 days and `refresh_access_token` marks the
source for reconnection, which is the correct outcome here.
"""

from __future__ import annotations
import logging
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

_BASE = "https://api.linkedin.com/rest"

# LinkedIn sunsets API versions on a rolling quarterly schedule, so this is
# deployment-tunable rather than a constant to be edited and released.
_VERSION_ENV = "TERNO_LINKEDIN_API_VERSION"
_DEFAULT_VERSION = "202506"

# The analytics finder caps a page at 1000 rows whatever `count` asks for.
_MAX_COUNT = 1000

_ACCOUNT_URN = "urn:li:sponsoredAccount:{}"


def api_version() -> str:
    return os.getenv(_VERSION_ENV, "").strip() or _DEFAULT_VERSION


# -- metric catalogue -------------------------------------------------------

def _metric(mid, name, desc="", data_type="integer", monetary=False,
            non_agg=False):
    return Field(mid, name, "metric", desc, data_type=data_type,
                 is_monetary=monetary, is_non_aggregatable=non_agg)


_DELIVERY_METRICS = [
    _metric("impressions", "Impressions", "Times an ad was shown."),
    _metric("clicks", "Clicks",
            "All clicks, including on the ad, the company name and the logo."),
    _metric("landingPageClicks", "Landing page clicks",
            "Clicks that sent a member to the ad's destination."),
    _metric("costInLocalCurrency", "Cost", "Spend in the account's currency.",
            data_type="number", monetary=True),
    _metric("costInUsd", "Cost (USD)",
            "Spend converted to US dollars. Use this — not 'Cost' — to compare "
            "accounts that bill in different currencies.",
            data_type="number", monetary=True),
]

_ENGAGEMENT_METRICS = [
    _metric("totalEngagements", "Engagements",
            "All social actions plus clicks."),
    _metric("likes", "Likes"),
    _metric("comments", "Comments"),
    _metric("shares", "Shares"),
    _metric("follows", "Follows", "New followers gained from the ad."),
    _metric("otherEngagements", "Other engagements"),
]

_VIDEO_METRICS = [
    _metric("videoViews", "Video views",
            "Plays of at least 2 continuous seconds with 50% on screen."),
    _metric("videoStarts", "Video starts"),
    _metric("videoCompletions", "Video completions"),
]

_CONVERSION_METRICS = [
    _metric("externalWebsiteConversions", "Conversions",
            "Conversions attributed by the LinkedIn Insight Tag."),
    _metric("oneClickLeads", "Leads", "Lead Gen Form submissions."),
    _metric("oneClickLeadFormOpens", "Lead form opens"),
    _metric("conversionValueInLocalCurrency", "Conversion value",
            "Total value of attributed conversions.",
            data_type="number", monetary=True),
]

_ALL_METRICS = [
    *_DELIVERY_METRICS, *_ENGAGEMENT_METRICS, *_VIDEO_METRICS,
    *_CONVERSION_METRICS,
]

# Metrics LinkedIn returns as decimal strings rather than numbers.
_DECIMAL_METRICS = frozenset({
    "costInLocalCurrency", "costInUsd", "conversionValueInLocalCurrency",
})

_DATE = Field("date", "Date", "dimension",
              "The day the statistics are for. Requesting it switches the "
              "report to a daily time series; omit it for one total per row.",
              data_type="date")


def _dim(did, name, desc=""):
    return Field(did, name, "dimension", desc)


# -- report types -----------------------------------------------------------

@dataclass(frozen=True)
class _Report:
    """One LinkedIn report: its pivot, and how that pivot is named in a row."""

    pivot: str
    # The URN prefix the pivot's values carry, e.g. 'urn:li:sponsoredCampaign'.
    urn_type: str
    id_field: str
    name_field: Optional[str] = None
    # Sub-resource under /adAccounts/{id} that resolves ids to names. None when
    # the entity has no name to resolve (a creative) or already has one (the
    # account itself, known from `list_accounts`).
    name_resource: Optional[str] = None

    def dimensions(self) -> List[Field]:
        dims = [_DATE, _dim(self.id_field, self.id_field.replace("_", " ").title())]
        if self.name_field:
            dims.append(_dim(self.name_field,
                             self.name_field.replace("_", " ").title()))
        return dims

    def catalogue(self) -> Dict[str, Field]:
        return {f.id: f for f in (*self.dimensions(), *_ALL_METRICS)}


_REPORTS: Dict[str, _Report] = {
    "Account": _Report(
        pivot="ACCOUNT", urn_type="sponsoredAccount",
        id_field="account_id", name_field="account_name",
    ),
    "CampaignGroup": _Report(
        pivot="CAMPAIGN_GROUP", urn_type="sponsoredCampaignGroup",
        id_field="campaign_group_id", name_field="campaign_group_name",
        name_resource="adCampaignGroups",
    ),
    "Campaign": _Report(
        pivot="CAMPAIGN", urn_type="sponsoredCampaign",
        id_field="campaign_id", name_field="campaign_name",
        name_resource="adCampaigns",
    ),
    # Creatives carry no name of their own in the Marketing API.
    "Creative": _Report(
        pivot="CREATIVE", urn_type="sponsoredCreative",
        id_field="creative_id",
    ),
}
_DEFAULT_REPORT = "Campaign"


def _report_for(report_type: Optional[str]) -> _Report:
    return _REPORTS.get(report_type or "", _REPORTS[_DEFAULT_REPORT])


# -- Rest.li parameter syntax ------------------------------------------------

def _date_range_param(start: str, end: str) -> str:
    """A Rest.li `dateRange`, e.g. `(start:(year:2026,month:8,day:1),end:(...))`.

    Months and days are sent unpadded because Rest.li expects integers; a
    zero-padded '08' is rejected as a malformed value.
    """
    def part(label: str, iso: str) -> str:
        year, month, day = iso.split("-")
        return f"{label}:(year:{int(year)},month:{int(month)},day:{int(day)})"

    return f"({part('start', start)},{part('end', end)})"


def _urn_id(urn: Any) -> Optional[str]:
    """The trailing id of a URN; the value unchanged if it is not one."""
    if urn is None:
        return None
    text = str(urn)
    return text.rsplit(":", 1)[-1] if ":" in text else text


def _iso_date(part: Optional[Dict[str, Any]]) -> Optional[str]:
    """`{'year': 2026, 'month': 8, 'day': 1}` -> `'2026-08-01'`."""
    if not isinstance(part, dict):
        return None
    try:
        return (f"{int(part['year']):04d}-{int(part['month']):02d}-"
                f"{int(part['day']):02d}")
    except (KeyError, TypeError, ValueError):
        return None


# -- transport --------------------------------------------------------------

def _default_http(method: str, url: str, token: str,
                  params: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    headers = {
        "Authorization": f"Bearer {token}",
        "LinkedIn-Version": api_version(),
        "X-Restli-Protocol-Version": "2.0.0",
    }
    resp = requests.request(method, url, headers=headers,
                            params=params or {}, timeout=30)
    if resp.status_code == 401:
        raise _AuthError()
    if resp.status_code >= 400:
        raise _linkedin_error(resp)
    return resp.json()


def _linkedin_error(resp) -> ApiError:
    """Surface LinkedIn's own message and service error code.

    A bare "try again" hides the two failures an operator actually hits: a
    sunset `LinkedIn-Version`, and an account the token's scopes do not cover.
    """
    status = resp.status_code
    message, service_code = "", ""
    try:
        body = resp.json() or {}
        message = body.get("message", "")
        if body.get("serviceErrorCode") is not None:
            service_code = str(body["serviceErrorCode"])
    except ValueError:
        message = (resp.text or "")[:200]

    if status in (400, 426):
        message = (message or "Bad request") + (
            f" (is LinkedIn-Version {api_version()} still supported? "
            f"Set {_VERSION_ENV} to a current one.)")
    label = f"{status} {service_code}".strip()
    return ApiError(
        ErrorCode.UPSTREAM_ERROR,
        f"LinkedIn Ads API error ({label}): {message or 'unknown error'}",
        retriable=status == 429 or status >= 500,
    )


class _AuthError(Exception):
    """Internal marker for a 401 from LinkedIn, mapped to AUTH_EXPIRED."""


class LinkedInAdsConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http
        # Entity names are re-read per account within one query; cache them so a
        # multi-account report resolves each account's campaigns only once.
        self._name_cache: Dict[str, Dict[str, str]] = {}

    # -- transport ----------------------------------------------------------

    def _call(self, method: str, url: str,
              params: Optional[Dict] = None) -> Dict[str, Any]:
        try:
            return self._http(method, url, self.access_token(), params)
        except _AuthError:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} access was rejected; reconnect the source.",
            )
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("LinkedIn Ads request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "LinkedIn Ads returned an error. Try again.",
            )

    @staticmethod
    def _account_id(account: str) -> str:
        """Accept a bare id or a `urn:li:sponsoredAccount:123`; return the id."""
        return _urn_id(account) or str(account)

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        data = self._call("GET", f"{_BASE}/adAccounts",
                          {"q": "search", "count": 100})
        accounts: List[Account] = []
        for element in data.get("elements", []):
            aid = element.get("id")
            if aid is None:
                continue
            extra = {}
            for key, out in (("status", "status"), ("type", "type")):
                if element.get(key):
                    extra[out] = element[key]
            accounts.append(Account(
                id=str(aid),
                name=element.get("name") or str(aid),
                # Carried so the dispatch layer can refuse to sum spend across
                # accounts that bill in different currencies.
                currency=element.get("currency"),
                extra=extra,
            ))
        return accounts

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(_report_for(report_type).catalogue().values())

    def _entity_names(self, report: _Report, account: str) -> Dict[str, str]:
        """`{entity_id: name}` for one account's pivot entities.

        Analytics rows name their pivot only by URN, so this is what turns
        `urn:li:sponsoredCampaign:123` into "Q3 Retargeting". Called only when a
        name field was requested, and cached per account — a report over ten
        accounts costs ten lookups, not one per row.
        """
        if report.name_field is None:
            return {}
        cache_key = f"{report.pivot}|{account}"
        cached = self._name_cache.get(cache_key)
        if cached is not None:
            return cached

        if report.name_resource is None:
            # The account's own name is already known from discovery.
            names = {a.id: a.name for a in self.list_accounts()}
        else:
            data = self._call(
                "GET",
                f"{_BASE}/adAccounts/{self._account_id(account)}/"
                f"{report.name_resource}",
                {"q": "search", "count": 1000})
            names = {}
            for element in data.get("elements", []):
                eid = _urn_id(element.get("id"))
                if eid and element.get("name"):
                    names[eid] = element["name"]

        self._name_cache[cache_key] = names
        return names

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        report = _report_for(spec.report_type)
        catalogue = report.catalogue()

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        metrics = [f for f in spec.fields if catalogue[f].kind == "metric"]
        if not metrics:
            # A report with no metric is a list of ids; default to the delivery
            # basics so an empty `fields` still answers something useful.
            metrics = [m.id for m in _DELIVERY_METRICS[:3]]
        dimensions = [f for f in spec.fields if catalogue[f].kind == "dimension"]

        # Asking for `date` is what makes this a time series. Without it
        # LinkedIn returns one aggregate row per pivot value, which is what a
        # caller comparing campaigns wants.
        daily = "date" in dimensions
        wants_name = report.name_field in dimensions

        # `pivotValues` and `dateRange` are not returned unless named in
        # `fields`, so they are requested whenever the row shape needs them.
        requested_api_fields = [*metrics, "pivotValues"]
        if daily:
            requested_api_fields.append("dateRange")

        base_params = {
            "q": "analytics",
            "pivot": report.pivot,
            "timeGranularity": "DAILY" if daily else "ALL",
            "dateRange": _date_range_param(spec.date_range.start,
                                           spec.date_range.end),
            "fields": ",".join(requested_api_fields),
            "count": min(spec.max_rows, _MAX_COUNT),
        }
        requested = list(spec.fields) or [report.id_field, *metrics]
        multi = len(spec.accounts) > 1

        def fetch(account):
            params = dict(base_params)
            params["accounts"] = f"List({_ACCOUNT_URN.format(self._account_id(account))})"
            data = self._call("GET", f"{_BASE}/adAnalytics", params)
            names = self._entity_names(report, account) if wants_name else {}
            return _parse_elements(data, report, requested, names,
                                   account, multi=multi)

        # Partial success: one account whose permissions have lapsed must not
        # sink a report across the rest of the portfolio.
        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )


def _parse_elements(data, report: _Report, requested: List[str],
                    names: Dict[str, str], account: str, *,
                    multi: bool = False) -> List[Dict[str, Any]]:
    """Map analytics elements back to field ids.

    Metrics sit at the top level of each element; the pivot entity arrives as
    `pivotValues` (a list, of which the pivot in force is the only member) and
    the day as a nested `dateRange`.
    """
    rows: List[Dict[str, Any]] = []
    for element in data.get("elements", []):
        pivot_values = element.get("pivotValues") or []
        entity_id = _urn_id(pivot_values[0]) if pivot_values else None

        record: Dict[str, Any] = {}
        if multi:
            record["_account"] = account
        for field_id in requested:
            if field_id == "date":
                record[field_id] = _iso_date(
                    (element.get("dateRange") or {}).get("start"))
            elif field_id == report.id_field:
                record[field_id] = entity_id
            elif field_id == report.name_field:
                record[field_id] = names.get(entity_id or "")
            else:
                record[field_id] = _coerce(field_id, element.get(field_id))
        rows.append(record)
    return rows


def _coerce(field_id: str, raw: Any) -> Any:
    """Numbers as numbers: LinkedIn sends money as a decimal *string*."""
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return raw
    if field_id in _DECIMAL_METRICS:
        return value
    return int(value) if value.is_integer() else value


def make_linkedin_ads_connector(datasource) -> LinkedInAdsConnector:
    """Build a LinkedIn Ads connector wired to refresh its token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return LinkedInAdsConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["LinkedInAdsConnector", "api_version", "make_linkedin_ads_connector"]
