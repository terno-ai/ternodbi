"""Google Analytics 4 connector.

Implements the `ApiConnector` interface using Google's REST APIs directly:

- `list_accounts()` — Admin API `accountSummaries.list`
- `list_fields()` — Data API `properties/{id}/metadata`
- `_run()` — Data API `properties/{id}:runReport`

HTTP is injected so the connector can be tested without a live provider.
`access_token()` handles token freshness, while 401 responses are mapped to
`AUTH_EXPIRED` so callers get a reconnect flow instead of a raw Google error.
"""

from __future__ import annotations
import logging
from typing import Any, Callable, Dict, List, Optional
from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec

logger = logging.getLogger(__name__)

_ADMIN_BASE = "https://analyticsadmin.googleapis.com/v1beta"
_DATA_BASE = "https://analyticsdata.googleapis.com/v1beta"

# GA4 does not flag non-aggregatable metrics in its metadata, so we maintain
# this list explicitly. Rates, ratios, and distinct-user counts must not be
# summed across rows.
_NON_AGGREGATABLE = frozenset({
    "activeUsers", "active1DayUsers", "active7DayUsers", "active28DayUsers",
    "totalUsers", "sessionsPerUser", "engagementRate", "bounceRate",
    "averageSessionDuration", "avgUserEngagementDuration",
    "dauPerMau", "dauPerWau", "wauPerMau",
    "screenPageViewsPerSession", "screenPageViewsPerUser",
    "crashFreeUsersRate", "engagedSessions",
    "sessionConversionRate", "userConversionRate",
    "purchaserConversionRate", "firstTimePurchaserConversionRate",
    "cartToViewRate", "purchaseToViewRate",
})


def _default_http(method: str, url: str, token: str,
                  json_body: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    resp = requests.request(
        method, url,
        headers={"Authorization": f"Bearer {token}"},
        json=json_body, timeout=30,
    )
    if resp.status_code == 401:
        raise _AuthError()
    resp.raise_for_status()
    return resp.json()


class _AuthError(Exception):
    """Internal marker for a 401 from Google, mapped to AUTH_EXPIRED."""


class GA4Connector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http
        self._metadata_cache: Optional[Dict[str, Field]] = None

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
            logger.warning("GA4 request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Google Analytics returned an error. Try again.",
            )

    @staticmethod
    def _property_path(account_id: str) -> str:
        """Accept '440705731' or 'properties/440705731'; return the latter."""
        aid = str(account_id)
        return aid if aid.startswith("properties/") else f"properties/{aid}"

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        data = self._call("GET", f"{_ADMIN_BASE}/accountSummaries")
        accounts: List[Account] = []
        for summary in data.get("accountSummaries", []):
            for prop in summary.get("propertySummaries", []):
                # 'properties/440705731' -> '440705731'
                pid = prop.get("property", "").split("/")[-1]
                if not pid:
                    continue
                accounts.append(Account(
                    id=pid,
                    name=prop.get("displayName", pid),
                    extra={"account": summary.get("displayName", "")},
                ))
        return accounts

    def _load_metadata(self) -> Dict[str, Field]:
        """Field metadata for a representative property, cached on the instance.

        GA4 metadata is per-property (custom dimensions differ), but the standard
        catalogue is shared; we fetch it once against the first accessible
        property. Custom dimensions of other properties are not surfaced here.
        """
        if self._metadata_cache is not None:
            return self._metadata_cache

        accounts = self.list_accounts()
        if not accounts:
            self._metadata_cache = {}
            return self._metadata_cache

        prop = self._property_path(accounts[0].id)
        data = self._call("GET", f"{_DATA_BASE}/{prop}/metadata")

        fields: Dict[str, Field] = {}
        for dim in data.get("dimensions", []):
            api_name = dim.get("apiName")
            if not api_name:
                continue
            fields[api_name] = Field(
                id=api_name,
                name=dim.get("uiName", api_name),
                kind="dimension",
                description=dim.get("description", ""),
                data_type="date" if api_name == "date" else "string",
                group=dim.get("category"),
            )
        for met in data.get("metrics", []):
            api_name = met.get("apiName")
            if not api_name:
                continue
            gtype = met.get("type", "")
            fields[api_name] = Field(
                id=api_name,
                name=met.get("uiName", api_name),
                kind="metric",
                description=met.get("description", ""),
                data_type=_metric_data_type(gtype),
                group=met.get("category"),
                is_non_aggregatable=api_name in _NON_AGGREGATABLE,
                is_monetary=gtype == "TYPE_CURRENCY",
            )
        self._metadata_cache = fields
        return fields

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(self._load_metadata().values())

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        meta = self._load_metadata()
        if meta:
            unknown = [f for f in spec.fields if f not in meta]
            if unknown:
                raise invalid_field(unknown[0], list(meta.keys()))

        dimensions = [f for f in spec.fields
                      if f not in meta or meta[f].kind == "dimension"]
        metrics = [f for f in spec.fields
                   if f in meta and meta[f].kind == "metric"]
        # Date-typed dimensions come back as 'YYYYMMDD'; normalise them to ISO so
        # a consumer (and the comparison layer, which parses dates) gets a real
        # date rather than a compact integer string.
        date_dims = {f for f in dimensions
                     if f in meta and meta[f].data_type == "date"}

        body = {
            "dimensions": [{"name": d} for d in dimensions],
            "metrics": [{"name": m} for m in metrics],
            "dateRanges": [{
                "startDate": spec.date_range.start,
                "endDate": spec.date_range.end,
            }],
            "limit": spec.max_rows,
        }
        # GA4 returns rows unordered by default, which reads as scrambled for a
        # time series. Order by the date dimension ascending when present;
        # otherwise by the first metric descending (largest first), the natural
        # default for a breakdown.
        order = _order_by(dimensions, metrics, date_dims)
        if order:
            body["orderBys"] = order
        if spec.report_type in ("CohortDaily", "CohortWeekly", "CohortMonthly"):
            body["cohortSpec"] = _cohort_spec(spec)

        rows: List[Dict[str, Any]] = []
        for account in spec.accounts:
            prop = self._property_path(account)
            data = self._call("POST", f"{_DATA_BASE}/{prop}:runReport", body)
            rows.extend(_parse_report_rows(data, dimensions, metrics, account,
                                           date_dims=date_dims,
                                           multi=len(spec.accounts) > 1))

        return QueryResult(
            requested_field_ids=list(spec.fields),
            rows=rows,
            row_count=len(rows),
        )


def _metric_data_type(gtype: str) -> str:
    return {
        "TYPE_INTEGER": "integer",
        "TYPE_FLOAT": "number",
        "TYPE_CURRENCY": "number",
        "TYPE_SECONDS": "number",
        "TYPE_MILLISECONDS": "number",
        "TYPE_MINUTES": "number",
        "TYPE_HOURS": "number",
        "TYPE_STANDARD": "number",
        "TYPE_PERCENT": "number",
    }.get(gtype, "string")


def _order_by(dimensions, metrics, date_dims):
    """A sensible default ordering for GA4, which otherwise returns rows unsorted.

    Time series (a date dimension present) → chronological. Otherwise a
    breakdown → largest metric first. No metric and no date → leave as-is.
    """
    for d in dimensions:
        if d in date_dims:
            return [{"dimension": {"dimensionName": d}}]        # ascending
    if metrics:
        return [{"metric": {"metricName": metrics[0]}, "desc": True}]
    return []


def _cohort_spec(spec: QuerySpec) -> Dict[str, Any]:
    granularity = {
        "CohortDaily": "DAILY",
        "CohortWeekly": "WEEKLY",
        "CohortMonthly": "MONTHLY",
    }[spec.report_type]
    return {
        "cohorts": [{
            "name": "cohort",
            "dimension": "firstSessionDate",
            "dateRange": {
                "startDate": spec.date_range.start,
                "endDate": spec.date_range.end,
            },
        }],
        "cohortsRange": {"granularity": granularity, "startOffset": 0, "endOffset": 6},
    }


def _parse_report_rows(data, dimensions, metrics, account, *, date_dims=frozenset(),
                       multi=False) -> List[Dict[str, Any]]:
    """Map GA4's positional dimension/metric values back to field ids.

    GA4 returns values in two parallel arrays (`dimensionValues`,
    `metricValues`) whose order matches the request, so alignment is by index.
    """
    result: List[Dict[str, Any]] = []
    for row in data.get("rows", []):
        record: Dict[str, Any] = {}
        if multi:
            record["_account"] = account
        dvals = row.get("dimensionValues", [])
        mvals = row.get("metricValues", [])
        for i, name in enumerate(dimensions):
            value = dvals[i].get("value") if i < len(dvals) else None
            if name in date_dims:
                value = _iso_date(value)
            record[name] = value
        for i, name in enumerate(metrics):
            record[name] = _coerce_number(
                mvals[i].get("value") if i < len(mvals) else None)
        result.append(record)
    return result


def _iso_date(value):
    """'20260819' -> '2026-08-19'. Leaves anything unexpected untouched."""
    if isinstance(value, str) and len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return value


def _coerce_number(raw):
    if raw is None:
        return None
    try:
        f = float(raw)
        return int(f) if f.is_integer() else f
    except (TypeError, ValueError):
        return raw


def make_ga4_connector(datasource) -> GA4Connector:
    """Build a GA4 connector wired to refresh its own OAuth token when due.

    The refresher is bound here (not in the model layer) so `access_token()`
    keeps the token fresh on every provider call, whatever the caller.
    """
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return GA4Connector(datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["GA4Connector", "make_ga4_connector"]
