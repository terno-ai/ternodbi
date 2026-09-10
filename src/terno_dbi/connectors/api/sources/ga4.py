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
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

_ADMIN_BASE = "https://analyticsadmin.googleapis.com/v1beta"
_DATA_BASE = "https://analyticsdata.googleapis.com/v1beta"
_DATA_ALPHA_BASE = "https://analyticsdata.googleapis.com/v1alpha"

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

_REALTIME_FIELDS: Dict[str, Field] = {f.id: f for f in (
    Field("unifiedScreenName", "Screen/page name", "dimension"),
    Field("eventName", "Event name", "dimension"),
    Field("country", "Country", "dimension"),
    Field("city", "City", "dimension"),
    Field("deviceCategory", "Device category", "dimension"),
    Field("platform", "Platform", "dimension"),
    Field("audienceName", "Audience name", "dimension"),
    Field("minutesAgo", "Minutes ago", "dimension",
          "How many minutes ago the activity happened (0-29)."),
    Field("activeUsers", "Active users", "metric", data_type="integer"),
    Field("screenPageViews", "Screen/page views", "metric",
          data_type="integer"),
    Field("eventCount", "Event count", "metric", data_type="integer"),
    Field("conversions", "Conversions", "metric", data_type="number"),
    Field("keyEvents", "Key events", "metric", data_type="integer"),
)}


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
        self._metadata_cache: Dict[str, Dict[str, Field]] = {}

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

    def _property_metadata(self, account_id: str) -> Dict[str, Field]:
        """Field metadata for one property, cached per property.

        The standard catalogue (~200 dimensions/metrics) is identical across
        properties, but custom dimensions/metrics (customEvent:*, customUser:*)
        are defined per property. Caching by property keeps each property's custom
        fields distinct, so both discovery and validation are correct for every
        property — not just the first accessible one.
        """
        prop = self._property_path(account_id)
        cached = self._metadata_cache.get(prop)
        if cached is not None:
            return cached

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
        self._metadata_cache[prop] = fields
        return fields

    def _merged_metadata(self, account_ids: Optional[List[str]] = None) -> Dict[str, Field]:
        """Union of field metadata across properties (all accessible ones when
        `account_ids` is None). Standard fields dedupe; every property's custom
        dimensions are included, so discovery surfaces the full catalogue."""
        if account_ids is None:
            account_ids = [a.id for a in self.list_accounts()]
        merged: Dict[str, Field] = {}
        for aid in account_ids:
            merged.update(self._property_metadata(aid))
        return merged

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        if report_type == "Realtime":
            return list(_REALTIME_FIELDS.values())
        if report_type == "Funnel":
            return []
        return list(self._merged_metadata().values())

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        if spec.report_type == "Realtime":
            return self._run_realtime(spec)
        if spec.report_type == "Funnel":
            return self._run_funnel(spec)

        meta_by_account = {a: self._property_metadata(a) for a in spec.accounts}
        for meta in meta_by_account.values():
            if not meta:
                continue
            unknown = [f for f in spec.fields if f not in meta]
            if unknown:
                raise invalid_field(unknown[0], list(meta.keys()))

        meta: Dict[str, Field] = {}
        for m in meta_by_account.values():
            meta.update(m)

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

        multi = len(spec.accounts) > 1

        def fetch(account):
            prop = self._property_path(account)
            data = self._call("POST", f"{_DATA_BASE}/{prop}:runReport", body)
            return _parse_report_rows(data, dimensions, metrics, account,
                                      date_dims=date_dims, multi=multi)

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=list(spec.fields),
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )

    def _run_realtime(self, spec: QuerySpec) -> QueryResult:
        """`runRealtimeReport` — activity from the last ~30 minutes.

        Uses the curated realtime catalogue rather than property metadata: the
        realtime surface accepts only a fixed subset of fields, and GA4's
        metadata endpoint does not flag which ones. No date range applies —
        `minuteRanges` is fixed at "the last 30 minutes" (the API's own default
        window), matching what an agent means by "right now".
        """
        unknown = [f for f in spec.fields if f not in _REALTIME_FIELDS]
        if unknown:
            raise invalid_field(unknown[0], list(_REALTIME_FIELDS.keys()))

        dimensions = [f for f in spec.fields
                      if _REALTIME_FIELDS[f].kind == "dimension"]
        metrics = [f for f in spec.fields
                   if _REALTIME_FIELDS[f].kind == "metric"]
        if not metrics:
            metrics = ["activeUsers"]

        body = {
            "dimensions": [{"name": d} for d in dimensions],
            "metrics": [{"name": m} for m in metrics],
            "limit": spec.max_rows,
        }
        order = _order_by(dimensions, metrics, date_dims=frozenset())
        if order:
            body["orderBys"] = order

        multi = len(spec.accounts) > 1

        def fetch(account):
            prop = self._property_path(account)
            data = self._call("POST", f"{_DATA_BASE}/{prop}:runRealtimeReport", body)
            return _parse_report_rows(data, dimensions, metrics, account,
                                      multi=multi)

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=list(spec.fields) or [*dimensions, *metrics],
            rows=rows,
            row_count=len(rows),
            notes=["Realtime data covers roughly the last 30 minutes and is "
                   "approximate; it is not comparable to a standard report."],
            warnings=warnings,
        )

    def _run_funnel(self, spec: QuerySpec) -> QueryResult:
        """`runFunnelReport` (Data API v1alpha) — step-by-step drop-off.

        `funnel_steps` is a required setting (validated generically before
        `_run` is reached): a list of `{"name": ..., "event_name": ...}`
        objects defining each step as an event-name filter, in order. The
        output shape (step name, active users, completion rate) is fixed by
        the API, not by `spec.fields` — a funnel is not a field-selection
        report.
        """
        steps = spec.settings.get("funnel_steps")
        if not isinstance(steps, list) or len(steps) < 2 or not all(
            isinstance(s, dict) and s.get("event_name") for s in steps
        ):
            raise ApiError(
                ErrorCode.INVALID_SETTING,
                "funnel_steps must be a list of at least two "
                "{name, event_name} objects, one per funnel step.",
                details={"setting_id": "funnel_steps"},
            )

        funnel = {
            "steps": [
                {
                    "name": step.get("name") or f"Step {i + 1}",
                    "filterExpression": {
                        "funnelEventFilter": {"eventName": step["event_name"]},
                    },
                }
                for i, step in enumerate(steps)
            ],
        }
        body = {
            "funnel": funnel,
            "dateRange": {
                "startDate": spec.date_range.start,
                "endDate": spec.date_range.end,
            },
        }

        multi = len(spec.accounts) > 1
        field_ids: List[str] = []

        def fetch(account):
            nonlocal field_ids
            prop = self._property_path(account)
            data = self._call("POST", f"{_DATA_ALPHA_BASE}/{prop}:runFunnelReport", body)
            parsed, field_ids = _parse_funnel_rows(data, account, multi=multi)
            return parsed

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=field_ids,
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
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


def _parse_funnel_rows(data, account, *, multi=False):
    """Map a `runFunnelReport` response back to records.

    The funnel's output columns (step name, active users, completion rate, …)
    are decided by the API, not by the caller, so — unlike a standard report —
    field ids are read from the response's own headers rather than from a
    requested field list.
    """
    table = data.get("funnelTable", {})
    dim_names = [h.get("name") for h in table.get("dimensionHeaders", [])]
    met_names = [h.get("name") for h in table.get("metricHeaders", [])]

    result: List[Dict[str, Any]] = []
    for row in table.get("rows", []):
        record: Dict[str, Any] = {}
        if multi:
            record["_account"] = account
        dvals = row.get("dimensionValues", [])
        mvals = row.get("metricValues", [])
        for i, name in enumerate(dim_names):
            record[name] = dvals[i].get("value") if i < len(dvals) else None
        for i, name in enumerate(met_names):
            record[name] = _coerce_number(
                mvals[i].get("value") if i < len(mvals) else None)
        result.append(record)
    return result, [*dim_names, *met_names]


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
