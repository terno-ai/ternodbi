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
import re
from typing import Any, Callable, Dict, List, Optional
from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import (
    Account,
    Action,
    ActionResult,
    Field,
    QueryResult,
    QuerySpec,
)
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
    # -- delivery & cost ----------------------------------------------------
    Field("metrics.impressions", "Impressions", "metric",
          "Times an ad was shown.", data_type="integer"),
    Field("metrics.clicks", "Clicks", "metric", "Ad clicks.",
          data_type="integer"),
    Field("metrics.interactions", "Interactions", "metric",
          "Primary interactions (clicks, video views, etc.).",
          data_type="integer"),
    Field("metrics.interaction_rate", "Interaction rate", "metric",
          "Interactions / impressions.", data_type="number",
          is_non_aggregatable=True),
    Field("metrics.ctr", "CTR", "metric",
          "Click-through rate (clicks / impressions).", data_type="number",
          is_non_aggregatable=True),
    Field("metrics.cost_micros", "Cost", "metric",
          "Spend for the row (converted from micros).", data_type="number",
          is_monetary=True),
    Field("metrics.average_cpc", "Avg. CPC", "metric",
          "Average cost per click (converted from micros).", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    Field("metrics.average_cpm", "Avg. CPM", "metric",
          "Average cost per thousand impressions (from micros).",
          data_type="number", is_monetary=True, is_non_aggregatable=True),
    Field("metrics.average_cost", "Avg. cost", "metric",
          "Average cost per interaction (from micros).", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    # -- conversions --------------------------------------------------------
    Field("metrics.conversions", "Conversions", "metric",
          "Attributed conversions.", data_type="number"),
    Field("metrics.conversions_value", "Conversion value", "metric",
          "Total value of conversions (already in currency).",
          data_type="number", is_monetary=True),
    Field("metrics.all_conversions", "All conversions", "metric",
          "Conversions incl. those not counted in 'Conversions'.",
          data_type="number"),
    Field("metrics.all_conversions_value", "All conversion value", "metric",
          "Value of all conversions (in currency).", data_type="number",
          is_monetary=True),
    Field("metrics.conversions_from_interactions_rate", "Conversion rate",
          "metric", "Conversions / interactions.", data_type="number",
          is_non_aggregatable=True),
    Field("metrics.cost_per_conversion", "Cost / conversion", "metric",
          "Average cost per conversion (from micros).", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    Field("metrics.cost_per_all_conversions", "Cost / all conversions", "metric",
          "Average cost per all-conversion (from micros).", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    Field("metrics.value_per_conversion", "Value / conversion", "metric",
          "Average value per conversion (in currency).", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    Field("metrics.value_per_all_conversions", "Value / all conversions",
          "metric", "Average value per all-conversion (in currency).",
          data_type="number", is_monetary=True, is_non_aggregatable=True),
    Field("metrics.view_through_conversions", "View-through conversions",
          "metric", "Conversions from impressions (no click).",
          data_type="integer"),
    # -- video / TrueView ---------------------------------------------------
    Field("metrics.video_views", "Video views", "metric",
          "Number of video-ad views.", data_type="integer"),
    Field("metrics.video_view_rate", "Video view rate", "metric",
          "TrueView view rate (video views / impressions).", data_type="number",
          is_non_aggregatable=True),
    Field("metrics.average_cpv", "Avg. CPV", "metric",
          "Average cost per video view (from micros).", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    Field("metrics.video_quartile_p25_rate", "Video played 25%", "metric",
          "Share who watched to 25%.", data_type="number",
          is_non_aggregatable=True),
    Field("metrics.video_quartile_p50_rate", "Video played 50%", "metric",
          "Share who watched to 50%.", data_type="number",
          is_non_aggregatable=True),
    Field("metrics.video_quartile_p75_rate", "Video played 75%", "metric",
          "Share who watched to 75%.", data_type="number",
          is_non_aggregatable=True),
    Field("metrics.video_quartile_p100_rate", "Video played 100%", "metric",
          "Share who watched to 100%.", data_type="number",
          is_non_aggregatable=True),
    # -- engagement ---------------------------------------------------------
    Field("metrics.engagements", "Engagements", "metric",
          "Ad engagements.", data_type="integer"),
    Field("metrics.engagement_rate", "Engagement rate", "metric",
          "Engagements / impressions.", data_type="number",
          is_non_aggregatable=True),
    # -- impression share (campaign/ad-group level) -------------------------
    Field("metrics.search_impression_share", "Search impr. share", "metric",
          "Impressions received / eligible (0-1).", data_type="number",
          is_non_aggregatable=True),
    Field("metrics.search_budget_lost_impression_share",
          "Search lost IS (budget)", "metric",
          "Share of impressions lost to budget.", data_type="number",
          is_non_aggregatable=True),
    Field("metrics.search_rank_lost_impression_share",
          "Search lost IS (rank)", "metric",
          "Share of impressions lost to Ad Rank.", data_type="number",
          is_non_aggregatable=True),
    Field("metrics.search_top_impression_share", "Search top IS", "metric",
          "Share of impressions in top location.", data_type="number",
          is_non_aggregatable=True),
    Field("metrics.search_absolute_top_impression_share",
          "Search abs. top IS", "metric",
          "Share of impressions in the very first position.",
          data_type="number", is_non_aggregatable=True),
    # -- viewability (Active View) ------------------------------------------
    Field("metrics.active_view_impressions", "Viewable impressions", "metric",
          "Active View measurable & viewable impressions.",
          data_type="integer"),
    Field("metrics.active_view_ctr", "Active View CTR", "metric",
          "Clicks / viewable impressions.", data_type="number",
          is_non_aggregatable=True),
    Field("metrics.active_view_viewability", "Viewability", "metric",
          "Viewable / measurable impressions.", data_type="number",
          is_non_aggregatable=True),
    Field("metrics.active_view_cpm", "Active View CPM", "metric",
          "Cost per thousand viewable impressions (from micros).",
          data_type="number", is_monetary=True, is_non_aggregatable=True),
]

_SHARED_SEGMENTS: List[Field] = [
    Field("segments.date", "Date", "dimension", "Day the stat occurred.",
          data_type="date"),
    Field("segments.day_of_week", "Day of week", "dimension",
          "MONDAY … SUNDAY."),
    Field("segments.week", "Week", "dimension",
          "Monday of the week the stat occurred.", data_type="date"),
    Field("segments.month", "Month", "dimension",
          "First day of the month.", data_type="date"),
    Field("segments.quarter", "Quarter", "dimension",
          "First day of the quarter.", data_type="date"),
    Field("segments.year", "Year", "dimension", "Year of the stat.",
          data_type="integer"),
    Field("segments.hour", "Hour", "dimension", "Hour of day (0-23).",
          data_type="integer"),
    Field("segments.device", "Device", "dimension",
          "Device class: MOBILE, DESKTOP, TABLET, CONNECTED_TV."),
    Field("segments.ad_network_type", "Network", "dimension",
          "SEARCH, SEARCH_PARTNERS, CONTENT, YOUTUBE_*, MIXED."),
    Field("segments.click_type", "Click type", "dimension",
          "The type of click (e.g. headline, sitelink)."),
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
# Google Ads returns these money metrics in micros with no naming convention to
# detect them by, so the set is explicit. `conversions_value`, `value_per_*` and
# the other monetary fields are already in currency and must NOT be listed here.
_MICROS_FIELDS = frozenset({
    "metrics.cost_micros",
    "metrics.average_cpc",
    "metrics.average_cpm",
    "metrics.average_cost",
    "metrics.average_cpv",
    "metrics.cost_per_conversion",
    "metrics.cost_per_all_conversions",
    "metrics.active_view_cpm",
})


# Monetary fields already in the account currency (NOT micros) — flagged as money
# but never divided. Dynamically-discovered fields use this for the monetary flag.
_CURRENCY_FIELDS = frozenset({
    "metrics.conversions_value",
    "metrics.all_conversions_value",
    "metrics.value_per_conversion",
    "metrics.value_per_all_conversions",
    "metrics.current_model_attributed_conversions_value",
})

# Segment fields that carry a date; everything else defaults to string/integer.
_DATE_SEGMENTS = frozenset({
    "segments.date", "segments.week", "segments.month", "segments.quarter",
})
_INTEGER_SEGMENTS = frozenset({"segments.hour", "segments.year"})

# Curated Field objects keyed by id — used to give a discovered field a nice
# label/description/flags when we have one, and as the static fallback catalogue.
_CURATED_BY_ID: Dict[str, Field] = {
    f.id: f for f in (*_SHARED_METRICS, *_SHARED_SEGMENTS)
}

# Substrings that mark a metric as a ratio/average (never summable across rows).
_RATIO_TOKENS = ("rate", "average", "_per_", "share", "percent", "ctr",
                 "cpc", "cpm", "cpv", "viewability")


def _static_fields_for(report_type: Optional[str]) -> Dict[str, Field]:
    """The curated catalogue — the fallback when field discovery is unavailable."""
    rt = report_type if report_type in _RESOURCE else _DEFAULT_REPORT
    fields = [*_REPORT_DIMENSIONS[rt], *_SHARED_SEGMENTS, *_SHARED_METRICS]
    return {f.id: f for f in fields}


def _prettify(field_id: str) -> str:
    """'metrics.video_view_rate' -> 'Video view rate' for a discovered field."""
    leaf = field_id.split(".")[-1]
    return leaf.replace("_", " ").strip().capitalize() or field_id


_MICROS_TOKENS = ("_cpc", "_cpm", "_cpv", "cost_per_", "average_cost")


def _is_micros(field_id: str) -> bool:
    # Suffixed micros are unambiguous; the explicit set + the cost-token heuristic
    # cover the money metrics that carry no `_micros` suffix.
    if field_id in _MICROS_FIELDS or field_id.endswith("_micros"):
        return True
    leaf = field_id.split(".")[-1]
    return any(tok in leaf for tok in _MICROS_TOKENS)


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _validate_dates(start: str, end: str) -> None:
    """Reject a non-absolute date range with an actionable message.

    GAQL's BETWEEN needs two 'YYYY-MM-DD' literals; a relative token like 'today'
    reaches the API as an invalid value and returns a cryptic 400. Callers must
    resolve relative ranges (via get_today) before querying — enforce that here
    so the error names the real problem.
    """
    for label, val in (("start", start), ("end", end)):
        if not (isinstance(val, str) and _DATE_RE.match(val)):
            raise ApiError(
                ErrorCode.INVALID_FILTER,
                f"date_range.{label} must be an absolute 'YYYY-MM-DD' date, got "
                f"{val!r}. Resolve relative ranges (e.g. 'today', 'last 30 days') "
                f"with get_today first.",
                retriable=False,
            )
    if start > end:
        raise ApiError(
            ErrorCode.INVALID_FILTER,
            f"date_range.start ({start}) is after date_range.end ({end}).",
            retriable=False,
        )


def _dynamic_metric_field(field_id: str) -> Field:
    if field_id in _CURATED_BY_ID:
        return _CURATED_BY_ID[field_id]
    leaf = field_id.split(".")[-1]
    monetary = _is_micros(field_id) or field_id in _CURRENCY_FIELDS
    non_agg = any(tok in leaf for tok in _RATIO_TOKENS)
    return Field(field_id, _prettify(field_id), "metric", "",
                 data_type="number", is_monetary=monetary,
                 is_non_aggregatable=non_agg)


def _dynamic_segment_field(field_id: str) -> Field:
    if field_id in _CURATED_BY_ID:
        return _CURATED_BY_ID[field_id]
    if field_id in _DATE_SEGMENTS:
        data_type = "date"
    elif field_id in _INTEGER_SEGMENTS:
        data_type = "integer"
    else:
        data_type = "string"
    return Field(field_id, _prettify(field_id), "dimension", "",
                 data_type=data_type)


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


def _id_prop(entity: str) -> Dict[str, Any]:
    return {"type": "string",
            "description": f"Numeric {entity} ID (digits only)."}


# The write actions this connector exposes. Deliberately narrow and safe: pause
# /enable and budget changes — the high-value, well-understood verbs both
# Supermetrics and Windsor lead with. No hard delete: turning something off is
# `pause_*`, never REMOVED. `enable_*` is called out as spend-starting so a
# caller (and the confirmation UI) treats it as the deliberate step it is.
_ACTIONS: List[Action] = [
    Action(
        "pause_campaign", "Pause campaign",
        "Pause a campaign so it stops serving and spending. Reversible with "
        "enable_campaign.",
        schema={"type": "object",
                "properties": {"campaign_id": _id_prop("campaign")},
                "required": ["campaign_id"], "additionalProperties": False},
    ),
    Action(
        "enable_campaign", "Enable campaign",
        "Enable (unpause) a campaign so it can serve. This starts spend — treat "
        "it as a deliberate, separately confirmed step.",
        schema={"type": "object",
                "properties": {"campaign_id": _id_prop("campaign")},
                "required": ["campaign_id"], "additionalProperties": False},
    ),
    Action(
        "set_campaign_budget", "Set campaign budget",
        "Set a campaign's daily budget, in the account's own currency (e.g. 50 "
        "means 50.00/day). Affects spend.",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign"),
                    "amount": {"type": "number", "exclusiveMinimum": 0,
                               "description": "New daily budget in account "
                                              "currency units, e.g. 50 for 50.00."},
                },
                "required": ["campaign_id", "amount"],
                "additionalProperties": False},
    ),
    Action(
        "pause_ad_group", "Pause ad group",
        "Pause an ad group so it stops serving. Reversible with enable_ad_group.",
        schema={"type": "object",
                "properties": {"ad_group_id": _id_prop("ad group")},
                "required": ["ad_group_id"], "additionalProperties": False},
    ),
    Action(
        "enable_ad_group", "Enable ad group",
        "Enable (unpause) an ad group. Starts serving when its campaign is live.",
        schema={"type": "object",
                "properties": {"ad_group_id": _id_prop("ad group")},
                "required": ["ad_group_id"], "additionalProperties": False},
    ),
]
_ACTIONS_BY_ID: Dict[str, Action] = {a.id: a for a in _ACTIONS}


class _AuthError(Exception):
    """Internal marker for a 401 from Google, mapped to AUTH_EXPIRED."""


class GoogleAdsConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http
        # Field catalogue per report type, cached for the connector's lifetime.
        self._catalogue_cache: Dict[str, Dict[str, Field]] = {}
        # Per-field `selectable_with` sets (None = unknown), for compatibility
        # pre-validation. Cached so repeated queries don't refetch metadata.
        self._selectable_cache: Dict[str, Optional[set]] = {}

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
        return list(self._resource_catalogue(report_type).values())

    def _resource_catalogue(self, report_type: Optional[str]) -> Dict[str, Field]:
        """The field catalogue for a report's resource.

        Metrics and segments are discovered live from GoogleAdsFieldService so
        coverage tracks the API (every metric selectable with the resource — video
        /TrueView, impression share, etc. — with no maintenance). The resource's
        identifying attributes stay curated (stable and few). If discovery fails
        for any reason, the whole catalogue falls back to the curated static set,
        so the connector never goes dark over a metadata hiccup.
        """
        rt = report_type if report_type in _RESOURCE else _DEFAULT_REPORT
        cached = self._catalogue_cache.get(rt)
        if cached is not None:
            return cached
        try:
            metric_names, segment_names = self._discover_fields(_RESOURCE[rt])
        except Exception as exc:   # noqa: BLE001
            logger.warning(
                "Google Ads field discovery failed for %s (%s); using the "
                "curated catalogue.", _RESOURCE[rt], exc)
            catalogue = _static_fields_for(rt)
            self._catalogue_cache[rt] = catalogue
            return catalogue

        # Attributes stay curated; metrics/segments come from discovery.
        catalogue: Dict[str, Field] = {f.id: f for f in _REPORT_DIMENSIONS[rt]}
        for name in segment_names:
            catalogue[name] = _dynamic_segment_field(name)
        for name in metric_names:
            catalogue[name] = _dynamic_metric_field(name)
        self._catalogue_cache[rt] = catalogue
        return catalogue

    def _discover_fields(self, resource: str) -> tuple:
        """`(metric_names, segment_names)` selectable with a resource.

        A GoogleAdsField RESOURCE row carries the full list of metric and segment
        field names selectable with it — so this is exactly the compatible set,
        not a guess.
        """
        data = self._call(
            "POST", f"{_BASE}/googleAdsFields:search",
            {"query": f"SELECT name, metrics, segments WHERE name = '{resource}'"})
        results = data.get("results") or []
        if not results:
            raise ApiError(ErrorCode.UPSTREAM_ERROR,
                           f"no field metadata for {resource}")
        row = results[0]
        metrics = list(row.get("metrics") or [])
        segments = list(row.get("segments") or [])
        if not metrics and not segments:
            raise ApiError(ErrorCode.UPSTREAM_ERROR,
                           f"empty field metadata for {resource}")
        return metrics, segments

    def _selectable_with(self, names: List[str]) -> Dict[str, Optional[set]]:
        """`{name: set(selectable_with) | None}` for each field.

        None means the field service did not return metadata for the name, so
        compatibility for it is unknown and must not be treated as a conflict.
        Batched and cached; a query only ever looks up the fields it selected.
        """
        missing = [n for n in names if n not in self._selectable_cache]
        if missing:
            in_list = ", ".join(f"'{n}'" for n in missing)
            data = self._call(
                "POST", f"{_BASE}/googleAdsFields:search",
                {"query": f"SELECT name, selectable_with WHERE name IN ({in_list})"})
            for row in data.get("results") or []:
                nm = row.get("name")
                if nm:
                    self._selectable_cache[nm] = set(row.get("selectableWith") or [])
            for n in missing:                      # unresolved -> unknown
                self._selectable_cache.setdefault(n, None)
        return {n: self._selectable_cache.get(n) for n in names}

    def _check_compatibility(self, segments: List[str], metrics: List[str]) -> None:
        """Pre-validate metric↔segment combinations against `selectable_with`.

        Google rejects some metric+segment pairs (e.g. in-feed TrueView rates with
        `segments.date`) with a cryptic 400; catching it here turns that into an
        actionable message. Only metric↔segment is checked — a metric's
        `selectable_with` enumerates its compatible *segments/attributes*, not
        other metrics, so a metric↔metric comparison would false-positive on
        ordinary combinations (e.g. cost with a video rate) and is deliberately
        not attempted.

        Best-effort and false-positive-safe: a pair is flagged only when *both*
        fields have a non-empty compatibility set and neither lists the other,
        and any metadata-lookup failure is skipped so a hiccup never blocks a
        valid query.
        """
        if not (segments and metrics):
            return
        try:
            compat = self._selectable_with([*segments, *metrics])
        except Exception as exc:   # noqa: BLE001
            logger.warning("Google Ads compatibility check skipped: %s", exc)
            return

        problems: List[tuple] = []
        for seg in segments:
            sw_seg = compat.get(seg)
            for met in metrics:
                sw_met = compat.get(met)
                if sw_seg and sw_met and met not in sw_seg and seg not in sw_met:
                    problems.append((met, seg))
        if problems:
            pairs = "; ".join(f"'{m}' with '{s}'" for m, s in problems[:6])
            raise ApiError(
                ErrorCode.INVALID_FILTER,
                "These Google Ads metrics can't be selected with the chosen "
                "segment: " + pairs + ". Remove the segment for those metrics, "
                "or split them into separate queries.",
                retriable=False,
            )

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = spec.report_type if spec.report_type in _RESOURCE else _DEFAULT_REPORT
        catalogue = self._resource_catalogue(report_type)

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        _validate_dates(spec.date_range.start, spec.date_range.end)

        dimensions = [f for f in spec.fields if catalogue[f].kind == "dimension"]
        metrics = [f for f in spec.fields if catalogue[f].kind == "metric"]
        if not (dimensions or metrics):
            # A report with no selected fields is a dead end; default to the
            # report's core metrics.
            metrics = [m.id for m in _SHARED_METRICS[:3]]

        segments = [d for d in dimensions if d.startswith("segments.")]
        self._check_compatibility(segments, metrics)

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

    # -- write actions ------------------------------------------------------

    def list_actions(self) -> List[Action]:
        return list(_ACTIONS)

    def execute_action(
        self, action_id: str, account: str, params: Optional[Dict[str, Any]] = None
    ) -> ActionResult:
        """Perform one write action. Account authorisation happens upstream.

        Read-before-write: every handler first reads the entity's current state
        (which also validates the id exists) and returns it as `before`, so the
        change is auditable and a stale target fails cleanly rather than mutating
        the wrong thing.
        """
        params = params or {}
        if action_id not in _ACTIONS_BY_ID:
            raise ApiError(
                ErrorCode.UNKNOWN_ACTION,
                f"Unknown action {action_id!r}. Call list_actions for the "
                f"available actions.",
                retriable=False,
                details={"action": action_id,
                         "available": sorted(_ACTIONS_BY_ID)},
            )
        cid = self._customer_id(account)
        if action_id == "pause_campaign":
            return self._set_campaign_status(cid, account, params, "PAUSED")
        if action_id == "enable_campaign":
            return self._set_campaign_status(cid, account, params, "ENABLED")
        if action_id == "set_campaign_budget":
            return self._set_campaign_budget(cid, account, params)
        if action_id == "pause_ad_group":
            return self._set_ad_group_status(cid, account, params, "PAUSED")
        if action_id == "enable_ad_group":
            return self._set_ad_group_status(cid, account, params, "ENABLED")
        # Unreachable: every id in _ACTIONS_BY_ID is handled above.
        raise ApiError(ErrorCode.UNKNOWN_ACTION,
                       f"Action {action_id!r} is declared but not implemented.",
                       retriable=False)

    def _require_id(self, params: Dict[str, Any], key: str) -> str:
        raw = params.get(key)
        if raw is None or not str(raw).strip():
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"Missing required parameter {key!r}.",
                           retriable=False, details={"param": key})
        digits = str(raw).strip()
        if not digits.isdigit():
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"{key!r} must be a numeric id, got {raw!r}.",
                           retriable=False, details={"param": key})
        return digits

    def _search_one(self, cid: str, gaql: str) -> Optional[Dict[str, Any]]:
        data = self._call("POST", f"{_BASE}/customers/{cid}/googleAds:search",
                          {"query": gaql})
        results = data.get("results", [])
        return results[0] if results else None

    def _mutate(self, cid: str, collection: str, operation: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{_BASE}/customers/{cid}/{collection}:mutate"
        return self._call("POST", url, {"operations": [operation]})

    def _set_campaign_status(self, cid, account, params, status) -> ActionResult:
        campaign_id = self._require_id(params, "campaign_id")
        row = self._search_one(
            cid,
            f"SELECT campaign.id, campaign.name, campaign.status "
            f"FROM campaign WHERE campaign.id = {campaign_id}",
        )
        if row is None:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"Campaign {campaign_id} was not found in this account.",
                           retriable=False, details={"campaign_id": campaign_id})
        camp = row.get("campaign", {})
        before = {"id": campaign_id, "name": camp.get("name"),
                  "status": camp.get("status")}
        self._mutate(cid, "campaigns", {
            "updateMask": "status",
            "update": {
                "resourceName": f"customers/{cid}/campaigns/{campaign_id}",
                "status": status,
            },
        })
        after = {**before, "status": status}
        verb = "paused" if status == "PAUSED" else "enabled"
        return ActionResult(
            action=("pause_campaign" if status == "PAUSED" else "enable_campaign"),
            account=account,
            summary=f"Campaign {camp.get('name') or campaign_id} {verb}.",
            before=before, after=after,
        )

    def _set_ad_group_status(self, cid, account, params, status) -> ActionResult:
        ad_group_id = self._require_id(params, "ad_group_id")
        row = self._search_one(
            cid,
            f"SELECT ad_group.id, ad_group.name, ad_group.status "
            f"FROM ad_group WHERE ad_group.id = {ad_group_id}",
        )
        if row is None:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"Ad group {ad_group_id} was not found in this account.",
                           retriable=False, details={"ad_group_id": ad_group_id})
        ag = row.get("adGroup", {})
        before = {"id": ad_group_id, "name": ag.get("name"),
                  "status": ag.get("status")}
        self._mutate(cid, "adGroups", {
            "updateMask": "status",
            "update": {
                "resourceName": f"customers/{cid}/adGroups/{ad_group_id}",
                "status": status,
            },
        })
        after = {**before, "status": status}
        verb = "paused" if status == "PAUSED" else "enabled"
        return ActionResult(
            action=("pause_ad_group" if status == "PAUSED" else "enable_ad_group"),
            account=account,
            summary=f"Ad group {ag.get('name') or ad_group_id} {verb}.",
            before=before, after=after,
        )

    def _set_campaign_budget(self, cid, account, params) -> ActionResult:
        campaign_id = self._require_id(params, "campaign_id")
        amount = params.get("amount")
        if not isinstance(amount, (int, float)) or isinstance(amount, bool) or amount <= 0:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "'amount' must be a positive number (account currency "
                           "units, e.g. 50 for 50.00).",
                           retriable=False, details={"param": "amount"})
        row = self._search_one(
            cid,
            f"SELECT campaign.id, campaign.name, campaign.campaign_budget, "
            f"campaign_budget.amount_micros FROM campaign "
            f"WHERE campaign.id = {campaign_id}",
        )
        if row is None:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"Campaign {campaign_id} was not found in this account.",
                           retriable=False, details={"campaign_id": campaign_id})
        budget_res = (row.get("campaign", {}) or {}).get("campaignBudget")
        if not budget_res:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"Campaign {campaign_id} has no editable budget "
                           f"(it may use a shared or portfolio budget).",
                           retriable=False, details={"campaign_id": campaign_id})
        old_micros = (row.get("campaignBudget", {}) or {}).get("amountMicros")
        micros = int(round(float(amount) * 1_000_000))
        self._mutate(cid, "campaignBudgets", {
            "updateMask": "amount_micros",
            "update": {"resourceName": budget_res, "amountMicros": micros},
        })

        def _to_units(m):
            try:
                return float(m) / 1_000_000
            except (TypeError, ValueError):
                return None
        before = {"campaign_id": campaign_id,
                  "name": (row.get("campaign", {}) or {}).get("name"),
                  "budget_resource": budget_res,
                  "amount": _to_units(old_micros)}
        after = {**before, "amount": amount}
        return ActionResult(
            action="set_campaign_budget", account=account,
            summary=(f"Budget for campaign "
                     f"{(row.get('campaign', {}) or {}).get('name') or campaign_id} "
                     f"set to {amount}."),
            before=before, after=after,
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
    if _is_micros(field_id):
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
