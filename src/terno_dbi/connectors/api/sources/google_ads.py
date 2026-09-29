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
import hashlib
import logging
import os
import re
import uuid
from dataclasses import replace
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
# Manager (MCC) customer id to send as `login-customer-id`. Required when the
# OAuth user reaches a client account *through* a manager account (agencies, and
# test client accounts under a test manager). Digits only, no hyphens.
_LOGIN_CUSTOMER_ID_ENV = "TERNO_GOOGLE_ADS_LOGIN_CUSTOMER_ID"

# Sentinel: "caller passed no login_customer_id override" (distinct from "" which
# means reach the account directly with no login-customer-id header).
_UNSET = object()

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
                  json_body: Optional[Dict] = None,
                  login_customer_id: Optional[str] = None) -> Dict[str, Any]:
    import requests
    headers = {"Authorization": f"Bearer {token}"}
    dev_token = os.getenv(_DEVELOPER_TOKEN_ENV, "").strip()
    if dev_token:
        headers["developer-token"] = dev_token

    login_cid = (str(login_customer_id or "").strip()
                 or os.getenv(_LOGIN_CUSTOMER_ID_ENV, "").strip()).replace("-", "")
    if login_cid:
        headers["login-customer-id"] = login_cid
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
    code_name, message, field_path = "", "", ""
    try:
        err = (resp.json() or {}).get("error", {})
        message = err.get("message", "")
        for detail in err.get("details", []):
            for e in detail.get("errors", []):
                ec = e.get("errorCode", {})
                if isinstance(ec, dict) and ec:
                    code_name = next(iter(ec.values()))
                message = e.get("message", message)
                elems = (e.get("location", {}) or {}).get("fieldPathElements", [])
                parts = [str(p.get("fieldName", "")) for p in elems if p.get("fieldName")]
                field_path = ".".join(parts)
                break
            if code_name:
                break
    except ValueError:
        message = (resp.text or "")[:200]   # non-JSON (e.g. a 404 HTML page)

    if field_path:
        message = f"{message} [field: {field_path}]"

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
    Action(
        "create_campaign", "Create campaign",
        "Create a new Search campaign with its own daily budget. Always created "
        "PAUSED — nothing serves or spends until you separately enable it with "
        "enable_campaign. Uses manual CPC bidding by default.",
        schema={"type": "object",
                "properties": {
                    "name": {"type": "string", "minLength": 1,
                             "description": "Campaign name (must be unique in the account)."},
                    "daily_budget": {"type": "number", "exclusiveMinimum": 0,
                                     "description": "Daily budget in account currency "
                                                    "units, e.g. 50 for 50.00."},
                },
                "required": ["name", "daily_budget"],
                "additionalProperties": False},
    ),
    Action(
        "add_keywords", "Add keywords",
        "Add one or more keywords to an ad group. Keywords are added ENABLED, so "
        "they can serve immediately if the ad group and campaign are live.",
        schema={"type": "object",
                "properties": {
                    "ad_group_id": _id_prop("ad group"),
                    "keywords": {"type": "array", "minItems": 1,
                                 "items": {"type": "string"},
                                 "description": "Keyword texts to add."},
                    "match_type": {"type": "string",
                                   "enum": ["BROAD", "PHRASE", "EXACT"],
                                   "description": "Match type for all keywords "
                                                  "(default PHRASE)."},
                },
                "required": ["ad_group_id", "keywords"],
                "additionalProperties": False},
    ),
    Action(
        "add_negative_keywords", "Add negative keywords",
        "Add campaign-level negative keywords so the campaign stops matching those "
        "terms. Safe: negatives only restrict serving, never expand it.",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign"),
                    "keywords": {"type": "array", "minItems": 1,
                                 "items": {"type": "string"},
                                 "description": "Negative keyword texts to add."},
                    "match_type": {"type": "string",
                                   "enum": ["BROAD", "PHRASE", "EXACT"],
                                   "description": "Match type for all negatives "
                                                  "(default PHRASE)."},
                },
                "required": ["campaign_id", "keywords"],
                "additionalProperties": False},
    ),
    Action(
        "remove_keyword", "Remove keyword",
        "Remove one keyword from an ad group by its criterion id (from a keyword "
        "report). Removes only that positive keyword criterion.",
        schema={"type": "object",
                "properties": {
                    "ad_group_id": _id_prop("ad group"),
                    "criterion_id": {"type": "string",
                                     "description": "Numeric keyword criterion id "
                                                    "(criteria id from a Keyword report)."},
                },
                "required": ["ad_group_id", "criterion_id"],
                "additionalProperties": False},
    ),
    Action(
        "set_target_cpa", "Set Target CPA bidding",
        "Switch a campaign to Target CPA bidding at the given cost-per-action, in "
        "account currency units. Changes how the campaign bids (and can change "
        "spend/volume).",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign"),
                    "target_cpa": {"type": "number", "exclusiveMinimum": 0,
                                   "description": "Target cost per conversion in "
                                                  "account currency units, e.g. 25 "
                                                  "for 25.00."},
                },
                "required": ["campaign_id", "target_cpa"],
                "additionalProperties": False},
    ),
    Action(
        "set_target_roas", "Set Target ROAS bidding",
        "Switch a campaign to Target ROAS bidding at the given ratio (Google's "
        "native multiplier: 4 means 400%, i.e. $4 revenue per $1 spend). Requires "
        "conversion-value tracking to be effective.",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign"),
                    "target_roas": {"type": "number", "exclusiveMinimum": 0,
                                    "description": "Target return on ad spend as a "
                                                   "multiplier, e.g. 4 for 400%."},
                },
                "required": ["campaign_id", "target_roas"],
                "additionalProperties": False},
    ),
    Action(
        "set_max_cpc", "Set ad group max CPC",
        "Set an ad group's default maximum CPC bid, in account currency units. "
        "Applies to manual-CPC (and CPC-ceiling) bidding. Affects spend.",
        schema={"type": "object",
                "properties": {
                    "ad_group_id": _id_prop("ad group"),
                    "max_cpc": {"type": "number", "exclusiveMinimum": 0,
                                "description": "Max CPC bid in account currency "
                                               "units, e.g. 1.50."},
                },
                "required": ["ad_group_id", "max_cpc"],
                "additionalProperties": False},
    ),
    Action(
        "create_responsive_search_ad", "Create responsive search ad",
        "Create a responsive search ad in an ad group. Needs at least 3 headlines "
        "(≤30 chars each) and 2 descriptions (≤90 chars each) and a final URL. "
        "Created ENABLED, but only serves when its ad group and campaign are live.",
        schema={"type": "object",
                "properties": {
                    "ad_group_id": _id_prop("ad group"),
                    "final_url": {"type": "string",
                                  "description": "Landing page URL (https://…)."},
                    "headlines": {"type": "array", "minItems": 3, "maxItems": 15,
                                  "items": {"type": "string"},
                                  "description": "3–15 headlines, ≤30 chars each."},
                    "descriptions": {"type": "array", "minItems": 2, "maxItems": 4,
                                     "items": {"type": "string"},
                                     "description": "2–4 descriptions, ≤90 chars each."},
                },
                "required": ["ad_group_id", "final_url", "headlines", "descriptions"],
                "additionalProperties": False},
    ),
    Action(
        "create_ad_group", "Create ad group",
        "Create an ad group in a campaign, always PAUSED. Optionally set its "
        "default max CPC bid (account currency units).",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign"),
                    "name": {"type": "string", "minLength": 1,
                             "description": "Ad group name (unique within the campaign)."},
                    "max_cpc": {"type": "number", "exclusiveMinimum": 0,
                                "description": "Optional default max CPC bid, e.g. 1.50."},
                },
                "required": ["campaign_id", "name"],
                "additionalProperties": False},
    ),
    Action(
        "remove_ad_group", "Remove ad group",
        "Permanently remove an ad group. This CANNOT be undone — prefer "
        "pause_ad_group to stop it reversibly.",
        schema={"type": "object",
                "properties": {"ad_group_id": _id_prop("ad group")},
                "required": ["ad_group_id"], "additionalProperties": False},
    ),
    Action(
        "pause_ad", "Pause ad",
        "Pause a single ad so it stops serving. Reversible with enable_ad.",
        schema={"type": "object",
                "properties": {"ad_group_id": _id_prop("ad group"),
                               "ad_id": _id_prop("ad")},
                "required": ["ad_group_id", "ad_id"], "additionalProperties": False},
    ),
    Action(
        "enable_ad", "Enable ad",
        "Enable a single ad. It serves only when its ad group and campaign are live.",
        schema={"type": "object",
                "properties": {"ad_group_id": _id_prop("ad group"),
                               "ad_id": _id_prop("ad")},
                "required": ["ad_group_id", "ad_id"], "additionalProperties": False},
    ),
    Action(
        "remove_ad", "Remove ad",
        "Permanently remove a single ad. This CANNOT be undone — prefer pause_ad "
        "to stop it reversibly.",
        schema={"type": "object",
                "properties": {"ad_group_id": _id_prop("ad group"),
                               "ad_id": _id_prop("ad")},
                "required": ["ad_group_id", "ad_id"], "additionalProperties": False},
    ),
    Action(
        "update_keyword", "Update keyword",
        "Change a keyword's status (ENABLED/PAUSED) and/or its max CPC bid, by "
        "criterion id (from a Keyword report). Provide at least one of status or "
        "max_cpc.",
        schema={"type": "object",
                "properties": {
                    "ad_group_id": _id_prop("ad group"),
                    "criterion_id": {"type": "string",
                                     "description": "Numeric keyword criterion id."},
                    "status": {"type": "string", "enum": ["ENABLED", "PAUSED"],
                               "description": "New keyword status."},
                    "max_cpc": {"type": "number", "exclusiveMinimum": 0,
                                "description": "New max CPC bid, account currency units."},
                },
                "required": ["ad_group_id", "criterion_id"],
                "additionalProperties": False},
    ),
    Action(
        "remove_campaign", "Remove campaign",
        "PERMANENTLY remove a campaign. This is IRREVERSIBLE — Google cannot "
        "restore a removed campaign. Prefer pause_campaign, which stops spend and "
        "is reversible. Only remove when the user explicitly asks to delete it.",
        schema={"type": "object",
                "properties": {"campaign_id": _id_prop("campaign")},
                "required": ["campaign_id"], "additionalProperties": False},
    ),
    # -- additional bidding strategies -------------------------------------
    Action(
        "set_maximize_conversions", "Set Maximize Conversions bidding",
        "Switch a campaign to Maximize Conversions bidding, optionally capped by a "
        "target CPA (account currency units). Affects how it bids and spends.",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign"),
                    "target_cpa": {"type": "number", "exclusiveMinimum": 0,
                                   "description": "Optional target CPA cap."},
                },
                "required": ["campaign_id"], "additionalProperties": False},
    ),
    Action(
        "set_maximize_conversion_value", "Set Maximize Conversion Value bidding",
        "Switch a campaign to Maximize Conversion Value bidding, optionally with a "
        "target ROAS (ratio, e.g. 4 = 400%).",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign"),
                    "target_roas": {"type": "number", "exclusiveMinimum": 0,
                                    "description": "Optional target ROAS multiplier."},
                },
                "required": ["campaign_id"], "additionalProperties": False},
    ),
    Action(
        "set_manual_cpc", "Set Manual CPC bidding",
        "Switch a campaign to Manual CPC bidding, optionally with Enhanced CPC.",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign"),
                    "enhanced": {"type": "boolean",
                                 "description": "Enable Enhanced CPC (default false)."},
                },
                "required": ["campaign_id"], "additionalProperties": False},
    ),
    Action(
        "set_target_impression_share", "Set Target Impression Share bidding",
        "Switch a campaign to Target Impression Share bidding: aim for a share of "
        "impressions at a page location, with an optional max CPC ceiling.",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign"),
                    "location": {"type": "string",
                                 "enum": ["ANYWHERE_ON_PAGE", "TOP_OF_PAGE",
                                          "ABSOLUTE_TOP_OF_PAGE"],
                                 "description": "Where on the page to target."},
                    "target_percentage": {"type": "number", "exclusiveMinimum": 0,
                                          "maximum": 100,
                                          "description": "Target impression share %, 1–100."},
                    "cpc_bid_ceiling": {"type": "number", "exclusiveMinimum": 0,
                                        "description": "Max CPC ceiling, account "
                                                       "currency units (required)."},
                },
                "required": ["campaign_id", "location", "target_percentage",
                             "cpc_bid_ceiling"],
                "additionalProperties": False},
    ),
    # -- portfolio (shared) bid strategies --------------------------------
    Action(
        "create_portfolio_bid_strategy", "Create portfolio bid strategy",
        "Create a shared (portfolio) bid strategy multiple campaigns can use. Type "
        "is TARGET_CPA (target in currency units) or TARGET_ROAS (ratio, 4 = 400%).",
        schema={"type": "object",
                "properties": {
                    "name": {"type": "string", "minLength": 1,
                             "description": "Strategy name (unique in the account)."},
                    "type": {"type": "string", "enum": ["TARGET_CPA", "TARGET_ROAS"],
                             "description": "Strategy type."},
                    "target": {"type": "number", "exclusiveMinimum": 0,
                               "description": "Target CPA (currency) or ROAS (ratio)."},
                },
                "required": ["name", "type", "target"],
                "additionalProperties": False},
    ),
    Action(
        "attach_campaign_to_portfolio", "Attach campaign to portfolio strategy",
        "Point a campaign at an existing portfolio (shared) bid strategy by id.",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign"),
                    "bidding_strategy_id": {"type": "string",
                                            "description": "Portfolio bid strategy id."},
                },
                "required": ["campaign_id", "bidding_strategy_id"],
                "additionalProperties": False},
    ),
    # -- shared budgets ----------------------------------------------------
    Action(
        "create_shared_budget", "Create shared budget",
        "Create a shared daily budget that multiple campaigns can draw from. "
        "Note: shared budgets are incompatible with Maximize Conversions/Value "
        "bidding — use a dedicated budget (create_campaign) for those.",
        schema={"type": "object",
                "properties": {
                    "name": {"type": "string", "minLength": 1,
                             "description": "Budget name (unique in the account)."},
                    "daily_budget": {"type": "number", "exclusiveMinimum": 0,
                                     "description": "Daily amount in account currency "
                                                    "units, e.g. 50 for 50.00."},
                },
                "required": ["name", "daily_budget"],
                "additionalProperties": False},
    ),
    Action(
        "attach_campaign_to_budget", "Attach campaign to shared budget",
        "Point a campaign at an existing budget by id (e.g. a shared budget).",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign"),
                    "budget_id": {"type": "string",
                                  "description": "Campaign budget id to attach."},
                },
                "required": ["campaign_id", "budget_id"],
                "additionalProperties": False},
    ),
    # -- ad extensions (assets) -------------------------------------------
    Action(
        "add_sitelink", "Add sitelink extension",
        "Add a sitelink to a campaign: creates the sitelink asset and links it.",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign"),
                    "link_text": {"type": "string", "minLength": 1, "maxLength": 25,
                                  "description": "Sitelink text, ≤25 chars."},
                    "final_url": {"type": "string",
                                  "description": "Sitelink landing URL."},
                    "description1": {"type": "string", "maxLength": 35,
                                     "description": "Optional line 1, ≤35 chars."},
                    "description2": {"type": "string", "maxLength": 35,
                                     "description": "Optional line 2, ≤35 chars."},
                },
                "required": ["campaign_id", "link_text", "final_url"],
                "additionalProperties": False},
    ),
    Action(
        "add_callout", "Add callout extension",
        "Add a callout (short highlight text) to a campaign.",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign"),
                    "text": {"type": "string", "minLength": 1, "maxLength": 25,
                             "description": "Callout text, ≤25 chars."},
                },
                "required": ["campaign_id", "text"],
                "additionalProperties": False},
    ),
    Action(
        "add_structured_snippet", "Add structured snippet extension",
        "Add a structured snippet (a header plus values, e.g. 'Brands: A, B, C') "
        "to a campaign.",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign"),
                    "header": {"type": "string", "minLength": 1,
                               "description": "Snippet header, e.g. 'Brands' "
                                              "(must be a valid Google header)."},
                    "values": {"type": "array", "minItems": 3, "maxItems": 10,
                               "items": {"type": "string"},
                               "description": "3–10 values, ≤25 chars each."},
                },
                "required": ["campaign_id", "header", "values"],
                "additionalProperties": False},
    ),
    # -- Customer Match audiences (PII) -----------------------------------
    Action(
        "create_customer_list", "Create Customer Match list",
        "Create an empty Customer Match user list you can later upload members to "
        "and target. No personal data is sent by this action.",
        schema={"type": "object",
                "properties": {
                    "name": {"type": "string", "minLength": 1,
                             "description": "User list name."},
                },
                "required": ["name"], "additionalProperties": False},
    ),
    Action(
        "add_customer_list_members", "Add Customer Match members",
        "Upload members (emails and/or phone numbers) to a Customer Match list. "
        "PRIVACY: the emails/phones are hashed (SHA-256) before sending and are "
        "the user's own first-party data — confirm the user has consent to upload "
        "them. Phones must be E.164 (e.g. +14155550123).",
        schema={"type": "object",
                "properties": {
                    "user_list_id": {"type": "string",
                                     "description": "Customer Match user list id."},
                    "emails": {"type": "array", "items": {"type": "string"},
                               "description": "Plain emails; hashed before upload."},
                    "phones": {"type": "array", "items": {"type": "string"},
                               "description": "E.164 phone numbers; hashed before upload."},
                },
                "required": ["user_list_id"],
                "additionalProperties": False},
    ),
    Action(
        "attach_audience", "Attach audience to ad group",
        "Target a user list (e.g. a Customer Match list) on an ad group.",
        schema={"type": "object",
                "properties": {
                    "ad_group_id": _id_prop("ad group"),
                    "user_list_id": {"type": "string",
                                     "description": "User list id to target."},
                },
                "required": ["ad_group_id", "user_list_id"],
                "additionalProperties": False},
    ),
    Action(
        "remove_audience", "Remove audience from ad group",
        "Stop targeting a user-list audience on an ad group, by its criterion id.",
        schema={"type": "object",
                "properties": {
                    "ad_group_id": _id_prop("ad group"),
                    "criterion_id": {"type": "string",
                                     "description": "Audience criterion id (from a read)."},
                },
                "required": ["ad_group_id", "criterion_id"],
                "additionalProperties": False},
    ),
]
_ACTIONS_BY_ID: Dict[str, Action] = {a.id: a for a in _ACTIONS}
_MATCH_TYPES = {"BROAD", "PHRASE", "EXACT"}


class _AuthError(Exception):
    """Internal marker for a 401 from Google, mapped to AUTH_EXPIRED."""


class GoogleAdsConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http
        self._catalogue_cache: Dict[str, Dict[str, Field]] = {}
        self._selectable_cache: Dict[str, Optional[set]] = {}
        self._dry_run = False
        self._active_login_cid: Optional[str] = None
        self.__manager_map: Optional[Dict[str, str]] = None

    # -- transport ----------------------------------------------------------

    def _mutate_call(self, url: str, operations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """POST a mutate request, injecting validateOnly in dry-run mode.

        Every write goes through here so dry-run coverage cannot be forgotten by
        an individual action handler.
        """
        body: Dict[str, Any] = {"operations": operations}
        if self._dry_run:
            body["validateOnly"] = True
        return self._call("POST", url, body)

    def _login_customer_id(self) -> str:
        """Manager (MCC) id to send as login-customer-id for this connection.

        Stored per connection in the encrypted token bundle at connect time; the
        env var is only a fallback for single-manager deployments. Empty when the
        account is accessed directly (no manager in the path).
        """
        try:
            raw = self._tokens().get("LOGIN_CUSTOMER_ID") or ""
        except ApiError:
            raw = ""
        return str(raw).strip().replace("-", "")

    def _manager_map(self) -> Dict[str, str]:
        """`{customer_id: manager_id}` for this connection, loaded once.

        Populated from the stored account selections (auto-discovered on connect
        /list). Lets a query for any account send the right login-customer-id
        without the user entering a manager id by hand.
        """
        if self.__manager_map is None:
            try:
                from terno_dbi.connectors.api.auth import account_selection
                raw = account_selection.account_manager_map(self.datasource)
                self.__manager_map = {
                    self._customer_id(k): str(v or "") for k, v in raw.items()
                }
            except Exception:   # noqa: BLE001 — routing map is best-effort
                self.__manager_map = {}
        return self.__manager_map

    def _manager_for(self, account: str) -> str:
        """Manager (login-customer-id) to reach `account`; '' for direct.

        Falls back to the connection-wide stored value for accounts discovered
        before per-account routing existed.
        """
        cid = self._customer_id(account)
        mapping = self._manager_map()
        if cid in mapping:
            return mapping[cid]
        return self._login_customer_id()

    def _call(self, method: str, url: str, body: Optional[Dict] = None,
              login_customer_id: Any = _UNSET) -> Dict[str, Any]:
        if login_customer_id is _UNSET:
            login_cid = (self._active_login_cid
                         if self._active_login_cid is not None
                         else self._login_customer_id())
        else:
            login_cid = login_customer_id
        try:
            return self._http(method, url, self.access_token(), body,
                              login_customer_id=(login_cid or None))
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
        """Every queryable account this credential can reach, auto-discovered.

        Managers are detected and expanded automatically — no manager id has to
        be entered by hand. For each account the OAuth user can access directly
        (`listAccessibleCustomers`), we look at its `customer_client` tree:

        * a non-manager account is a normal, directly-reachable account
          (`manager_id=''`);
        * a manager (MCC) is expanded to its leaf client accounts, each tagged
          with the manager id needed to reach it (`manager_id=<manager>`), which
          the query path then sends as login-customer-id automatically.

        An account reachable both directly and under a manager is kept as direct
        (no login-customer-id needed).
        """
        accessible = self._accessible_customer_ids()
        if not accessible:
            return []

        if len(accessible) > 1:
            self.access_token()
        contributions = self._parallel_map(accessible, self._discover_from)

        discovered: Dict[str, tuple] = {}
        for part in contributions:
            for ccid, entry in (part or {}).items():
                prev = discovered.get(ccid)
                if prev is None or (prev[1] != "" and entry[1] == ""):
                    discovered[ccid] = entry

        accounts: List[Account] = []
        for cid, (name, manager_id, manager_name) in discovered.items():
            extra = {}
            if manager_id:
                extra["manager_id"] = manager_id
                extra["manager_name"] = manager_name
            accounts.append(Account(id=cid, name=name, extra=extra))
        return accounts

    def _discover_from(self, cid: str) -> Dict[str, tuple]:
        """One accessible account's contribution: {id: (name, manager_id, manager_name)}.

        A single `customer` lookup gives both the account's name and whether it is
        a manager. A plain account contributes only itself (one HTTP call total);
        a manager additionally walks `customer_client` to surface its leaf clients,
        each routed through it. Fanned out across accessible ids by the caller.
        """
        cust = self._customer_row(cid)
        name = str(cust.get("descriptiveName") or "") or cid

        if not cust.get("manager"):
            return {cid: (name, "", "")}

        out: Dict[str, tuple] = {}
        for cc in self._customer_client_rows(cid):
            if cc.get("manager"):
                continue
            ccid = str(cc.get("id") or "")
            if not ccid or ccid == cid:
                continue
            out[ccid] = (cc.get("descriptiveName") or ccid, cid, name)
        return out

    def _customer_row(self, customer_id: str) -> Dict[str, Any]:
        """The `customer` resource for `customer_id`: name + manager flag in one call.

        More reliable and cheaper than reading them from `customer_client` (which
        omits the descriptive name of the account you're logged in as). Returns {}
        on error so discovery degrades gracefully.
        """
        try:
            data = self._call(
                "POST", f"{_BASE}/customers/{customer_id}/googleAds:search",
                {"query": "SELECT customer.id, customer.descriptive_name, "
                          "customer.manager FROM customer"},
                login_customer_id=customer_id)
        except ApiError:
            return {}
        for row in data.get("results", []):
            return row.get("customer", {}) or {}
        return {}

    @staticmethod
    def _parallel_map(items: List[str], fn: Callable) -> List[Any]:
        """Run `fn` over `items` concurrently, preserving input order.

        Bounded so a manager with many linked accounts can't open an unbounded
        number of sockets. A single item runs inline (no pool overhead).
        """
        if len(items) <= 1:
            return [fn(i) for i in items]
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(len(items), 8)) as pool:
            return list(pool.map(fn, items))

    def _accessible_customer_ids(self) -> List[str]:
        """Customer ids the OAuth user can access directly (managers included)."""
        data = self._call(
            "GET", f"{_BASE}/customers:listAccessibleCustomers",
            login_customer_id="")
        ids: List[str] = []
        for name in data.get("resourceNames", []):
            cid = str(name).split("/")[-1]
            if cid:
                ids.append(cid)
        return ids

    def _customer_client_rows(self, customer_id: str) -> List[Dict[str, Any]]:
        """`customer_client` rows for `customer_id` (self + any descendants).

        Queried logged in as the account itself, so it works whether the account
        is a manager (returns the whole subtree) or a plain account (returns just
        itself). Returns [] on error so discovery degrades gracefully.
        """
        gaql = (
            "SELECT customer_client.id, customer_client.descriptive_name, "
            "customer_client.manager, customer_client.status, customer_client.level "
            "FROM customer_client"
        )
        try:
            data = self._call(
                "POST", f"{_BASE}/customers/{customer_id}/googleAds:search",
                {"query": gaql}, login_customer_id=customer_id)
        except ApiError:
            return []
        return [row.get("customerClient", {}) or {} for row in data.get("results", [])]

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
            # Route this account through its own manager automatically.
            self._active_login_cid = self._manager_for(account)
            try:
                data = self._call("POST", url, {"query": gaql})
            finally:
                self._active_login_cid = None
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
        self, action_id: str, account: str,
        params: Optional[Dict[str, Any]] = None, dry_run: bool = False,
    ) -> ActionResult:
        """Perform one write action. Account authorisation happens upstream.

        With `dry_run=True`, every mutate is sent with Google's `validateOnly`
        flag: the request is fully validated server-side but nothing is applied,
        so you can check an action is correct with zero side effects and zero
        spend. Multi-step actions (create_campaign, add_sitelink, …) validate
        their first step and skip the dependent step(s) in dry-run, since those
        reference a resource that was never created.
        """
        self._dry_run = bool(dry_run)
        # Route this account's mutates through its own manager automatically.
        self._active_login_cid = self._manager_for(account)
        try:
            result = self._dispatch_action(action_id, account, params or {})
        finally:
            was_dry = self._dry_run
            self._dry_run = False
            self._active_login_cid = None
        if was_dry and not (result.details or {}).get("dry_run"):
            result = replace(
                result,
                summary="[dry-run — not applied] " + result.summary,
                details={**(result.details or {}), "dry_run": True, "applied": False},
            )
        return result

    def _dispatch_action(
        self, action_id: str, account: str, params: Dict[str, Any]
    ) -> ActionResult:
        """Route to the concrete handler.

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
        if action_id == "create_campaign":
            return self._create_campaign(cid, account, params)
        if action_id == "add_keywords":
            return self._add_keywords(cid, account, params)
        if action_id == "add_negative_keywords":
            return self._add_negative_keywords(cid, account, params)
        if action_id == "remove_keyword":
            return self._remove_keyword(cid, account, params)
        if action_id == "set_target_cpa":
            return self._set_target_cpa(cid, account, params)
        if action_id == "set_target_roas":
            return self._set_target_roas(cid, account, params)
        if action_id == "set_max_cpc":
            return self._set_max_cpc(cid, account, params)
        if action_id == "create_responsive_search_ad":
            return self._create_rsa(cid, account, params)
        if action_id == "create_ad_group":
            return self._create_ad_group(cid, account, params)
        if action_id == "remove_ad_group":
            return self._remove_ad_group(cid, account, params)
        if action_id == "pause_ad":
            return self._set_ad_status(cid, account, params, "PAUSED")
        if action_id == "enable_ad":
            return self._set_ad_status(cid, account, params, "ENABLED")
        if action_id == "remove_ad":
            return self._remove_ad(cid, account, params)
        if action_id == "update_keyword":
            return self._update_keyword(cid, account, params)
        if action_id == "remove_campaign":
            return self._remove_campaign(cid, account, params)
        if action_id == "set_maximize_conversions":
            return self._set_maximize_conversions(cid, account, params)
        if action_id == "set_maximize_conversion_value":
            return self._set_maximize_conversion_value(cid, account, params)
        if action_id == "set_manual_cpc":
            return self._set_manual_cpc(cid, account, params)
        if action_id == "set_target_impression_share":
            return self._set_target_impression_share(cid, account, params)
        if action_id == "create_portfolio_bid_strategy":
            return self._create_portfolio_bid_strategy(cid, account, params)
        if action_id == "attach_campaign_to_portfolio":
            return self._attach_campaign_to_portfolio(cid, account, params)
        if action_id == "create_shared_budget":
            return self._create_shared_budget(cid, account, params)
        if action_id == "attach_campaign_to_budget":
            return self._attach_campaign_to_budget(cid, account, params)
        if action_id == "add_sitelink":
            return self._add_sitelink(cid, account, params)
        if action_id == "add_callout":
            return self._add_callout(cid, account, params)
        if action_id == "add_structured_snippet":
            return self._add_structured_snippet(cid, account, params)
        if action_id == "create_customer_list":
            return self._create_customer_list(cid, account, params)
        if action_id == "add_customer_list_members":
            return self._add_customer_list_members(cid, account, params)
        if action_id == "attach_audience":
            return self._attach_audience(cid, account, params)
        if action_id == "remove_audience":
            return self._remove_audience(cid, account, params)
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
        return self._mutate_call(
            f"{_BASE}/customers/{cid}/{collection}:mutate", [operation])

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

    def _match_type(self, params: Dict[str, Any]) -> str:
        mt = str(params.get("match_type") or "PHRASE").upper()
        if mt not in _MATCH_TYPES:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"match_type must be one of {sorted(_MATCH_TYPES)}.",
                           retriable=False, details={"param": "match_type"})
        return mt

    def _keyword_texts(self, params: Dict[str, Any]) -> List[str]:
        raw = params.get("keywords")
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list) or not raw:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "'keywords' must be a non-empty list of keyword texts.",
                           retriable=False, details={"param": "keywords"})
        texts = [str(k).strip() for k in raw if str(k).strip()]
        if not texts:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "'keywords' contained no non-empty texts.",
                           retriable=False, details={"param": "keywords"})
        return texts

    def _new_resource_id(self, data: Dict[str, Any]) -> Optional[str]:
        """The trailing id of the first mutated resource, e.g. .../campaigns/123."""
        results = data.get("results") or []
        if not results:
            return None
        name = results[0].get("resourceName", "")
        return name.split("/")[-1] or None

    def _create_campaign(self, cid, account, params) -> ActionResult:
        name = str(params.get("name") or "").strip()
        if not name:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "'name' is required.", retriable=False,
                           details={"param": "name"})
        budget = params.get("daily_budget")
        if not isinstance(budget, (int, float)) or isinstance(budget, bool) or budget <= 0:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "'daily_budget' must be a positive number (account "
                           "currency units).", retriable=False,
                           details={"param": "daily_budget"})
        micros = int(round(float(budget) * 1_000_000))

        # 1. A dedicated budget for this campaign (name must be unique).
        budget_name = f"{name} budget {uuid.uuid4().hex[:8]}"
        budget_data = self._mutate(cid, "campaignBudgets", {
            "create": {"name": budget_name, "amountMicros": micros,
                       "deliveryMethod": "STANDARD", "explicitlyShared": False},
        })
        if self._dry_run:
            return self._dry_run_partial(
                "create_campaign", account,
                {"name": name, "status": "PAUSED", "daily_budget": budget,
                 "channel_type": "SEARCH"},
                f"Validated budget for campaign {name!r}.")
        budget_res = self._new_resource_id_full(budget_data)
        if not budget_res:
            raise ApiError(ErrorCode.UPSTREAM_ERROR,
                           "Google Ads did not return the new budget resource.",
                           retriable=False)

        # 2. The campaign itself — PAUSED, Search network, manual CPC.
        data = self._mutate(cid, "campaigns", {
            "create": {
                "name": name,
                "status": "PAUSED",   # create-paused: never serves until enabled
                "advertisingChannelType": "SEARCH",
                "manualCpc": {},
                "campaignBudget": budget_res,
                "containsEuPoliticalAdvertising":
                    "DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING",
                "networkSettings": {
                    "targetGoogleSearch": True,
                    "targetSearchNetwork": True,
                    "targetContentNetwork": False,
                    "targetPartnerSearchNetwork": False,
                },
            },
        })
        new_id = self._new_resource_id(data)
        after = {"id": new_id, "name": name, "status": "PAUSED",
                 "daily_budget": budget, "channel_type": "SEARCH"}
        return ActionResult(
            action="create_campaign", account=account,
            summary=(f"Created Search campaign {name!r} (id {new_id}) PAUSED with a "
                     f"{budget}/day budget. Enable it to start serving."),
            before=None, after=after,
        )

    def _new_resource_id_full(self, data: Dict[str, Any]) -> Optional[str]:
        """Full resourceName of the first mutated resource (for referencing)."""
        results = data.get("results") or []
        if not results:
            return None
        return results[0].get("resourceName") or None

    @staticmethod
    def _criterion_ids(data: Dict[str, Any]) -> List[str]:
        """Trailing criterion ids from a criteria mutate (`.../{ag}~{criterion}`)."""
        ids = []
        for r in (data.get("results") or []):
            rn = r.get("resourceName", "")
            if "~" in rn:
                ids.append(rn.split("~")[-1])
        return ids

    def _add_keywords(self, cid, account, params) -> ActionResult:
        ad_group_id = self._require_id(params, "ad_group_id")
        texts = self._keyword_texts(params)
        match_type = self._match_type(params)
        ag_res = f"customers/{cid}/adGroups/{ad_group_id}"
        operations = [{
            "create": {
                "adGroup": ag_res,
                "status": "ENABLED",
                "keyword": {"text": t, "matchType": match_type},
            },
        } for t in texts]
        data = self._mutate_call(
            f"{_BASE}/customers/{cid}/adGroupCriteria:mutate", operations)
        after = {"ad_group_id": ad_group_id, "match_type": match_type,
                 "keywords": texts, "criterion_ids": self._criterion_ids(data)}
        return ActionResult(
            action="add_keywords", account=account,
            summary=(f"Added {len(texts)} {match_type} keyword"
                     f"{'' if len(texts) == 1 else 's'} to ad group {ad_group_id}."),
            before=None, after=after,
        )

    def _add_negative_keywords(self, cid, account, params) -> ActionResult:
        campaign_id = self._require_id(params, "campaign_id")
        texts = self._keyword_texts(params)
        match_type = self._match_type(params)
        camp_res = f"customers/{cid}/campaigns/{campaign_id}"
        operations = [{
            "create": {
                "campaign": camp_res,
                "negative": True,
                "keyword": {"text": t, "matchType": match_type},
            },
        } for t in texts]
        self._mutate_call(
            f"{_BASE}/customers/{cid}/campaignCriteria:mutate", operations)
        after = {"campaign_id": campaign_id, "match_type": match_type,
                 "negative_keywords": texts}
        return ActionResult(
            action="add_negative_keywords", account=account,
            summary=(f"Added {len(texts)} {match_type} negative keyword"
                     f"{'' if len(texts) == 1 else 's'} to campaign {campaign_id}."),
            before=None, after=after,
        )

    def _remove_keyword(self, cid, account, params) -> ActionResult:
        ad_group_id = self._require_id(params, "ad_group_id")
        criterion_id = self._require_id(params, "criterion_id")
        resource = f"customers/{cid}/adGroupCriteria/{ad_group_id}~{criterion_id}"
        self._mutate(cid, "adGroupCriteria", {"remove": resource})
        after = {"ad_group_id": ad_group_id, "criterion_id": criterion_id,
                 "removed": True}
        return ActionResult(
            action="remove_keyword", account=account,
            summary=f"Removed keyword {criterion_id} from ad group {ad_group_id}.",
            before={"ad_group_id": ad_group_id, "criterion_id": criterion_id},
            after=after,
        )

    def _positive_amount(self, params: Dict[str, Any], key: str) -> float:
        v = params.get(key)
        if not isinstance(v, (int, float)) or isinstance(v, bool) or v <= 0:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"{key!r} must be a positive number.",
                           retriable=False, details={"param": key})
        return float(v)

    def _campaign_bidding_before(self, cid, campaign_id) -> Dict[str, Any]:
        row = self._search_one(
            cid,
            f"SELECT campaign.id, campaign.name, campaign.bidding_strategy_type "
            f"FROM campaign WHERE campaign.id = {campaign_id}",
        )
        if row is None:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"Campaign {campaign_id} was not found in this account.",
                           retriable=False, details={"campaign_id": campaign_id})
        camp = row.get("campaign", {})
        return {"id": campaign_id, "name": camp.get("name"),
                "bidding_strategy_type": camp.get("biddingStrategyType")}

    def _set_target_cpa(self, cid, account, params) -> ActionResult:
        campaign_id = self._require_id(params, "campaign_id")
        cpa = self._positive_amount(params, "target_cpa")
        before = self._campaign_bidding_before(cid, campaign_id)
        micros = int(round(cpa * 1_000_000))
        self._mutate(cid, "campaigns", {
            "updateMask": "maximize_conversions.target_cpa_micros",
            "update": {"resourceName": f"customers/{cid}/campaigns/{campaign_id}",
                       "maximizeConversions": {"targetCpaMicros": micros}},
        })
        after = {**before, "bidding_strategy_type": "MAXIMIZE_CONVERSIONS",
                 "target_cpa": cpa}
        return ActionResult(
            action="set_target_cpa", account=account,
            summary=(f"Campaign {before.get('name') or campaign_id} set to Maximize "
                     f"Conversions with a target CPA of {cpa}."),
            before=before, after=after,
        )

    def _set_target_roas(self, cid, account, params) -> ActionResult:
        campaign_id = self._require_id(params, "campaign_id")
        roas = self._positive_amount(params, "target_roas")
        before = self._campaign_bidding_before(cid, campaign_id)
        self._mutate(cid, "campaigns", {
            "updateMask": "maximize_conversion_value.target_roas",
            "update": {"resourceName": f"customers/{cid}/campaigns/{campaign_id}",
                       "maximizeConversionValue": {"targetRoas": roas}},
        })
        after = {**before, "bidding_strategy_type": "MAXIMIZE_CONVERSION_VALUE",
                 "target_roas": roas}
        return ActionResult(
            action="set_target_roas", account=account,
            summary=(f"Campaign {before.get('name') or campaign_id} set to Maximize "
                     f"Conversion Value with a target ROAS of {roas} "
                     f"({roas * 100:g}%)."),
            before=before, after=after,
        )

    def _set_max_cpc(self, cid, account, params) -> ActionResult:
        ad_group_id = self._require_id(params, "ad_group_id")
        max_cpc = self._positive_amount(params, "max_cpc")
        row = self._search_one(
            cid,
            f"SELECT ad_group.id, ad_group.name, ad_group.cpc_bid_micros "
            f"FROM ad_group WHERE ad_group.id = {ad_group_id}",
        )
        if row is None:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"Ad group {ad_group_id} was not found in this account.",
                           retriable=False, details={"ad_group_id": ad_group_id})
        ag = row.get("adGroup", {})
        old = ag.get("cpcBidMicros")
        micros = int(round(max_cpc * 1_000_000))
        self._mutate(cid, "adGroups", {
            "updateMask": "cpc_bid_micros",
            "update": {"resourceName": f"customers/{cid}/adGroups/{ad_group_id}",
                       "cpcBidMicros": micros},
        })
        def _units(m):
            try:
                return float(m) / 1_000_000
            except (TypeError, ValueError):
                return None
        before = {"ad_group_id": ad_group_id, "name": ag.get("name"),
                  "max_cpc": _units(old)}
        after = {**before, "max_cpc": max_cpc}
        return ActionResult(
            action="set_max_cpc", account=account,
            summary=(f"Ad group {ag.get('name') or ad_group_id} max CPC set to "
                     f"{max_cpc}."),
            before=before, after=after,
        )

    def _text_list(self, params, key, min_n, max_len, label) -> List[str]:
        raw = params.get(key)
        if not isinstance(raw, list):
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"{key!r} must be a list of {label}.",
                           retriable=False, details={"param": key})
        texts = [str(t).strip() for t in raw if str(t).strip()]
        if len(texts) < min_n:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"At least {min_n} {label} are required (got {len(texts)}).",
                           retriable=False, details={"param": key})
        too_long = [t for t in texts if len(t) > max_len]
        if too_long:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"Each of the {label} must be ≤{max_len} characters; "
                           f"too long: {too_long[0]!r}.",
                           retriable=False, details={"param": key})
        return texts

    def _create_rsa(self, cid, account, params) -> ActionResult:
        ad_group_id = self._require_id(params, "ad_group_id")
        final_url = str(params.get("final_url") or "").strip()
        if not final_url:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "'final_url' is required.", retriable=False,
                           details={"param": "final_url"})
        headlines = self._text_list(params, "headlines", 3, 30, "headlines")
        descriptions = self._text_list(params, "descriptions", 2, 90, "descriptions")
        data = self._mutate(cid, "adGroupAds", {
            "create": {
                "adGroup": f"customers/{cid}/adGroups/{ad_group_id}",
                "status": "ENABLED",
                "ad": {
                    "finalUrls": [final_url],
                    "responsiveSearchAd": {
                        "headlines": [{"text": h} for h in headlines],
                        "descriptions": [{"text": d} for d in descriptions],
                    },
                },
            },
        })
        new_res = self._new_resource_id_full(data)
        ad_id = new_res.split("~")[-1] if new_res and "~" in new_res else None
        after = {"ad_group_id": ad_group_id, "final_url": final_url,
                 "headlines": headlines, "descriptions": descriptions,
                 "resource": new_res, "ad_id": ad_id, "status": "ENABLED"}
        return ActionResult(
            action="create_responsive_search_ad", account=account,
            summary=(f"Created a responsive search ad in ad group {ad_group_id} "
                     f"({len(headlines)} headlines, {len(descriptions)} descriptions). "
                     f"It serves only when the ad group and campaign are enabled."),
            before=None, after=after,
        )

    def _create_ad_group(self, cid, account, params) -> ActionResult:
        campaign_id = self._require_id(params, "campaign_id")
        name = str(params.get("name") or "").strip()
        if not name:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "'name' is required.", retriable=False,
                           details={"param": "name"})
        create = {
            "campaign": f"customers/{cid}/campaigns/{campaign_id}",
            "name": name,
            "status": "PAUSED",   # create-paused
            "type": "SEARCH_STANDARD",
        }
        max_cpc = None
        if params.get("max_cpc") is not None:
            max_cpc = self._positive_amount(params, "max_cpc")
            create["cpcBidMicros"] = int(round(max_cpc * 1_000_000))
        data = self._mutate(cid, "adGroups", {"create": create})
        new_id = self._new_resource_id(data)
        after = {"id": new_id, "name": name, "status": "PAUSED",
                 "campaign_id": campaign_id, "max_cpc": max_cpc}
        return ActionResult(
            action="create_ad_group", account=account,
            summary=(f"Created ad group {name!r} (id {new_id}) PAUSED in campaign "
                     f"{campaign_id}."),
            before=None, after=after,
        )

    def _remove_ad_group(self, cid, account, params) -> ActionResult:
        ad_group_id = self._require_id(params, "ad_group_id")
        row = self._search_one(
            cid,
            f"SELECT ad_group.id, ad_group.name FROM ad_group "
            f"WHERE ad_group.id = {ad_group_id}",
        )
        if row is None:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"Ad group {ad_group_id} was not found in this account.",
                           retriable=False, details={"ad_group_id": ad_group_id})
        name = row.get("adGroup", {}).get("name")
        self._mutate(cid, "adGroups",
                     {"remove": f"customers/{cid}/adGroups/{ad_group_id}"})
        return ActionResult(
            action="remove_ad_group", account=account,
            summary=f"Permanently removed ad group {name or ad_group_id}.",
            before={"id": ad_group_id, "name": name},
            after={"id": ad_group_id, "removed": True},
        )

    def _set_ad_status(self, cid, account, params, status) -> ActionResult:
        ad_group_id = self._require_id(params, "ad_group_id")
        ad_id = self._require_id(params, "ad_id")
        row = self._search_one(
            cid,
            f"SELECT ad_group_ad.status, ad_group_ad.ad.id FROM ad_group_ad "
            f"WHERE ad_group_ad.ad.id = {ad_id} AND ad_group.id = {ad_group_id}",
        )
        if row is None:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"Ad {ad_id} was not found in ad group {ad_group_id}.",
                           retriable=False,
                           details={"ad_group_id": ad_group_id, "ad_id": ad_id})
        before_status = row.get("adGroupAd", {}).get("status")
        self._mutate(cid, "adGroupAds", {
            "updateMask": "status",
            "update": {
                "resourceName": f"customers/{cid}/adGroupAds/{ad_group_id}~{ad_id}",
                "status": status,
            },
        })
        verb = "paused" if status == "PAUSED" else "enabled"
        return ActionResult(
            action=("pause_ad" if status == "PAUSED" else "enable_ad"),
            account=account,
            summary=f"Ad {ad_id} in ad group {ad_group_id} {verb}.",
            before={"ad_group_id": ad_group_id, "ad_id": ad_id, "status": before_status},
            after={"ad_group_id": ad_group_id, "ad_id": ad_id, "status": status},
        )

    def _remove_ad(self, cid, account, params) -> ActionResult:
        ad_group_id = self._require_id(params, "ad_group_id")
        ad_id = self._require_id(params, "ad_id")
        self._mutate(cid, "adGroupAds", {
            "remove": f"customers/{cid}/adGroupAds/{ad_group_id}~{ad_id}",
        })
        return ActionResult(
            action="remove_ad", account=account,
            summary=f"Permanently removed ad {ad_id} from ad group {ad_group_id}.",
            before={"ad_group_id": ad_group_id, "ad_id": ad_id},
            after={"ad_group_id": ad_group_id, "ad_id": ad_id, "removed": True},
        )

    def _update_keyword(self, cid, account, params) -> ActionResult:
        ad_group_id = self._require_id(params, "ad_group_id")
        criterion_id = self._require_id(params, "criterion_id")
        update = {
            "resourceName": f"customers/{cid}/adGroupCriteria/{ad_group_id}~{criterion_id}",
        }
        masks: List[str] = []
        changed: Dict[str, Any] = {}
        if params.get("status") is not None:
            status = str(params["status"]).upper()
            if status not in ("ENABLED", "PAUSED"):
                raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                               "status must be ENABLED or PAUSED.",
                               retriable=False, details={"param": "status"})
            update["status"] = status
            masks.append("status")
            changed["status"] = status
        if params.get("max_cpc") is not None:
            max_cpc = self._positive_amount(params, "max_cpc")
            update["cpcBidMicros"] = int(round(max_cpc * 1_000_000))
            masks.append("cpc_bid_micros")
            changed["max_cpc"] = max_cpc
        if not masks:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "Provide at least one of 'status' or 'max_cpc' to update.",
                           retriable=False)
        self._mutate(cid, "adGroupCriteria",
                     {"updateMask": ",".join(masks), "update": update})
        after = {"ad_group_id": ad_group_id, "criterion_id": criterion_id, **changed}
        return ActionResult(
            action="update_keyword", account=account,
            summary=(f"Updated keyword {criterion_id} in ad group {ad_group_id} "
                     f"({', '.join(masks)})."),
            before={"ad_group_id": ad_group_id, "criterion_id": criterion_id},
            after=after,
        )

    def _remove_campaign(self, cid, account, params) -> ActionResult:
        campaign_id = self._require_id(params, "campaign_id")
        row = self._search_one(
            cid,
            f"SELECT campaign.id, campaign.name, campaign.status FROM campaign "
            f"WHERE campaign.id = {campaign_id}",
        )
        if row is None:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"Campaign {campaign_id} was not found in this account.",
                           retriable=False, details={"campaign_id": campaign_id})
        camp = row.get("campaign", {})
        self._mutate(cid, "campaigns",
                     {"remove": f"customers/{cid}/campaigns/{campaign_id}"})
        return ActionResult(
            action="remove_campaign", account=account,
            summary=(f"PERMANENTLY removed campaign {camp.get('name') or campaign_id}. "
                     f"This cannot be undone."),
            before={"id": campaign_id, "name": camp.get("name"),
                    "status": camp.get("status")},
            after={"id": campaign_id, "status": "REMOVED", "removed": True},
        )

    # -- additional bidding strategies -------------------------------------

    def _update_campaign_bidding(self, cid, account, params, action_id,
                                 update_fields, mask, after_extra, verb) -> ActionResult:
        campaign_id = self._require_id(params, "campaign_id")
        before = self._campaign_bidding_before(cid, campaign_id)
        self._mutate(cid, "campaigns", {
            "updateMask": mask,
            "update": {"resourceName": f"customers/{cid}/campaigns/{campaign_id}",
                       **update_fields},
        })
        after = {**before, **after_extra}
        return ActionResult(
            action=action_id, account=account,
            summary=f"Campaign {before.get('name') or campaign_id} set to {verb}.",
            before=before, after=after,
        )

    def _set_maximize_conversions(self, cid, account, params) -> ActionResult:
        # Always mask the scalar leaf (target_cpa_micros), never the message —
        # masking the bare `maximize_conversions` message raises FIELD_HAS_SUBFIELDS.
        # 0 means "no target CPA" while still switching the strategy.
        cpa = (self._positive_amount(params, "target_cpa")
               if params.get("target_cpa") is not None else 0.0)
        mc = {"targetCpaMicros": int(round(cpa * 1_000_000))}
        extra = {"bidding_strategy_type": "MAXIMIZE_CONVERSIONS"}
        if cpa:
            extra["target_cpa"] = cpa
        return self._update_campaign_bidding(
            cid, account, params, "set_maximize_conversions",
            {"maximizeConversions": mc}, "maximize_conversions.target_cpa_micros",
            extra, "Maximize Conversions bidding")

    def _set_maximize_conversion_value(self, cid, account, params) -> ActionResult:
        roas = (self._positive_amount(params, "target_roas")
                if params.get("target_roas") is not None else 0.0)
        extra = {"bidding_strategy_type": "MAXIMIZE_CONVERSION_VALUE"}
        if roas:
            extra["target_roas"] = roas
        return self._update_campaign_bidding(
            cid, account, params, "set_maximize_conversion_value",
            {"maximizeConversionValue": {"targetRoas": roas}},
            "maximize_conversion_value.target_roas", extra,
            "Maximize Conversion Value bidding")

    def _set_manual_cpc(self, cid, account, params) -> ActionResult:
        enhanced = bool(params.get("enhanced", False))
        return self._update_campaign_bidding(
            cid, account, params, "set_manual_cpc",
            {"manualCpc": {"enhancedCpcEnabled": enhanced}},
            "manual_cpc.enhanced_cpc_enabled",
            {"bidding_strategy_type": "MANUAL_CPC", "enhanced": enhanced},
            f"Manual CPC bidding (enhanced={enhanced})")

    def _set_target_impression_share(self, cid, account, params) -> ActionResult:
        location = str(params.get("location") or "").upper()
        valid_loc = {"ANYWHERE_ON_PAGE", "TOP_OF_PAGE", "ABSOLUTE_TOP_OF_PAGE"}
        if location not in valid_loc:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"location must be one of {sorted(valid_loc)}.",
                           retriable=False, details={"param": "location"})
        pct = params.get("target_percentage")
        if not isinstance(pct, (int, float)) or isinstance(pct, bool) or not (0 < pct <= 100):
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "target_percentage must be a number in (0, 100].",
                           retriable=False, details={"param": "target_percentage"})
        tis: Dict[str, Any] = {
            "location": location,
            "locationFractionMicros": int(round(pct / 100 * 1_000_000)),
        }
        if params.get("cpc_bid_ceiling") is None:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "cpc_bid_ceiling is required for Target Impression "
                           "Share bidding.",
                           retriable=False, details={"param": "cpc_bid_ceiling"})
        ceil = self._positive_amount(params, "cpc_bid_ceiling")
        tis["cpcBidCeilingMicros"] = int(round(ceil * 1_000_000))
        masks = ["target_impression_share.location",
                 "target_impression_share.location_fraction_micros",
                 "target_impression_share.cpc_bid_ceiling_micros"]
        extra = {"bidding_strategy_type": "TARGET_IMPRESSION_SHARE",
                 "location": location, "target_percentage": pct,
                 "cpc_bid_ceiling": ceil}
        return self._update_campaign_bidding(
            cid, account, params, "set_target_impression_share",
            {"targetImpressionShare": tis}, ",".join(masks), extra,
            "Target Impression Share bidding")

    # -- portfolio bid strategies -----------------------------------------

    def _create_portfolio_bid_strategy(self, cid, account, params) -> ActionResult:
        name = str(params.get("name") or "").strip()
        if not name:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "'name' is required.", retriable=False,
                           details={"param": "name"})
        stype = str(params.get("type") or "").upper()
        target = self._positive_amount(params, "target")
        if stype == "TARGET_CPA":
            strategy = {"targetCpa": {"targetCpaMicros": int(round(target * 1_000_000))}}
        elif stype == "TARGET_ROAS":
            strategy = {"targetRoas": {"targetRoas": target}}
        else:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "type must be TARGET_CPA or TARGET_ROAS.",
                           retriable=False, details={"param": "type"})
        data = self._mutate(cid, "biddingStrategies",
                            {"create": {"name": name, **strategy}})
        new_id = self._new_resource_id(data)
        after = {"id": new_id, "name": name, "type": stype, "target": target}
        return ActionResult(
            action="create_portfolio_bid_strategy", account=account,
            summary=(f"Created portfolio bid strategy {name!r} (id {new_id}, "
                     f"{stype} {target})."),
            before=None, after=after,
        )

    def _attach_campaign_to_portfolio(self, cid, account, params) -> ActionResult:
        campaign_id = self._require_id(params, "campaign_id")
        strategy_id = self._require_id(params, "bidding_strategy_id")
        before = self._campaign_bidding_before(cid, campaign_id)
        strat_res = f"customers/{cid}/biddingStrategies/{strategy_id}"
        self._mutate(cid, "campaigns", {
            "updateMask": "bidding_strategy",
            "update": {"resourceName": f"customers/{cid}/campaigns/{campaign_id}",
                       "biddingStrategy": strat_res},
        })
        after = {**before, "bidding_strategy_id": strategy_id}
        return ActionResult(
            action="attach_campaign_to_portfolio", account=account,
            summary=(f"Campaign {before.get('name') or campaign_id} attached to "
                     f"portfolio bid strategy {strategy_id}."),
            before=before, after=after,
        )

    # -- shared budgets ----------------------------------------------------

    def _create_shared_budget(self, cid, account, params) -> ActionResult:
        name = str(params.get("name") or "").strip()
        if not name:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS, "'name' is required.",
                           retriable=False, details={"param": "name"})
        amount = self._positive_amount(params, "daily_budget")
        micros = int(round(amount * 1_000_000))
        data = self._mutate(cid, "campaignBudgets", {
            "create": {"name": name, "amountMicros": micros,
                       "deliveryMethod": "STANDARD", "explicitlyShared": True},
        })
        new_id = self._new_resource_id(data)
        return ActionResult(
            action="create_shared_budget", account=account,
            summary=f"Created shared budget {name!r} (id {new_id}) at {amount}/day.",
            before=None,
            after={"id": new_id, "name": name, "daily_budget": amount,
                   "shared": True})

    def _attach_campaign_to_budget(self, cid, account, params) -> ActionResult:
        campaign_id = self._require_id(params, "campaign_id")
        budget_id = self._require_id(params, "budget_id")
        row = self._search_one(
            cid,
            f"SELECT campaign.id, campaign.name, campaign.campaign_budget "
            f"FROM campaign WHERE campaign.id = {campaign_id}",
        )
        if row is None:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"Campaign {campaign_id} was not found in this account.",
                           retriable=False, details={"campaign_id": campaign_id})
        camp = row.get("campaign", {})
        before = {"campaign_id": campaign_id, "name": camp.get("name"),
                  "budget_resource": camp.get("campaignBudget")}
        budget_res = f"customers/{cid}/campaignBudgets/{budget_id}"
        self._mutate(cid, "campaigns", {
            "updateMask": "campaign_budget",
            "update": {"resourceName": f"customers/{cid}/campaigns/{campaign_id}",
                       "campaignBudget": budget_res},
        })
        after = {**before, "budget_resource": budget_res, "budget_id": budget_id}
        return ActionResult(
            action="attach_campaign_to_budget", account=account,
            summary=(f"Campaign {camp.get('name') or campaign_id} attached to "
                     f"budget {budget_id}."),
            before=before, after=after,
        )

    # -- ad extensions (assets) -------------------------------------------

    def _create_asset(self, cid, asset_body) -> Optional[str]:
        data = self._mutate_call(
            f"{_BASE}/customers/{cid}/assets:mutate", [{"create": asset_body}])
        if self._dry_run:
            return None   # validated only; nothing created to reference
        res = self._new_resource_id_full(data)
        if not res:
            raise ApiError(ErrorCode.UPSTREAM_ERROR,
                           "Google Ads did not return the new asset resource.",
                           retriable=False)
        return res

    def _link_campaign_asset(self, cid, campaign_id, asset_res, field_type) -> None:
        self._mutate_call(f"{_BASE}/customers/{cid}/campaignAssets:mutate",
                          [{"create": {
                              "campaign": f"customers/{cid}/campaigns/{campaign_id}",
                              "asset": asset_res,
                              "fieldType": field_type,
                          }}])

    def _dry_run_partial(self, action, account, after, note) -> ActionResult:
        """Result for a multi-step action whose first step validated in dry-run."""
        return ActionResult(
            action=action, account=account,
            summary=("[dry-run — validated first step; dependent step(s) skipped] "
                     + note),
            before=None, after=after,
            details={"dry_run": True, "applied": False, "partial": True})

    def _add_sitelink(self, cid, account, params) -> ActionResult:
        campaign_id = self._require_id(params, "campaign_id")
        link_text = str(params.get("link_text") or "").strip()
        final_url = str(params.get("final_url") or "").strip()
        if not link_text or not final_url:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "'link_text' and 'final_url' are required.",
                           retriable=False)
        sitelink: Dict[str, Any] = {"linkText": link_text}
        if params.get("description1"):
            sitelink["description1"] = str(params["description1"])
        if params.get("description2"):
            sitelink["description2"] = str(params["description2"])
        asset_res = self._create_asset(cid, {
            "finalUrls": [final_url], "sitelinkAsset": sitelink})
        after = {"campaign_id": campaign_id, "link_text": link_text,
                 "final_url": final_url, "asset": asset_res}
        if self._dry_run:
            return self._dry_run_partial(
                "add_sitelink", account, after,
                f"Validated sitelink {link_text!r} for campaign {campaign_id}.")
        self._link_campaign_asset(cid, campaign_id, asset_res, "SITELINK")
        return ActionResult(
            action="add_sitelink", account=account,
            summary=f"Added sitelink {link_text!r} to campaign {campaign_id}.",
            before=None, after=after,
        )

    def _add_callout(self, cid, account, params) -> ActionResult:
        campaign_id = self._require_id(params, "campaign_id")
        text = str(params.get("text") or "").strip()
        if not text:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS, "'text' is required.",
                           retriable=False, details={"param": "text"})
        asset_res = self._create_asset(cid, {"calloutAsset": {"calloutText": text}})
        after = {"campaign_id": campaign_id, "text": text, "asset": asset_res}
        if self._dry_run:
            return self._dry_run_partial(
                "add_callout", account, after,
                f"Validated callout {text!r} for campaign {campaign_id}.")
        self._link_campaign_asset(cid, campaign_id, asset_res, "CALLOUT")
        return ActionResult(
            action="add_callout", account=account,
            summary=f"Added callout {text!r} to campaign {campaign_id}.",
            before=None, after=after,
        )

    def _add_structured_snippet(self, cid, account, params) -> ActionResult:
        campaign_id = self._require_id(params, "campaign_id")
        header = str(params.get("header") or "").strip()
        values = self._text_list(params, "values", 3, 25, "values")
        if not header:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS, "'header' is required.",
                           retriable=False, details={"param": "header"})
        asset_res = self._create_asset(cid, {"structuredSnippetAsset": {
            "header": header, "values": values}})
        after = {"campaign_id": campaign_id, "header": header, "values": values,
                 "asset": asset_res}
        if self._dry_run:
            return self._dry_run_partial(
                "add_structured_snippet", account, after,
                f"Validated structured snippet {header!r} for campaign {campaign_id}.")
        self._link_campaign_asset(cid, campaign_id, asset_res, "STRUCTURED_SNIPPET")
        return ActionResult(
            action="add_structured_snippet", account=account,
            summary=(f"Added structured snippet {header!r} ({len(values)} values) "
                     f"to campaign {campaign_id}."),
            before=None, after=after,
        )

    # -- Customer Match audiences -----------------------------------------

    @staticmethod
    def _hash_identifier(value: str) -> str:
        """SHA-256 of a normalised identifier (lowercased, trimmed), hex."""
        return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()

    def _create_customer_list(self, cid, account, params) -> ActionResult:
        name = str(params.get("name") or "").strip()
        if not name:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS, "'name' is required.",
                           retriable=False, details={"param": "name"})
        data = self._mutate_call(
            f"{_BASE}/customers/{cid}/userLists:mutate",
            [{"create": {
                "name": name,
                "membershipStatus": "OPEN",
                "crmBasedUserList": {"uploadKeyType": "CONTACT_INFO"},
            }}])
        new_id = self._new_resource_id(data)
        return ActionResult(
            action="create_customer_list", account=account,
            summary=f"Created Customer Match list {name!r} (id {new_id}).",
            before=None, after={"id": new_id, "name": name})

    def _add_customer_list_members(self, cid, account, params) -> ActionResult:
        user_list_id = self._require_id(params, "user_list_id")
        emails = params.get("emails") or []
        phones = params.get("phones") or []
        if not isinstance(emails, list) or not isinstance(phones, list):
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "'emails' and 'phones' must be lists.", retriable=False)
        identifiers: List[Dict[str, str]] = []
        for e in emails:
            e = str(e).strip()
            if e:
                identifiers.append({"hashedEmail": self._hash_identifier(e)})
        for p in phones:
            p = str(p).strip()
            if p:
                identifiers.append({"hashedPhoneNumber": self._hash_identifier(p)})
        if not identifiers:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "Provide at least one email or phone number.",
                           retriable=False)
        if self._dry_run:
            return self._dry_run_partial(
                "add_customer_list_members", account,
                {"user_list_id": user_list_id, "member_count": len(identifiers)},
                f"Validated {len(identifiers)} member identifier(s) for list "
                f"{user_list_id}; no upload performed.")
        user_list_res = f"customers/{cid}/userLists/{user_list_id}"
        # Offline user-data job: create -> add operations -> run.
        created = self._call(
            "POST", f"{_BASE}/customers/{cid}/offlineUserDataJobs:create",
            {"job": {"type": "CUSTOMER_MATCH_USER_LIST",
                     "customerMatchUserListMetadata": {"userList": user_list_res}}})
        job_res = created.get("resourceName")
        if not job_res:
            raise ApiError(ErrorCode.UPSTREAM_ERROR,
                           "Google Ads did not return an offline-user-data job.",
                           retriable=False)
        self._call("POST", f"{_BASE}/{job_res}:addOperations",
                   {"enablePartialFailure": True,
                    "operations": [{"create": {"userIdentifiers": [ident]}}
                                   for ident in identifiers]})
        self._call("POST", f"{_BASE}/{job_res}:run", {})
        return ActionResult(
            action="add_customer_list_members", account=account,
            summary=(f"Uploaded {len(identifiers)} hashed member identifier"
                     f"{'' if len(identifiers) == 1 else 's'} to Customer Match "
                     f"list {user_list_id} (processing is asynchronous)."),
            before=None,
            after={"user_list_id": user_list_id, "member_count": len(identifiers),
                   "job": job_res})

    def _attach_audience(self, cid, account, params) -> ActionResult:
        ad_group_id = self._require_id(params, "ad_group_id")
        user_list_id = self._require_id(params, "user_list_id")
        data = self._mutate_call(
            f"{_BASE}/customers/{cid}/adGroupCriteria:mutate",
            [{"create": {
                "adGroup": f"customers/{cid}/adGroups/{ad_group_id}",
                "status": "ENABLED",
                "userList": {"userList": f"customers/{cid}/userLists/{user_list_id}"},
            }}])
        crit = self._criterion_ids(data)
        return ActionResult(
            action="attach_audience", account=account,
            summary=(f"Targeting user list {user_list_id} on ad group {ad_group_id}."),
            before=None,
            after={"ad_group_id": ad_group_id, "user_list_id": user_list_id,
                   "criterion_id": (crit[0] if crit else None)})

    def _remove_audience(self, cid, account, params) -> ActionResult:
        ad_group_id = self._require_id(params, "ad_group_id")
        criterion_id = self._require_id(params, "criterion_id")
        self._mutate(cid, "adGroupCriteria",
                     {"remove": f"customers/{cid}/adGroupCriteria/{ad_group_id}~{criterion_id}"})
        return ActionResult(
            action="remove_audience", account=account,
            summary=f"Removed audience {criterion_id} from ad group {ad_group_id}.",
            before={"ad_group_id": ad_group_id, "criterion_id": criterion_id},
            after={"ad_group_id": ad_group_id, "criterion_id": criterion_id,
                   "removed": True})


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
