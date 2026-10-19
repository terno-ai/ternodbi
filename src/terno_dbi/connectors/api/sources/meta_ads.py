"""Meta Ads connector.

Implements the `ApiConnector` interface against the Meta Marketing (Graph) API
directly:

- `list_accounts()` — `GET /me/adaccounts`
- `list_fields()`   — a *curated* catalogue per report type.
- `_run()`          — `GET /{account}/insights` for the metric report, or
                      `GET /{account}/{campaigns|adsets|ads}` for entity reports.

Meta differs from the Google sources:
  * requests are GET with query params, not POST bodies;
  * insights takes `level`, `breakdowns` and `time_increment` params — demographic
    and delivery splits (age, gender, country, platform) are `breakdowns`, while
    entity names (campaign_name, …) are ordinary `fields` that also set `level`;
  * the daily date split comes from `time_increment=1`, surfaced as `date_start`;
  * money (spend, cpc, cpm) is already in the account currency — no micros;
  * conversion metrics (purchases, leads, ROAS, …) are not flat columns — they
    live in the `actions`/`action_values`/`purchase_roas` arrays and are pulled
    out by action_type (see `_ACTION_METRICS`).
"""

from __future__ import annotations
import json
import logging
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

_API_VERSION = "v25.0"
_BASE = f"https://graph.facebook.com/{_API_VERSION}"

# --- insights (the metric report) ------------------------------------------

_INSIGHTS_METRICS: List[Field] = [
    Field("impressions", "Impressions", "metric", "Times ads were shown.",
          data_type="integer"),
    Field("clicks", "Clicks", "metric", "All clicks.", data_type="integer"),
    Field("reach", "Reach", "metric", "People who saw the ads.",
          data_type="integer"),
    Field("spend", "Spend", "metric", "Amount spent, in account currency.",
          data_type="number", is_monetary=True),
    Field("cpc", "CPC", "metric", "Average cost per click.", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    Field("cpm", "CPM", "metric", "Cost per 1,000 impressions.",
          data_type="number", is_monetary=True, is_non_aggregatable=True),
    Field("ctr", "CTR", "metric", "Click-through rate.", data_type="number",
          is_non_aggregatable=True),
    Field("frequency", "Frequency", "metric", "Average impressions per person.",
          data_type="number", is_non_aggregatable=True),
    # Link engagement (distinct from `clicks`, which counts every click type).
    Field("inline_link_clicks", "Link clicks", "metric",
          "Clicks on links to destinations.", data_type="integer"),
    Field("inline_link_click_ctr", "Link CTR", "metric",
          "Link click-through rate.", data_type="number",
          is_non_aggregatable=True),
    Field("cost_per_inline_link_click", "Cost per link click", "metric",
          "Average cost per link click.", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    # Unique / reach-based and cost metrics.
    Field("unique_clicks", "Unique clicks", "metric",
          "People who clicked at least once.", data_type="integer"),
    Field("unique_ctr", "Unique CTR", "metric",
          "Unique click-through rate.", data_type="number",
          is_non_aggregatable=True),
    Field("cpp", "Cost per 1,000 reached", "metric",
          "Cost per 1,000 people reached.", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    # Auction quality diagnostics (categorical ratings, not numbers).
    Field("quality_ranking", "Quality ranking", "metric",
          "Ad quality vs competitors (e.g. ABOVE_AVERAGE).",
          data_type="string", is_non_aggregatable=True),
    Field("engagement_rate_ranking", "Engagement rate ranking", "metric",
          "Expected engagement vs competitors.", data_type="string",
          is_non_aggregatable=True),
    Field("conversion_rate_ranking", "Conversion rate ranking", "metric",
          "Expected conversion rate vs competitors.", data_type="string",
          is_non_aggregatable=True),
]

# Conversion / action metrics. These do NOT come back as flat columns: Meta
# returns them inside the `actions`, `action_values` and `purchase_roas`
# arrays, each a list of {action_type, value}. We request the underlying array
# and pull the first matching action_type. The preferred-type lists cover the
# common omni_* (cross-channel) and bare variants; the exact key an advertiser
# emits depends on their pixel/CAPI event setup, so these are a curated best
# effort and should be spot-checked live per account.
# Preferred action_type order: omni_* (cross-publisher aggregate, matches the
# Ads Manager "Results" column) first, then the bare name, then pixel/onsite
# variants some accounts emit instead. `_action_value` takes the first match,
# so the omni_* aggregate is never double-counted with its pixel variant.
_P = ["omni_purchase", "purchase",
      "offsite_conversion.fb_pixel_purchase", "onsite_web_purchase",
      "onsite_web_app_purchase"]
_LEAD = ["lead", "onsite_conversion.lead_grouped",
         "offsite_conversion.fb_pixel_lead", "onsite_web_lead"]
_MSG = ["onsite_conversion.messaging_conversation_started_7d",
        "messaging_conversation_started_7d"]
_VC = ["omni_view_content", "view_content",
       "offsite_conversion.fb_pixel_view_content"]
_ATC = ["omni_add_to_cart", "add_to_cart",
        "offsite_conversion.fb_pixel_add_to_cart"]
_IC = ["omni_initiated_checkout", "initiate_checkout",
       "offsite_conversion.fb_pixel_initiate_checkout"]
_REG = ["omni_complete_registration", "complete_registration",
        "offsite_conversion.fb_pixel_complete_registration"]
_LPV = ["landing_page_view"]
_VID = ["video_view"]

_ACTION_METRICS: Dict[str, Dict[str, Any]] = {
    # --- conversion counts (from `actions`) ---
    "purchases": {"src": "actions", "types": _P},
    "leads": {"src": "actions", "types": _LEAD},
    "messaging_conversations": {"src": "actions", "types": _MSG},
    "view_content": {"src": "actions", "types": _VC},
    "add_to_cart": {"src": "actions", "types": _ATC},
    "initiate_checkout": {"src": "actions", "types": _IC},
    "add_payment_info": {"src": "actions",
                         "types": ["omni_add_payment_info", "add_payment_info",
                                   "offsite_conversion.fb_pixel_add_payment_info"]},
    "add_to_wishlist": {"src": "actions",
                        "types": ["omni_add_to_wishlist", "add_to_wishlist",
                                  "offsite_conversion.fb_pixel_add_to_wishlist"]},
    "search": {"src": "actions", "types": ["omni_search", "search",
                                           "offsite_conversion.fb_pixel_search"]},
    "subscribe": {"src": "actions",
                  "types": ["subscribe", "omni_subscribe"]},
    "start_trial": {"src": "actions",
                    "types": ["start_trial", "omni_start_trial"]},
    "contacts": {"src": "actions", "types": ["contact", "omni_contact"]},
    "app_installs": {"src": "actions",
                     "types": ["omni_app_install", "mobile_app_install"]},
    "landing_page_views": {"src": "actions", "types": _LPV},
    "registrations": {"src": "actions", "types": _REG},
    "post_engagement": {"src": "actions", "types": ["post_engagement"]},
    "video_views": {"src": "actions", "types": _VID},
    # --- conversion value + ROAS ---
    "purchase_value": {"src": "action_values", "types": _P, "monetary": True},
    "purchase_roas": {"src": "purchase_roas", "types": _P},
    # --- cost per result (from `cost_per_action_type`) ---
    "cost_per_purchase": {"src": "cost_per_action_type", "types": _P, "monetary": True},
    "cost_per_lead": {"src": "cost_per_action_type", "types": _LEAD, "monetary": True},
    "cost_per_messaging_conversation": {"src": "cost_per_action_type",
                                        "types": _MSG, "monetary": True},
    "cost_per_view_content": {"src": "cost_per_action_type", "types": _VC,
                              "monetary": True},
    "cost_per_add_to_cart": {"src": "cost_per_action_type", "types": _ATC,
                             "monetary": True},
    "cost_per_landing_page_view": {"src": "cost_per_action_type", "types": _LPV,
                                   "monetary": True},
    "cost_per_registration": {"src": "cost_per_action_type", "types": _REG,
                              "monetary": True},
    # --- video depth ---
    "thruplays": {"src": "video_thruplay_watched_actions", "types": _VID},
    "video_p25": {"src": "video_p25_watched_actions", "types": _VID},
    "video_p50": {"src": "video_p50_watched_actions", "types": _VID},
    "video_p75": {"src": "video_p75_watched_actions", "types": _VID},
    "video_p100": {"src": "video_p100_watched_actions", "types": _VID},
    # --- outbound clicks ---
    "outbound_clicks": {"src": "outbound_clicks", "types": ["outbound_click"]},
    "outbound_clicks_ctr": {"src": "outbound_clicks_ctr", "types": ["outbound_click"]},
    "cost_per_outbound_click": {"src": "cost_per_outbound_click",
                                "types": ["outbound_click"], "monetary": True},
}
# (id, label, description, monetary?, integer?)
_ACTION_METRIC_SPECS = [
    ("purchases", "Purchases", "Purchase conversions.", False, True),
    ("leads", "Leads", "Lead conversions.", False, True),
    ("messaging_conversations", "Messaging conversations",
     "Click-to-Message / WhatsApp conversations started (7d).", False, True),
    ("view_content", "View content", "ViewContent events.", False, True),
    ("add_to_cart", "Adds to cart", "Add-to-cart events.", False, True),
    ("initiate_checkout", "Checkouts initiated", "Initiate-checkout events.", False, True),
    ("add_payment_info", "Payment info added", "Add-payment-info events.", False, True),
    ("add_to_wishlist", "Adds to wishlist", "Add-to-wishlist events.", False, True),
    ("search", "Searches", "Search events.", False, True),
    ("subscribe", "Subscriptions", "Subscribe events.", False, True),
    ("start_trial", "Trials started", "Start-trial events.", False, True),
    ("contacts", "Contacts", "Contact events.", False, True),
    ("app_installs", "App installs", "Mobile app installs.", False, True),
    ("landing_page_views", "Landing page views",
     "Landing page views after a link click.", False, True),
    ("registrations", "Registrations", "Completed registrations.", False, True),
    ("post_engagement", "Post engagement",
     "Total post engagements (reactions, comments, shares, clicks).", False, True),
    ("video_views", "Video views", "Video views (3-second).", False, True),
    ("purchase_value", "Purchase value",
     "Total conversion value of purchases, in account currency.", True, False),
    ("purchase_roas", "Purchase ROAS",
     "Return on ad spend for purchases (value / spend).", False, False),
    ("cost_per_purchase", "Cost per purchase", "Average cost per purchase.", True, False),
    ("cost_per_lead", "Cost per lead", "Average cost per lead.", True, False),
    ("cost_per_messaging_conversation", "Cost per messaging conversation",
     "Average cost per conversation started.", True, False),
    ("cost_per_view_content", "Cost per view content",
     "Average cost per ViewContent.", True, False),
    ("cost_per_add_to_cart", "Cost per add to cart",
     "Average cost per add-to-cart.", True, False),
    ("cost_per_landing_page_view", "Cost per landing page view",
     "Average cost per landing page view.", True, False),
    ("cost_per_registration", "Cost per registration",
     "Average cost per registration.", True, False),
    ("thruplays", "ThruPlays",
     "Video plays to completion or 15s.", False, True),
    ("video_p25", "Video 25% views", "Views to 25% of the video.", False, True),
    ("video_p50", "Video 50% views", "Views to 50% of the video.", False, True),
    ("video_p75", "Video 75% views", "Views to 75% of the video.", False, True),
    ("video_p100", "Video 100% views", "Views to 100% of the video.", False, True),
    ("outbound_clicks", "Outbound clicks",
     "Clicks that leave Meta properties.", False, True),
    ("outbound_clicks_ctr", "Outbound CTR", "Outbound click-through rate.", False, False),
    ("cost_per_outbound_click", "Cost per outbound click",
     "Average cost per outbound click.", True, False),
]
_ACTION_METRIC_FIELDS: List[Field] = [
    Field(mid, label, "metric", desc,
          data_type="integer" if is_int else ("number" if not is_money else "number"),
          is_monetary=is_money,
          is_non_aggregatable=not is_int)
    for (mid, label, desc, is_money, is_int) in _ACTION_METRIC_SPECS
]
# API fields to request when any action metric is asked for.
_ACTION_SRC_FIELDS = tuple(dict.fromkeys(
    m["src"] for m in _ACTION_METRICS.values()))

# Entity-name fields select the insights `level`; each also comes back as a
# column. Ordered coarse->fine so the finest requested one wins.
_LEVEL_FIELDS: List[tuple] = [
    ("campaign_name", "campaign"),
    ("adset_name", "adset"),
    ("ad_name", "ad"),
]
_LEVEL_RANK = {name: i for i, (name, _lvl) in enumerate(_LEVEL_FIELDS)}
_LEVEL_OF = dict(_LEVEL_FIELDS)

# Demographic / delivery splits — passed as `breakdowns`, not `fields`.
_BREAKDOWNS: List[Field] = [
    Field("age", "Age", "dimension", "Age bracket of the audience."),
    Field("gender", "Gender", "dimension", "Gender of the audience."),
    Field("country", "Country", "dimension", "Country of the audience."),
    Field("region", "Region", "dimension",
          "Sub-national region of the audience."),
    Field("publisher_platform", "Platform", "dimension",
          "Facebook, Instagram, Audience Network, Messenger."),
    Field("platform_position", "Placement", "dimension",
          "Where the ad showed — feed, stories, reels, etc."),
    Field("device_platform", "Device", "dimension",
          "Desktop vs mobile device platform."),
    Field("impression_device", "Impression device", "dimension",
          "Specific device the impression showed on (iPhone, Android, …). "
          "Requires the impression_device feature enabled on the account."),
    Field("dma", "DMA", "dimension",
          "US Designated Market Area. May need enabling in Ads Manager."),
    Field("hour", "Hour", "dimension",
          "Hour of day, in the advertiser's timezone."),
    Field("hour_audience", "Hour (audience TZ)", "dimension",
          "Hour of day, in the audience's timezone. Requires the "
          "time_of_day_viewer_tz feature enabled on the account."),
]
# Breakdown id -> Meta API breakdown name, when they differ (friendlier ids).
_BREAKDOWN_API: Dict[str, str] = {
    "hour": "hourly_stats_aggregated_by_advertiser_time_zone",
    "hour_audience": "hourly_stats_aggregated_by_audience_time_zone",
}

_INSIGHTS_DIMENSIONS: List[Field] = [
    Field("date", "Date", "dimension", "Day the stat occurred.",
          data_type="date"),
    Field("campaign_name", "Campaign", "dimension"),
    Field("adset_name", "Ad set", "dimension"),
    Field("ad_name", "Ad", "dimension"),
    *_BREAKDOWNS,
]
_BREAKDOWN_IDS = frozenset(f.id for f in _BREAKDOWNS)

# --- entity reports --------------------------------------------------------

_ENTITY_ENDPOINT: Dict[str, str] = {
    "Campaigns": "campaigns",
    "AdSets": "adsets",
    "Ads": "ads",
}
# Budgets/bid amounts come back in the account currency's MINOR unit (e.g.
# cents). `_run_entities` converts them to major units using the account's
# currency offset, so these are exposed as real money (is_monetary).
_ENTITY_MONEY_FIELDS = frozenset({"daily_budget", "lifetime_budget", "bid_amount"})
_ENTITY_FIELDS: Dict[str, List[Field]] = {
    "Campaigns": [
        Field("id", "Campaign ID", "dimension", data_type="string"),
        Field("name", "Campaign", "dimension"),
        Field("status", "Status", "dimension"),
        Field("effective_status", "Effective status", "dimension"),
        Field("objective", "Objective", "dimension"),
        Field("buying_type", "Buying type", "dimension"),
        Field("bid_strategy", "Bid strategy", "dimension"),
        Field("daily_budget", "Daily budget", "dimension",
              data_type="number", is_monetary=True),
        Field("lifetime_budget", "Lifetime budget", "dimension",
              data_type="number", is_monetary=True),
        Field("start_time", "Start time", "dimension", data_type="date"),
        Field("stop_time", "Stop time", "dimension", data_type="date"),
        Field("created_time", "Created", "dimension", data_type="date"),
        Field("updated_time", "Updated", "dimension", data_type="date"),
    ],
    "AdSets": [
        Field("id", "Ad set ID", "dimension", data_type="string"),
        Field("name", "Ad set", "dimension"),
        Field("status", "Status", "dimension"),
        Field("effective_status", "Effective status", "dimension"),
        Field("campaign_id", "Campaign ID", "dimension", data_type="string"),
        Field("optimization_goal", "Optimization goal", "dimension"),
        Field("billing_event", "Billing event", "dimension"),
        Field("bid_strategy", "Bid strategy", "dimension"),
        Field("bid_amount", "Bid amount", "dimension",
              data_type="number", is_monetary=True),
        Field("daily_budget", "Daily budget", "dimension",
              data_type="number", is_monetary=True),
        Field("lifetime_budget", "Lifetime budget", "dimension",
              data_type="number", is_monetary=True),
        Field("start_time", "Start time", "dimension", data_type="date"),
        Field("end_time", "End time", "dimension", data_type="date"),
        Field("created_time", "Created", "dimension", data_type="date"),
        Field("updated_time", "Updated", "dimension", data_type="date"),
    ],
    "Ads": [
        Field("id", "Ad ID", "dimension", data_type="string"),
        Field("name", "Ad", "dimension"),
        Field("status", "Status", "dimension"),
        Field("effective_status", "Effective status", "dimension"),
        Field("adset_id", "Ad set ID", "dimension", data_type="string"),
        Field("campaign_id", "Campaign ID", "dimension", data_type="string"),
        Field("created_time", "Created", "dimension", data_type="date"),
        Field("updated_time", "Updated", "dimension", data_type="date"),
    ],
}

_DEFAULT_REPORT = "Insights"

# Diagnostic report: lists the raw action_type keys an account actually emits,
# so conversion mappings can be verified/extended per account instead of guessed.
_ACTION_TYPE_REPORT = "ActionTypes"
_ACTION_TYPE_FIELDS: List[Field] = [
    Field("action_type", "Action type", "dimension",
          "The Meta action_type key exactly as this account emits it."),
    Field("count", "Count", "metric",
          "Number of these actions in the period.", data_type="number"),
    Field("value", "Value", "metric",
          "Conversion value for this action_type, if any.",
          data_type="number", is_monetary=True),
]

# ISO 4217 minor-unit exponents that differ from the default of 2 (cents).
# Meta returns budgets/bids in minor units; dividing by the offset gives money.
_ZERO_DECIMAL_CCY = frozenset({
    "BIF", "CLP", "DJF", "GNF", "ISK", "JPY", "KMF", "KRW", "MGA", "PYG",
    "RWF", "UGX", "VND", "VUV", "XAF", "XOF", "XPF"})
_THREE_DECIMAL_CCY = frozenset({
    "BHD", "IQD", "JOD", "KWD", "LYD", "OMR", "TND"})


def _currency_offset(currency: Optional[str]) -> int:
    if currency in _ZERO_DECIMAL_CCY:
        return 1
    if currency in _THREE_DECIMAL_CCY:
        return 1000
    return 100


def _to_major(raw, offset: int):
    """Minor-unit budget string -> major-unit money float (e.g. 50000 -> 500.0)."""
    if raw is None:
        return None
    try:
        return int(raw) / offset
    except (TypeError, ValueError):
        return raw


# Documented breakdown x field incompatibilities (Insights Breakdowns docs).
# Enforced up front so a bad combo gives a clear message instead of Meta's
# opaque error surfaced as "try again".
_HOURLY_IDS = frozenset({"hour", "hour_audience"})
# Metrics backed by `video_*` API fields (quartiles + thruplays).
_VIDEO_FIELD_METRICS = frozenset({
    "thruplays", "video_p25", "video_p50", "video_p75", "video_p100"})
_VIDEO_QUARTILE_METRICS = frozenset({
    "video_p25", "video_p50", "video_p75", "video_p100"})
# Not supported under hourly breakdowns (return 0 / error).
_NO_HOURLY_METRICS = _VIDEO_FIELD_METRICS | frozenset({
    "unique_clicks", "unique_ctr", "reach", "frequency", "cpp"})


def _check_breakdown_support(breakdowns, metrics) -> None:
    """Reject documented breakdown x field combinations Meta can't serve."""
    def _bad(split, bad_metrics, why):
        hit = [m for m in metrics if m in bad_metrics]
        if hit:
            raise ApiError(
                ErrorCode.INVALID_FILTER,
                f"{hit[0]} can't be broken down by {split}: {why}.")

    if any(b in _HOURLY_IDS for b in breakdowns):
        split = next(b for b in breakdowns if b in _HOURLY_IDS)
        _bad(split, _NO_HOURLY_METRICS,
             "video, unique, reach and frequency metrics aren't available by hour")
    if "region" in breakdowns:
        _bad("region", _VIDEO_QUARTILE_METRICS,
             "video quartile metrics don't support the region split")
    if "dma" in breakdowns:
        _bad("dma", {"thruplays"}, "ThruPlays aren't available by DMA")


def _fields_for(report_type: Optional[str]) -> Dict[str, Field]:
    if report_type in _ENTITY_ENDPOINT:
        return {f.id: f for f in _ENTITY_FIELDS[report_type]}
    if report_type == _ACTION_TYPE_REPORT:
        return {f.id: f for f in _ACTION_TYPE_FIELDS}
    return {f.id: f for f in (*_INSIGHTS_DIMENSIONS, *_INSIGHTS_METRICS,
                              *_ACTION_METRIC_FIELDS)}


def _default_http(method: str, url: str, token: str,
                  json_body: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    kwargs: Dict[str, Any] = {}
    # Meta calls are GET with query params; the connector passes those as the
    # "body" slot, and they go on the query string rather than a JSON body.
    if method == "GET":
        kwargs["params"] = json_body or {}
    else:
        kwargs["json"] = json_body
    resp = requests.request(
        method, url, headers={"Authorization": f"Bearer {token}"},
        timeout=30, **kwargs)
    if resp.status_code == 401:
        raise _AuthError()
    if resp.status_code >= 400:
        raise ApiError(ErrorCode.UPSTREAM_ERROR,
                       f"Meta Ads error: {_meta_error_message(resp)}")
    return resp.json()


def _meta_error_message(resp) -> str:
    """Pull Meta's human-readable error out of the response body."""
    try:
        err = resp.json().get("error", {})
        msg = err.get("error_user_msg") or err.get("message") or ""
    except Exception:   # noqa: BLE001  (non-JSON body)
        msg = ""
    msg = (msg or resp.reason or "request failed").strip()
    return msg[:300]


class _AuthError(Exception):
    """Internal marker for a 401 from Meta, mapped to AUTH_EXPIRED."""


class MetaAdsConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http
        self._currency_cache: Dict[str, Optional[str]] = {}

    # -- transport ----------------------------------------------------------

    def _call(self, method: str, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
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
            logger.warning("Meta Ads request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Meta returned an error. Try again.",
            )

    @staticmethod
    def _account_path(account_id: str) -> str:
        """Meta ad-account ids are 'act_<digits>'; accept a bare id too."""
        aid = str(account_id)
        return aid if aid.startswith("act_") else f"act_{aid}"

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        data = self._call(
            "GET", f"{_BASE}/me/adaccounts",
            {"fields": "id,name,account_id,currency,timezone_name"})
        accounts: List[Account] = []
        for acc in data.get("data", []):
            acc_id = acc.get("id") or acc.get("account_id")
            if not acc_id:
                continue
            accounts.append(Account(
                id=acc_id,
                name=acc.get("name", acc_id),
                currency=acc.get("currency"),
                timezone=acc.get("timezone_name"),
            ))
        return accounts

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(_fields_for(report_type).values())

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = spec.report_type or _DEFAULT_REPORT
        catalogue = _fields_for(report_type)

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        if report_type in _ENTITY_ENDPOINT:
            return self._run_entities(spec, report_type, catalogue)
        if report_type == _ACTION_TYPE_REPORT:
            return self._run_action_types(spec, catalogue)
        return self._run_insights(spec, catalogue)

    def _run_action_types(self, spec: QuerySpec, catalogue) -> QueryResult:
        """Diagnostic: every action_type present, with count and value.

        Pulls the raw `actions`/`action_values` arrays at account level and
        flattens them, so you can see exactly which conversion keys an account
        emits (e.g. omni_purchase vs offsite_conversion.fb_pixel_purchase).
        """
        multi = len(spec.accounts) > 1
        params: Dict[str, Any] = {
            "time_range": json.dumps(
                {"since": spec.date_range.start, "until": spec.date_range.end}),
            "level": "account",
            "fields": "actions,action_values",
            "use_unified_attribution_setting": "true",
        }

        def fetch(account):
            url = f"{_BASE}/{self._account_path(account)}/insights"
            data = self._call("GET", url, params)
            out: List[Dict[str, Any]] = []
            for row in data.get("data", []):
                values = {a.get("action_type"): a.get("value")
                          for a in (row.get("action_values") or [])
                          if isinstance(a, dict)}
                for a in (row.get("actions") or []):
                    if not isinstance(a, dict):
                        continue
                    at = a.get("action_type")
                    rec: Dict[str, Any] = {}
                    if multi:
                        rec["_account"] = account
                    rec["action_type"] = at
                    rec["count"] = _coerce("count", a.get("value"), catalogue)
                    rec["value"] = _coerce("value", values.get(at), catalogue)
                    out.append(rec)
            return out

        rows, warnings = gather_accounts(spec.accounts, fetch)
        return QueryResult(
            requested_field_ids=["action_type", "count", "value"],
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )

    def _run_insights(self, spec: QuerySpec, catalogue) -> QueryResult:
        metrics = [f for f in spec.fields if catalogue[f].kind == "metric"]
        dims = [f for f in spec.fields if catalogue[f].kind == "dimension"]
        if not (metrics or dims):
            metrics = [m.id for m in _INSIGHTS_METRICS[:3]]

        breakdowns = [d for d in dims if d in _BREAKDOWN_IDS]
        level_fields = [d for d in dims if d in _LEVEL_OF]
        want_date = "date" in dims

        _check_breakdown_support(breakdowns, metrics)

        # Action/conversion metrics aren't flat columns — swap each for its
        # backing array field (actions / action_values / purchase_roas), deduped.
        flat_metrics = [m for m in metrics if m not in _ACTION_METRICS]
        action_metrics = [m for m in metrics if m in _ACTION_METRICS]
        action_srcs = [s for s in _ACTION_SRC_FIELDS
                       if any(_ACTION_METRICS[m]["src"] == s for m in action_metrics)]
        # Everything sent in `fields`: flat metrics, backing arrays, and entity-name
        # columns. The date split and breakdowns are separate params, not fields.
        api_fields = list(dict.fromkeys([*flat_metrics, *action_srcs, *level_fields]))

        params: Dict[str, Any] = {
            "time_range": json.dumps(
                {"since": spec.date_range.start, "until": spec.date_range.end}),
            "level": self._insights_level(level_fields),
            "limit": spec.max_rows,
            # Report conversions on the ad set's own attribution setting so the
            # numbers line up with Ads Manager (the API default is 7d-click only).
            "use_unified_attribution_setting": "true",
        }
        if api_fields:
            params["fields"] = ",".join(api_fields)
        if breakdowns:
            params["breakdowns"] = ",".join(
                _BREAKDOWN_API.get(b, b) for b in breakdowns)
        if want_date:
            params["time_increment"] = 1

        multi = len(spec.accounts) > 1

        def fetch(account):
            url = f"{_BASE}/{self._account_path(account)}/insights"
            data = self._call("GET", url, params)
            return _parse_insights(data, spec.fields, catalogue, account,
                                   multi=multi)

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=list(spec.fields) or api_fields,
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )

    @staticmethod
    def _insights_level(level_fields) -> str:
        if not level_fields:
            return "account"
        # The finest requested entity wins (ad > adset > campaign).
        finest = max(level_fields, key=lambda f: _LEVEL_RANK[f])
        return _LEVEL_OF[finest]

    def _account_currency(self, account) -> Optional[str]:
        """The account's ISO currency code, cached. None if it can't be read."""
        key = self._account_path(account)
        if key not in self._currency_cache:
            try:
                data = self._call("GET", f"{_BASE}/{key}", {"fields": "currency"})
                self._currency_cache[key] = data.get("currency")
            except ApiError:
                self._currency_cache[key] = None
        return self._currency_cache[key]

    def _run_entities(self, spec: QuerySpec, report_type, catalogue) -> QueryResult:
        fields = spec.fields or [f.id for f in _ENTITY_FIELDS[report_type][:2]]
        endpoint = _ENTITY_ENDPOINT[report_type]
        params = {"fields": ",".join(fields), "limit": spec.max_rows}

        multi = len(spec.accounts) > 1
        want_money = any(f in _ENTITY_MONEY_FIELDS for f in fields)

        def fetch(account):
            url = f"{_BASE}/{self._account_path(account)}/{endpoint}"
            data = self._call("GET", url, params)
            # Budgets are minor units; convert with the account's currency offset.
            offset = _currency_offset(
                self._account_currency(account)) if want_money else 100
            out = []
            for obj in data.get("data", []):
                record: Dict[str, Any] = {}
                if multi:
                    record["_account"] = account
                for name in fields:
                    raw = obj.get(name)
                    record[name] = (_to_major(raw, offset)
                                    if name in _ENTITY_MONEY_FIELDS else raw)
                out.append(record)
            return out

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=list(fields),
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )


def _parse_insights(data, requested_fields, catalogue, account, *,
                    multi=False) -> List[Dict[str, Any]]:
    """Map an insights response back to the requested field ids.

    Most columns are flat; three remappings apply:
      * `date` arrives as `date_start` under `time_increment=1`;
      * a friendlier breakdown id (e.g. `hour`) is keyed by its Meta API name;
      * action/conversion metrics are pulled from the `actions`/`action_values`/
        `purchase_roas` arrays by action_type.
    Metric values come as strings and are coerced to numbers.
    """
    result: List[Dict[str, Any]] = []
    for row in data.get("data", []):
        record: Dict[str, Any] = {}
        if multi:
            record["_account"] = account
        for name in requested_fields:
            if name in _ACTION_METRICS:
                raw = _action_value(row, _ACTION_METRICS[name])
            else:
                key = "date_start" if name == "date" else _BREAKDOWN_API.get(name, name)
                raw = row.get(key)
            record[name] = _coerce(name, raw, catalogue)
        result.append(record)
    return result


def _action_value(row, spec) -> Optional[str]:
    """Pull one action_type's value out of an `actions`-style array."""
    arr = row.get(spec["src"])
    if not isinstance(arr, list):
        return None
    by_type = {a.get("action_type"): a.get("value")
               for a in arr if isinstance(a, dict)}
    for action_type in spec["types"]:
        if action_type in by_type:
            return by_type[action_type]
    return None


def _coerce(field_id, raw, catalogue):
    if raw is None:
        return None
    field = catalogue.get(field_id)
    if field and field.kind == "metric":
        try:
            f = float(raw)
            # Money keeps decimals; counts collapse to int when whole.
            if field.is_monetary:
                return f
            return int(f) if f.is_integer() else f
        except (TypeError, ValueError):
            return raw
    return raw


def make_meta_ads_connector(datasource) -> MetaAdsConnector:
    """Build a Meta Ads connector wired to refresh its own OAuth token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return MetaAdsConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["MetaAdsConnector", "make_meta_ads_connector"]
