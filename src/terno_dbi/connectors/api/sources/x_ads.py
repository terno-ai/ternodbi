"""X Ads connector.

Implements the `ApiConnector` interface against the X Ads API (v12) directly:

- `list_accounts()` — `GET /accounts` (the ad accounts the user can reach), with
                      each account's currency from its funding instruments.
- `list_fields()`   — a *curated* catalogue per report type.
- `_run()`          — `GET /stats/accounts/:id` for the performance report, or
                      `GET /accounts/:id/{campaigns|line_items|promoted_tweets}`
                      for entity reports.
- `execute_action()` — pause or resume a campaign or ad group (line item), and
                      set a campaign's budget, with `PUT` on the entity.

X Ads differs from the other ad sources:
  * it accepts only OAuth 1.0a: every request is signed with the app's consumer
    secret and the user's token secret (see `auth.oauth1`), and the token never
    expires;
  * stats are asked for by entity id, at most 20 ids and 7 days per request.
    The ids come from `active_entities` — the entities that served in the
    range — and the range is read window by window;
  * every metric is a time series: one value per day or hour of the window, or a
    single value for TOTAL. `null` means the entity did not serve;
  * an account-level stats request offers only a handful of metrics, so a report
    with no entity column is read per campaign and summed;
  * DAY buckets start at midnight in the account's timezone;
  * names and parent ids are not in the stats; they come from the entity lists;
  * money is in micros of the account currency (`*_local_micro`).
"""

from __future__ import annotations
import logging
from collections import namedtuple
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from terno_dbi.connectors.api.auth import oauth1
from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import (
    Account, Action, ActionResult, Field, QueryResult, QuerySpec,
)
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

_BASE = "https://ads-api.x.com/12"

# The most items an entity list returns per page.
_PAGE_SIZE = 1000
# The most entity ids, and days, one stats request takes.
_STATS_BATCH = 20
_STATS_DAYS = 7
# The longest range one active_entities request spans.
_ACTIVE_DAYS = 90
_MICRO = 1_000_000

_PLACEMENTS = ("ALL_ON_TWITTER", "PUBLISHER_NETWORK", "SPOTLIGHT", "TREND")
_DEFAULT_PLACEMENT = "ALL_ON_TWITTER"

Credentials = namedtuple(
    "Credentials", "consumer_key consumer_secret token token_secret")

# --- performance report ----------------------------------------------------

# Metric field id -> (metric group, the metric's key in the stats). A
# conversion is an object of series; its `metric` series is the count.
_METRIC_SOURCE: Dict[str, Tuple[str, str]] = {
    "spend": ("BILLING", "billed_charge_local_micro"),
    "billed_engagements": ("BILLING", "billed_engagements"),
    "impressions": ("ENGAGEMENT", "impressions"),
    "engagements": ("ENGAGEMENT", "engagements"),
    "clicks": ("ENGAGEMENT", "clicks"),
    "url_clicks": ("ENGAGEMENT", "url_clicks"),
    "app_clicks": ("ENGAGEMENT", "app_clicks"),
    "card_engagements": ("ENGAGEMENT", "card_engagements"),
    "qualified_impressions": ("ENGAGEMENT", "qualified_impressions"),
    "likes": ("ENGAGEMENT", "likes"),
    "reposts": ("ENGAGEMENT", "retweets"),
    "replies": ("ENGAGEMENT", "replies"),
    "follows": ("ENGAGEMENT", "follows"),
    "carousel_swipes": ("ENGAGEMENT", "carousel_swipes"),
    "video_views": ("VIDEO", "video_total_views"),
    "video_content_starts": ("VIDEO", "video_content_starts"),
    "video_views_25": ("VIDEO", "video_views_25"),
    "video_views_50": ("VIDEO", "video_views_50"),
    "video_views_75": ("VIDEO", "video_views_75"),
    "video_views_100": ("VIDEO", "video_views_100"),
    "video_3s100pct_views": ("VIDEO", "video_3s100pct_views"),
    "video_6s_views": ("VIDEO", "video_6s_views"),
    "video_15s_views": ("VIDEO", "video_15s_views"),
    "video_cta_clicks": ("VIDEO", "video_cta_clicks"),
    "conversion_purchases": ("WEB_CONVERSION", "conversion_purchases"),
    "conversion_sign_ups": ("WEB_CONVERSION", "conversion_sign_ups"),
    "conversion_site_visits": ("WEB_CONVERSION", "conversion_site_visits"),
    "conversion_downloads": ("WEB_CONVERSION", "conversion_downloads"),
    "conversion_add_to_carts": ("WEB_CONVERSION", "conversion_add_to_carts"),
    "conversion_checkouts_initiated": (
        "WEB_CONVERSION", "conversion_checkouts_initiated"),
    "conversion_custom": ("WEB_CONVERSION", "conversion_custom"),
}

_METRICS: List[Field] = [
    Field("spend", "Spend", "metric", "Amount billed, in account currency.",
          data_type="number", is_monetary=True),
    Field("impressions", "Impressions", "metric", "Times ads were shown.",
          data_type="integer"),
    Field("engagements", "Engagements", "metric",
          "Every interaction with a promoted post.", data_type="integer"),
    Field("clicks", "Clicks", "metric", "Clicks anywhere on the ad.",
          data_type="integer"),
    Field("url_clicks", "Link clicks", "metric",
          "Clicks on the ad's link or website card.", data_type="integer"),
    Field("app_clicks", "App clicks", "metric",
          "Clicks to install or open the app.", data_type="integer"),
    Field("billed_engagements", "Billed engagements", "metric",
          "Engagements the account was charged for.", data_type="integer"),
    Field("card_engagements", "Card engagements", "metric",
          data_type="integer"),
    Field("qualified_impressions", "Qualified impressions", "metric",
          data_type="integer"),
    Field("likes", "Likes", "metric", data_type="integer"),
    Field("reposts", "Reposts", "metric", data_type="integer"),
    Field("replies", "Replies", "metric", data_type="integer"),
    Field("follows", "Follows", "metric", "New followers from the ad.",
          data_type="integer"),
    Field("carousel_swipes", "Carousel swipes", "metric", data_type="integer"),
    Field("video_views", "Video views", "metric",
          "Views of the video, by X's definition.", data_type="integer"),
    Field("video_content_starts", "Video starts", "metric",
          data_type="integer"),
    Field("video_views_25", "Video 25% views", "metric", data_type="integer"),
    Field("video_views_50", "Video 50% views", "metric", data_type="integer"),
    Field("video_views_75", "Video 75% views", "metric", data_type="integer"),
    Field("video_views_100", "Video 100% views", "metric",
          data_type="integer"),
    Field("video_3s100pct_views", "3-second full-screen views", "metric",
          "Views of at least 3 seconds, 100% in view.", data_type="integer"),
    Field("video_6s_views", "6-second views", "metric", data_type="integer"),
    Field("video_15s_views", "15-second views", "metric",
          data_type="integer"),
    Field("video_cta_clicks", "Video CTA clicks", "metric",
          data_type="integer"),
    Field("conversion_purchases", "Purchases (web)", "metric",
          "Purchase conversions reported by the X Pixel.",
          data_type="integer"),
    Field("conversion_sign_ups", "Sign-ups (web)", "metric",
          data_type="integer"),
    Field("conversion_site_visits", "Site visits (web)", "metric",
          data_type="integer"),
    Field("conversion_downloads", "Downloads (web)", "metric",
          data_type="integer"),
    Field("conversion_add_to_carts", "Add to carts (web)", "metric",
          data_type="integer"),
    Field("conversion_checkouts_initiated", "Checkouts initiated (web)",
          "metric", data_type="integer"),
    Field("conversion_custom", "Custom conversions (web)", "metric",
          data_type="integer"),
]

# Entity columns, by level (0 campaign, 1 line item, 2 promoted post). The
# finest level requested is the level stats are asked for at.
_ENTITY_LEVEL: Dict[str, int] = {
    "campaign_id": 0, "campaign_name": 0,
    "line_item_id": 1, "line_item_name": 1,
    "promoted_tweet_id": 2, "tweet_id": 2,
}
_LEVELS = ("CAMPAIGN", "LINE_ITEM", "PROMOTED_TWEET")

# Time splits: field id -> stats granularity.
_TIME_SPLITS = {"date": "DAY", "hour": "HOUR"}

_REPORT_DIMENSIONS: List[Field] = [
    Field("date", "Date", "dimension",
          "Day of the stat, in the ad account's timezone.", data_type="date"),
    Field("hour", "Hour", "dimension",
          "Hour of the stat, in the ad account's timezone.",
          data_type="datetime"),
    Field("campaign_id", "Campaign ID", "dimension"),
    Field("campaign_name", "Campaign", "dimension"),
    Field("line_item_id", "Ad group ID", "dimension",
          "The line item (X's ad group)."),
    Field("line_item_name", "Ad group", "dimension"),
    Field("promoted_tweet_id", "Promoted post ID", "dimension"),
    Field("tweet_id", "Post ID", "dimension",
          "The X post being promoted."),
]

# --- entity reports --------------------------------------------------------

_ENTITY_ENDPOINT: Dict[str, str] = {
    "Campaigns": "campaigns",
    "LineItems": "line_items",
    "PromotedTweets": "promoted_tweets",
}
# Field id -> key on the API object, where they differ. Money keys are micros.
_ENTITY_KEYS: Dict[str, Dict[str, str]] = {
    "Campaigns": {"campaign_id": "id", "campaign_name": "name"},
    "LineItems": {"line_item_id": "id", "line_item_name": "name"},
    "PromotedTweets": {"promoted_tweet_id": "id"},
}
_MONEY_KEYS: Dict[str, str] = {
    "daily_budget": "daily_budget_amount_local_micro",
    "total_budget": "total_budget_amount_local_micro",
    "bid_amount": "bid_amount_local_micro",
    "target_cpa": "target_cpa_local_micro",
}
_ENTITY_FIELDS: Dict[str, List[Field]] = {
    "Campaigns": [
        Field("campaign_id", "Campaign ID", "dimension"),
        Field("campaign_name", "Campaign", "dimension"),
        Field("entity_status", "Status", "dimension",
              "ACTIVE, PAUSED or DRAFT, as set by the advertiser."),
        Field("effective_status", "Delivery status", "dimension"),
        Field("servable", "Servable", "dimension", data_type="boolean"),
        Field("funding_instrument_id", "Funding instrument ID", "dimension"),
        Field("daily_budget", "Daily budget", "dimension", data_type="number",
              is_monetary=True),
        Field("total_budget", "Total budget", "dimension", data_type="number",
              is_monetary=True),
        Field("budget_optimization", "Budget optimisation", "dimension"),
        Field("standard_delivery", "Standard delivery", "dimension",
              "Paced through the day rather than spent as fast as possible.",
              data_type="boolean"),
        Field("currency", "Currency", "dimension"),
        Field("created_at", "Created", "dimension", data_type="datetime"),
        Field("updated_at", "Modified", "dimension", data_type="datetime"),
    ],
    "LineItems": [
        Field("line_item_id", "Ad group ID", "dimension"),
        Field("line_item_name", "Ad group", "dimension"),
        Field("campaign_id", "Campaign ID", "dimension"),
        Field("entity_status", "Status", "dimension"),
        Field("objective", "Objective", "dimension"),
        Field("goal", "Optimisation goal", "dimension"),
        Field("product_type", "Product type", "dimension"),
        Field("placements", "Placements", "dimension",
              "Comma-separated placements."),
        Field("bid_strategy", "Bid strategy", "dimension"),
        Field("bid_amount", "Bid", "dimension", data_type="number",
              is_monetary=True),
        Field("target_cpa", "Target CPA", "dimension", data_type="number",
              is_monetary=True),
        Field("pay_by", "Billed by", "dimension"),
        Field("daily_budget", "Daily budget", "dimension", data_type="number",
              is_monetary=True),
        Field("total_budget", "Total budget", "dimension", data_type="number",
              is_monetary=True),
        Field("currency", "Currency", "dimension"),
        Field("start_time", "Start time", "dimension", data_type="datetime"),
        Field("end_time", "End time", "dimension", data_type="datetime"),
        Field("created_at", "Created", "dimension", data_type="datetime"),
        Field("updated_at", "Modified", "dimension", data_type="datetime"),
    ],
    "PromotedTweets": [
        Field("promoted_tweet_id", "Promoted post ID", "dimension"),
        Field("tweet_id", "Post ID", "dimension"),
        Field("line_item_id", "Ad group ID", "dimension"),
        Field("entity_status", "Status", "dimension"),
        Field("approval_status", "Review status", "dimension"),
        Field("created_at", "Created", "dimension", data_type="datetime"),
        Field("updated_at", "Modified", "dimension", data_type="datetime"),
    ],
}

_DEFAULT_REPORT = "Performance"

# --- write actions ---------------------------------------------------------


def _id_prop(noun: str, column: str) -> Dict[str, Any]:
    return {"type": "string", "minLength": 1,
            "description": f"The {noun}'s id, from the {column} column of a "
                           f"report."}


_BUDGET_PROP = {"type": "number", "exclusiveMinimum": 0}

_ACTIONS: List[Action] = [
    Action(
        "pause_campaign", "Pause campaign",
        "Pause a campaign so it stops serving and spending. Reversible with "
        "resume_campaign.",
        schema={"type": "object",
                "properties": {"campaign_id": _id_prop("campaign", "campaign_id")},
                "required": ["campaign_id"], "additionalProperties": False},
    ),
    Action(
        "resume_campaign", "Resume campaign",
        "Set a paused campaign back to active so it can serve. This starts "
        "spend — treat it as a deliberate, separately confirmed step.",
        schema={"type": "object",
                "properties": {"campaign_id": _id_prop("campaign", "campaign_id")},
                "required": ["campaign_id"], "additionalProperties": False},
    ),
    Action(
        "pause_ad_group", "Pause ad group",
        "Pause an ad group (X calls it a line item) so it stops serving. "
        "Reversible with resume_ad_group.",
        schema={"type": "object",
                "properties": {"line_item_id": _id_prop("ad group", "line_item_id")},
                "required": ["line_item_id"], "additionalProperties": False},
    ),
    Action(
        "resume_ad_group", "Resume ad group",
        "Set a paused ad group (line item) back to active. It serves, and "
        "spends, once its campaign is active too.",
        schema={"type": "object",
                "properties": {"line_item_id": _id_prop("ad group", "line_item_id")},
                "required": ["line_item_id"], "additionalProperties": False},
    ),
    Action(
        "set_campaign_budget", "Set campaign budget",
        "Set a campaign's daily budget, total budget, or both, in the ad "
        "account's own currency (e.g. 50 means 50.00). Affects spend.",
        schema={"type": "object",
                "properties": {
                    "campaign_id": _id_prop("campaign", "campaign_id"),
                    "daily_budget": {**_BUDGET_PROP,
                                     "description": "New daily budget, e.g. 50 "
                                                    "for 50.00 a day."},
                    "total_budget": {**_BUDGET_PROP,
                                     "description": "New lifetime budget for "
                                                    "the campaign."},
                },
                "required": ["campaign_id"],
                "anyOf": [{"required": ["daily_budget"]},
                          {"required": ["total_budget"]}],
                "additionalProperties": False},
    ),
]
_ACTIONS_BY_ID: Dict[str, Action] = {a.id: a for a in _ACTIONS}

# Status actions: action id -> (noun, entity path, id param, entity_status).
_STATUS_ACTIONS: Dict[str, Tuple[str, str, str, str]] = {
    "pause_campaign": ("Campaign", "campaigns", "campaign_id", "PAUSED"),
    "resume_campaign": ("Campaign", "campaigns", "campaign_id", "ACTIVE"),
    "pause_ad_group": ("Ad group", "line_items", "line_item_id", "PAUSED"),
    "resume_ad_group": ("Ad group", "line_items", "line_item_id", "ACTIVE"),
}
# Budget param -> the campaign's micros key.
_BUDGET_KEYS = {"daily_budget": "daily_budget_amount_local_micro",
                "total_budget": "total_budget_amount_local_micro"}


def _fields_for(report_type: Optional[str]) -> Dict[str, Field]:
    if report_type in _ENTITY_ENDPOINT:
        return {f.id: f for f in _ENTITY_FIELDS[report_type]}
    return {f.id: f for f in (*_REPORT_DIMENSIONS, *_METRICS)}


def _default_http(method: str, url: str, params: Optional[Dict],
                  creds: Credentials) -> Dict[str, Any]:
    """One signed request. The query is encoded here so it is sent as signed."""
    import requests
    full = f"{url}?{oauth1.encode_query(params)}" if params else url
    header = oauth1.authorization_header(
        method, full,
        consumer_key=creds.consumer_key, consumer_secret=creds.consumer_secret,
        token=creds.token, token_secret=creds.token_secret)
    resp = requests.request(method, full, headers={"Authorization": header},
                            timeout=30)
    if resp.status_code == 401:
        raise _AuthError()
    if resp.status_code >= 400:
        raise _http_error(resp, method)
    return resp.json()


def _http_error(resp, method: str = "GET") -> ApiError:
    """An X Ads refusal, by HTTP status first — some arrive as HTML."""
    status = resp.status_code
    code, message = "", ""
    try:
        err = (resp.json().get("errors") or [{}])[0]
        code, message = str(err.get("code") or ""), str(err.get("message") or "")
    except Exception:   # noqa: BLE001
        pass
    message = message.strip()[:300]
    label = f"{code}: {message}" if code and message else (code or message)
    if status == 429:
        return ApiError(
            ErrorCode.RATE_LIMITED,
            f"X Ads rate limit reached{': ' + label if label else ''}.",
            retry_after_seconds=_retry_after(resp))
    if code == "UNAUTHORIZED_CLIENT_APPLICATION":
        return ApiError(
            ErrorCode.UPSTREAM_ERROR,
            "This X app is not approved for the X Ads API. Apply for Ads API "
            "access for the app in the X developer portal.", retriable=False)
    if method != "GET" and status == 403:
        return ApiError(
            ErrorCode.ACCOUNT_FORBIDDEN,
            f"X Ads refused the change{': ' + label if label else ''}. Changes "
            f"need the X app set to \"Read and write\" and the connected user "
            f"to be an Ad manager or Account administrator on this ad account.",
            retriable=False, details={"status": status, "code": code})
    if method != "GET" and status == 400:
        # A value X will not take: a budget under its minimum, daily over total.
        return ApiError(
            ErrorCode.INVALID_ACTION_PARAMS,
            f"X Ads refused the change{': ' + label if label else ''}.",
            retriable=False, details={"status": status, "code": code})
    if status in (403, 404):
        return ApiError(
            ErrorCode.ACCOUNT_FORBIDDEN,
            f"X Ads refused access{': ' + label if label else ''}. The "
            f"connected user needs a role on this ad account.",
            retriable=False, details={"status": status, "code": code})
    if status >= 500:
        return ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"X Ads is temporarily unavailable (HTTP {status})"
            f"{': ' + label if label else ''}.",
            retry_after_seconds=_retry_after(resp))
    return ApiError(
        ErrorCode.UPSTREAM_ERROR,
        f"X Ads error: HTTP {status}{' ' + label if label else ''}",
        retriable=False, details={"status": status, "code": code})


def _retry_after(resp) -> Optional[int]:
    """Seconds to wait: the account-level reset if X sent one, else the user's."""
    headers = getattr(resp, "headers", None) or {}
    for name in ("x-account-rate-limit-reset", "x-rate-limit-reset"):
        try:
            reset = int(headers.get(name))
        except (TypeError, ValueError):
            continue
        return max(1, reset - int(datetime.now(timezone.utc).timestamp()))
    try:
        return int(headers.get("retry-after"))
    except (TypeError, ValueError):
        return None


class _AuthError(Exception):
    """Internal marker for a rejected token, mapped to AUTH_EXPIRED."""


def _tz(name: Optional[str]):
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        return ZoneInfo(name) if name else timezone.utc
    except (ZoneInfoNotFoundError, ValueError):
        return timezone.utc


def _midnight(day: date, tz) -> datetime:
    """Local midnight of `day`, as a UTC instant."""
    return datetime(day.year, day.month, day.day, tzinfo=tz).astimezone(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_time(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _windows(start: str, end: str, days: int) -> Iterator[Tuple[date, date]]:
    """Split an inclusive date range into consecutive windows of `days` days."""
    lo, hi = date.fromisoformat(start), date.fromisoformat(end)
    while lo <= hi:
        upper = min(hi, lo + timedelta(days=days - 1))
        yield lo, upper
        lo = upper + timedelta(days=1)


class XAdsConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http

    # -- transport ----------------------------------------------------------

    def _credentials(self) -> Credentials:
        from terno_dbi.connectors.api.auth.providers import get_provider
        provider = get_provider("x_ads")
        if provider is None or not provider.is_configured():
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "This deployment has no X app consumer keys. Set "
                "TERNO_X_CONSUMER_KEY and TERNO_X_CONSUMER_SECRET.",
                retriable=False,
            )
        token = self.access_token()
        secret = self._tokens().get("ACCESS_TOKEN_SECRET")
        if not secret:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} has no usable access token; reconnect the source.",
            )
        return Credentials(provider.client_id(), provider.client_secret(),
                           token, secret)

    def _call(self, path: str, params: Optional[Dict] = None, *,
              method: str = "GET") -> Dict[str, Any]:
        try:
            return self._http(method, f"{_BASE}/{path}", params or {},
                              self._credentials())
        except _AuthError:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} access was rejected; reconnect the source.",
            )
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("X Ads request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "X Ads returned an error. Try again.",
            )

    def _list(self, path: str, params: Optional[Dict] = None,
              limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Every item across cursor pages, stopping once `limit` are in."""
        items: List[Dict[str, Any]] = []
        cursor = None
        while limit is None or len(items) < limit:
            page = {**(params or {}), "count": _PAGE_SIZE}
            if cursor:
                page["cursor"] = cursor
            data = self._call(path, page)
            items.extend(data.get("data") or [])
            cursor = data.get("next_cursor")
            if not cursor:
                break
        return items if limit is None else items[:limit]

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        accounts: List[Account] = []
        for acc in self._list("accounts"):
            if not acc.get("id") or acc.get("deleted"):
                continue
            aid = str(acc["id"])
            accounts.append(Account(
                id=aid,
                name=acc.get("name") or acc.get("business_name") or aid,
                currency=self._currency(aid),
                timezone=acc.get("timezone"),
            ))
        return accounts

    def _currency(self, account: str) -> Optional[str]:
        """The account currency, from its funding instruments; best effort."""
        try:
            data = self._call(f"accounts/{account}/funding_instruments",
                              {"count": 10})
        except ApiError as exc:
            # Only the picker's label depends on this; the account still works.
            logger.warning("X Ads funding_instruments failed: %s", exc.message)
            return None
        for fi in data.get("data") or []:
            if fi.get("currency"):
                return fi["currency"]
        return None

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
            return self._run_entities(spec, report_type)
        return self._run_report(spec, catalogue)

    def _run_report(self, spec: QuerySpec, catalogue) -> QueryResult:
        metrics = [f for f in spec.fields if catalogue[f].kind == "metric"]
        dims = [f for f in spec.fields if catalogue[f].kind == "dimension"]
        if not metrics:
            metrics = [m.id for m in _METRICS[:3]]

        times = [d for d in dims if d in _TIME_SPLITS]
        if len(times) > 1:
            raise ApiError(
                ErrorCode.INVALID_FILTER,
                "date and hour can't be combined: X splits stats by one time "
                "column. Pick one.")
        placement = str(spec.settings.get("placement") or _DEFAULT_PLACEMENT).upper()
        if placement not in _PLACEMENTS:
            raise ApiError(
                ErrorCode.INVALID_SETTING,
                f"placement must be one of {', '.join(_PLACEMENTS)}.",
                retriable=False)

        entity = [d for d in dims if d in _ENTITY_LEVEL]
        # No entity column: read per campaign and sum, since account-level
        # stats leave out most metrics.
        level = max((_ENTITY_LEVEL[d] for d in entity), default=0)
        granularity = _TIME_SPLITS[times[0]] if times else "TOTAL"
        groups = sorted({_METRIC_SOURCE[m][0] for m in metrics})
        requested = list(spec.fields) or metrics
        multi = len(spec.accounts) > 1

        def fetch(account):
            tz = _tz(self._account_timezone(account))
            start = _midnight(date.fromisoformat(spec.date_range.start), tz)
            end = _midnight(
                date.fromisoformat(spec.date_range.end) + timedelta(days=1), tz)
            active = self._active_entities(
                account, _LEVELS[level], start, end, placement)
            if not active:
                return []
            buckets: Dict[Tuple, Dict[str, Any]] = {}
            for lo, hi in _windows(spec.date_range.start, spec.date_range.end,
                                   _STATS_DAYS):
                w_start = _midnight(lo, tz)
                w_end = _midnight(hi + timedelta(days=1), tz)
                ids = [eid for eid, (a_start, a_end) in active.items()
                       if (a_start is None or a_start < w_end)
                       and (a_end is None or a_end > w_start)]
                for i in range(0, len(ids), _STATS_BATCH):
                    data = self._call(f"stats/accounts/{account}", {
                        "entity": _LEVELS[level],
                        "entity_ids": ",".join(ids[i:i + _STATS_BATCH]),
                        "start_time": _iso(w_start),
                        "end_time": _iso(w_end),
                        "granularity": granularity,
                        "metric_groups": ",".join(groups),
                        "placement": placement,
                    })
                    _accumulate(buckets, data, metrics, granularity, lo,
                                w_start, tz, keep_entity=bool(entity))
            attrs = (self._attributes(account, level, entity) if entity
                     else (lambda eid: {}))
            rows = []
            for (eid, when) in sorted(buckets, key=lambda k: (k[1] or "", k[0] or "")):
                values = buckets[(eid, when)]
                about = attrs(eid)
                record: Dict[str, Any] = {}
                if multi:
                    record["_account"] = account
                for name in requested:
                    if name in values:
                        record[name] = values[name]
                    elif name in _TIME_SPLITS:
                        record[name] = when
                    else:
                        record[name] = about.get(name)
                rows.append(record)
            return rows[:spec.max_rows]

        rows, warnings = gather_accounts(spec.accounts, fetch)
        notes: List[str] = []
        if date.fromisoformat(spec.date_range.end) >= (
                datetime.now(timezone.utc).date() - timedelta(days=3)):
            notes.append("X finalises stats after 24 hours, and spend can still "
                         "change for up to 3 days.")
        return QueryResult(
            requested_field_ids=requested,
            rows=rows[:spec.max_rows],
            row_count=min(len(rows), spec.max_rows),
            notes=notes,
            warnings=warnings,
        )

    def _account_timezone(self, account: str) -> Optional[str]:
        data = self._call(f"accounts/{account}")
        return (data.get("data") or {}).get("timezone")

    def _active_entities(self, account, entity, start, end, placement):
        """Entity id -> (first, last) instant it served in the range.

        An entity with no placement list is kept; one that lists placements
        is kept only if the requested one is among them.
        """
        active: Dict[str, Tuple[Optional[datetime], Optional[datetime]]] = {}
        lo = start
        while lo < end:
            hi = min(end, lo + timedelta(days=_ACTIVE_DAYS))
            data = self._call(f"stats/accounts/{account}/active_entities", {
                "entity": entity, "start_time": _iso(lo), "end_time": _iso(hi),
            })
            for item in data.get("data") or []:
                eid = str(item.get("entity_id") or "")
                placements = item.get("placements")
                if not eid or (placements and placement not in placements):
                    continue
                a_start = _parse_time(item.get("activity_start_time"))
                a_end = _parse_time(item.get("activity_end_time"))
                if eid in active:
                    prev_start, prev_end = active[eid]
                    a_start = min(filter(None, (prev_start, a_start)), default=None)
                    a_end = max(filter(None, (prev_end, a_end)), default=None)
                active[eid] = (a_start, a_end)
            lo = hi
        return active

    def _attributes(self, account, level, entity_fields):
        """A lookup from a stats-entity id to its entity columns.

        Names and parent ids come from the entity lists, fetched only when a
        requested column needs them. Deleted entities are included: they can
        still have stats in the range.
        """
        wanted = set(entity_fields)
        params = {"with_deleted": "true"}

        campaigns: Dict[str, Dict[str, Any]] = {}
        if "campaign_name" in wanted:
            campaigns = {c["id"]: c for c in self._list(
                f"accounts/{account}/campaigns", params) if c.get("id")}
        line_items: Dict[str, Dict[str, Any]] = {}
        if level >= 1 and wanted & {"line_item_name", "campaign_id", "campaign_name"}:
            line_items = {li["id"]: li for li in self._list(
                f"accounts/{account}/line_items", params) if li.get("id")}
        promoted: Dict[str, Dict[str, Any]] = {}
        if level == 2 and wanted - {"promoted_tweet_id"}:
            promoted = {pt["id"]: pt for pt in self._list(
                f"accounts/{account}/promoted_tweets", params) if pt.get("id")}

        def chain(eid):
            """(promoted tweet, line item, campaign) ids above a stats entity."""
            pt_id = eid if level == 2 else None
            li_id = (promoted.get(eid, {}).get("line_item_id") if level == 2
                     else eid if level == 1 else None)
            c_id = (line_items.get(li_id, {}).get("campaign_id") if level >= 1
                    else eid)
            return pt_id, li_id, c_id

        def attrs(eid):
            pt_id, li_id, c_id = chain(eid)
            return {
                "promoted_tweet_id": pt_id,
                "tweet_id": promoted.get(eid, {}).get("tweet_id"),
                "line_item_id": li_id,
                "line_item_name": line_items.get(li_id, {}).get("name"),
                "campaign_id": c_id,
                "campaign_name": campaigns.get(c_id, {}).get("name"),
            }

        return attrs

    def _run_entities(self, spec: QuerySpec, report_type) -> QueryResult:
        fields = spec.fields or [f.id for f in _ENTITY_FIELDS[report_type][:2]]
        endpoint = _ENTITY_ENDPOINT[report_type]
        keys = _ENTITY_KEYS[report_type]
        multi = len(spec.accounts) > 1

        def fetch(account):
            items = self._list(f"accounts/{account}/{endpoint}",
                               limit=spec.max_rows)
            out = []
            for obj in items:
                record: Dict[str, Any] = {}
                if multi:
                    record["_account"] = account
                for name in fields:
                    record[name] = _entity_value(obj, name, keys)
                out.append(record)
            return out

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=list(fields),
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
        """Perform one write action on one ad account.

        Account authorisation happens upstream. Every action reads the entity
        first, which confirms it exists in this ad account and records its
        state as `before`. X Ads has no validate-only mode, so `dry_run=True`
        stops after that read.
        """
        if action_id not in _ACTIONS_BY_ID:
            raise ApiError(
                ErrorCode.UNKNOWN_ACTION,
                f"Unknown action {action_id!r}. Call list_actions for the "
                f"available actions.",
                retriable=False,
                details={"action": action_id, "available": sorted(_ACTIONS_BY_ID)},
            )
        params = params or {}
        if action_id in _STATUS_ACTIONS:
            return self._set_status(action_id, account, params, bool(dry_run))
        return self._set_campaign_budget(account, params, bool(dry_run))

    def _entity(self, account, path, noun, entity_id) -> Dict[str, Any]:
        """One campaign or line item, or a clear error if it is not here."""
        try:
            data = self._call(f"accounts/{account}/{path}/{entity_id}")
        except ApiError as exc:
            if exc.details.get("code") in ("NOT_FOUND", "INVALID_PARAMETER"):
                raise ApiError(
                    ErrorCode.INVALID_ACTION_PARAMS,
                    f"{noun} {entity_id} was not found in ad account {account}.",
                    retriable=False, details={"id": entity_id})
            raise
        return data.get("data") or {}

    def _set_status(self, action_id, account, params, dry_run) -> ActionResult:
        noun, path, id_key, status = _STATUS_ACTIONS[action_id]
        entity_id = _require_id(params, id_key)
        current = self._entity(account, path, noun, entity_id)
        before = {"id": entity_id, "name": current.get("name"),
                  "entity_status": current.get("entity_status")}
        label = current.get("name") or entity_id
        verb = "pause" if status == "PAUSED" else "resume"
        if dry_run:
            return _would(action_id, account,
                          f"Would {verb} {noun.lower()} {label}.", before=before)
        updated = self._call(f"accounts/{account}/{path}/{entity_id}",
                             {"entity_status": status}, method="PUT")
        after = {**before,
                 "entity_status": (updated.get("data") or {}).get("entity_status")
                 or status}
        return ActionResult(
            action=action_id, account=account,
            summary=f"{noun} {label} {verb}d.",
            before=before, after=after)

    def _set_campaign_budget(self, account, params, dry_run) -> ActionResult:
        campaign_id = _require_id(params, "campaign_id")
        changes: Dict[str, str] = {}
        for name, key in _BUDGET_KEYS.items():
            if params.get(name) is not None:
                changes[key] = str(round(_require_amount(params, name) * _MICRO))
        if not changes:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "Give daily_budget, total_budget or both.",
                           retriable=False)
        current = self._entity(account, "campaigns", "Campaign", campaign_id)
        before = _budget_state(campaign_id, current)
        label = current.get("name") or campaign_id
        if dry_run:
            would = {**current, **{k: int(v) for k, v in changes.items()}}
            return _would("set_campaign_budget", account,
                          f"Would set campaign {label}'s budget: "
                          f"{_describe_budget(_budget_state(campaign_id, would))}.",
                          before=before)
        updated = self._call(f"accounts/{account}/campaigns/{campaign_id}",
                             changes, method="PUT")
        after = _budget_state(campaign_id, updated.get("data") or {
            **current, **{k: int(v) for k, v in changes.items()}})
        return ActionResult(
            action="set_campaign_budget", account=account,
            summary=f"Campaign {label}'s budget set: {_describe_budget(after)}.",
            before=before, after=after)


def _require_id(params, key) -> str:
    value = params.get(key)
    if isinstance(value, int) and not isinstance(value, bool):
        value = str(value)
    if not isinstance(value, str) or not value.strip():
        raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                       f"'{key}' is required.", retriable=False)
    value = value.strip()
    # X Ads ids are short base-36 strings, e.g. '8wku2'.
    if not value.isalnum():
        raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                       f"'{key}' must be an X Ads id, e.g. '8wku2'.",
                       retriable=False, details={key: value})
    return value


def _require_amount(params, key) -> float:
    value = params.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                       f"'{key}' must be a positive amount in the ad account's "
                       f"currency, e.g. 50 for 50.00.", retriable=False,
                       details={key: value})
    return float(value)


def _budget_state(campaign_id, campaign) -> Dict[str, Any]:
    state: Dict[str, Any] = {"id": campaign_id, "name": campaign.get("name")}
    for name, key in _BUDGET_KEYS.items():
        micros = campaign.get(key)
        state[name] = micros / _MICRO if isinstance(micros, (int, float)) else None
    state["currency"] = campaign.get("currency")
    return state


def _describe_budget(state) -> str:
    currency = f" {state['currency']}" if state.get("currency") else ""
    parts = [f"{label} {state[name]:g}{currency}"
             for name, label in (("daily_budget", "daily"),
                                 ("total_budget", "total"))
             if state.get(name) is not None]
    return ", ".join(parts) or "none"


def _would(action, account, summary, *, before=None) -> ActionResult:
    """A dry run's result: what would happen, with nothing applied."""
    return ActionResult(action=action, account=account, summary=summary,
                        before=before, details={"dry_run": True, "applied": False})


def _entity_value(obj, name, keys):
    if name in _MONEY_KEYS:
        micros = obj.get(_MONEY_KEYS[name])
        return micros / _MICRO if isinstance(micros, (int, float)) else None
    value = obj.get(keys.get(name, name))
    if isinstance(value, list):
        return ",".join(str(v) for v in value)
    return value


def _series(metrics, metric_id) -> Optional[List[Any]]:
    """The time series behind one metric field, or None if it did not serve."""
    _group, key = _METRIC_SOURCE[metric_id]
    raw = metrics.get(key)
    if isinstance(raw, dict):   # a conversion: its `metric` series is the count
        raw = raw.get("metric")
    return raw if isinstance(raw, list) else None


def _accumulate(buckets, data, metrics, granularity, first_day, w_start, tz,
                *, keep_entity):
    """Fold one stats response into `buckets`, keyed by (entity, time label).

    Every X Ads metric is additive, so a TOTAL read in several windows, or a
    report without an entity column, sums into one bucket. A bucket in which
    no requested metric served is left out.
    """
    for item in data.get("data") or []:
        eid = str(item.get("id"))
        for seg in item.get("id_data") or []:
            series = {m: _series(seg.get("metrics") or {}, m) for m in metrics}
            length = max((len(s) for s in series.values() if s), default=0)
            for i in range(length):
                values = {m: (s[i] if s and i < len(s) else None)
                          for m, s in series.items()}
                if all(v is None for v in values.values()):
                    continue
                if granularity == "DAY":
                    when = (first_day + timedelta(days=i)).isoformat()
                elif granularity == "HOUR":
                    when = (w_start + timedelta(hours=i)).astimezone(tz) \
                        .strftime("%Y-%m-%dT%H:%M:%S")
                else:
                    when = None
                bucket = buckets.setdefault(
                    (eid if keep_entity else None, when), {})
                for m, v in values.items():
                    if v is None:
                        bucket.setdefault(m, None)
                        continue
                    if m == "spend":
                        v = v / _MICRO
                    bucket[m] = v if bucket.get(m) is None else bucket[m] + v


def make_x_ads_connector(datasource) -> XAdsConnector:
    """Build an X Ads connector.

    X Ads tokens do not expire, so the refresher is a no-op unless the token
    goes missing — then it marks the source for reconnection.
    """
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return XAdsConnector(datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["XAdsConnector", "make_x_ads_connector"]
