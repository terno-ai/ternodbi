"""TikTok Ads connector.

Implements the `ApiConnector` interface against the TikTok API for Business
(Marketing API) directly:

- `list_accounts()` — `GET /oauth2/advertiser/get/` (the advertisers the user
                      authorised), then `GET /advertiser/info/` for currency and
                      timezone.
- `list_fields()`   — a *curated* catalogue per report type.
- `_run()`          — `GET /report/integrated/get/` for the performance report,
                      or `GET /{campaign|adgroup|ad}/get/` for entity reports.

TikTok differs from the other ad sources:
  * the token travels in an `Access-Token` header, not `Authorization: Bearer`,
    and it is long-term — TikTok issues advertisers no refresh token;
  * every response is an HTTP 200 envelope `{code, message, data}`, and a
    failure is a non-zero `code` rather than an HTTP status;
  * array parameters (`dimensions`, `metrics`, `fields`) are JSON-encoded
    strings on the query string;
  * a report groups by exactly one entity id (`campaign_id`, `adgroup_id` or
    `ad_id`, picked by `data_level`) plus at most one time column. Entity names
    and coarser ids are not dimensions but "attribute metrics": `campaign_name`
    is sent in `metrics` and read back as a column;
  * demographic and delivery splits (age, gender, country, platform) switch the
    report from BASIC to AUDIENCE;
  * one request may span at most 30 days with the daily split and one day with
    the hourly one, so longer ranges are fetched window by window;
  * money (spend, cpc, budget) is already in the account currency — no micros;
    metric values arrive as strings, and as "-" where a metric does not apply.
"""

from __future__ import annotations
import json
import logging
from datetime import date, timedelta
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

_BASE = "https://business-api.tiktok.com/open_api/v1.3"

# The most rows a report or entity listing returns per page.
_PAGE_SIZE = 1000
# `advertiser/info` takes at most 100 advertiser ids per call.
_INFO_BATCH = 100

# Envelope codes. Anything else non-zero is a request TikTok refused outright.
_AUTH_CODES = frozenset({40105})                  # token incorrect or revoked
_RATE_LIMIT_CODES = frozenset({40016, 40100, 40133})
_TRANSIENT_CODES = frozenset({50000, 51001, 51002, 51004, 51041, 51043, 60001})

# --- performance report ----------------------------------------------------

_METRICS: List[Field] = [
    Field("spend", "Spend", "metric", "Amount spent, in account currency.",
          data_type="number", is_monetary=True),
    Field("impressions", "Impressions", "metric", "Times ads were shown.",
          data_type="integer"),
    Field("clicks", "Clicks", "metric",
          "Clicks from the ad to its destination.", data_type="integer"),
    Field("ctr", "CTR", "metric", "Click-through rate, as a percentage.",
          data_type="number", is_non_aggregatable=True),
    Field("cpc", "CPC", "metric", "Average cost per click.", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    Field("cpm", "CPM", "metric", "Cost per 1,000 impressions.",
          data_type="number", is_monetary=True, is_non_aggregatable=True),
    # Reach counts people once per row, so it does not sum across days.
    Field("reach", "Reach", "metric", "People who saw the ads at least once.",
          data_type="integer", is_non_aggregatable=True),
    Field("cost_per_1000_reached", "Cost per 1,000 reached", "metric",
          "Cost per 1,000 people reached.", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    Field("frequency", "Frequency", "metric", "Average impressions per person.",
          data_type="number", is_non_aggregatable=True),
    # Conversions, counted against each ad group's optimisation event.
    Field("conversion", "Conversions", "metric",
          "Conversions for the ad group's optimisation event.",
          data_type="integer"),
    Field("cost_per_conversion", "Cost per conversion", "metric",
          "Average cost per conversion.", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    Field("conversion_rate", "Conversion rate", "metric",
          "Conversions per click, as a percentage.", data_type="number",
          is_non_aggregatable=True),
    Field("result", "Results", "metric",
          "Results for the ad group's optimisation goal.", data_type="integer"),
    Field("cost_per_result", "Cost per result", "metric",
          "Average cost per result.", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    Field("result_rate", "Result rate", "metric",
          "Results per impression, as a percentage.", data_type="number",
          is_non_aggregatable=True),
    Field("complete_payment", "Payments completed (web)", "metric",
          "CompletePayment events reported by the TikTok Pixel.",
          data_type="integer"),
    Field("value_per_complete_payment", "Value per payment (web)", "metric",
          "Average value of a completed payment.", data_type="number",
          is_monetary=True, is_non_aggregatable=True),
    Field("total_purchase_value", "Purchase value (app)", "metric",
          "Total value of in-app purchase events.", data_type="number",
          is_monetary=True),
    Field("onsite_shopping", "Purchases (TikTok Shop)", "metric",
          "Purchases made on TikTok Shop.", data_type="integer"),
    Field("total_onsite_shopping_value", "Gross revenue (TikTok Shop)",
          "metric", "Total value of TikTok Shop purchases.",
          data_type="number", is_monetary=True),
    Field("app_install", "App installs", "metric", "Attributed app installs.",
          data_type="integer"),
    # Video and engagement.
    Field("video_play_actions", "Video views", "metric",
          "Times the video started playing.", data_type="integer"),
    Field("video_watched_2s", "2-second views", "metric",
          "Plays of at least 2 seconds.", data_type="integer"),
    Field("video_watched_6s", "6-second views", "metric",
          "Plays of at least 6 seconds.", data_type="integer"),
    Field("video_views_p25", "Video 25% views", "metric",
          "Plays to 25% of the video.", data_type="integer"),
    Field("video_views_p50", "Video 50% views", "metric",
          "Plays to 50% of the video.", data_type="integer"),
    Field("video_views_p75", "Video 75% views", "metric",
          "Plays to 75% of the video.", data_type="integer"),
    Field("video_views_p100", "Video 100% views", "metric",
          "Plays to the end of the video.", data_type="integer"),
    Field("average_video_play", "Average play time", "metric",
          "Average seconds watched per play.", data_type="number",
          is_non_aggregatable=True),
    Field("average_video_play_per_user", "Average play time per person",
          "metric", "Average seconds watched per person.", data_type="number",
          is_non_aggregatable=True),
    Field("profile_visits", "Profile visits", "metric",
          "Visits to the advertiser's TikTok profile from the ad.",
          data_type="integer"),
    Field("likes", "Likes", "metric", "Likes on the ad.", data_type="integer"),
    Field("comments", "Comments", "metric", "Comments on the ad.",
          data_type="integer"),
    Field("shares", "Shares", "metric", "Shares of the ad.",
          data_type="integer"),
    Field("follows", "Follows", "metric",
          "New followers gained from the ad.", data_type="integer"),
]

# Entity columns, by level (0 campaign, 1 ad group, 2 ad). The finest level
# requested sets `data_level`, and its id is the report's grouping dimension;
# every other entity column is an attribute metric of that level.
_ENTITY_LEVEL: Dict[str, int] = {
    "campaign_id": 0, "campaign_name": 0,
    "adgroup_id": 1, "adgroup_name": 1,
    "ad_id": 2, "ad_name": 2,
}
_DATA_LEVELS: List[Tuple[str, str]] = [
    ("AUCTION_CAMPAIGN", "campaign_id"),
    ("AUCTION_ADGROUP", "adgroup_id"),
    ("AUCTION_AD", "ad_id"),
]
_ADVERTISER_LEVEL = ("AUCTION_ADVERTISER", "advertiser_id")

# Time splits: field id -> (TikTok dimension, the most days one request spans).
_TIME_SPLITS: Dict[str, Tuple[str, int]] = {
    "date": ("stat_time_day", 30),
    "hour": ("stat_time_hour", 1),
}
# The most days one request spans with no time split.
_MAX_TOTAL_DAYS = 365

# Audience splits. Any of these makes the report an AUDIENCE report.
_AUDIENCE: List[Field] = [
    Field("age", "Age", "dimension", "Age bracket of the audience."),
    Field("gender", "Gender", "dimension", "Gender of the audience."),
    Field("country_code", "Country", "dimension",
          "Country of the audience (ISO code)."),
    Field("province_id", "Region", "dimension",
          "Sub-national region of the audience (TikTok location id)."),
    Field("platform", "Platform", "dimension",
          "Device operating system — iPhone, Android, PC."),
    Field("placement", "Placement", "dimension",
          "Where the ad showed — TikTok, Pangle, Global App Bundle."),
    Field("language", "Language", "dimension", "Language of the audience."),
    Field("ac", "Network", "dimension",
          "Network connection type — Wi-Fi, 4G, 5G."),
]
_AUDIENCE_IDS = frozenset(f.id for f in _AUDIENCE)

_REPORT_DIMENSIONS: List[Field] = [
    Field("date", "Date", "dimension", "Day the stat occurred, in the "
          "advertiser's timezone.", data_type="date"),
    Field("hour", "Hour", "dimension", "Hour the stat occurred, in the "
          "advertiser's timezone. One day per request.", data_type="datetime"),
    Field("campaign_id", "Campaign ID", "dimension"),
    Field("campaign_name", "Campaign", "dimension"),
    Field("adgroup_id", "Ad group ID", "dimension"),
    Field("adgroup_name", "Ad group", "dimension"),
    Field("ad_id", "Ad ID", "dimension"),
    Field("ad_name", "Ad", "dimension"),
    *_AUDIENCE,
]

# --- entity reports --------------------------------------------------------

_ENTITY_ENDPOINT: Dict[str, str] = {
    "Campaigns": "campaign/get/",
    "AdGroups": "adgroup/get/",
    "Ads": "ad/get/",
}
_ENTITY_FIELDS: Dict[str, List[Field]] = {
    "Campaigns": [
        Field("campaign_id", "Campaign ID", "dimension"),
        Field("campaign_name", "Campaign", "dimension"),
        Field("objective_type", "Objective", "dimension"),
        Field("operation_status", "Status", "dimension",
              "ENABLE or DISABLE, as set by the advertiser."),
        Field("secondary_status", "Delivery status", "dimension",
              "Effective delivery status."),
        Field("budget_mode", "Budget mode", "dimension"),
        Field("budget", "Budget", "dimension", data_type="number",
              is_monetary=True),
        Field("create_time", "Created", "dimension", data_type="datetime"),
        Field("modify_time", "Modified", "dimension", data_type="datetime"),
    ],
    "AdGroups": [
        Field("adgroup_id", "Ad group ID", "dimension"),
        Field("adgroup_name", "Ad group", "dimension"),
        Field("campaign_id", "Campaign ID", "dimension"),
        Field("campaign_name", "Campaign", "dimension"),
        Field("operation_status", "Status", "dimension"),
        Field("secondary_status", "Delivery status", "dimension"),
        Field("placement_type", "Placement type", "dimension"),
        Field("optimization_goal", "Optimization goal", "dimension"),
        Field("billing_event", "Billing event", "dimension"),
        Field("bid_type", "Bid strategy", "dimension"),
        Field("bid_price", "Bid", "dimension", data_type="number",
              is_monetary=True),
        Field("budget_mode", "Budget mode", "dimension"),
        Field("budget", "Budget", "dimension", data_type="number",
              is_monetary=True),
        Field("schedule_start_time", "Start time", "dimension",
              data_type="datetime"),
        Field("schedule_end_time", "End time", "dimension",
              data_type="datetime"),
        Field("create_time", "Created", "dimension", data_type="datetime"),
        Field("modify_time", "Modified", "dimension", data_type="datetime"),
    ],
    "Ads": [
        Field("ad_id", "Ad ID", "dimension"),
        Field("ad_name", "Ad", "dimension"),
        Field("adgroup_id", "Ad group ID", "dimension"),
        Field("adgroup_name", "Ad group", "dimension"),
        Field("campaign_id", "Campaign ID", "dimension"),
        Field("campaign_name", "Campaign", "dimension"),
        Field("operation_status", "Status", "dimension"),
        Field("secondary_status", "Delivery status", "dimension"),
        Field("ad_format", "Ad format", "dimension"),
        Field("ad_text", "Ad text", "dimension"),
        Field("call_to_action", "Call to action", "dimension"),
        Field("landing_page_url", "Landing page", "dimension"),
        Field("video_id", "Video ID", "dimension"),
        Field("create_time", "Created", "dimension", data_type="datetime"),
        Field("modify_time", "Modified", "dimension", data_type="datetime"),
    ],
}

_DEFAULT_REPORT = "Performance"


def _fields_for(report_type: Optional[str]) -> Dict[str, Field]:
    if report_type in _ENTITY_ENDPOINT:
        return {f.id: f for f in _ENTITY_FIELDS[report_type]}
    return {f.id: f for f in (*_REPORT_DIMENSIONS, *_METRICS)}


def _default_http(method: str, url: str, token: str,
                  params: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    resp = requests.request(
        method, url, headers={"Access-Token": token}, params=params or {},
        timeout=30)
    if resp.status_code == 401:
        raise _AuthError()
    if resp.status_code >= 400:
        raise ApiError(ErrorCode.UPSTREAM_ERROR,
                       f"TikTok Ads error: HTTP {resp.status_code} "
                       f"{(resp.reason or '').strip()}".rstrip())
    return resp.json()


class _AuthError(Exception):
    """Internal marker for a rejected token, mapped to AUTH_EXPIRED."""


def _unwrap(envelope: Dict[str, Any]) -> Dict[str, Any]:
    """The `data` of a TikTok response envelope, or the failure it reports."""
    code = envelope.get("code")
    if code == 0:
        return envelope.get("data") or {}
    message = (str(envelope.get("message") or "") or "request failed").strip()[:300]
    if code in _AUTH_CODES:
        raise _AuthError()
    if code in _RATE_LIMIT_CODES:
        raise ApiError(ErrorCode.RATE_LIMITED,
                       f"TikTok rate limit reached: {message}",
                       retry_after_seconds=60)
    if code in _TRANSIENT_CODES:
        raise ApiError(ErrorCode.UPSTREAM_ERROR,
                       f"TikTok is temporarily unavailable ({code}): {message}")
    raise ApiError(ErrorCode.UPSTREAM_ERROR,
                   f"TikTok Ads error {code}: {message}", retriable=False)


def _windows(start: str, end: str, days: int) -> Iterator[Tuple[str, str]]:
    """Split an inclusive date range into consecutive windows of `days` days."""
    lo, hi = date.fromisoformat(start), date.fromisoformat(end)
    while lo <= hi:
        upper = min(hi, lo + timedelta(days=days - 1))
        yield lo.isoformat(), upper.isoformat()
        lo = upper + timedelta(days=1)


class TikTokAdsConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http

    # -- transport ----------------------------------------------------------

    def _call(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        try:
            envelope = self._http(
                "GET", f"{_BASE}/{path}", self.access_token(), params)
            return _unwrap(envelope)
        except _AuthError:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} access was rejected; reconnect the source.",
            )
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("TikTok Ads request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "TikTok returned an error. Try again.",
            )

    def _pages(self, path: str, params: Dict[str, Any],
               limit: int) -> List[Dict[str, Any]]:
        """Every `data.list` item across pages, stopping once `limit` are in."""
        items: List[Dict[str, Any]] = []
        page = 1
        while len(items) < limit:
            data = self._call(path, {**params, "page": page,
                                     "page_size": _PAGE_SIZE})
            items.extend(data.get("list") or [])
            total_pages = (data.get("page_info") or {}).get("total_page") or 1
            if page >= total_pages:
                break
            page += 1
        return items[:limit]

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        # TikTok lists an authorisation's advertisers only to the app that
        # holds it, so this call carries the app's own credentials.
        from terno_dbi.connectors.api.auth.providers import get_provider
        provider = get_provider("tiktok_ads")
        if provider is None or not provider.is_configured():
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "This deployment has no TikTok app credentials. Set "
                "TERNO_TIKTOK_APP_ID and TERNO_TIKTOK_APP_SECRET.",
                retriable=False,
            )
        data = self._call("oauth2/advertiser/get/", {
            "app_id": provider.client_id(),
            "secret": provider.client_secret(),
        })
        authorised = [(str(a["advertiser_id"]), a.get("advertiser_name"))
                      for a in data.get("list") or [] if a.get("advertiser_id")]
        info = self._advertiser_info([aid for aid, _name in authorised])
        accounts: List[Account] = []
        for aid, name in authorised:
            detail = info.get(aid, {})
            accounts.append(Account(
                id=aid,
                name=detail.get("name") or name or aid,
                currency=detail.get("currency"),
                timezone=detail.get("display_timezone") or detail.get("timezone"),
            ))
        return accounts

    def _advertiser_info(self, ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Name, currency and timezone per advertiser id; best effort."""
        info: Dict[str, Dict[str, Any]] = {}
        for i in range(0, len(ids), _INFO_BATCH):
            try:
                data = self._call("advertiser/info/", {
                    "advertiser_ids": json.dumps(ids[i:i + _INFO_BATCH]),
                    "fields": json.dumps(["advertiser_id", "name", "currency",
                                          "timezone", "display_timezone"]),
                })
            except ApiError as exc:
                # Only the picker's labels depend on this; the ids still work.
                logger.warning("TikTok advertiser/info failed: %s", exc.message)
                continue
            for adv in data.get("list") or []:
                if adv.get("advertiser_id"):
                    info[str(adv["advertiser_id"])] = adv
        return info

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
        if not (metrics or dims):
            metrics = [m.id for m in _METRICS[:3]]

        times = [d for d in dims if d in _TIME_SPLITS]
        if len(times) > 1:
            raise ApiError(
                ErrorCode.INVALID_FILTER,
                "date and hour can't be combined: TikTok splits a report by "
                "one time column. Pick one.")
        audience = [d for d in dims if d in _AUDIENCE_IDS]
        entity = [d for d in dims if d in _ENTITY_LEVEL]

        data_level, id_dim = (
            _DATA_LEVELS[max(_ENTITY_LEVEL[d] for d in entity)]
            if entity else _ADVERTISER_LEVEL)
        api_dims = [id_dim, *(_TIME_SPLITS[t][0] for t in times), *audience]
        # Names and coarser ids ride along as attribute metrics. TikTok needs at
        # least one metric, so a dimensions-only query asks for spend unseen.
        api_metrics = list(dict.fromkeys(
            [*metrics, *(d for d in entity if d != id_dim)])) or ["spend"]

        if times:
            span = _TIME_SPLITS[times[0]][1]
        else:
            span = _MAX_TOTAL_DAYS
            days = (date.fromisoformat(spec.date_range.end)
                    - date.fromisoformat(spec.date_range.start)).days + 1
            if days > _MAX_TOTAL_DAYS:
                raise ApiError(
                    ErrorCode.INVALID_FILTER,
                    f"TikTok totals cover at most {_MAX_TOTAL_DAYS} days; this "
                    f"range is {days}. Add the date dimension or shorten the "
                    f"range.")

        params: Dict[str, Any] = {
            "service_type": "AUCTION",
            "report_type": "AUDIENCE" if audience else "BASIC",
            "data_level": data_level,
            "dimensions": json.dumps(api_dims),
            "metrics": json.dumps(api_metrics),
        }
        requested = list(spec.fields) or metrics
        multi = len(spec.accounts) > 1

        def fetch(account):
            rows: List[Dict[str, Any]] = []
            for start, end in _windows(
                    spec.date_range.start, spec.date_range.end, span):
                remaining = spec.max_rows - len(rows)
                if remaining <= 0:
                    break
                items = self._pages("report/integrated/get/", {
                    **params, "advertiser_id": account,
                    "start_date": start, "end_date": end,
                }, remaining)
                rows.extend(_parse_report_row(item, requested, catalogue,
                                              account, multi=multi)
                            for item in items)
            return rows

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )

    def _run_entities(self, spec: QuerySpec, report_type) -> QueryResult:
        fields = spec.fields or [f.id for f in _ENTITY_FIELDS[report_type][:2]]
        endpoint = _ENTITY_ENDPOINT[report_type]
        multi = len(spec.accounts) > 1

        def fetch(account):
            items = self._pages(endpoint, {
                "advertiser_id": account, "fields": json.dumps(fields),
            }, spec.max_rows)
            out = []
            for obj in items:
                record: Dict[str, Any] = {}
                if multi:
                    record["_account"] = account
                for name in fields:
                    record[name] = obj.get(name)
                out.append(record)
            return out

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=list(fields),
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )


def _parse_report_row(item, requested_fields, catalogue, account, *,
                      multi=False) -> Dict[str, Any]:
    """Map one report row back to the requested field ids.

    A row is `{dimensions: {...}, metrics: {...}}`; a requested column may be in
    either (entity names are attribute metrics). Two remappings apply: `date`
    and `hour` arrive as `stat_time_day`/`stat_time_hour` timestamps, and the
    date keeps only its day.
    """
    dims = item.get("dimensions") or {}
    mets = item.get("metrics") or {}
    record: Dict[str, Any] = {}
    if multi:
        record["_account"] = account
    for name in requested_fields:
        key = _TIME_SPLITS[name][0] if name in _TIME_SPLITS else name
        raw = dims[key] if key in dims else mets.get(key)
        if name == "date" and isinstance(raw, str):
            raw = raw[:10]
        record[name] = _coerce(name, raw, catalogue)
    return record


def _coerce(field_id, raw, catalogue):
    if raw is None:
        return None
    field = catalogue.get(field_id)
    if field and field.kind == "metric":
        if raw == "-":   # TikTok's "not applicable"
            return None
        try:
            f = float(raw)
            # Money keeps decimals; counts collapse to int when whole.
            if field.is_monetary:
                return f
            return int(f) if f.is_integer() else f
        except (TypeError, ValueError):
            return raw
    return raw


def make_tiktok_ads_connector(datasource) -> TikTokAdsConnector:
    """Build a TikTok Ads connector wired to refresh its own OAuth token when due.

    TikTok's advertiser token is long-term, so the refresher is a no-op unless
    the token goes missing — then it marks the source for reconnection.
    """
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return TikTokAdsConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["TikTokAdsConnector", "make_tiktok_ads_connector"]
