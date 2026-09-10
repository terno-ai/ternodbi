"""YouTube connector.

Implements the `ApiConnector` interface against Google's YouTube APIs directly:

- `list_accounts()` — Data API v3 `channels?mine=true` (the channels the user owns)
- `list_fields()`   — a *curated* catalogue per report type. YouTube Analytics
  accepts only specific metric/dimension combinations, so — unlike GA4's free
  field selection — each report type fixes its breakdown dimensions and the agent
  picks metrics from the valid set.
- `_run()`          — YouTube Analytics API v2 `reports`

Design note: the report type decides the *dimensions* (Geo → country, Device →
deviceType, Demographic → age/gender, …), and the caller's `fields` are the
*metrics* to return. This mirrors how YouTube's own reports are shaped and keeps
every generated query to a combination the API actually accepts.
"""

from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

_ANALYTICS_BASE = "https://youtubeanalytics.googleapis.com/v2/reports"
_DATA_BASE = "https://www.googleapis.com/youtube/v3"
_MONETARY_SCOPE = "https://www.googleapis.com/auth/yt-analytics-monetary.readonly"
_MEMBERSHIPS_SCOPE = "https://www.googleapis.com/auth/youtube.channel-memberships.creator"


# -- metric catalogue -------------------------------------------------------

def _metric(mid, name, desc="", data_type="integer", non_agg=False):
    return Field(mid, name, "metric", desc, data_type=data_type,
                 is_non_aggregatable=non_agg)


_CORE_METRICS = [
    _metric("views", "Views", "Number of views."),
    _metric("estimatedMinutesWatched", "Watch time (minutes)",
            "Estimated minutes watched."),
    _metric("averageViewDuration", "Avg. view duration (s)",
            "Average length of a view, in seconds.", non_agg=True),
]
_ENGAGEMENT_METRICS = [
    _metric("likes", "Likes"),
    _metric("dislikes", "Dislikes"),
    _metric("comments", "Comments"),
    _metric("shares", "Shares"),
    _metric("averageViewPercentage", "Avg. view %",
            "Average percentage of each video watched.", data_type="number",
            non_agg=True),
]
_SUBS_METRICS = [
    _metric("subscribersGained", "Subscribers gained"),
    _metric("subscribersLost", "Subscribers lost"),
]
_VIEWER_PERCENTAGE = _metric(
    "viewerPercentage", "Viewer %",
    "Share of views for the age/gender bucket.", data_type="number",
    non_agg=True)


def _money(mid, name, desc="", non_agg=False):
    return Field(mid, name, "metric", desc, data_type="number",
                 is_monetary=True, is_non_aggregatable=non_agg)


_MONETARY_METRICS = [
    _money("estimatedRevenue", "Estimated revenue",
           "Total estimated net revenue (ads + YouTube Premium)."),
    _money("estimatedAdRevenue", "Estimated ad revenue",
           "Estimated net revenue from ads."),
    _money("estimatedRedPartnerRevenue", "YouTube Premium revenue",
           "Estimated revenue from YouTube Premium subscribers."),
    _money("grossRevenue", "Gross revenue",
           "Estimated gross ad revenue before revenue share."),
    _money("cpm", "CPM", "Estimated cost per thousand ad impressions.",
           non_agg=True),
    _money("playbackBasedCpm", "Playback CPM",
           "Estimated CPM per thousand playbacks.", non_agg=True),
    _metric("adImpressions", "Ad impressions", "Number of ad impressions served."),
    _metric("monetizedPlaybacks", "Monetized playbacks",
            "Playbacks that showed at least one ad."),
]


def _dim(did, name, desc=""):
    return Field(did, name, "dimension", desc)


@dataclass(frozen=True)
class _Report:
    """One YouTube report type: its fixed dimensions and its valid metrics."""
    dimensions: List[Field]
    metrics: List[Field]
    sort: Optional[str] = None            # e.g. "-views" for a top-N list
    limited: bool = False                 # apply maxResults (top-N reports)
    filter_setting: Optional[str] = None  # settings key -> a required filter
    filter_dimension: Optional[str] = None
    endpoint: str = "analytics"           # "analytics" or "members" (Data API)

    def catalogue(self) -> Dict[str, Field]:
        return {f.id: f for f in (*self.dimensions, *self.metrics)}


_MEMBER_FIELDS = [
    _dim("member_name", "Member", "The member's display name."),
    _dim("member_channel_id", "Member channel ID"),
    _dim("level", "Membership level", "The member's current tier."),
    _dim("member_since", "Member since", "When they became a member."),
    _dim("total_months", "Total months", "Total months at this level."),
]


_REPORTS: Dict[str, _Report] = {
    "ChannelTotals": _Report(
        dimensions=[],
        metrics=[*_CORE_METRICS, *_ENGAGEMENT_METRICS, *_SUBS_METRICS],
    ),
    # Top videos by views.
    "LatestVideos": _Report(
        dimensions=[_dim("video", "Video", "The video id.")],
        metrics=[*_CORE_METRICS, *_ENGAGEMENT_METRICS],
        sort="-views", limited=True,
    ),
    # A single video's totals; video id supplied as a setting (see catalog).
    "VideoTotals": _Report(
        dimensions=[],
        metrics=[*_CORE_METRICS, *_ENGAGEMENT_METRICS],
        filter_setting="video_id", filter_dimension="video",
    ),
    "Geo": _Report(
        dimensions=[_dim("country", "Country", "ISO-3166-1 alpha-2 country.")],
        metrics=[*_CORE_METRICS],
    ),
    "Demographic": _Report(
        dimensions=[_dim("ageGroup", "Age group"), _dim("gender", "Gender")],
        metrics=[_VIEWER_PERCENTAGE],
    ),
    "Device": _Report(
        dimensions=[_dim("deviceType", "Device type")],
        metrics=[*_CORE_METRICS],
    ),
    "TrafficSources": _Report(
        dimensions=[_dim("insightTrafficSourceType", "Traffic source")],
        metrics=[*_CORE_METRICS],
    ),
    # Revenue: monetary metrics (needs the monetary scope + a monetized channel).
    "Revenue": _Report(
        dimensions=[],
        metrics=[*_MONETARY_METRICS],
    ),
    # Channel members: a Data API list, not an analytics report.
    "Members": _Report(
        dimensions=_MEMBER_FIELDS,
        metrics=[],
        endpoint="members",
    ),
}
_DEFAULT_REPORT = "ChannelTotals"


def _report_for(report_type: Optional[str]) -> _Report:
    return _REPORTS.get(report_type or "", _REPORTS[_DEFAULT_REPORT])


def _default_http(method: str, url: str, token: str,
                  json_body: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    kwargs: Dict[str, Any] = {}
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
        raise _yt_error(resp)
    return resp.json()


def _yt_error(resp) -> ApiError:
    """Surface Google's real error reason instead of a generic message."""
    message, reason = "", ""
    try:
        err = (resp.json() or {}).get("error", {})
        message = err.get("message", "")
        errors = err.get("errors") or []
        if errors:
            reason = errors[0].get("reason", "")
    except ValueError:
        message = (resp.text or "")[:200]
    label = f"{resp.status_code} {reason}".strip()
    return ApiError(
        ErrorCode.UPSTREAM_ERROR,
        f"YouTube API error ({label}): {message or 'unknown error'}",
        retriable=resp.status_code == 429 or resp.status_code >= 500,
    )


class _AuthError(Exception):
    """Internal marker for a 401 from Google, mapped to AUTH_EXPIRED."""


class YouTubeConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http

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
            logger.warning("YouTube request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "YouTube returned an error. Try again.",
            )

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        data = self._call(
            "GET", f"{_DATA_BASE}/channels",
            {"part": "snippet,statistics", "mine": "true", "maxResults": 50})
        accounts: List[Account] = []
        for item in data.get("items", []):
            cid = item.get("id")
            if not cid:
                continue
            title = (item.get("snippet") or {}).get("title", cid)
            stats = item.get("statistics") or {}
            extra = {}
            for key, out in (("videoCount", "video_count"),
                             ("subscriberCount", "subscriber_count"),
                             ("viewCount", "view_count")):
                if stats.get(key) is not None:
                    extra[out] = _coerce_number(stats.get(key))
            accounts.append(Account(id=cid, name=title, extra=extra))
        return accounts

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(_report_for(report_type).catalogue().values())

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        report = _report_for(spec.report_type)
        catalogue = report.catalogue()

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        if report.endpoint == "members":
            self.require_scope(_MEMBERSHIPS_SCOPE, "The channel members report")
            return self._run_members(spec)

        if spec.report_type == "Revenue":
            self.require_scope(_MONETARY_SCOPE, "The revenue report")

        # The report type fixes the breakdown dimensions; the caller's fields are
        # the metrics. A metric they did not name is simply not returned; if they
        # named none, fall back to the report's first metric so the query is valid.
        metrics = [f for f in spec.fields if catalogue[f].kind == "metric"]
        if not metrics:
            metrics = [report.metrics[0].id]
        dimensions = [d.id for d in report.dimensions]

        filters = []
        if report.filter_setting:
            value = (spec.settings or {}).get(report.filter_setting)
            filters.append(f"{report.filter_dimension}=={value}")

        base_params = {
            "startDate": spec.date_range.start,
            "endDate": spec.date_range.end,
            "metrics": ",".join(metrics),
        }
        if dimensions:
            base_params["dimensions"] = ",".join(dimensions)
        if report.sort:
            base_params["sort"] = report.sort
        if report.limited:
            base_params["maxResults"] = spec.max_rows
        filter_str = ";".join(filters)

        requested = [*dimensions, *metrics]
        multi = len(spec.accounts) > 1

        def fetch(account):
            params = dict(base_params, ids=f"channel=={account}")
            if filter_str:
                params["filters"] = filter_str
            data = self._call("GET", _ANALYTICS_BASE, params)
            return _parse_report(data, account, multi=multi)

        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )

    def _run_members(self, spec: QuerySpec) -> QueryResult:
        """`members.list` (Data API) — the caller's active channel members.

        Unlike the analytics reports this is a list of the authenticated owner's
        own channel members (there is no per-channel `ids` parameter and no date
        range — membership is current state), so it makes a single call and
        returns one row per member. Requires the
        `youtube.channel-memberships.creator` scope and a channel with the
        memberships program enabled.
        """
        data = self._call(
            "GET", f"{_DATA_BASE}/members",
            {"part": "snippet", "maxResults": min(spec.max_rows, 1000)})
        rows = []
        for item in data.get("items", []):
            snippet = item.get("snippet") or {}
            details = snippet.get("memberDetails") or {}
            md = snippet.get("membershipsDetails") or {}
            duration = md.get("membershipsDuration") or {}
            rows.append({
                "member_name": details.get("displayName"),
                "member_channel_id": details.get("channelId"),
                "level": md.get("highestAccessibleLevelDisplayName")
                or md.get("highestAccessibleLevel"),
                "member_since": duration.get("memberSince"),
                "total_months": duration.get("totalDurationMonths"),
            })
        return QueryResult(
            requested_field_ids=[f.id for f in _MEMBER_FIELDS],
            rows=rows,
            row_count=len(rows),
        )


def _parse_report(data, account, *, multi=False) -> List[Dict[str, Any]]:
    """Map a YouTube Analytics response to records.

    The response is column-oriented: `columnHeaders` names each column (in order)
    and `rows` are positional arrays. Reading ids from the headers keeps parsing
    correct whatever order YouTube returns dimensions and metrics in.
    """
    headers = data.get("columnHeaders", [])
    names = [h.get("name") for h in headers]
    kinds = [h.get("columnType") for h in headers]   # DIMENSION / METRIC

    result: List[Dict[str, Any]] = []
    for row in data.get("rows", []):
        record: Dict[str, Any] = {}
        if multi:
            record["_account"] = account
        for i, name in enumerate(names):
            value = row[i] if i < len(row) else None
            if kinds[i] == "METRIC":
                value = _coerce_number(value)
            record[name] = value
        result.append(record)
    return result


def _coerce_number(raw):
    if raw is None:
        return None
    try:
        f = float(raw)
        return int(f) if f.is_integer() else f
    except (TypeError, ValueError):
        return raw


def make_youtube_connector(datasource) -> YouTubeConnector:
    """Build a YouTube connector wired to refresh its own OAuth token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return YouTubeConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["YouTubeConnector", "make_youtube_connector"]
