"""Instagram Insights connector — your own account's performance.

Read-only over the Instagram Graph API for the Business/Creator accounts this
Meta credential manages:

- `list_accounts()` — the linked Instagram Business accounts (see `_instagram`).
- `list_fields()`   — a curated catalogue per report type.
- `_run()`
    * "AccountInsights" — day-period account metrics (reach, impressions,
      profile views, …) via `GET /{ig-id}/insights`, pivoted into one row per
      day with a column per metric.
    * "Media" — recent posts via `GET /{ig-id}/media`, with per-post fields and
      a couple of media insights.

Read-only today; the connector is structured (base `list_actions` /
`execute_action` hooks unused) so write actions — publishing media, replying to
comments — can be added later the way Google Ads did, behind the same
`connector:write` gate.
"""

from __future__ import annotations
import logging
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec
from terno_dbi.connectors.api.sources import _instagram as ig
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

# --- Account insights (day-period time series) -----------------------------

_ACCOUNT_METRICS: List[Field] = [
    Field("reach", "Reach", "metric", "Unique accounts that saw any content.",
          data_type="integer"),
    Field("impressions", "Impressions", "metric",
          "Total times content was shown.", data_type="integer"),
    Field("profile_views", "Profile views", "metric",
          "Times the profile was viewed.", data_type="integer"),
    Field("website_clicks", "Website clicks", "metric",
          "Taps on the website link in the profile.", data_type="integer"),
    Field("follower_count", "New followers", "metric",
          "Accounts that started following on the day.", data_type="integer"),
]
_ACCOUNT_METRIC_IDS = frozenset(f.id for f in _ACCOUNT_METRICS)
_ACCOUNT_DIMENSIONS: List[Field] = [
    Field("date", "Date", "dimension", "Day the stat occurred.",
          data_type="date"),
]

# --- Media (recent posts) --------------------------------------------------

_MEDIA_FIELDS: List[Field] = [
    Field("id", "Media ID", "dimension", data_type="string"),
    Field("timestamp", "Published", "dimension", "When the post was published.",
          data_type="string"),
    Field("media_type", "Type", "dimension", "IMAGE, VIDEO, CAROUSEL_ALBUM."),
    Field("caption", "Caption", "dimension", data_type="string"),
    Field("permalink", "Permalink", "dimension", data_type="string"),
    Field("like_count", "Likes", "metric", "Likes on the post.",
          data_type="integer"),
    Field("comments_count", "Comments", "metric", "Comments on the post.",
          data_type="integer"),
    Field("reach", "Reach", "metric", "Unique accounts that saw the post.",
          data_type="integer"),
    Field("saved", "Saves", "metric", "Times the post was saved.",
          data_type="integer"),
]
# The subset that comes from the per-media `insights` edge, not plain fields.
_MEDIA_INSIGHT_IDS = frozenset({"reach", "saved"})
_MEDIA_PLAIN_IDS = frozenset(
    f.id for f in _MEDIA_FIELDS) - _MEDIA_INSIGHT_IDS

_REPORTS: Dict[str, List[Field]] = {
    "AccountInsights": [*_ACCOUNT_DIMENSIONS, *_ACCOUNT_METRICS],
    "Media": list(_MEDIA_FIELDS),
}
_DEFAULT_REPORT = "AccountInsights"


def _fields_for(report_type: Optional[str]) -> Dict[str, Field]:
    fields = _REPORTS.get(report_type or _DEFAULT_REPORT, _REPORTS[_DEFAULT_REPORT])
    return {f.id: f for f in fields}


class InstagramInsightsConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or ig.default_http

    # -- transport ----------------------------------------------------------

    def _login_method(self) -> str:
        """"instagram" for Instagram Login, else "" (Facebook Login/default)."""
        try:
            return str(self._tokens().get("LOGIN_METHOD") or "")
        except ApiError:
            return ""

    def _base(self) -> str:
        """The API host for this connection's login method."""
        return ig.IG_LOGIN_BASE if self._login_method() == "instagram" else ig.BASE

    def _call(self, method: str, url: str,
              params: Optional[Dict] = None) -> Dict[str, Any]:
        try:
            return self._http(method, url, self.access_token(), params)
        except ig.AuthError:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} access was rejected; reconnect the source.",
            )
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("Instagram Insights request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Instagram returned an error. Try again.",
            )

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        if self._login_method() == "instagram":
            return ig.discover_ig_login_account(self._call)
        return ig.discover_ig_accounts(self._call)

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(_fields_for(report_type).values())

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = spec.report_type if spec.report_type in _REPORTS else _DEFAULT_REPORT
        catalogue = _fields_for(report_type)

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        if report_type == "Media":
            return self._run_media(spec, catalogue)
        return self._run_account_insights(spec, catalogue)

    def _run_account_insights(self, spec: QuerySpec, catalogue) -> QueryResult:
        metrics = [f for f in spec.fields if f in _ACCOUNT_METRIC_IDS]
        if not metrics:
            metrics = [m.id for m in _ACCOUNT_METRICS[:3]]
        params = {
            "metric": ",".join(metrics),
            "period": "day",
            "since": spec.date_range.start,
            "until": spec.date_range.end,
        }
        multi = len(spec.accounts) > 1
        base = self._base()

        def fetch(account):
            data = self._call("GET", f"{base}/{account}/insights", params)
            return _pivot_account_insights(data, metrics, account, multi=multi)

        rows, warnings = gather_accounts(spec.accounts, fetch)
        return QueryResult(
            requested_field_ids=list(spec.fields) or ["date", *metrics],
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )

    def _run_media(self, spec: QuerySpec, catalogue) -> QueryResult:
        requested = list(spec.fields) or ["id", "timestamp", "media_type",
                                          "like_count", "comments_count"]
        plain = [f for f in requested if f in _MEDIA_PLAIN_IDS]
        insight_metrics = [f for f in requested if f in _MEDIA_INSIGHT_IDS]
        # `id` always comes back; ensure the plain field list is non-empty.
        api_fields = list(dict.fromkeys(["id", *plain]))
        if insight_metrics:
            api_fields.append(f"insights.metric({','.join(insight_metrics)})")
        params = {"fields": ",".join(api_fields), "limit": spec.max_rows}
        multi = len(spec.accounts) > 1
        base = self._base()

        def fetch(account):
            data = self._call("GET", f"{base}/{account}/media", params)
            return _parse_media(data, requested, insight_metrics, account,
                                multi=multi)

        rows, warnings = gather_accounts(spec.accounts, fetch)
        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )


def _pivot_account_insights(data, metrics, account, *, multi=False):
    """Pivot Graph's per-metric time series into one row per day.

    Graph returns `{"data": [{"name": "reach", "values": [{"value": N,
    "end_time": "...T07:00:00+0000"}, ...]}, ...]}`. Collapse to
    `{date: {metric: value}}` keyed by the date part of `end_time`.
    """
    by_date: Dict[str, Dict[str, Any]] = {}
    for series in data.get("data", []):
        name = series.get("name")
        if name not in metrics:
            continue
        for point in series.get("values", []):
            end = str(point.get("end_time") or "")
            day = end[:10]
            if not day:
                continue
            by_date.setdefault(day, {})[name] = point.get("value")
    rows: List[Dict[str, Any]] = []
    for day in sorted(by_date):
        record: Dict[str, Any] = {}
        if multi:
            record["_account"] = account
        record["date"] = day
        for m in metrics:
            record[m] = by_date[day].get(m)
        rows.append(record)
    return rows


def _parse_media(data, requested, insight_metrics, account, *, multi=False):
    rows: List[Dict[str, Any]] = []
    for obj in data.get("data", []):
        record: Dict[str, Any] = {}
        if multi:
            record["_account"] = account
        insight_values = _media_insight_values(obj) if insight_metrics else {}
        for name in requested:
            if name in insight_metrics:
                record[name] = insight_values.get(name)
            else:
                record[name] = obj.get(name)
        rows.append(record)
    return rows


def _media_insight_values(obj) -> Dict[str, Any]:
    """Flatten a media object's nested `insights` edge to `{metric: value}`."""
    out: Dict[str, Any] = {}
    for series in ((obj.get("insights") or {}).get("data", []) or []):
        name = series.get("name")
        values = series.get("values") or []
        if name and values:
            out[name] = values[0].get("value")
    return out


def make_instagram_insights_connector(datasource) -> InstagramInsightsConnector:
    """Build an Instagram Insights connector that refreshes its token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return InstagramInsightsConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["InstagramInsightsConnector", "make_instagram_insights_connector"]
