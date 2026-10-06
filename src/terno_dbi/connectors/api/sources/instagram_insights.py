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
import time
from dataclasses import replace
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import (
    ApiError, ErrorCode, invalid_field, missing_setting,
)
from terno_dbi.connectors.api.model.types import (
    Account, Action, ActionResult, Field, QueryResult, QuerySpec,
)
from terno_dbi.connectors.api.sources import _instagram as ig
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

# --- Account insights ------------------------------------------------------

_ACCOUNT_METRICS: List[Field] = [
    Field("reach", "Reach", "metric", "Unique accounts that saw any content.",
          data_type="integer"),
    Field("follower_count", "New followers", "metric",
          "Accounts that started following on the day.", data_type="integer"),
]
_ACCOUNT_METRIC_IDS = frozenset(f.id for f in _ACCOUNT_METRICS)
_ACCOUNT_DIMENSIONS: List[Field] = [
    Field("date", "Date", "dimension", "Day the stat occurred.",
          data_type="date"),
]

# Total-value account metrics: one aggregate over the whole date range.
_ACCOUNT_TOTAL_METRICS: List[Field] = [
    Field("reach", "Reach", "metric", "Unique accounts reached in the range.",
          data_type="integer"),
    Field("profile_views", "Profile views", "metric",
          "Times the profile was viewed.", data_type="integer"),
    Field("website_clicks", "Website clicks", "metric",
          "Taps on the website link in the profile.", data_type="integer"),
    Field("accounts_engaged", "Accounts engaged", "metric",
          "Accounts that interacted with the profile.", data_type="integer"),
    Field("total_interactions", "Interactions", "metric",
          "Likes, saves, comments and shares combined.", data_type="integer"),
    Field("likes", "Likes", "metric", "Likes across content.",
          data_type="integer"),
    Field("comments", "Comments", "metric", "Comments across content.",
          data_type="integer"),
    Field("saves", "Saves", "metric", "Saves across content.",
          data_type="integer"),
    Field("shares", "Shares", "metric", "Shares across content.",
          data_type="integer"),
    Field("views", "Views", "metric",
          "Content views (the v22 replacement for impressions).",
          data_type="integer"),
]
_ACCOUNT_TOTAL_METRIC_IDS = frozenset(f.id for f in _ACCOUNT_TOTAL_METRICS)

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

# --- Comments (on one of your posts) ---------------------------------------
_COMMENT_FIELDS: List[Field] = [
    Field("id", "Comment ID", "dimension", data_type="string"),
    Field("text", "Text", "dimension", "The comment body.", data_type="string"),
    Field("username", "Username", "dimension", "Who commented.",
          data_type="string"),
    Field("timestamp", "Posted", "dimension", data_type="string"),
    Field("like_count", "Likes", "metric", "Likes on the comment.",
          data_type="integer"),
    Field("hidden", "Hidden", "dimension", "Whether the comment is hidden.",
          data_type="boolean"),
]
_COMMENT_API_FIELDS = ["id", "text", "username", "timestamp", "like_count",
                       "hidden"]

_REPORTS: Dict[str, List[Field]] = {
    "AccountInsights": [*_ACCOUNT_DIMENSIONS, *_ACCOUNT_METRICS],
    "AccountTotals": list(_ACCOUNT_TOTAL_METRICS),
    "Media": list(_MEDIA_FIELDS),
    "Comments": list(_COMMENT_FIELDS),
}
_DEFAULT_REPORT = "AccountInsights"


def _fields_for(report_type: Optional[str]) -> Dict[str, Field]:
    fields = _REPORTS.get(report_type or _DEFAULT_REPORT, _REPORTS[_DEFAULT_REPORT])
    return {f.id: f for f in fields}


# --- Write actions ---------------------------------------------------------
#
# Publishing + comment moderation. Gated upstream by `connector:write` (Org
# Admin only) and the per-account write opt-in, and audited — this module only
# performs an already-authorised action. Publishing is two-step (create a media
# container, then publish it); `dry_run` creates the container and stops, so the
# request is validated end to end with nothing going live.

def _url_prop(label: str) -> Dict[str, Any]:
    return {"type": "string", "format": "uri",
            "description": f"Public URL of the {label}. Instagram fetches the "
                           f"media from this URL; it must be reachable."}


_CAPTION_PROP = {"type": "string",
                 "description": "Caption text (optional, up to ~2,200 chars)."}
_COMMENT_ID_PROP = {"type": "string",
                    "description": "The target comment's id."}

_ACTIONS: List[Action] = [
    Action(
        "publish_photo", "Publish photo",
        "Publish a single photo to the feed. The image is fetched from a public "
        "URL. Goes live immediately once published — confirm before applying.",
        schema={"type": "object",
                "properties": {"image_url": _url_prop("image (JPEG)"),
                               "caption": _CAPTION_PROP},
                "required": ["image_url"], "additionalProperties": False},
        destructive=True,
    ),
    Action(
        "publish_video", "Publish video",
        "Publish a video to the feed, fetched from a public URL. Instagram must "
        "finish processing the video before it goes live.",
        schema={"type": "object",
                "properties": {"video_url": _url_prop("video (MP4/MOV)"),
                               "caption": _CAPTION_PROP},
                "required": ["video_url"], "additionalProperties": False},
        destructive=True,
    ),
    Action(
        "publish_reel", "Publish reel",
        "Publish a reel from a public video URL. `share_to_feed` also shows it "
        "in the main feed.",
        schema={"type": "object",
                "properties": {
                    "video_url": _url_prop("video (MP4/MOV)"),
                    "caption": _CAPTION_PROP,
                    "share_to_feed": {"type": "boolean",
                                      "description": "Also show in the feed "
                                                     "(default true)."}},
                "required": ["video_url"], "additionalProperties": False},
        destructive=True,
    ),
    Action(
        "publish_carousel", "Publish carousel",
        "Publish a carousel (2–10 items) from public image/video URLs.",
        schema={"type": "object",
                "properties": {
                    "items": {
                        "type": "array", "minItems": 2, "maxItems": 10,
                        "description": "Ordered carousel items.",
                        "items": {
                            "type": "object",
                            "properties": {"image_url": {"type": "string"},
                                           "video_url": {"type": "string"}},
                            "additionalProperties": False}},
                    "caption": _CAPTION_PROP},
                "required": ["items"], "additionalProperties": False},
        destructive=True,
    ),
    Action(
        "reply_to_comment", "Reply to comment",
        "Post a public reply to a comment on your media.",
        schema={"type": "object",
                "properties": {"comment_id": _COMMENT_ID_PROP,
                               "message": {"type": "string",
                                           "description": "Reply text."}},
                "required": ["comment_id", "message"],
                "additionalProperties": False},
        destructive=True,
    ),
    Action(
        "hide_comment", "Hide comment",
        "Hide (or unhide) a comment on your media. Reversible.",
        schema={"type": "object",
                "properties": {"comment_id": _COMMENT_ID_PROP,
                               "hide": {"type": "boolean",
                                        "description": "True to hide (default), "
                                                       "false to unhide."}},
                "required": ["comment_id"], "additionalProperties": False},
        destructive=True,
    ),
    Action(
        "delete_comment", "Delete comment",
        "Permanently delete a comment on your media. Not reversible.",
        schema={"type": "object",
                "properties": {"comment_id": _COMMENT_ID_PROP},
                "required": ["comment_id"], "additionalProperties": False},
        destructive=True,
    ),
    Action(
        "like_media", "Like post",
        "Like one of your media objects on behalf of the account. Reversible "
        "with unlike_media. (Facebook Login connections only.)",
        schema={"type": "object",
                "properties": {"media_id": {"type": "string",
                                            "description": "The media's id."}},
                "required": ["media_id"], "additionalProperties": False},
        destructive=True,
    ),
    Action(
        "unlike_media", "Unlike post",
        "Remove a like from a media object. (Facebook Login connections only.)",
        schema={"type": "object",
                "properties": {"media_id": {"type": "string",
                                            "description": "The media's id."}},
                "required": ["media_id"], "additionalProperties": False},
        destructive=True,
    ),
    Action(
        "like_comment", "Like comment",
        "Like a comment on your media. Reversible with unlike_comment. "
        "(Facebook Login connections only.)",
        schema={"type": "object",
                "properties": {"comment_id": _COMMENT_ID_PROP},
                "required": ["comment_id"], "additionalProperties": False},
        destructive=True,
    ),
    Action(
        "unlike_comment", "Unlike comment",
        "Remove a like from a comment. (Facebook Login connections only.)",
        schema={"type": "object",
                "properties": {"comment_id": _COMMENT_ID_PROP},
                "required": ["comment_id"], "additionalProperties": False},
        destructive=True,
    ),
]
_ACTIONS_BY_ID: Dict[str, Action] = {a.id: a for a in _ACTIONS}
_FB_LOGIN_ONLY_ACTIONS = frozenset({
    "like_media", "unlike_media", "like_comment", "unlike_comment"})

_CONTAINER_POLL_SECONDS = 2.0
_CONTAINER_POLL_TRIES = 15


class InstagramInsightsConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or ig.default_http

        self._dry_run = False

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
        if report_type == "AccountTotals":
            return self._run_account_totals(spec, catalogue)
        if report_type == "Comments":
            return self._run_comments(spec, catalogue)
        return self._run_account_insights(spec, catalogue)

    def _run_comments(self, spec: QuerySpec, catalogue) -> QueryResult:
        media_id = str(spec.settings.get("media_id") or "").strip()
        if not media_id:
            raise missing_setting("media_id", "Post/media id", "Comments")
        requested = list(spec.fields) or ["id", "text", "username", "timestamp"]
        base = self._base()
        data = self._call(
            "GET", f"{base}/{media_id}/comments",
            {"fields": ",".join(_COMMENT_API_FIELDS), "limit": spec.max_rows})
        rows = []
        for obj in data.get("data", []):
            rows.append({name: obj.get(name) for name in requested})
        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            warnings=[],
        )

    def _run_account_insights(self, spec: QuerySpec, catalogue) -> QueryResult:
        metrics = [f for f in spec.fields if f in _ACCOUNT_METRIC_IDS]
        if not metrics:
            metrics = [m.id for m in _ACCOUNT_METRICS]
        base_params = {
            "metric_type": "time_series",
            "period": "day",
            "since": spec.date_range.start,
            "until": spec.date_range.end,
        }
        multi = len(spec.accounts) > 1
        base = self._base()
        warnings: List[str] = []

        def fetch(account):
            series = self._fetch_account_series(
                base, account, metrics, base_params, warnings)
            return _pivot_account_insights(series, metrics, account, multi=multi)

        rows, acc_warnings = gather_accounts(spec.accounts, fetch)
        return QueryResult(
            requested_field_ids=list(spec.fields) or ["date", *metrics],
            rows=rows,
            row_count=len(rows),
            warnings=warnings + acc_warnings,
        )

    def _run_account_totals(self, spec: QuerySpec, catalogue) -> QueryResult:
        """Total-value account metrics: one aggregate row per account."""
        metrics = [f for f in spec.fields if f in _ACCOUNT_TOTAL_METRIC_IDS]
        if not metrics:
            metrics = [m.id for m in _ACCOUNT_TOTAL_METRICS]
        base_params = {
            "metric_type": "total_value",
            "period": "day",
            "since": spec.date_range.start,
            "until": spec.date_range.end,
        }
        multi = len(spec.accounts) > 1
        base = self._base()
        warnings: List[str] = []

        def fetch(account):
            values = self._fetch_account_totals(
                base, account, metrics, base_params, warnings)
            record: Dict[str, Any] = {}
            if multi:
                record["_account"] = account
            for m in metrics:
                record[m] = values.get(m)
            return [record]

        rows, acc_warnings = gather_accounts(spec.accounts, fetch)
        return QueryResult(
            requested_field_ids=list(spec.fields) or list(metrics),
            rows=rows,
            row_count=len(rows),
            warnings=warnings + acc_warnings,
        )

    def _fetch_account_series(self, base, account, metrics, base_params,
                              warnings) -> List[Dict[str, Any]]:
        """Daily account-insight series, resilient to per-metric failures.

        Tries all metrics in one call; if Instagram rejects the batch (a single
        deprecated/unsupported metric hard-errors the whole request), retries
        each metric alone and keeps the ones that succeed, recording a warning
        for the rest. So one bad metric never sinks the whole pull.
        """
        url = f"{base}/{account}/insights"
        try:
            return self._call(
                "GET", url, {**base_params, "metric": ",".join(metrics)},
            ).get("data", [])
        except ApiError:
            pass
        series: List[Dict[str, Any]] = []
        for metric in metrics:
            try:
                data = self._call("GET", url, {**base_params, "metric": metric})
                series.extend(data.get("data", []))
            except ApiError:
                warnings.append(
                    f"Instagram metric '{metric}' is unavailable for this "
                    f"account/API version and was skipped.")
        return series

    def _fetch_account_totals(self, base, account, metrics, base_params,
                              warnings) -> Dict[str, Any]:
        """`{metric: total}` for total-value metrics, resilient to failures."""
        url = f"{base}/{account}/insights"
        try:
            data = self._call(
                "GET", url, {**base_params, "metric": ",".join(metrics)})
            return _total_values(data)
        except ApiError:
            pass
        out: Dict[str, Any] = {}
        for metric in metrics:
            try:
                data = self._call("GET", url, {**base_params, "metric": metric})
                out.update(_total_values(data))
            except ApiError:
                warnings.append(
                    f"Instagram metric '{metric}' is unavailable for this "
                    f"account/API version and was skipped.")
        return out

    def _run_media(self, spec: QuerySpec, catalogue) -> QueryResult:
        requested = list(spec.fields) or ["id", "timestamp", "media_type",
                                          "like_count", "comments_count"]
        plain = [f for f in requested if f in _MEDIA_PLAIN_IDS]
        insight_metrics = [f for f in requested if f in _MEDIA_INSIGHT_IDS]
        # `id` always comes back; ensure the plain field list is non-empty.
        api_fields = list(dict.fromkeys(["id", *plain]))
        params = {"fields": ",".join(api_fields), "limit": spec.max_rows}
        multi = len(spec.accounts) > 1
        base = self._base()
        warnings: List[str] = []

        def fetch(account):
            data = self._call("GET", f"{base}/{account}/media", params)
            media = data.get("data", [])
            if insight_metrics:
                for obj in media:
                    obj["_insights"] = self._media_insights(
                        base, obj.get("id"), insight_metrics, warnings)
            return _parse_media(media, requested, insight_metrics, account,
                                multi=multi)

        rows, acc_warnings = gather_accounts(spec.accounts, fetch)
        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            warnings=warnings + acc_warnings,
        )

    def _media_insights(self, base, media_id, metrics, warnings) -> Dict[str, Any]:
        """Per-media insight values `{metric: value}`, resilient to failures."""
        if not media_id:
            return {}
        url = f"{base}/{media_id}/insights"
        try:
            data = self._call("GET", url, {"metric": ",".join(metrics)})
            return _insight_series_to_values(data)
        except ApiError:
            pass
        out: Dict[str, Any] = {}
        for metric in metrics:
            try:
                data = self._call("GET", url, {"metric": metric})
                out.update(_insight_series_to_values(data))
            except ApiError:
                warnings.append(
                    f"Instagram media metric '{metric}' was unavailable for one "
                    f"or more posts and was skipped.")
        return out

    # -- write actions ------------------------------------------------------

    def list_actions(self) -> List[Action]:
        if self._login_method() == "instagram":
            return [a for a in _ACTIONS if a.id not in _FB_LOGIN_ONLY_ACTIONS]
        return list(_ACTIONS)

    def execute_action(
        self, action_id: str, account: str,
        params: Optional[Dict[str, Any]] = None, dry_run: bool = False,
    ) -> ActionResult:
        """Perform one write action. Authorisation happens upstream.

        With `dry_run=True`, publishing builds the media container(s) and stops
        before `media_publish`, so the request is fully validated with nothing
        going live; comment actions validate their inputs and skip the mutation.
        """
        if action_id not in _ACTIONS_BY_ID:
            raise ApiError(
                ErrorCode.UNKNOWN_ACTION,
                f"Unknown action '{action_id}'.",
                details={"action": action_id, "available": sorted(_ACTIONS_BY_ID)},
            )
        if (action_id in _FB_LOGIN_ONLY_ACTIONS
                and self._login_method() == "instagram"):
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"'{action_id}' (likes) needs a Facebook Login connection; "
                f"this account is connected with Instagram Login.",
            )
        self._dry_run = bool(dry_run)
        try:
            result = self._dispatch_action(action_id, account, params or {})
        finally:
            was_dry = self._dry_run
            self._dry_run = False
        if was_dry and not (result.details or {}).get("dry_run"):
            result = replace(
                result,
                summary="[dry-run — not applied] " + result.summary,
                details={**(result.details or {}), "dry_run": True,
                         "applied": False},
            )
        return result

    def _dispatch_action(self, action_id, account, params) -> ActionResult:
        base = self._base()
        handlers = {
            "publish_photo": self._publish_photo,
            "publish_video": self._publish_video,
            "publish_reel": self._publish_reel,
            "publish_carousel": self._publish_carousel,
            "reply_to_comment": self._reply_to_comment,
            "hide_comment": self._hide_comment,
            "delete_comment": self._delete_comment,
            "like_media": self._like_media,
            "unlike_media": self._unlike_media,
            "like_comment": self._like_comment,
            "unlike_comment": self._unlike_comment,
        }
        return handlers[action_id](base, account, params)

    # -- publishing ---------------------------------------------------------

    def _create_container(self, base, account, fields) -> str:
        data = self._call("POST", f"{base}/{account}/media", fields)
        container = data.get("id")
        if not container:
            raise ApiError(ErrorCode.UPSTREAM_ERROR,
                           "Instagram did not return a media container id.")
        return str(container)

    def _publish_container(self, base, account, container, *, label, extra=None):
        """Publish a prepared container, or report the dry-run preview."""
        if self._dry_run:
            return ActionResult(
                action="publish", account=account,
                summary=f"Would publish {label} (container {container}).",
                details={"dry_run": True, "applied": False,
                         "creation_id": container, **(extra or {})})
        published = self._call(
            "POST", f"{base}/{account}/media_publish",
            {"creation_id": container})
        media_id = str(published.get("id") or "")
        return ActionResult(
            action="publish", account=account,
            summary=f"Published {label} (media {media_id}).",
            after={"media_id": media_id, **(extra or {})})

    def _wait_for_container(self, base, container) -> None:
        """Poll a video/reel container until it finishes processing."""
        if self._dry_run:
            return
        url = f"{base}/{container}"
        for _ in range(_CONTAINER_POLL_TRIES):
            data = self._call("GET", url, {"fields": "status_code"})
            status = data.get("status_code")
            if status == "FINISHED":
                return
            if status == "ERROR":
                raise ApiError(ErrorCode.UPSTREAM_ERROR,
                               "Instagram failed to process the video.")
            time.sleep(_CONTAINER_POLL_SECONDS)
        raise ApiError(ErrorCode.UPSTREAM_ERROR,
                       "Video is still processing; try publishing again shortly.")

    def _publish_photo(self, base, account, params) -> ActionResult:
        image_url = _require(params, "image_url")
        container = self._create_container(
            base, account,
            {"image_url": image_url, "caption": params.get("caption") or ""})
        return self._publish_container(base, account, container, label="photo")

    def _publish_video(self, base, account, params) -> ActionResult:
        video_url = _require(params, "video_url")
        container = self._create_container(
            base, account,
            {"media_type": "VIDEO", "video_url": video_url,
             "caption": params.get("caption") or ""})
        self._wait_for_container(base, container)
        return self._publish_container(base, account, container, label="video")

    def _publish_reel(self, base, account, params) -> ActionResult:
        video_url = _require(params, "video_url")
        fields = {"media_type": "REELS", "video_url": video_url,
                  "caption": params.get("caption") or ""}
        if "share_to_feed" in params:
            fields["share_to_feed"] = bool(params["share_to_feed"])
        container = self._create_container(base, account, fields)
        self._wait_for_container(base, container)
        return self._publish_container(base, account, container, label="reel")

    def _publish_carousel(self, base, account, params) -> ActionResult:
        items = params.get("items")
        if not isinstance(items, list) or len(items) < 2:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "A carousel needs at least 2 items.")
        child_ids: List[str] = []
        for item in items:
            if item.get("video_url"):
                fields = {"media_type": "VIDEO", "video_url": item["video_url"],
                          "is_carousel_item": "true"}
            elif item.get("image_url"):
                fields = {"image_url": item["image_url"],
                          "is_carousel_item": "true"}
            else:
                raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                               "Each carousel item needs an image_url or video_url.")
            child = self._create_container(base, account, fields)
            if fields.get("media_type") == "VIDEO":
                self._wait_for_container(base, child)
            child_ids.append(child)
        container = self._create_container(
            base, account,
            {"media_type": "CAROUSEL", "children": ",".join(child_ids),
             "caption": params.get("caption") or ""})
        return self._publish_container(
            base, account, container, label=f"carousel ({len(child_ids)} items)",
            extra={"children": child_ids})

    # -- comment moderation -------------------------------------------------

    def _reply_to_comment(self, base, account, params) -> ActionResult:
        comment_id = _require(params, "comment_id")
        message = _require(params, "message")
        if self._dry_run:
            return ActionResult(
                action="reply_to_comment", account=account,
                summary=f"Would reply to comment {comment_id}.",
                details={"dry_run": True, "applied": False})
        data = self._call("POST", f"{base}/{comment_id}/replies",
                          {"message": message})
        return ActionResult(
            action="reply_to_comment", account=account,
            summary=f"Replied to comment {comment_id}.",
            after={"reply_id": str(data.get("id") or "")})

    def _hide_comment(self, base, account, params) -> ActionResult:
        comment_id = _require(params, "comment_id")
        hide = bool(params.get("hide", True))
        verb = "hide" if hide else "unhide"
        if self._dry_run:
            return ActionResult(
                action="hide_comment", account=account,
                summary=f"Would {verb} comment {comment_id}.",
                details={"dry_run": True, "applied": False})
        self._call("POST", f"{base}/{comment_id}",
                   {"hide": "true" if hide else "false"})
        return ActionResult(
            action="hide_comment", account=account,
            summary=f"{verb.capitalize()}d comment {comment_id}.",
            after={"hidden": hide})

    def _delete_comment(self, base, account, params) -> ActionResult:
        comment_id = _require(params, "comment_id")
        if self._dry_run:
            return ActionResult(
                action="delete_comment", account=account,
                summary=f"Would delete comment {comment_id}.",
                details={"dry_run": True, "applied": False})
        self._call("DELETE", f"{base}/{comment_id}", {})
        return ActionResult(
            action="delete_comment", account=account,
            summary=f"Deleted comment {comment_id}.",
            before={"comment_id": comment_id})

    # -- likes (Facebook Login only) ----------------------------------------

    def _toggle_like(self, base, account, target_id, *, action, like, noun,
                     param_key):

        verb = "like" if like else "unlike"
        if self._dry_run:
            return ActionResult(
                action=action, account=account,
                summary=f"Would {verb} {noun} {target_id}.",
                details={"dry_run": True, "applied": False})
        method = "POST" if like else "DELETE"
        self._call(method, f"{base}/{account}/likes", {param_key: target_id})
        return ActionResult(
            action=action, account=account,
            summary=f"{verb.capitalize()}d {noun} {target_id}.",
            after={"liked": like})

    def _like_media(self, base, account, params) -> ActionResult:
        return self._toggle_like(base, account, _require(params, "media_id"),
                                 action="like_media", like=True, noun="post",
                                 param_key="media_id")

    def _unlike_media(self, base, account, params) -> ActionResult:
        return self._toggle_like(base, account, _require(params, "media_id"),
                                 action="unlike_media", like=False, noun="post",
                                 param_key="media_id")

    def _like_comment(self, base, account, params) -> ActionResult:
        return self._toggle_like(base, account, _require(params, "comment_id"),
                                 action="like_comment", like=True, noun="comment",
                                 param_key="comment_id")

    def _unlike_comment(self, base, account, params) -> ActionResult:
        return self._toggle_like(base, account, _require(params, "comment_id"),
                                 action="unlike_comment", like=False,
                                 noun="comment", param_key="comment_id")


def _require(params, key):
    value = params.get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                       f"'{key}' is required.")
    return value


def _pivot_account_insights(series_list, metrics, account, *, multi=False):
    """Pivot per-metric time series into one row per day.

    `series_list` is Graph's `data` array: `[{"name": "reach", "values":
    [{"value": N, "end_time": "...T07:00:00+0000"}, ...]}, ...]`. Collapse to
    `{date: {metric: value}}` keyed by the date part of `end_time`.
    """
    by_date: Dict[str, Dict[str, Any]] = {}
    for series in series_list:
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


def _parse_media(media, requested, insight_metrics, account, *, multi=False):
    rows: List[Dict[str, Any]] = []
    for obj in media:
        record: Dict[str, Any] = {}
        if multi:
            record["_account"] = account
        insight_values = obj.get("_insights") or {}
        for name in requested:
            if name in insight_metrics:
                record[name] = insight_values.get(name)
            else:
                record[name] = obj.get(name)
        rows.append(record)
    return rows


def _total_values(data) -> Dict[str, Any]:
    """Flatten a `metric_type=total_value` response to `{metric: value}`.

    Shape: `{"data": [{"name": "reach", "total_value": {"value": N}}, ...]}`.
    """
    out: Dict[str, Any] = {}
    for series in (data.get("data", []) or []):
        name = series.get("name")
        if name:
            out[name] = (series.get("total_value") or {}).get("value")
    return out


def _insight_series_to_values(data) -> Dict[str, Any]:
    """Flatten an insights `/insights` response to `{metric: value}`."""
    out: Dict[str, Any] = {}
    for series in (data.get("data", []) or []):
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
