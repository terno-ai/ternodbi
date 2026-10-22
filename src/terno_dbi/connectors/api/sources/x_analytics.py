"""X Analytics connector — the connected account's own posts and profile.

Implements the `ApiConnector` interface against the X API v2 directly:

- `list_accounts()` — `GET /2/users/me`: the one account that connected.
- `list_fields()`   — a *curated* catalogue per report type.
- `_run()`          — `GET /2/users/:id/tweets` (Posts), `GET /2/users/:id/mentions`
                      (Mentions) or `GET /2/users/me` (Profile).
- `execute_action()` — post, reply, delete a post, like/unlike, and hide or
                      unhide a reply, as the connected account.

X differs from the other social sources:
  * a connection is one X account — the token acts as the user, and there are no
    pages or channels behind it to choose from;
  * there is no account-level time series: no daily impressions, no follower
    history. Metrics are per post and lifetime-to-date, and a post's `date` is
    the day it was published, not the day it was seen;
  * the owner-only metrics (engagements, link and profile clicks, organic
    counts) exist only for posts from the last 30 days, and asking for them
    drops older posts from a timeline outright. Older posts in a range are read
    with the public metrics alone, and those columns come back empty;
  * a timeline reaches back at most 3,200 posts, mentions 800;
  * reposts are left out of Posts: a repost carries the original post's public
    counts, not the account's own performance;
  * reads are billed per post on X's pay-per-use plan, so a query reads no
    more pages than `max_rows` needs;
  * a 200 can still carry an `errors` list for parts X would not return; those
    become warnings.
"""

from __future__ import annotations
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import (
    Account, Action, ActionResult, Field, QueryResult, QuerySpec,
)
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

_BASE = "https://api.x.com/2"

# The most posts a timeline page returns.
_PAGE_SIZE = 100
# Owner-only metrics are kept this long after a post is published. The margin
# keeps a post that crosses the line mid-query from failing the request.
_PRIVATE_METRICS_WINDOW = timedelta(days=30) - timedelta(hours=1)

_USER_FIELDS = ("id,name,username,created_at,description,location,url,"
                "verified,verified_type,protected,public_metrics")

# --- Posts and Mentions ----------------------------------------------------

# Metric field id -> (the post object holding it, its key there).
_PUBLIC: Dict[str, Tuple[str, str]] = {
    "impressions": ("public_metrics", "impression_count"),
    "likes": ("public_metrics", "like_count"),
    "reposts": ("public_metrics", "retweet_count"),
    "replies": ("public_metrics", "reply_count"),
    "quotes": ("public_metrics", "quote_count"),
    "bookmarks": ("public_metrics", "bookmark_count"),
}
_PRIVATE: Dict[str, Tuple[str, str]] = {
    "engagements": ("non_public_metrics", "engagements"),
    "url_link_clicks": ("non_public_metrics", "url_link_clicks"),
    "profile_clicks": ("non_public_metrics", "user_profile_clicks"),
    "organic_impressions": ("organic_metrics", "impression_count"),
    "organic_likes": ("organic_metrics", "like_count"),
    "organic_reposts": ("organic_metrics", "retweet_count"),
    "organic_replies": ("organic_metrics", "reply_count"),
}

_PUBLIC_METRICS: List[Field] = [
    Field("impressions", "Impressions", "metric",
          "Times the post was seen, lifetime to date.", data_type="integer"),
    Field("likes", "Likes", "metric", data_type="integer"),
    Field("reposts", "Reposts", "metric", data_type="integer"),
    Field("replies", "Replies", "metric", data_type="integer"),
    Field("quotes", "Quotes", "metric", "Quote posts of this post.",
          data_type="integer"),
    Field("bookmarks", "Bookmarks", "metric", data_type="integer"),
]
_VIDEO_METRICS: List[Field] = [
    Field("video_views", "Video views", "metric",
          "Views of the post's videos and GIFs; empty for a post without one.",
          data_type="integer"),
]
_PRIVATE_METRICS: List[Field] = [
    Field("engagements", "Engagements", "metric",
          "Every interaction with the post — likes, reposts, replies, "
          "clicks, expands. Last 30 days of posts only.", data_type="integer"),
    Field("url_link_clicks", "Link clicks", "metric",
          "Clicks on a link in the post. Last 30 days of posts only.",
          data_type="integer"),
    Field("profile_clicks", "Profile clicks", "metric",
          "Clicks through to the account's profile. Last 30 days of posts "
          "only.", data_type="integer"),
    Field("organic_impressions", "Organic impressions", "metric",
          "Impressions outside of promotion. Last 30 days of posts only.",
          data_type="integer"),
    Field("organic_likes", "Organic likes", "metric",
          "Last 30 days of posts only.", data_type="integer"),
    Field("organic_reposts", "Organic reposts", "metric",
          "Last 30 days of posts only.", data_type="integer"),
    Field("organic_replies", "Organic replies", "metric",
          "Last 30 days of posts only.", data_type="integer"),
]
_PRIVATE_IDS = frozenset(_PRIVATE)

_POST_DIMENSIONS: List[Field] = [
    Field("post_id", "Post ID", "dimension"),
    Field("created_at", "Published", "dimension", "When the post was "
          "published (UTC).", data_type="datetime"),
    Field("date", "Date", "dimension", "Day the post was published (UTC).",
          data_type="date"),
    Field("text", "Text", "dimension"),
    Field("post_type", "Post type", "dimension",
          "post, reply or quote."),
    Field("lang", "Language", "dimension", "Language X detected (BCP 47)."),
    Field("conversation_id", "Conversation ID", "dimension",
          "The post that started the thread."),
    Field("url", "URL", "dimension"),
]
_AUTHOR_DIMENSIONS: List[Field] = [
    Field("author_id", "Author ID", "dimension"),
    Field("author_username", "Author", "dimension", "The author's @username."),
    Field("author_name", "Author name", "dimension"),
    Field("author_followers", "Author followers", "dimension",
          "The author's follower count now.", data_type="integer"),
]

# --- Profile ---------------------------------------------------------------

_PROFILE_DIMENSIONS: List[Field] = [
    Field("user_id", "User ID", "dimension"),
    Field("username", "Username", "dimension"),
    Field("name", "Name", "dimension"),
    Field("description", "Bio", "dimension"),
    Field("location", "Location", "dimension"),
    Field("url", "Website", "dimension"),
    Field("created_at", "Joined", "dimension", data_type="datetime"),
    Field("verified", "Verified", "dimension", data_type="boolean"),
    Field("verified_type", "Verification type", "dimension",
          "blue, business, government or none."),
    Field("protected", "Protected", "dimension",
          "Whether the posts are visible to approved followers only.",
          data_type="boolean"),
]
# Field id -> key in the user's `public_metrics`. Counts as of now; X keeps no
# history of them.
_PROFILE_COUNTS: Dict[str, str] = {
    "followers": "followers_count",
    "following": "following_count",
    "posts": "tweet_count",
    "listed": "listed_count",
    "likes_given": "like_count",
    "media": "media_count",
}
_PROFILE_METRICS: List[Field] = [
    Field("followers", "Followers", "metric", "Followers now.",
          data_type="integer", is_non_aggregatable=True),
    Field("following", "Following", "metric", "Accounts followed now.",
          data_type="integer", is_non_aggregatable=True),
    Field("posts", "Posts", "metric", "Posts published, including replies "
          "and reposts.", data_type="integer", is_non_aggregatable=True),
    Field("listed", "Listed", "metric", "Lists the account is on.",
          data_type="integer", is_non_aggregatable=True),
    Field("likes_given", "Likes given", "metric", "Posts the account has "
          "liked.", data_type="integer", is_non_aggregatable=True),
    Field("media", "Media", "metric", "Photos and videos posted.",
          data_type="integer", is_non_aggregatable=True),
]

# X is renaming `tweet`/`retweet` keys to `post`/`repost`; either is read.
_RENAMED = {"tweet_count": "post_count", "retweet_count": "repost_count"}

_TIMELINES = {"Posts": "tweets", "Mentions": "mentions"}
_DEFAULT_REPORT = "Posts"
_DEFAULT_FIELDS = {
    "Posts": ["post_id", "created_at", "text", "impressions", "likes",
              "reposts", "replies"],
    "Mentions": ["post_id", "created_at", "author_username", "text",
                 "impressions", "likes"],
    "Profile": ["username", "followers", "following", "posts"],
}


def _id_prop(noun: str) -> Dict[str, Any]:
    return {"type": "string", "minLength": 1,
            "description": f"The {noun}'s id, e.g. from the post_id column of "
                           f"the Posts or Mentions report."}


_TEXT_PROP = {"type": "string", "minLength": 1,
              "description": "The post's text. Standard accounts are limited "
                             "to 280 characters; X refuses longer text."}

# --- write actions ---------------------------------------------------------

_ACTIONS: List[Action] = [
    Action(
        "create_post", "Create post",
        "Publish a new post from the account. It goes live to the account's "
        "followers immediately — treat it as a deliberate, separately "
        "confirmed step.",
        schema={"type": "object", "properties": {"text": _TEXT_PROP},
                "required": ["text"], "additionalProperties": False},
    ),
    Action(
        "reply_to_post", "Reply to post",
        "Reply from the account to a post, by its id. The reply goes live "
        "immediately.",
        schema={"type": "object",
                "properties": {"post_id": _id_prop("post being replied to"),
                               "text": _TEXT_PROP},
                "required": ["post_id", "text"], "additionalProperties": False},
    ),
    Action(
        "delete_post", "Delete post",
        "Permanently delete one of the account's own posts or replies. Cannot "
        "be undone.",
        schema={"type": "object", "properties": {"post_id": _id_prop("post")},
                "required": ["post_id"], "additionalProperties": False},
    ),
    Action(
        "like_post", "Like post",
        "Like a post as the account. Reversible with unlike_post.",
        schema={"type": "object", "properties": {"post_id": _id_prop("post")},
                "required": ["post_id"], "additionalProperties": False},
        destructive=False,
    ),
    Action(
        "unlike_post", "Unlike post",
        "Remove the account's like from a post.",
        schema={"type": "object", "properties": {"post_id": _id_prop("post")},
                "required": ["post_id"], "additionalProperties": False},
        destructive=False,
    ),
    Action(
        "hide_reply", "Hide or unhide reply",
        "Hide a reply to one of the account's own posts, or unhide it with "
        "hidden=false. A hidden reply is not deleted; it moves behind X's "
        "'hidden replies' view.",
        schema={"type": "object",
                "properties": {
                    "reply_id": _id_prop("reply"),
                    "hidden": {"type": "boolean",
                               "description": "true (default) hides the "
                                              "reply, false unhides it."},
                },
                "required": ["reply_id"], "additionalProperties": False},
        destructive=False,
    ),
]
_ACTIONS_BY_ID: Dict[str, Action] = {a.id: a for a in _ACTIONS}

# The scope each action needs, and how to name it when it was not granted.
_ACTION_SCOPES: Dict[str, Tuple[str, str]] = {
    "create_post": ("tweet.write", "Posting"),
    "reply_to_post": ("tweet.write", "Replying"),
    "delete_post": ("tweet.write", "Deleting a post"),
    "like_post": ("like.write", "Liking a post"),
    "unlike_post": ("like.write", "Unliking a post"),
    "hide_reply": ("tweet.moderate.write", "Hiding a reply"),
}


def _fields_for(report_type: Optional[str]) -> Dict[str, Field]:
    if report_type == "Profile":
        return {f.id: f for f in (*_PROFILE_DIMENSIONS, *_PROFILE_METRICS)}
    if report_type == "Mentions":
        return {f.id: f for f in (*_POST_DIMENSIONS, *_AUTHOR_DIMENSIONS,
                                  *_PUBLIC_METRICS, *_VIDEO_METRICS)}
    return {f.id: f for f in (*_POST_DIMENSIONS, *_PUBLIC_METRICS,
                              *_VIDEO_METRICS, *_PRIVATE_METRICS)}


def _default_http(method: str, url: str, token: str,
                  params: Optional[Dict] = None,
                  body: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    resp = requests.request(
        method, url, headers={"Authorization": f"Bearer {token}"},
        params=params or {}, json=body, timeout=30)
    if resp.status_code == 401:
        raise _AuthError()
    if resp.status_code >= 400:
        raise _http_error(resp, method)
    return resp.json()


def _http_error(resp, method: str = "GET") -> ApiError:
    """An X API refusal, carrying X's own explanation."""
    status = resp.status_code
    detail = ""
    try:
        body = resp.json()
        detail = (body.get("detail") or body.get("title")
                  or ((body.get("errors") or [{}])[0].get("message")) or "")
    except Exception:   # noqa: BLE001
        detail = (resp.text or "")[:300]
    # The messages below add their own full stop.
    detail = str(detail).strip()[:300].rstrip(".")
    if status == 429:
        return ApiError(
            ErrorCode.RATE_LIMITED,
            f"X's rate limit was reached{': ' + detail if detail else ''}.",
            retry_after_seconds=_retry_after(resp))
    if status == 403 and method != "GET":
        # A refused change: duplicate text, a reply X does not allow, a post
        # that is not the account's own. X's reason is the useful part.
        return ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"X refused the change{': ' + detail if detail else ''}.",
            retriable=False, details={"status": status})
    if status in (402, 403):
        # The app's plan: no read access on its tier, or credits/usage cap
        # spent. Retrying does not help until the plan changes.
        return ApiError(
            ErrorCode.QUOTA_EXCEEDED if status == 402 else ErrorCode.UPSTREAM_ERROR,
            f"X refused the request (HTTP {status})"
            f"{': ' + detail if detail else ''}. Check the X app's access "
            f"plan and usage in the X developer portal.",
            retriable=False, details={"status": status})
    return ApiError(
        ErrorCode.UPSTREAM_ERROR,
        f"X API error: HTTP {status}{' ' + detail if detail else ''}",
        retriable=status >= 500, details={"status": status})


def _retry_after(resp) -> Optional[int]:
    """Seconds until the window resets, from X's `x-rate-limit-reset` epoch."""
    headers = getattr(resp, "headers", None) or {}
    try:
        reset = int(headers.get("x-rate-limit-reset"))
    except (TypeError, ValueError):
        return None
    return max(1, reset - int(datetime.now(timezone.utc).timestamp()))


class _AuthError(Exception):
    """Internal marker for a rejected token, mapped to AUTH_EXPIRED."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


class XAnalyticsConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http

    # -- transport ----------------------------------------------------------

    def _call(self, path: str, params: Optional[Dict] = None, *,
              method: str = "GET", body: Optional[Dict] = None) -> Dict[str, Any]:
        try:
            if method == "GET":
                return self._http("GET", f"{_BASE}/{path}", self.access_token(),
                                  params)
            return self._http(method, f"{_BASE}/{path}", self.access_token(),
                              params, body)
        except _AuthError:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} access was rejected; reconnect the source.",
            )
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("X API request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "X returned an error. Try again.",
            )

    def _me(self) -> Dict[str, Any]:
        return self._call("users/me", {"user.fields": _USER_FIELDS}).get("data") or {}

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        me = self._me()
        if not me.get("id"):
            return []
        username = me.get("username") or ""
        name = me.get("name") or username or me["id"]
        return [Account(
            id=str(me["id"]),
            name=f"{name} (@{username})" if username else name,
            extra={"username": username} if username else {},
        )]

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(_fields_for(report_type).values())

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = spec.report_type or _DEFAULT_REPORT
        catalogue = _fields_for(report_type)

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        requested = list(spec.fields) or _DEFAULT_FIELDS[report_type]
        if report_type == "Profile":
            return self._run_profile(spec, requested)
        return self._run_timeline(spec, report_type, requested)

    def _run_profile(self, spec: QuerySpec, requested) -> QueryResult:
        multi = len(spec.accounts) > 1

        def fetch(account):
            me = self._me()
            if str(me.get("id")) != str(account):
                raise ApiError(
                    ErrorCode.ACCOUNT_FORBIDDEN,
                    f"This connection is X account {me.get('id')}, not "
                    f"{account}.", retriable=False)
            return [_profile_row(me, requested, account, multi=multi)]

        rows, warnings = gather_accounts(spec.accounts, fetch)
        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )

    def _run_timeline(self, spec: QuerySpec, report_type, requested) -> QueryResult:
        start = datetime.fromisoformat(spec.date_range.start).replace(
            tzinfo=timezone.utc)
        end = (datetime.fromisoformat(spec.date_range.end).replace(
            tzinfo=timezone.utc) + timedelta(days=1))
        now = _now()
        end = min(end, now - timedelta(seconds=30))
        if start >= end:
            return QueryResult(requested_field_ids=requested, rows=[], row_count=0)

        private = [f for f in requested if f in _PRIVATE_IDS]
        # Asked for the owner-only metrics, X silently drops every post past
        # their window, so the range is read in two parts: recent posts with
        # them, older posts without. Newest first, as X orders a timeline.
        if private:
            cutoff = now - _PRIVATE_METRICS_WINDOW
            spans = [(max(start, cutoff), end, True), (start, min(end, cutoff), False)]
            spans = [s for s in spans if s[0] < s[1]]
        else:
            spans = [(start, end, False)]

        base = _timeline_params(report_type, requested)
        path = _TIMELINES[report_type]
        multi = len(spec.accounts) > 1
        partial: List[str] = []

        def fetch(account):
            rows: List[Dict[str, Any]] = []
            for lo, hi, with_private in spans:
                remaining = spec.max_rows - len(rows)
                if remaining <= 0:
                    break
                params = dict(base, start_time=_iso(lo), end_time=_iso(hi))
                if with_private:
                    params["tweet.fields"] += ",non_public_metrics,organic_metrics"
                posts, includes = self._timeline(
                    f"users/{account}/{path}", params, remaining, partial)
                rows.extend(_post_row(p, includes, requested, account, multi=multi)
                            for p in posts)
            return rows

        rows, warnings = gather_accounts(spec.accounts, fetch)

        notes: List[str] = []
        if private and any(not with_private for _lo, _hi, with_private in spans):
            notes.append(
                "X keeps " + ", ".join(private) + " only for posts from the "
                "last 30 days; older posts in this range have them empty.")
        if report_type == "Posts":
            notes.append("Reposts are not included; replies and quotes are.")
        if partial:
            warnings.append(
                f"X left out part of the data ({len(partial)} item(s)): "
                f"{partial[0]}")
        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            notes=notes,
            warnings=warnings,
        )

    def _timeline(self, path, params, limit, partial):
        """Every post across pages (stopping at `limit`), plus the includes."""
        posts: List[Dict[str, Any]] = []
        includes: Dict[str, Dict[str, Any]] = {"users": {}, "media": {}}
        token = None
        while len(posts) < limit:
            page = dict(params, max_results=_PAGE_SIZE)
            if token:
                page["pagination_token"] = token
            data = self._call(path, page)
            posts.extend(data.get("data") or [])
            inc = data.get("includes") or {}
            includes["users"].update(
                (u.get("id"), u) for u in inc.get("users") or [])
            includes["media"].update(
                (m.get("media_key"), m) for m in inc.get("media") or [])
            partial.extend(
                str(e.get("detail") or e.get("title") or e)
                for e in data.get("errors") or [])
            token = (data.get("meta") or {}).get("next_token")
            if not token:
                break
        return posts[:limit], includes

    # -- write actions ------------------------------------------------------

    def list_actions(self) -> List[Action]:
        return list(_ACTIONS)

    def execute_action(
        self, action_id: str, account: str,
        params: Optional[Dict[str, Any]] = None, dry_run: bool = False,
    ) -> ActionResult:
        """Perform one write action as the connected account.

        Account authorisation happens upstream. X has no validate-only mode, so
        `dry_run=True` checks the inputs (and for a delete, reads the post to
        confirm it is the account's own) and stops before the change.
        """
        if action_id not in _ACTIONS_BY_ID:
            raise ApiError(
                ErrorCode.UNKNOWN_ACTION,
                f"Unknown action {action_id!r}. Call list_actions for the "
                f"available actions.",
                retriable=False,
                details={"action": action_id, "available": sorted(_ACTIONS_BY_ID)},
            )
        # A source connected before the write scopes were requested has a
        # read-only token: say so, rather than pass on X's 403.
        self.require_scope(*_ACTION_SCOPES[action_id])
        handler = getattr(self, f"_{action_id}")
        return handler(account, params or {}, bool(dry_run))

    def _create_post(self, account, params, dry_run) -> ActionResult:
        text = _require_text(params)
        if dry_run:
            return _would("create_post", account, f"Would post: {_clip(text)}")
        data = self._call("tweets", method="POST", body={"text": text}).get("data") or {}
        return ActionResult(
            action="create_post", account=account,
            summary=f"Posted {_post_url(data.get('id'))}.",
            after={"post_id": data.get("id"), "text": data.get("text"),
                   "url": _post_url(data.get("id"))})

    def _reply_to_post(self, account, params, dry_run) -> ActionResult:
        post_id = _require_id(params, "post_id")
        text = _require_text(params)
        if dry_run:
            return _would("reply_to_post", account,
                          f"Would reply to post {post_id}: {_clip(text)}")
        data = self._call("tweets", method="POST", body={
            "text": text, "reply": {"in_reply_to_tweet_id": post_id},
        }).get("data") or {}
        return ActionResult(
            action="reply_to_post", account=account,
            summary=f"Replied to post {post_id}: {_post_url(data.get('id'))}.",
            after={"post_id": data.get("id"), "in_reply_to": post_id,
                   "text": data.get("text"), "url": _post_url(data.get("id"))})

    def _delete_post(self, account, params, dry_run) -> ActionResult:
        post_id = _require_id(params, "post_id")
        post = self._call(f"tweets/{post_id}", {
            "tweet.fields": "author_id,created_at"}).get("data")
        if not post:
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"Post {post_id} was not found.", retriable=False,
                           details={"post_id": post_id})
        if str(post.get("author_id")) != str(account):
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           f"Post {post_id} is not this account's own; only "
                           f"its own posts can be deleted.", retriable=False,
                           details={"post_id": post_id})
        before = {"post_id": post_id, "text": post.get("text"),
                  "created_at": post.get("created_at")}
        if dry_run:
            return _would("delete_post", account,
                          f"Would delete post {post_id}: {_clip(post.get('text'))}",
                          before=before)
        self._call(f"tweets/{post_id}", method="DELETE")
        return ActionResult(
            action="delete_post", account=account,
            summary=f"Deleted post {post_id}.",
            before=before, after={"post_id": post_id, "deleted": True})

    def _like_post(self, account, params, dry_run) -> ActionResult:
        return self._set_like(account, params, dry_run, like=True)

    def _unlike_post(self, account, params, dry_run) -> ActionResult:
        return self._set_like(account, params, dry_run, like=False)

    def _set_like(self, account, params, dry_run, *, like) -> ActionResult:
        post_id = _require_id(params, "post_id")
        action, verb = ("like_post", "like") if like else ("unlike_post", "unlike")
        if dry_run:
            return _would(action, account, f"Would {verb} post {post_id}.")
        if like:
            self._call(f"users/{account}/likes", method="POST",
                       body={"tweet_id": post_id})
        else:
            self._call(f"users/{account}/likes/{post_id}", method="DELETE")
        return ActionResult(
            action=action, account=account,
            summary=f"{verb.capitalize()}d post {post_id}.",
            after={"post_id": post_id, "liked": like})

    def _hide_reply(self, account, params, dry_run) -> ActionResult:
        reply_id = _require_id(params, "reply_id")
        hidden = params.get("hidden", True)
        if not isinstance(hidden, bool):
            raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                           "'hidden' must be true or false.", retriable=False)
        verb, done = ("hide", "Hid") if hidden else ("unhide", "Unhid")
        if dry_run:
            return _would("hide_reply", account, f"Would {verb} reply {reply_id}.")
        self._call(f"tweets/{reply_id}/hidden", method="PUT", body={"hidden": hidden})
        return ActionResult(
            action="hide_reply", account=account,
            summary=f"{done} reply {reply_id}.",
            after={"reply_id": reply_id, "hidden": hidden})


def _timeline_params(report_type, requested) -> Dict[str, Any]:
    tweet_fields = ["created_at", "public_metrics", "referenced_tweets", "lang",
                    "conversation_id", "author_id"]
    params: Dict[str, Any] = {}
    expansions: List[str] = []
    if "video_views" in requested:
        tweet_fields.append("attachments")
        expansions.append("attachments.media_keys")
        params["media.fields"] = "type,public_metrics"
    if report_type == "Mentions":
        expansions.append("author_id")
        params["user.fields"] = "username,name,public_metrics"
    else:
        params["exclude"] = "retweets"
    params["tweet.fields"] = ",".join(tweet_fields)
    if expansions:
        params["expansions"] = ",".join(expansions)
    return params


def _post_type(post) -> str:
    kinds = {r.get("type") for r in post.get("referenced_tweets") or []}
    if "replied_to" in kinds:
        return "reply"
    if "quoted" in kinds:
        return "quote"
    if "retweeted" in kinds:
        return "repost"
    return "post"


def _post_row(post, includes, requested, account, *, multi=False) -> Dict[str, Any]:
    author = includes["users"].get(post.get("author_id")) or {}
    record: Dict[str, Any] = {}
    if multi:
        record["_account"] = account
    for name in requested:
        if name in _PUBLIC or name in _PRIVATE:
            group, key = _PUBLIC.get(name) or _PRIVATE[name]
            record[name] = _count(post.get(group), key)
        elif name == "post_id":
            record[name] = post.get("id")
        elif name == "date":
            record[name] = (post.get("created_at") or "")[:10] or None
        elif name == "post_type":
            record[name] = _post_type(post)
        elif name == "url":
            record[name] = _post_url(post.get("id"))
        elif name == "video_views":
            record[name] = _video_views(post, includes["media"])
        elif name == "author_username":
            record[name] = author.get("username")
        elif name == "author_name":
            record[name] = author.get("name")
        elif name == "author_followers":
            record[name] = (author.get("public_metrics") or {}).get("followers_count")
        else:
            record[name] = post.get(name)
    return record


def _count(metrics, key) -> Optional[int]:
    metrics = metrics or {}
    value = metrics.get(key)
    return metrics.get(_RENAMED[key]) if value is None and key in _RENAMED else value


def _video_views(post, media) -> Optional[int]:
    keys = (post.get("attachments") or {}).get("media_keys") or []
    views = [(media.get(k) or {}).get("public_metrics", {}).get("view_count")
             for k in keys]
    views = [v for v in views if v is not None]
    return sum(views) if views else None


def _profile_row(me, requested, account, *, multi=False) -> Dict[str, Any]:
    counts = me.get("public_metrics") or {}
    record: Dict[str, Any] = {}
    if multi:
        record["_account"] = account
    for name in requested:
        if name in _PROFILE_COUNTS:
            record[name] = _count(counts, _PROFILE_COUNTS[name])
        elif name == "user_id":
            record[name] = me.get("id")
        else:
            record[name] = me.get(name)
    return record


def _post_url(post_id) -> Optional[str]:
    return f"https://x.com/i/web/status/{post_id}" if post_id else None


def _require_id(params, key) -> str:
    value = params.get(key)
    if isinstance(value, int) and not isinstance(value, bool):
        value = str(value)
    if not isinstance(value, str) or not value.strip().isdigit():
        raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                       f"'{key}' must be a post id (digits only).",
                       retriable=False, details={key: value})
    return value.strip()


def _require_text(params) -> str:
    text = params.get("text")
    if not isinstance(text, str) or not text.strip():
        raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                       "'text' is required.", retriable=False)
    return text


def _clip(text, limit: int = 80) -> str:
    text = (text or "").replace("\n", " ")
    return text if len(text) <= limit else text[:limit - 1] + "…"


def _would(action, account, summary, *, before=None) -> ActionResult:
    """A dry run's result: what would happen, with nothing applied."""
    return ActionResult(action=action, account=account, summary=summary,
                        before=before, details={"dry_run": True, "applied": False})


def make_x_analytics_connector(datasource) -> XAnalyticsConnector:
    """Build an X Analytics connector wired to refresh its own OAuth token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return XAnalyticsConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["XAnalyticsConnector", "make_x_analytics_connector"]
