"""Facebook Pages connector — read-only over the Meta Graph API.

A minimal, honest Facebook Pages reader. The user connects with Facebook Login
and Terno reads the Pages they manage and the content on those Pages:

Reports:
  * "Pages"        — the Facebook Pages this credential manages, with headline
                     metadata (category, followers, fan count, link).
                     Uses `pages_show_list`.
  * "Posts"        — a Page's own recently published posts.
                     Uses `pages_read_engagement`.
  * "PostComments" — comments left by users on a given Page post (`post_id`).
                     Uses `pages_read_user_content`.
  * "Ratings"      — recommendations / reviews left by users on the Page.
                     Uses `pages_read_user_content`.

The `visitor_posts` edge (posts made by other people on the Page's timeline) is
intentionally not exposed: Meta deprecated it for the New Pages Experience, which
is now the default, so it returns a permissions error for most Pages.

Page-scoped reads require a *Page* access token, not the user token. Those
tokens come from `GET /me/accounts` (the same call that lists the Pages), so a
single discovery call yields both the account list and the per-Page tokens.

Read-only: this connector exposes no write actions.
"""

from __future__ import annotations
import logging
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import (
    ApiError, ErrorCode, invalid_field, missing_setting,
)
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec
from terno_dbi.connectors.api.sources import _instagram as ig
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

_BASE = ig.BASE


def _fb_http(method: str, url: str, token: str,
             params: Optional[Dict] = None) -> Dict[str, Any]:
    """Graph API transport that surfaces Facebook's own error message.

    Mirrors the Instagram transport but labels errors as Facebook so warnings
    read correctly for this connector.
    """
    import requests

    resp = requests.request(
        method, url, headers={"Authorization": f"Bearer {token}"},
        params=params or {}, timeout=30)
    if resp.status_code == 401:
        raise ig.AuthError()
    if resp.status_code >= 400:
        detail = ""
        try:
            err = resp.json().get("error", {}) or {}
            detail = err.get("message") or ""
            if err.get("error_user_msg"):
                detail = f"{detail} {err['error_user_msg']}".strip()
        except Exception:   # noqa: BLE001
            detail = (resp.text or "")[:300]
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"Facebook API error: {detail}" if detail
            else f"Facebook API error (HTTP {resp.status_code}).",
            details={"status": resp.status_code})
    return resp.json() if resp.content else {"success": True}


# --- Pages (the managed-Page list) -----------------------------------------

_PAGE_FIELDS: List[Field] = [
    Field("id", "Page ID", "dimension", data_type="string"),
    Field("name", "Name", "dimension", data_type="string"),
    Field("category", "Category", "dimension", data_type="string"),
    Field("followers_count", "Followers", "metric", data_type="integer"),
    Field("fan_count", "Likes", "metric",
          "People who like the Page.", data_type="integer"),
    Field("link", "Link", "dimension", data_type="string"),
    Field("about", "About", "dimension", data_type="string"),
]
_PAGE_API_FIELDS = ["id", "name", "category", "followers_count",
                    "fan_count", "link", "about"]

# --- Posts (the Page's own published posts) --------------------------------

_POST_FIELDS: List[Field] = [
    Field("id", "Post ID", "dimension", data_type="string"),
    Field("created_time", "Published", "dimension", data_type="string"),
    Field("message", "Message", "dimension", data_type="string"),
    Field("permalink_url", "Permalink", "dimension", data_type="string"),
    Field("status_type", "Type", "dimension", data_type="string"),
]
_POST_API_FIELDS = ["id", "created_time", "message", "permalink_url",
                    "status_type"]

# --- Comments on a Page post ------------------------------------------------

_COMMENT_FIELDS: List[Field] = [
    Field("id", "Comment ID", "dimension", data_type="string"),
    Field("created_time", "Commented", "dimension", data_type="string"),
    Field("from_name", "Author", "dimension", data_type="string"),
    Field("message", "Comment", "dimension", data_type="string"),
    Field("like_count", "Likes", "metric", data_type="integer"),
]
_COMMENT_API_FIELDS = ["id", "created_time", "from", "message", "like_count"]

# --- Ratings / recommendations ---------------------------------------------

_RATING_FIELDS: List[Field] = [
    Field("created_time", "Reviewed", "dimension", data_type="string"),
    Field("recommendation_type", "Recommendation", "dimension",
          data_type="string"),
    Field("review_text", "Review", "dimension", data_type="string"),
    Field("reviewer_name", "Reviewer", "dimension", data_type="string"),
]
_RATING_API_FIELDS = ["created_time", "recommendation_type", "review_text",
                      "reviewer"]

_REPORTS: Dict[str, List[Field]] = {
    "Pages": list(_PAGE_FIELDS),
    "Posts": list(_POST_FIELDS),
    "PostComments": list(_COMMENT_FIELDS),
    "Ratings": list(_RATING_FIELDS),
}
_DEFAULT_REPORT = "Pages"


def _fields_for(report_type: Optional[str]) -> Dict[str, Field]:
    fields = _REPORTS.get(report_type or _DEFAULT_REPORT, _REPORTS[_DEFAULT_REPORT])
    return {f.id: f for f in fields}


class FacebookPagesConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _fb_http
        self._page_cache: Optional[Dict[str, Dict[str, Any]]] = None

    # -- transport ----------------------------------------------------------

    def _call(self, method: str, url: str,
              params: Optional[Dict] = None,
              token: Optional[str] = None) -> Dict[str, Any]:
        try:
            return self._http(method, url, token or self.access_token(), params)
        except ig.AuthError:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} access was rejected; reconnect the source.",
            )
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("Facebook Pages request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Facebook returned an error. Try again.",
            )

    # -- discovery ----------------------------------------------------------

    def _pages(self) -> Dict[str, Dict[str, Any]]:
        """Managed Pages keyed by id, each with name, metadata and Page token.

        A single `GET /me/accounts` yields the account list *and* the per-Page
        access tokens that the Page-scoped reads need.
        """
        if self._page_cache is not None:
            return self._page_cache
        fields = ",".join(_PAGE_API_FIELDS + ["access_token"])
        data = self._call("GET", f"{_BASE}/me/accounts",
                          {"fields": fields, "limit": 200})
        cache: Dict[str, Dict[str, Any]] = {}
        for page in data.get("data", []):
            pid = page.get("id")
            if not pid:
                continue
            cache[str(pid)] = page
        self._page_cache = cache
        return cache

    def _page_token(self, page_id: str) -> Optional[str]:
        return (self._pages().get(str(page_id)) or {}).get("access_token")

    def list_accounts(self) -> List[Account]:
        return [
            Account(id=str(pid), name=str(page.get("name") or pid))
            for pid, page in self._pages().items()
        ]

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(_fields_for(report_type).values())

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = (spec.report_type if spec.report_type in _REPORTS
                       else _DEFAULT_REPORT)
        catalogue = _fields_for(report_type)

        unknown = [f for f in spec.fields if f not in catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(catalogue.keys()))

        if report_type == "Pages":
            return self._run_pages(spec)
        if report_type == "PostComments":
            return self._run_post_comments(spec)
        return self._run_page_edge(spec, report_type)

    def _run_pages(self, spec: QuerySpec) -> QueryResult:
        requested = list(spec.fields) or ["id", "name", "category",
                                          "followers_count", "fan_count"]
        pages = self._pages()
        selected = set(str(a) for a in spec.accounts) if spec.accounts else None
        rows: List[Dict[str, Any]] = []
        for pid, page in pages.items():
            if selected is not None and pid not in selected:
                continue
            rows.append({name: page.get(name) for name in requested})
        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            warnings=[],
        )

    # Posts / VisitorPosts / Ratings: one edge per Page, using its Page token.
    def _run_page_edge(self, spec: QuerySpec, report_type: str) -> QueryResult:
        edge, api_fields, default_fields = {
            "Posts": ("posts", _POST_API_FIELDS,
                      ["id", "created_time", "message", "permalink_url"]),
            "Ratings": ("ratings", _RATING_API_FIELDS,
                        ["created_time", "recommendation_type", "review_text",
                         "reviewer_name"]),
        }[report_type]
        requested = list(spec.fields) or default_fields
        multi = len(spec.accounts) > 1

        def fetch(account):
            token = self._page_token(account)
            data = self._call(
                "GET", f"{_BASE}/{account}/{edge}",
                {"fields": ",".join(api_fields), "limit": spec.max_rows},
                token=token)
            return _parse_rows(data.get("data", []), requested, account,
                               multi=multi)

        rows, warnings = gather_accounts([str(a) for a in spec.accounts], fetch)
        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )

    def _run_post_comments(self, spec: QuerySpec) -> QueryResult:
        post_id = str(spec.settings.get("post_id") or "").strip()
        if not post_id:
            raise missing_setting("post_id", "Post id", "PostComments")
        requested = list(spec.fields) or ["id", "created_time", "from_name",
                                          "message", "like_count"]
        multi = len(spec.accounts) > 1

        def fetch(account):
            token = self._page_token(account)
            data = self._call(
                "GET", f"{_BASE}/{post_id}/comments",
                {"fields": ",".join(_COMMENT_API_FIELDS),
                 "limit": spec.max_rows},
                token=token)
            return _parse_rows(data.get("data", []), requested, account,
                               multi=multi)

        rows, warnings = gather_accounts([str(a) for a in spec.accounts], fetch)
        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )


def _parse_rows(items, requested, account, *, multi=False):
    """Flatten Graph objects into rows, folding nested `from`/`reviewer`.

    Facebook returns the author of a visitor post, comment or review as a nested
    object; the connector exposes it as a flat `*_name` string.
    """
    rows: List[Dict[str, Any]] = []
    for obj in items:
        record: Dict[str, Any] = {}
        if multi:
            record["_account"] = account
        for name in requested:
            if name == "from_name":
                record[name] = (obj.get("from") or {}).get("name")
            elif name == "reviewer_name":
                record[name] = (obj.get("reviewer") or {}).get("name")
            else:
                record[name] = obj.get(name)
        rows.append(record)
    return rows


def make_facebook_pages_connector(datasource) -> FacebookPagesConnector:
    """Build a Facebook Pages connector that refreshes its token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return FacebookPagesConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["FacebookPagesConnector", "make_facebook_pages_connector"]
