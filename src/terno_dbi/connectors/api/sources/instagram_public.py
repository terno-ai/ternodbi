"""Instagram Public Data connector — public metrics of other accounts/hashtags.

Read-only over the Instagram Graph API. Even though the data is public, the call
is made *as* one of your own Instagram Business accounts (the `account`), which
is why discovery is the same as Instagram Insights.

Reports (each driven by a required setting, validated from the catalog):
  * "Profile"  — a public business/creator account's headline stats
                 (followers, media count, …) via Business Discovery. `username`.
  * "Media"    — that public account's recent posts with public engagement
                 (likes, comments) via Business Discovery. `username`.
  * "Hashtag"  — top public media for a hashtag: `GET /ig_hashtag_search` then
                 `GET /{hashtag-id}/top_media`. `hashtag`.

Read-only; there is nothing to write on public data.
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

# --- Business Discovery: account profile -----------------------------------

_PROFILE_FIELDS: List[Field] = [
    Field("username", "Username", "dimension", data_type="string"),
    Field("name", "Name", "dimension", data_type="string"),
    Field("followers_count", "Followers", "metric",
          "Public follower count.", data_type="integer"),
    Field("follows_count", "Following", "metric",
          "Accounts this account follows.", data_type="integer"),
    Field("media_count", "Posts", "metric", "Total public posts.",
          data_type="integer"),
    Field("biography", "Bio", "dimension", data_type="string"),
    Field("website", "Website", "dimension", data_type="string"),
]
# The account-level fields requested inside business_discovery(...) — the two
# text profile fields are always fetched so a row is never empty.
_PROFILE_API_FIELDS = ["username", "name", "followers_count",
                       "follows_count", "media_count", "biography", "website"]

# --- Media (public posts of a discovered account, or hashtag top media) -----

_MEDIA_FIELDS: List[Field] = [
    Field("id", "Media ID", "dimension", data_type="string"),
    Field("timestamp", "Published", "dimension", data_type="string"),
    Field("media_type", "Type", "dimension"),
    Field("caption", "Caption", "dimension", data_type="string"),
    Field("permalink", "Permalink", "dimension", data_type="string"),
    Field("like_count", "Likes", "metric", data_type="integer"),
    Field("comments_count", "Comments", "metric", data_type="integer"),
]
_MEDIA_API_FIELDS = ["id", "timestamp", "media_type", "caption",
                     "permalink", "like_count", "comments_count"]

_REPORTS: Dict[str, List[Field]] = {
    "Profile": list(_PROFILE_FIELDS),
    "Media": list(_MEDIA_FIELDS),
    "Hashtag": list(_MEDIA_FIELDS),
}
_DEFAULT_REPORT = "Profile"


def _fields_for(report_type: Optional[str]) -> Dict[str, Field]:
    fields = _REPORTS.get(report_type or _DEFAULT_REPORT, _REPORTS[_DEFAULT_REPORT])
    return {f.id: f for f in fields}


class InstagramPublicConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or ig.default_http

    # -- transport ----------------------------------------------------------

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
            logger.warning("Instagram Public request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Instagram returned an error. Try again.",
            )

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        # The querying account: public data is fetched *as* one of your own IG
        # Business accounts.
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

        if report_type == "Hashtag":
            return self._run_hashtag(spec, catalogue)
        return self._run_business_discovery(spec, report_type)

    # Business Discovery: profile stats and/or recent media of a public account.
    def _run_business_discovery(self, spec: QuerySpec, report_type) -> QueryResult:
        username = str(spec.settings.get("username") or "").strip().lstrip("@")
        if not username:
            raise missing_setting("username", "Username", report_type)

        want_media = report_type == "Media"
        requested = list(spec.fields) or (
            ["id", "timestamp", "like_count", "comments_count"] if want_media
            else ["username", "followers_count", "media_count"])

        inner = ",".join(_PROFILE_API_FIELDS)
        if want_media:
            media_fields = ",".join(_MEDIA_API_FIELDS)
            inner = f"{inner},media.limit({int(spec.max_rows)}){{{media_fields}}}"
        fields_param = f"business_discovery.username({username}){{{inner}}}"

        multi = len(spec.accounts) > 1

        def fetch(account):
            data = self._call("GET", f"{_BASE}/{account}",
                              {"fields": fields_param})
            bd = data.get("business_discovery") or {}
            if want_media:
                return _parse_media(
                    (bd.get("media") or {}).get("data", []), requested,
                    account, multi=multi)
            return [_profile_row(bd, requested, account, multi=multi)]

        rows, warnings = gather_accounts(spec.accounts, fetch)
        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )

    def _run_hashtag(self, spec: QuerySpec, catalogue) -> QueryResult:
        hashtag = str(spec.settings.get("hashtag") or "").strip().lstrip("#")
        if not hashtag:
            raise missing_setting("hashtag", "Hashtag", "Hashtag")
        requested = list(spec.fields) or ["id", "timestamp", "like_count",
                                          "comments_count", "permalink"]
        multi = len(spec.accounts) > 1

        def fetch(account):
            search = self._call(
                "GET", f"{_BASE}/ig_hashtag_search",
                {"user_id": account, "q": hashtag})
            results = search.get("data") or []
            if not results:
                return []
            hashtag_id = results[0].get("id")
            if not hashtag_id:
                return []
            data = self._call(
                "GET", f"{_BASE}/{hashtag_id}/top_media",
                {"user_id": account, "fields": ",".join(_MEDIA_API_FIELDS),
                 "limit": spec.max_rows})
            return _parse_media(data.get("data", []), requested, account,
                                multi=multi)

        rows, warnings = gather_accounts(spec.accounts, fetch)
        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            warnings=warnings,
        )


def _profile_row(bd, requested, account, *, multi=False) -> Dict[str, Any]:
    record: Dict[str, Any] = {}
    if multi:
        record["_account"] = account
    for name in requested:
        record[name] = bd.get(name)
    return record


def _parse_media(items, requested, account, *, multi=False):
    rows: List[Dict[str, Any]] = []
    for obj in items:
        record: Dict[str, Any] = {}
        if multi:
            record["_account"] = account
        for name in requested:
            record[name] = obj.get(name)
        rows.append(record)
    return rows


def make_instagram_public_connector(datasource) -> InstagramPublicConnector:
    """Build an Instagram Public connector that refreshes its token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return InstagramPublicConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["InstagramPublicConnector", "make_instagram_public_connector"]
