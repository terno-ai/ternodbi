"""TikTok Ads connector (TikTok API for Business v1.3).

Things that differ from other ad APIs:

* The token goes in an `Access-Token` header and does not expire; there is no
  refresh token.
* Every response is HTTP 200 with `{code, message, data}`; a non-zero `code`
  is the error.
* List parameters (`dimensions`, `metrics`, `fields`) are JSON strings in the
  query string.
* A report groups by one ID column, chosen by `data_level`. Names and other
  entity properties are requested as metrics ("attribute metrics").
* Daily reports cover at most 30 days per request and hourly ones 1 day, so
  longer ranges are fetched in windows.
* Money is in the account currency, not micros. Values come back as strings,
  and "-" means not applicable.
* There is no validate-only mode: a dry run checks input and reads the
  entities involved, then stops.

Permissions come from the app settings in the TikTok developer portal, not
from OAuth scopes. Writes need Ads, Creative, Audience, Comment, Automated
Rules or Pixel Management, depending on the action.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from terno_dbi.connectors.api.model.types import Account, Field
from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.sources.tiktok.ads.reports import ReportsMixin
from terno_dbi.connectors.api.sources.tiktok.ads.smart_plus import (
    SmartPlusMixin,
)
from terno_dbi.connectors.api.sources.tiktok.ads.writes import WritesMixin
from terno_dbi.connectors.api.sources.tiktok._api import (
    _AuthError,
    _BASE,
    _PAGE_SIZE,
    _default_http,
    _unwrap,
)
from terno_dbi.connectors.api.sources.tiktok.ads.fields import _fields_for


logger = logging.getLogger(__name__)

# advertiser/info/ accepts at most 100 ids per call.
_INFO_BATCH = 100


class TikTokAdsConnector(SmartPlusMixin, ReportsMixin, WritesMixin,
                         ApiConnector):

    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http
        self._sleep = time.sleep

    def _call(self, path: str, params: Optional[Dict] = None, *,
              method: str = "GET", body: Optional[Dict] = None,
              files: Optional[Dict] = None,
              write: Optional[bool] = None) -> Any:
        url = f"{_BASE}/{path}"
        try:
            if method == "GET":
                envelope = self._http("GET", url, self.access_token(), params)
            else:
                kwargs: Dict[str, Any] = {"body": body or {}}
                if files is not None:
                    kwargs["files"] = files
                envelope = self._http(method, url, self.access_token(), None,
                                      **kwargs)
            return _unwrap(envelope, write=(method != "GET") if write is None
                           else write)
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

    def _post(self, path: str, body: Dict[str, Any], *,
              files: Optional[Dict] = None) -> Any:
        return self._call(path, method="POST", body=body, files=files)

    def _pages(self, path: str, params: Dict[str, Any], limit: int, *,
               page_size: int = _PAGE_SIZE,
               list_keys: Tuple[str, ...] = ("list",)) -> List[Dict[str, Any]]:
        """Collect items across pages, up to `limit`."""
        items: List[Dict[str, Any]] = []
        page = 1
        while len(items) < limit:
            data = self._call(path, {**params, "page": page,
                                     "page_size": page_size})
            if not isinstance(data, dict):
                break
            key = next((k for k in list_keys if k in data), list_keys[0])
            items.extend(data.get(key) or [])
            total_pages = (data.get("page_info") or {}).get("total_page") or 1
            if page >= total_pages:
                break
            page += 1
        return items[:limit]

    def list_accounts(self) -> List[Account]:
        # This endpoint needs the app's id and secret, not just the token.
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
        """Name, currency and timezone per advertiser; failures are ignored."""
        info: Dict[str, Dict[str, Any]] = {}
        for i in range(0, len(ids), _INFO_BATCH):
            try:
                data = self._call("advertiser/info/", {
                    "advertiser_ids": json.dumps(ids[i:i + _INFO_BATCH]),
                    "fields": json.dumps(["advertiser_id", "name", "currency",
                                          "timezone", "display_timezone"]),
                })
            except ApiError as exc:
                # Only labels depend on this; the ids still work.
                logger.warning("TikTok advertiser/info failed: %s", exc.message)
                continue
            for adv in data.get("list") or []:
                if adv.get("advertiser_id"):
                    info[str(adv["advertiser_id"])] = adv
        return info

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(_fields_for(report_type).values())


def make_tiktok_ads_connector(datasource) -> TikTokAdsConnector:
    """Build the connector for a data source.

    The token does not expire, so the refresher only flags the source for
    reconnection if the token is missing.
    """
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return TikTokAdsConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["TikTokAdsConnector", "make_tiktok_ads_connector"]
