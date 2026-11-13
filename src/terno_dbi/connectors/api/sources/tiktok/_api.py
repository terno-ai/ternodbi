"""HTTP helpers shared by the TikTok API for Business connectors."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterator, Optional, Tuple
from datetime import date, timedelta

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode


_BASE = "https://business-api.tiktok.com/open_api/v1.3"


_PAGE_SIZE = 1000


# TikTok answers HTTP 200 with a non-zero `code` on failure.
_AUTH_CODES = frozenset({40105})                  # token incorrect or revoked


_RATE_LIMIT_CODES = frozenset({40016, 40100, 40133})


_TRANSIENT_CODES = frozenset({50000, 51001, 51002, 51004, 51041, 51043, 60001})


_NO_PERMISSION_CODE = 40001


# Key under which a CSV download body is returned in `data`.
_RAW_TEXT = "_raw_text"


def _default_http(method: str, url: str, token: str,
                  params: Optional[Dict] = None, *,
                  body: Optional[Dict] = None,
                  files: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    headers = {"Access-Token": token}
    if method == "GET":
        resp = requests.request(
            method, url, headers=headers, params=params or {}, timeout=30)
    elif files is not None:
        # Only file uploads are multipart; uploads by URL take JSON.
        parts = {k: (None, v if isinstance(v, str) else json.dumps(v))
                 for k, v in (body or {}).items()}
        resp = requests.request(
            method, url, headers=headers, files={**parts, **files},
            timeout=120)
    else:
        resp = requests.request(
            method, url, headers={**headers, "Content-Type": "application/json"},
            json=body or {}, timeout=60)
    if resp.status_code == 401:
        raise _AuthError()
    if resp.status_code >= 400:
        raise ApiError(ErrorCode.UPSTREAM_ERROR,
                       f"TikTok Ads error: HTTP {resp.status_code} "
                       f"{(resp.reason or '').strip()}".rstrip())
    try:
        return resp.json()
    except ValueError:
        # Async report downloads return raw CSV. Anywhere else a non-JSON
        # body means something in between failed (proxy, maintenance page).
        if url.rstrip("/").endswith("report/task/download"):
            return {"code": 0, "message": "OK",
                    "data": {_RAW_TEXT: resp.text}}
        raise ApiError(ErrorCode.UPSTREAM_ERROR,
                       "TikTok returned a non-JSON response. Try again.")


class _AuthError(Exception):
    """Raised for a revoked or invalid token."""


def _unwrap(envelope: Dict[str, Any], *, write: bool = False) -> Any:
    """Return `data` from a response, or raise the error it reports.

    For writes, 40001 means the app lacks a permission and any other 4xxxx
    code means bad input, so neither is worth retrying.
    """
    code = envelope.get("code")
    if code == 0:
        data = envelope.get("data")
        return {} if data is None else data
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
    if write and code == _NO_PERMISSION_CODE:
        raise ApiError(
            ErrorCode.ACCOUNT_FORBIDDEN,
            f"TikTok refused the change ({code}: {message}). The TikTok app "
            f"needs the matching permission in the TikTok API for Business "
            f"portal (Ads, Creative, Audience, Comment, Automated Rules or "
            f"Pixel Management) and the connected user an Admin or Operator "
            f"role on this advertiser; reconnect after changing the app.",
            retriable=False, details={"code": code})
    if write and isinstance(code, int) and 40000 <= code < 50000:
        raise ApiError(
            ErrorCode.INVALID_ACTION_PARAMS,
            f"TikTok refused the change ({code}): {message}",
            retriable=False, details={"code": code})
    raise ApiError(ErrorCode.UPSTREAM_ERROR,
                   f"TikTok Ads error {code}: {message}", retriable=False)


def _windows(start: str, end: str, days: int) -> Iterator[Tuple[str, str]]:
    """Split an inclusive date range into windows of at most `days` days."""
    lo, hi = date.fromisoformat(start), date.fromisoformat(end)
    while lo <= hi:
        upper = min(hi, lo + timedelta(days=days - 1))
        yield lo.isoformat(), upper.isoformat()
        lo = upper + timedelta(days=1)
