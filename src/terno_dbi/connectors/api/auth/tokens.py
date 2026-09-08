"""Refresh API access tokens proactively and safely under concurrency.

Tokens are refreshed before expiry to avoid a 401 round-trip. Concurrent
requests for the same datasource are serialized so only one refresh runs;
some providers invalidate the previous refresh token when it is used.

The provider-specific refresh logic and encrypted token persistence are injected
as a callable. This module only handles when a refresh is needed and ensuring
it happens once.
"""

from __future__ import annotations
import logging
import time
from typing import Any, Callable, Dict, Optional
from django.core.cache import cache

logger = logging.getLogger(__name__)

# Refresh this many seconds before the token actually expires, so an in-flight
# query never carries one that dies mid-request.
DEFAULT_SKEW_SECONDS = 300


def _expires_at(tokens: Dict[str, Any]) -> Optional[float]:
    for key in ("TOKEN_EXPIRES_AT", "expires_at"):
        value = tokens.get(key)
        if value is not None:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
    return None


def token_needs_refresh(
    tokens: Dict[str, Any],
    skew: int = DEFAULT_SKEW_SECONDS,
    now: Optional[float] = None,
) -> bool:
    """True when the access token is missing, unexpiring-but-absent, or due.

    A token with no expiry recorded is treated as *not* needing refresh — some
    providers issue long-lived tokens — unless the access token itself is
    absent, which always needs one.
    """
    if not (tokens.get("ACCESS_TOKEN") or tokens.get("access_token")):
        return True
    expires_at = _expires_at(tokens)
    if expires_at is None:
        return False
    now = now if now is not None else time.time()
    return now >= (expires_at - skew)


def _lock_key(datasource_id) -> str:
    return f"api_token_refresh:{datasource_id}"


def ensure_fresh_token(
    datasource,
    refresh_fn: Callable[[], Dict[str, Any]],
    read_tokens: Callable[[], Dict[str, Any]],
    *,
    skew: int = DEFAULT_SKEW_SECONDS,
    lock_timeout: int = 30,
    wait_timeout: float = 5.0,
) -> Dict[str, Any]:
    """Return the current tokens, refreshing them once when needed.
    `read_tokens` returns the current bundle; `refresh_fn` performs the
    provider refresh, persists the result, and returns the new bundle.
    Both are injected so
    this function stays independent of OAuth and storage details.

    `lock_timeout` limits how long the refresh lock can be held if the
    refresher fails.
    `wait_timeout` limits how long other requests wait for the refresh;
    losers re-read the tokens after waiting instead of refreshing themselves.
    """
    tokens = read_tokens()
    if not token_needs_refresh(tokens, skew=skew):
        return tokens

    lock_key = _lock_key(datasource.id)
    got_lock = cache.add(lock_key, "1", timeout=lock_timeout)
    if not got_lock:
        # Someone else is refreshing. Wait briefly for them, then re-read.
        deadline = time.time() + wait_timeout
        while time.time() < deadline:
            time.sleep(0.05)
            if cache.get(lock_key) is None:
                break
        tokens = read_tokens()
        # If the winner already stored fresh tokens, take them.
        if not token_needs_refresh(tokens, skew=skew):
            return tokens
        # Otherwise the winner failed or is still going; try to take the lock
        # ourselves rather than hand back an expired token.
        got_lock = cache.add(lock_key, "1", timeout=lock_timeout)
        if not got_lock:
            return tokens

    try:
        return refresh_fn()
    finally:
        cache.delete(lock_key)


__all__ = [
    "DEFAULT_SKEW_SECONDS",
    "ensure_fresh_token",
    "token_needs_refresh",
]
