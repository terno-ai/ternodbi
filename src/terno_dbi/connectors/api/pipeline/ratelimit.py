"""Rate-limit API queries per source and organisation using Django's cache.

Each connector has a short burst limit and a daily limit. Requests that exceed
either limit fail fast with a clear error instead of silently queueing or
consuming provider quota.

The limiter uses the same cache backend as `services/shield.py`: LocMemCache in
tests and Redis in production. Counters are best-effort; with a non-atomic
backend, a small race may allow an extra request through, which is acceptable
for preventing retry storms rather than exact usage metering.
"""

from __future__ import annotations
import time
from dataclasses import dataclass
from typing import Optional
from django.core.cache import cache
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode


@dataclass(frozen=True)
class RateLimit:
    """A connector's declared limits. Absent windows are not enforced."""

    per_second: Optional[int] = None
    per_day: Optional[int] = None


def _bucket_key(source_key: str, org_id, window: str, slot) -> str:
    return f"api_rl:{source_key}:{org_id}:{window}:{slot}"


def _incr(key: str, ttl: int) -> int:
    """Increment a counter that starts at 1 and expires after `ttl`.

    `cache.add` seeds the key only if absent (so the TTL is set once, at the
    start of the window); `cache.incr` then bumps it. If the key expired between
    the two calls, `incr` raises and we re-seed — a rare race that at worst
    resets the window early.
    """
    if cache.add(key, 1, timeout=ttl):
        return 1
    try:
        return cache.incr(key)
    except ValueError:
        cache.add(key, 1, timeout=ttl)
        return 1


def _now() -> float:
    # Wrapped so tests can freeze it without patching the stdlib globally.
    return time.time()


def check_rate_limit(source_key: str, org_id, limit: RateLimit) -> None:
    """Raise `ApiError(RATE_LIMITED / QUOTA_EXCEEDED)` if a window is exhausted.

    Called once per query, before any provider request or cache lookup — a
    rate-limited call should not even consult the cache, so a storm cannot be
    disguised as cheap.
    """
    now = _now()

    if limit.per_second is not None:
        slot = int(now)
        count = _incr(_bucket_key(source_key, org_id, "s", slot), ttl=2)
        if count > limit.per_second:
            raise ApiError(
                ErrorCode.RATE_LIMITED,
                f"Too many {source_key} requests this second "
                f"({limit.per_second}/s). Retry shortly.",
                retry_after_seconds=1,
            )

    if limit.per_day is not None:
        slot = time.strftime("%Y%m%d", time.gmtime(now))
        # Seconds until the next UTC midnight, so the counter's TTL matches the
        # window it guards rather than a rolling 24h.
        secs_into_day = int(now) % 86400
        ttl = 86400 - secs_into_day
        count = _incr(_bucket_key(source_key, org_id, "d", slot), ttl=ttl)
        if count > limit.per_day:
            raise ApiError(
                ErrorCode.QUOTA_EXCEEDED,
                f"Daily {source_key} quota for this organisation is exhausted "
                f"({limit.per_day}/day). Resets at 00:00 UTC.",
                retry_after_seconds=ttl,
            )


__all__ = ["RateLimit", "check_rate_limit"]
