"""Cache API query results using Django's cache.

Closed date ranges can be cached longer because their metrics should not change.
Ranges that include today are cached briefly since the data is still changing.
Caching also reduces latency and provider quota usage when agents repeat queries.

The cache key includes every value that affects the result, including the
organisation ID. Accounts and fields are sorted before hashing so different
orders produce the same key, while results are never shared across tenants.
"""

from __future__ import annotations
import hashlib
import json
from datetime import date
from typing import Any, Dict, Optional
from django.core.cache import cache
from terno_dbi.connectors.api.model.types import QuerySpec

# Closed ranges are immutable; cache them for a day.
CLOSED_RANGE_TTL = 1 * 3600
# Ranges touching today are still moving; a short TTL keeps them fresh.
OPEN_RANGE_TTL = 15 * 60


def _settings_hash(settings: Dict[str, Any]) -> str:
    if not settings:
        return "0"
    blob = json.dumps(settings, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def cache_key(source_key: str, org_id, spec: QuerySpec) -> str:
    """A key capturing every input that changes the result.

    Accounts and fields are sorted so column/account order does not fork the
    key. Filters and compare are included verbatim — they change the answer.
    """
    parts = [
        source_key,
        str(org_id),
        ",".join(sorted(spec.accounts)),
        ",".join(sorted(spec.fields)),
        spec.report_type or "-",
        _settings_hash(spec.settings),
        f"{spec.date_range.start}:{spec.date_range.end}",
        spec.filters or "-",
        json.dumps(spec.compare.as_dict(), sort_keys=True) if spec.compare else "-",
        spec.timezone,
        str(spec.max_rows),
    ]
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()
    return f"api_q:{source_key}:{org_id}:{digest}"


def _is_closed(spec: QuerySpec, today: Optional[str] = None) -> bool:
    """True when the range ends strictly before today, so it can never change.

    `today` should be resolved in the source's timezone by the caller; it
    defaults to UTC today, which is a safe over-approximation — treating a range
    as open when it is actually closed only shortens the TTL.
    """
    today = today or date.today().isoformat()
    if spec.date_range.inclusive_of_today:
        return False
    return spec.date_range.end < today


def get_cached(source_key: str, org_id, spec: QuerySpec) -> Optional[Dict[str, Any]]:
    return cache.get(cache_key(source_key, org_id, spec))


def set_cached(
    source_key: str,
    org_id,
    spec: QuerySpec,
    payload: Dict[str, Any],
    today: Optional[str] = None,
) -> int:
    """Store a result, choosing the TTL from whether the range is closed.

    Returns the TTL used, so callers and tests can see which policy applied.
    """
    ttl = CLOSED_RANGE_TTL if _is_closed(spec, today) else OPEN_RANGE_TTL
    cache.set(cache_key(source_key, org_id, spec), payload, timeout=ttl)
    return ttl


__all__ = [
    "CLOSED_RANGE_TTL",
    "OPEN_RANGE_TTL",
    "cache_key",
    "get_cached",
    "set_cached",
]
