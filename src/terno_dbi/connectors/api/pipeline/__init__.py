"""Pipeline layer: everything that wraps a single query.

Authorisation, rate limiting, caching, token refresh and comparison compose
here around a connector's `query()`; async dispatch turns one query into a job.
Depends on `model` only.
"""

from terno_dbi.connectors.api.pipeline.cache import (
    CLOSED_RANGE_TTL,
    OPEN_RANGE_TTL,
    cache_key,
    get_cached,
    set_cached,
)
from terno_dbi.connectors.api.pipeline.compare import (
    add_comparison,
    resolve_compare_range,
)
from terno_dbi.connectors.api.pipeline.dispatch import run_query
from terno_dbi.connectors.api.pipeline.executor import (
    Executor,
    SynchronousExecutor,
    get_executor,
    set_executor,
)
from terno_dbi.connectors.api.pipeline.filters import (
    Condition,
    FilterAst,
    parse_filters,
)
from terno_dbi.connectors.api.pipeline.jobs import enqueue_query, get_query_results
from terno_dbi.connectors.api.pipeline.ratelimit import RateLimit, check_rate_limit

__all__ = [
    "CLOSED_RANGE_TTL",
    "Condition",
    "Executor",
    "FilterAst",
    "OPEN_RANGE_TTL",
    "RateLimit",
    "SynchronousExecutor",
    "add_comparison",
    "cache_key",
    "check_rate_limit",
    "enqueue_query",
    "get_cached",
    "get_executor",
    "get_query_results",
    "parse_filters",
    "resolve_compare_range",
    "run_query",
    "set_cached",
    "set_executor",
]
