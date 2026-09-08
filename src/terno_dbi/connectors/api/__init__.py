"""API connectors: the marketing/analytics half of the catalog.

Layered into four subpackages, each buildable on the ones before it:

    model     value types, the ApiConnector contract, validation  (no deps)
    pipeline  query wrapping: authz, rate limit, cache, async      (-> model)
    auth      OAuth, token refresh, account allowlist              (-> model)
    sources   concrete provider connectors (GA4, …)     (-> model, pipeline, auth)

Plus three top-level modules that belong to no single layer: `registry`
(datasource -> connector), `dates` (get_today), and `web` (browser connect
routes).

Everything a caller commonly needs is re-exported flat here, so
`from terno_dbi.connectors.api import run_query` keeps working regardless of
which subpackage a symbol lives in.
"""

from terno_dbi.connectors.api.dates import get_today
from terno_dbi.connectors.api.model import (
    Account,
    ApiConnector,
    ApiError,
    Compare,
    DateRange,
    ErrorCode,
    Field,
    QueryResult,
    QuerySpec,
    invalid_field,
    invalid_setting,
    missing_setting,
    validate_settings,
)
from terno_dbi.connectors.api.pipeline import (
    Condition,
    Executor,
    FilterAst,
    RateLimit,
    SynchronousExecutor,
    add_comparison,
    cache_key,
    check_rate_limit,
    enqueue_query,
    get_cached,
    get_executor,
    get_query_results,
    parse_filters,
    resolve_compare_range,
    run_query,
    set_cached,
    set_executor,
)
from terno_dbi.connectors.api.auth import (
    ensure_fresh_token,
    token_needs_refresh,
)

__all__ = [
    "Account",
    "ApiConnector",
    "ApiError",
    "Compare",
    "Condition",
    "DateRange",
    "ErrorCode",
    "Executor",
    "Field",
    "FilterAst",
    "QueryResult",
    "QuerySpec",
    "RateLimit",
    "SynchronousExecutor",
    "add_comparison",
    "cache_key",
    "check_rate_limit",
    "ensure_fresh_token",
    "enqueue_query",
    "get_cached",
    "get_executor",
    "get_query_results",
    "get_today",
    "invalid_field",
    "invalid_setting",
    "missing_setting",
    "parse_filters",
    "resolve_compare_range",
    "run_query",
    "set_cached",
    "set_executor",
    "token_needs_refresh",
    "validate_settings",
]
