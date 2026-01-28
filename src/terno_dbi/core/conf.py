from django.conf import settings

DEFAULTS = {
    # Pagination
    "DEFAULT_PAGE_SIZE": 50,
    "MAX_PAGE_SIZE": 500,
    "DEFAULT_PAGINATION_MODE": "offset",
    "CURSOR_PAGINATION_ENABLED": True,
    "SKIP_TOTAL_COUNT_THRESHOLD": 100000,
    "STREAM_YIELD_SIZE": 1000,
    "COUNT_QUERY_TIMEOUT": 10,

    # Caching
    "CACHE_TIMEOUT": 3600,
    "CACHE_PREFIX": "dbi_",

    # Connection Pool
    "DEFAULT_POOL_SIZE": 20,
    "DEFAULT_MAX_OVERFLOW": 30,
    "DEFAULT_POOL_TIMEOUT": 60,
    "DEFAULT_POOL_RECYCLE": 1800,

    # Query Limits
    "MAX_QUERY_ROWS": 10000,
    "QUERY_TIMEOUT": 300,
    "MAX_EXPORT_ROWS": 100000,

    # Access Control
    "ALLOW_SUPERTOKEN": False,  # If True, tokens without org/DS links can access all
    "REQUIRE_TOKEN_SCOPE": True,  # If True, tokens must have org or DS links to access anything
}


def get(key: str):
    user_settings = getattr(settings, "DBI_LAYER", {})
    return user_settings.get(key, DEFAULTS.get(key))


def get_all():
    user_settings = getattr(settings, "DBI_LAYER", {})
    return {**DEFAULTS, **user_settings}
