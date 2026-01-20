from django.conf import settings

DEFAULTS = {
    # Pagination
    "DEFAULT_PAGE_SIZE": 50,
    "MAX_PAGE_SIZE": 500,
    "DEFAULT_PAGINATION_MODE": "offset",  # offset, cursor, or stream
    "CURSOR_PAGINATION_ENABLED": True,
    "SKIP_TOTAL_COUNT_THRESHOLD": 100000,  # Skip COUNT(*) for large tables
    "STREAM_YIELD_SIZE": 1000,  # Rows per yield for streaming
    "COUNT_QUERY_TIMEOUT": 10,  # Seconds before COUNT query times out

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
}


def get(key: str):
    """
    Get a Terno DBI configuration value.
    
    First checks Django settings.DBI_LAYER dict, then falls back to defaults.
    
    Args:
        key: Configuration key to retrieve
        
    Returns:
        Configuration value
        
    Example:
        from terno_dbi.core.conf import get
        page_size = get("DEFAULT_PAGE_SIZE")  # Returns 50 or overridden value
    """
    user_settings = getattr(settings, "DBI_LAYER", {})
    return user_settings.get(key, DEFAULTS.get(key))


def get_all():
    """
    Get all configuration values (defaults merged with user settings).

    Returns:
        Dict of all configuration key-value pairs
    """
    user_settings = getattr(settings, "DBI_LAYER", {})
    return {**DEFAULTS, **user_settings}
