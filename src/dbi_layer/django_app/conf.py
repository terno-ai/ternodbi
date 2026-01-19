from django.conf import settings

DEFAULTS = {
    "DEFAULT_PAGE_SIZE": 50,
    "MAX_PAGE_SIZE": 500,

    "CACHE_TIMEOUT": 3600,
    "CACHE_PREFIX": "dbi_",

    "DEFAULT_POOL_SIZE": 20,
    "DEFAULT_MAX_OVERFLOW": 30,
    "DEFAULT_POOL_TIMEOUT": 60,
    "DEFAULT_POOL_RECYCLE": 1800,

    "MAX_QUERY_ROWS": 10000,
    "QUERY_TIMEOUT": 300,

    "MAX_EXPORT_ROWS": 100000,
}


def get(key: str):
    """
    Get a DBI Layer configuration value.
    
    First checks Django settings.DBI_LAYER dict, then falls back to defaults.
    
    Args:
        key: Configuration key to retrieve
        
    Returns:
        Configuration value
        
    Example:
        from dbi_layer.django_app.conf import get
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
