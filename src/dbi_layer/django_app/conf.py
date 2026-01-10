"""
Configuration module for DBI Layer.

Provides centralized, configurable settings with sensible defaults.
Consuming applications can override settings via Django's settings.py:

    # In settings.py
    DBI_LAYER = {
        "DEFAULT_PAGE_SIZE": 100,
        "MAX_PAGE_SIZE": 1000,
        "CACHE_TIMEOUT": 7200,
    }
"""

from django.conf import settings

DEFAULTS = {
    # Pagination
    "DEFAULT_PAGE_SIZE": 50,
    "MAX_PAGE_SIZE": 500,
    
    # Cache
    "CACHE_TIMEOUT": 3600,  # 1 hour in seconds
    "CACHE_PREFIX": "dbi_",
    
    # Connection pooling
    "DEFAULT_POOL_SIZE": 20,
    "DEFAULT_MAX_OVERFLOW": 30,
    "DEFAULT_POOL_TIMEOUT": 60,
    "DEFAULT_POOL_RECYCLE": 1800,
    
    # Query limits
    "MAX_QUERY_ROWS": 10000,
    "QUERY_TIMEOUT": 300,  # 5 minutes
    
    # Export limits
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


# =============================================================================
# Model Access Functions
# =============================================================================

def get_organisation_model():
    """
    Get the Organisation model configured by the consuming app.
    
    Configure in settings.py:
        DBI_ORGANISATION_MODEL = 'terno.Organisation'
    """
    from django.apps import apps
    model_path = getattr(settings, 'DBI_ORGANISATION_MODEL', None)
    if model_path:
        return apps.get_model(model_path)
    return None


def get_organisation_user_model():
    """
    Get the OrganisationUser model configured by the consuming app.
    
    Configure in settings.py:
        DBI_ORGANISATION_USER_MODEL = 'terno.OrganisationUser'
    """
    from django.apps import apps
    model_path = getattr(settings, 'DBI_ORGANISATION_USER_MODEL', None)
    if model_path:
        return apps.get_model(model_path)
    return None


def get_organisation_datasource_model():
    """
    Get the OrganisationDataSource model configured by the consuming app.
    
    Configure in settings.py:
        DBI_ORGANISATION_DATASOURCE_MODEL = 'terno.OrganisationDataSource'
    """
    from django.apps import apps
    model_path = getattr(settings, 'DBI_ORGANISATION_DATASOURCE_MODEL', None)
    if model_path:
        return apps.get_model(model_path)
    return None


def get_org_datasources(org_id):
    """
    Get datasource IDs accessible by an organization.
    
    Uses DBI_ORGANISATION_DATASOURCE_MODEL if configured,
    otherwise falls back to callback DBI_ORG_DATASOURCE_IDS_GETTER.
    """
    # Try model-based lookup first
    OrgDS = get_organisation_datasource_model()
    if OrgDS:
        return list(OrgDS.objects.filter(organisation_id=org_id).values_list('datasource_id', flat=True))
    
    # Fallback to callback
    getter = getattr(settings, 'DBI_ORG_DATASOURCE_IDS_GETTER', None)
    if getter and callable(getter):
        return getter(org_id)
    
    return []


def check_org_datasource_access(org_id, datasource_id):
    """
    Check if a datasource belongs to an organization.
    
    Uses DBI_ORGANISATION_DATASOURCE_MODEL if configured,
    otherwise falls back to callback DBI_ORG_DATASOURCE_CHECKER.
    """
    # Try model-based lookup first
    OrgDS = get_organisation_datasource_model()
    if OrgDS:
        return OrgDS.objects.filter(organisation_id=org_id, datasource_id=datasource_id).exists()
    
    # Fallback to callback
    checker = getattr(settings, 'DBI_ORG_DATASOURCE_CHECKER', None)
    if checker and callable(checker):
        return checker(org_id, datasource_id)
    
    return True  # Allow if no checker configured


def get_user_org(user):
    """
    Get the organization for a user (first org found).
    
    Uses DBI_ORGANISATION_USER_MODEL if configured.
    """
    OrgUser = get_organisation_user_model()
    if OrgUser and user:
        org_user = OrgUser.objects.filter(user=user).first()
        if org_user:
            return getattr(org_user, 'organisation_id', None)
    return None


def check_org_membership(user, org_id):
    """
    Verify if a user belongs to an organization.
    
    Uses DBI_ORGANISATION_USER_MODEL if configured (Option A pattern).
    This is the same verification TernoAI does with OrganisationUser.
    
    Args:
        user: Django User instance
        org_id: Organization ID to check membership for
        
    Returns:
        True if user belongs to org, False otherwise.
        If no OrganisationUser model is configured, returns True (permissive).
    """
    if not user or not org_id:
        return False
    
    OrgUser = get_organisation_user_model()
    if OrgUser:
        return OrgUser.objects.filter(user=user, organisation_id=org_id).exists()
    
    # No model configured = skip check (standalone mode without orgs)
    return True



def trigger_post_datasource_creation(datasource_id, org_id=None, user_id=None):
    """
    Trigger post-creation events for a datasource.
    
    This function implements a hybrid approach:
    1. Sends a Django signal (modern approach, supports multiple receivers)
    2. Calls a configured callback (legacy/simple approach, supports one handler)
    """
    # 1. Send Django Signal
    from dbi_layer.django_app.signals import datasource_created
    datasource_created.send(
        sender=None,
        datasource_id=datasource_id,
        org_id=org_id,
        user_id=user_id
    )
    
    # 2. Call configured callback (if any)
    callback_path = getattr(settings, 'DBI_POST_DATASOURCE_CREATION_CALLBACK', None)
    if callback_path:
        try:
            from django.utils.module_loading import import_string
            callback = import_string(callback_path)
            if callable(callback):
                callback(datasource_id=datasource_id, org_id=org_id, user_id=user_id)
        except Exception as e:
            # Log but don't fail the request if background task trigger fails
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to trigger post-creation callback: {e}")

