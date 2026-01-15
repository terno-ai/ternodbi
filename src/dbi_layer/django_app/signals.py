"""
Django signals for DBI Layer.

These signals allow external apps (like TernoAI) to react to events
in TernoDBI without tight coupling. This enables:
1. Multiple receivers for the same event
2. Third-party extensions
3. Clean separation between TernoDBI and consuming apps

Usage (in your app's receivers.py):
    from django.dispatch import receiver
    from dbi_layer.django_app.signals import datasource_created
    
    @receiver(datasource_created)
    def handle_datasource_created(sender, datasource_id, org_id, **kwargs):
        # Your logic here (e.g., trigger metadata generation)
        pass
"""

from django.dispatch import Signal
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver


# =============================================================================
# Datasource Signals
# =============================================================================

# Sent after a new datasource is created
# Provides: datasource_id (int), org_id (int or None), user_id (int or None)
datasource_created = Signal()

# Sent after a datasource is updated
# Provides: datasource_id (int), org_id (int or None)
datasource_updated = Signal()

# Sent after a datasource is deleted
# Provides: datasource_id (int), org_id (int or None)
datasource_deleted = Signal()


# =============================================================================
# Query Signals
# =============================================================================

# Sent after a SQL query is executed (for audit logging)
# Provides:
#   sender: None
#   datasource: DataSource instance
#   user: User instance (or None if token auth)
#   user_sql: str (original SQL entered by user)
#   native_sql: str (actual executed SQL after group transformation)
#   status: str ('success' or 'error')
#   error: str (optional error message)
query_executed = Signal()


# =============================================================================
# Cache Invalidation Signal Receivers
# =============================================================================

import logging

logger = logging.getLogger(__name__)


def _invalidate_cache_for_datasource(datasource):
    """Helper to invalidate cache for a datasource."""
    from dbi_layer.services.shield import delete_cache
    delete_cache(datasource)


def connect_cache_invalidation_signals():
    """
    Connect cache invalidation signals to models.
    
    Called from apps.py ready() after models are loaded.
    Uses direct model class references instead of string references
    to avoid app label resolution issues.
    
    Uses dispatch_uid to prevent duplicate connections if ready() is called
    multiple times (e.g., during testing or hot reload).
    """
    from dbi_layer.django_app.models import Table, TableColumn
    
    def invalidate_cache_on_table_change(sender, instance, **kwargs):
        """Invalidate cache when a Table is created, updated, or deleted."""
        logger.debug(
            f"Table signal fired: {instance.name} (ID: {instance.id}) - "
            f"invalidating cache for datasource {instance.data_source_id}"
        )
        _invalidate_cache_for_datasource(instance.data_source)
    
    def invalidate_cache_on_column_change(sender, instance, **kwargs):
        """Invalidate cache when a TableColumn is created, updated, or deleted."""
        logger.debug(
            f"Column signal fired: {instance.name} (ID: {instance.id}) - "
            f"invalidating cache for datasource {instance.table.data_source_id}"
        )
        _invalidate_cache_for_datasource(instance.table.data_source)
    
    # Connect Table signals with dispatch_uid to prevent duplicates
    post_save.connect(
        invalidate_cache_on_table_change, 
        sender=Table,
        dispatch_uid="dbi_layer_table_save_cache_invalidation"
    )
    post_delete.connect(
        invalidate_cache_on_table_change, 
        sender=Table,
        dispatch_uid="dbi_layer_table_delete_cache_invalidation"
    )
    
    # Connect TableColumn signals with dispatch_uid to prevent duplicates
    post_save.connect(
        invalidate_cache_on_column_change, 
        sender=TableColumn,
        dispatch_uid="dbi_layer_column_save_cache_invalidation"
    )
    post_delete.connect(
        invalidate_cache_on_column_change, 
        sender=TableColumn,
        dispatch_uid="dbi_layer_column_delete_cache_invalidation"
    )
    
    logger.debug("Cache invalidation signals connected successfully")
