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

