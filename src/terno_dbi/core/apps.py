"""Django app configuration for DBI Layer."""

from django.apps import AppConfig


class DbiLayerConfig(AppConfig):
    """Django app config for terno_dbi."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "terno_dbi.core"
    verbose_name = "DBI Layer"

    def ready(self):
        """Initialize app when Django starts."""
        # Connect cache invalidation signals using direct model references
        from terno_dbi.core.signals import connect_cache_invalidation_signals
        connect_cache_invalidation_signals()

