"""Django app configuration for Terno DBI."""

from django.apps import AppConfig


class TernoDBIConfig(AppConfig):
    """Django app config for terno_dbi.core."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "terno_dbi.core"
    verbose_name = "Terno DBI"

    def ready(self):
        """Initialize app when Django starts."""
        # Connect cache invalidation signals using direct model references
        from terno_dbi.core.signals import connect_cache_invalidation_signals
        connect_cache_invalidation_signals()

