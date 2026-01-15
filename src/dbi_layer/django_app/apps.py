"""Django app configuration for DBI Layer."""

from django.apps import AppConfig


class DbiLayerConfig(AppConfig):
    """Django app config for dbi_layer."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "dbi_layer.django_app"
    verbose_name = "DBI Layer"

    def ready(self):
        """Initialize app when Django starts."""
        # Connect cache invalidation signals using direct model references
        from dbi_layer.django_app.signals import connect_cache_invalidation_signals
        connect_cache_invalidation_signals()

