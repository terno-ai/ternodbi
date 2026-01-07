"""Django app configuration for DBI Layer."""

from django.apps import AppConfig


class DbiLayerConfig(AppConfig):
    """Django app config for dbi_layer."""

    default_auto_field = "django.db.models.BigAutoField"
    name = "dbi_layer.django_app"
    verbose_name = "DBI Layer"

    def ready(self):
        """Initialize app when Django starts."""
        # Import signals if any
        pass
