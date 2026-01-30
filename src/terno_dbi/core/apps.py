"""Django app configuration for Terno DBI."""

import logging
from django.apps import AppConfig

logger = logging.getLogger(__name__)


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
        logger.debug("TernoDBI app initialized: cache invalidation signals connected")

