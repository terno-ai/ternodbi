import logging
from django.apps import AppConfig

logger = logging.getLogger(__name__)


class TernoDBIConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "terno_dbi.core"
    verbose_name = "Terno DBI"

    def ready(self):
        """Initialize app when Django starts."""
        from . import receivers
        logger.debug("TernoDBI app initialized: receivers imported")
