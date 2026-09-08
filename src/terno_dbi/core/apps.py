import logging
from django.apps import AppConfig
from django.db.models.signals import post_migrate

logger = logging.getLogger(__name__)


def _refresh_catalog(sender, **kwargs):
    """Keep `ConnectorCatalog` in step with the declarations after a migrate.

    Errors are logged, not raised: a catalog that is briefly stale is a far
    smaller problem than a deploy that fails to complete.
    """
    from django.db import connection
    from terno_dbi.catalog.refresh import refresh_catalog
    from terno_dbi.core.models import ConnectorCatalog

    # `post_migrate` can also run when migrating backwards past the migration that
    # created this table, so the table being absent is expected. Check for it
    # explicitly so real failures below are not hidden by a broad exception handler.
    table = ConnectorCatalog._meta.db_table
    if table not in connection.introspection.table_names():
        logger.debug("Skipping catalog refresh: %s does not exist", table)
        return

    try:
        refresh_catalog()
    except Exception:
        logger.exception(
            "Connector catalog refresh failed; the catalog may be stale until "
            "the next deploy."
        )


class TernoDBIConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "terno_dbi.core"
    verbose_name = "Terno DBI"

    def ready(self):
        """Initialize app when Django starts."""
        from . import receivers
        post_migrate.connect(_refresh_catalog, sender=self)
        _register_api_connectors()
        logger.debug("TernoDBI app initialized: receivers imported")


def _register_api_connectors():
    """Register the concrete API connectors with the dispatch registry.

    Done at startup so the tool layer can build a connector for a datasource
    without importing any provider SDK itself.
    """
    from terno_dbi.connectors.api import registry
    from terno_dbi.connectors.api.sources.ga4 import make_ga4_connector

    registry.register("googleanalytics4", make_ga4_connector)
