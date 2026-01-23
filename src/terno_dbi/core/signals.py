from django.dispatch import Signal
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
import logging

datasource_created = Signal()

datasource_updated = Signal()

datasource_deleted = Signal()

query_executed = Signal()


logger = logging.getLogger(__name__)


def _invalidate_cache_for_datasource(datasource):
    from terno_dbi.services.shield import delete_cache
    delete_cache(datasource)


def connect_cache_invalidation_signals():
    from terno_dbi.core.models import Table, TableColumn

    def invalidate_cache_on_table_change(sender, instance, **kwargs):
        logger.debug(
            f"Table signal fired: {instance.name} (ID: {instance.id}) - "
            f"invalidating cache for datasource {instance.data_source_id}"
        )
        _invalidate_cache_for_datasource(instance.data_source)

    def invalidate_cache_on_column_change(sender, instance, **kwargs):
        try:
            logger.debug(
                f"Column signal fired: {instance.name} (ID: {instance.id}) - "
                f"invalidating cache for datasource {instance.table.data_source_id}"
            )
            _invalidate_cache_for_datasource(instance.table.data_source)
        except Exception as e:
            logger.debug(f"Skipping column cache invalidation (parent table likely deleted): {e}")

    post_save.connect(
        invalidate_cache_on_table_change, 
        sender=Table,
        dispatch_uid="terno_dbi_table_save_cache_invalidation"
    )
    post_delete.connect(
        invalidate_cache_on_table_change, 
        sender=Table,
        dispatch_uid="terno_dbi_table_delete_cache_invalidation"
    )

    post_save.connect(
        invalidate_cache_on_column_change, 
        sender=TableColumn,
        dispatch_uid="terno_dbi_column_save_cache_invalidation"
    )
    post_delete.connect(
        invalidate_cache_on_column_change, 
        sender=TableColumn,
        dispatch_uid="terno_dbi_column_delete_cache_invalidation"
    )

    logger.debug("Cache invalidation signals connected successfully")
