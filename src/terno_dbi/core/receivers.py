import logging
from django.db.models.signals import post_save, post_delete, pre_delete, m2m_changed
from django.dispatch import receiver
from terno_dbi.core import models

logger = logging.getLogger(__name__)


def _invalidate_cache_for_datasource(datasource):
    if not datasource:
        return
    try:
        from terno_dbi.services.shield import delete_cache
        delete_cache(datasource)
    except Exception as e:
        logger.error(f"Failed to invalidate cache for datasource {datasource}: {e}")


@receiver(post_save, sender=models.TableRowFilter)
@receiver(post_save, sender=models.GroupTableRowFilter)
@receiver(post_save, sender=models.GroupColumnSelector)
@receiver(post_save, sender=models.PrivateColumnSelector)
@receiver(post_save, sender=models.GroupTableSelector)
@receiver(post_save, sender=models.PrivateTableSelector)
@receiver(post_save, sender=models.ForeignKey)
@receiver(post_save, sender=models.TableColumn)
@receiver(post_save, sender=models.Table)
@receiver(post_delete, sender=models.TableRowFilter)
@receiver(post_delete, sender=models.GroupTableRowFilter)
@receiver(post_delete, sender=models.PrivateColumnSelector)
@receiver(post_delete, sender=models.PrivateTableSelector)
@receiver(post_delete, sender=models.ForeignKey)
@receiver(post_delete, sender=models.TableColumn)
@receiver(post_delete, sender=models.Table)
@receiver(pre_delete, sender=models.GroupColumnSelector)
@receiver(pre_delete, sender=models.GroupTableSelector)
def invalidate_datasource_cache(sender, instance, **kwargs):
    """
    Invalidates the shield cache (metadata MDB object) whenever any
    metadata definition, table, column, or access policy changes.
    """
    created = kwargs.get('created', False)
    is_delete = kwargs.get('signal') in (post_delete, pre_delete)

    data_sources = set()

    if sender is models.Table:
        if instance.data_source:
            data_sources.add(instance.data_source)

    elif sender is models.TableColumn:
        if hasattr(instance, 'table') and instance.table:
            data_sources.add(instance.table.data_source)

    elif sender is models.ForeignKey:
        if hasattr(instance, 'constrained_table') and instance.constrained_table:
            data_sources.add(instance.constrained_table.data_source)

    elif sender in (models.PrivateTableSelector, models.PrivateColumnSelector, 
                    models.GroupTableRowFilter, models.TableRowFilter):
        if hasattr(instance, 'data_source') and instance.data_source:
            data_sources.add(instance.data_source)

    elif sender is models.GroupTableSelector:
        # We hook this to pre_delete so we can still read M2M relations before they are removed
        try:
            for table in instance.tables.all():
                if table.data_source:
                    data_sources.add(table.data_source)
            for table in instance.exclude_tables.all():
                if table.data_source:
                    data_sources.add(table.data_source)
        except Exception:
            pass

    elif sender is models.GroupColumnSelector:
        try:
            for column in instance.columns.select_related('table').all():
                if column.table and column.table.data_source:
                    data_sources.add(column.table.data_source)
            for column in instance.exclude_columns.select_related('table').all():
                if column.table and column.table.data_source:
                    data_sources.add(column.table.data_source)
        except Exception:
            pass

    # Skip invalidation for bulk creation of schemas to prevent performance degradation.
    if sender in [models.Table, models.TableColumn, models.ForeignKey] and created and not is_delete:
        return

    for ds in data_sources:
        logger.debug(f"Signal fired on {sender.__name__} for DS: {ds.id}. Invalidating cache.")
        _invalidate_cache_for_datasource(ds)


# Helpers for ManyToMany field changes (which don't trigger post_save on the parent model)
def _get_instance_datasource_id(instance):
    """Safely extract the data_source_id from an instance (Selector, Table, or TableColumn)."""
    if getattr(instance, 'data_source_id', None):
        return instance.data_source_id
    if hasattr(instance, 'table') and getattr(instance.table, 'data_source_id', None):
        return instance.table.data_source_id
    return None


def _get_m2m_target_datasource_ids(sender, pk_set):
    """Fetch data_source_ids for the target tables/columns being modified in the M2M relation."""
    if sender in (models.GroupTableSelector.tables.through, models.GroupTableSelector.exclude_tables.through):
        return models.Table.objects.filter(id__in=pk_set).values_list('data_source_id', flat=True)
    if sender in (models.GroupColumnSelector.columns.through, models.GroupColumnSelector.exclude_columns.through):
        return models.TableColumn.objects.filter(id__in=pk_set).values_list('table__data_source_id', flat=True)
    return []


def _get_pre_clear_datasource_ids(sender, instance):
    """Fetch data_source_ids for all currently attached targets before a clear operation."""
    try:
        if sender == models.GroupTableSelector.tables.through:
            return instance.tables.values_list('data_source_id', flat=True)
        if sender == models.GroupTableSelector.exclude_tables.through:
            return instance.exclude_tables.values_list('data_source_id', flat=True)
        if sender == models.GroupColumnSelector.columns.through:
            return instance.columns.values_list('table__data_source_id', flat=True)
        if sender == models.GroupColumnSelector.exclude_columns.through:
            return instance.exclude_columns.values_list('table__data_source_id', flat=True)
    except Exception as e:
        logger.error(f"Error extracting datasources during pre_clear: {e}")
    return []


@receiver(m2m_changed, sender=models.PrivateTableSelector.tables.through)
@receiver(m2m_changed, sender=models.PrivateColumnSelector.columns.through)
@receiver(m2m_changed, sender=models.GroupTableSelector.tables.through)
@receiver(m2m_changed, sender=models.GroupTableSelector.exclude_tables.through)
@receiver(m2m_changed, sender=models.GroupColumnSelector.columns.through)
@receiver(m2m_changed, sender=models.GroupColumnSelector.exclude_columns.through)
def invalidate_cache_on_m2m_change(sender, instance, action, reverse, pk_set, **kwargs):
    if action not in ['post_add', 'post_remove', 'pre_clear', 'post_clear']:
        return

    logger.debug(f"M2M signal {action} fired on {sender.__name__}. reverse: {reverse}")

    ds_ids = set()

    # Pre-Clear: Gather the soon-to-be-deleted IDs
    if action == 'pre_clear':
        if reverse:
            ds_id = _get_instance_datasource_id(instance)
            if ds_id: ds_ids.add(ds_id)
        else:
            ds_id = _get_instance_datasource_id(instance)
            if ds_id:
                ds_ids.add(ds_id)
            else:
                ds_ids.update(_get_pre_clear_datasource_ids(sender, instance))

        instance._m2m_clear_ds_ids = list(ds_ids)
        return

    # Post-Clear: Execute using collected IDs
    if action == 'post_clear':
        ds_ids = set(getattr(instance, '_m2m_clear_ds_ids', []))

    # 3. Add/Remove: Resolve from instance or pk_set
    elif action in ['post_add', 'post_remove']:
        if not pk_set:
            return

        if reverse:
            ds_id = _get_instance_datasource_id(instance)
            if ds_id: ds_ids.add(ds_id)
        else:
            ds_id = _get_instance_datasource_id(instance)
            if ds_id:
                ds_ids.add(ds_id)
            else:
                ds_ids.update(_get_m2m_target_datasource_ids(sender, pk_set))

    # Invalidate Target Caches
    for ds_id in ds_ids:
        if ds_id:
            _invalidate_cache_for_datasource(ds_id)
