import logging
from terno_dbi.core import models

logger = logging.getLogger(__name__)


def get_all_group_tables(datasource, roles):
    global_tables = models.Table.objects.filter(data_source=datasource, is_hidden=False)

    private_table_object = models.PrivateTableSelector.objects.filter(
        data_source=datasource).first()
    if private_table_object:
        private_tables_ids = private_table_object.tables.all().values_list('id', flat=True)
        global_tables = global_tables.exclude(id__in=private_tables_ids)

    group_tables_object = models.GroupTableSelector.objects.filter(
        group__in=roles,
        tables__data_source=datasource).first()
    if group_tables_object:
        group_tables = group_tables_object.tables.all()
        all_group_tables = global_tables.union(group_tables)
        all_group_tables = models.Table.objects.filter(
            id__in=all_group_tables.values('id'))
    else:
        all_group_tables = global_tables

    return all_group_tables


def get_all_group_columns(datasource, tables, roles):
    tables_ids = list(tables.values_list('id', flat=True))

    table_columns = models.TableColumn.objects.filter(table_id__in=tables_ids, is_hidden=False)

    private_columns_object = models.PrivateColumnSelector.objects.filter(
        data_source=datasource).first()
    if private_columns_object:
        private_columns_ids = private_columns_object.columns.all().values_list('id', flat=True)
        table_columns = table_columns.exclude(id__in=private_columns_ids)

    group_columns_object = models.GroupColumnSelector.objects.filter(
        group__in=roles,
        columns__table__in=tables).first()
    if group_columns_object:
        group_columns = group_columns_object.columns.all()
        all_table_columns = table_columns.union(group_columns)
        all_table_columns = models.TableColumn.objects.filter(
            id__in=all_table_columns.values('id'))
    else:
        all_table_columns = table_columns

    return all_table_columns


def get_admin_config_object(datasource, roles):
    all_group_tables = get_all_group_tables(datasource, roles)
    group_columns = get_all_group_columns(datasource, all_group_tables, roles)
    return all_group_tables, group_columns
