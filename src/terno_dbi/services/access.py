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

    #  Get explicit tables granted to any of the user's groups in ONE query
    group_tables = models.Table.objects.filter(
        include_tables__group__in=roles,
        data_source=datasource
    )

    # Bitwise OR (|) safely constructs a single unified SQL query
    all_group_tables = (global_tables | group_tables).distinct()

    return all_group_tables


def get_all_group_columns(datasource, tables, roles):
    tables_ids = list(tables.values_list('id', flat=True))

    table_columns = models.TableColumn.objects.filter(table_id__in=tables_ids, is_hidden=False)

    private_columns_object = models.PrivateColumnSelector.objects.filter(
        data_source=datasource).first()
    if private_columns_object:
        private_columns_ids = private_columns_object.columns.all().values_list('id', flat=True)
        table_columns = table_columns.exclude(id__in=private_columns_ids)

    group_columns = models.TableColumn.objects.filter(
        include_columns__group__in=roles,
        table__in=tables
    )

    all_table_columns = (table_columns | group_columns).distinct()

    return all_table_columns


def get_admin_config_object(datasource, roles):
    all_group_tables = get_all_group_tables(datasource, roles)
    group_columns = get_all_group_columns(datasource, all_group_tables, roles)
    return all_group_tables, group_columns
