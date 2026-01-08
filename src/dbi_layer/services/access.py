"""
Access control services for DBI Layer.
"""

import logging
from dbi_layer.django_app import models

logger = logging.getLogger(__name__)


def get_all_group_tables(datasource, roles):
    """
    Get all tables accessible by the given roles.
    
    Args:
        datasource: DataSource model instance
        roles: QuerySet of Group instances
        
    Returns:
        QuerySet of accessible Table instances
    """
    # Get all tables from datasource
    global_tables = models.Table.objects.filter(data_source=datasource)
    
    # Get private tables in datasource
    private_table_object = models.PrivateTableSelector.objects.filter(
        data_source=datasource).first()
    if private_table_object:
        private_tables_ids = private_table_object.tables.all().values_list('id', flat=True)
        # Get tables excluding private tables
        global_tables = global_tables.exclude(id__in=private_tables_ids)

    # Add tables accessible by the user's groups
    group_tables_object = models.GroupTableSelector.objects.filter(
        group__in=roles,
        tables__data_source=datasource).first()
    if group_tables_object:
        group_tables = group_tables_object.tables.all()
        all_group_tables = global_tables.union(group_tables)
        # Using subquery to allow filtering after union
        all_group_tables = models.Table.objects.filter(
            id__in=all_group_tables.values('id'))
    else:
        all_group_tables = global_tables
    
    return all_group_tables


def get_all_group_columns(datasource, tables, roles):
    """
    Get all columns accessible by the given roles.
    
    Args:
        datasource: DataSource model instance
        tables: QuerySet of Table instances
        roles: QuerySet of Group instances
        
    Returns:
        QuerySet of accessible TableColumn instances
    """
    tables_ids = list(tables.values_list('id', flat=True))
    
    # Get all columns for the tables
    table_columns = models.TableColumn.objects.filter(table_id__in=tables_ids)
    
    # Get private columns for tables
    private_columns_object = models.PrivateColumnSelector.objects.filter(
        data_source=datasource).first()
    if private_columns_object:
        private_columns_ids = private_columns_object.columns.all().values_list('id', flat=True)
        table_columns = table_columns.exclude(id__in=private_columns_ids)

    # Add columns accessible by the user's groups
    group_columns_object = models.GroupColumnSelector.objects.filter(
        group__in=roles,
        columns__table__in=tables).first()
    if group_columns_object:
        group_columns = group_columns_object.columns.all()
        all_table_columns = table_columns.union(group_columns)
        # Using subquery to allow filtering after union
        all_table_columns = models.TableColumn.objects.filter(
            id__in=all_table_columns.values('id'))
    else:
        all_table_columns = table_columns
    
    return all_table_columns


def get_admin_config_object(datasource, roles):
    """
    Get tables and columns accessible for a user.
    
    Args:
        datasource: DataSource model instance
        roles: QuerySet of Group instances
        
    Returns:
        Tuple of (accessible_tables, accessible_columns)
    """
    all_group_tables = get_all_group_tables(datasource, roles)
    group_columns = get_all_group_columns(datasource, all_group_tables, roles)
    return all_group_tables, group_columns
