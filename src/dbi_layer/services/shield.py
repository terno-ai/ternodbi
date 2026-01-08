"""
SQLShield integration for DBI Layer.

Functions for building MDatabase, generating native SQL, and applying filters.
"""

import logging
from django.core.cache import cache
from sqlshield.shield import Session
from sqlshield.models import MDatabase

from dbi_layer.django_app import models

logger = logging.getLogger(__name__)


def generate_mdb(datasource):
    """
    Generate SQLShield MDatabase from datasource metadata.
    
    Args:
        datasource: DataSource model instance
        
    Returns:
        MDatabase instance
    """
    tables = {}
    dbtables = models.Table.objects.filter(data_source=datasource)
    columns = {}
    
    for dbt in dbtables:
        tables[dbt.name] = {
            'name': dbt.name,
            'public_name': dbt.public_name,
            'description': dbt.description
        }
        dbcolumns = models.TableColumn.objects.filter(table=dbt)
        column_data = []
        for dbc in dbcolumns:
            column_data.append({
                'name': dbc.name,
                'pub_name': dbc.public_name,
                'type': dbc.data_type,
                'primary_key': '',
                'nullable': '',
                'desc': ''
            })
        columns[dbt.name] = column_data

    foreign_keys = {}
    for dbt in dbtables:
        dbfks = models.ForeignKey.objects.filter(constrained_table=dbt)
        fk_data = []
        for dbfk in dbfks:
            fk_data.append({
                'constrained_columns': [dbfk.constrained_columns.name],
                'referred_table': dbfk.referred_table.name,
                'referred_columns': [dbfk.referred_columns.name],
                'referred_schema': '',
            })
        foreign_keys[dbt.name] = fk_data

    mdb = MDatabase.from_data(tables, columns, foreign_keys)
    return mdb


def generate_native_sql(mDb, user_sql, dialect):
    """
    Generate native SQL from user SQL using SQLShield.
    
    Args:
        mDb: MDatabase instance
        user_sql: SQL with public names
        dialect: Target SQL dialect
        
    Returns:
        Dict with 'status' and 'native_sql' or 'error'
    """
    sess = Session(mDb, '')
    try:
        native_sql = sess.generateNativeSQL(user_sql, dialect)
        return {
            'status': 'success',
            'native_sql': native_sql
        }
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e)
        }



def get_cache_key(datasource_id, role_ids):
    """
    Generate a standardized cache key for datasource roles.
    
    Args:
        datasource_id: ID of the DataSource
        role_ids: List or iterable of role/group IDs
        
    Returns:
        String cache key
    """
    ids = sorted(role_ids)
    return f"dbi_datasource_{datasource_id}_roles_{'_'.join(map(str, ids))}"


def prepare_mdb(datasource, roles):
    """
    Prepare MDatabase with access control filters.
    
    Args:
        datasource: DataSource model instance
        roles: QuerySet of user groups/roles
        
    Returns:
        MDatabase instance with filters applied
    """
    from dbi_layer.services.access import (
        get_admin_config_object, get_all_group_tables, get_all_group_columns
    )
    
    role_ids = sorted(roles.values_list('id', flat=True))
    cache_key = get_cache_key(datasource.id, role_ids)
    cached_mdb = cache.get(cache_key)

    if cached_mdb is not None:
        return cached_mdb

    allowed_tables, allowed_columns = get_admin_config_object(datasource, roles)

    mDb = generate_mdb(datasource)
    mDb.keep_only_tables(allowed_tables.values_list('name', flat=True))
    _keep_only_columns(mDb, allowed_tables, allowed_columns)

    tables = mDb.get_table_dict()
    _update_table_descriptions(tables)
    _update_filters(tables, datasource, roles)

    cache.set(cache_key, mDb, timeout=3600)

    return mDb


def _keep_only_columns(mDb, tables, columns):
    """Filter MDatabase to only include allowed columns."""
    for _, table in mDb.tables.items():
        table_obj = tables.filter(name=table.name)
        if table_obj:
            table.pub_name = table_obj.first().public_name
            keep_columns = columns.filter(table__name=table.name).values_list('name', flat=True)
            table_columns = models.TableColumn.objects.filter(
                table__in=table_obj).values_list('name', flat=True)
            drop_columns = set(table_columns).difference(keep_columns)
            table.drop_columns(drop_columns)
            for _, col in table.columns.items():
                allowed_column = columns.filter(table=table_obj.first(), name=col.name)
                if allowed_column:
                    col.pub_name = allowed_column.first().public_name


def _update_table_descriptions(tables):
    """Update table descriptions from database."""
    for tbl_name, tbl_object in tables.items():
        table_obj = models.Table.objects.filter(name=tbl_name).first()
        if table_obj:
            tbl_object.desc = table_obj.description


def _update_filters(tables, datasource, roles):
    """Apply row-level filters to tables."""
    tbl_base_filters = _get_base_filters(datasource)
    tbls_grp_filter = _get_grp_filters(datasource, roles)
    _merge_grp_filters(tbl_base_filters, tbls_grp_filter)
    for tbl, filters_list in tbl_base_filters.items():
        if len(filters_list) > 0:
            tables[tbl].filters = 'WHERE ' + ' AND '.join(filters_list)


def _get_base_filters(datasource):
    """Get base row filters for datasource."""
    tbl_base_filters = {}
    for trf in models.TableRowFilter.objects.filter(data_source=datasource):
        filter_str = trf.filter_str.strip()
        if len(filter_str) > 0:
            tbl_base_filters[trf.table.name] = ["(" + filter_str + ")"]
    return tbl_base_filters


def _get_grp_filters(datasource, roles):
    """Get group-specific row filters."""
    tbls_grp_filter = {}
    for gtrf in models.GroupTableRowFilter.objects.filter(data_source=datasource, group__in=roles):
        filter_str = gtrf.filter_str.strip()
        if len(filter_str) > 0:
            tbl_name = gtrf.table.name
            lst = []
            if tbl_name not in tbls_grp_filter:
                tbls_grp_filter[tbl_name] = lst
            else:
                lst = tbls_grp_filter[tbl_name]
            lst.append("(" + filter_str + ")")
    return tbls_grp_filter


def _merge_grp_filters(tbl_base_filters, tbls_grp_filter):
    """Merge group filters with base filters."""
    for tbl, grp_filters in tbls_grp_filter.items():
        role_filter_str = " ( " + ' OR '.join(grp_filters) + " ) "
        all_filters = []
        if tbl in tbl_base_filters:
            all_filters = tbl_base_filters[tbl]
        else:
            tbl_base_filters[tbl] = all_filters
        all_filters.append(role_filter_str)


def delete_cache(datasource):
    """
    Clear cache for a datasource.
    
    Note: Since TernoDBI doesn't manage Organisations/Users, it cannot iterate 
    through all role combinations to clear specific cache keys. 
    The consuming application should handle cache invalidation using 
    `get_cache_key` or by clearing the entire cache namespace if supported.
    """
    try:
        # TernoAI logic was: iterate OrgUsers -> Get Roles -> Build Keys -> Delete
        # TernoDBI logic: We can't see OrgUsers. 
        # Ideally, we would rely on a cache tag or prefix clearing, but standard Django cache 
        # doesn't strictly support that efficiently without specific backends.
        logger.warning(
            f"delete_cache called for datasource {datasource.id}. "
            "TernoDBI cannot clear specific role-based caches without Organisation context. "
            "Please handle cache invalidation in the application layer."
        )
    except Exception as e:
        logger.warning(f"Could not clear cache: {e}")

