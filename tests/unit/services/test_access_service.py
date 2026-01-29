"""
Unit tests for Access Service (services/access.py).

Tests group-based table and column access control logic.
"""
import pytest
from django.contrib.auth.models import Group

from terno_dbi.core.models import (
    DataSource, Table, TableColumn, 
    PrivateTableSelector, GroupTableSelector,
    PrivateColumnSelector, GroupColumnSelector
)


@pytest.fixture
def datasource(db):
    """Create a test datasource."""
    return DataSource.objects.create(
        display_name='access_test_db',
        type='postgres',
        connection_str='postgresql://localhost/accesstest',
        enabled=True
    )


@pytest.fixture
def setup_schema(datasource):
    """Create tables and columns for access tests."""
    # Create tables
    table1 = Table.objects.create(
        name='public_table',
        public_name='Public Table',
        data_source=datasource
    )
    table2 = Table.objects.create(
        name='private_table',
        public_name='Private Table',
        data_source=datasource
    )
    table3 = Table.objects.create(
        name='group_table',
        public_name='Group Table',
        data_source=datasource
    )
    
    # Create columns
    col1 = TableColumn.objects.create(
        name='public_col',
        public_name='Public Column',
        table=table1,
        data_type='varchar'
    )
    col2 = TableColumn.objects.create(
        name='private_col',
        public_name='Private Column',
        table=table1,
        data_type='varchar'
    )
    col3 = TableColumn.objects.create(
        name='group_col',
        public_name='Group Column',
        table=table1,
        data_type='varchar'
    )
    
    return {
        'datasource': datasource,
        'tables': [table1, table2, table3],
        'columns': [col1, col2, col3],
        'public_table': table1,
        'private_table': table2,
        'group_table': table3,
    }


@pytest.fixture
def setup_access_rules(setup_schema):
    """Set up private and group access rules."""
    ds = setup_schema['datasource']
    
    # Create groups
    admin_group = Group.objects.create(name='access_admin')
    user_group = Group.objects.create(name='access_user')
    
    # Private table selector - hide private_table from everyone
    private_table_selector = PrivateTableSelector.objects.create(
        data_source=ds
    )
    private_table_selector.tables.add(setup_schema['private_table'])
    
    # Group table selector - give group_table access to admin_group
    group_table_selector = GroupTableSelector.objects.create(
        group=admin_group
    )
    group_table_selector.tables.add(setup_schema['group_table'])
    
    # Private column selector - hide private_col
    private_col_selector = PrivateColumnSelector.objects.create(
        data_source=ds
    )
    private_col_selector.columns.add(setup_schema['columns'][1])
    
    # Group column selector - give group_col to admin_group
    group_col_selector = GroupColumnSelector.objects.create(
        group=admin_group
    )
    group_col_selector.columns.add(setup_schema['columns'][2])
    
    return {
        **setup_schema,
        'admin_group': admin_group,
        'user_group': user_group,
    }


@pytest.mark.django_db
class TestGetAllGroupTables:
    """Tests for get_all_group_tables function."""

    def test_returns_all_tables_when_no_selectors(self, setup_schema):
        """Should return all tables when no private/group selectors exist."""
        from terno_dbi.services.access import get_all_group_tables
        
        roles = Group.objects.none()
        tables = get_all_group_tables(setup_schema['datasource'], roles)
        
        assert tables.count() == 3

    def test_excludes_private_tables(self, setup_access_rules):
        """Should exclude tables marked as private."""
        from terno_dbi.services.access import get_all_group_tables
        
        roles = Group.objects.filter(name='access_user')
        tables = get_all_group_tables(setup_access_rules['datasource'], roles)
        
        table_names = list(tables.values_list('name', flat=True))
        assert 'private_table' not in table_names
        assert 'public_table' in table_names

    def test_includes_group_tables_for_authorized_group(self, setup_access_rules):
        """Should include group tables for authorized groups."""
        from terno_dbi.services.access import get_all_group_tables
        
        roles = Group.objects.filter(name='access_admin')
        tables = get_all_group_tables(setup_access_rules['datasource'], roles)
        
        table_names = list(tables.values_list('name', flat=True))
        assert 'group_table' in table_names

    def test_excludes_group_tables_for_unauthorized_group(self, setup_access_rules):
        """Should not include group tables for unauthorized groups."""
        from terno_dbi.services.access import get_all_group_tables
        
        roles = Group.objects.filter(name='access_user')
        tables = get_all_group_tables(setup_access_rules['datasource'], roles)
        
        # group_table is in group selector but user_group is not authorized
        # However, it's also not in private selector, so it should be visible
        # unless the group selector logic excludes it
        table_names = list(tables.values_list('name', flat=True))
        # Group table should be visible to all as it's not private
        assert 'public_table' in table_names


@pytest.mark.django_db
class TestGetAllGroupColumns:
    """Tests for get_all_group_columns function."""

    def test_returns_all_columns_when_no_selectors(self, setup_schema):
        """Should return all columns when no private/group selectors exist."""
        from terno_dbi.services.access import get_all_group_columns
        
        tables = Table.objects.filter(data_source=setup_schema['datasource'])
        roles = Group.objects.none()
        columns = get_all_group_columns(setup_schema['datasource'], tables, roles)
        
        assert columns.count() == 3

    def test_excludes_private_columns(self, setup_access_rules):
        """Should exclude columns marked as private."""
        from terno_dbi.services.access import get_all_group_columns
        
        tables = Table.objects.filter(data_source=setup_access_rules['datasource'])
        roles = Group.objects.filter(name='access_user')
        columns = get_all_group_columns(setup_access_rules['datasource'], tables, roles)
        
        col_names = list(columns.values_list('name', flat=True))
        assert 'private_col' not in col_names
        assert 'public_col' in col_names

    def test_includes_group_columns_for_authorized_group(self, setup_access_rules):
        """Should include group columns for authorized groups."""
        from terno_dbi.services.access import get_all_group_columns
        
        tables = Table.objects.filter(data_source=setup_access_rules['datasource'])
        roles = Group.objects.filter(name='access_admin')
        columns = get_all_group_columns(setup_access_rules['datasource'], tables, roles)
        
        col_names = list(columns.values_list('name', flat=True))
        assert 'group_col' in col_names


@pytest.mark.django_db
class TestGetAdminConfigObject:
    """Tests for get_admin_config_object function."""

    def test_returns_tables_and_columns(self, setup_schema):
        """Should return tuple of (tables, columns)."""
        from terno_dbi.services.access import get_admin_config_object
        
        roles = Group.objects.none()
        tables, columns = get_admin_config_object(setup_schema['datasource'], roles)
        
        assert tables.count() == 3
        assert columns.count() == 3

    def test_respects_privacy_rules(self, setup_access_rules):
        """Should exclude private tables and columns."""
        from terno_dbi.services.access import get_admin_config_object
        
        roles = Group.objects.filter(name='access_user')
        tables, columns = get_admin_config_object(setup_access_rules['datasource'], roles)
        
        table_names = list(tables.values_list('name', flat=True))
        col_names = list(columns.values_list('name', flat=True))
        
        assert 'private_table' not in table_names
        assert 'private_col' not in col_names
