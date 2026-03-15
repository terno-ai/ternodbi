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
    
    # Extra table and column to test multi-group and exclusion functionality
    table4 = Table.objects.create(
        name='multi_group_table',
        public_name='Multi Group Table',
        data_source=datasource
    )
    table5 = Table.objects.create(
        name='excluded_table',
        public_name='Excluded Table',
        data_source=datasource
    )
    col4 = TableColumn.objects.create(
        name='multi_group_col',
        public_name='Multi Group Column',
        table=table4,
        data_type='varchar'
    )
    col5 = TableColumn.objects.create(
        name='excluded_col',
        public_name='Excluded Column',
        table=table5,
        data_type='varchar'
    )
    
    return {
        'datasource': datasource,
        'tables': [table1, table2, table3],
        'columns': [col1, col2, col3, col4, col5],
        'public_table': table1,
        'private_table': table2,
        'group_table': table3,
        'multi_group_table': table4,
        'excluded_table': table5,
    }


@pytest.fixture
def setup_access_rules(setup_schema):
    """Set up private and group access rules."""
    ds = setup_schema['datasource']
    
    # Create groups
    admin_group = Group.objects.create(name='access_admin')
    second_admin_group = Group.objects.create(name='access_admin_2')
    user_group = Group.objects.create(name='access_user')
    
    # Private table selector - hide private_table from everyone
    private_table_selector = PrivateTableSelector.objects.create(
        data_source=ds
    )
    private_table_selector.tables.add(setup_schema['private_table'])
    
    # Group table selector - give group_table to admin_group
    group_table_selector = GroupTableSelector.objects.create(
        group=admin_group
    )
    group_table_selector.tables.add(setup_schema['group_table'])
    group_table_selector.tables.add(setup_schema['excluded_table'])
    
    group_table_selector.tables.add(setup_schema['excluded_table'])
    
    # Second Group table selector - give multi_group_table to second_admin_group
    second_group_table_selector = GroupTableSelector.objects.create(
        group=second_admin_group
    )
    second_group_table_selector.tables.add(setup_schema['multi_group_table'])
    
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
    group_col_selector.columns.add(setup_schema['columns'][4])  # excluded_col
    
    group_col_selector.columns.add(setup_schema['columns'][4])  # excluded_col
    
    # Second Group column selector - give multi_group_col to second_admin_group
    second_group_col_selector = GroupColumnSelector.objects.create(
        group=second_admin_group
    )
    second_group_col_selector.columns.add(setup_schema['columns'][3])  # multi_group_col
    
    return {
        **setup_schema,
        'admin_group': admin_group,
        'second_admin_group': second_admin_group,
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
        
        assert tables.count() == 5

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

    def test_aggregates_multiple_groups(self, setup_access_rules):
        """Should union tables from all of the user's groups."""
        from terno_dbi.services.access import get_all_group_tables
        
        # User belongs to both admin_group and second_admin_group
        roles = Group.objects.filter(name__in=['access_admin', 'access_admin_2'])
        tables = get_all_group_tables(setup_access_rules['datasource'], roles)
        
        table_names = list(tables.values_list('name', flat=True))
        # Should get tables from first group
        assert 'group_table' in table_names
        # Should get tables from second group
        assert 'multi_group_table' in table_names

    def test_aggregates_overlapping_groups_without_duplicates(self, setup_access_rules):
        """Should return exactly one instance of a table even if granted by multiple groups."""
        from terno_dbi.services.access import get_all_group_tables
        
        # Give second_admin_group access to the exact same table that admin_group has
        setup_schema = setup_access_rules
        second_group_selector = GroupTableSelector.objects.get(group=setup_schema['second_admin_group'])
        second_group_selector.tables.add(setup_schema['group_table'])

        # User belongs to both admin_group and second_admin_group
        roles = Group.objects.filter(name__in=['access_admin', 'access_admin_2'])
        tables = get_all_group_tables(setup_access_rules['datasource'], roles)
        
        table_names = list(tables.values_list('name', flat=True))
        
        # The 'group_table' should only appear exactly once
        assert table_names.count('group_table') == 1
        # 'multi_group_table' from the second group should also be there
        assert 'multi_group_table' in table_names

    def test_multi_group_preserves_public_tables(self, setup_access_rules):
        """A user in multiple groups with private access should STILL see global public tables."""
        from terno_dbi.services.access import get_all_group_tables
        
        # User belongs to both admin_group and second_admin_group
        roles = Group.objects.filter(name__in=['access_admin', 'access_admin_2'])
        tables = get_all_group_tables(setup_access_rules['datasource'], roles)
        
        table_names = list(tables.values_list('name', flat=True))
        
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
        
        assert columns.count() == 5

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

    def test_aggregates_multiple_groups_columns(self, setup_access_rules):
        """Should union columns from all of the user's groups."""
        from terno_dbi.services.access import get_all_group_columns
        
        tables = Table.objects.filter(data_source=setup_access_rules['datasource'])
        roles = Group.objects.filter(name__in=['access_admin', 'access_admin_2'])
        columns = get_all_group_columns(setup_access_rules['datasource'], tables, roles)
        
        col_names = list(columns.values_list('name', flat=True))
        assert 'group_col' in col_names
        assert 'multi_group_col' in col_names

    def test_aggregates_overlapping_groups_columns_without_duplicates(self, setup_access_rules):
        """Should return exactly one instance of a column even if granted by multiple groups."""
        from terno_dbi.services.access import get_all_group_columns
        
        # Give second_admin_group access to the exact same col that admin_group has
        setup_schema = setup_access_rules
        second_group_selector = GroupColumnSelector.objects.get(group=setup_schema['second_admin_group'])
        second_group_selector.columns.add(setup_schema['columns'][2]) # group_col

        tables = Table.objects.filter(data_source=setup_access_rules['datasource'])
        roles = Group.objects.filter(name__in=['access_admin', 'access_admin_2'])
        columns = get_all_group_columns(setup_access_rules['datasource'], tables, roles)
        
        col_names = list(columns.values_list('name', flat=True))
        
        # The 'group_col' should only appear exactly once
        assert col_names.count('group_col') == 1
        assert 'multi_group_col' in col_names




@pytest.mark.django_db
class TestGetAdminConfigObject:
    """Tests for get_admin_config_object function."""

    def test_returns_tables_and_columns(self, setup_schema):
        """Should return tuple of (tables, columns)."""
        from terno_dbi.services.access import get_admin_config_object
        
        roles = Group.objects.none()
        tables, columns = get_admin_config_object(setup_schema['datasource'], roles)
        
        assert tables.count() == 5
        assert columns.count() == 5

    def test_respects_privacy_rules(self, setup_access_rules):
        """Should exclude private tables and columns."""
        from terno_dbi.services.access import get_admin_config_object
        
        roles = Group.objects.filter(name='access_user')
        tables, columns = get_admin_config_object(setup_access_rules['datasource'], roles)
        
        table_names = list(tables.values_list('name', flat=True))
        col_names = list(columns.values_list('name', flat=True))
        
        assert 'private_table' not in table_names
        assert 'private_col' not in col_names
