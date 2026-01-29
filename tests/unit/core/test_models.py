"""
Unit tests for TernoDBI Core Models.

Tests model methods, properties, and relationships.
"""
import pytest
import hashlib
from datetime import datetime, timedelta
from django.utils import timezone
from django.contrib.auth.models import User, Group

from terno_dbi.core.models import (
    DataSource, Table, TableColumn, ForeignKey,
    GroupTableRowFilter, ServiceToken, PrivateTableSelector,
    GroupTableSelector, PrivateColumnSelector, GroupColumnSelector
)


@pytest.fixture
def user(db):
    """Create a test user."""
    return User.objects.create_user('modeluser', 'model@example.com', 'password')


@pytest.fixture
def datasource(db):
    """Create a test datasource."""
    return DataSource.objects.create(
        display_name='model_test_db',
        type='postgres',
        connection_str='postgresql://localhost/modeltest',
        enabled=True
    )


@pytest.fixture
def table(datasource):
    """Create a test table."""
    return Table.objects.create(
        name='test_table',
        public_name='Test Table',
        data_source=datasource,
        description='A test table'
    )


@pytest.fixture
def column(table):
    """Create a test column."""
    return TableColumn.objects.create(
        name='test_column',
        public_name='Test Column',
        table=table,
        data_type='varchar',
        description='A test column'
    )


@pytest.mark.django_db
class TestDataSourceModel:
    """Tests for DataSource model."""

    def test_str_representation(self, datasource):
        """String representation should be display_name."""
        assert str(datasource) == 'model_test_db'

    def test_defaults(self, db):
        """Should have correct defaults."""
        ds = DataSource.objects.create(
            display_name='default_test',
            type='postgres',
            connection_str='postgresql://localhost/db'
        )
        
        assert ds.enabled is True
        assert ds.description == ''

    def test_connection_json_nullable(self, db):
        """connection_json should be nullable."""
        ds = DataSource.objects.create(
            display_name='json_test',
            type='postgres',
            connection_str='postgresql://localhost/db'
        )
        
        assert ds.connection_json is None

    def test_connection_json_stores_dict(self, db):
        """connection_json should store dict."""
        ds = DataSource.objects.create(
            display_name='json_test',
            type='bigquery',
            connection_str='bigquery://project/dataset',
            connection_json={'type': 'service_account'}
        )
        
        ds.refresh_from_db()
        assert ds.connection_json['type'] == 'service_account'


@pytest.mark.django_db
class TestTableModel:
    """Tests for Table model."""

    def test_str_representation(self, table):
        """String representation should include name and datasource."""
        assert 'test_table' in str(table)

    def test_related_datasource(self, table, datasource):
        """Should relate to datasource."""
        assert table.data_source == datasource

    def test_columns_relationship(self, table, column):
        """Should have related columns through reverse relation."""
        # Get columns through the related manager
        columns = TableColumn.objects.filter(table=table)
        assert column in columns

    def test_sample_rows_nullable(self, datasource):
        """sample_rows should be nullable."""
        table = Table.objects.create(
            name='sample_test',
            data_source=datasource
        )
        
        assert table.sample_rows is None


@pytest.mark.django_db
class TestTableColumnModel:
    """Tests for TableColumn model."""

    def test_str_representation(self, column):
        """String representation should include name."""
        assert 'test_column' in str(column)

    def test_related_table(self, column, table):
        """Should relate to table."""
        assert column.table == table

    def test_nullable_defaults(self, table):
        """Optional fields should be nullable."""
        col = TableColumn.objects.create(
            name='nullable_test',
            table=table,
            data_type='int'
        )
        
        # public_name and description are nullable (None, not empty string)
        assert col.public_name is None or col.public_name == ''
        assert col.description is None or col.description == ''


@pytest.mark.django_db
class TestForeignKeyModel:
    """Tests for ForeignKey model."""

    def test_creates_foreign_key_relationship(self, datasource):
        """Should create FK relationship between columns."""
        table1 = Table.objects.create(name='users', data_source=datasource)
        table2 = Table.objects.create(name='orders', data_source=datasource)
        
        col1 = TableColumn.objects.create(name='id', table=table1, data_type='int')
        col2 = TableColumn.objects.create(name='user_id', table=table2, data_type='int')
        
        fk = ForeignKey.objects.create(
            constrained_table=table2,
            constrained_columns=col2,
            referred_table=table1,
            referred_columns=col1
        )
        
        assert fk.constrained_columns == col2
        assert fk.referred_columns == col1


@pytest.mark.django_db
class TestGroupTableRowFilterModel:
    """Tests for GroupTableRowFilter model."""

    def test_creates_row_filter(self, datasource, table, db):
        """Should create row filter with SQL expression."""
        group = Group.objects.create(name='row_filter_group')
        
        row_filter = GroupTableRowFilter.objects.create(
            data_source=datasource,
            table=table,
            group=group,
            filter_str='active = 1'
        )
        
        assert row_filter.filter_str == 'active = 1'
        assert row_filter.data_source == datasource
        assert row_filter.group == group


@pytest.mark.django_db
class TestServiceTokenModel:
    """Tests for ServiceToken model."""

    def test_token_type_choices(self, user):
        """Should support QUERY and ADMIN token types."""
        query_token = ServiceToken.objects.create(
            name='Query Token',
            token_type=ServiceToken.TokenType.QUERY,
            key_prefix='dbi_query_',
            key_hash='hash1',
            created_by=user
        )
        
        admin_token = ServiceToken.objects.create(
            name='Admin Token',
            token_type=ServiceToken.TokenType.ADMIN,
            key_prefix='dbi_admin_',
            key_hash='hash2',
            created_by=user
        )
        
        # token_type is the enum value, check against enum
        assert query_token.token_type == ServiceToken.TokenType.QUERY
        assert admin_token.token_type == ServiceToken.TokenType.ADMIN

    def test_is_active_default(self, user):
        """is_active should default to True."""
        token = ServiceToken.objects.create(
            name='Active Test',
            token_type=ServiceToken.TokenType.QUERY,
            key_prefix='dbi_',
            key_hash='hash',
            created_by=user
        )
        
        assert token.is_active is True

    def test_hash_key_classmethod(self, user):
        """hash_key should SHA256 hash a key."""
        raw_key = 'dbi_query_testkey123'
        hashed = ServiceToken.hash_key(raw_key)
        
        assert hashed == hashlib.sha256(raw_key.encode()).hexdigest()

    def test_get_accessible_datasources(self, user, datasource):
        """get_accessible_datasources should return linked datasources."""
        token = ServiceToken.objects.create(
            name='DS Test',
            token_type=ServiceToken.TokenType.QUERY,
            key_prefix='dbi_',
            key_hash='hash',
            created_by=user
        )
        token.datasources.add(datasource)
        
        accessible = token.get_accessible_datasources()
        
        assert datasource in accessible

    def test_has_access_to_datasource(self, user, datasource):
        """has_access_to_datasource checks if token has access."""
        token = ServiceToken.objects.create(
            name='Access Test',
            token_type=ServiceToken.TokenType.QUERY,
            key_prefix='dbi_',
            key_hash='hash3',
            created_by=user
        )
        token.datasources.add(datasource)
        
        assert token.has_access_to_datasource(datasource) is True


@pytest.mark.django_db  
class TestSelectorModels:
    """Tests for Private/Group Table/Column Selector models."""

    def test_private_table_selector(self, datasource, table):
        """PrivateTableSelector should hide tables from all users."""
        selector = PrivateTableSelector.objects.create(
            data_source=datasource
        )
        selector.tables.add(table)
        
        assert table in selector.tables.all()

    def test_group_table_selector(self, table, db):
        """GroupTableSelector should grant table access to group."""
        group = Group.objects.create(name='table_group')
        
        selector = GroupTableSelector.objects.create(group=group)
        selector.tables.add(table)
        
        assert table in selector.tables.all()
        assert selector.group == group

    def test_private_column_selector(self, datasource, column):
        """PrivateColumnSelector should hide columns from all users."""
        selector = PrivateColumnSelector.objects.create(
            data_source=datasource
        )
        selector.columns.add(column)
        
        assert column in selector.columns.all()

    def test_group_column_selector(self, column, db):
        """GroupColumnSelector should grant column access to group."""
        group = Group.objects.create(name='column_group')
        
        selector = GroupColumnSelector.objects.create(group=group)
        selector.columns.add(column)
        
        assert column in selector.columns.all()
        assert selector.group == group
