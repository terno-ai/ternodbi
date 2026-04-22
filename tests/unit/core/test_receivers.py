"""
Unit tests for receivers.py.

Tests Django signal definitions and cache invalidation handlers.
"""
import pytest
from unittest.mock import patch, MagicMock
from django.dispatch import Signal

from terno_dbi.core.models import DataSource, Table, TableColumn, PrivateTableSelector


@pytest.fixture
def datasource(db):
    return DataSource.objects.create(
        display_name='signals_test_db',
        type='postgres',
        connection_str='postgresql://localhost/test',
        enabled=True
    )

@pytest.fixture
def table(datasource):
    return Table.objects.create(
        name='signals_table',
        data_source=datasource
    )

@pytest.fixture
def column(table):
    return TableColumn.objects.create(
        name='signals_col',
        table=table,
        data_type='varchar'
    )


class TestCacheInvalidation:
    """Tests for cache invalidation helper."""

    @patch('terno_dbi.core.receivers.delete_cache')
    def test_invalidate_cache_calls_delete_cache(self, mock_delete_cache, datasource):
        """_invalidate_cache_for_datasource should call delete_cache."""
        from terno_dbi.core.receivers import _invalidate_cache_for_datasource

        _invalidate_cache_for_datasource(datasource)

        mock_delete_cache.assert_called_once_with(datasource)


@pytest.mark.django_db
class TestReceiverHandlers:
    """Tests for table/column/config change signal handlers."""

    @patch('terno_dbi.core.receivers._invalidate_cache_for_datasource')
    def test_table_update_invalidates_cache(self, mock_invalidate, table, datasource):
        """Updating a table should invalidate datasource cache."""
        # Update table
        table.description = 'Updated description'
        table.save()

        # Should be called once during save
        assert mock_invalidate.call_count >= 1

    @patch('terno_dbi.core.receivers._invalidate_cache_for_datasource')
    def test_table_delete_invalidates_cache(self, mock_invalidate, table, datasource):
        """Deleting a table should invalidate datasource cache."""
        # Delete table
        table.delete()

        assert mock_invalidate.call_count >= 1

    @patch('terno_dbi.core.receivers._invalidate_cache_for_datasource')
    def test_column_update_invalidates_cache(self, mock_invalidate, column, datasource):
        """Updating a column should invalidate datasource cache."""
        # Update column
        column.description = 'Updated description'
        column.save()

        assert mock_invalidate.call_count >= 1

    @patch('terno_dbi.core.receivers._invalidate_cache_for_datasource')
    def test_private_table_selector_creation_invalidates_cache(self, mock_invalidate, datasource):
        """Creating a metadata configuration object should invalidate datasource cache."""
        pts = PrivateTableSelector.objects.create(data_source=datasource)
        assert mock_invalidate.call_count >= 1

    @patch('terno_dbi.core.receivers._invalidate_cache_for_datasource')
    def test_private_table_selector_m2m_invalidates_cache(self, mock_invalidate, datasource, table):
        """Adding to an m2m configuration object should invalidate datasource cache."""
        pts = PrivateTableSelector.objects.create(data_source=datasource)
        mock_invalidate.reset_mock()
        pts.tables.add(table)
        assert mock_invalidate.call_count >= 1

    @patch('terno_dbi.core.receivers._invalidate_cache_for_datasource')
    def test_group_table_selector_m2m_invalidates_cache(self, mock_invalidate, datasource, table):
        """Adding/removing to GroupTableSelector should invalidate datasource cache."""
        from django.contrib.auth.models import Group
        from terno_dbi.core.models import GroupTableSelector
        
        g = Group.objects.create(name='test_group')
        gts = GroupTableSelector.objects.create(group=g)
        
        mock_invalidate.reset_mock()
        gts.tables.add(table)
        mock_invalidate.assert_called_with(datasource.id)
        
        mock_invalidate.reset_mock()
        gts.tables.remove(table)
        mock_invalidate.assert_called_with(datasource.id)

    @patch('terno_dbi.core.receivers._invalidate_cache_for_datasource')
    def test_group_table_selector_exclude_invalidates_cache(self, mock_invalidate, datasource, table):
        """Adding/removing to GroupTableSelector.exclude_tables should invalidate."""
        from django.contrib.auth.models import Group
        from terno_dbi.core.models import GroupTableSelector
        
        g = Group.objects.create(name='test_group_exclude')
        gts = GroupTableSelector.objects.create(group=g)
        
        mock_invalidate.reset_mock()
        gts.exclude_tables.add(table)
        mock_invalidate.assert_called_with(datasource.id)

    @patch('terno_dbi.core.receivers._invalidate_cache_for_datasource')
    def test_group_table_selector_clear_invalidates_cache(self, mock_invalidate, datasource, table):
        """Pre_clear and Post_clear on GroupTableSelector should correctly invalidate."""
        from django.contrib.auth.models import Group
        from terno_dbi.core.models import GroupTableSelector
        
        g = Group.objects.create(name='test_group_clear')
        gts = GroupTableSelector.objects.create(group=g)
        gts.tables.add(table)
        
        mock_invalidate.reset_mock()
        gts.tables.clear()
        
        # Action occurs entirely during post_clear using data from pre_clear
        mock_invalidate.assert_called_with(datasource.id)

    @patch('terno_dbi.core.receivers._invalidate_cache_for_datasource')
    def test_m2m_reverse_relation_invalidates_cache(self, mock_invalidate, datasource, table):
        """Adding a GroupTableSelector from the Table's side (reverse) should invalidate."""
        from django.contrib.auth.models import Group
        from terno_dbi.core.models import GroupTableSelector
        
        g = Group.objects.create(name='test_group_reverse')
        gts = GroupTableSelector.objects.create(group=g)
        
        mock_invalidate.reset_mock()
        # Admin edits the Table and adds the group selector
        table.include_tables.add(gts)
        
        mock_invalidate.assert_called_with(datasource.id)

    @patch('terno_dbi.core.receivers._invalidate_cache_for_datasource')
    def test_group_column_selector_m2m_invalidates_cache(self, mock_invalidate, datasource, column):
        """Adding to GroupColumnSelector should invalidate the specific column's table's datasource."""
        from django.contrib.auth.models import Group
        from terno_dbi.core.models import GroupColumnSelector
        
        g = Group.objects.create(name='test_group_col')
        gcs = GroupColumnSelector.objects.create(group=g)
        
        mock_invalidate.reset_mock()
        gcs.columns.add(column)
        mock_invalidate.assert_called_with(datasource.id)
