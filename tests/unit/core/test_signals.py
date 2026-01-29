"""
Unit tests for signals.py.

Tests Django signal definitions and cache invalidation handlers.
"""
import pytest
from unittest.mock import patch, MagicMock
from django.dispatch import Signal

from terno_dbi.core.models import DataSource, Table, TableColumn


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


class TestCustomSignals:
    """Tests for custom signal definitions."""

    def test_datasource_created_signal_exists(self):
        """datasource_created signal should be defined."""
        from terno_dbi.core.signals import datasource_created
        
        assert isinstance(datasource_created, Signal)

    def test_datasource_updated_signal_exists(self):
        """datasource_updated signal should be defined."""
        from terno_dbi.core.signals import datasource_updated
        
        assert isinstance(datasource_updated, Signal)

    def test_datasource_deleted_signal_exists(self):
        """datasource_deleted signal should be defined."""
        from terno_dbi.core.signals import datasource_deleted
        
        assert isinstance(datasource_deleted, Signal)

    def test_query_executed_signal_exists(self):
        """query_executed signal should be defined."""
        from terno_dbi.core.signals import query_executed
        
        assert isinstance(query_executed, Signal)


class TestCacheInvalidation:
    """Tests for cache invalidation helper."""

    @patch('terno_dbi.services.shield.delete_cache')
    def test_invalidate_cache_calls_delete_cache(self, mock_delete_cache, datasource):
        """_invalidate_cache_for_datasource should call delete_cache."""
        from terno_dbi.core.signals import _invalidate_cache_for_datasource
        
        _invalidate_cache_for_datasource(datasource)
        
        mock_delete_cache.assert_called_once_with(datasource)


@pytest.mark.django_db
class TestTableSignalHandlers:
    """Tests for table change signal handlers."""

    @patch('terno_dbi.core.signals._invalidate_cache_for_datasource')
    def test_table_save_invalidates_cache(self, mock_invalidate, datasource):
        """Saving a table should invalidate datasource cache."""
        from terno_dbi.core.signals import connect_cache_invalidation_signals
        
        # Connect signals
        connect_cache_invalidation_signals()
        
        # Create a table (triggers post_save)
        table = Table.objects.create(
            name='cache_test_table',
            data_source=datasource
        )
        
        # Cache should be invalidated
        mock_invalidate.assert_called()
        # Get the call args - should include the datasource
        call_args = mock_invalidate.call_args_list[-1]
        assert call_args[0][0] == datasource

    @patch('terno_dbi.core.signals._invalidate_cache_for_datasource')
    def test_table_update_invalidates_cache(self, mock_invalidate, table, datasource):
        """Updating a table should invalidate datasource cache."""
        from terno_dbi.core.signals import connect_cache_invalidation_signals
        
        connect_cache_invalidation_signals()
        
        # Update table
        table.description = 'Updated description'
        table.save()
        
        mock_invalidate.assert_called()

    @patch('terno_dbi.core.signals._invalidate_cache_for_datasource')
    def test_table_delete_invalidates_cache(self, mock_invalidate, table, datasource):
        """Deleting a table should invalidate datasource cache."""
        from terno_dbi.core.signals import connect_cache_invalidation_signals
        
        connect_cache_invalidation_signals()
        
        # Delete table
        table.delete()
        
        mock_invalidate.assert_called()


@pytest.mark.django_db
class TestColumnSignalHandlers:
    """Tests for column change signal handlers."""

    @patch('terno_dbi.core.signals._invalidate_cache_for_datasource')
    def test_column_save_invalidates_cache(self, mock_invalidate, table, datasource):
        """Saving a column should invalidate datasource cache."""
        from terno_dbi.core.signals import connect_cache_invalidation_signals
        
        connect_cache_invalidation_signals()
        
        # Create a column (triggers post_save)
        column = TableColumn.objects.create(
            name='cache_test_col',
            table=table,
            data_type='int'
        )
        
        mock_invalidate.assert_called()

    @patch('terno_dbi.core.signals._invalidate_cache_for_datasource')
    def test_column_update_invalidates_cache(self, mock_invalidate, column, datasource):
        """Updating a column should invalidate datasource cache."""
        from terno_dbi.core.signals import connect_cache_invalidation_signals
        
        connect_cache_invalidation_signals()
        
        # Update column
        column.description = 'Updated description'
        column.save()
        
        mock_invalidate.assert_called()


class TestConnectCacheInvalidationSignals:
    """Tests for signal connection function."""

    def test_connect_function_exists(self):
        """connect_cache_invalidation_signals should be a callable."""
        from terno_dbi.core.signals import connect_cache_invalidation_signals
        
        assert callable(connect_cache_invalidation_signals)

    @patch('django.db.models.signals.post_save.connect')
    @patch('django.db.models.signals.post_delete.connect')
    def test_connects_table_signals(self, mock_delete_connect, mock_save_connect):
        """Should connect post_save and post_delete for Table."""
        from terno_dbi.core.signals import connect_cache_invalidation_signals
        from terno_dbi.core.models import Table
        
        connect_cache_invalidation_signals()
        
        # Check that connect was called with Table sender
        table_save_calls = [c for c in mock_save_connect.call_args_list 
                           if c[1].get('sender') == Table]
        table_delete_calls = [c for c in mock_delete_connect.call_args_list 
                             if c[1].get('sender') == Table]
        
        assert len(table_save_calls) >= 1
        assert len(table_delete_calls) >= 1
