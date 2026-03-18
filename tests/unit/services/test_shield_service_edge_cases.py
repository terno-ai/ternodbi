"""
Edge case tests for Shield Service.

Covers MDB generation edge cases, error handling, and cache logic.
"""
import pytest
from unittest.mock import patch, MagicMock
from terno_dbi.services.shield import (
    generate_native_sql,
    get_cache_key,
    prepare_mdb,
    _get_cache_version,
    delete_cache
)
from terno_dbi.core.models import DataSource, Table, Group, ServiceToken

@pytest.fixture
def datasource(db):
    return DataSource.objects.create(
        display_name="shield_test",
        type="postgres",
        connection_str="postgresql://localhost/db",
        enabled=True
    )

@pytest.mark.django_db
class TestShieldEdgeCases:
    """Tests for shield service edge cases."""

    def test_native_sql_exception(self):
        """Should handle SQL generation errors."""
        mock_mdb = MagicMock()
        # Mock Session to raise
        with patch('terno_dbi.services.shield.Session') as mock_session_cls:
            mock_session = MagicMock()
            mock_session.generateNativeSQL.side_effect = Exception("Parsing error")
            mock_session_cls.return_value = mock_session
            
            result = generate_native_sql(mock_mdb, "SELECT *", "postgres")
            
        assert result['status'] == 'error'
        assert "Parsing error" in result['error']

    def test_get_cache_version_none(self, datasource):
        """Should return 0 if no version cached."""
        with patch('terno_dbi.services.shield.cache') as mock_cache:
            mock_cache.get.return_value = None
            version = _get_cache_version(datasource.id)
            assert version == 0

    def test_get_cache_version_exists(self, datasource):
        """Should return cached version."""
        with patch('terno_dbi.services.shield.cache') as mock_cache:
            mock_cache.get.return_value = 5
            version = _get_cache_version(datasource.id)
            assert version == 5

    def test_cache_key_sorting(self, datasource):
        """Roles should be sorted in cache key."""
        key1 = get_cache_key(datasource.id, [2, 1, 3])
        key2 = get_cache_key(datasource.id, [3, 2, 1])
        assert key1 == key2
        assert "roles_1_2_3" in key1

    def test_prepare_mdb_cached(self, datasource):
        """Should return cached MDB if available."""
        roles = Group.objects.none()
        with patch('terno_dbi.services.shield.cache') as mock_cache:
            mock_cache.get.return_value = "cached_mdb_obj"
            
            result = prepare_mdb(datasource, roles)
            
            assert result == "cached_mdb_obj"

    def test_delete_cache_bumps_version(self, datasource):
        """Delete cache should increment version."""
        with patch('terno_dbi.services.shield.cache') as mock_cache:
            mock_cache.get.return_value = 10
            
            delete_cache(datasource)
            
            # verify set was called with version 11
            args = mock_cache.set.call_args
            assert args[0][1] == 11

    def test_keep_only_columns_missing_table(self):
        from terno_dbi.services.shield import _keep_only_columns
        class MockTable:
            def __init__(self):
                self.name = "not_found_tbl"
        mock_mdb = MagicMock()
        mock_table = MockTable()
        mock_mdb.tables = {"not_found_tbl": mock_table}
        
        mock_tables_qs = MagicMock()
        mock_tables_qs.filter.return_value = []  # No table obj found
        
        # Should skip modifying the table if table_obj is empty
        _keep_only_columns(mock_mdb, mock_tables_qs, MagicMock())
        assert not hasattr(mock_table, 'pub_name')

    def test_keep_only_columns_missing_column(self):
        from terno_dbi.services.shield import _keep_only_columns
        class MockTable:
            def __init__(self):
                self.name = "tbl1"
        mock_mdb = MagicMock()
        mock_table = MockTable()
        mock_table.drop_columns = MagicMock()
        
        class MockCol:
            def __init__(self):
                self.name = "col1"
        mock_col = MockCol()
        mock_table.columns = {"col1": mock_col}
        mock_mdb.tables = {"tbl1": mock_table}
        
        mock_tbl_obj = MagicMock()
        mock_tbl_obj.public_name = "PubTbl"
        
        mock_tables_qs = MagicMock()
        mock_qs = MagicMock()
        mock_qs.__bool__ = lambda self: True
        mock_qs.first.return_value = mock_tbl_obj
        mock_tables_qs.filter.return_value = mock_qs
        
        mock_columns_qs = MagicMock()
        mock_col_filter_qs = MagicMock()
        mock_col_filter_qs.__bool__ = lambda self: False  # Doesn't exist, evaluates to False
        mock_col_filter_qs.values_list.return_value = []
        mock_columns_qs.filter.return_value = mock_col_filter_qs
        
        # We need to mock TableColumn.objects.filter directly
        with patch('terno_dbi.services.shield.models.TableColumn.objects.filter') as mock_tc_filter:
            mock_tc_filter.return_value.values_list.return_value = ["col1"]
            _keep_only_columns(mock_mdb, mock_tables_qs, mock_columns_qs)
            
        mock_table.drop_columns.assert_called_with({'col1'})
        assert not hasattr(mock_col, 'pub_name')

    def test_base_filters_empty_string(self):
        from terno_dbi.services.shield import _get_base_filters
        ds = MagicMock()
        with patch('terno_dbi.services.shield.models.TableRowFilter.objects.filter') as mock_trf_filter:
            mock_trf = MagicMock()
            mock_trf.filter_str = "   "  # Empty when stripped
            mock_trf_filter.return_value = [mock_trf]
            
            result = _get_base_filters(ds)
            assert result == {}

    def test_grp_filters_empty_and_multiple(self):
        from terno_dbi.services.shield import _get_grp_filters
        ds = MagicMock()
        roles = [1]
        with patch('terno_dbi.services.shield.models.GroupTableRowFilter.objects.filter') as mock_gtrf_filter:
            mock_trf_empty = MagicMock()
            mock_trf_empty.filter_str = ""
            
            mock_trf_1 = MagicMock()
            mock_trf_1.filter_str = "id > 0"
            mock_trf_1.table.name = "tbl1"
            
            mock_trf_2 = MagicMock()
            mock_trf_2.filter_str = "status = 'A'"
            mock_trf_2.table.name = "tbl1"
            
            mock_gtrf_filter.return_value = [mock_trf_empty, mock_trf_1, mock_trf_2]
            
            result = _get_grp_filters(ds, roles)
            assert result == {"tbl1": ["(id > 0)", "(status = 'A')"]}

    def test_update_filters_empty_list(self):
        from terno_dbi.services.shield import _update_filters
        ds = MagicMock()
        roles = [1]
        class MockTbl: pass
        tables = {"tbl1": MockTbl()}
        
        with patch('terno_dbi.services.shield._get_base_filters') as mock_base, \
             patch('terno_dbi.services.shield._get_grp_filters') as mock_grp:
             
             mock_base.return_value = {"tbl1": []}
             mock_grp.return_value = {}
             
             _update_filters(tables, ds, roles)
             assert not hasattr(tables['tbl1'], 'filters')

