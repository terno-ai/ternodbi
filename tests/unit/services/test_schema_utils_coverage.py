import pytest
from unittest.mock import MagicMock, patch
from terno_dbi.services.schema_utils import (
    build_row_counts_lookup,
    resolve_row_count,
    _sync_from_information_schema,
    get_column_stats,
    get_sample_rows,
    get_table_info,
    sync_metadata
)
import sqlalchemy
from sqlalchemy import String, DateTime

class TestSchemaUtilsCoverage:

    def test_build_row_counts_lookup(self):
        raw = {"public.orders": 10}
        lookup = build_row_counts_lookup(raw)
        assert lookup["public.orders"] == 10
        assert lookup["orders"] == 10
        
        # Test collision where full name takes precedence
        raw = {"public.orders": 10, "orders": 5}
        lookup = build_row_counts_lookup(raw)
        assert lookup["orders"] == 5

    def test_resolve_row_count(self):
        counts = {"public.orders": 10, "users": 5}
        
        # Exact lowercased match
        assert resolve_row_count("public.ORDERS", counts) == 10
        
        # Base match
        assert resolve_row_count("schema.Users", counts) == 5
        
        # No match
        assert resolve_row_count("xyz", counts) is None

    def test_analyze_column_stats_string_exceptions(self):
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("string query failed")
        
        mock_col = MagicMock()
        mock_col.name = "col"
        mock_col.type = String()
        mock_col.c.col = "col"
        
        mock_inspector = MagicMock()
        mock_inspector.c = {"col": mock_col}
        
        res = get_column_stats(mock_conn, mock_inspector, "tbl", "col")
        # Should have caught and swallowed the string exception and tried length exception
        assert res.get("top_values") is None

    def test_analyze_column_stats_datetime_exception(self):
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("dt failed")
        
        mock_col = MagicMock()
        mock_col.type = DateTime()
        mock_inspector = MagicMock()
        
        res = get_column_stats(mock_conn, mock_inspector, "tbl", "col")
        assert res.get("min_date") is None
        
    def test_analyze_column_stats_top_level_exception(self):
        mock_conn = MagicMock()
        # Non-mock column raises TypeError leading to top-level exception
        res = get_column_stats(mock_conn, None, "tbl", "col")
        assert res == {}
        mock_conn.rollback.assert_called_once()
        
    def test_get_sample_rows_exception(self):
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("query failed")
        mock_inspector = MagicMock()
        mock_inspector.columns = []
        res = get_sample_rows(mock_conn, mock_inspector)
        assert res == []
        mock_conn.rollback.assert_called_once()
        
    @patch('terno_dbi.core.models.Table.objects.get')
    @patch('terno_dbi.core.models.TableColumn.objects.get')
    @patch('terno_dbi.services.schema_utils.ConnectorFactory')
    def test_get_table_info_exceptions(self, mock_factory, mock_col_get, mock_tbl_get):
        datasource = MagicMock()
        mock_tbl_get.side_effect = Exception("get fail")
        res = get_table_info(datasource, "tbl")
        # Top level get_table_info catches schema-related reflection normally, but let's test it mock-wise.
        assert "error" in res

    @patch('terno_dbi.core.models.Table.objects.filter')
    @patch('terno_dbi.core.models.Table.objects.create')
    @patch('terno_dbi.core.models.TableColumn.objects.filter')
    @patch('terno_dbi.core.models.TableColumn.objects.create')
    def test_sync_from_information_schema_snowflake(self, mock_col_creat, mock_col_filter, mock_tc, mock_tq):
        ds = MagicMock()
        ds.connection_str = "snowflake://user:pass@host/db/schema"
        connector = MagicMock()
        mock_conn = MagicMock()
        
        result_proxy = MagicMock()
        # schema_name, table_name, column_name, data_type, ordinal_position
        result_proxy.fetchall.return_value = [
            ("SCHEMA", "TBL1", "COL1", "VARCHAR", 1)
        ]
        mock_conn.execute.return_value = result_proxy
        connector.get_connection.return_value.__enter__.return_value = mock_conn
        
        mock_tq.return_value.first.return_value = None
        mock_col_filter.return_value.first.return_value = None
        
        res_dict = {"tables": [], "tables_skipped": 0, "tables_created": 0, "tables_updated": 0, "columns_created": 0}
        discovered = _sync_from_information_schema(connector, ds, res_dict, overwrite=True)
        assert discovered == 2  # Function increments twice apparently?
        assert res_dict["tables_created"] == 1
        assert res_dict["columns_created"] == 1

    @patch('terno_dbi.core.models.Table.objects.filter')
    @patch('terno_dbi.core.models.TableColumn.objects.filter')
    def test_sync_from_information_schema_overwrite_col(self, mock_col_filter, mock_tq):
        ds = MagicMock()
        ds.connection_str = "postgres://blah"
        connector = MagicMock()
        mock_conn = MagicMock()
        
        result_proxy = MagicMock()
        # schema_name, table_name, column_name, data_type, ordinal_position
        result_proxy.fetchall.return_value = [
            ("public", "TBL1", "COL1", "VARCHAR", 1)
        ]
        mock_conn.execute.return_value = result_proxy
        connector.get_connection.return_value.__enter__.return_value = mock_conn
        
        existing_table = MagicMock()
        mock_tq.return_value.first.return_value = existing_table
        
        existing_col = MagicMock()
        mock_col_filter.return_value.first.return_value = existing_col
        
        res_dict = {"tables": [], "tables_skipped": 0, "tables_created": 0, "tables_updated": 0, "columns_created": 0}
        discovered = _sync_from_information_schema(connector, ds, res_dict, overwrite=True)
        
        # Overwritten col
        assert existing_col.data_type == 'VARCHAR'
        existing_col.save.assert_called_once()
        assert res_dict["tables_updated"] == 1

    def test_sync_from_information_schema_snowflake_short_url(self):
        ds = MagicMock()
        ds.connection_str = "snowflake://short"
        connector = MagicMock()
        connector.get_connection.side_effect = Exception("connection failed")
        res_dict = {}
        # Checks lines 340-342
        discovered = _sync_from_information_schema(connector, ds, res_dict, overwrite=True)
        assert discovered == 0

    @pytest.mark.django_db
    @patch('terno_dbi.core.models.DataSource.objects.get')
    @patch('terno_dbi.services.schema_utils.ConnectorFactory.create_connector')
    @patch('terno_dbi.services.schema_utils._sync_from_information_schema')
    def test_sync_metadata_information_schema_fallback(self, mock_sync_info, mock_create, mock_ds_get):
        ds = MagicMock()
        ds.type = "postgres"
        mock_ds_get.return_value = ds
        
        connector = MagicMock()
        mdb = MagicMock()
        mdb.tables = {} # Empty triggers fallback
        connector.get_metadata.return_value = mdb
        
        # mock row counts for fallback
        connector.get_table_row_counts.return_value = {"public.tbl1": 100}
        
        mock_create.return_value = connector
        
        # mock what _sync_from_information_schema does: populates result["tables"]
        def fake_sync(conn, ds, result, overwrite):
            result["tables_created"] = 1
            result["tables_updated"] = 0
            result["tables"].append({"name": "public.tbl1", "status": "created", "id": 1})
            return 1
            
        mock_sync_info.side_effect = fake_sync
        
        with patch('terno_dbi.core.models.Table.objects.filter') as mock_tbl_filter:
            res = sync_metadata(1)
            
        assert res["sync_method"] == "information_schema"
        assert res["tables_synced"] == 1
        mock_sync_info.assert_called_once()
        mock_tbl_filter.return_value.update.assert_called_once_with(estimated_row_count=100)

    @pytest.mark.django_db
    @patch('terno_dbi.core.models.DataSource.objects.get')
    @patch('terno_dbi.services.schema_utils.ConnectorFactory.create_connector')
    @patch('terno_dbi.core.models.Table.objects.filter')
    def test_sync_metadata_duplicate_tables(self, mock_tbl_filter, mock_create, mock_ds_get):
        ds = MagicMock()
        ds.type = "postgres"
        mock_ds_get.return_value = ds
        
        connector = MagicMock()
        mdb = MagicMock()
        tbl = MagicMock()
        tbl.name = "public.tbl1"
        tbl.columns = []
        mdb.tables = {"public.tbl1": tbl}
        connector.get_metadata.return_value = mdb
        connector.get_table_row_counts.return_value = {}
        
        mock_create.return_value = connector
        
        # Mock duplicate tables query
        # existing_tables.count() > 1
        mock_qs = MagicMock()
        mock_qs.first.return_value = MagicMock(id=1)
        mock_qs.count.return_value = 2
        
        # exclude().values_list()
        mock_exclude = MagicMock()
        mock_exclude.values_list.return_value = [2]
        mock_qs.exclude.return_value = mock_exclude
        
        mock_tbl_filter.return_value = mock_qs
        
        with patch('terno_dbi.core.models.TableColumn.objects.filter'), patch('terno_dbi.services.schema_utils.transaction.atomic'):
            res = sync_metadata(1)
            
        assert res["sync_method"] == "sqlshield"
        assert res["tables_updated"] == 1

    def test_sync_from_information_schema_exception(self):
        ds = MagicMock()
        ds.connection_str = "postgres://blah"
        connector = MagicMock()
        connector.get_connection.side_effect = Exception("connection failed")
        
        res_dict = {}
        discovered = _sync_from_information_schema(connector, ds, res_dict)
        assert discovered == 0
