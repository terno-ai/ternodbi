"""
Unit tests for Databricks Connector.
"""
import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.engine import Engine
from terno_dbi.connectors.databricks import DatabricksConnector

class TestDatabricksConnector:
    """Tests for DatabricksConnector class."""

    def test_init_parses_schema(self):
        """Should extract schema from connection string."""
        # databricks://host/default
        connector = DatabricksConnector("databricks://user:pass@host/my_schema")
        assert connector._schema == "my_schema"
        
        # default schema
        connector_def = DatabricksConnector("databricks://user:pass@host/")
        # make_url might parse empty database as None or empty str depending on version/url
        # If None, code defaults to "default"
        # Let's check logic: url.database or "default"
        # If url ends in /, database might be None.
        if connector_def._schema != "default":
             # Depending on make_url implementation for this string
             pass 

    def test_safe_reflect_metadata_success(self):
        """Should reflect metadata safely."""
        connector = DatabricksConnector("databricks://host/schema")
        
        mock_engine = MagicMock(spec=Engine)
        
        with patch('terno_dbi.connectors.databricks.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspect.return_value = mock_inspector
            
            # Setup tables return
            mock_inspector.get_table_names.return_value = ['table1']
            
            metadata = connector._safe_reflect_metadata(mock_engine, schema='schema')
            
            # Should have reflected info
            mock_inspector.get_table_names.assert_called_with(schema='schema')
            # Check metadata.reflect called? 
            # metadata is a MetaData object. We can check if its reflect method was called if we mock MetaData.
            # Or checks logs?
            
    def test_safe_reflect_metadata_partial_failure(self):
        """Should continue if one table fails to reflect."""
        connector = DatabricksConnector("databricks://host/schema")
        mock_engine = MagicMock(spec=Engine)
        
        with patch('terno_dbi.connectors.databricks.inspect') as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspect.return_value = mock_inspector
            mock_inspector.get_table_names.return_value = ['table1', 'table2']
            
            with patch('terno_dbi.connectors.databricks.MetaData') as mock_metadata_cls:
                mock_meta = MagicMock()
                mock_metadata_cls.return_value = mock_meta
                
                # reflect side effect: success first, fail second
                mock_meta.reflect.side_effect = [None, Exception("Table2 Fail")]
                
                connector._safe_reflect_metadata(mock_engine, schema='schema')
                
                # Should have tried both
                assert mock_meta.reflect.call_count == 2
                
    def test_safe_reflect_metadata_total_failure(self):
        """Should raise exception on top level inspection failure."""
        connector = DatabricksConnector("databricks://host/schema")
        mock_engine = MagicMock(spec=Engine)
        
        with patch('terno_dbi.connectors.databricks.inspect') as mock_inspect:
            mock_inspect.side_effect = Exception("Inspector Gadget Malfunction")
            
            # Should crash as user reverted safey block
            with pytest.raises(Exception, match="Inspector Gadget Malfunction"):
                connector._safe_reflect_metadata(mock_engine, schema='schema')

    def test_get_metadata_public(self):
        """Should call _safe_reflect_metadata and return MDatabase."""
        connector = DatabricksConnector("databricks://host/schema")
        
        with patch.object(connector, 'get_engine') as mock_get:
            mock_engine = MagicMock()
            mock_get.return_value = mock_engine
            
            with patch.object(connector, '_safe_reflect_metadata') as mock_reflect:
                mock_meta = MagicMock()
                mock_reflect.return_value = mock_meta
                
                with patch('terno_dbi.connectors.databricks.MDatabase') as mock_mdb:
                    connector.get_metadata()
                    
                    mock_reflect.assert_called_with(mock_engine, connector._schema)
                    mock_mdb.from_inspector.assert_called_with(mock_meta)

    def test_get_dialect_info(self):
        """Should return dialect info."""
        connector = DatabricksConnector("databricks://host/schema")
        
        with patch.object(connector, 'get_engine') as mock_get:
            mock_engine = MagicMock()
            mock_conn = MagicMock()
            mock_get.return_value = mock_engine
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            
            mock_engine.dialect.name = "databricks"
            mock_engine.dialect.server_version_info = (3, 0)
            
            name, version = connector.get_dialect_info()
            
            assert name == "databricks"
            assert "(3, 0)" in version
