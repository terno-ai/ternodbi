import unittest
from unittest.mock import MagicMock, patch
from terno_dbi.connectors.snowflake import SnowflakeConnector
from sqlshield.models import MDatabase
from sqlalchemy.engine import Engine

class TestSnowflakeConnector(unittest.TestCase):
    
    def test_init(self):
        """Should initialize."""
        connector = SnowflakeConnector("snowflake://uri")
        assert connector.pool_size == 20

    def test_get_metadata(self):
        """Should use from_snowflake_dialect."""
        connector = SnowflakeConnector("snowflake://uri")
        mock_engine = MagicMock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            # Snowflake connector calls MDatabase.from_snowflake_dialect directly,
            # bypassing _reflect_metadata
            with patch('terno_dbi.connectors.snowflake.MDatabase.from_snowflake_dialect') as mock_from_sf:
                result = connector.get_metadata()
                
                mock_from_sf.assert_called_with(mock_engine)
                assert result == mock_from_sf.return_value

    def test_get_dialect_info(self):
        """Should return dialect info."""
        connector = SnowflakeConnector("snowflake://uri")
        mock_engine = MagicMock()
        
        mock_engine.connect.return_value.__enter__.return_value = MagicMock()
        mock_engine.dialect.name = "snowflake"
        mock_engine.dialect.server_version_info = (7, 0)
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            name, version = connector.get_dialect_info()
            assert name == "snowflake"
            assert version == "(7, 0)"

    @patch('terno_dbi.connectors.snowflake.text')
    def test_get_table_row_counts(self, mock_text):
        connector = SnowflakeConnector("snowflake://user:pass@acc/db/schema")
        mock_engine = MagicMock()
        mock_engine.url.database = "db/schema"
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, 'get_connection') as mock_get_conn:
                mock_conn = MagicMock()
                mock_get_conn.return_value.__enter__.return_value = mock_conn
                mock_conn.execute.return_value.fetchall.return_value = [("TBL", 50)]
                counts = connector.get_table_row_counts()
                assert counts == {"TBL": 50}

    @patch('terno_dbi.connectors.snowflake.text')
    def test_get_table_row_counts_schema_fallback(self, mock_text):
        connector = SnowflakeConnector("snowflake://user:pass@acc")
        mock_engine = MagicMock()
        mock_engine.url.database = None
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, 'get_connection') as mock_get_conn:
                mock_conn = MagicMock()
                mock_get_conn.return_value.__enter__.return_value = mock_conn
                
                def execute_side_effect(*args, **kwargs):
                    if mock_text.return_value == args[0] and "CURRENT_SCHEMA" in args[0].text:
                        mock_r = MagicMock()
                        mock_r.scalar.return_value = "public"
                        return mock_r
                    mock_result = MagicMock()
                    mock_result.fetchall.return_value = [("users", 50)]
                    return mock_result
                
                mock_conn.execute.side_effect = execute_side_effect
                counts = connector.get_table_row_counts()
                assert counts == {"users": 50}

    @patch('terno_dbi.connectors.snowflake.text')
    def test_get_table_row_counts_schema_exception(self, mock_text):
        connector = SnowflakeConnector("snowflake://user:pass@acc")
        mock_engine = MagicMock()
        mock_engine.url.database = None
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, 'get_connection') as mock_get_conn:
                mock_conn = MagicMock()
                mock_get_conn.return_value.__enter__.return_value = mock_conn
                mock_conn.execute.side_effect = Exception("error")
                counts = connector.get_table_row_counts()
                assert counts == {}
