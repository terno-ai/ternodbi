import unittest
from unittest.mock import MagicMock, patch
from terno_dbi.connectors.mysql import MySQLConnector
from sqlshield.models import MDatabase
from sqlalchemy.engine import Engine

class TestMySQLConnector(unittest.TestCase):
    
    def test_init_defaults(self):
        """Should initialize with defaults."""
        connector = MySQLConnector("mysql://user:pass@host/db")
        assert connector.pool_size == 20
        assert connector.use_pool is True

    def test_get_metadata(self):
        """Should reflect metadata and return MDatabase."""
        connector = MySQLConnector("mysql://uri")
        mock_engine = MagicMock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, '_reflect_metadata') as mock_reflect:
                mock_meta = MagicMock()
                mock_reflect.return_value = mock_meta
                
                with patch('terno_dbi.connectors.mysql.MDatabase.from_inspector') as mock_from_inspector:
                    result = connector.get_metadata()
                    
                    mock_reflect.assert_called_with(mock_engine)
                    mock_from_inspector.assert_called_with(mock_meta)
                    assert result == mock_from_inspector.return_value

    def test_get_dialect_info(self):
        """Should return dialect name and version."""
        connector = MySQLConnector("mysql://uri")
        mock_engine = MagicMock()
        
        # Mock connection context
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        
        # Mock dialect
        mock_engine.dialect.name = "mysql"
        mock_engine.dialect.server_version_info = (8, 0, 1)
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            name, version = connector.get_dialect_info()
            assert name == "mysql"
            assert version == "(8, 0, 1)"

    @patch('terno_dbi.connectors.mysql.text')
    def test_get_table_row_counts(self, mock_text):
        connector = MySQLConnector("mysql://uri")
        mock_engine = MagicMock()
        mock_engine.url.database = "mydb"
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, 'get_connection') as mock_get_conn:
                mock_conn = MagicMock()
                mock_get_conn.return_value.__enter__.return_value = mock_conn
                mock_conn.execute.return_value.fetchall.return_value = [("tbl", 5)]
                
                counts = connector.get_table_row_counts()
                assert counts == {"tbl": 5}

    @patch('terno_dbi.connectors.mysql.text')
    def test_get_table_row_counts_schema_fallback(self, mock_text):
        connector = MySQLConnector("mysql://uri")
        mock_engine = MagicMock()
        mock_engine.url.database = None
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, 'get_connection') as mock_get_conn:
                mock_conn = MagicMock()
                mock_get_conn.return_value.__enter__.return_value = mock_conn
                
                def execute_side_effect(*args, **kwargs):
                    if mock_text.return_value == args[0] and "DATABASE()" in args[0].text:
                        mock_r = MagicMock()
                        mock_r.scalar.return_value = "db"
                        return mock_r
                    mock_result = MagicMock()
                    mock_result.fetchall.return_value = [("users", 50)]
                    return mock_result
                
                mock_conn.execute.side_effect = execute_side_effect
                counts = connector.get_table_row_counts()
                assert counts == {"users": 50}

    @patch('terno_dbi.connectors.mysql.text')
    def test_get_table_row_counts_schema_exception(self, mock_text):
        connector = MySQLConnector("mysql://uri")
        mock_engine = MagicMock()
        mock_engine.url.database = None
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, 'get_connection') as mock_get_conn:
                mock_conn = MagicMock()
                mock_get_conn.return_value.__enter__.return_value = mock_conn
                
                mock_conn.execute.side_effect = Exception("error details")
                
                counts = connector.get_table_row_counts()
                assert counts == {}
