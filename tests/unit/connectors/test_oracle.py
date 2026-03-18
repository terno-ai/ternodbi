import unittest
from unittest.mock import MagicMock, patch
from terno_dbi.connectors.oracle import OracleConnector
from sqlshield.models import MDatabase
from sqlalchemy.engine import Engine

class TestOracleConnector(unittest.TestCase):
    
    def test_init(self):
        """Should initialize."""
        connector = OracleConnector("oracle://uri")
        assert connector.use_pool is True

    def test_get_metadata(self):
        """Should reflect metadata."""
        connector = OracleConnector("oracle://uri")
        mock_engine = MagicMock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, '_reflect_metadata') as mock_reflect:
                mock_meta = MagicMock()
                mock_reflect.return_value = mock_meta
                
                with patch('terno_dbi.connectors.oracle.MDatabase.from_inspector') as mock_from_inspector:
                    result = connector.get_metadata()
                    assert result == mock_from_inspector.return_value

    def test_get_dialect_info(self):
        """Should return dialect info."""
        connector = OracleConnector("oracle://uri")
        mock_engine = MagicMock()
        
        mock_engine.connect.return_value.__enter__.return_value = MagicMock()
        mock_engine.dialect.name = "oracle"
        mock_engine.dialect.server_version_info = (19, 0)
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            name, version = connector.get_dialect_info()
            assert name == "oracle"
            assert version == "(19, 0)"

    @patch('terno_dbi.connectors.oracle.text')
    def test_get_table_row_counts(self, mock_text):
        connector = OracleConnector("oracle://scott:tiger@localhost:1521/orcl")
        mock_engine = MagicMock()
        mock_engine.url.username = "scott"
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, 'get_connection') as mock_get_conn:
                mock_conn = MagicMock()
                mock_get_conn.return_value.__enter__.return_value = mock_conn
                mock_conn.execute.return_value.fetchall.return_value = [("EMP", 14), ("DEPT", 4)]
                
                counts = connector.get_table_row_counts()
                assert counts == {"EMP": 14, "DEPT": 4}

    @patch('terno_dbi.connectors.oracle.text')
    def test_get_table_row_counts_schema_fallback(self, mock_text):
        connector = OracleConnector("oracle://localhost:1521/orcl")
        mock_engine = MagicMock()
        mock_engine.url.username = None
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, 'get_connection') as mock_get_conn:
                mock_conn = MagicMock()
                mock_get_conn.return_value.__enter__.return_value = mock_conn
                
                def execute_side_effect(*args, **kwargs):
                    if mock_text.return_value == args[0] and "SYS_CONTEXT" in args[0].text:
                        mock_r = MagicMock()
                        mock_r.scalar.return_value = "HR"
                        return mock_r
                    mock_result = MagicMock()
                    mock_result.fetchall.return_value = [("EMPLOYEES", 100)]
                    return mock_result
                
                mock_conn.execute.side_effect = execute_side_effect
                counts = connector.get_table_row_counts()
                assert counts == {"EMPLOYEES": 100}

    @patch('terno_dbi.connectors.oracle.text')
    def test_get_table_row_counts_schema_exception(self, mock_text):
        connector = OracleConnector("oracle://localhost:1521/orcl")
        mock_engine = MagicMock()
        mock_engine.url.username = None
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, 'get_connection') as mock_get_conn:
                mock_conn = MagicMock()
                mock_get_conn.return_value.__enter__.return_value = mock_conn
                mock_conn.execute.side_effect = Exception("error context")
                
                counts = connector.get_table_row_counts()
                assert counts == {}
