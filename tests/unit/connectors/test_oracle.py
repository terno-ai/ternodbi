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
