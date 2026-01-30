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
