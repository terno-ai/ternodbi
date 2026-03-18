import unittest
from unittest.mock import MagicMock, patch
from terno_dbi.connectors.postgres import PostgresConnector
from sqlshield.models import MDatabase
from sqlalchemy.engine import Engine

class TestPostgresConnector(unittest.TestCase):
    
    def test_init_defaults(self):
        """Should initialize with defaults."""
        connector = PostgresConnector("postgresql://uri")
        assert connector.pool_size == 20

    def test_get_metadata(self):
        """Should reflect metadata."""
        connector = PostgresConnector("postgresql://uri")
        mock_engine = MagicMock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, '_reflect_metadata') as mock_reflect:
                mock_meta = MagicMock()
                mock_reflect.return_value = mock_meta
                
                with patch('terno_dbi.connectors.postgres.MDatabase.from_inspector') as mock_from_inspector:
                    result = connector.get_metadata()
                    assert result == mock_from_inspector.return_value

    def test_get_dialect_info_normalizes_name(self):
        """Should normalize 'postgresql' to 'postgres'."""
        connector = PostgresConnector("postgresql://uri")
        mock_engine = MagicMock()
        
        mock_engine.connect.return_value.__enter__.return_value = MagicMock()
        mock_engine.dialect.name = "postgresql"
        mock_engine.dialect.server_version_info = (14, 1)
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            name, version = connector.get_dialect_info()
            assert name == "postgres" # Normalized
            assert version == "(14, 1)"
            
    def test_get_dialect_info_other_name(self):
        """Should keep other dialect names as is (edge case)."""
        connector = PostgresConnector("postgres://uri")
        mock_engine = MagicMock()
        
        mock_engine.connect.return_value.__enter__.return_value = MagicMock()
        mock_engine.dialect.name = "redshift"
        mock_engine.dialect.server_version_info = (1, 0)
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            name, _ = connector.get_dialect_info()
            assert name == "redshift"

    @patch('terno_dbi.connectors.postgres.text')
    def test_get_table_row_counts_default_schema(self, mock_text):
        connector = PostgresConnector("postgresql://uri")
        with patch.object(connector, 'get_connection') as mock_get_conn:
            mock_conn = MagicMock()
            mock_get_conn.return_value.__enter__.return_value = mock_conn
            
            # mock getting current schema
            mock_conn.execute.return_value.scalar.return_value = "public"
            # mock fetching row counts
            mock_conn.execute.return_value.fetchall.return_value = [("users", 100), ("orders", -1)]
            
            counts = connector.get_table_row_counts()
            
            assert counts == {"users": 100}

    @patch('terno_dbi.connectors.postgres.text')
    def test_get_table_row_counts_schema_fallback(self, mock_text):
        connector = PostgresConnector("postgresql://uri")
        with patch.object(connector, 'get_connection') as mock_get_conn:
            mock_conn = MagicMock()
            mock_get_conn.return_value.__enter__.return_value = mock_conn
            
            # First execute raises, fallback to 'public'
            def execute_side_effect(*args, **kwargs):
                if mock_text.return_value == args[0] and "current_schema" in args[0].text:
                    raise Exception("no schema")
                mock_result = MagicMock()
                mock_result.fetchall.return_value = [("users", 50)]
                return mock_result
            
            mock_conn.execute.side_effect = execute_side_effect
            counts = connector.get_table_row_counts()
            assert counts == {"users": 50}

    @patch('terno_dbi.connectors.postgres.text')
    def test_get_table_row_counts_with_tables(self, mock_text):
        connector = PostgresConnector("postgresql://uri")
        with patch.object(connector, 'get_connection') as mock_get_conn:
            mock_conn = MagicMock()
            mock_get_conn.return_value.__enter__.return_value = mock_conn
            
            # Use explicit schema and tables
            mock_conn.execute.return_value.fetchall.return_value = [("users", 50)]
            
            counts = connector.get_table_row_counts(schema="test", tables=["public.users"])
            assert counts == {"users": 50}
            
            # Verify the query execution was called correctly
            mock_conn.execute.assert_called_with(mock_text.return_value, {"schema": "test", "tables": ["users"]})
