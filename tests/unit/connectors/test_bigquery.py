"""
Unit tests for BigQuery Connector.
"""
import pytest
from unittest.mock import MagicMock, patch
from sqlalchemy.pool import QueuePool, NullPool
from terno_dbi.connectors.bigquery import BigQueryConnector

class TestBigQueryConnector:
    """Tests for BigQueryConnector class."""

    def test_init_requires_credentials(self):
        """Should raise ValueError if credentials are missing."""
        with pytest.raises(ValueError, match="requires credentials"):
            BigQueryConnector('bigquery://proj/dataset')

    def test_init_success(self):
        """Should initialize with valid credentials."""
        connector = BigQueryConnector(
            'bigquery://proj/dataset',
            credentials={'type': 'service_account'}
        )
        assert connector.credentials == {'type': 'service_account'}

    def test_create_engine_pool_config(self):
        """Should configure pool correctly when use_pool is True."""
        connector = BigQueryConnector(
            'bigquery://proj/dataset',
            credentials={'type': 'service_account'},
            pool_size=5,
            max_overflow=10,
            use_pool=True
        )
        
        with patch('terno_dbi.connectors.bigquery.sqlalchemy.create_engine') as mock_create:
            connector.get_engine()
            
            # Check kwargs passed to create_engine
            _, kwargs = mock_create.call_args
            assert kwargs['poolclass'] == QueuePool
            assert kwargs['pool_size'] == 5
            assert kwargs['max_overflow'] == 10
            assert 'pool_pre_ping' in kwargs

    def test_create_engine_no_pool(self):
        """Should use NullPool when use_pool is False."""
        connector = BigQueryConnector(
            'bigquery://proj/dataset',
            credentials={'type': 'service_account'},
            use_pool=False
        )
        
        with patch('terno_dbi.connectors.bigquery.sqlalchemy.create_engine') as mock_create:
            connector.get_engine()
            
            _, kwargs = mock_create.call_args
            assert kwargs['poolclass'] == NullPool

    def test_get_metadata(self):
        """Should reflect metadata using engine."""
        connector = BigQueryConnector(
            'bigquery://proj/dataset', 
            credentials={'type': 'service_account'}
        )
        
        with patch.object(connector, 'get_engine') as mock_get_engine:
            mock_engine = MagicMock()
            mock_get_engine.return_value = mock_engine
            
            with patch.object(connector, '_reflect_metadata') as mock_reflect:
                mock_meta = MagicMock()
                mock_reflect.return_value = mock_meta
                
                with patch('terno_dbi.connectors.bigquery.MDatabase') as mock_mdb:
                    connector.get_metadata()
                    
                    mock_reflect.assert_called_with(mock_engine)
                    mock_mdb.from_inspector.assert_called_with(mock_meta)

    def test_get_dialect_info(self):
        """Should return dialect info."""
        connector = BigQueryConnector(
            'bigquery://proj/dataset', 
            credentials={'type': 'service_account'}
        )
        
        with patch.object(connector, 'get_engine') as mock_get_engine:
            mock_engine = MagicMock()
            mock_conn = MagicMock()
            mock_get_engine.return_value = mock_engine
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            
            mock_engine.dialect.name = "bigquery"
            mock_engine.dialect.server_version_info = (1, 0, 0)
            
            name, version = connector.get_dialect_info()
            
            assert name == "bigquery"
            assert "(1, 0, 0)" in version
