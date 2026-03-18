"""
Unit tests for Connector Factory and Base Connector.

Tests the factory pattern for database connectors and common connector behavior.
"""
import pytest
from unittest.mock import patch, MagicMock
import sqlalchemy


class TestConnectorFactory:
    """Tests for ConnectorFactory class."""

    def test_get_supported_databases(self):
        """Should return list of supported database types."""
        from terno_dbi.connectors import ConnectorFactory
        
        supported = ConnectorFactory.get_supported_databases()
        
        assert isinstance(supported, list)
        assert 'postgres' in supported
        assert 'mysql' in supported
        assert 'sqlite' in supported

    def test_create_postgres_connector(self):
        """Should create PostgresConnector for postgres type."""
        from terno_dbi.connectors import ConnectorFactory
        from terno_dbi.connectors.postgres import PostgresConnector
        
        connector = ConnectorFactory.create_connector(
            'postgres',
            'postgresql://localhost/test'
        )
        
        assert isinstance(connector, PostgresConnector)
        connector.close()

    def test_create_mysql_connector(self):
        """Should create MySQLConnector for mysql type."""
        from terno_dbi.connectors import ConnectorFactory
        from terno_dbi.connectors.mysql import MySQLConnector
        
        connector = ConnectorFactory.create_connector(
            'mysql',
            'mysql://localhost/test'
        )
        
        assert isinstance(connector, MySQLConnector)
        connector.close()

    def test_create_sqlite_connector(self):
        """Should create SQLiteConnector for sqlite type."""
        from terno_dbi.connectors import ConnectorFactory
        from terno_dbi.connectors.sqlite import SQLiteConnector
        
        connector = ConnectorFactory.create_connector(
            'sqlite',
            'sqlite:///:memory:'
        )
        
        assert isinstance(connector, SQLiteConnector)
        connector.close()

    def test_raises_for_unsupported_type(self):
        """Should raise UnsupportedDatabaseError for unknown type."""
        from terno_dbi.connectors import ConnectorFactory
        from terno_dbi.connectors.factory import UnsupportedDatabaseError
        
        with pytest.raises(UnsupportedDatabaseError):
            ConnectorFactory.create_connector('unknowndb', 'unknown://localhost')

    def test_case_insensitive_type(self):
        """Should handle database type case-insensitively."""
        from terno_dbi.connectors import ConnectorFactory
        from terno_dbi.connectors.sqlite import SQLiteConnector
        
        connector1 = ConnectorFactory.create_connector('SQLITE', 'sqlite:///:memory:')
        connector2 = ConnectorFactory.create_connector('SQLite', 'sqlite:///:memory:')
        
        assert isinstance(connector1, SQLiteConnector)
        assert isinstance(connector2, SQLiteConnector)
        connector1.close()
        connector2.close()

    def test_register_custom_connector(self):
        """Should allow registering custom connectors."""
        from terno_dbi.connectors import ConnectorFactory
        from terno_dbi.connectors.base import BaseConnector
        
        class CustomConnector(BaseConnector):
            def _create_engine(self):
                return MagicMock()
            def get_metadata(self):
                return MagicMock()
            def get_dialect_info(self):
                return ('custom', '1.0')
        
        ConnectorFactory.register_connector('custom', CustomConnector)
        connector = ConnectorFactory.create_connector('custom', 'custom://localhost')
        
        assert isinstance(connector, CustomConnector)

    def test_mask_connection_string(self):
        """Should mask passwords in connection strings."""
        from terno_dbi.connectors import ConnectorFactory
        
        conn_str = 'postgresql://user:secret_password@localhost/db'
        masked = ConnectorFactory._mask_connection_string(conn_str)
        
        assert 'secret_password' not in masked
        assert 'user' in masked
        assert '****' in masked
        assert '****' in masked

    def test_mask_connection_string_exception(self):
        """Should handle non-string input gracefully."""
        from terno_dbi.connectors import ConnectorFactory
        
        # Passing None or int might cause exception in _mask_connection_string string methods
        # and trigger the except block returning '***masked***'
        assert ConnectorFactory._mask_connection_string(None) == '***masked***'

    def test_lazy_registration_on_get_supported(self):
        """Should trigger registration if connectors empty."""
        from terno_dbi.connectors import ConnectorFactory
        
        # Clear existing
        ConnectorFactory._connectors = {}
        
        supported = ConnectorFactory.get_supported_databases()
        
        # detailed verification
        assert 'postgres' in supported
        assert ConnectorFactory._connectors  # Should be populated



class TestBaseConnector:
    """Tests for BaseConnector class."""

    def test_sqlite_connector_creates_engine(self):
        """SQLite connector should create a working engine."""
        from terno_dbi.connectors.sqlite import SQLiteConnector
        
        connector = SQLiteConnector('sqlite:///:memory:')
        engine = connector.get_engine()
        
        assert engine is not None
        assert isinstance(engine, sqlalchemy.Engine)
        connector.close()

    def test_get_connection_returns_connection(self):
        """get_connection should return a valid connection."""
        from terno_dbi.connectors.sqlite import SQLiteConnector
        
        connector = SQLiteConnector('sqlite:///:memory:')
        
        with connector.get_connection() as conn:
            result = conn.execute(sqlalchemy.text("SELECT 1"))
            assert result.fetchone()[0] == 1
        
        connector.close()

    def test_close_disposes_engine(self):
        """close() should dispose the engine."""
        from terno_dbi.connectors.sqlite import SQLiteConnector
        
        connector = SQLiteConnector('sqlite:///:memory:')
        _ = connector.get_engine()  # Create engine
        
        connector.close()
        
        assert connector._engine is None

    def test_context_manager_closes_connector(self):
        """Context manager should close connector on exit."""
        from terno_dbi.connectors.sqlite import SQLiteConnector
        
        with SQLiteConnector('sqlite:///:memory:') as connector:
            _ = connector.get_engine()
        
        assert connector._engine is None

    def test_pool_configuration(self):
        """Should accept pool configuration."""
        from terno_dbi.connectors.sqlite import SQLiteConnector
        
        connector = SQLiteConnector(
            'sqlite:///:memory:',
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            use_pool=True
        )
        
        assert connector.pool_size == 5
        assert connector.max_overflow == 10
        assert connector.pool_timeout == 30
        connector.close()

    def test_no_pool_option(self):
        """Should support disabling connection pooling."""
        from terno_dbi.connectors.sqlite import SQLiteConnector
        
        connector = SQLiteConnector('sqlite:///:memory:', use_pool=False)
        
        assert connector.use_pool is False
        connector.close()


class TestSQLiteConnector:
    """Tests specific to SQLite connector."""

    def test_dialect_info(self):
        """Should return correct dialect info."""
        from terno_dbi.connectors.sqlite import SQLiteConnector
        
        connector = SQLiteConnector('sqlite:///:memory:')
        dialect, version = connector.get_dialect_info()
        
        assert 'sqlite' in dialect.lower()
        connector.close()

    def test_get_metadata(self):
        """Should reflect database metadata."""
        from terno_dbi.connectors.sqlite import SQLiteConnector
        
        connector = SQLiteConnector('sqlite:///:memory:')
        
        # Create a table first
        with connector.get_connection() as conn:
            conn.execute(sqlalchemy.text(
                "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)"
            ))
            conn.commit()
        
        metadata = connector.get_metadata()
        
        # Metadata should be an MDatabase or similar structure
        assert metadata is not None
        connector.close()


class TestPostgresConnector:
    """Tests specific to PostgreSQL connector."""

    def test_creates_with_valid_connection_string(self):
        """Should initialize with valid connection string."""
        from terno_dbi.connectors.postgres import PostgresConnector
        
        connector = PostgresConnector('postgresql://localhost/test')
        
        assert connector.connection_string == 'postgresql://localhost/test'
        # Don't try to connect - just verify initialization

    @pytest.mark.skip(reason="Requires live PostgreSQL database")
    def test_dialect_info(self):
        """Should return postgres dialect."""
        from terno_dbi.connectors.postgres import PostgresConnector
        
        connector = PostgresConnector('postgresql://localhost/test')
        dialect, _ = connector.get_dialect_info()
        
        assert 'postgres' in dialect.lower() or 'postgresql' in dialect.lower()


class TestMySQLConnector:
    """Tests specific to MySQL connector."""

    def test_creates_with_valid_connection_string(self):
        """Should initialize with valid connection string."""
        from terno_dbi.connectors.mysql import MySQLConnector
        
        connector = MySQLConnector('mysql://localhost/test')
        
        assert connector.connection_string == 'mysql+pymysql://localhost/test'

    @pytest.mark.skip(reason="Requires live MySQL database")
    def test_dialect_info(self):
        """Should return mysql dialect."""
        from terno_dbi.connectors.mysql import MySQLConnector
        
        connector = MySQLConnector('mysql://localhost/test')
        dialect, _ = connector.get_dialect_info()
        
        assert 'mysql' in dialect.lower()


class TestExecuteWithRetry:
    """Tests for execute_with_retry function."""

    def test_succeeds_on_first_try(self):
        """Should return result on successful first try."""
        from terno_dbi.connectors.sqlite import SQLiteConnector
        
        connector = SQLiteConnector('sqlite:///:memory:')
        
        def success_func():
            return "success"
        
        result = connector.execute_with_retry(success_func)
        
        assert result == "success"
        connector.close()

    def test_retries_on_pool_timeout(self):
        """Should retry on pool timeout errors."""
        from terno_dbi.connectors.sqlite import SQLiteConnector
        from sqlalchemy.exc import TimeoutError as PoolTimeoutError
        
        connector = SQLiteConnector('sqlite:///:memory:')
        
        call_count = [0]
        
        def eventually_succeeds():
            call_count[0] += 1
            if call_count[0] < 2:
                raise PoolTimeoutError()
            return "success"
        
        result = connector.execute_with_retry(eventually_succeeds, max_retries=3)
        
        assert result == "success"
        assert call_count[0] == 2
        connector.close()

    def test_raises_non_retryable_errors(self):
        """Should raise non-retryable errors immediately."""
        from terno_dbi.connectors.sqlite import SQLiteConnector
        
        connector = SQLiteConnector('sqlite:///:memory:')
        
        def fails():
            raise ValueError("Not retryable")
        
        with pytest.raises(ValueError):
            connector.execute_with_retry(fails)
        
        connector.close()
