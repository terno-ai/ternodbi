"""
Comprehensive unit tests for the Database Connector Factory module.

Coverage includes:
- BaseConnector: initialization, engine caching, connection pooling, context managers
- ConnectorFactory: connector creation, registration, error handling
- All database connectors: Postgres, MySQL, Snowflake, BigQuery, Databricks, Oracle, SQLite
- Pool configuration: size, overflow, timeout, recycle
- Retry logic: exponential backoff
- Connection safety: get_connection context manager
- Metadata reflection for all connectors
- Dialect info for all connectors
"""

import unittest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.pool import QueuePool, NullPool

# Import test targets
from terno.connectors.base import (
    BaseConnector, 
    DEFAULT_POOL_SIZE, 
    DEFAULT_MAX_OVERFLOW,
    DEFAULT_POOL_TIMEOUT,
    DEFAULT_POOL_RECYCLE,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_BASE_DELAY
)
from terno.connectors.factory import ConnectorFactory, UnsupportedDatabaseError
from terno.connectors.postgres import PostgresConnector
from terno.connectors.mysql import MySQLConnector
from terno.connectors.snowflake import SnowflakeConnector
from terno.connectors.bigquery import BigQueryConnector
from terno.connectors.databricks import DatabricksConnector
from terno.connectors.oracle import OracleConnector
from terno.connectors.sqlite import SQLiteConnector


class TestConnectorFactory(unittest.TestCase):
    """Tests for ConnectorFactory class."""

    def test_create_postgres_connector(self):
        """Test creating a PostgreSQL connector."""
        connector = ConnectorFactory.create_connector(
            'postgres',
            'postgresql://user:pass@localhost/db'
        )
        self.assertIsInstance(connector, PostgresConnector)

    def test_create_postgresql_alias(self):
        """Test 'postgresql' alias maps to PostgresConnector."""
        connector = ConnectorFactory.create_connector(
            'postgresql',
            'postgresql://user:pass@localhost/db'
        )
        self.assertIsInstance(connector, PostgresConnector)

    def test_create_mysql_connector(self):
        """Test creating a MySQL connector."""
        connector = ConnectorFactory.create_connector(
            'mysql',
            'mysql+pymysql://user:pass@localhost/db'
        )
        self.assertIsInstance(connector, MySQLConnector)

    def test_create_snowflake_connector(self):
        """Test creating a Snowflake connector."""
        connector = ConnectorFactory.create_connector(
            'snowflake',
            'snowflake://user:pass@account/db/schema'
        )
        self.assertIsInstance(connector, SnowflakeConnector)

    def test_create_bigquery_connector_with_credentials(self):
        """Test creating a BigQuery connector with credentials."""
        credentials = {'type': 'service_account', 'project_id': 'test'}
        connector = ConnectorFactory.create_connector(
            'bigquery',
            'bigquery://project/dataset',
            credentials
        )
        self.assertIsInstance(connector, BigQueryConnector)

    def test_create_bigquery_without_credentials_raises_error(self):
        """Test BigQuery connector requires credentials."""
        with self.assertRaises(ValueError) as context:
            ConnectorFactory.create_connector(
                'bigquery',
                'bigquery://project/dataset'
            )
        self.assertIn('credentials', str(context.exception).lower())

    def test_create_databricks_connector(self):
        """Test creating a Databricks connector."""
        connector = ConnectorFactory.create_connector(
            'databricks',
            'databricks://token:xxx@host:443/schema'
        )
        self.assertIsInstance(connector, DatabricksConnector)

    def test_create_oracle_connector(self):
        """Test creating an Oracle connector."""
        connector = ConnectorFactory.create_connector(
            'oracle',
            'oracle+oracledb://user:pass@host/service'
        )
        self.assertIsInstance(connector, OracleConnector)

    def test_create_sqlite_connector(self):
        """Test creating an SQLite connector."""
        connector = ConnectorFactory.create_connector(
            'sqlite',
            'sqlite:///test.db'
        )
        self.assertIsInstance(connector, SQLiteConnector)

    def test_case_insensitive_db_type(self):
        """Test database type is case-insensitive."""
        connector1 = ConnectorFactory.create_connector('POSTGRES', 'postgresql://test')
        connector2 = ConnectorFactory.create_connector('Postgres', 'postgresql://test')
        connector3 = ConnectorFactory.create_connector('postgres', 'postgresql://test')
        
        self.assertIsInstance(connector1, PostgresConnector)
        self.assertIsInstance(connector2, PostgresConnector)
        self.assertIsInstance(connector3, PostgresConnector)

    def test_unsupported_database_raises_error(self):
        """Test unsupported database type raises UnsupportedDatabaseError."""
        with self.assertRaises(UnsupportedDatabaseError) as context:
            ConnectorFactory.create_connector('mongodb', 'mongodb://localhost')
        
        self.assertEqual(context.exception.db_type, 'mongodb')

    def test_get_supported_databases(self):
        """Test getting list of supported databases."""
        supported = ConnectorFactory.get_supported_databases()
        
        self.assertIn('postgres', supported)
        self.assertIn('mysql', supported)
        self.assertIn('snowflake', supported)
        self.assertIn('bigquery', supported)
        self.assertIn('databricks', supported)
        self.assertIn('oracle', supported)
        self.assertIn('sqlite', supported)

    def test_register_custom_connector(self):
        """Test registering a custom connector."""
        class CustomConnector(BaseConnector):
            def get_metadata(self):
                pass
            def get_dialect_info(self):
                pass
        
        ConnectorFactory.register_connector('custom', CustomConnector)
        connector = ConnectorFactory.create_connector('custom', 'custom://test')
        
        self.assertIsInstance(connector, CustomConnector)


class TestBaseConnector(unittest.TestCase):
    """Tests for BaseConnector base class."""

    def setUp(self):
        """Set up test fixtures."""
        self.connector = PostgresConnector('postgresql://user:pass@localhost/db')

    def test_initialization_defaults(self):
        """Test default pool configuration values."""
        self.assertEqual(self.connector.pool_size, DEFAULT_POOL_SIZE)
        self.assertEqual(self.connector.max_overflow, DEFAULT_MAX_OVERFLOW)
        self.assertEqual(self.connector.pool_timeout, DEFAULT_POOL_TIMEOUT)
        self.assertEqual(self.connector.pool_recycle, DEFAULT_POOL_RECYCLE)
        self.assertTrue(self.connector.use_pool)

    def test_initialization_custom_pool_settings(self):
        """Test custom pool configuration."""
        connector = PostgresConnector(
            'postgresql://test',
            pool_size=10,
            max_overflow=5,
            pool_timeout=30,
            pool_recycle=900,
            use_pool=False
        )
        
        self.assertEqual(connector.pool_size, 10)
        self.assertEqual(connector.max_overflow, 5)
        self.assertEqual(connector.pool_timeout, 30)
        self.assertEqual(connector.pool_recycle, 900)
        self.assertFalse(connector.use_pool)

    def test_engine_is_cached(self):
        """Test engine is created only once and cached."""
        with patch.object(self.connector, '_create_engine') as mock_create:
            mock_engine = Mock()
            mock_create.return_value = mock_engine
            
            engine1 = self.connector.get_engine()
            engine2 = self.connector.get_engine()
            
            # _create_engine should only be called once
            mock_create.assert_called_once()
            self.assertIs(engine1, engine2)

    def test_close_disposes_engine(self):
        """Test close() disposes the engine."""
        with patch.object(self.connector, '_create_engine') as mock_create:
            mock_engine = Mock()
            mock_create.return_value = mock_engine
            
            self.connector.get_engine()
            self.connector.close()
            
            mock_engine.dispose.assert_called_once()
            self.assertIsNone(self.connector._engine)

    def test_close_on_no_engine(self):
        """Test close() is safe when no engine exists."""
        self.connector.close()  # Should not raise

    def test_context_manager_closes_on_exit(self):
        """Test context manager calls close on exit."""
        with patch.object(self.connector, '_create_engine') as mock_create:
            mock_engine = Mock()
            mock_create.return_value = mock_engine
            
            with self.connector as conn:
                conn.get_engine()
            
            mock_engine.dispose.assert_called_once()

    def test_context_manager_closes_on_exception(self):
        """Test context manager closes even on exception."""
        with patch.object(self.connector, '_create_engine') as mock_create:
            mock_engine = Mock()
            mock_create.return_value = mock_engine
            
            try:
                with self.connector as conn:
                    conn.get_engine()
                    raise ValueError("Test error")
            except ValueError:
                pass
            
            mock_engine.dispose.assert_called_once()


class TestGetConnection(unittest.TestCase):
    """Tests for get_connection context manager."""

    def test_get_connection_returns_connection(self):
        """Test get_connection yields a connection."""
        connector = PostgresConnector('postgresql://test')
        
        with patch.object(connector, 'get_engine') as mock_get_engine:
            mock_engine = Mock()
            mock_connection = Mock()
            mock_engine.connect.return_value = mock_connection
            mock_get_engine.return_value = mock_engine
            
            with connector.get_connection() as conn:
                self.assertIs(conn, mock_connection)

    def test_get_connection_closes_on_exit(self):
        """Test connection is closed after context exits."""
        connector = PostgresConnector('postgresql://test')
        
        with patch.object(connector, 'get_engine') as mock_get_engine:
            mock_engine = Mock()
            mock_connection = Mock()
            mock_engine.connect.return_value = mock_connection
            mock_get_engine.return_value = mock_engine
            
            with connector.get_connection():
                pass
            
            mock_connection.close.assert_called_once()

    def test_get_connection_closes_on_exception(self):
        """Test connection is closed even on exception."""
        connector = PostgresConnector('postgresql://test')
        
        with patch.object(connector, 'get_engine') as mock_get_engine:
            mock_engine = Mock()
            mock_connection = Mock()
            mock_engine.connect.return_value = mock_connection
            mock_get_engine.return_value = mock_engine
            
            try:
                with connector.get_connection():
                    raise RuntimeError("Test error")
            except RuntimeError:
                pass
            
            mock_connection.close.assert_called_once()


class TestPoolConfiguration(unittest.TestCase):
    """Tests for connection pool configuration."""

    @patch('terno.connectors.base.sqlalchemy.create_engine')
    def test_pool_enabled_uses_queue_pool(self, mock_create_engine):
        """Test pool enabled uses QueuePool."""
        connector = PostgresConnector('postgresql://test', use_pool=True)
        connector.get_engine()
        
        call_kwargs = mock_create_engine.call_args[1]
        self.assertEqual(call_kwargs['poolclass'], QueuePool)

    @patch('terno.connectors.base.sqlalchemy.create_engine')
    def test_pool_disabled_uses_null_pool(self, mock_create_engine):
        """Test pool disabled uses NullPool."""
        connector = PostgresConnector('postgresql://test', use_pool=False)
        connector.get_engine()
        
        call_kwargs = mock_create_engine.call_args[1]
        self.assertEqual(call_kwargs['poolclass'], NullPool)

    @patch('terno.connectors.base.sqlalchemy.create_engine')
    def test_pool_size_passed_to_engine(self, mock_create_engine):
        """Test pool_size is passed to create_engine."""
        connector = PostgresConnector('postgresql://test', pool_size=15)
        connector.get_engine()
        
        call_kwargs = mock_create_engine.call_args[1]
        self.assertEqual(call_kwargs['pool_size'], 15)

    @patch('terno.connectors.base.sqlalchemy.create_engine')
    def test_pool_pre_ping_enabled(self, mock_create_engine):
        """Test pool_pre_ping is enabled."""
        connector = PostgresConnector('postgresql://test')
        connector.get_engine()
        
        call_kwargs = mock_create_engine.call_args[1]
        self.assertTrue(call_kwargs['pool_pre_ping'])


class TestRetryLogic(unittest.TestCase):
    """Tests for execute_with_retry with exponential backoff."""

    def test_successful_execution_no_retry(self):
        """Test successful execution doesn't retry."""
        connector = PostgresConnector('postgresql://test')
        mock_func = Mock(return_value='success')
        
        result = connector.execute_with_retry(mock_func)
        
        self.assertEqual(result, 'success')
        mock_func.assert_called_once()

    @patch('terno.connectors.base.time.sleep')
    def test_retry_on_pool_timeout(self, mock_sleep):
        """Test retry on pool timeout."""
        connector = PostgresConnector('postgresql://test')
        mock_func = Mock(side_effect=[PoolTimeoutError(), 'success'])
        
        result = connector.execute_with_retry(mock_func)
        
        self.assertEqual(result, 'success')
        self.assertEqual(mock_func.call_count, 2)
        mock_sleep.assert_called_once_with(1)  # First retry delay

    @patch('terno.connectors.base.time.sleep')
    def test_exponential_backoff_delays(self, mock_sleep):
        """Test exponential backoff delay pattern: 1s, 2s, 4s."""
        connector = PostgresConnector('postgresql://test')
        mock_func = Mock(side_effect=[
            PoolTimeoutError(),
            PoolTimeoutError(),
            PoolTimeoutError()
        ])
        
        with self.assertRaises(PoolTimeoutError):
            connector.execute_with_retry(mock_func, max_retries=3)
        
        # Check delays: 1, 2, 4 (exponential)
        delays = [call[0][0] for call in mock_sleep.call_args_list]
        self.assertEqual(delays, [1, 2, 4])

    def test_non_timeout_error_not_retried(self):
        """Test non-timeout errors are not retried."""
        connector = PostgresConnector('postgresql://test')
        mock_func = Mock(side_effect=ValueError("Not a timeout"))
        
        with self.assertRaises(ValueError):
            connector.execute_with_retry(mock_func)
        
        mock_func.assert_called_once()

    @patch('terno.connectors.base.time.sleep')
    def test_max_retries_exhausted(self, mock_sleep):
        """Test raises after max retries exhausted."""
        connector = PostgresConnector('postgresql://test')
        mock_func = Mock(side_effect=PoolTimeoutError())
        
        with self.assertRaises(PoolTimeoutError):
            connector.execute_with_retry(mock_func, max_retries=2)
        
        self.assertEqual(mock_func.call_count, 2)


class TestDatabricksConnector(unittest.TestCase):
    """Tests specific to DatabricksConnector."""

    def test_schema_extracted_from_url(self):
        """Test schema is extracted from connection string."""
        connector = DatabricksConnector('databricks://token:xxx@host:443/my_schema')
        self.assertEqual(connector._schema, 'my_schema')

    def test_default_schema_when_not_specified(self):
        """Test default schema when not in URL."""
        connector = DatabricksConnector('databricks://token:xxx@host:443')
        self.assertEqual(connector._schema, 'default')

    @patch('terno.connectors.databricks.MDatabase')
    @patch('terno.connectors.databricks.inspect')
    def test_get_metadata_uses_safe_reflect(self, mock_inspect, mock_mdb):
        """Test get_metadata uses safe reflection."""
        connector = DatabricksConnector('databricks://token:xxx@host:443/test_schema')
        
        # Mock engine
        mock_engine = Mock()
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            # Mock inspector
            mock_inspector = Mock()
            mock_inspector.get_table_names.return_value = ['table1', 'table2']
            mock_inspect.return_value = mock_inspector
            
            # Mock MDatabase
            mock_mdb.from_inspector.return_value = Mock()
            
            connector.get_metadata()
            
            mock_mdb.from_inspector.assert_called_once()

    @patch('terno.connectors.databricks.inspect')
    def test_safe_reflect_handles_table_errors(self, mock_inspect):
        """Test safe reflection continues on individual table errors."""
        connector = DatabricksConnector('databricks://token:xxx@host:443/schema')
        
        mock_engine = Mock()
        mock_inspector = Mock()
        mock_inspector.get_table_names.return_value = ['good_table', 'bad_table']
        mock_inspect.return_value = mock_inspector
        
        # Mock reflect to fail on one table
        def reflect_side_effect(**kwargs):
            if kwargs.get('only') == ['bad_table']:
                raise Exception("Table error")
        
        with patch('terno.connectors.databricks.MetaData') as mock_metadata_class:
            mock_metadata = Mock()
            mock_metadata.reflect.side_effect = reflect_side_effect
            mock_metadata_class.return_value = mock_metadata
            
            # Should not raise - continues to next table
            result = connector._safe_reflect_metadata(mock_engine, 'schema')
            
            # Both tables should have been attempted
            self.assertEqual(mock_metadata.reflect.call_count, 2)

    @patch('terno.connectors.databricks.inspect')
    def test_safe_reflect_handles_inspector_error(self, mock_inspect):
        """Test safe reflection handles inspector errors."""
        connector = DatabricksConnector('databricks://token:xxx@host:443/schema')
        
        mock_engine = Mock()
        mock_inspector = Mock()
        mock_inspector.get_table_names.side_effect = Exception("Inspector error")
        mock_inspect.return_value = mock_inspector
        
        with patch('terno.connectors.databricks.MetaData') as mock_metadata_class:
            mock_metadata = Mock()
            mock_metadata_class.return_value = mock_metadata
            
            # Should not raise
            result = connector._safe_reflect_metadata(mock_engine, 'schema')
            
            self.assertIsNotNone(result)

    def test_get_dialect_info(self):
        """Test get_dialect_info returns correct values."""
        connector = DatabricksConnector('databricks://test')
        
        mock_engine = Mock()
        mock_engine.dialect.name = 'databricks'
        mock_engine.dialect.server_version_info = (1, 0, 0)
        mock_engine.connect.return_value.__enter__ = Mock()
        mock_engine.connect.return_value.__exit__ = Mock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            dialect_name, dialect_version = connector.get_dialect_info()
            
        self.assertEqual(dialect_name, 'databricks')
        self.assertEqual(dialect_version, '(1, 0, 0)')


class TestSQLiteConnector(unittest.TestCase):
    """Tests specific to SQLiteConnector."""

    def test_pool_disabled_by_default(self):
        """Test SQLite has pool disabled by default."""
        connector = SQLiteConnector('sqlite:///test.db')
        self.assertFalse(connector.use_pool)

    @patch('terno.connectors.sqlite.MDatabase')
    def test_get_metadata(self, mock_mdb):
        """Test get_metadata works correctly."""
        connector = SQLiteConnector('sqlite:///test.db')
        
        mock_engine = Mock()
        mock_metadata = Mock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, '_reflect_metadata', return_value=mock_metadata):
                mock_mdb.from_inspector.return_value = Mock()
                
                connector.get_metadata()
                
                mock_mdb.from_inspector.assert_called_once_with(mock_metadata)

    def test_get_dialect_info(self):
        """Test get_dialect_info for SQLite."""
        connector = SQLiteConnector('sqlite:///test.db')
        
        mock_engine = Mock()
        mock_engine.dialect.name = 'sqlite'
        mock_engine.dialect.server_version_info = (3, 36, 0)
        mock_engine.connect.return_value.__enter__ = Mock()
        mock_engine.connect.return_value.__exit__ = Mock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            dialect_name, dialect_version = connector.get_dialect_info()
            
        self.assertEqual(dialect_name, 'sqlite')
        self.assertEqual(dialect_version, '(3, 36, 0)')


class TestPostgresConnector(unittest.TestCase):
    """Tests specific to PostgresConnector."""

    def test_dialect_name_normalized(self):
        """Test 'postgresql' is normalized to 'postgres'."""
        connector = PostgresConnector('postgresql://test')
        
        mock_engine = Mock()
        mock_engine.dialect.name = 'postgresql'
        mock_engine.dialect.server_version_info = (13, 0, 0)
        mock_engine.connect.return_value.__enter__ = Mock()
        mock_engine.connect.return_value.__exit__ = Mock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            dialect_name, dialect_version = connector.get_dialect_info()
            
        self.assertEqual(dialect_name, 'postgres')

    @patch('terno.connectors.postgres.MDatabase')
    def test_get_metadata(self, mock_mdb):
        """Test get_metadata works correctly."""
        connector = PostgresConnector('postgresql://test')
        
        mock_engine = Mock()
        mock_metadata = Mock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, '_reflect_metadata', return_value=mock_metadata):
                mock_mdb.from_inspector.return_value = Mock()
                
                connector.get_metadata()
                
                mock_mdb.from_inspector.assert_called_once_with(mock_metadata)


class TestMySQLConnector(unittest.TestCase):
    """Tests specific to MySQLConnector."""

    @patch('terno.connectors.mysql.MDatabase')
    def test_get_metadata(self, mock_mdb):
        """Test get_metadata works correctly."""
        connector = MySQLConnector('mysql+pymysql://test')
        
        mock_engine = Mock()
        mock_metadata = Mock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, '_reflect_metadata', return_value=mock_metadata):
                mock_mdb.from_inspector.return_value = Mock()
                
                connector.get_metadata()
                
                mock_mdb.from_inspector.assert_called_once_with(mock_metadata)

    def test_get_dialect_info(self):
        """Test get_dialect_info for MySQL."""
        connector = MySQLConnector('mysql+pymysql://test')
        
        mock_engine = Mock()
        mock_engine.dialect.name = 'mysql'
        mock_engine.dialect.server_version_info = (8, 0, 0)
        mock_engine.connect.return_value.__enter__ = Mock()
        mock_engine.connect.return_value.__exit__ = Mock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            dialect_name, dialect_version = connector.get_dialect_info()
            
        self.assertEqual(dialect_name, 'mysql')
        self.assertEqual(dialect_version, '(8, 0, 0)')


class TestOracleConnector(unittest.TestCase):
    """Tests specific to OracleConnector."""

    @patch('terno.connectors.oracle.MDatabase')
    def test_get_metadata(self, mock_mdb):
        """Test get_metadata works correctly."""
        connector = OracleConnector('oracle+oracledb://test')
        
        mock_engine = Mock()
        mock_metadata = Mock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, '_reflect_metadata', return_value=mock_metadata):
                mock_mdb.from_inspector.return_value = Mock()
                
                connector.get_metadata()
                
                mock_mdb.from_inspector.assert_called_once_with(mock_metadata)

    def test_get_dialect_info(self):
        """Test get_dialect_info for Oracle."""
        connector = OracleConnector('oracle+oracledb://test')
        
        mock_engine = Mock()
        mock_engine.dialect.name = 'oracle'
        mock_engine.dialect.server_version_info = (19, 0, 0)
        mock_engine.connect.return_value.__enter__ = Mock()
        mock_engine.connect.return_value.__exit__ = Mock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            dialect_name, dialect_version = connector.get_dialect_info()
            
        self.assertEqual(dialect_name, 'oracle')
        self.assertEqual(dialect_version, '(19, 0, 0)')


class TestSnowflakeConnector(unittest.TestCase):
    """Tests specific to SnowflakeConnector."""

    @patch('terno.connectors.snowflake.MDatabase')
    def test_get_metadata_uses_snowflake_dialect(self, mock_mdb):
        """Test get_metadata uses from_snowflake_dialect."""
        connector = SnowflakeConnector('snowflake://test')
        
        mock_engine = Mock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            mock_mdb.from_snowflake_dialect.return_value = Mock()
            
            connector.get_metadata()
            
            # Should use from_snowflake_dialect, NOT from_inspector
            mock_mdb.from_snowflake_dialect.assert_called_once_with(mock_engine)

    def test_get_dialect_info(self):
        """Test get_dialect_info for Snowflake."""
        connector = SnowflakeConnector('snowflake://test')
        
        mock_engine = Mock()
        mock_engine.dialect.name = 'snowflake'
        mock_engine.dialect.server_version_info = (7, 0, 0)
        mock_engine.connect.return_value.__enter__ = Mock()
        mock_engine.connect.return_value.__exit__ = Mock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            dialect_name, dialect_version = connector.get_dialect_info()
            
        self.assertEqual(dialect_name, 'snowflake')
        self.assertEqual(dialect_version, '(7, 0, 0)')


class TestBigQueryConnector(unittest.TestCase):
    """Tests specific to BigQueryConnector."""

    def test_requires_credentials(self):
        """Test BigQuery requires credentials."""
        with self.assertRaises(ValueError):
            BigQueryConnector('bigquery://project/dataset')

    def test_credentials_stored(self):
        """Test credentials are stored."""
        creds = {'type': 'service_account', 'project_id': 'test'}
        connector = BigQueryConnector('bigquery://project/dataset', creds)
        
        self.assertEqual(connector.credentials, creds)

    @patch('terno.connectors.bigquery.sqlalchemy.create_engine')
    def test_create_engine_passes_credentials(self, mock_create_engine):
        """Test _create_engine passes credentials to SQLAlchemy."""
        creds = {'type': 'service_account', 'project_id': 'test'}
        connector = BigQueryConnector('bigquery://project/dataset', creds)
        
        connector.get_engine()
        
        call_kwargs = mock_create_engine.call_args[1]
        self.assertEqual(call_kwargs['credentials_info'], creds)

    @patch('terno.connectors.bigquery.sqlalchemy.create_engine')
    def test_create_engine_with_pool(self, mock_create_engine):
        """Test _create_engine uses QueuePool when enabled."""
        creds = {'type': 'service_account'}
        connector = BigQueryConnector('bigquery://test', creds, use_pool=True)
        
        connector.get_engine()
        
        call_kwargs = mock_create_engine.call_args[1]
        self.assertEqual(call_kwargs['poolclass'], QueuePool)

    @patch('terno.connectors.bigquery.sqlalchemy.create_engine')
    def test_create_engine_without_pool(self, mock_create_engine):
        """Test _create_engine uses NullPool when disabled."""
        creds = {'type': 'service_account'}
        connector = BigQueryConnector('bigquery://test', creds, use_pool=False)
        
        connector.get_engine()
        
        call_kwargs = mock_create_engine.call_args[1]
        self.assertEqual(call_kwargs['poolclass'], NullPool)

    @patch('terno.connectors.bigquery.MDatabase')
    def test_get_metadata(self, mock_mdb):
        """Test get_metadata works correctly."""
        creds = {'type': 'service_account'}
        connector = BigQueryConnector('bigquery://test', creds)
        
        mock_engine = Mock()
        mock_metadata = Mock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            with patch.object(connector, '_reflect_metadata', return_value=mock_metadata):
                mock_mdb.from_inspector.return_value = Mock()
                
                connector.get_metadata()
                
                mock_mdb.from_inspector.assert_called_once_with(mock_metadata)

    def test_get_dialect_info(self):
        """Test get_dialect_info for BigQuery."""
        creds = {'type': 'service_account'}
        connector = BigQueryConnector('bigquery://test', creds)
        
        mock_engine = Mock()
        mock_engine.dialect.name = 'bigquery'
        mock_engine.dialect.server_version_info = None
        type(mock_engine.dialect).server_version_info = PropertyMock(return_value=None)
        mock_engine.connect.return_value.__enter__ = Mock()
        mock_engine.connect.return_value.__exit__ = Mock()
        
        with patch.object(connector, 'get_engine', return_value=mock_engine):
            dialect_name, dialect_version = connector.get_dialect_info()
            
        self.assertEqual(dialect_name, 'bigquery')


class TestConnectionStringMasking(unittest.TestCase):
    """Tests for connection string masking in logs."""

    def test_password_masked_in_logs(self):
        """Test password is masked in factory logs."""
        masked = ConnectorFactory._mask_connection_string(
            'postgresql://user:secretpassword@localhost/db'
        )
        self.assertIn('****', masked)
        self.assertNotIn('secretpassword', masked)

    def test_user_preserved_in_masked_string(self):
        """Test username is preserved in masked string."""
        masked = ConnectorFactory._mask_connection_string(
            'postgresql://myuser:secretpassword@localhost/db'
        )
        self.assertIn('myuser', masked)

    def test_masking_handles_no_password(self):
        """Test masking handles connection without password."""
        masked = ConnectorFactory._mask_connection_string(
            'sqlite:///test.db'
        )
        self.assertIsNotNone(masked)

    def test_masking_handles_malformed_url(self):
        """Test masking handles malformed URLs gracefully."""
        masked = ConnectorFactory._mask_connection_string(
            'not_a_valid_url'
        )
        self.assertIsNotNone(masked)

    def test_masking_truncates_long_strings(self):
        """Test masking truncates very long connection strings."""
        long_conn = 'postgresql://user:pass@localhost/' + 'a' * 100
        masked = ConnectorFactory._mask_connection_string(long_conn)
        # Should be truncated or masked
        self.assertIsNotNone(masked)


class TestReflectMetadata(unittest.TestCase):
    """Tests for _reflect_metadata base method."""

    @patch('terno.connectors.base.sqlalchemy.MetaData')
    def test_reflect_metadata_with_schema(self, mock_metadata_class):
        """Test _reflect_metadata passes schema correctly."""
        connector = PostgresConnector('postgresql://test')
        
        mock_metadata = Mock()
        mock_metadata.tables = {'table1': Mock(), 'table2': Mock()}  # Must have len()
        mock_metadata_class.return_value = mock_metadata
        mock_engine = Mock()
        
        connector._reflect_metadata(mock_engine, schema='my_schema')
        
        mock_metadata_class.assert_called_once_with(schema='my_schema')
        mock_metadata.reflect.assert_called_once_with(bind=mock_engine)

    @patch('terno.connectors.base.sqlalchemy.MetaData')
    def test_reflect_metadata_returns_metadata(self, mock_metadata_class):
        """Test _reflect_metadata returns the metadata object."""
        connector = PostgresConnector('postgresql://test')
        
        mock_metadata = Mock()
        mock_metadata.tables = {'table1': Mock(), 'table2': Mock()}
        mock_metadata_class.return_value = mock_metadata
        mock_engine = Mock()
        
        result = connector._reflect_metadata(mock_engine)
        
        self.assertEqual(result, mock_metadata)


class TestGenericConnectorMapping(unittest.TestCase):
    """Tests for 'generic' type which maps to SQLiteConnector."""

    def test_create_generic_connector_via_factory(self):
        """Test creating a 'generic' connector returns SQLiteConnector."""
        connector = ConnectorFactory.create_connector(
            'generic',
            'sqlite:///test.db'
        )
        self.assertIsInstance(connector, SQLiteConnector)

    def test_generic_and_sqlite_return_same_type(self):
        """Test 'generic' and 'sqlite' both return SQLiteConnector."""
        generic = ConnectorFactory.create_connector('generic', 'sqlite:///test.db')
        sqlite = ConnectorFactory.create_connector('sqlite', 'sqlite:///test.db')
        
        self.assertIsInstance(generic, SQLiteConnector)
        self.assertIsInstance(sqlite, SQLiteConnector)
        self.assertEqual(type(generic), type(sqlite))


if __name__ == '__main__':
    unittest.main()
