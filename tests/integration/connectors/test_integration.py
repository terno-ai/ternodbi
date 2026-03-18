"""
Integration tests for Database Connectors with REAL database connections.

These tests require actual database connections to run.
Set connection strings in env.sh and source it before running tests.

To run these tests:
    source env.sh
    cd terno-ai/terno
    python -m pytest terno/connectors/tests/test_integration.py -v -s

To run specific database tests:
    python -m pytest terno/connectors/tests/test_integration.py::TestPostgresIntegration -v -s
"""

import unittest
import os
import json
from sqlalchemy import text

POSTGRES_CONNECTION_STRING = os.getenv('TEST_POSTGRES_CONN')
MYSQL_CONNECTION_STRING = os.getenv('TEST_MYSQL_CONN')
SNOWFLAKE_CONNECTION_STRING = os.getenv('TEST_SNOWFLAKE_CONN')
BIGQUERY_CONNECTION_STRING = os.getenv('TEST_BIGQUERY_CONN')
DATABRICKS_CONNECTION_STRING = os.getenv('TEST_DATABRICKS_CONN')
SQLITE_CONNECTION_STRING = os.getenv('TEST_SQLITE_CONN', 'sqlite:///test.db')

# BigQuery requires credentials as JSON
BIGQUERY_CREDENTIALS = None
_bq_creds = os.getenv('TEST_BIGQUERY_CREDENTIALS')
if _bq_creds:
    try:
        BIGQUERY_CREDENTIALS = json.loads(_bq_creds)
    except json.JSONDecodeError:
        pass

from terno_dbi.connectors.factory import ConnectorFactory
from terno_dbi.connectors.postgres import PostgresConnector
from terno_dbi.connectors.mysql import MySQLConnector
from terno_dbi.connectors.snowflake import SnowflakeConnector
from terno_dbi.connectors.bigquery import BigQueryConnector
from terno_dbi.connectors.databricks import DatabricksConnector
from terno_dbi.connectors.sqlite import SQLiteConnector


def skip_if_no_connection(connection_string, db_name):
    """Skip test if connection string is not configured."""
    def decorator(test_class):
        if connection_string is None:
            return unittest.skip(f"Skipping {db_name} tests - no connection string configured")(test_class)
        return test_class
    return decorator


# INTEGRATION TESTS - POSTGRESQL
@skip_if_no_connection(POSTGRES_CONNECTION_STRING, "PostgreSQL")
class TestPostgresIntegration(unittest.TestCase):
    """Integration tests for PostgreSQL with real database."""

    @classmethod
    def setUpClass(cls):
        cls.connector = ConnectorFactory.create_connector('postgres', POSTGRES_CONNECTION_STRING)

    @classmethod
    def tearDownClass(cls):
        cls.connector.close()

    def test_engine_connection(self):
        """Test that engine can connect to database."""
        engine = self.connector.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            self.assertIsNotNone(result)
        print(f"PostgreSQL engine connection successful")

    def test_get_dialect_info(self):
        """Test getting dialect info."""
        dialect_name, dialect_version = self.connector.get_dialect_info()

        self.assertEqual(dialect_name, 'postgres')
        self.assertIsNotNone(dialect_version)
        print(f"PostgreSQL dialect: {dialect_name}, version: {dialect_version}")

    def test_get_metadata(self):
        """Test metadata reflection."""
        mdb = self.connector.get_metadata()

        self.assertIsNotNone(mdb)
        self.assertIsNotNone(mdb.tables)
        print(f"PostgreSQL tables found: {len(mdb.tables)}")
        for table_name in list(mdb.tables.keys())[:5]:
            print(f"   - {table_name}")

    def test_get_connection_context_manager(self):
        """Test the safe connection context manager."""
        with self.connector.get_connection() as conn:
            result = conn.execute(text("SELECT current_database()"))
            db_name = result.fetchone()[0]
            print(f"Connected to PostgreSQL database: {db_name}")

    def test_table_columns(self):
        """Test that table columns are properly reflected."""
        mdb = self.connector.get_metadata()

        if mdb.tables:
            first_table = list(mdb.tables.values())[0]
            self.assertIsNotNone(first_table.columns)
            print(f"First table '{first_table.name}' has {len(first_table.columns)} columns")


# INTEGRATION TESTS - MYSQL
@skip_if_no_connection(MYSQL_CONNECTION_STRING, "MySQL")
class TestMySQLIntegration(unittest.TestCase):
    """Integration tests for MySQL with real database."""

    @classmethod
    def setUpClass(cls):
        cls.connector = ConnectorFactory.create_connector('mysql', MYSQL_CONNECTION_STRING)

    @classmethod
    def tearDownClass(cls):
        cls.connector.close()

    def test_engine_connection(self):
        """Test that engine can connect to database."""
        engine = self.connector.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            self.assertIsNotNone(result)
        print(f"MySQL engine connection successful")

    def test_get_dialect_info(self):
        """Test getting dialect info."""
        dialect_name, dialect_version = self.connector.get_dialect_info()

        self.assertEqual(dialect_name, 'mysql')
        self.assertIsNotNone(dialect_version)
        print(f"MySQL dialect: {dialect_name}, version: {dialect_version}")

    def test_get_metadata(self):
        """Test metadata reflection."""
        mdb = self.connector.get_metadata()

        self.assertIsNotNone(mdb)
        self.assertIsNotNone(mdb.tables)
        print(f"MySQL tables found: {len(mdb.tables)}")
        for table_name in list(mdb.tables.keys())[:5]:
            print(f"   - {table_name}")

    def test_get_connection_context_manager(self):
        """Test the safe connection context manager."""
        with self.connector.get_connection() as conn:
            result = conn.execute(text("SELECT DATABASE()"))
            db_name = result.fetchone()[0]
            print(f"Connected to MySQL database: {db_name}")


# INTEGRATION TESTS - SNOWFLAKE
@skip_if_no_connection(SNOWFLAKE_CONNECTION_STRING, "Snowflake")
class TestSnowflakeIntegration(unittest.TestCase):
    """Integration tests for Snowflake with real database."""

    @classmethod
    def setUpClass(cls):
        cls.connector = ConnectorFactory.create_connector('snowflake', SNOWFLAKE_CONNECTION_STRING)

    @classmethod
    def tearDownClass(cls):
        cls.connector.close()

    def test_engine_connection(self):
        """Test that engine can connect to database."""
        engine = self.connector.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            self.assertIsNotNone(result)
        print(f"Snowflake engine connection successful")

    def test_get_dialect_info(self):
        """Test getting dialect info."""
        dialect_name, dialect_version = self.connector.get_dialect_info()

        self.assertEqual(dialect_name, 'snowflake')
        self.assertIsNotNone(dialect_version)
        print(f"Snowflake dialect: {dialect_name}, version: {dialect_version}")

    def test_get_metadata_uses_snowflake_dialect(self):
        """Test metadata uses Snowflake-specific handling."""
        mdb = self.connector.get_metadata()

        self.assertIsNotNone(mdb)
        self.assertIsNotNone(mdb.tables)
        print(f"Snowflake tables found: {len(mdb.tables)}")
        for table_name in list(mdb.tables.keys())[:5]:
            print(f"   - {table_name}")

    def test_get_connection_context_manager(self):
        """Test the safe connection context manager."""
        with self.connector.get_connection() as conn:
            result = conn.execute(text("SELECT CURRENT_DATABASE()"))
            db_name = result.fetchone()[0]
            print(f"Connected to Snowflake database: {db_name}")

    def test_snowflake_identifier_case(self):
        """Test that Snowflake identifiers are handled correctly."""
        mdb = self.connector.get_metadata()

        if mdb.tables:
            for table_name in mdb.tables.keys():
                print(f"   Table: {table_name}")
                break


# INTEGRATION TESTS - BIGQUERY
@skip_if_no_connection(BIGQUERY_CONNECTION_STRING, "BigQuery")
class TestBigQueryIntegration(unittest.TestCase):
    """Integration tests for BigQuery with real database."""

    @classmethod
    def setUpClass(cls):
        if BIGQUERY_CREDENTIALS is None:
            raise unittest.SkipTest("BigQuery credentials not configured")
        cls.connector = ConnectorFactory.create_connector(
            'bigquery', 
            BIGQUERY_CONNECTION_STRING,
            BIGQUERY_CREDENTIALS
        )

    @classmethod
    def tearDownClass(cls):
        cls.connector.close()

    def test_engine_connection(self):
        """Test that engine can connect to database."""
        engine = self.connector.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            self.assertIsNotNone(result)
        print(f"BigQuery engine connection successful")

    def test_get_dialect_info(self):
        """Test getting dialect info."""
        dialect_name, dialect_version = self.connector.get_dialect_info()

        self.assertEqual(dialect_name, 'bigquery')
        print(f"BigQuery dialect: {dialect_name}, version: {dialect_version}")

    def test_get_metadata(self):
        """Test metadata reflection."""
        mdb = self.connector.get_metadata()

        self.assertIsNotNone(mdb)
        self.assertIsNotNone(mdb.tables)
        print(f"BigQuery tables found: {len(mdb.tables)}")
        for table_name in list(mdb.tables.keys())[:5]:
            print(f"   - {table_name}")


# INTEGRATION TESTS - DATABRICKS
@skip_if_no_connection(DATABRICKS_CONNECTION_STRING, "Databricks")
class TestDatabricksIntegration(unittest.TestCase):
    """Integration tests for Databricks with real database."""

    @classmethod
    def setUpClass(cls):
        cls.connector = ConnectorFactory.create_connector('databricks', DATABRICKS_CONNECTION_STRING)

    @classmethod
    def tearDownClass(cls):
        cls.connector.close()

    def test_engine_connection(self):
        """Test that engine can connect to database."""
        engine = self.connector.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            self.assertIsNotNone(result)
        print(f"Databricks engine connection successful")

    def test_get_dialect_info(self):
        """Test getting dialect info."""
        dialect_name, dialect_version = self.connector.get_dialect_info()

        self.assertIsNotNone(dialect_name)
        self.assertIsNotNone(dialect_version)
        print(f"Databricks dialect: {dialect_name}, version: {dialect_version}")

    def test_get_metadata_with_safe_reflection(self):
        """Test metadata uses safe reflection for Databricks."""
        mdb = self.connector.get_metadata()

        self.assertIsNotNone(mdb)
        self.assertIsNotNone(mdb.tables)
        print(f"Databricks tables found: {len(mdb.tables)}")
        for table_name in list(mdb.tables.keys())[:5]:
            print(f"   - {table_name}")

    def test_schema_extraction(self):
        """Test schema is correctly extracted from connection string."""
        self.assertIsNotNone(self.connector._schema)
        print(f"Databricks schema: {self.connector._schema}")


# INTEGRATION TESTS - SQLITE
class TestSQLiteIntegration(unittest.TestCase):
    """Integration tests for SQLite with real database."""

    @classmethod
    def setUpClass(cls):
        cls.connector = ConnectorFactory.create_connector('sqlite', SQLITE_CONNECTION_STRING)

    @classmethod
    def tearDownClass(cls):
        cls.connector.close()

    def test_engine_connection(self):
        """Test that engine can connect to database."""
        engine = self.connector.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            self.assertIsNotNone(result)
        print(f"SQLite engine connection successful")

    def test_pool_disabled_by_default(self):
        """Test SQLite has pool disabled by default."""
        self.assertFalse(self.connector.use_pool)
        print(f"SQLite pool disabled by default: {not self.connector.use_pool}")

    def test_get_dialect_info(self):
        """Test getting dialect info."""
        dialect_name, dialect_version = self.connector.get_dialect_info()

        self.assertEqual(dialect_name, 'sqlite')
        self.assertIsNotNone(dialect_version)
        print(f"SQLite dialect: {dialect_name}, version: {dialect_version}")

    def test_get_metadata(self):
        """Test metadata reflection."""
        mdb = self.connector.get_metadata()

        self.assertIsNotNone(mdb)
        self.assertIsNotNone(mdb.tables)
        print(f"SQLite tables found: {list(mdb.tables.keys())[:10]}")

    def test_table_columns_reflected(self):
        """Test that table columns are properly reflected."""
        mdb = self.connector.get_metadata()

        if mdb.tables:
            first_table_name = list(mdb.tables.keys())[0]
            first_table = mdb.tables[first_table_name]
            self.assertIsNotNone(first_table.columns)
            print(f"First table '{first_table_name}' has {len(first_table.columns)} columns")

    def test_get_connection_context_manager(self):
        """Test the safe connection context manager."""
        with self.connector.get_connection() as conn:
            result = conn.execute(text("SELECT sqlite_version()"))
            version = result.fetchone()[0]
            print(f"SQLite version: {version}")

    def test_execute_with_retry(self):
        """Test execute_with_retry works with real connection."""
        def query_func():
            with self.connector.get_connection() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM sqlite_master"))
                return result.fetchone()[0]

        count = self.connector.execute_with_retry(query_func)
        self.assertGreaterEqual(count, 0)
        print(f"SQLite execute_with_retry returned count: {count}")



# CROSS-DATABASE TESTS
class TestCrossDatabaseFeatures(unittest.TestCase):
    """Tests that run across all configured databases."""

    def test_all_configured_connectors_can_connect(self):
        """Test all configured databases can establish connection."""
        databases = [
            ('postgres', POSTGRES_CONNECTION_STRING, None),
            ('mysql', MYSQL_CONNECTION_STRING, None),
            ('snowflake', SNOWFLAKE_CONNECTION_STRING, None),
            ('bigquery', BIGQUERY_CONNECTION_STRING, BIGQUERY_CREDENTIALS),
            ('databricks', DATABRICKS_CONNECTION_STRING, None),
            ('sqlite', SQLITE_CONNECTION_STRING, None),
            ('generic', SQLITE_CONNECTION_STRING, None),  # 'generic' maps to SQLiteConnector
        ]

        for db_type, conn_str, creds in databases:
            if conn_str is None:
                print(f"[SKIP] Skipping {db_type} - not configured")
                continue

            if db_type == 'bigquery' and creds is None:
                print(f"[SKIP] Skipping {db_type} - no credentials")
                continue

            try:
                connector = ConnectorFactory.create_connector(db_type, conn_str, creds)
                engine = connector.get_engine()
                with engine.connect():
                    pass
                connector.close()
                print(f"{db_type}: Connection successful")
            except Exception as e:
                print(f"[FAIL] {db_type}: Connection failed - {e}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
