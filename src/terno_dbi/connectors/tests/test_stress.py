"""
Stress and Resilience Tests for Database Connectors.

Tests cover:
- Pool exhaustion scenarios
- Concurrent access patterns
- Network failure handling
- Recovery after failures

To run:
    source env.sh
    cd terno-ai/terno
    python -m pytest terno/connectors/tests/test_stress.py -v -s
"""

import unittest
import threading
import time
import queue
import os
import json
from sqlalchemy import text
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from terno.connectors.factory import ConnectorFactory

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


def skip_if_no_connection(connection_string, db_name):
    """Skip test if connection string is not configured."""
    def decorator(test_class):
        if connection_string is None:
            return unittest.skip(f"Skipping {db_name} tests - no connection string configured")(test_class)
        return test_class
    return decorator


# POSTGRESQL STRESS TESTS
@skip_if_no_connection(POSTGRES_CONNECTION_STRING, "PostgreSQL")
class TestPostgresStress(unittest.TestCase):
    """PostgreSQL stress tests."""

    @classmethod
    def setUpClass(cls):
        cls.connector = ConnectorFactory.create_connector('postgres', POSTGRES_CONNECTION_STRING)

    @classmethod
    def tearDownClass(cls):
        cls.connector.close()

    def test_concurrent_queries_10_threads(self):
        """Test 10 concurrent threads making queries."""
        results = queue.Queue()
        errors = queue.Queue()
        num_threads = 10
        queries_per_thread = 5

        def worker(thread_id):
            try:
                for i in range(queries_per_thread):
                    with self.connector.get_connection() as conn:
                        result = conn.execute(text("SELECT 1"))
                        row = result.fetchone()
                        results.put((thread_id, i, row[0]))
            except Exception as e:
                errors.put((thread_id, str(e)))

        threads = []
        start_time = time.time()

        print(f"PostgreSQL: Starting {num_threads} concurrent threads...")
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        elapsed = time.time() - start_time
        total_queries = num_threads * queries_per_thread
        successful_queries = results.qsize()

        print(f"PostgreSQL: {successful_queries}/{total_queries} queries in {elapsed:.2f}s")
        self.assertEqual(successful_queries, total_queries)

    def test_connection_reuse_100_queries(self):
        """Test 100 sequential queries reuse connections."""
        start_time = time.time()

        for i in range(100):
            with self.connector.get_connection() as conn:
                conn.execute(text("SELECT 1"))

        elapsed = time.time() - start_time
        print(f"PostgreSQL: 100 queries in {elapsed:.2f}s ({100/elapsed:.1f} qps)")


# MYSQL STRESS TESTS
@skip_if_no_connection(MYSQL_CONNECTION_STRING, "MySQL")
class TestMySQLStress(unittest.TestCase):
    """MySQL stress tests."""

    @classmethod
    def setUpClass(cls):
        cls.connector = ConnectorFactory.create_connector('mysql', MYSQL_CONNECTION_STRING)

    @classmethod
    def tearDownClass(cls):
        cls.connector.close()

    def test_concurrent_queries_10_threads(self):
        """Test 10 concurrent threads making queries."""
        results = queue.Queue()
        errors = queue.Queue()
        num_threads = 10
        queries_per_thread = 3

        def worker(thread_id):
            try:
                for i in range(queries_per_thread):
                    with self.connector.get_connection() as conn:
                        result = conn.execute(text("SELECT 1"))
                        row = result.fetchone()
                        results.put((thread_id, i, row[0]))
            except Exception as e:
                errors.put((thread_id, str(e)))

        threads = []
        start_time = time.time()

        print(f"MySQL: Starting {num_threads} concurrent threads...")
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        elapsed = time.time() - start_time
        total_queries = num_threads * queries_per_thread
        successful_queries = results.qsize()

        print(f"MySQL: {successful_queries}/{total_queries} queries in {elapsed:.2f}s")
        self.assertGreaterEqual(successful_queries, total_queries * 0.8)  # 80% success rate

    def test_connection_reuse_50_queries(self):
        """Test 50 sequential queries reuse connections."""
        start_time = time.time()

        for i in range(50):
            with self.connector.get_connection() as conn:
                conn.execute(text("SELECT 1"))

        elapsed = time.time() - start_time
        print(f"MySQL: 50 queries in {elapsed:.2f}s ({50/elapsed:.1f} qps)")


# BIGQUERY STRESS TESTS
@skip_if_no_connection(BIGQUERY_CONNECTION_STRING, "BigQuery")
class TestBigQueryStress(unittest.TestCase):
    """BigQuery stress tests."""

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

    def test_concurrent_queries_5_threads(self):
        """Test 5 concurrent threads (BigQuery has rate limits)."""
        results = queue.Queue()
        errors = queue.Queue()
        num_threads = 5
        queries_per_thread = 2

        def worker(thread_id):
            try:
                for i in range(queries_per_thread):
                    with self.connector.get_connection() as conn:
                        result = conn.execute(text("SELECT 1"))
                        row = result.fetchone()
                        results.put((thread_id, i, row[0]))
                    time.sleep(0.5)  # Rate limiting
            except Exception as e:
                errors.put((thread_id, str(e)))

        threads = []
        start_time = time.time()

        print(f"BigQuery: Starting {num_threads} concurrent threads...")
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        elapsed = time.time() - start_time
        total_queries = num_threads * queries_per_thread
        successful_queries = results.qsize()
        error_count = errors.qsize()

        print(f"BigQuery: {successful_queries}/{total_queries} queries in {elapsed:.2f}s")
        if error_count > 0:
            while not errors.empty():
                tid, err = errors.get()
                print(f"   [WARN] Thread {tid}: {err[:100]}...")

        self.assertGreaterEqual(successful_queries, total_queries * 0.6)  # 60% success rate

    def test_sequential_queries_10(self):
        """Test 10 sequential queries."""
        start_time = time.time()
        success = 0

        for i in range(10):
            try:
                with self.connector.get_connection() as conn:
                    conn.execute(text("SELECT 1"))
                    success += 1
            except Exception as e:
                print(f"   [WARN] Query {i+1} failed: {str(e)[:50]}...")

        elapsed = time.time() - start_time
        print(f"BigQuery: {success}/10 queries in {elapsed:.2f}s")
        self.assertGreaterEqual(success, 7)


# DATABRICKS STRESS TESTS
@skip_if_no_connection(DATABRICKS_CONNECTION_STRING, "Databricks")
class TestDatabricksStress(unittest.TestCase):
    """Databricks stress tests."""

    @classmethod
    def setUpClass(cls):
        cls.connector = ConnectorFactory.create_connector('databricks', DATABRICKS_CONNECTION_STRING)

    @classmethod
    def tearDownClass(cls):
        cls.connector.close()

    def test_concurrent_queries_5_threads(self):
        """Test 5 concurrent threads (Databricks has rate limits)."""
        results = queue.Queue()
        errors = queue.Queue()
        num_threads = 5
        queries_per_thread = 2

        def worker(thread_id):
            try:
                for i in range(queries_per_thread):
                    with self.connector.get_connection() as conn:
                        result = conn.execute(text("SELECT 1"))
                        row = result.fetchone()
                        results.put((thread_id, i, row[0]))
                    time.sleep(0.3)  # Rate limiting
            except Exception as e:
                errors.put((thread_id, str(e)))

        threads = []
        start_time = time.time()

        print(f"Databricks: Starting {num_threads} concurrent threads...")
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        elapsed = time.time() - start_time
        total_queries = num_threads * queries_per_thread
        successful_queries = results.qsize()
        error_count = errors.qsize()

        print(f"Databricks: {successful_queries}/{total_queries} queries in {elapsed:.2f}s")
        if error_count > 0:
            while not errors.empty():
                tid, err = errors.get()
                print(f"   [WARN] Thread {tid}: {err[:100]}...")

        self.assertGreaterEqual(successful_queries, total_queries * 0.6)

    def test_sequential_queries_10(self):
        """Test 10 sequential queries."""
        start_time = time.time()
        success = 0

        for i in range(10):
            try:
                with self.connector.get_connection() as conn:
                    conn.execute(text("SELECT 1"))
                    success += 1
            except Exception as e:
                print(f"   [WARN] Query {i+1} failed: {str(e)[:50]}...")

        elapsed = time.time() - start_time
        print(f"Databricks: {success}/10 queries in {elapsed:.2f}s")
        self.assertGreaterEqual(success, 7)


# SNOWFLAKE STRESS TESTS
@skip_if_no_connection(SNOWFLAKE_CONNECTION_STRING, "Snowflake")
class TestSnowflakeStress(unittest.TestCase):
    """Snowflake stress tests."""

    @classmethod
    def setUpClass(cls):
        cls.connector = ConnectorFactory.create_connector('snowflake', SNOWFLAKE_CONNECTION_STRING)

    @classmethod
    def tearDownClass(cls):
        cls.connector.close()

    def test_concurrent_queries_10_threads(self):
        """Test 10 concurrent threads making queries."""
        results = queue.Queue()
        errors = queue.Queue()
        num_threads = 10
        queries_per_thread = 3

        def worker(thread_id):
            try:
                for i in range(queries_per_thread):
                    with self.connector.get_connection() as conn:
                        result = conn.execute(text("SELECT 1"))
                        row = result.fetchone()
                        results.put((thread_id, i, row[0]))
            except Exception as e:
                errors.put((thread_id, str(e)))

        threads = []
        start_time = time.time()

        print(f"Snowflake: Starting {num_threads} concurrent threads...")
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        elapsed = time.time() - start_time
        total_queries = num_threads * queries_per_thread
        successful_queries = results.qsize()

        print(f"Snowflake: {successful_queries}/{total_queries} queries in {elapsed:.2f}s")
        self.assertGreaterEqual(successful_queries, total_queries * 0.8)

    def test_connection_reuse_50_queries(self):
        """Test 50 sequential queries reuse connections."""
        start_time = time.time()

        for i in range(50):
            with self.connector.get_connection() as conn:
                conn.execute(text("SELECT 1"))

        elapsed = time.time() - start_time
        print(f"Snowflake: 50 queries in {elapsed:.2f}s ({50/elapsed:.1f} qps)")


# SQLITE STRESS TESTS
class TestSQLiteStress(unittest.TestCase):
    """SQLite stress tests """

    @classmethod
    def setUpClass(cls):
        cls.connector = ConnectorFactory.create_connector('sqlite', SQLITE_CONNECTION_STRING)

    @classmethod
    def tearDownClass(cls):
        cls.connector.close()

    def test_rapid_sequential_queries_1000(self):
        """Test rapid sequential query execution."""
        start_time = time.time()
        count = 1000

        for i in range(count):
            with self.connector.get_connection() as conn:
                conn.execute(text("SELECT 1"))

        elapsed = time.time() - start_time
        print(f"SQLite: {count} queries in {elapsed:.2f}s ({count/elapsed:.0f} qps)")

    def test_concurrent_reads_10_threads(self):
        """Test concurrent read access to SQLite."""
        success_count = [0]
        lock = threading.Lock()

        def reader(thread_id):
            try:
                with self.connector.get_connection() as conn:
                    result = conn.execute(text("SELECT COUNT(*) FROM sqlite_master"))
                    result.fetchone()
                    with lock:
                        success_count[0] += 1
            except Exception as e:
                print(f"[FAIL] Thread {thread_id}: {e}")

        threads = []
        for i in range(10):
            t = threading.Thread(target=reader, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        print(f"SQLite: {success_count[0]}/10 concurrent reads successful")
        self.assertGreaterEqual(success_count[0], 8)


# POOL EXHAUSTION TESTS (PostgreSQL)
@skip_if_no_connection(POSTGRES_CONNECTION_STRING, "PostgreSQL")
class TestPoolExhaustion(unittest.TestCase):
    """Pool exhaustion tests."""

    def test_pool_recovery_after_release(self):
        """Test that pool recovers when connections are released."""
        connector = ConnectorFactory.create_connector('postgres', POSTGRES_CONNECTION_STRING)
        connector.pool_size = 2
        connector.max_overflow = 0

        engine = connector.get_engine()

        try:
            # Exhaust pool
            conn1 = engine.connect()
            conn2 = engine.connect()
            print("Pool exhausted with 2 connections")

            # Release one
            conn1.close()
            print("Released 1 connection")

            # Should be able to get a new one
            conn3 = engine.connect()
            print("New connection acquired after release")

            conn2.close()
            conn3.close()

        finally:
            connector.close()


# NETWORK FAILURE TESTS
@skip_if_no_connection(POSTGRES_CONNECTION_STRING, "PostgreSQL")
class TestNetworkFailures(unittest.TestCase):
    """Network failure handling tests."""

    def test_invalid_connection_handling(self):
        """Test graceful handling of invalid connections."""
        invalid_strings = [
            ("postgresql://invalid:invalid@nonexistent.host.xyz:5432/db", "Invalid host"),
        ]

        for conn_str, description in invalid_strings:
            try:
                connector = ConnectorFactory.create_connector('postgres', conn_str)
                engine = connector.get_engine()
                with engine.connect():
                    pass
                print(f"[FAIL] {description}: Expected failure")
            except Exception as e:
                print(f"{description}: Correctly failed")
            finally:
                if 'connector' in locals():
                    connector.close()

    def test_execute_with_retry(self):
        """Test retry logic works."""
        connector = ConnectorFactory.create_connector('postgres', POSTGRES_CONNECTION_STRING)

        attempt_count = [0]

        def flaky_query():
            attempt_count[0] += 1
            if attempt_count[0] < 2:
                raise PoolTimeoutError("Simulated timeout")
            with connector.get_connection() as conn:
                result = conn.execute(text("SELECT 1"))
                return result.fetchone()[0]

        try:
            result = connector.execute_with_retry(flaky_query, max_retries=3)
            print(f"Query succeeded after {attempt_count[0]} attempts")
            self.assertEqual(result, 1)
        finally:
            connector.close()


# CONNECTION LEAK DETECTION
@skip_if_no_connection(POSTGRES_CONNECTION_STRING, "PostgreSQL")
class TestConnectionLeakDetection(unittest.TestCase):
    """Connection leak detection tests."""

    def test_no_leak_with_context_manager(self):
        """Verify no leak when using context manager."""
        connector = ConnectorFactory.create_connector('postgres', POSTGRES_CONNECTION_STRING)
        connector.pool_size = 3
        connector.max_overflow = 0

        engine = connector.get_engine()

        try:
            # Execute many queries
            for i in range(20):
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))

            print("20 queries completed - testing for leaks...")

            # Get all pool connections
            connections = []
            for i in range(3):
                connections.append(engine.connect())

            print(f"All 3 pool connections available - no leak")

            for conn in connections:
                conn.close()

        finally:
            connector.close()

if __name__ == '__main__':
    unittest.main(verbosity=2)
