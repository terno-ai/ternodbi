"""
Unit tests for Query Service (services/query.py).

Tests query execution, pagination integration, and export functions.
"""
import pytest
import json
import io
from unittest.mock import patch, MagicMock, Mock
from decimal import Decimal
from datetime import datetime, date

from terno_dbi.core.models import DataSource


@pytest.fixture
def mock_connector():
    """Create a mock database connector."""
    connector = MagicMock()
    connection = MagicMock()
    connector.get_connection.return_value.__enter__ = Mock(return_value=connection)
    connector.get_connection.return_value.__exit__ = Mock(return_value=False)
    connector.get_dialect_info.return_value = ('postgres', '15.0')
    return connector


@pytest.fixture
def datasource(db):
    """Create a test datasource."""
    return DataSource.objects.create(
        display_name='query_test_db',
        type='postgres',
        connection_str='postgresql://localhost/querytest',
        enabled=True
    )


class TestMakeJsonSafe:
    """Tests for _make_json_safe helper function."""

    def test_handles_decimal(self):
        """Decimal should be converted to string (actual behavior)."""
        from terno_dbi.services.query import _make_json_safe
        
        result = _make_json_safe(Decimal('123.45'))
        
        # The actual implementation converts non-primitives to str
        assert result == '123.45'
        assert isinstance(result, str)

    def test_handles_datetime(self):
        """Datetime should be converted to string."""
        from terno_dbi.services.query import _make_json_safe
        
        dt = datetime(2025, 1, 29, 12, 30, 45)
        result = _make_json_safe(dt)
        
        assert '2025-01-29' in result
        assert isinstance(result, str)

    def test_handles_date(self):
        """Date should be converted to string."""
        from terno_dbi.services.query import _make_json_safe
        
        d = date(2025, 1, 29)
        result = _make_json_safe(d)
        
        assert '2025-01-29' in result
        assert isinstance(result, str)

    def test_handles_bytes(self):
        """Bytes should be base64 encoded."""
        from terno_dbi.services.query import _make_json_safe
        
        result = _make_json_safe(b'hello')
        
        assert isinstance(result, str)
        # Base64 encoded 'hello'
        assert 'aGVsbG8' in result

    def test_handles_bytearray(self):
        """Bytearray should be base64 encoded."""
        from terno_dbi.services.query import _make_json_safe
        
        result = _make_json_safe(bytearray(b'world'))
        
        assert isinstance(result, str)

    def test_passes_through_primitives(self):
        """Primitives should pass through unchanged."""
        from terno_dbi.services.query import _make_json_safe
        
        assert _make_json_safe('hello') == 'hello'
        assert _make_json_safe(123) == 123
        assert _make_json_safe(45.67) == 45.67
        assert _make_json_safe(True) is True
        assert _make_json_safe(None) is None


class TestPrepareTableData:
    """Tests for _prepare_table_data function."""

    def test_converts_result_to_dict(self):
        """Should convert result to dict with columns and data."""
        from terno_dbi.services.query import _prepare_table_data
        
        # Mock SQLAlchemy result object
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id', 'name']
        mock_result.fetchall.return_value = [
            (1, 'Test'),
            (2, 'Test2')
        ]
        mock_result.rowcount = 2
        
        result = _prepare_table_data(mock_result, page=1, per_page=10)
        
        assert 'data' in result
        assert 'columns' in result
        assert len(result['data']) == 2
        assert result['columns'] == ['id', 'name']
        assert result['row_count'] == 2

    def test_handles_empty_results(self):
        """Should handle empty result set."""
        from terno_dbi.services.query import _prepare_table_data
        
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id', 'name']
        mock_result.fetchall.return_value = []
        mock_result.rowcount = 0
        
        result = _prepare_table_data(mock_result, page=1, per_page=10)
        
        assert result['data'] == []
        assert result['columns'] == ['id', 'name']
        assert result['row_count'] == 0

    def test_pagination_info(self):
        """Should include pagination info."""
        from terno_dbi.services.query import _prepare_table_data
        
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id']
        mock_result.fetchall.return_value = [(i,) for i in range(100)]
        mock_result.rowcount = 100
        
        result = _prepare_table_data(mock_result, page=1, per_page=10)
        
        assert result['page'] == 1
        assert result['total_pages'] == 10
        assert result['has_next'] is True
        assert result['has_prev'] is False

    def test_negative_rowcount_uses_fetchall_length(self):
        """Should use fetchall length when rowcount is negative."""
        from terno_dbi.services.query import _prepare_table_data
        
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id']
        mock_result.fetchall.return_value = [(1,), (2,), (3,)]
        mock_result.rowcount = -1  # Some DBs return -1
        
        result = _prepare_table_data(mock_result, page=1, per_page=10)
        
        assert result['row_count'] == 3

    def test_page_2_pagination(self):
        """Should correctly paginate page 2."""
        from terno_dbi.services.query import _prepare_table_data
        
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id']
        mock_result.fetchall.return_value = [(i,) for i in range(25)]
        mock_result.rowcount = 25
        
        result = _prepare_table_data(mock_result, page=2, per_page=10)
        
        assert result['page'] == 2
        assert result['has_prev'] is True
        assert result['has_next'] is True
        assert len(result['data']) == 10

    def test_last_page_pagination(self):
        """Should correctly identify last page."""
        from terno_dbi.services.query import _prepare_table_data
        
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id']
        mock_result.fetchall.return_value = [(i,) for i in range(25)]
        mock_result.rowcount = 25
        
        result = _prepare_table_data(mock_result, page=3, per_page=10)
        
        assert result['page'] == 3
        assert result['has_next'] is False
        assert len(result['data']) == 5


@pytest.mark.django_db
class TestExecuteNativeSql:
    """Tests for execute_native_sql function."""

    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_executes_select_query(self, mock_factory, datasource):
        """Should execute SELECT query and return results."""
        from terno_dbi.services.query import execute_native_sql
        
        # Setup connector mock
        mock_connector = MagicMock()
        mock_factory.create_connector.return_value = mock_connector
        
        # Setup connection mock
        mock_conn = MagicMock()
        mock_connector.get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_connector.get_connection.return_value.__exit__ = Mock(return_value=False)
        
        # Setup result mock
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id', 'name']
        mock_result.fetchall.return_value = [(1, 'Test')]
        mock_result.rowcount = 1
        mock_conn.execute.return_value = mock_result
        
        result = execute_native_sql(datasource, 'SELECT * FROM test')
        
        assert result['status'] == 'success'
        assert 'table_data' in result
        assert result['table_data']['row_count'] == 1

    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_handles_sql_error(self, mock_factory, datasource):
        """Should handle SQL execution errors gracefully."""
        from terno_dbi.services.query import execute_native_sql
        
        mock_connector = MagicMock()
        mock_factory.create_connector.return_value = mock_connector
        
        mock_conn = MagicMock()
        mock_connector.get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_connector.get_connection.return_value.__exit__ = Mock(return_value=False)
        mock_conn.execute.side_effect = Exception("Table not found")
        
        result = execute_native_sql(datasource, 'SELECT * FROM nonexistent')
        
        assert result['status'] == 'error'
        assert 'error' in result
        assert 'Table not found' in result['error']


@pytest.mark.django_db
class TestExecutePaginatedQuery:
    """Tests for execute_paginated_query function."""

    @patch('terno_dbi.services.query.execute_streaming_query')
    def test_returns_streaming_data(self, mock_streaming, datasource):
        """Should return streaming query results formatted as a flat dict."""
        from terno_dbi.services.query import execute_paginated_query
        
        # Mock streaming result as an iterator of NDJSON strings
        mock_streaming.return_value = iter([
            '{"columns":["id","name"]}\n',
            '{"id":1,"name":"Test"}\n',
            '{"__done__":true,"row_count":1}\n'
        ])
        
        result = execute_paginated_query(datasource, 'SELECT * FROM test', max_rows=100)
        
        assert result['status'] == 'success'
        assert result['columns'] == ['id', 'name']
        assert len(result['data']) == 1
        mock_streaming.assert_called_once_with(datasource, 'SELECT * FROM test')

    @patch('terno_dbi.services.query.execute_streaming_query')
    def test_handles_general_exception(self, mock_streaming, datasource):
        """Should handle general exceptions."""
        from terno_dbi.services.query import execute_paginated_query
        
        mock_streaming.side_effect = Exception("Database error")
        
        result = execute_paginated_query(datasource, 'SELECT * FROM test')
        
        assert result['status'] == 'error'
        assert 'Database error' in result['error']


@pytest.mark.django_db
class TestExecuteNativeSqlReturnDf:
    """Tests for execute_native_sql_return_df function."""

    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_returns_parquet_base64(self, mock_factory, datasource):
        """Should return parquet as base64."""
        from terno_dbi.services.query import execute_native_sql_return_df
        
        mock_connector = MagicMock()
        mock_factory.create_connector.return_value = mock_connector
        
        mock_conn = MagicMock()
        mock_connector.get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_connector.get_connection.return_value.__exit__ = Mock(return_value=False)
        
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id', 'name']
        mock_result.fetchall.return_value = [(1, 'Test')]
        mock_conn.execute.return_value = mock_result
        
        result = execute_native_sql_return_df(datasource, 'SELECT * FROM test')
        
        assert result['status'] == 'success'
        assert 'parquet_b64' in result

    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_handles_error(self, mock_factory, datasource):
        """Should handle errors."""
        from terno_dbi.services.query import execute_native_sql_return_df
        
        mock_factory.create_connector.side_effect = Exception("Connection failed")
        
        result = execute_native_sql_return_df(datasource, 'SELECT * FROM test')
        
        assert result['status'] == 'error'


@pytest.mark.django_db
class TestExportFunctions:
    """Tests for export functions."""

    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_export_returns_http_response(self, mock_factory, datasource):
        """Should return HTTP response for CSV export."""
        from terno_dbi.services.query import export_native_sql_result
        from django.http import HttpResponse
        
        mock_connector = MagicMock()
        mock_factory.create_connector.return_value = mock_connector
        
        mock_conn = MagicMock()
        mock_connector.get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_connector.get_connection.return_value.__exit__ = Mock(return_value=False)
        
        mock_result = MagicMock()
        mock_result.keys.return_value = ['id', 'name']
        mock_result.__iter__ = Mock(return_value=iter([(1, 'Test')]))
        mock_conn.execute.return_value = mock_result
        
        result = export_native_sql_result(datasource, 'SELECT * FROM test')
        
        assert isinstance(result, HttpResponse)
        assert 'text/csv' in result['Content-Type']
        assert 'attachment' in result['Content-Disposition']

    @patch('terno_dbi.services.query.PaginationService')
    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_export_streaming(self, mock_factory, mock_pag_cls, datasource):
        """Should return streaming response."""
        from terno_dbi.services.query import export_native_sql_streaming
        from django.http import StreamingHttpResponse
        
        mock_connector = MagicMock()
        mock_factory.create_connector.return_value = mock_connector
        
        mock_pag = MagicMock()
        mock_pag.stream_all.return_value = iter([[(1, 'Test'), (2, 'Test2')]])
        mock_pag_cls.return_value = mock_pag
        
        result = export_native_sql_streaming(datasource, 'SELECT * FROM test')
        
        assert isinstance(result, StreamingHttpResponse)
        assert 'text/csv' in result['Content-Type']

    @patch('terno_dbi.services.query.PaginationService')
    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_export_streaming_content(self, mock_factory, mock_pag_cls, datasource):
        """Should yield CSV content with headers and data."""
        from terno_dbi.services.query import export_native_sql_streaming
        
        mock_connector = MagicMock()
        mock_factory.create_connector.return_value = mock_connector
        
        mock_pag = MagicMock()
        # Mock stream_all to return an iterator of batches
        # Batch 1: 2 rows
        # Batch 2: 1 row
        mock_pag.stream_all.return_value = iter([
            [[1, 'Alice'], [2, 'Bob']],
            [[3, 'Charlie']]
        ])
        mock_pag_cls.return_value = mock_pag
        
        response = export_native_sql_streaming(datasource, 'SELECT * FROM users')
        
        # Consume streaming content
        content = b"".join(response.streaming_content).decode('utf-8')
        
        # Verify headers (col_0, col_1 generated by logic)
        assert "col_0,col_1" in content
        
        # Verify data
        assert "1,Alice" in content
        assert "2,Bob" in content
        assert "3,Charlie" in content

