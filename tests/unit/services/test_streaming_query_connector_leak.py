import json
from unittest.mock import patch, MagicMock, Mock

import pytest

from terno_dbi.core.models import DataSource
from terno_dbi.services.query import (
    execute_streaming_query,
    execute_paginated_query,
    execute_native_sql,
    execute_native_sql_return_df,
    export_native_sql_result,
    export_native_sql_streaming,
)
from terno_dbi.connectors.base import DEFAULT_POOL_SIZE, DEFAULT_MAX_OVERFLOW


def _mock_connector_and_result(rows, columns):
    """Build a MagicMock connector whose get_connection() context manager
    yields a connection whose execute() returns an iterable result."""
    mock_connector = MagicMock()
    mock_conn = MagicMock()
    mock_connector.get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
    mock_connector.get_connection.return_value.__exit__ = Mock(return_value=False)

    mock_result = MagicMock()
    mock_result.keys.return_value = columns
    mock_result.__iter__ = Mock(return_value=iter(rows))
    mock_result.fetchall.return_value = rows
    mock_conn.execute.return_value = mock_result
    return mock_connector


def _datasource():
    return DataSource.objects.create(
        display_name='leak_test_db',
        type='databricks',
        connection_str='databricks://token:x@host/default',
        enabled=True,
    )


class TestPoolMath:
    def test_default_pool_is_configured_for_50_max_connections(self):
        """The '50 max' figure: pool_size(20) + max_overflow(30) = 50."""
        assert DEFAULT_POOL_SIZE + DEFAULT_MAX_OVERFLOW == 50


class TestExecuteStreamingQueryClosesConnector:
    """Fix verification for the connector leak: execute_streaming_query must
    dispose the connector (connector.close()) in every exit path."""

    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_close_called_once_on_full_consumption(self, mock_factory, db):
        ds = _datasource()
        mock_connector = _mock_connector_and_result([(1, 'a'), (2, 'b')], ['id', 'name'])
        mock_factory.create_connector.return_value = mock_connector

        list(execute_streaming_query(ds, "SELECT * FROM t"))

        mock_connector.close.assert_called_once()

    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_close_called_when_generator_closed_early(self, mock_factory, db):
        """If the consumer stops iterating and closes the generator (the
        early-break path), the finally block must still dispose the connector."""
        ds = _datasource()
        mock_connector = _mock_connector_and_result(
            [(i, f'row{i}') for i in range(10_000)], ['id', 'name']
        )
        mock_factory.create_connector.return_value = mock_connector

        gen = execute_streaming_query(ds, "SELECT * FROM t")
        next(gen)          # consume only the column header
        gen.close()        # simulate caller abandoning the stream

        mock_connector.close.assert_called_once()

    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_close_called_on_query_error(self, mock_factory, db):
        """Even when execution raises mid-stream, the connector is disposed."""
        ds = _datasource()
        mock_connector = MagicMock()
        mock_conn = MagicMock()
        mock_connector.get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_connector.get_connection.return_value.__exit__ = Mock(return_value=False)
        mock_conn.execute.side_effect = Exception("boom")
        mock_factory.create_connector.return_value = mock_connector

        chunks = list(execute_streaming_query(ds, "SELECT * FROM t"))

        assert "__error__" in json.loads(chunks[-1])
        mock_connector.close.assert_called_once()

    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_close_called_when_create_connector_fails(self, mock_factory, db):
        """If the connector never gets created, close() must not blow up
        (connector is None) — the generator just yields an error."""
        ds = _datasource()
        mock_factory.create_connector.side_effect = Exception("Connection Refused")

        chunks = list(execute_streaming_query(ds, "SELECT * FROM t"))

        assert "Connection Refused" in json.loads(chunks[0])["__error__"]


class TestExecutePaginatedQueryClosesConnector:
    """execute_paginated_query drives the generator; on an early break
    (max_rows) it must close the generator so the connector is disposed
    deterministically rather than at GC time."""

    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_close_called_on_early_break_via_max_rows(self, mock_factory, db):
        ds = _datasource()
        # 2500 rows -> streaming yields in batches of 1000, so max_rows=1
        # forces a break long before the stream is exhausted.
        mock_connector = _mock_connector_and_result(
            [(i, f'row{i}') for i in range(2500)], ['id', 'name']
        )
        mock_factory.create_connector.return_value = mock_connector

        result = execute_paginated_query(ds, "SELECT * FROM t", max_rows=1)

        assert result['status'] == 'success'
        assert len(result['data']) == 1
        mock_connector.close.assert_called_once()

    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_close_called_on_full_consumption(self, mock_factory, db):
        ds = _datasource()
        mock_connector = _mock_connector_and_result([(1, 'a'), (2, 'b')], ['id', 'name'])
        mock_factory.create_connector.return_value = mock_connector

        result = execute_paginated_query(ds, "SELECT * FROM t")

        assert result['status'] == 'success'
        mock_connector.close.assert_called_once()


class TestOtherQueryPathsCloseConnector:
    """The remaining connector-creating query helpers must also dispose."""

    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_execute_native_sql_closes(self, mock_factory, db):
        ds = _datasource()
        mock_connector = _mock_connector_and_result([(1,)], ['id'])
        mock_factory.create_connector.return_value = mock_connector

        execute_native_sql(ds, "SELECT 1")

        mock_connector.close.assert_called_once()

    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_execute_native_sql_closes_on_error(self, mock_factory, db):
        ds = _datasource()
        mock_connector = MagicMock()
        mock_conn = MagicMock()
        mock_connector.get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_connector.get_connection.return_value.__exit__ = Mock(return_value=False)
        mock_conn.execute.side_effect = Exception("boom")
        mock_factory.create_connector.return_value = mock_connector

        res = execute_native_sql(ds, "SELECT 1")

        assert res['status'] == 'error'
        mock_connector.close.assert_called_once()

    @patch('terno_dbi.services.query.pd')
    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_execute_native_sql_return_df_closes(self, mock_factory, mock_pd, db):
        ds = _datasource()
        mock_connector = _mock_connector_and_result([(1,)], ['id'])
        mock_factory.create_connector.return_value = mock_connector
        # keep to_parquet cheap/harmless
        mock_pd.DataFrame.return_value.to_parquet = Mock()

        execute_native_sql_return_df(ds, "SELECT 1")

        mock_connector.close.assert_called_once()

    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_export_native_sql_result_closes(self, mock_factory, db):
        ds = _datasource()
        mock_connector = _mock_connector_and_result([(1,)], ['id'])
        mock_factory.create_connector.return_value = mock_connector

        export_native_sql_result(ds, "SELECT 1")

        mock_connector.close.assert_called_once()

    @patch('terno_dbi.services.query.PaginationService')
    @patch('terno_dbi.services.query.ConnectorFactory')
    def test_export_native_sql_streaming_closes_after_stream_consumed(
        self, mock_factory, mock_pagination, db
    ):
        """The streaming export must NOT close the connector before the
        StreamingHttpResponse is consumed, but MUST close it once the
        generator is exhausted."""
        ds = _datasource()
        mock_connector = MagicMock()
        mock_factory.create_connector.return_value = mock_connector
        mock_pagination.return_value.stream_all.return_value = iter([[(1,), (2,)]])

        response = export_native_sql_streaming(ds, "SELECT 1")

        # Before consuming the streaming body, the connector must still be open.
        mock_connector.close.assert_not_called()

        # Drain the streaming response body.
        list(response.streaming_content)

        mock_connector.close.assert_called_once()
