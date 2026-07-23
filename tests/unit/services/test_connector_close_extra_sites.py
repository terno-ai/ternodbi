"""Connector-leak fix verification for the non-query call sites:
validation.validate_datasource_input, schema_utils.get_table_info, and
schema_utils.sync_metadata must dispose the connector on every path."""
from unittest.mock import patch, MagicMock, Mock

from terno_dbi.core.models import DataSource
from terno_dbi.services.validation import validate_datasource_input
from terno_dbi.services.schema_utils import get_table_info, sync_metadata


def _datasource():
    return DataSource.objects.create(
        display_name='close_test_db',
        type='postgres',
        connection_str='postgresql://u:p@localhost:5432/db',
        enabled=True,
    )


class TestValidateConnectionClosesConnector:
    @patch('terno_dbi.services.validation.ConnectorFactory')
    def test_close_called_on_successful_probe(self, mock_factory, db):
        mock_connector = MagicMock()
        mock_connector.get_connection.return_value.__enter__ = Mock(return_value=MagicMock())
        mock_connector.get_connection.return_value.__exit__ = Mock(return_value=False)
        mock_factory.create_connector.return_value = mock_connector

        err = validate_datasource_input('postgres', 'postgresql://u:p@localhost:5432/db')

        assert err is None
        mock_connector.close.assert_called_once()

    @patch('terno_dbi.services.validation.ConnectorFactory')
    def test_close_called_when_probe_fails(self, mock_factory, db):
        """The common case for a validation probe: connection fails. The
        connector must still be disposed (previously it leaked here)."""
        mock_connector = MagicMock()
        mock_connector.get_connection.side_effect = Exception("access denied")
        mock_factory.create_connector.return_value = mock_connector

        err = validate_datasource_input('postgres', 'postgresql://u:p@localhost:5432/db')

        assert err is not None  # an error message is returned
        mock_connector.close.assert_called_once()


class TestGetTableInfoClosesConnector:
    @patch('terno_dbi.services.schema_utils.ConnectorFactory')
    def test_close_called_on_error_path(self, mock_factory, db):
        ds = _datasource()
        mock_connector = MagicMock()
        # Force the body to raise so we exercise the except+finally path.
        mock_connector.get_connection.side_effect = Exception("reflect boom")
        mock_factory.create_connector.return_value = mock_connector

        result = get_table_info(ds, 'some_table')

        assert 'error' in result
        mock_connector.close.assert_called_once()


class TestSyncMetadataClosesConnector:
    @patch('terno_dbi.services.schema_utils.ConnectorFactory')
    def test_close_called_on_error_path(self, mock_factory, db):
        ds = _datasource()
        mock_connector = MagicMock()
        mock_connector.get_dialect_info.return_value = ("postgresql", "14")
        # get_metadata raising drives the outer except+finally.
        mock_connector.get_metadata.side_effect = Exception("metadata boom")
        mock_factory.create_connector.return_value = mock_connector

        result = sync_metadata(ds.id)

        assert 'error' in result
        mock_connector.close.assert_called_once()
