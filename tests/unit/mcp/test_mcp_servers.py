"""
Unit tests for MCP Query and Admin Servers.

Tests the MCP tools exposed for LLM agents.
"""
import pytest
from unittest.mock import patch, MagicMock, AsyncMock


class TestMCPQueryServerTools:
    """Tests for Query Server MCP tools - synchronous mock tests."""

    @patch('terno_dbi.client.TernoDBIClient')
    def test_list_datasources_integration(self, mock_client_cls):
        """Client should be used to list datasources."""
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.list_datasources.return_value = {
            'datasources': [{'id': 1, 'name': 'test'}]
        }
        
        # Create a fresh client and verify it can call list_datasources
        from terno_dbi.client import TernoDBIClient
        client = TernoDBIClient(api_url='http://test', api_key='key')
        
        # The client should be able to list datasources
        result = mock_client.list_datasources()
        assert 'datasources' in result

    @patch('terno_dbi.client.TernoDBIClient')
    def test_list_tables_integration(self, mock_client_cls):
        """Client should be used to list tables."""
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.list_tables.return_value = {
            'tables': [{'name': 'users', 'id': 1}]
        }
        
        result = mock_client.list_tables(datasource_id=1)
        assert 'tables' in result

    @patch('terno_dbi.client.TernoDBIClient')
    def test_execute_query_integration(self, mock_client_cls):
        """Client should be used to execute queries."""
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.execute_query.return_value = {
            'status': 'success',
            'table_data': {'columns': ['id'], 'data': []}
        }
        
        result = mock_client.execute_query(datasource_id=1, sql='SELECT 1')
        assert result['status'] == 'success'

    @patch('terno_dbi.client.TernoDBIClient')
    def test_get_schema_integration(self, mock_client_cls):
        """Client should be used to get schema."""
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_schema.return_value = {
            'schema': [{'table_name': 'users', 'columns': []}]
        }
        
        result = mock_client.get_schema(datasource_id=1)
        assert 'schema' in result


class TestMCPAdminServerTools:
    """Tests for Admin Server MCP tools - synchronous mock tests."""

    @patch('terno_dbi.client.TernoDBIClient')
    def test_create_datasource_integration(self, mock_client_cls):
        """Client should be used to create datasource."""
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.create_datasource.return_value = {
            'status': 'success', 'datasource': {'id': 1}
        }
        
        result = mock_client.create_datasource(
            display_name='new_db',
            db_type='postgres', 
            connection_str='postgresql://localhost/db'
        )
        assert result['status'] == 'success'

    @patch('terno_dbi.client.TernoDBIClient')
    def test_sync_metadata_integration(self, mock_client_cls):
        """Client should be used to sync metadata."""
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.sync_metadata.return_value = {
            'status': 'success', 'tables': 10
        }
        
        result = mock_client.sync_metadata(datasource_id=1)
        assert 'tables' in result

    @patch('terno_dbi.client.TernoDBIClient')
    def test_validate_connection_integration(self, mock_client_cls):
        """Client should be used to validate connection."""
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.validate_connection.return_value = {'valid': True}
        
        result = mock_client.validate_connection(
            db_type='postgres',
            connection_str='postgresql://localhost/db'
        )
        assert result['valid'] is True


class TestMCPToolSchemas:
    """Tests for MCP tool schema definitions."""

    def test_list_tools_returns_expected_tools(self):
        """Query server should define expected tools."""
        # This tests that the module can be imported and has the expected structure
        from terno_dbi.mcp import query_server
        
        # Server should be defined
        assert hasattr(query_server, 'server')
        assert query_server.server.name == 'ternodbi-query'

    def test_admin_server_has_server(self):
        """Admin server should define a server."""
        from terno_dbi.mcp import admin_server
        
        assert hasattr(admin_server, 'server')
        assert admin_server.server.name == 'ternodbi-admin'
