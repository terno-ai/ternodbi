"""
Unit tests for TernoDBIClient SDK.

Tests the Python client for interacting with TernoDBI APIs.
"""
import pytest
import json
from unittest.mock import patch, MagicMock, Mock
import responses


class TestClientInitialization:
    """Tests for TernoDBIClient initialization."""

    def test_init_with_explicit_params(self):
        """Should accept explicit base_url and api_key."""
        from terno_dbi.client import TernoDBIClient
        
        client = TernoDBIClient(
            base_url='https://api.example.com',
            api_key='dbi_query_testkey123'
        )
        
        assert client.base_url == 'https://api.example.com'
        assert client.api_key == 'dbi_query_testkey123'

    @patch.dict('os.environ', {'TERNODBI_API_URL': 'https://env.example.com', 'TERNODBI_API_KEY': 'dbi_query_envkey'})
    def test_init_from_env_vars(self):
        """Should read from environment variables if not provided."""
        from terno_dbi.client import TernoDBIClient
        
        client = TernoDBIClient()
        
        assert client.base_url == 'https://env.example.com'
        assert client.api_key == 'dbi_query_envkey'

    def test_strips_trailing_slash(self):
        """Should strip trailing slash from base_url."""
        from terno_dbi.client import TernoDBIClient
        
        client = TernoDBIClient(base_url='https://test.com/', api_key='key')
        
        assert client.base_url == 'https://test.com'

    def test_default_base_url(self):
        """Should use default localhost URL if not provided."""
        from terno_dbi.client import TernoDBIClient
        
        with patch.dict('os.environ', {}, clear=True):
            client = TernoDBIClient(api_key='key')
            assert 'localhost' in client.base_url or '127.0.0.1' in client.base_url


class TestClientHeaders:
    """Tests for request header generation."""

    def test_includes_bearer_token(self):
        """Should include Bearer token in Authorization header."""
        from terno_dbi.client import TernoDBIClient
        
        client = TernoDBIClient(
            base_url='https://test.com',
            api_key='dbi_query_mytoken'
        )
        
        headers = client._get_headers()
        
        assert 'Authorization' in headers
        assert headers['Authorization'] == 'Bearer dbi_query_mytoken'

    def test_includes_content_type(self):
        """Should include JSON content type."""
        from terno_dbi.client import TernoDBIClient
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        headers = client._get_headers()
        
        assert headers.get('Content-Type') == 'application/json'

    def test_omits_auth_when_no_api_key(self):
        """Should not include Authorization when no api_key."""
        from terno_dbi.client import TernoDBIClient
        
        client = TernoDBIClient(base_url='https://test.com', api_key=None)
        headers = client._get_headers()
        
        assert 'Authorization' not in headers


class TestClientMethods:
    """Tests for client API methods."""

    @responses.activate
    def test_list_datasources(self):
        """Should call list datasources endpoint."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.GET,
            'https://test.com/api/query/datasources/',
            json={'status': 'success', 'datasources': [{'id': 1, 'name': 'test'}]},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.list_datasources()
        
        assert len(responses.calls) == 1
        assert isinstance(result, list)

    @responses.activate
    def test_list_tables(self):
        """Should call list tables endpoint with datasource ID."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.GET,
            'https://test.com/api/query/datasources/1/tables/',
            json={'status': 'success', 'tables': [{'id': 1, 'name': 'users'}]},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.list_tables(datasource=1)
        
        assert len(responses.calls) == 1
        assert isinstance(result, list)

    @responses.activate
    def test_list_columns(self):
        """Should call list columns endpoint."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.GET,
            'https://test.com/api/query/tables/1/columns/',
            json={'status': 'success', 'columns': [{'name': 'id'}]},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.list_columns(table_id=1)
        
        assert isinstance(result, list)

    @responses.activate
    def test_execute_query(self):
        """Should execute query and return results."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/query/datasources/1/query/',
            json={
                'status': 'success',
                'table_data': {'columns': ['id'], 'data': [{'id': 1}]}
            },
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.execute_query(datasource=1, sql='SELECT * FROM test')
        
        assert result['status'] == 'success'

    @responses.activate
    def test_execute_query_with_cursor(self):
        """Should pass cursor parameter."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/query/datasources/1/query/',
            json={'status': 'success', 'table_data': {}},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        client.execute_query(
            datasource=1, 
            sql='SELECT * FROM test',
            cursor='abc123',
            direction='backward',
            order_by=[{'column': 'id', 'direction': 'DESC'}]
        )
        
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body['cursor'] == 'abc123'
        assert request_body['direction'] == 'backward'
        assert request_body['order_by'] == [{'column': 'id', 'direction': 'DESC'}]

    @responses.activate
    def test_execute_query_with_limit(self):
        """Should use limit as per_page."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/query/datasources/1/query/',
            json={'status': 'success', 'table_data': {}},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        client.execute_query(datasource=1, sql='SELECT * FROM test', limit=100)
        
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body['per_page'] == 100

    @responses.activate
    def test_get_schema(self):
        """Should retrieve full schema for datasource."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.GET,
            'https://test.com/api/query/datasources/1/schema/',
            json={'datasource': 'test', 'schema': []},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.get_schema(datasource=1)
        
        assert 'schema' in result

    @responses.activate
    def test_create_datasource(self):
        """Should create new datasource via admin API."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/admin/datasources/',
            json={'status': 'success', 'datasource': {'id': 2}},
            status=201
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='dbi_admin_key')
        result = client.create_datasource(
            display_name='new_db',
            db_type='postgres',
            connection_str='postgresql://localhost/new'
        )
        
        assert result['status'] == 'success'

    @responses.activate
    def test_delete_datasource(self):
        """Should delete datasource."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.DELETE,
            'https://test.com/api/admin/datasources/1/delete/',
            json={'status': 'success'},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.delete_datasource(datasource=1)
        
        assert result['status'] == 'success'

    @responses.activate
    def test_sync_metadata(self):
        """Should trigger metadata sync."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/admin/datasources/1/sync/',
            json={'status': 'success', 'tables': 5},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.sync_metadata(datasource=1)
        
        assert 'tables' in result

    @responses.activate
    def test_validate_connection(self):
        """Should validate connection."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/admin/validate/',
            json={'status': 'success', 'valid': True},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.validate_connection(
            db_type='postgres',
            connection_str='postgresql://localhost/test'
        )
        
        assert result['valid'] is True

    @responses.activate
    def test_update_table(self):
        """Should update table metadata."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.PATCH,
            'https://test.com/api/admin/tables/1/',
            json={'status': 'success'},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.update_table(table_id=1, public_name='Users Table')
        
        assert result['status'] == 'success'

    @responses.activate
    def test_update_column(self):
        """Should update column metadata."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.PATCH,
            'https://test.com/api/admin/columns/1/',
            json={'status': 'success'},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.update_column(column_id=1, description='User identifier')
        
        assert result['status'] == 'success'

    @responses.activate
    def test_get_table_info(self):
        """Should get table info."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.GET,
            'https://test.com/api/admin/datasources/1/tables/users/info/',
            json={'table': 'users', 'columns': []},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.get_table_info(datasource=1, table_name='users')
        
        assert 'table' in result

    @responses.activate
    def test_get_all_tables_info(self):
        """Should get all tables info."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/admin/datasources/1/tables/info/',
            json={'tables': []},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.get_all_tables_info(datasource=1)
        
        assert 'tables' in result

    @responses.activate
    def test_get_sample_data(self):
        """Should get sample data from table."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.GET,
            'https://test.com/api/query/tables/1/sample/',
            json={'data': [[1, 'test']]},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.get_sample_data(table_id=1, rows=5)
        
        assert 'data' in result


class TestIterQuery:
    """Tests for iter_query pagination helper."""

    @responses.activate
    def test_iter_query_single_page(self):
        """Should iterate over single page."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/query/datasources/1/query/',
            json={
                'status': 'success',
                'table_data': {
                    'data': [{'id': 1}, {'id': 2}],
                    'has_next': False,
                    'next_cursor': None
                }
            },
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        batches = list(client.iter_query(datasource=1, sql='SELECT * FROM test'))
        
        assert len(batches) == 1
        assert len(batches[0]) == 2

    @responses.activate
    def test_iter_query_multiple_pages(self):
        """Should iterate over multiple pages."""
        from terno_dbi.client import TernoDBIClient
        
        # First page
        responses.add(
            responses.POST,
            'https://test.com/api/query/datasources/1/query/',
            json={
                'status': 'success',
                'table_data': {
                    'data': [{'id': 1}],
                    'has_next': True,
                    'next_cursor': 'cursor1'
                }
            },
            status=200
        )
        # Second page
        responses.add(
            responses.POST,
            'https://test.com/api/query/datasources/1/query/',
            json={
                'status': 'success',
                'table_data': {
                    'data': [{'id': 2}],
                    'has_next': False,
                    'next_cursor': None
                }
            },
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        batches = list(client.iter_query(datasource=1, sql='SELECT * FROM test'))
        
        assert len(batches) == 2

    @responses.activate
    def test_iter_query_handles_error(self):
        """Should raise on error response."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/query/datasources/1/query/',
            json={'status': 'error', 'error': 'Query failed'},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        
        with pytest.raises(Exception, match='Query failed'):
            list(client.iter_query(datasource=1, sql='SELECT * FROM test'))


class TestClientErrorHandling:
    """Tests for error handling in client."""

    @responses.activate
    def test_handles_401_unauthorized(self):
        """Should raise on 401."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.GET,
            'https://test.com/api/query/datasources/',
            json={'error': 'Unauthorized'},
            status=401
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='bad_key')
        
        with pytest.raises(Exception) as exc_info:
            client.list_datasources()
        assert 'Unauthorized' in str(exc_info.value)

    @responses.activate
    def test_handles_500_server_error(self):
        """Should raise on server errors."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.GET,
            'https://test.com/api/query/datasources/',
            json={'error': 'Internal Server Error'},
            status=500
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        
        with pytest.raises(Exception):
            client.list_datasources()

    @responses.activate
    def test_handles_non_json_error(self):
        """Should handle non-JSON error response."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.GET,
            'https://test.com/api/query/datasources/',
            body='Service Unavailable',
            status=503
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        
        with pytest.raises(Exception):
            client.list_datasources()



    @responses.activate
    def test_execute_query_error_branch(self):
        """Should return error dict without raising exception if status is error."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/query/datasources/1/query/',
            json={'status': 'error', 'error': 'DB Error'},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        
        result = client.execute_query(1, "SELECT")
        assert result['status'] == 'error'
        assert result['error'] == 'DB Error'

    @responses.activate
    def test_get_all_tables_info_payload(self):
        """Should include table_names in payload."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/admin/datasources/1/tables/info/',
            json={'tables': []},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        client.get_all_tables_info(1, table_names=['t1', 't2'])
        
        import json
        body = json.loads(responses.calls[0].request.body)
        assert body['table_names'] == ['t1', 't2']


