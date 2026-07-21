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
    def test_get_org_prompt(self):
        """Should fetch the organisation's prompt via the query API."""
        from terno_dbi.client import TernoDBIClient

        responses.add(
            responses.GET,
            'https://test.com/api/query/organisation/prompt/',
            json={'status': 'success', 'org_prompt': 'Always answer in French.'},
            status=200
        )

        client = TernoDBIClient(base_url='https://test.com', api_key='dbi_query_key')
        result = client.get_org_prompt()

        assert len(responses.calls) == 1
        assert result['org_prompt'] == 'Always answer in French.'
        assert responses.calls[0].request.params == {}

    @responses.activate
    def test_get_org_prompt_with_pagination(self):
        """Should pass offset/limit as query params when provided."""
        from terno_dbi.client import TernoDBIClient

        responses.add(
            responses.GET,
            'https://test.com/api/query/organisation/prompt/',
            json={'status': 'success', 'org_prompt': 'line 5', 'has_more': True, 'next_offset': 6},
            status=200
        )

        client = TernoDBIClient(base_url='https://test.com', api_key='dbi_query_key')
        result = client.get_org_prompt(offset=5, limit=1)

        assert responses.calls[0].request.params == {'offset': '5', 'limit': '1'}
        assert result['next_offset'] == 6

    @responses.activate
    def test_grep_org_prompt(self):
        """Should regex-search the organisation's prompt via the query API."""
        from terno_dbi.client import TernoDBIClient

        responses.add(
            responses.GET,
            'https://test.com/api/query/organisation/prompt/grep/',
            json={'status': 'success', 'matches': [{'line': 1, 'text': 'Always answer in French.'}], 'count': 1},
            status=200
        )

        client = TernoDBIClient(base_url='https://test.com', api_key='dbi_query_key')
        result = client.grep_org_prompt('french')

        assert len(responses.calls) == 1
        assert responses.calls[0].request.params['pattern'] == 'french'
        assert result['count'] == 1

    @responses.activate
    def test_update_org_prompt(self):
        """Should update the organisation's prompt via the admin API."""
        from terno_dbi.client import TernoDBIClient

        responses.add(
            responses.POST,
            'https://test.com/api/admin/organisation/prompt/',
            json={'status': 'success', 'org_prompt': 'Be concise.'},
            status=200
        )

        client = TernoDBIClient(base_url='https://test.com', api_key='dbi_admin_key')
        result = client.update_org_prompt('Be concise.')

        assert len(responses.calls) == 1
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body['org_prompt'] == 'Be concise.'
        assert 'expected_hash' not in request_body
        assert result['org_prompt'] == 'Be concise.'

    @responses.activate
    def test_update_org_prompt_with_expected_hash(self):
        """Should include expected_hash when replacing an existing prompt."""
        from terno_dbi.client import TernoDBIClient

        responses.add(
            responses.POST,
            'https://test.com/api/admin/organisation/prompt/',
            json={'status': 'success', 'org_prompt': 'Be concise.'},
            status=200
        )

        client = TernoDBIClient(base_url='https://test.com', api_key='dbi_admin_key')
        client.update_org_prompt('Be concise.', expected_hash='abc123')

        request_body = json.loads(responses.calls[0].request.body)
        assert request_body['expected_hash'] == 'abc123'

    @responses.activate
    def test_edit_org_prompt(self):
        """Should exact-string-replace within the organisation's prompt via the admin API."""
        from terno_dbi.client import TernoDBIClient

        responses.add(
            responses.POST,
            'https://test.com/api/admin/organisation/prompt/edit/',
            json={'status': 'success', 'org_prompt': 'Always answer in Spanish.', 'content_hash': 'newhash'},
            status=200
        )

        client = TernoDBIClient(base_url='https://test.com', api_key='dbi_admin_key')
        result = client.edit_org_prompt(
            old_string='French', new_string='Spanish', expected_hash='oldhash',
        )

        assert len(responses.calls) == 1
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body == {
            'old_string': 'French', 'new_string': 'Spanish',
            'expected_hash': 'oldhash', 'replace_all': False,
        }
        assert result['org_prompt'] == 'Always answer in Spanish.'

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
    def test_list_table_columns(self):
        """Should call list table columns endpoint."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.GET,
            'https://test.com/api/query/datasources/1/tables/users/columns/',
            json={'status': 'success', 'columns': [{'name': 'id'}]},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.list_table_columns(datasource=1, table='users')
        
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
    def test_execute_query_with_max_rows(self):
        """Should pass max_rows parameter."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/query/datasources/1/query/',
            json={'status': 'success'},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        client.execute_query(
            datasource=1, 
            sql='SELECT * FROM test',
            max_rows=500
        )
        
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body['max_rows'] == 500

    @responses.activate
    def test_stream_query(self):
        """Should hit stream endpoint."""
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/query/datasources/1/stream/',
            body='{"columns":["id"]}\n{"id":1}\n{"__done__":true,"row_count":1}\n',
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        df = client.stream_query(datasource=1, sql='SELECT * FROM test')
        
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body['sql'] == 'SELECT * FROM test'
        assert list(df.columns) == ['id']
        assert len(df) == 1

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


class TestClientMemoryMethods:
    """Tests for memory API client methods."""

    @responses.activate
    def test_list_memories(self):
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.GET,
            'https://test.com/api/query/memory/?datasource_id=1&render=1',
            json={'status': 'success', 'memories': []},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.list_memories(datasource_id=1, render=True)
        
        assert len(responses.calls) == 1
        assert result['status'] == 'success'

    @responses.activate
    def test_get_memory(self):
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.GET,
            'https://test.com/api/query/memory/test-mem/?datasource_id=1',
            json={'status': 'success', 'memory': {'name': 'test-mem'}},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.get_memory('test-mem', datasource_id=1)
        
        assert len(responses.calls) == 1
        assert result['name'] == 'test-mem'

    @responses.activate
    def test_grep_memory(self):
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.GET,
            'https://test.com/api/query/memory/grep/?pattern=foo',
            json={'status': 'success', 'matches': [{'name': 'test'}]},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.grep_memory('foo')
        
        assert len(responses.calls) == 1
        assert len(result) == 1

    @responses.activate
    def test_save_memory(self):
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/query/memory/save/',
            json={'status': 'success', 'memory': {'name': 'new-mem'}},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.save_memory(
            name='new-mem', description='desc', content='c',
            memory_type='project', store='user'
        )
        
        assert len(responses.calls) == 1
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body['name'] == 'new-mem'
        assert request_body['store'] == 'user'

    @responses.activate
    def test_edit_memory(self):
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/query/memory/edit-mem/edit/',
            json={'status': 'success'},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.edit_memory(
            name='edit-mem', old_string='old', new_string='new',
            expected_hash='hash123', store='user'
        )
        
        assert len(responses.calls) == 1
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body['old_string'] == 'old'
        assert request_body['expected_hash'] == 'hash123'

    @responses.activate
    def test_delete_memory(self):
        from terno_dbi.client import TernoDBIClient
        
        responses.add(
            responses.POST,
            'https://test.com/api/query/memory/del-mem/delete/',
            json={'status': 'success'},
            status=200
        )
        
        client = TernoDBIClient(base_url='https://test.com', api_key='key')
        result = client.delete_memory(name='del-mem', store='user')
        
        assert len(responses.calls) == 1
        request_body = json.loads(responses.calls[0].request.body)
        assert request_body['store'] == 'user'
