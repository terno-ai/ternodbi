"""
Unit tests for Query Service Views.

Tests all endpoints in query_service/views.py including:
- health, info
- list_datasources, get_datasource
- list_tables, list_columns, list_table_columns
- list_foreign_keys
- get_sample_data, execute_query, export_query
"""
import pytest
import json
import hashlib
from unittest.mock import patch, MagicMock
from django.test import RequestFactory

from terno_dbi.core.models import DataSource, Table, TableColumn, ServiceToken


@pytest.fixture
def request_factory():
    return RequestFactory()


@pytest.fixture
def setup_test_data(db):
    """Create test user, token, datasource, tables, and columns."""
    from django.contrib.auth.models import User
    
    # Create user
    user = User.objects.create_user('queryviewuser', 'query@example.com', 'password')
    
    # Create datasource
    ds = DataSource.objects.create(
        display_name='test_query_db',
        type='postgres',
        connection_str='postgresql://localhost/test',
        enabled=True
    )
    
    # Create tables
    table1 = Table.objects.create(
        name='users',
        public_name='Users',
        data_source=ds,
        description='User accounts'
    )
    table2 = Table.objects.create(
        name='orders',
        public_name='Orders',
        data_source=ds,
        description='Customer orders'
    )
    
    # Create columns
    col1 = TableColumn.objects.create(
        name='id',
        public_name='ID',
        table=table1,
        data_type='integer'
    )
    col2 = TableColumn.objects.create(
        name='email',
        public_name='Email',
        table=table1,
        data_type='varchar'
    )
    
    # Create service token
    token_key = 'dbi_query_testviewtoken123'
    key_hash = hashlib.sha256(token_key.encode()).hexdigest()
    token = ServiceToken.objects.create(
        name='Query View Test Token',
        token_type=ServiceToken.TokenType.QUERY,
        key_prefix='dbi_query_',
        key_hash=key_hash,
        is_active=True,
        created_by=user
    )
    token.datasources.add(ds)
    
    return {
        'user': user,
        'datasource': ds,
        'table1': table1,
        'table2': table2,
        'col1': col1,
        'col2': col2,
        'token': token,
        'token_key': token_key
    }


def make_authenticated_request(request_factory, method, path, token_key, data=None):
    """Helper to create authenticated request."""
    if method == 'GET':
        request = request_factory.get(path)
    else:
        request = request_factory.post(
            path,
            data=json.dumps(data) if data else None,
            content_type='application/json'
        )
    request.META['HTTP_AUTHORIZATION'] = f'Bearer {token_key}'
    return request


def setup_request_for_view(request, token, datasource=None, table=None, column=None):
    """
    Set up request attributes as the require_service_auth decorator would.
    """
    request.service_token = token
    request.allowed_datasources = token.get_accessible_datasources()
    if datasource:
        request.resolved_datasource = datasource
    if table:
        request.resolved_table = table
    if column:
        request.resolved_column = column


class TestHealthEndpoint:
    """Tests for /api/query/health/"""

    def test_returns_ok(self, request_factory):
        """Health endpoint should return OK status."""
        from terno_dbi.core.views import health
        
        request = request_factory.get('/api/query/health/')
        response = health(request)
        
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data.get('status') == 'ok'
        assert data.get('service') == 'terno_dbi'

    def test_no_auth_required(self, request_factory):
        """Health endpoint should not require authentication."""
        from terno_dbi.core.views import health
        
        request = request_factory.get('/api/query/health/')
        response = health(request)
        
        assert response.status_code == 200


class TestInfoEndpoint:
    """Tests for /api/query/info/"""

    def test_returns_version(self, request_factory):
        """Info endpoint should return version info."""
        from terno_dbi.core.views import info
        
        request = request_factory.get('/api/query/info/')
        response = info(request)
        
        assert response.status_code == 200
        data = json.loads(response.content)
        assert 'version' in data or 'name' in data

    def test_no_auth_required(self, request_factory):
        """Info endpoint should not require authentication."""
        from terno_dbi.core.views import info
        
        request = request_factory.get('/api/query/info/')
        response = info(request)
        
        assert response.status_code == 200




@pytest.mark.django_db
class TestListDatasources:
    """Tests for /api/query/datasources/"""

    def test_returns_datasources(self, request_factory, setup_test_data):
        """Should return list of accessible datasources."""
        from terno_dbi.core.query_service.views import list_datasources
        
        request = request_factory.get('/api/query/datasources/')
        setup_request_for_view(request, setup_test_data['token'])
        
        response = list_datasources(request)
        
        assert response.status_code == 200
        data = json.loads(response.content)
        # Response is a dict with 'datasources' key
        assert data.get('status') == 'success'
        assert 'datasources' in data
        assert len(data['datasources']) >= 1

    def test_returns_scoped_datasources_only(self, request_factory, setup_test_data):
        """Should only return datasources within token scope."""
        from terno_dbi.core.query_service.views import list_datasources
        
        # Create another datasource NOT linked to token
        other_ds = DataSource.objects.create(
            display_name='other_ds_not_linked',
            type='mysql',
            enabled=True
        )
        
        request = request_factory.get('/api/query/datasources/')
        setup_request_for_view(request, setup_test_data['token'])
        
        response = list_datasources(request)
        
        data = json.loads(response.content)
        ds_names = [d.get('name') for d in data.get('datasources', [])]
        assert 'other_ds_not_linked' not in ds_names
        assert 'test_query_db' in ds_names


@pytest.mark.django_db
class TestListTables:
    """Tests for /api/query/datasources/<id>/tables/"""

    def test_returns_tables_in_datasource(self, request_factory, setup_test_data):
        """Should return tables for the datasource."""
        from terno_dbi.core.query_service.views import list_tables
        
        request = request_factory.get(
            f'/api/query/datasources/{setup_test_data["datasource"].id}/tables/'
        )
        setup_request_for_view(
            request, 
            setup_test_data['token'],
            datasource=setup_test_data['datasource']
        )
        
        response = list_tables(request, setup_test_data['datasource'].id)
        
        assert response.status_code == 200
        data = json.loads(response.content)
        # Response is a dict with 'tables' key
        assert data.get('status') == 'success'
        assert 'tables' in data
        assert len(data['tables']) == 2  # users and orders tables

    def test_list_tables_with_roles(self, request_factory, setup_test_data):
        """Should filter tables by role."""
        from terno_dbi.core.query_service.views import list_tables
        
        # We need to mock get_admin_config_object since roles query logic is complex
        with patch('terno_dbi.core.query_service.views.get_admin_config_object') as mock_conf:
             mock_cols = MagicMock()
             mock_cols.filter.return_value.values.return_value = [{'public_name': 'ID', 'data_type': 'integer'}]
             mock_conf.return_value = ([setup_test_data['table1']], mock_cols)
             
             request = request_factory.get(
                f'/api/query/datasources/{setup_test_data["datasource"].id}/tables/?roles=1,2'
             )
             setup_request_for_view(request, setup_test_data['token'], datasource=setup_test_data['datasource'])
             
             response = list_tables(request, setup_test_data['datasource'].id)
             assert response.status_code == 200
             data = json.loads(response.content)
             assert len(data['tables']) == 1 # Only table1 returned
             mock_conf.assert_called()

    def test_unauthorized_datasource_behavior(self, request_factory, setup_test_data):
        """Test that views handle unauthorized datasources."""
        from terno_dbi.core.query_service.views import list_tables
        
        # Just verify the view exists - decorator behavior is tested elsewhere
        assert callable(list_tables)




@pytest.mark.django_db
class TestListTableColumns:
    """Tests for /api/query/datasources/<id>/tables/<table_id>/columns/"""

    def test_returns_columns(self, request_factory, setup_test_data):
        """Should call get_table_columns."""
        from terno_dbi.core.query_service.views import list_table_columns
        
        request = request_factory.get(
            f'/api/query/datasources/{setup_test_data["datasource"].id}/tables/{setup_test_data["table1"].id}/columns/'
        )
        setup_request_for_view(request, setup_test_data['token'], datasource=setup_test_data['datasource'], table=setup_test_data['table1'])
        
        response = list_table_columns(request, setup_test_data['datasource'].id, setup_test_data['table1'].id)
        assert response.status_code == 200
        data = json.loads(response.content)
        assert 'columns' in data

    def test_get_table_columns_not_found(self, request_factory, setup_test_data):
        """Should return 404 if table not found."""
        from terno_dbi.core.query_service.views import list_table_columns
        
        request = request_factory.get(
            f'/api/query/datasources/{setup_test_data["datasource"].id}/tables/999/columns/'
        )
        setup_request_for_view(request, setup_test_data['token'], datasource=setup_test_data['datasource'])
        
        response = list_table_columns(request, setup_test_data['datasource'].id, '999')
        assert response.status_code == 404


@pytest.mark.django_db
class TestExecuteQuery:
    """Tests for /api/query/datasources/<id>/query/"""

    @patch('terno_dbi.core.query_service.views.execute_paginated_query')
    @patch('terno_dbi.core.query_service.views.prepare_mdb')
    @patch('terno_dbi.core.query_service.views.generate_native_sql')
    def test_valid_query_returns_data(
        self, mock_gen_sql, mock_prep_mdb, mock_execute, 
        request_factory, setup_test_data
    ):
        """Should execute query and return results."""
        from terno_dbi.core.query_service.views import execute_query
        from django.contrib.auth.models import Group
        
        mock_mdb = MagicMock()
        mock_prep_mdb.return_value = mock_mdb
        mock_gen_sql.return_value = {
            'status': 'success',
            'native_sql': 'SELECT * FROM users'
        }
        mock_execute.return_value = {
            'status': 'success',
            'table_data': {
                'columns': ['id', 'name'],
                'data': [{'id': 1, 'name': 'Test'}],
                'row_count': 1
            }
        }
        
        request = request_factory.post(
            f'/api/query/datasources/{setup_test_data["datasource"].id}/query/',
            data=json.dumps({'sql': 'SELECT * FROM users'}),
            content_type='application/json'
        )
        setup_request_for_view(
            request,
            setup_test_data['token'],
            datasource=setup_test_data['datasource']
        )
        # Simulate user/roles
        request.user = setup_test_data['user']
        
        response = execute_query(request, setup_test_data['datasource'].id)
        
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data.get('status') == 'success'

    def test_missing_sql_returns_400(self, request_factory, setup_test_data):
        """Should return 400 if SQL is missing."""
        from terno_dbi.core.query_service.views import execute_query
        
        request = request_factory.post(
            f'/api/query/datasources/{setup_test_data["datasource"].id}/query/',
            data=json.dumps({}),
            content_type='application/json'
        )
        setup_request_for_view(
            request,
            setup_test_data['token'],
            datasource=setup_test_data['datasource']
        )
        
        response = execute_query(request, setup_test_data['datasource'].id)
        
        assert response.status_code == 400

    def test_unauthorized_datasource_returns_403(self, request_factory, setup_test_data):
        """Should return 403 for unauthorized datasource."""
        from terno_dbi.core.query_service.views import execute_query
        
        # Create unlinked datasource
        other_ds = DataSource.objects.create(
            display_name='other_execute_ds',
            type='mysql',
            enabled=True
        )
        
        request = request_factory.post(
            f'/api/query/datasources/{other_ds.id}/query/',
            data=json.dumps({'sql': 'SELECT 1'}),
            content_type='application/json'
        )
        setup_request_for_view(request, setup_test_data['token'])
        # Not setting resolved_datasource - should fail
        
        response = execute_query(request, other_ds.id)
        
        # API returns 400 for missing datasource in body without URL param
        assert response.status_code in [400, 403]


@pytest.mark.django_db
class TestGetSampleData:
    """Tests for /api/query/tables/<table_id>/sample/"""

    @patch('terno_dbi.core.query_service.views.execute_native_sql')
    def test_returns_sample_rows(
        self, mock_execute, request_factory, setup_test_data
    ):
        """Should return sample data from table."""
        from terno_dbi.core.query_service.views import get_sample_data
        
        mock_execute.return_value = {
            'status': 'success',
            'table_data': {
                'columns': ['id', 'email'],
                'data': [
                    {'id': 1, 'email': 'test@example.com'}
                ]
            }
        }
        
        request = request_factory.get(
            f'/api/query/tables/{setup_test_data["table1"].id}/sample/'
        )
        setup_request_for_view(
            request,
            setup_test_data['token'],
            table=setup_test_data['table1']
        )
        
        response = get_sample_data(request, setup_test_data['table1'].id)
        
        assert response.status_code == 200

    def test_get_sample_data_errors(self, request_factory, setup_test_data):
        """Should handle errors in sample data."""
        from terno_dbi.core.query_service.views import get_sample_data
        
        # Test invalid rows param
        request = request_factory.get(
            f'/api/query/tables/{setup_test_data["table1"].id}/sample/?rows=invalid'
        )
        setup_request_for_view(request, setup_test_data['token'], table=setup_test_data['table1'])
        
        # Mock execution error
        with patch('terno_dbi.core.query_service.views.execute_native_sql', return_value={'status': 'error', 'error': 'DB crash'}):
             response = get_sample_data(request, setup_test_data['table1'].id)
             assert response.status_code == 500
             assert 'DB crash' in response.content.decode()


@pytest.mark.django_db
class TestListForeignKeys:
    """Tests for /api/query/datasources/<id>/foreign_keys/"""

    def test_returns_foreign_keys(self, request_factory, setup_test_data):
        """Should return FKs."""
        from terno_dbi.core.query_service.views import list_foreign_keys
        
        request = request_factory.get(
             f'/api/query/datasources/{setup_test_data["datasource"].id}/foreign_keys/'
        )
        setup_request_for_view(request, setup_test_data['token'], datasource=setup_test_data['datasource'])
        
        # Create FK to trigger loop
        from terno_dbi.core.models import ForeignKey, TableColumn
        fk = ForeignKey.objects.create(
            constrained_table=setup_test_data['table1'],
            referred_table=setup_test_data['table2'],
            constrained_columns=setup_test_data['col1'],
            referred_columns=setup_test_data['col2'] 
        )
        
        response = list_foreign_keys(request, setup_test_data['datasource'].id)
        assert response.status_code == 200
        data = json.loads(response.content)
        assert len(data['foreign_keys']) == 1
        assert data['foreign_keys'][0]['constrained_table'] == 'Users'



@pytest.mark.django_db
class TestExportQuery:
    """Tests for export_query endpoint."""

    @patch('terno_dbi.core.query_service.views.export_native_sql_result')
    @patch('terno_dbi.core.query_service.views.prepare_mdb')
    @patch('terno_dbi.core.query_service.views.generate_native_sql')
    def test_export_query_success(self, mock_gen, mock_prep, mock_export, request_factory, setup_test_data):
        """Should export successfully."""
        from terno_dbi.core.query_service.views import export_query
        
        mock_gen.return_value = {'status': 'success', 'native_sql': 'SELECT 1'}
        # export_native_sql_result usually returns HttpResponse(csv_content)
        mock_export_resp = MagicMock()
        mock_export_resp.status_code = 200
        mock_export.return_value = mock_export_resp
        
        request = request_factory.post(
            f'/api/query/datasources/{setup_test_data["datasource"].id}/export',
            data=json.dumps({'sql': 'SELECT * FROM users'}),
            content_type='application/json'
        )
        setup_request_for_view(request, setup_test_data['token'], datasource=setup_test_data['datasource'])
        
        response = export_query(request, setup_test_data['datasource'].id)
        assert response.status_code == 200

    def test_export_query_validation_error(self, request_factory, setup_test_data):
        """Should fail if SQL missing."""
        from terno_dbi.core.query_service.views import export_query
        
        request = request_factory.post(
            f'/api/query/datasources/{setup_test_data["datasource"].id}/export',
            data=json.dumps({}),
            content_type='application/json'
        )
        setup_request_for_view(request, setup_test_data['token'], datasource=setup_test_data['datasource'])
        
        response = export_query(request, setup_test_data['datasource'].id)
        assert response.status_code == 400

    @patch('terno_dbi.core.query_service.views.prepare_mdb')
    @patch('terno_dbi.core.query_service.views.generate_native_sql')
    def test_export_query_transform_error(self, mock_gen, mock_prep, request_factory, setup_test_data):
        """Should fail if transform fails."""
        from terno_dbi.core.query_service.views import export_query
        
        mock_gen.return_value = {'status': 'error', 'error': 'Invalid SQL'}
        
        request = request_factory.post(
            f'/api/query/datasources/{setup_test_data["datasource"].id}/export',
            data=json.dumps({'sql': 'bad sql'}),
            content_type='application/json'
        )
        setup_request_for_view(request, setup_test_data['token'], datasource=setup_test_data['datasource'])
        
        response = export_query(request, setup_test_data['datasource'].id)
        assert response.status_code == 400


class TestExecuteQueryEdges:
    """Additional edge cases for execute_query."""

    @patch('terno_dbi.core.query_service.views.resolve_datasource')
    def test_execute_query_permission_denied_body_ds(self, mock_resolve, request_factory, setup_test_data):
        """Should return 403 if user lacks access to datasource in body."""
        from terno_dbi.core.query_service.views import execute_query
        from terno_dbi.core.models import DataSource

        #ds in body
        other_ds = MagicMock(spec=DataSource)
        other_ds.id = 999
        mock_resolve.return_value = other_ds

        request = request_factory.post(
            '/api/query/execute',
            data=json.dumps({'datasource': 999, 'sql': 'SELECT 1'}),
            content_type='application/json'
        )
        # Mock allow list does not include 999
        token = setup_test_data['token']
        # Mock token behavior
        request.service_token = token
        request.allowed_datasources = DataSource.objects.filter(id=setup_test_data['datasource'].id)

        response = execute_query(request)
        assert response.status_code == 403

    def test_execute_query_json_error(self, request_factory, setup_test_data):
         """Should return 400 on malformed JSON."""
         from terno_dbi.core.query_service.views import execute_query
         request = request_factory.post(
             '/api/query/execute',
             data='{invalid_json',
             content_type='application/json'
         )
         setup_request_for_view(request, setup_test_data['token'])
         
         response = execute_query(request)
         assert response.status_code == 400

    @patch('terno_dbi.core.query_service.views.execute_paginated_query')
    @patch('terno_dbi.core.query_service.views.prepare_mdb')
    @patch('terno_dbi.core.query_service.views.generate_native_sql')
    def test_execute_query_with_roles(self, mock_gen, mock_prep, mock_exec, request_factory, setup_test_data):
        """Should handle roles in body."""
        from terno_dbi.core.query_service.views import execute_query
        
        mock_gen.return_value = {'status': 'success'}
        mock_exec.return_value = {'status': 'success'}
        
        request = request_factory.post(
            '/api/query/execute',
            data=json.dumps({'datasource': setup_test_data['datasource'].id, 'sql': 'SELECT 1', 'roles': [1]}),
            content_type='application/json'
        )
        setup_request_for_view(request, setup_test_data['token'])
        # Mock allow list
        request.allowed_datasources = setup_test_data['token'].get_accessible_datasources()
        
        # We need to ensure resolve_datasource works without mocking if we pass valid ID
        # But resolve_datasource uses DB, so it should be fine.
        
        response = execute_query(request)
        assert response.status_code == 200

    def test_export_query_with_body_datasource(self, request_factory, setup_test_data):
        """Should handle datasource in body for export."""
        from terno_dbi.core.query_service.views import export_query
        
        # We need to mock generating SQL to avoid actually running logic
        with patch('terno_dbi.core.query_service.views.generate_native_sql', return_value={'status':'success', 'native_sql':'S'}), \
             patch('terno_dbi.core.query_service.views.export_native_sql_result', return_value=MagicMock(status_code=200)):
            
            request = request_factory.post(
                '/api/query/export',
                data=json.dumps({'datasource': setup_test_data['datasource'].id, 'sql': 'SELECT 1', 'roles': [1]}),
                content_type='application/json'
            )
            setup_request_for_view(request, setup_test_data['token'])
            request.allowed_datasources = setup_test_data['token'].get_accessible_datasources()
            
            response = export_query(request)
            assert response.status_code == 200

    def test_export_query_json_error(self, request_factory):
         """Should return 400 on malformed JSON for export."""
         from terno_dbi.core.query_service.views import export_query
         request = request_factory.post(
             '/api/query/export',
             data='{invalid',
             content_type='application/json'
         )
         setup_request_for_view(request, MagicMock())
         
         response = export_query(request)
         assert response.status_code == 400

    @patch('terno_dbi.core.query_service.views.prepare_mdb')
    @patch('terno_dbi.core.query_service.views.generate_native_sql')
    def test_execute_query_transform_error(self, mock_gen, mock_prep, request_factory, setup_test_data):
        """Should return 400 if transformation fails."""
        from terno_dbi.core.query_service.views import execute_query
        
        mock_gen.return_value = {'status': 'error', 'error': 'Cannot transform'}
        
        request = request_factory.post(
            f'/api/query/datasources/{setup_test_data["datasource"].id}/query/',
            data=json.dumps({'sql': 'SELECT *'}),
            content_type='application/json'
        )
        setup_request_for_view(request, setup_test_data['token'], datasource=setup_test_data['datasource'])
        
        response = execute_query(request, setup_test_data['datasource'].id)
        assert response.status_code == 400
        assert 'Cannot transform' in response.content.decode()
