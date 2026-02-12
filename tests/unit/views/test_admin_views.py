"""
Unit tests for Admin Service Views.

Tests all endpoints in admin_service/views.py including:
- create_datasource, update_datasource, delete_datasource
- update_table, update_column
- validate_connection, sync_metadata
- get_table_info
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
def setup_admin_data(db):
    """Create test user, admin token, datasource, tables."""
    from django.contrib.auth.models import User
    
    user = User.objects.create_user('adminviewuser', 'admin@example.com', 'password')
    
    ds = DataSource.objects.create(
        display_name='admin_test_db',
        type='postgres',
        connection_str='postgresql://localhost/admintest',
        enabled=True
    )
    
    table1 = Table.objects.create(
        name='products',
        public_name='Products',
        data_source=ds,
        description='Product catalog'
    )
    
    col1 = TableColumn.objects.create(
        name='product_id',
        public_name='ProductID',
        table=table1,
        data_type='integer'
    )
    
    # Create ADMIN token
    token_key = 'dbi_admin_testadmintoken456'
    key_hash = hashlib.sha256(token_key.encode()).hexdigest()
    token = ServiceToken.objects.create(
        name='Admin Test Token',
        token_type=ServiceToken.TokenType.ADMIN,
        key_prefix='dbi_admin_',
        key_hash=key_hash,
        is_active=True,
        created_by=user
    )
    token.datasources.add(ds)
    
    return {
        'user': user,
        'datasource': ds,
        'table1': table1,
        'col1': col1,
        'token': token,
        'token_key': token_key
    }


def setup_request_for_admin(request, token, datasource=None, table=None, column=None):
    """Set up request attributes as the require_service_auth decorator would."""
    request.service_token = token
    request.allowed_datasources = token.get_accessible_datasources()
    if datasource:
        request.resolved_datasource = datasource
    if table:
        request.resolved_table = table
    if column:
        request.resolved_column = column


@pytest.mark.django_db
class TestCreateDatasource:
    """Tests for POST /api/admin/datasources/"""

    @patch('terno_dbi.core.admin_service.views.validate_datasource_input')
    def test_creates_datasource_success(
        self, mock_validate, request_factory, setup_admin_data
    ):
        """Should create datasource with valid input."""
        from terno_dbi.core.admin_service.views import create_datasource
        
        mock_validate.return_value = None  # No validation error
        
        request = request_factory.post(
            '/api/admin/datasources/',
            data=json.dumps({
                'display_name': 'new_production_db',
                'type': 'postgres',
                'connection_str': 'postgresql://localhost/newdb'
            }),
            content_type='application/json'
        )
        setup_request_for_admin(request, setup_admin_data['token'])
        
        response = create_datasource(request)
        
        assert response.status_code == 201
        data = json.loads(response.content)
        assert data.get('status') == 'success'

    def test_missing_required_fields_returns_400(
        self, request_factory, setup_admin_data
    ):
        """Should return 400 if required fields missing."""
        from terno_dbi.core.admin_service.views import create_datasource
        
        request = request_factory.post(
            '/api/admin/datasources/',
            data=json.dumps({'display_name': 'incomplete'}),
            content_type='application/json'
        )
        setup_request_for_admin(request, setup_admin_data['token'])
        
        response = create_datasource(request)
        
        assert response.status_code == 400

    @patch('terno_dbi.core.admin_service.views.validate_datasource_input')
    def test_invalid_connection_returns_400(
        self, mock_validate, request_factory, setup_admin_data
    ):
        """Should return 400 if connection validation fails."""
        from terno_dbi.core.admin_service.views import create_datasource
        
        mock_validate.return_value = "Could not connect to database"
        
        request = request_factory.post(
            '/api/admin/datasources/',
            data=json.dumps({
                'display_name': 'invalid_db',
                'type': 'postgres',
                'connection_str': 'postgresql://invalid/db'
            }),
            content_type='application/json'
        )
        setup_request_for_admin(request, setup_admin_data['token'])
        
        response = create_datasource(request)
        
        assert response.status_code == 400


@pytest.mark.django_db
class TestUpdateDatasource:
    """Tests for PATCH /api/admin/datasources/<id>/"""

    def test_updates_display_name(self, request_factory, setup_admin_data):
        """Should update datasource display name."""
        from terno_dbi.core.admin_service.views import update_datasource
        
        # Use PATCH instead of PUT
        request = request_factory.patch(
            f'/api/admin/datasources/{setup_admin_data["datasource"].id}/',
            data=json.dumps({'display_name': 'updated_name'}),
            content_type='application/json'
        )
        setup_request_for_admin(
            request, 
            setup_admin_data['token'],
            datasource=setup_admin_data['datasource']
        )
        
        response = update_datasource(request, setup_admin_data['datasource'].id)
        
        assert response.status_code == 200
        setup_admin_data['datasource'].refresh_from_db()
        assert setup_admin_data['datasource'].display_name == 'updated_name'


@pytest.mark.django_db
class TestDeleteDatasource:
    """Tests for DELETE /api/admin/datasources/<id>/"""

    def test_deletes_datasource(self, request_factory, setup_admin_data):
        """Should delete the datasource."""
        from terno_dbi.core.admin_service.views import delete_datasource
        
        ds_id = setup_admin_data['datasource'].id
        
        request = request_factory.delete(
            f'/api/admin/datasources/{ds_id}/'
        )
        setup_request_for_admin(
            request,
            setup_admin_data['token'],
            datasource=setup_admin_data['datasource']
        )
        
        response = delete_datasource(request, ds_id)
        
        assert response.status_code == 200
        assert not DataSource.objects.filter(id=ds_id).exists()


@pytest.mark.django_db
class TestUpdateTable:
    """Tests for PATCH /api/admin/tables/<id>/"""

    def test_updates_public_name(self, request_factory, setup_admin_data):
        """Should update table public name."""
        from terno_dbi.core.admin_service.views import update_table
        
        # Use PATCH instead of PUT
        request = request_factory.patch(
            f'/api/admin/tables/{setup_admin_data["table1"].id}/',
            data=json.dumps({
                'public_name': 'Product Catalog',
                'description': 'Updated description'
            }),
            content_type='application/json'
        )
        setup_request_for_admin(
            request,
            setup_admin_data['token'],
            table=setup_admin_data['table1']
        )
        
        response = update_table(request, setup_admin_data['table1'].id)
        
        assert response.status_code == 200
        setup_admin_data['table1'].refresh_from_db()
        assert setup_admin_data['table1'].public_name == 'Product Catalog'


@pytest.mark.django_db
class TestUpdateColumn:
    """Tests for PATCH /api/admin/columns/<id>/"""

    def test_updates_column_description(self, request_factory, setup_admin_data):
        """Should update column description."""
        from terno_dbi.core.admin_service.views import update_column
        
        # Use PATCH instead of PUT
        request = request_factory.patch(
            f'/api/admin/columns/{setup_admin_data["col1"].id}/',
            data=json.dumps({
                'public_name': 'Product ID',
                'description': 'Unique product identifier'
            }),
            content_type='application/json'
        )
        setup_request_for_admin(
            request,
            setup_admin_data['token'],
            column=setup_admin_data['col1']
        )
        
        response = update_column(request, setup_admin_data['col1'].id)
        
        assert response.status_code == 200
        setup_admin_data['col1'].refresh_from_db()
        assert setup_admin_data['col1'].public_name == 'Product ID'


@pytest.mark.django_db
class TestValidateConnection:
    """Tests for POST /api/admin/validate-connection/"""

    @patch('terno_dbi.core.admin_service.views.validate_datasource_input')
    def test_valid_connection_returns_success(
        self, mock_validate, request_factory, setup_admin_data
    ):
        """Should return success for valid connection."""
        from terno_dbi.core.admin_service.views import validate_connection
        
        mock_validate.return_value = None  # No error = valid
        
        request = request_factory.post(
            '/api/admin/validate-connection/',
            data=json.dumps({
                'type': 'postgres',
                'connection_str': 'postgresql://localhost/valid'
            }),
            content_type='application/json'
        )
        setup_request_for_admin(request, setup_admin_data['token'])
        
        response = validate_connection(request)
        
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data.get('valid') is True

    @patch('terno_dbi.core.admin_service.views.validate_datasource_input')
    def test_invalid_connection_returns_error(
        self, mock_validate, request_factory, setup_admin_data
    ):
        """Should return error details for invalid connection."""
        from terno_dbi.core.admin_service.views import validate_connection
        
        mock_validate.return_value = "Connection refused"
        
        request = request_factory.post(
            '/api/admin/validate-connection/',
            data=json.dumps({
                'type': 'postgres',
                'connection_str': 'postgresql://invalid/db'
            }),
            content_type='application/json'
        )
        setup_request_for_admin(request, setup_admin_data['token'])
        
        response = validate_connection(request)
        
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data.get('valid') is False
        assert 'error' in data


@pytest.mark.django_db
class TestSyncMetadata:
    """Tests for POST /api/admin/datasources/<id>/sync/"""

    @patch('terno_dbi.services.schema_utils.sync_metadata')
    def test_sync_returns_success(
        self, mock_sync, request_factory, setup_admin_data
    ):
        """Should sync and return success status."""
        # The view is named sync_metadata, same as the service function
        from terno_dbi.core.admin_service.views import sync_metadata as sync_metadata_view
        
        mock_sync.return_value = {}  # sync_metadata returns dict
        
        request = request_factory.post(
            f'/api/admin/datasources/{setup_admin_data["datasource"].id}/sync/'
        )
        setup_request_for_admin(
            request,
            setup_admin_data['token'],
            datasource=setup_admin_data['datasource']
        )
        
        response = sync_metadata_view(request, setup_admin_data['datasource'].id)
        
        assert response.status_code == 200


@pytest.mark.django_db
class TestGetTableInfo:
    """Tests for GET /api/admin/datasources/<id>/tables/<name>/info/"""

    def test_returns_table_with_columns(self, request_factory, setup_admin_data):
        """Should return table info with columns."""
        from terno_dbi.core.admin_service.views import get_table_info
        
        request = request_factory.get(
            f'/api/admin/datasources/{setup_admin_data["datasource"].id}/tables/products/info/'
        )
        setup_request_for_admin(
            request,
            setup_admin_data['token'],
            datasource=setup_admin_data['datasource']
        )
        
        response = get_table_info(
            request, 
            setup_admin_data['datasource'].id,
            'products'
        )
        
        assert response.status_code == 200
        data = json.loads(response.content)
        assert 'columns' in data or 'table' in data

