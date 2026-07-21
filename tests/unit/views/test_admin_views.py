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



@pytest.mark.django_db
class TestUpdateOrgPrompt:
    """Tests for POST /api/admin/organisation/prompt/"""

    def _make_org_and_token(self, token_type=None, scopes=None):
        from django.contrib.auth.models import User
        from terno_dbi.core.models import CoreOrganisation, ServiceToken

        token_type = token_type or ServiceToken.TokenType.ADMIN
        user = User.objects.create_user('orgpromptadmin', 'orgpromptadmin@example.com', 'password')
        org = CoreOrganisation.objects.create(name='Admin Prompt Org', subdomain='adminpromptorg', owner=user)
        token = ServiceToken.objects.create(
            name='Admin Prompt Token',
            token_type=token_type,
            created_by=user,
            organisation=org,
            scopes=scopes or [],
            key_hash='admin-prompt-hash',
        )
        return org, token

    def test_updates_org_prompt(self, request_factory):
        from terno_dbi.core.admin_service.views import update_org_prompt

        org, token = self._make_org_and_token()
        request = request_factory.post(
            '/api/admin/organisation/prompt/',
            data=json.dumps({'org_prompt': 'Be concise and use bullet points.'}),
            content_type='application/json'
        )
        request.service_token = token

        response = update_org_prompt(request)

        assert response.status_code == 200
        data = json.loads(response.content)
        assert data['status'] == 'success'
        assert data['org_prompt'] == 'Be concise and use bullet points.'
        org.refresh_from_db()
        assert org.org_prompt == 'Be concise and use bullet points.'

    def test_replaces_existing_prompt_with_correct_hash(self, request_factory):
        from terno_dbi.core.admin_service.views import update_org_prompt

        org, token = self._make_org_and_token()
        org.org_prompt = 'Old prompt'
        org.save(update_fields=['org_prompt'])
        expected_hash = org.org_prompt_hash

        request = request_factory.post(
            '/api/admin/organisation/prompt/',
            data=json.dumps({'org_prompt': 'New prompt', 'expected_hash': expected_hash}),
            content_type='application/json'
        )
        request.service_token = token

        response = update_org_prompt(request)

        assert response.status_code == 200
        data = json.loads(response.content)
        assert data['content_hash'] == org.__class__.objects.get(pk=org.pk).org_prompt_hash
        org.refresh_from_db()
        assert org.org_prompt == 'New prompt'

    def test_replacing_existing_prompt_without_hash_returns_409(self, request_factory):
        from terno_dbi.core.admin_service.views import update_org_prompt

        org, token = self._make_org_and_token()
        org.org_prompt = 'Old prompt'
        org.save(update_fields=['org_prompt'])

        request = request_factory.post(
            '/api/admin/organisation/prompt/',
            data=json.dumps({'org_prompt': 'New prompt'}),
            content_type='application/json'
        )
        request.service_token = token

        response = update_org_prompt(request)

        assert response.status_code == 409
        org.refresh_from_db()
        assert org.org_prompt == 'Old prompt'

    def test_replacing_existing_prompt_with_stale_hash_returns_409(self, request_factory):
        from terno_dbi.core.admin_service.views import update_org_prompt

        org, token = self._make_org_and_token()
        org.org_prompt = 'Old prompt'
        org.save(update_fields=['org_prompt'])

        request = request_factory.post(
            '/api/admin/organisation/prompt/',
            data=json.dumps({'org_prompt': 'New prompt', 'expected_hash': 'stale-hash'}),
            content_type='application/json'
        )
        request.service_token = token

        response = update_org_prompt(request)

        assert response.status_code == 409
        org.refresh_from_db()
        assert org.org_prompt == 'Old prompt'

    def test_first_write_to_blank_prompt_does_not_require_hash(self, request_factory):
        from terno_dbi.core.admin_service.views import update_org_prompt

        org, token = self._make_org_and_token()
        assert org.org_prompt == ''

        request = request_factory.post(
            '/api/admin/organisation/prompt/',
            data=json.dumps({'org_prompt': 'First value'}),
            content_type='application/json'
        )
        request.service_token = token

        response = update_org_prompt(request)

        assert response.status_code == 200
        org.refresh_from_db()
        assert org.org_prompt == 'First value'

    def test_rejects_prompt_over_length_cap(self, request_factory):
        from terno_dbi.core.admin_service.views import (
            update_org_prompt, AGENT_ORG_PROMPT_MAX_CHARS,
        )

        org, token = self._make_org_and_token()
        too_long = 'x' * (AGENT_ORG_PROMPT_MAX_CHARS + 1)

        request = request_factory.post(
            '/api/admin/organisation/prompt/',
            data=json.dumps({'org_prompt': too_long}),
            content_type='application/json'
        )
        request.service_token = token

        response = update_org_prompt(request)

        assert response.status_code == 400
        org.refresh_from_db()
        assert org.org_prompt == ''

    def test_accepts_prompt_at_length_cap(self, request_factory):
        from terno_dbi.core.admin_service.views import (
            update_org_prompt, AGENT_ORG_PROMPT_MAX_CHARS,
        )

        org, token = self._make_org_and_token()
        at_cap = 'x' * AGENT_ORG_PROMPT_MAX_CHARS

        request = request_factory.post(
            '/api/admin/organisation/prompt/',
            data=json.dumps({'org_prompt': at_cap}),
            content_type='application/json'
        )
        request.service_token = token

        response = update_org_prompt(request)

        assert response.status_code == 200
        org.refresh_from_db()
        assert org.org_prompt == at_cap

    def test_defaults_to_empty_when_missing(self, request_factory):
        from terno_dbi.core.admin_service.views import update_org_prompt

        org, token = self._make_org_and_token()
        request = request_factory.post(
            '/api/admin/organisation/prompt/',
            data=json.dumps({}),
            content_type='application/json'
        )
        request.service_token = token

        response = update_org_prompt(request)

        assert response.status_code == 200
        org.refresh_from_db()
        assert org.org_prompt == ''

    def test_invalid_json_returns_400(self, request_factory):
        from terno_dbi.core.admin_service.views import update_org_prompt

        org, token = self._make_org_and_token()
        request = request_factory.post(
            '/api/admin/organisation/prompt/',
            data='{invalid',
            content_type='application/json'
        )
        request.service_token = token

        response = update_org_prompt(request)

        assert response.status_code == 400

    def test_no_organisation_returns_400(self, request_factory):
        from django.contrib.auth.models import User
        from terno_dbi.core.models import DataSource, ServiceToken
        from terno_dbi.core.admin_service.views import update_org_prompt

        user = User.objects.create_user('noorgadmin', 'noorgadmin@example.com', 'password')
        ds = DataSource.objects.create(
            display_name='no_org_admin_ds', type='postgres',
            connection_str='postgresql://localhost/noorgadmin', enabled=True
        )
        token = ServiceToken.objects.create(
            name='No Org Admin Token',
            token_type=ServiceToken.TokenType.ADMIN,
            created_by=user,
            organisation=None,
            key_hash='no-org-admin-hash',
        )
        token.datasources.add(ds)
        request = request_factory.post(
            '/api/admin/organisation/prompt/',
            data=json.dumps({'org_prompt': 'x'}),
            content_type='application/json'
        )
        request.service_token = token

        response = update_org_prompt(request)

        assert response.status_code == 400

    def test_query_token_without_admin_write_scope_forbidden(self, request_factory):
        from terno_dbi.core.models import ServiceToken
        from terno_dbi.core.admin_service.views import update_org_prompt

        _, token = self._make_org_and_token(token_type=ServiceToken.TokenType.QUERY)
        request = request_factory.post(
            '/api/admin/organisation/prompt/',
            data=json.dumps({'org_prompt': 'x'}),
            content_type='application/json'
        )
        request.service_token = token

        response = update_org_prompt(request)

        assert response.status_code == 403

    def test_requires_authentication(self, request_factory):
        from terno_dbi.core.admin_service.views import update_org_prompt

        request = request_factory.post(
            '/api/admin/organisation/prompt/',
            data=json.dumps({'org_prompt': 'x'}),
            content_type='application/json'
        )

        response = update_org_prompt(request)

        assert response.status_code == 401


@pytest.mark.django_db
class TestEditOrgPrompt:
    """Tests for POST /api/admin/organisation/prompt/edit/"""

    def _make_org_and_token(self, token_type=None, scopes=None):
        from django.contrib.auth.models import User
        from terno_dbi.core.models import CoreOrganisation, ServiceToken

        token_type = token_type or ServiceToken.TokenType.ADMIN
        user = User.objects.create_user('editorgpromptadmin', 'editorgpromptadmin@example.com', 'password')
        org = CoreOrganisation.objects.create(
            name='Edit Prompt Org', subdomain='editpromptorg', owner=user,
            org_prompt='Always answer in French. Be concise.'
        )
        token = ServiceToken.objects.create(
            name='Edit Prompt Token',
            token_type=token_type,
            created_by=user,
            organisation=org,
            scopes=scopes or [],
            key_hash='edit-prompt-hash',
        )
        return org, token

    def test_edit_replaces_unique_substring(self, request_factory):
        from terno_dbi.core.admin_service.views import edit_org_prompt

        org, token = self._make_org_and_token()
        request = request_factory.post(
            '/api/admin/organisation/prompt/edit/',
            data=json.dumps({
                'old_string': 'French',
                'new_string': 'Spanish',
                'expected_hash': org.org_prompt_hash,
            }),
            content_type='application/json'
        )
        request.service_token = token

        response = edit_org_prompt(request)

        assert response.status_code == 200
        org.refresh_from_db()
        assert org.org_prompt == 'Always answer in Spanish. Be concise.'

    def test_edit_requires_expected_hash(self, request_factory):
        from terno_dbi.core.admin_service.views import edit_org_prompt

        org, token = self._make_org_and_token()
        request = request_factory.post(
            '/api/admin/organisation/prompt/edit/',
            data=json.dumps({'old_string': 'French', 'new_string': 'Spanish'}),
            content_type='application/json'
        )
        request.service_token = token

        response = edit_org_prompt(request)

        assert response.status_code == 409
        org.refresh_from_db()
        assert org.org_prompt == 'Always answer in French. Be concise.'

    def test_edit_with_stale_hash_returns_409(self, request_factory):
        from terno_dbi.core.admin_service.views import edit_org_prompt

        org, token = self._make_org_and_token()
        request = request_factory.post(
            '/api/admin/organisation/prompt/edit/',
            data=json.dumps({
                'old_string': 'French', 'new_string': 'Spanish', 'expected_hash': 'stale',
            }),
            content_type='application/json'
        )
        request.service_token = token

        response = edit_org_prompt(request)

        assert response.status_code == 409

    def test_edit_old_string_not_found_returns_400(self, request_factory):
        from terno_dbi.core.admin_service.views import edit_org_prompt

        org, token = self._make_org_and_token()
        request = request_factory.post(
            '/api/admin/organisation/prompt/edit/',
            data=json.dumps({
                'old_string': 'Klingon', 'new_string': 'Spanish', 'expected_hash': org.org_prompt_hash,
            }),
            content_type='application/json'
        )
        request.service_token = token

        response = edit_org_prompt(request)

        assert response.status_code == 400

    def test_edit_non_unique_old_string_without_replace_all_returns_400(self, request_factory):
        from terno_dbi.core.admin_service.views import edit_org_prompt

        org, token = self._make_org_and_token()
        org.org_prompt = 'repeat repeat repeat'
        org.save(update_fields=['org_prompt'])

        request = request_factory.post(
            '/api/admin/organisation/prompt/edit/',
            data=json.dumps({
                'old_string': 'repeat', 'new_string': 'once', 'expected_hash': org.org_prompt_hash,
            }),
            content_type='application/json'
        )
        request.service_token = token

        response = edit_org_prompt(request)

        assert response.status_code == 400
        org.refresh_from_db()
        assert org.org_prompt == 'repeat repeat repeat'

    def test_edit_replace_all_replaces_every_occurrence(self, request_factory):
        from terno_dbi.core.admin_service.views import edit_org_prompt

        org, token = self._make_org_and_token()
        org.org_prompt = 'repeat repeat repeat'
        org.save(update_fields=['org_prompt'])

        request = request_factory.post(
            '/api/admin/organisation/prompt/edit/',
            data=json.dumps({
                'old_string': 'repeat', 'new_string': 'once',
                'expected_hash': org.org_prompt_hash, 'replace_all': True,
            }),
            content_type='application/json'
        )
        request.service_token = token

        response = edit_org_prompt(request)

        assert response.status_code == 200
        org.refresh_from_db()
        assert org.org_prompt == 'once once once'

    def test_edit_missing_old_string_returns_400(self, request_factory):
        from terno_dbi.core.admin_service.views import edit_org_prompt

        org, token = self._make_org_and_token()
        request = request_factory.post(
            '/api/admin/organisation/prompt/edit/',
            data=json.dumps({'new_string': 'x', 'expected_hash': org.org_prompt_hash}),
            content_type='application/json'
        )
        request.service_token = token

        response = edit_org_prompt(request)

        assert response.status_code == 400

    def test_edit_requires_authentication(self, request_factory):
        from terno_dbi.core.admin_service.views import edit_org_prompt

        request = request_factory.post(
            '/api/admin/organisation/prompt/edit/',
            data=json.dumps({'old_string': 'a', 'new_string': 'b', 'expected_hash': 'x'}),
            content_type='application/json'
        )

        response = edit_org_prompt(request)

        assert response.status_code == 401

    def test_edit_query_token_without_admin_write_scope_forbidden(self, request_factory):
        from terno_dbi.core.models import ServiceToken
        from terno_dbi.core.admin_service.views import edit_org_prompt

        org, token = self._make_org_and_token(token_type=ServiceToken.TokenType.QUERY)
        request = request_factory.post(
            '/api/admin/organisation/prompt/edit/',
            data=json.dumps({
                'old_string': 'French', 'new_string': 'Spanish', 'expected_hash': org.org_prompt_hash,
            }),
            content_type='application/json'
        )
        request.service_token = token

        response = edit_org_prompt(request)

        assert response.status_code == 403

    def test_edit_allowed_to_grow_prompt_past_update_cap(self, request_factory):
        """edit_org_prompt has no length cap (matching the workspace `edit` tool) —
        only the full-replace path (update_org_prompt) is capped. An agent builds
        a large org_prompt incrementally via edit, same as write-then-edit for
        oversized files."""
        from terno_dbi.core.admin_service.views import (
            edit_org_prompt, AGENT_ORG_PROMPT_MAX_CHARS,
        )

        org, token = self._make_org_and_token()
        org.org_prompt = 'A' + 'x' * (AGENT_ORG_PROMPT_MAX_CHARS - 1)
        org.save(update_fields=['org_prompt'])

        big_replacement = 'B' * (AGENT_ORG_PROMPT_MAX_CHARS + 10)
        request = request_factory.post(
            '/api/admin/organisation/prompt/edit/',
            data=json.dumps({
                'old_string': 'A', 'new_string': big_replacement,
                'expected_hash': org.org_prompt_hash,
            }),
            content_type='application/json'
        )
        request.service_token = token

        response = edit_org_prompt(request)

        assert response.status_code == 200
        org.refresh_from_db()
        assert org.org_prompt == big_replacement + 'x' * (AGENT_ORG_PROMPT_MAX_CHARS - 1)
        assert len(org.org_prompt) > AGENT_ORG_PROMPT_MAX_CHARS

    def test_edit_allowed_to_shrink_oversized_prompt(self, request_factory):
        from terno_dbi.core.admin_service.views import (
            edit_org_prompt, AGENT_ORG_PROMPT_MAX_CHARS,
        )

        org, token = self._make_org_and_token()
        org.org_prompt = 'typo ' + 'x' * (AGENT_ORG_PROMPT_MAX_CHARS + 500)
        org.save(update_fields=['org_prompt'])

        request = request_factory.post(
            '/api/admin/organisation/prompt/edit/',
            data=json.dumps({
                'old_string': 'typo ', 'new_string': '',  # shrinks it
                'expected_hash': org.org_prompt_hash,
            }),
            content_type='application/json'
        )
        request.service_token = token

        response = edit_org_prompt(request)

        assert response.status_code == 200
        org.refresh_from_db()
        assert not org.org_prompt.startswith('typo ')
