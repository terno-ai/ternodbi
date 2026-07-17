import pytest
import json
from unittest.mock import patch, MagicMock
from django.test import RequestFactory
from django.contrib.auth.models import User

from terno_dbi.core.models import CoreOrganisation, ServiceToken
from terno_dbi.core.query_service import views as memory_views
from terno_dbi.services.memory import MemoryNotFound, MemoryConflict, MemoryPermission

pytestmark = pytest.mark.django_db


@pytest.fixture
def request_factory():
    return RequestFactory()


@pytest.fixture
def setup_test_data(db):
    user = User.objects.create_user('viewuser', 'view@example.com', 'password')
    org = CoreOrganisation.objects.create(name='Test Org', subdomain='testorg', owner=user)
    
    token = ServiceToken.objects.create(
        name='Test Token',
        token_type=ServiceToken.TokenType.QUERY,
        created_by=user,
        created_for=user,   # memory identity is read from created_for, not created_by
        organisation=org,
        key_hash='hash1'
    )

    admin_token = ServiceToken.objects.create(
        name='Admin Token',
        token_type=ServiceToken.TokenType.ADMIN,
        created_by=user,
        created_for=user,   # memory identity is read from created_for, not created_by
        organisation=org,
        key_hash='hash2'
    )
    
    return {
        'org': org,
        'user': user,
        'token': token,
        'admin_token': admin_token
    }


def setup_request_for_view(request, token):
    request.service_token = token
    request.token_organisation = token.organisation


class TestMemoryViews:
    @patch('terno_dbi.core.query_service.views.memory_service.list_memories')
    def test_list_memories(self, mock_list, request_factory, setup_test_data):
        mock_list.return_value = [{"name": "test-mem"}]
        
        request = request_factory.get('/api/query/memory/')
        setup_request_for_view(request, setup_test_data['token'])
        
        response = memory_views.list_memories(request)
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data['status'] == 'success'
        assert data['count'] == 1
        assert data['memories'][0]['name'] == 'test-mem'
        
        mock_list.assert_called_once_with(
            organisation_id=setup_test_data['org'].id,
            user_id=setup_test_data['user'].id,
            data_source_id=None
        )

    @patch('terno_dbi.core.query_service.views.memory_service.list_memories')
    @patch('terno_dbi.core.query_service.views.memory_service.render_index')
    def test_list_memories_render(self, mock_render, mock_list, request_factory, setup_test_data):
        mock_list.return_value = []
        mock_render.return_value = "# index"
        
        request = request_factory.get('/api/query/memory/?render=1')
        setup_request_for_view(request, setup_test_data['token'])
        
        response = memory_views.list_memories(request)
        data = json.loads(response.content)
        assert 'index' in data
        assert data['index'] == '# index'

    @patch('terno_dbi.core.query_service.views.memory_service.serialize')
    @patch('terno_dbi.core.query_service.views.memory_service.read_memory')
    def test_get_memory_success(self, mock_read, mock_serialize, request_factory, setup_test_data):
        mock_mem = MagicMock()
        mock_read.return_value = mock_mem
        mock_serialize.return_value = {"name": "test-mem"}
        
        request = request_factory.get('/api/query/memory/test-mem/')
        setup_request_for_view(request, setup_test_data['token'])
        
        response = memory_views.get_memory(request, name="test-mem")
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data['memory']['name'] == 'test-mem'

    @patch('terno_dbi.core.query_service.views.memory_service.read_memory')
    def test_get_memory_not_found(self, mock_read, request_factory, setup_test_data):
        mock_read.side_effect = MemoryNotFound("test-mem")
        
        request = request_factory.get('/api/query/memory/test-mem/')
        setup_request_for_view(request, setup_test_data['token'])
        
        response = memory_views.get_memory(request, name="test-mem")
        assert response.status_code == 404

    @patch('terno_dbi.core.query_service.views.memory_service.grep_memory')
    def test_grep_memory(self, mock_grep, request_factory, setup_test_data):
        mock_grep.return_value = [{"name": "match-mem"}]
        
        request = request_factory.get('/api/query/memory/grep/?pattern=test')
        setup_request_for_view(request, setup_test_data['token'])
        
        response = memory_views.grep_memory(request)
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data['count'] == 1
        assert data['matches'][0]['name'] == 'match-mem'

    @patch('terno_dbi.core.query_service.views.memory_service.write_memory')
    def test_save_memory_success(self, mock_write, request_factory, setup_test_data):
        mock_mem = MagicMock()
        mock_mem.name = "new-mem"
        mock_mem.store = "user"
        mock_mem.data_source_id = None
        mock_mem.content_hash = "hash123"
        mock_write.return_value = (mock_mem, "create")
        
        payload = {"name": "new-mem", "content": "c", "store": "user"}
        request = request_factory.post('/api/query/memory/save/', data=json.dumps(payload), content_type='application/json')
        setup_request_for_view(request, setup_test_data['token'])
        
        response = memory_views.save_memory(request)
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data['status'] == 'success'
        assert data['action'] == 'create'
        assert data['memory']['name'] == 'new-mem'

    def test_save_memory_org_permission(self, request_factory, setup_test_data):
        # Query token does not have admin:write scope, so saving org store should fail
        payload = {"name": "new-mem", "content": "c", "store": "org"}
        request = request_factory.post('/api/query/memory/save/', data=json.dumps(payload), content_type='application/json')
        setup_request_for_view(request, setup_test_data['token'])
        
        response = memory_views.save_memory(request)
        assert response.status_code == 403

    @patch('terno_dbi.core.query_service.views.memory_service.write_memory')
    def test_save_memory_admin_org(self, mock_write, request_factory, setup_test_data):
        # Admin token can save org store
        mock_mem = MagicMock()
        mock_mem.name = "new-mem"
        mock_mem.store = "org"
        mock_mem.data_source_id = None
        mock_mem.content_hash = "hash123"
        mock_write.return_value = (mock_mem, "create")
        
        payload = {"name": "new-mem", "content": "c", "store": "org"}
        request = request_factory.post('/api/query/memory/save/', data=json.dumps(payload), content_type='application/json')
        setup_request_for_view(request, setup_test_data['admin_token'])
        
        response = memory_views.save_memory(request)
        assert response.status_code == 200

    @patch('terno_dbi.core.query_service.views.memory_service.edit_memory')
    def test_edit_memory_success(self, mock_edit, request_factory, setup_test_data):
        mock_mem = MagicMock()
        mock_mem.name = "edit-mem"
        mock_mem.content_hash = "hash123"
        mock_edit.return_value = mock_mem
        
        payload = {"old_string": "old", "new_string": "new", "expected_hash": "hash0", "store": "user"}
        request = request_factory.post('/api/query/memory/edit-mem/edit/', data=json.dumps(payload), content_type='application/json')
        setup_request_for_view(request, setup_test_data['token'])
        
        response = memory_views.edit_memory(request, name="edit-mem")
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data['status'] == 'success'

    @patch('terno_dbi.core.query_service.views.memory_service.delete_memory')
    def test_delete_memory_success(self, mock_delete, request_factory, setup_test_data):
        mock_delete.return_value = 1
        
        payload = {"store": "user"}
        request = request_factory.post('/api/query/memory/del-mem/delete/', data=json.dumps(payload), content_type='application/json')
        setup_request_for_view(request, setup_test_data['token'])
        
        response = memory_views.delete_memory(request, name="del-mem")
        assert response.status_code == 200
        data = json.loads(response.content)
        assert data['status'] == 'success'
        assert data['removed'] == 1
