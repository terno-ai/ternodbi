import pytest
import json
from unittest.mock import MagicMock, patch
from django.test import RequestFactory
from django.http import JsonResponse, HttpRequest

from terno_dbi.core.admin_service.views import (
    create_datasource,
    update_datasource,
    delete_datasource,
    update_table,
    update_column,
    validate_connection,
    sync_metadata,
    get_table_info,
)
from terno_dbi.core.models import DataSource, Table, TableColumn, CoreOrganisation

@pytest.fixture
def req_factory():
    return RequestFactory()

class TestCreateDatasourceEdgeCases:
    
    def test_missing_fields(self, req_factory):
        request = req_factory.post('/api/create', data={}, content_type='application/json')
        # Unwrap to skip auth
        view = create_datasource.__wrapped__.__wrapped__.__wrapped__ # multiply wrapped? 
        # Checking decorators: @csrf_exempt @require_service_auth @require_http_methods
        # require_service_auth uses @wraps. require_http_methods uses @wraps (usually). csrf_exempt uses @wraps.
        # Let's try unwrapping until we hit the function.
        
        # Actually safer to just assume it's wrapped and traverse if needed, or just mock the auth pass.
        # But unwrapping is cleaner for unit tests.
        # create_datasource is decorated with @csrf_exempt(outer), @require_service_auth(mid), @require_http_methods(inner implicit? no explicit)
        # Wait, the code:
        # @csrf_exempt
        # @require_service_auth(...)
        # @require_http_methods(...)
        # def create_datasource...
        
        # So create_datasource is csrf_exempt(require_service_auth(require_http_methods(func)))
        # accessing .__wrapped__ might peel one layer.
        
        # Let's try to just call it and mock request.service_token to satisfy decorator?
        # No, previous attempts failed. Unwrapping is best.
        
        func = create_datasource
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        response = func(request)
        assert response.status_code == 400
        assert "Missing required fields" in json.loads(response.content)['error']

    def test_bigquery_missing_json(self, req_factory):
        data = {
            "display_name": "BQ",
            "type": "bigquery",
            "connection_str": "bq://"
        }
        request = req_factory.post('/', data=data, content_type='application/json')
        
        func = create_datasource
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        response = func(request)
        assert response.status_code == 400
        assert "connection_json is required" in json.loads(response.content)['error']

    def test_bigquery_invalid_json(self, req_factory):
        data = {
            "display_name": "BQ",
            "type": "bigquery",
            "connection_str": "bq://",
            "connection_json": "{invalid"
        }
        request = req_factory.post('/', data=data, content_type='application/json')
        
        func = create_datasource
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        response = func(request)
        assert response.status_code == 400
        assert "Invalid connection_json" in json.loads(response.content)['error']

    def test_validation_failure(self, req_factory):
        data = {
            "display_name": "DS",
            "type": "postgres",
            "connection_str": "invalid"
        }
        request = req_factory.post('/', data=data, content_type='application/json')
        
        func = create_datasource
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        with patch('terno_dbi.core.admin_service.views.validate_datasource_input', return_value="Bad connection"):
            response = func(request)
            assert response.status_code == 400
            assert "Validation validation failed" in json.loads(response.content)['error'] or "Connection validation failed" in json.loads(response.content)['error']

    def test_json_decode_error(self, req_factory):
        request = req_factory.post('/', data="{bad", content_type='application/json')
        func = create_datasource
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
        response = func(request)
        assert response.status_code == 400
        assert "Invalid JSON" in json.loads(response.content)['error']

    def test_general_exception(self, req_factory):
        # We need to construct request carefully so body access doesn't fail before our patch
        request = req_factory.post('/', data={}, content_type='application/json')
        func = create_datasource
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        # Patch json.loads to raise a generic Exception (not JSONDecodeError)
        with patch('terno_dbi.core.admin_service.views.json.loads', side_effect=Exception("Crash")) as mock_json:
            response = func(request)
        
        # Check response outside patch context to avoid patching json.loads during assertion
        assert response.status_code == 500
        assert "Crash" in json.loads(response.content)['error']

class TestUpdateDatasourceEdgeCases:
    def test_update_json_error(self, req_factory):
        request = req_factory.patch('/', data="{bad", content_type='application/json')
        request.resolved_datasource = MagicMock()
        
        func = update_datasource
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        response = func(request, 1)
        assert response.status_code == 400
        assert "Invalid JSON" in json.loads(response.content)['error']

    def test_update_success(self, req_factory):
        request = req_factory.patch('/', data={"name": "New Name", "description": "desc", "enabled": False}, content_type='application/json')
        ds = MagicMock(spec=DataSource)
        ds.id = 1
        ds.display_name = "Old"
        ds.type = "pg"
        ds.description = ""
        ds.enabled = True
        request.resolved_datasource = ds
        
        func = update_datasource
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        response = func(request, 1)
        assert response.status_code == 200
        ds.save.assert_called()
        assert ds.display_name == "New Name"

class TestDeleteDatasourceEdgeCases:
    def test_delete_success(self, req_factory):
        request = req_factory.delete('/')
        ds = MagicMock(spec=DataSource)
        ds.display_name = "DS"
        request.resolved_datasource = ds
        
        func = delete_datasource
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        with patch('terno_dbi.core.admin_service.views.models.DataSource') as mock_ds_cls:
             response = func(request, 1)
             assert response.status_code == 200
             ds.delete.assert_called()

class TestUpdateTableEdgeCases:
    def test_update_table_success(self, req_factory):
        request = req_factory.patch('/', data={"public_name": "Pub", "description": "Desc"}, content_type='application/json')
        table = MagicMock(spec=Table)
        table.id = 1
        request.resolved_table = table
        
        func = update_table
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        response = func(request, 1)
        assert response.status_code == 200
        table.save.assert_called()

    def test_update_table_json_error(self, req_factory):
        request = req_factory.patch('/', data="{bad", content_type='application/json')
        request.resolved_table = MagicMock()
        func = update_table
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
        response = func(request, 1)
        assert response.status_code == 400

class TestUpdateColumnEdgeCases:
    def test_update_column_success(self, req_factory):
        request = req_factory.patch('/', data={"public_name": "Pub", "description": "Desc"}, content_type='application/json')
        col = MagicMock(spec=TableColumn)
        col.id = 1
        col.public_name = "Pub"
        col.data_type = "text" # Needs to be serializable
        request.resolved_column = col
        
        func = update_column
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        response = func(request, 1)
        assert response.status_code == 200
        col.save.assert_called()

    def test_update_column_json_error(self, req_factory):
        request = req_factory.patch('/', data="{bad", content_type='application/json')
        request.resolved_column = MagicMock()
        func = update_column
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
        response = func(request, 1)
        assert response.status_code == 400

class TestSyncMetadataEdgeCases:
    def test_sync_success(self, req_factory):
        request = req_factory.post('/', data={"overwrite": True}, content_type='application/json')
        ds = MagicMock(spec=DataSource)
        ds.id = 1
        request.resolved_datasource = ds
        
        func = sync_metadata
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        with patch('terno_dbi.services.schema_utils.sync_metadata', return_value={'ok': True}) as mock_sync:
            response = func(request, 1)
            assert response.status_code == 200
            mock_sync.assert_called_with(1, True)

    def test_sync_json_error_defaults_overwrite_false(self, req_factory):
        request = req_factory.post('/', data="{bad", content_type='application/json')
        ds = MagicMock(spec=DataSource)
        ds.id = 1
        request.resolved_datasource = ds
        
        func = sync_metadata
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        with patch('terno_dbi.services.schema_utils.sync_metadata', return_value={'ok': True}) as mock_sync:
            response = func(request, 1)
            assert response.status_code == 200
            mock_sync.assert_called_with(1, False)

    def test_sync_exception(self, req_factory):
        request = req_factory.post('/', data={}, content_type='application/json')
        ds = MagicMock(spec=DataSource)
        request.resolved_datasource = ds
        
        func = sync_metadata
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        with patch('terno_dbi.services.schema_utils.sync_metadata', side_effect=Exception("Sync Fail")):
            response = func(request, 1)
            assert response.status_code == 500
            assert "Sync Fail" in json.loads(response.content)['error']

class TestValidateConnectionEdgeCases:
    def test_missing_fields(self, req_factory):
        request = req_factory.post('/', data={}, content_type='application/json')
        func = validate_connection
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
        response = func(request)
        assert response.status_code == 400
        assert "Missing 'type'" in json.loads(response.content)['error']

    def test_validation_error_returns_false(self, req_factory):
        request = req_factory.post('/', data={"type":"pg", "connection_str":"s"}, content_type='application/json')
        func = validate_connection
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        with patch('terno_dbi.core.admin_service.views.validate_datasource_input', return_value="Err"):
            response = func(request)
            data = json.loads(response.content)
            assert data['status'] == 'error'
            assert data['valid'] is False
            assert "Err" in data['error']

    def test_json_error(self, req_factory):
        request = req_factory.post('/', data="{bad", content_type='application/json')
        func = validate_connection
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
        response = func(request)
        assert response.status_code == 400

    def test_general_exception(self, req_factory):
        request = req_factory.post('/', data={}, content_type='application/json')
        func = validate_connection
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
        with patch('json.loads', side_effect=Exception("Crash")):
            response = func(request)
            assert response.status_code == 500

class TestGetTableInfoEdgeCases:
    def test_table_not_found(self, req_factory):
        request = req_factory.get('/')
        ds = MagicMock(spec=DataSource)
        request.resolved_datasource = ds
        
        func = get_table_info
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        # Mock Table.objects.get to raise
        with patch('terno_dbi.core.models.Table.objects.get', side_effect=Table.DoesNotExist):
            response = func(request, 1, "missing_table")
            assert response.status_code == 404
            assert "not found" in json.loads(response.content)['error']

    def test_sample_data_failure_handled(self, req_factory):
        request = req_factory.get('/')
        ds = MagicMock(spec=DataSource)
        request.resolved_datasource = ds
        
        func = get_table_info
        while hasattr(func, '__wrapped__'):
            func = func.__wrapped__
            
        table = MagicMock()
        table.public_name = "T1"
        table.description = "Desc"
        
        with patch('terno_dbi.core.models.Table.objects.get', return_value=table), \
             patch('terno_dbi.core.models.TableColumn.objects.filter') as mock_cols, \
             patch('terno_dbi.services.query.execute_native_sql', side_effect=Exception("DB Error")):
            
            mock_cols.return_value.values.return_value = []
            
            response = func(request, 1, "t1")
            
            assert response.status_code == 200
            data = json.loads(response.content)
            assert data['table']['sample_rows'] == []


