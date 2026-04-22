import pytest
from unittest.mock import MagicMock, patch
from django.contrib import messages
from terno_dbi.core.admin import (
    DataSourceAdmin,
    ServiceTokenAdmin
)
from terno_dbi.core.models import DataSource, ServiceToken

class TestDataSourceAdmin:
    @pytest.fixture
    def mock_request(self):
        req = MagicMock()
        req.user = MagicMock()
        return req

    def test_save_model_new_enabled(self, mock_request):
        admin = DataSourceAdmin(DataSource, MagicMock())
        obj = MagicMock(spec=DataSource)
        obj.enabled = True
        obj.id = 1
        form = MagicMock()
        
        with patch('terno_dbi.core.admin.reversion.admin.VersionAdmin.save_model'), \
             patch('terno_dbi.core.admin.sync_metadata') as mock_sync, \
             patch('django.contrib.messages.success') as mock_success:
            
            mock_sync.return_value = {'tables_created': 1, 'columns_created': 2}
            admin.save_model(mock_request, obj, form, change=False)
            
            mock_sync.assert_called_with(1)
            mock_success.assert_called()

    def test_save_model_sync_error(self, mock_request):
        admin = DataSourceAdmin(DataSource, MagicMock())
        obj = MagicMock(spec=DataSource)
        obj.enabled = True
        obj.id = 1
        form = MagicMock()
        
        with patch('terno_dbi.core.admin.reversion.admin.VersionAdmin.save_model'), \
             patch('terno_dbi.core.admin.sync_metadata') as mock_sync, \
             patch('django.contrib.messages.warning') as mock_warn:
            
            mock_sync.return_value = {'error': 'Failed'}
            admin.save_model(mock_request, obj, form, change=False)
            
            mock_warn.assert_called()
            assert "metadata sync failed" in mock_warn.call_args[0][1]

    def test_save_model_exception(self, mock_request):
        admin = DataSourceAdmin(DataSource, MagicMock())
        obj = MagicMock(spec=DataSource)
        obj.enabled = True
        obj.id = 1
        form = MagicMock()
        
        with patch('terno_dbi.core.admin.reversion.admin.VersionAdmin.save_model'), \
             patch('terno_dbi.core.admin.sync_metadata') as mock_sync, \
             patch('django.contrib.messages.warning') as mock_warn:
            
            mock_sync.side_effect = Exception("System Failure")
            admin.save_model(mock_request, obj, form, change=False)
            
            mock_warn.assert_called()
            assert "metadata sync failed" in mock_warn.call_args[0][1]

    def test_save_model_new_disabled(self, mock_request):
        admin = DataSourceAdmin(DataSource, MagicMock())
        obj = MagicMock(spec=DataSource)
        obj.enabled = False
        obj.id = 1
        form = MagicMock()
        
        with patch('terno_dbi.core.admin.reversion.admin.VersionAdmin.save_model') as mock_super_save, \
             patch('terno_dbi.core.admin.sync_metadata') as mock_sync:
            
            admin.save_model(mock_request, obj, form, change=False)
            
            mock_super_save.assert_called_with(mock_request, obj, form, False)
            mock_sync.assert_not_called()

    def test_trigger_sync_metadata_action(self, mock_request):
        admin = DataSourceAdmin(DataSource, MagicMock())
        
        ds1 = MagicMock(spec=DataSource, id=1, display_name="DS1", enabled=True)
        ds2 = MagicMock(spec=DataSource, id=2, display_name="DS2", enabled=False)
        
        queryset = MagicMock()
        queryset.__iter__.return_value = [ds1, ds2]
        queryset.count.return_value = 2
        
        with patch('terno_dbi.core.admin.sync_metadata') as mock_sync, \
             patch('django.contrib.messages.success') as mock_success, \
             patch('django.contrib.messages.warning') as mock_warn:
             
            mock_sync.return_value = {'tables_created': 1}
            admin.trigger_sync_metadata(mock_request, queryset)
            
            mock_sync.assert_called_with(1, overwrite=False)
            mock_success.assert_called()
            mock_warn.assert_called()

    def test_trigger_sync_metadata_action_error(self, mock_request):
        admin = DataSourceAdmin(DataSource, MagicMock())
        ds1 = MagicMock(spec=DataSource, id=1, display_name="DS1", enabled=True)
        queryset = MagicMock()
        queryset.__iter__.return_value = [ds1]
        
        with patch('terno_dbi.core.admin.sync_metadata') as mock_sync, \
             patch('django.contrib.messages.error') as mock_error:
             
            mock_sync.return_value = {'error': 'Could not connect'}
            admin.trigger_sync_metadata(mock_request, queryset)
            
            mock_error.assert_called()
            assert "Could not connect" in mock_error.call_args[0][1]
            
    def test_trigger_sync_metadata_action_exception(self, mock_request):
        admin = DataSourceAdmin(DataSource, MagicMock())
        ds1 = MagicMock(spec=DataSource, id=1, display_name="DS1", enabled=True)
        queryset = MagicMock()
        queryset.__iter__.return_value = [ds1]
        
        with patch('terno_dbi.core.admin.sync_metadata') as mock_sync, \
             patch('django.contrib.messages.error') as mock_error:
             
            mock_sync.side_effect = Exception("Database crash")
            admin.trigger_sync_metadata(mock_request, queryset)
            
            mock_error.assert_called()
            assert "Database crash" in mock_error.call_args[0][1]

class TestServiceTokenAdmin:
    def test_save_model_new_token(self):
        admin = ServiceTokenAdmin(ServiceToken, MagicMock())
        obj = MagicMock(spec=ServiceToken)
        obj.name = "Test"
        obj.token_type = ServiceToken.TokenType.QUERY
        
        form = MagicMock()
        form.cleaned_data = {'organisation': None, 'datasources': ['ds1']}
        
        mock_request = MagicMock()
        mock_request.user = MagicMock()
        
        mock_new_token = MagicMock()
        mock_new_token.id = 1
        mock_new_token.key_hash = "hash"
        mock_new_token.key_prefix = "pre"
        
        with patch('terno_dbi.core.admin.generate_service_token') as mock_gen, \
             patch('django.contrib.messages.add_message'), \
             patch('django.contrib.messages.success') as mock_msg:
             
             mock_gen.return_value = (mock_new_token, "raw_key")
             
             admin.save_model(mock_request, obj, form, change=False)
             
             assert obj.id == 1
             assert obj.key_hash == "hash"
             mock_msg.assert_called()
             mock_new_token.datasources.set.assert_called_with(['ds1'])

    def test_save_model_existing_token(self):
        admin = ServiceTokenAdmin(ServiceToken, MagicMock())
        obj = MagicMock(spec=ServiceToken)
        form = MagicMock()
        mock_request = MagicMock()
        
        with patch('django.contrib.admin.ModelAdmin.save_model') as mock_super_save:
             admin.save_model(mock_request, obj, form, change=True)
             mock_super_save.assert_called_with(mock_request, obj, form, True)

    def test_save_model_new_token_no_datasources(self):
        admin = ServiceTokenAdmin(ServiceToken, MagicMock())
        obj = MagicMock(spec=ServiceToken)
        obj.name = "Test"
        obj.token_type = ServiceToken.TokenType.QUERY
        
        form = MagicMock()
        form.cleaned_data = {'organisation': None, 'datasources': []}
        
        mock_request = MagicMock()
        mock_request.user = MagicMock()
        
        mock_new_token = MagicMock()
        mock_new_token.id = 2
        
        with patch('terno_dbi.core.admin.generate_service_token') as mock_gen, \
             patch('django.contrib.messages.add_message'), \
             patch('django.contrib.messages.success'):
             
             mock_gen.return_value = (mock_new_token, "raw_key")
             
             admin.save_model(mock_request, obj, form, change=False)
             
             mock_new_token.datasources.set.assert_not_called()

from terno_dbi.core.admin import (
    TableAdmin, PrivateTableSelectorAdmin, GroupTableSelectorAdmin,
    PrivateColumnSelectorAdmin, GroupColumnSelectorAdmin
)
from terno_dbi.core.models import Table

class TestTableAdmin:
    def test_column_count(self):
        admin = TableAdmin(Table, MagicMock())
        obj = MagicMock()
        obj.tablecolumn_set.count.return_value = 5
        assert admin.column_count(obj) == 5

class TestSelectorsAdmin:
    def test_private_table_count(self):
        admin = PrivateTableSelectorAdmin(MagicMock(), MagicMock())
        obj = MagicMock()
        obj.tables.count.return_value = 2
        assert admin.table_count(obj) == 2

    def test_group_table_count(self):
        admin = GroupTableSelectorAdmin(MagicMock(), MagicMock())
        obj = MagicMock()
        obj.tables.count.return_value = 3
        assert admin.table_count(obj) == 3

    def test_private_column_count(self):
        admin = PrivateColumnSelectorAdmin(MagicMock(), MagicMock())
        obj = MagicMock()
        obj.columns.count.return_value = 4
        assert admin.column_count(obj) == 4

    def test_group_column_count(self):
        admin = GroupColumnSelectorAdmin(MagicMock(), MagicMock())
        obj = MagicMock()
        obj.columns.count.return_value = 5
        assert admin.column_count(obj) == 5
