import pytest
from unittest.mock import MagicMock, patch
from django.contrib import messages
from terno_dbi.core.admin import (
    DataSourceAdmin,
    TableAdmin,
    PrivateTableSelectorAdmin,
    GroupTableSelectorAdmin,
    PrivateColumnSelectorAdmin,
    GroupColumnSelectorAdmin,
    ServiceTokenAdmin
)
from terno_dbi.core.models import DataSource, Table, ServiceToken

class TestDataSourceAdmin:
    
    @pytest.fixture
    def mock_request(self):
        req = MagicMock()
        req.user = MagicMock()
        return req

    def test_save_model_new_enabled(self, mock_request):
        """Test save_model trigger sync when new and enabled."""
        admin = DataSourceAdmin(DataSource, MagicMock())
        obj = MagicMock(spec=DataSource)
        obj.enabled = True
        obj.id = 1
        form = MagicMock()
        
        # Patch sync_metadata only within the scope of save_model execution?
        # save_model calls sync_metadata which is imported inside the method
        with patch('terno_dbi.core.admin.reversion.admin.VersionAdmin.save_model'), \
             patch('terno_dbi.services.schema_utils.sync_metadata') as mock_sync, \
             patch('django.contrib.messages.success') as mock_success:
            
            mock_sync.return_value = {'tables_created': 1, 'columns_created': 2}
            
            admin.save_model(mock_request, obj, form, change=False)
            
            mock_sync.assert_called_with(1)
            mock_success.assert_called()

    def test_save_model_sync_error(self, mock_request):
        """Test save_model handles sync error."""
        admin = DataSourceAdmin(DataSource, MagicMock())
        obj = MagicMock(spec=DataSource)
        obj.enabled = True
        obj.id = 1
        form = MagicMock()
        
        with patch('terno_dbi.core.admin.reversion.admin.VersionAdmin.save_model'), \
             patch('terno_dbi.services.schema_utils.sync_metadata') as mock_sync, \
             patch('django.contrib.messages.warning') as mock_warn:
            
            mock_sync.return_value = {'error': 'Failed'}
            
            admin.save_model(mock_request, obj, form, change=False)
            
            mock_warn.assert_called()
            assert "metadata sync failed" in mock_warn.call_args[0][1]

    def test_trigger_sync_metadata_action(self, mock_request):
        """Test trigger_sync_metadata action."""
        admin = DataSourceAdmin(DataSource, MagicMock())
        
        ds1 = MagicMock(spec=DataSource, id=1, display_name="DS1", enabled=True)
        ds2 = MagicMock(spec=DataSource, id=2, display_name="DS2", enabled=False)
        queryset = [ds1, ds2]
        
        with patch('terno_dbi.services.schema_utils.sync_metadata') as mock_sync, \
             patch('django.contrib.messages.success') as mock_success, \
             patch('django.contrib.messages.warning') as mock_warn:
             
            mock_sync.return_value = {'tables_created': 1}
            
            admin.trigger_sync_metadata(mock_request, queryset)
            
            # DS1 synced
            mock_sync.assert_called_with(1, overwrite=False)
            mock_success.assert_called()
            # DS2 skipped
            mock_warn.assert_called()


class TestTableAdmin:
    def test_column_count(self):
        admin = TableAdmin(Table, MagicMock())
        obj = MagicMock()
        obj.tablecolumn_set.count.return_value = 5
        assert admin.column_count(obj) == 5

class TestServiceTokenAdmin:
    def test_save_model_new_token(self):
        """Test save_model generates token for new ServiceToken."""
        admin = ServiceTokenAdmin(ServiceToken, MagicMock())
        obj = MagicMock(spec=ServiceToken)
        obj.name = "Test"
        obj.token_type = ServiceToken.TokenType.QUERY
        
        form = MagicMock()
        form.cleaned_data = {'organisation': 'org', 'datasources': ['ds1']}
        
        mock_request = MagicMock()
        
        mock_new_token = MagicMock()
        mock_new_token.id = 1
        mock_new_token.key_hash = "hash"
        mock_new_token.key_prefix = "pre"
        
        with patch('terno_dbi.core.admin.generate_service_token') as mock_gen, \
             patch('django.contrib.messages.success') as mock_msg, \
             patch('django.contrib.admin.ModelAdmin.save_model'): # Mock super().save_model for 'else' case? 
             # Wait, logic calls super for change=True. For change=False it does custom logic then implicitly returns?
             # Ah, looking at code: line 241 'else: super().save_model'.
             # line 214 'if not change:'.
             # It sets obj fields but DOES NOT CALL obj.save() directly?
             # Ah, 'generate_service_token' likely creates/saves the token model under the hood. 
             # And 'obj' passed to save_model is the instance created by form.
             # The code modifies 'obj' attributes to match the created token.
             # But does it save 'obj'? 
             # Django's save_model is responsible for saving 'obj'.
             # If create logic is custom, usually we save it there.
             # In lines 225-228 it updates obj.
             # It seems it assumes generate_service_token saves it?
             # Or obj is just updated for display?
             # Actually, generate_service_token returns (token, key). token is a saved model instance.
             # The admin method updates `obj` (the instance from form) with `token`'s data.
             # But `obj` itself might be transient if not saved.
             # Wait, `obj.id = token.id`. This effectively swaps it.
             # But if `obj` isn't saved, does Admin care?
             # The `save_model` hook is where saving happens. If we don't call `super().save_model`, we must save `obj`.
             # The code does NOT call `obj.save()`.
             # However, `generate_service_token` creates a record.
             # So the record exists. `obj` is just the python object in admin view.
             # This seems correct for `generate_service_token`.
             
             mock_gen.return_value = (mock_new_token, "raw_key")
             
             admin.save_model(mock_request, obj, form, change=False)
             
             assert obj.id == 1
             assert obj.key_hash == "hash"
             mock_msg.assert_called()
             assert "raw_key" in mock_msg.call_args[0][1] # Validates format_html args
             mock_new_token.datasources.set.assert_called_with(['ds1'])


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
