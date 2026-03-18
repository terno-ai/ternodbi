"""
Unit tests for core migrations logic.
"""
import pytest
from unittest.mock import MagicMock, patch
from django.db.migrations.state import ProjectState
from django.db import connection

# Import migration functions dynamically due to numerical prefix
import importlib
initial_migration = importlib.import_module('terno_dbi.core.migrations.0001_initial')
org_migration = importlib.import_module('terno_dbi.core.migrations.0002_organization_support')

class TestInitialMigration:
    """Tests for 0001_initial.py logic."""

    def test_table_exists(self):
        """Should return True if table exists."""
        with patch('django.db.connection.cursor') as mock_cursor:
            mock_cursor.return_value.__enter__.return_value = MagicMock()
            with patch('django.db.connection.introspection.table_names') as mock_names:
                mock_names.return_value = ['table1', 'table2']
                
                assert initial_migration.table_exists('table1') is True
                assert initial_migration.table_exists('missing') is False

    def test_create_tables_if_needed_skips_existing(self):
        """Should not create models if tables exist."""
        apps = MagicMock()
        schema_editor = MagicMock()
        
        # Mock table_exists to return True for all
        with patch.object(initial_migration, 'table_exists', return_value=True):
            initial_migration.create_tables_if_needed(apps, schema_editor)
            
            schema_editor.create_model.assert_not_called()

    def test_create_tables_if_needed_creates_missing(self):
        """Should create models if tables missing."""
        apps = MagicMock()
        schema_editor = MagicMock()
        
        # Mock table_exists to return False
        with patch.object(initial_migration, 'table_exists', return_value=False):
            initial_migration.create_tables_if_needed(apps, schema_editor)
            
            # Should call create_model for each model in list (11 models)
            assert schema_editor.create_model.call_count == 11



    def test_create_tables_error(self):
        """Should raise exception on creation error."""
        apps = MagicMock()
        schema_editor = MagicMock()
        
        with patch.object(initial_migration, 'table_exists', return_value=False):
            schema_editor.create_model.side_effect = Exception("Create Fail")
            with pytest.raises(Exception, match="Create Fail"):
                initial_migration.create_tables_if_needed(apps, schema_editor)




class TestOrgMigration:
    """Tests for 0002_organization_support.py logic."""

    def test_create_org_tables_skips_existing(self):
        """Should skip creation if tables exist."""
        apps = MagicMock()
        schema_editor = MagicMock()
        
        with patch.object(org_migration, 'table_exists', return_value=True):
            org_migration.create_org_tables_if_needed(apps, schema_editor)
            schema_editor.create_model.assert_not_called()

    def test_add_datasource_org_fk_adds_column(self):
        """Should add column if missing."""
        apps = MagicMock()
        schema_editor = MagicMock()
        
        with patch('django.db.connection.cursor'):
            with patch('django.db.connection.introspection.get_table_description') as mock_desc:
                mock_desc.return_value = []
                
                org_migration.add_datasource_org_fk(apps, schema_editor)
                schema_editor.add_field.assert_called_once()

    def test_add_datasource_org_fk_skips_if_exists(self):
        """Should skip adding column if it exists."""
        apps = MagicMock()
        schema_editor = MagicMock()
        
        col_mock = MagicMock()
        col_mock.name = 'organisation_id'
        
        with patch('django.db.connection.cursor'):
            with patch('django.db.connection.introspection.get_table_description') as mock_desc:
                mock_desc.return_value = [col_mock]
                
                org_migration.add_datasource_org_fk(apps, schema_editor)
                schema_editor.add_field.assert_not_called()

    def test_create_org_tables_error(self):
        """Should raise exception on creation error."""
        apps = MagicMock()
        schema_editor = MagicMock()
        
        # Mock table_exists to false
        with patch.object(org_migration, 'table_exists', return_value=False):
            # Mock create_model to raise
            schema_editor.create_model.side_effect = Exception("DB Fail")
            
            with pytest.raises(Exception, match="DB Fail"):
                org_migration.create_org_tables_if_needed(apps, schema_editor)

    def test_table_exists_check(self):
        """Should verify table_exists helper."""
        # It's hard to test local function in module scope if not imported directly or patched.
        # But we imported it as org_migration.table_exists
        
        with patch('django.db.connection.cursor') as mock_cursor:
            mock_cur = MagicMock()
            mock_cursor.return_value.__enter__.return_value = mock_cur
            with patch('django.db.connection.introspection.table_names') as mock_names:
                mock_names.return_value = ['foo']
                assert org_migration.table_exists('foo') is True
                assert org_migration.table_exists('bar') is False
