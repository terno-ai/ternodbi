"""
Edge case tests for Shield Service.

Covers MDB generation edge cases, error handling, and cache logic.
"""
import pytest
from unittest.mock import patch, MagicMock
from terno_dbi.services.shield import (
    generate_native_sql,
    get_cache_key,
    prepare_mdb,
    _get_cache_version,
    delete_cache
)
from terno_dbi.core.models import DataSource, Table, Group, ServiceToken

@pytest.fixture
def datasource(db):
    return DataSource.objects.create(
        display_name="shield_test",
        type="postgres",
        connection_str="postgresql://localhost/db",
        enabled=True
    )

@pytest.mark.django_db
class TestShieldEdgeCases:
    """Tests for shield service edge cases."""

    def test_native_sql_exception(self):
        """Should handle SQL generation errors."""
        mock_mdb = MagicMock()
        # Mock Session to raise
        with patch('terno_dbi.services.shield.Session') as mock_session_cls:
            mock_session = MagicMock()
            mock_session.generateNativeSQL.side_effect = Exception("Parsing error")
            mock_session_cls.return_value = mock_session
            
            result = generate_native_sql(mock_mdb, "SELECT *", "postgres")
            
        assert result['status'] == 'error'
        assert "Parsing error" in result['error']

    def test_get_cache_version_none(self, datasource):
        """Should return 0 if no version cached."""
        with patch('terno_dbi.services.shield.cache') as mock_cache:
            mock_cache.get.return_value = None
            version = _get_cache_version(datasource.id)
            assert version == 0

    def test_get_cache_version_exists(self, datasource):
        """Should return cached version."""
        with patch('terno_dbi.services.shield.cache') as mock_cache:
            mock_cache.get.return_value = 5
            version = _get_cache_version(datasource.id)
            assert version == 5

    def test_cache_key_sorting(self, datasource):
        """Roles should be sorted in cache key."""
        key1 = get_cache_key(datasource.id, [2, 1, 3])
        key2 = get_cache_key(datasource.id, [3, 2, 1])
        assert key1 == key2
        assert "roles_1_2_3" in key1

    def test_prepare_mdb_cached(self, datasource):
        """Should return cached MDB if available."""
        roles = Group.objects.none()
        with patch('terno_dbi.services.shield.cache') as mock_cache:
            mock_cache.get.return_value = "cached_mdb_obj"
            
            result = prepare_mdb(datasource, roles)
            
            assert result == "cached_mdb_obj"

    def test_delete_cache_bumps_version(self, datasource):
        """Delete cache should increment version."""
        with patch('terno_dbi.services.shield.cache') as mock_cache:
            mock_cache.get.return_value = 10
            
            delete_cache(datasource)
            
            # verify set was called with version 11
            args = mock_cache.set.call_args
            assert args[0][1] == 11
