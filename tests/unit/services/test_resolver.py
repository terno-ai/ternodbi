"""
Unit tests for Resolver Service.

Tests the datasource resolution functions that allow lookup by
numeric ID or display name.
"""
import pytest
from django.http import Http404

from terno_dbi.services.resolver import resolve_datasource, get_datasource_id
from terno_dbi.core.models import DataSource


@pytest.mark.django_db
class TestResolveDatasource:
    """Tests for resolve_datasource function."""

    @pytest.fixture
    def datasource(self):
        """Create a test datasource."""
        return DataSource.objects.create(
            display_name='test_production_db',
            type='postgres',
            connection_str='postgresql://localhost/test',
            enabled=True
        )

    @pytest.fixture
    def disabled_datasource(self):
        """Create a disabled datasource."""
        return DataSource.objects.create(
            display_name='disabled_db',
            type='mysql',
            connection_str='mysql://localhost/disabled',
            enabled=False
        )

    def test_resolve_by_id_integer(self, datasource):
        """Should resolve datasource when given integer ID."""
        result = resolve_datasource(datasource.id)
        
        assert result.id == datasource.id
        assert result.display_name == 'test_production_db'

    def test_resolve_by_id_string(self, datasource):
        """Should resolve datasource when given string that looks like ID."""
        result = resolve_datasource(str(datasource.id))
        
        assert result.id == datasource.id

    def test_resolve_by_name(self, datasource):
        """Should resolve datasource by display_name."""
        result = resolve_datasource('test_production_db')
        
        assert result.id == datasource.id
        assert result.display_name == 'test_production_db'

    def test_nonexistent_id_raises_404(self):
        """Should raise Http404 for non-existent ID."""
        with pytest.raises(Http404) as exc_info:
            resolve_datasource(99999)
        
        assert 'not found' in str(exc_info.value).lower()

    def test_nonexistent_name_raises_404(self):
        """Should raise Http404 for non-existent name."""
        with pytest.raises(Http404) as exc_info:
            resolve_datasource('nonexistent_database')
        
        assert 'not found' in str(exc_info.value).lower()

    def test_disabled_datasource_raises_404_by_default(self, disabled_datasource):
        """Should raise Http404 for disabled datasource when enabled_only=True."""
        with pytest.raises(Http404):
            resolve_datasource(disabled_datasource.id)

    def test_disabled_datasource_found_when_enabled_only_false(self, disabled_datasource):
        """Should find disabled datasource when enabled_only=False."""
        result = resolve_datasource(disabled_datasource.id, enabled_only=False)
        
        assert result.id == disabled_datasource.id

    def test_case_sensitive_name_match(self, datasource):
        """Name matching should be case-sensitive."""
        with pytest.raises(Http404):
            resolve_datasource('TEST_PRODUCTION_DB')

    def test_multiple_datasources_same_name_raises_404(self):
        """Should raise Http404 if multiple datasources have same name."""
        # Note: This should be prevented by DB constraint, but test the edge case
        DataSource.objects.create(
            display_name='duplicate_name',
            type='postgres',
            connection_str='postgresql://localhost/test1',
            enabled=True
        )
        # Force create another with same name (bypassing unique constraint for test)
        # This tests the MultipleObjectsReturned handling in the resolver
        # In practice, the unique constraint should prevent this
        from unittest.mock import patch
        with patch('terno_dbi.services.resolver.DataSource.objects.all') as mock_all:
            mock_qs = mock_all.return_value.filter.return_value
            mock_qs.get.side_effect = DataSource.MultipleObjectsReturned()
            
            with pytest.raises(Http404) as exc_info:
                resolve_datasource('duplicate_name')
                
            assert 'Multiple datasources found' in str(exc_info.value)


@pytest.mark.django_db  
class TestGetDatasourceId:
    """Tests for get_datasource_id convenience function."""

    @pytest.fixture
    def datasource(self):
        return DataSource.objects.create(
            display_name='id_test_db',
            type='sqlite',
            connection_str='sqlite:///test.db',
            enabled=True
        )

    def test_returns_id_from_integer(self, datasource):
        """Should return ID when given integer."""
        result = get_datasource_id(datasource.id)
        
        assert result == datasource.id
        assert isinstance(result, int)

    def test_returns_id_from_name(self, datasource):
        """Should return ID when given name."""
        result = get_datasource_id('id_test_db')
        
        assert result == datasource.id


@pytest.mark.django_db
class TestResolverEdgeCases:
    """Edge case tests for resolver functions."""

    def test_empty_string_raises_404(self):
        """Empty string should raise Http404."""
        with pytest.raises(Http404):
            resolve_datasource('')

    def test_whitespace_name_raises_404(self):
        """Whitespace-only name should raise Http404."""
        with pytest.raises(Http404):
            resolve_datasource('   ')

    def test_zero_id_raises_404(self):
        """ID of 0 should raise Http404."""
        with pytest.raises(Http404):
            resolve_datasource(0)

    def test_negative_id_raises_404(self):
        """Negative ID should raise Http404."""
        with pytest.raises(Http404):
            resolve_datasource(-1)

    def test_name_with_special_characters(self):
        """Datasource name with special characters should work."""
        ds = DataSource.objects.create(
            display_name='my-database_v2.0',
            type='postgres',
            connection_str='postgresql://localhost/test',
            enabled=True
        )
        
        result = resolve_datasource('my-database_v2.0')
        
        assert result.id == ds.id
