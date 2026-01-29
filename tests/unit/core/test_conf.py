"""
Unit tests for conf.py (Configuration module).

Tests configuration retrieval and defaults.
"""
import pytest
from unittest.mock import patch, MagicMock


class TestConfigDefaults:
    """Tests for configuration defaults."""

    def test_defaults_dict_exists(self):
        """DEFAULTS dict should be defined."""
        from terno_dbi.core.conf import DEFAULTS
        
        assert isinstance(DEFAULTS, dict)
        assert len(DEFAULTS) > 0

    def test_pagination_defaults(self):
        """Pagination defaults should be present."""
        from terno_dbi.core.conf import DEFAULTS
        
        assert 'DEFAULT_PAGE_SIZE' in DEFAULTS
        assert 'MAX_PAGE_SIZE' in DEFAULTS
        assert 'DEFAULT_PAGINATION_MODE' in DEFAULTS
        assert DEFAULTS['DEFAULT_PAGE_SIZE'] == 50
        assert DEFAULTS['MAX_PAGE_SIZE'] == 500
        assert DEFAULTS['DEFAULT_PAGINATION_MODE'] == 'offset'

    def test_cache_defaults(self):
        """Cache defaults should be present."""
        from terno_dbi.core.conf import DEFAULTS
        
        assert 'CACHE_TIMEOUT' in DEFAULTS
        assert 'CACHE_PREFIX' in DEFAULTS
        assert DEFAULTS['CACHE_TIMEOUT'] == 3600
        assert DEFAULTS['CACHE_PREFIX'] == 'dbi_'

    def test_connection_pool_defaults(self):
        """Connection pool defaults should be present."""
        from terno_dbi.core.conf import DEFAULTS
        
        assert 'DEFAULT_POOL_SIZE' in DEFAULTS
        assert 'DEFAULT_MAX_OVERFLOW' in DEFAULTS
        assert 'DEFAULT_POOL_TIMEOUT' in DEFAULTS
        assert 'DEFAULT_POOL_RECYCLE' in DEFAULTS

    def test_query_limit_defaults(self):
        """Query limit defaults should be present."""
        from terno_dbi.core.conf import DEFAULTS
        
        assert 'MAX_QUERY_ROWS' in DEFAULTS
        assert 'QUERY_TIMEOUT' in DEFAULTS
        assert 'MAX_EXPORT_ROWS' in DEFAULTS

    def test_access_control_defaults(self):
        """Access control defaults should be present."""
        from terno_dbi.core.conf import DEFAULTS
        
        assert 'ALLOW_SUPERTOKEN' in DEFAULTS
        assert 'REQUIRE_TOKEN_SCOPE' in DEFAULTS
        assert DEFAULTS['ALLOW_SUPERTOKEN'] is False
        assert DEFAULTS['REQUIRE_TOKEN_SCOPE'] is True


class TestGetFunction:
    """Tests for the get() configuration function."""

    def test_returns_default_when_no_django_setting(self):
        """Should return default value when no Django setting exists."""
        from terno_dbi.core import conf
        
        with patch.object(conf, 'settings') as mock_settings:
            mock_settings.DBI_LAYER = {}
            
            result = conf.get('DEFAULT_PAGE_SIZE')
            
            assert result == 50

    def test_returns_user_setting_when_present(self):
        """Should return user setting when defined in Django settings."""
        from terno_dbi.core import conf
        
        with patch.object(conf, 'settings') as mock_settings:
            mock_settings.DBI_LAYER = {'DEFAULT_PAGE_SIZE': 100}
            
            result = conf.get('DEFAULT_PAGE_SIZE')
            
            assert result == 100

    def test_returns_none_for_unknown_key(self):
        """Should return None for unknown configuration key."""
        from terno_dbi.core import conf
        
        with patch.object(conf, 'settings') as mock_settings:
            mock_settings.DBI_LAYER = {}
            
            result = conf.get('UNKNOWN_KEY_XYZ')
            
            assert result is None


class TestGetAllFunction:
    """Tests for the get_all() configuration function."""

    def test_returns_merged_settings(self):
        """Should return defaults merged with user settings."""
        from terno_dbi.core import conf
        
        with patch.object(conf, 'settings') as mock_settings:
            mock_settings.DBI_LAYER = {'CUSTOM_KEY': 'custom_value'}
            
            result = conf.get_all()
            
            assert isinstance(result, dict)
            assert 'DEFAULT_PAGE_SIZE' in result  # From DEFAULTS
            assert result.get('CUSTOM_KEY') == 'custom_value'  # From user

    def test_user_settings_override_defaults(self):
        """User settings should override default values."""
        from terno_dbi.core import conf
        
        with patch.object(conf, 'settings') as mock_settings:
            mock_settings.DBI_LAYER = {'DEFAULT_PAGE_SIZE': 200}
            
            result = conf.get_all()
            
            assert result['DEFAULT_PAGE_SIZE'] == 200

    def test_preserves_all_defaults(self):
        """Should include all defaults in output."""
        from terno_dbi.core import conf
        from terno_dbi.core.conf import DEFAULTS
        
        with patch.object(conf, 'settings') as mock_settings:
            mock_settings.DBI_LAYER = {}
            
            result = conf.get_all()
            
            for key in DEFAULTS:
                assert key in result
