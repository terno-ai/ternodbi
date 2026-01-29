import pytest
import os
import sys
from unittest.mock import patch
from terno_dbi.agents.mcp_config import get_default_server_params

class TestMCPConfig:

    def test_get_params_defaults(self):
        """Test default params without specific env vars."""
        # Clear specific keys but keep others
        with patch.dict(os.environ, {}, clear=True):
            # Restore PATH or basic env if needed, or mock os.environ.copy
            # simpler: just Ensure TERNODBI_ keys are absent
            if "TERNODBI_QUERY_KEY" in os.environ: del os.environ["TERNODBI_QUERY_KEY"]
            if "TERNODBI_ADMIN_KEY" in os.environ: del os.environ["TERNODBI_ADMIN_KEY"]
            
            params = get_default_server_params()
            
            assert len(params) == 2
            
            query_param = params[0]
            assert query_param.command == sys.executable
            assert "-m" in query_param.args
            assert "terno_dbi.mcp.query_server" in query_param.args
            assert "TERNODBI_API_KEY" not in query_param.env
            
            admin_param = params[1]
            assert admin_param.command == sys.executable
            assert "terno_dbi.mcp.admin_server" in admin_param.args
            assert "TERNODBI_API_KEY" not in admin_param.env

    def test_get_params_with_keys(self):
        """Test params when keys are present in env."""
        env_update = {
            "TERNODBI_QUERY_KEY": "query-secret",
            "TERNODBI_ADMIN_KEY": "admin-secret"
        }
        with patch.dict(os.environ, env_update):
            params = get_default_server_params()
            
            query_param = params[0]
            assert query_param.env["TERNODBI_API_KEY"] == "query-secret"
            
            admin_param = params[1]
            assert admin_param.env["TERNODBI_API_KEY"] == "admin-secret"
