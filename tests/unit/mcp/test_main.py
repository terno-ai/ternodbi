"""
Unit tests for MCP Main CLI.
"""
import pytest
import sys
from unittest.mock import patch, MagicMock

class TestMCPMain:
    """Tests for MCP main module execution."""

    def test_main_query_server(self):
        """Should run query server when invoked with 'query'."""
        from terno_dbi.mcp.__main__ import main
        
        with patch.object(sys, 'argv', ['mcp', 'query']):
            # Mock the imported main function
            # Since import happens inside function, we patch module import
            with patch('terno_dbi.mcp.query_server.main') as mock_query_main:
                main()
                mock_query_main.assert_called_once()

    def test_main_admin_server(self):
        """Should run admin server when invoked with 'admin'."""
        from terno_dbi.mcp.__main__ import main
        
        with patch.object(sys, 'argv', ['mcp', 'admin']):
            with patch('terno_dbi.mcp.admin_server.main') as mock_admin_main:
                main()
                mock_admin_main.assert_called_once()

    def test_no_args_exit(self):
        """Should exit if no args provided."""
        from terno_dbi.mcp.__main__ import main
        
        with patch.object(sys, 'argv', ['mcp']):
            # sys.exit must stop flow, so we mock it to raise SystemExit
            with patch.object(sys, 'exit', side_effect=SystemExit) as mock_exit:
                with pytest.raises(SystemExit):
                    main()
                mock_exit.assert_called_with(1)

    def test_invalid_args_exit(self):
        """Should exit if invalid server type provided."""
        from terno_dbi.mcp.__main__ import main
        
        with patch.object(sys, 'argv', ['mcp', 'unknown']):
            with patch.object(sys, 'exit', side_effect=SystemExit) as mock_exit:
                with pytest.raises(SystemExit):
                    main()
                mock_exit.assert_called_with(1)
