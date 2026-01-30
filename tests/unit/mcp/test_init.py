import unittest
import importlib
from terno_dbi.mcp import get_query_main, get_admin_main

class TestMCPInit(unittest.TestCase):
    
    def test_get_query_main(self):
        """Should return the main function for query server."""
        main_func = get_query_main()
        assert callable(main_func)
        assert main_func.__module__ == 'terno_dbi.mcp.query_server'

    def test_get_admin_main(self):
        """Should return the main function for admin server."""
        main_func = get_admin_main()
        assert callable(main_func)
        assert main_func.__module__ == 'terno_dbi.mcp.admin_server'
