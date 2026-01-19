"""
TernoDBI MCP package.

Provides two MCP servers:
- Query MCP: Read-only database operations
- Admin MCP: Administrative operations

Run with:
    python -m dbi_layer.mcp query
    python -m dbi_layer.mcp admin
"""


def get_query_main():
    from dbi_layer.mcp.query_server import main
    return main

def get_admin_main():
    from dbi_layer.mcp.admin_server import main
    return main
