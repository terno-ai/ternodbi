"""
TernoDBI MCP package.

Provides two MCP servers:
- Query MCP: Read-only database operations
- Admin MCP: Administrative operations

Run with:
    python -m terno_dbi.mcp query
    python -m terno_dbi.mcp admin
"""

# Don't import servers at module level to avoid Django setup during import
# Use functions to get them lazily

def get_query_main():
    from terno_dbi.mcp.query_server import main
    return main

def get_admin_main():
    from terno_dbi.mcp.admin_server import main
    return main
