"""
TernoDBI MCP Servers.

Two independent MCP servers for AI agents:
- Query MCP: Read-only database operations (query, list tables, get schema)
- Admin MCP: Administrative operations (rename tables/columns, manage suggestions)

Usage:
    python -m dbi_layer.mcp query   # Start Query MCP server
    python -m dbi_layer.mcp admin   # Start Admin MCP server
"""

import sys

def main():
    if len(sys.argv) < 2:
        print("Usage: python -m dbi_layer.mcp <query|admin>")
        print("")
        print("Available servers:")
        print("  query  - Query MCP server (list tables, execute queries, get schema)")
        print("  admin  - Admin MCP server (rename tables/columns, manage suggestions)")
        sys.exit(1)
    
    server_type = sys.argv[1].lower()
    
    if server_type == "query":
        from dbi_layer.mcp.query_server import main as query_main
        query_main()
    elif server_type == "admin":
        from dbi_layer.mcp.admin_server import main as admin_main
        admin_main()
    else:
        print(f"Unknown server type: {server_type}")
        print("Use 'query' or 'admin'")
        sys.exit(1)


if __name__ == "__main__":
    main()
