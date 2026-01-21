import sys


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m terno_dbi.mcp <query|admin>", file=sys.stderr)
        print("", file=sys.stderr)
        print("Available servers:", file=sys.stderr)
        print("  query  - Query MCP server (list tables, execute queries, get schema)", file=sys.stderr)
        print("  admin  - Admin MCP server (rename tables/columns)", file=sys.stderr)
        sys.exit(1)

    server_type = sys.argv[1].lower()

    if server_type == "query":
        from terno_dbi.mcp.query_server import main as query_main
        query_main()
    elif server_type == "admin":
        from terno_dbi.mcp.admin_server import main as admin_main
        admin_main()
    else:
        print(f"Unknown server type: {server_type}")
        print("Use 'query' or 'admin'")
        sys.exit(1)


if __name__ == "__main__":
    main()
