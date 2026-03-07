# Model Context Protocol (MCP) Integration

TernoDBI provides first-class support for the Model Context Protocol, enabling it to function as a tool provider for Claude Desktop, Terno Agents, or any other MCP-compliant client.

## Servers

We expose two separate MCP servers to separate concerns:

### 1. Query Server (`ternodbi-query`)
*   **Purpose**: Safe analysis and data retrieval.
*   **Tools Provided**:
    *   `list_datasources`: See available databases.
    *   `list_tables`: See tables in a database.
    *   `list_table_columns`: Get columns for a specific table.
    *   `get_sample_data`: Get sample rows from a table.
    *   `execute_query`: Run `SELECT` queries with advanced pagination.
        *   Supports `offset` (page-based) and `cursor` (infinite scroll) modes.
        *   Security: All cursors are HMAC-signed to prevent tampering.

### 2. Admin Server (`ternodbi-admin`)
*   **Purpose**: Management and Curation.
*   **Tools Provided**:
    *   `add_datasource`: Safely connect a new database.
    *   `delete_datasource`: Remove a connection.
    *   `validate_connection`: Test credentials before saving.
    *   `sync_metadata`: Sync schemas from the database.
    *   `rename_table`: Change the public-facing name of a table.
    *   `rename_column`: Change the public-facing name of a column.
    *   `update_table_description`: Add documentation to a table.
    *   `update_column_description`: Add documentation to a column.
    *   `get_table_info`: Fetch detailed context for AI curation.

## Connecting to Claude Desktop

Add the following to your `claude_desktop_config.json`.

### Production Config (Recommended)
This uses `uvx` to download and run the latest version of TernoDBI automatically.

```json
{
  "mcpServers": {
    "ternodbi-query": {
      "command": "uvx",
      "args": ["--from", "terno-dbi", "dbi-mcp", "query"],
      "env": {
        "TERNODBI_API_URL": "http://127.0.0.1:8376",
        "TERNODBI_API_KEY": "dbi_query_..."
      }
    },
    "ternodbi-admin": {
      "command": "uvx",
      "args": ["--from", "terno-dbi", "dbi-mcp", "admin"],
      "env": {
        "TERNODBI_API_URL": "http://127.0.0.1:8376",
        "TERNODBI_API_KEY": "dbi_admin_..."
      }
    }
  }
}
```

### Local Development Config
Use this if you are modifying TernoDBI code locally.

```json
{
  "mcpServers": {
    "ternodbi-query": {
      "command": "/absolute/path/to/venv/bin/dbi-mcp",
      "args": ["query"],
      "env": {
        "TERNODBI_API_URL": "http://127.0.0.1:8376",
        "TERNODBI_API_KEY": "dbi_query_..."
      }
    }
  }
}
```

## Troubleshooting

*   **Connection Refused**: Ensure the Django server is running (`python manage.py runserver`).
*   **Authentication Failed**: Check that your `TERNODBI_API_KEY` matches a valid active token in the database.
*   **Module Not Found**: If using local dev, ensure you ran `pip install -e .` and are pointing to the `dbi-mcp` binary in your virtualenv.
