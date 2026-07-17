# Model Context Protocol (MCP) Integration

TernoDBI provides first-class support for the Model Context Protocol, enabling it to function as a tool provider for Claude Desktop, Terno Agents, or any other MCP-compliant client.

## Servers

We expose two separate MCP servers to separate concerns:

### 1. Query Server (`ternodbi-query`)
*   **Purpose**: Safe, read-only analysis, data retrieval, and reading durable memory.
*   **Tools Provided**:
    *   `list_datasources`: See available databases.
    *   `list_tables`: See tables in a database.
    *   `list_table_columns`: Get columns for a specific table.
    *   `get_sample_data`: Get sample rows from a table.
    *   `execute_query`: Run `SELECT` queries with advanced pagination.
        *   Supports `offset` (page-based) and `cursor` (infinite scroll) modes.
        *   Security: All cursors are HMAC-signed to prevent tampering.
    *   `list_memories`: The memory index (name/description/type/scope) — not full content.
    *   `get_memory`: Full content of one memory by name, plus its `content_hash`.
    *   `grep_memory`: Regex search over memory bodies.

### 2. Admin Server (`ternodbi-admin`)
*   **Purpose**: Management, curation, and writing durable memory.
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
    *   `save_memory`: Create, or fully replace (with `expected_hash`), a memory.
    *   `edit_memory`: Exact string replacement in an existing memory's body.
    *   `delete_memory`: Remove a memory.

Memory has two independent scopes: **store** (`user` = private to whoever wrote it,
`org` = shared with every agent working on this organisation's data — writing to
`org` requires an `admin:write`-scoped token) and **datasource** (omit for a global
fact, set `datasource_id` for a fact specific to one database).

## Connecting to Claude Desktop

First, mint a token for each server, bound to your organisation and a user —
an unbound token has no identity and can't use org-scoped features like memory:

```bash
ternodbi manage issue_token --name "claude-query" --type query --org <subdomain> --user <username>
ternodbi manage issue_token --name "claude-admin" --type admin --org <subdomain> --user <username>
```

Then add the following to your `claude_desktop_config.json`, using the two keys
each command prints.

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
*   **Memory tools return "no organisation/user" errors**: Your token wasn't minted with `--org`/`--user`. Re-issue it — see `issue_token` above.
