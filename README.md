# TernoDBI: Database Interface Layer

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)
[![Django](https://img.shields.io/badge/django-4.2%2B-green)](https://www.djangoproject.com/)
[![Coverage](https://img.shields.io/badge/coverage-99%25-brightgreen)](tests/unit/services/pagination/)

**TernoDBI** is a database interface layer designed for Security and Accuracy, bridging the gap between **AI Agents** and **Enterprise Data**. It provides a unified, secure API for interacting with warehouse-scale databases while enforcing strict access controls and optimizing schema context for LLMs.

---

## Key Features

*   **Multi-Database Support**: Unified connection handling for **Postgres, MySQL, Snowflake, BigQuery, Databricks, Oracle, and SQLite**.
*   **Split MCP Architecture**:
    *   **Query Server**: Read-only operations (list tables, schema info, execute SELECT queries) optimized for agents.
    *   **Admin Server**: Write/Management operations (rename tables, update metadata, manage descriptions) for human-in-the-loop workflows.
*   **Enterprise Security**:
    *   **Row Level Security (RLS)**: Define strict SQL-based filters (e.g., `department_id = 5`) automatically injected into every query.
    *   **Privacy-by-Default**: Hide sensitive tables/columns from LLM context unless explicitly exposed to specific Roles.
    *   **SQLShield**: Automatic AST-based SQL validation preventing injection and destructive operations.
*   **LLM-Ready Schema Enrichment**:
    *   **Semantic Metadata**: Decouple physical DB names (`t_users_v2`) from user-facing semantic names (`Customers`).
    *   **Statistical Profiling**: Automatic cardinality and distribution stats to help LLMs generate correct filters.
*   **High-Performance Pagination**:
    *   **Cursor-Based**: **O(1) performance** (HMAC-signed). Benchmarks show **~28x speedup** over offset pagination.
    *   **Streaming**: Server-side cursor support for exporting millions of rows.

---

## Documentation

Detailed guides for setting up and using TernoDBI:

*   **[Setup Guide](docs/setup.md)**: Installation, Environment Variables, and Server Startup.
*   **[Architecture](docs/architecture.md)**: System design, request flow, and component breakdown.
*   **[MCP Integration](docs/mcp-guide.md)**: How to connect agents (Claude Desktop, Terno Agents).
*   **[Security & SQLShield](docs/security.md)**: Deep dive into our security model and token system.

---

## Installation

```bash
pip install terno-dbi
# OR for local development
pip install -e .
```

---

## Configuration

Copy the sample environment file to start:

```bash
cp server/env-sample.sh server/env.sh
source server/env.sh
```

### Essential Variables

| Variable | Description | Default |
| :--- | :--- | :--- |
| `DBI_SECRET_KEY` | Cryptographic key for signing cursors/tokens. | Unsafe Default |
| `DBI_DEBUG` | Toggle debug mode. | `True` |
| `DATABASE_ENGINE` | `MYSQL`, `POSTGRESQL`, or empty for SQLite. | `SQLite` |

### Database Setup Scenarios

*   **Standalone SQLite**: Set `DATABASE_ENGINE=` (empty). DB created in `server/db.sqlite3`.
*   **Shared SQLite (Embedded)**: Set `DJANGO_PROJECT_PATH=/path/to/other/django`. TernoDBI will attach to that project's database.
*   **Production (Postgres/MySQL)**: Set `DATABASE_ENGINE=POSTGRESQL` and provide `POSTGRES_DB`, `POSTGRES_USER`, etc.

---

## Usage

### 1. Running the API Server

```bash
cd server
python manage.py migrate
python manage.py runserver 0.0.0.0:8000
```

### 2. Management Commands (CLI)

Use the built-in CLI to manage access tokens for your agents:

```bash
# General Query Token
python manage.py issue_token --name "Claude Agent" --type query --expires 30

# Admin Token (Full Access)
python manage.py issue_token --name "System Admin" --type admin

# Scoped Token (Specific Datasource)
python manage.py issue_token --name "Finance Data Only" --type query --datasource 1
```

### 3. Query API & Pagination

**Offset Mode (Default)** - Best for UI.
```json
POST /api/query/datasources/1/query/
{
    "sql": "SELECT * FROM users",
    "pagination_mode": "offset",
    "page": 2,
    "per_page": 50
}
```

**Cursor Mode (High Performance)** - Best for Agents & Data Export.
```json
POST /api/query/datasources/1/query/
{
    "sql": "SELECT * FROM users",
    "pagination_mode": "cursor",
    "per_page": 50,
    "cursor": "eyJ2IjoxLCJ2YWx..." 
}
```

---

## MCP Server Integration

### Claude Desktop Setup

#### Step 1: Download & Install Claude Desktop

Download Claude Desktop from [https://claude.ai/download](https://claude.ai/download) and install it on your machine.

#### Step 2: Open Configuration

1.  Launch Claude Desktop
2.  Go to **Account** → **Settings**
3.  Navigate to **Developer** section
4.  Click **Edit Config** to open `claude_desktop_config.json`
![Claude Desktop Settings](assets/config.png)

#### Step 3: Add MCP Server Configuration

Add the following configuration to your `claude_desktop_config.json`:

**Local Development:**
```json
{
  "mcpServers": {
    "ternodbi-admin": {
      "command": "/path/to/your/venv/bin/dbi-mcp",
      "args": ["admin"],
      "env": {
        "TERNODBI_API_URL": "http://127.0.0.1:8000",
        "TERNODBI_API_KEY": "dbi_admin_..."
      }
    },
    "ternodbi-query": {
      "command": "/path/to/your/venv/bin/dbi-mcp",
      "args": ["query"],
      "env": {
        "TERNODBI_API_URL": "http://127.0.0.1:8000",
        "TERNODBI_API_KEY": "dbi_query_..."
      }
    }
  }
}
```
> [!TIP]
> Run `which dbi-mcp` in your terminal to find the absolute path to use in the configuration above.
**Production (UVX):**
```json
{
  "mcpServers": {
    "ternodbi-query": {
      "command": "uvx",
      "args": ["--from", "terno-dbi", "dbi-mcp", "query"],
      "env": {
        "TERNODBI_API_URL": "https://dbi.yourdomain.com",
        "TERNODBI_API_KEY": "dbi_query_..."
      }
    }
  }
}
```

#### Step 4: Restart & Verify

1.  Save and close the `claude_desktop_config.json` file.
2.  **Completely quit** Claude Desktop (not just close the window).
3.  Reopen Claude Desktop.
4.  Ask Claude: *"Show me all datasources"*.

---

## Security Deep Dive

### Row Level Security (RLS)
RLS filters (`TableRowFilter`) are injected into the AST of every query via `sqlshield`.
*   **Logic**: `Global Filters AND (Role A Filter OR Role B Filter)`.
*   **Example**: `region = 'US'` is automatically appended if the user is in the "US Region" group.

### Privacy & Column Hiding
*   **PrivateColumnSelector**: Columns marked as private (e.g., `salary`) are removed from the schema context sent to the LLM.
*   **Access**: Only roles explicitly granted permission in `GroupColumnSelector` will see these columns.

---

## Testing & Quality

*   **Coverage**: **99%** unit test coverage for core services.
*   **Benchmarks**: Validated performance gains (~28x) for large datasets.

To run tests:
```bash
pytest tests/unit/services/pagination/
```

---

## License
Apache 2.0 - See [LICENSE](LICENSE) for details.
