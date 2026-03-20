<img width="2332" height="1276" alt="image" src="https://github.com/user-attachments/assets/91bc17e5-39f5-4934-9ffb-27d8040a3185" />


# TernoDBI: Database Intelligence Layer

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)
[![Django](https://img.shields.io/badge/django-4.2%2B-green)](https://www.djangoproject.com/)

**TernoDBI** is a database intelligence layer designed for **Security** and **Accuracy**, bridging the gap between **AI Agents** and **Enterprise Data**. It acts as a powerful standalone **Model Context Protocol (MCP)** server, or it can be directly embedded into your existing **Django** projects. Either way, it provides a unified, secure API for interacting with warehouse-scale databases while enforcing strict access controls and optimizing the database schema context for LLMs.



## Quick Start: Chat with your DB in 5 Minutes

The easiest way to get started is to run TernoDBI locally and connect your favorite AI agent.

1. **Install TernoDBI**
   ```bash
   pip install terno-dbi
   ```
2. **Start the Server**
   ```bash
   ternodbi start
   ```
   *(This automatically runs migrations, creates a default `admin`/`admin` user, and starts the server securely on `127.0.0.1:8376`)*
3. **Configure your Database**
   Open the admin panel at [http://127.0.0.1:8376/admin](http://127.0.0.1:8376/admin) and add your datasource connections.
4. **Generate an Access Token**
   Generate a token from the Admin UI or via the CLI:
   ```bash
   ternodbi manage issue_token --name "My Agent" --type query
   ```
5. **Configure MCP** (See [MCP Integration](#ternodbi-as-an-mcp-server) below)
6. **Start chatting with your enterprise data!**



## Key Features

* **Multi-Database Support:** Out-of-the-box unified connection handling for Postgres, MySQL, Snowflake, BigQuery, Databricks, Oracle, and SQLite.
* **Split MCP Architecture:**
  * **Query Server:** Read-only operations (list tables, schema info, execute SELECT queries) highly optimized for AI agents.
  * **Admin Server:** Write/Management operations (rename tables, update metadata, manage descriptions) designed for human-in-the-loop workflows.
* **Enterprise-Grade Security:**
  * **Row-Level Security (RLS):** Define strict SQL-based filters (e.g., `department_id = 5`) that are automatically injected into every executed query.
  * **Privacy-by-Default:** Hide sensitive tables or columns from the LLM's context window unless explicitly exposed to specific Roles.
  * **SQLShield:** Automatic AST-based SQL validation preventing prompt injection and destructive operations.
* **LLM-Ready Schema Enrichment:**
  * **Semantic Metadata:** Decouple physical database names (e.g., `t_users_v2_fnl`) from clean, user-facing semantic names (`Customers`).
  * **Statistical Profiling:** Automatic cardinality and distribution statistics injection to help LLMs consistently generate correct SQL filters.
* **High-Performance Pagination:**
  * **Cursor-Based (HMAC):** $O(1)$ performance. Benchmarks demonstrate a ~28x speedup over offset pagination.
  * **Server-Side Streaming:** Effortlessly export millions of rows via server-side cursors.



## Usage & Core APIs

### Running the API Server
```bash
ternodbi start
```

### Management Commands (CLI)
Automate your credential and access management simply via the built-in CLI:

```bash
# General Query Token (For standard AI Assistants)
ternodbi manage issue_token --name "Claude Agent" --type query --expires 30

# Admin Token (Full System Access)
ternodbi manage issue_token --name "System Admin" --type admin

# Scoped Token (Restricted to a Specific Datasource)
ternodbi manage issue_token --name "Finance Data Only" --type query --datasource 1
```

### Query API & Pagination

TernoDBI provides versatile REST endpoints.

**Offset Mode (Default)** - Best for standard UI implementations.
```json
POST /api/query/datasources/1/query/
{
    "sql": "SELECT * FROM users",
    "pagination_mode": "offset",
    "page": 2,
    "per_page": 50
}
```

**Cursor Mode (High Performance)** - Best for headless Agents & large Data Exports.
```json
POST /api/query/datasources/1/query/
{
    "sql": "SELECT * FROM users",
    "pagination_mode": "cursor",
    "per_page": 50,
    "cursor": "eyJ2IjoxLCJ2YWx..." 
}
```



## TernoDBI as an MCP Server

TernoDBI exposes Model Context Protocol (MCP) servers to effortlessly plug into MCP-compatible clients.

**Provided MCP Tools:**
* **Query Service:** `list_datasource`, `list_tables`, `list_table_columns`, `execute_query` (restricted securely via SQLShield).
* **Admin Service:** `add_datasource`, `delete_datasource`, `validate_connection`, `sync_metadata`, `rename_table`, `rename_column`, `update_table_description`, `update_column_description`, `get_table_info`.

### Example: Connecting Claude Desktop

1. Download and install [Claude Desktop](https://claude.ai/download).
2. Open Claude Desktop, navigate to **Account → Settings → Developer**.
3. Click **Edit Config** to open your `claude_desktop_config.json`.
4. Paste the following configuration:

```json
{
  "mcpServers": {
    "ternodbi-query": {
      "command": "uvx",
      "args": [
        "--from",
        "terno-dbi",
        "dbi-mcp",
        "query"
      ],
      "env": {
        "TERNODBI_API_URL": "http://127.0.0.1:8376",
        "TERNODBI_API_KEY": "dbi_query_YOUR_TOKEN_HERE"
      }
    },
    "ternodbi-admin": {
      "command": "uvx",
      "args": [
        "--from",
        "terno-dbi",
        "dbi-mcp",
        "admin"
      ],
      "env": {
        "TERNODBI_API_URL": "http://127.0.0.1:8376",
        "TERNODBI_API_KEY": "dbi_admin_YOUR_TOKEN_HERE"
      }
    }
  }
}
```
5. **Restart Claude Desktop.** You can now prompt Claude: *"Show me the available datasources."*



## Advanced Integrations

### Integrating TernoDBI inside a Custom Django Project
If you already have a mature Django infrastructure, TernoDBI can be integrated directly as a Django App.

**Step-by-Step Integration:**
1. Install the package in your Django environment: `pip install terno-dbi`
2. Add the core apps to your `INSTALLED_APPS` in `settings.py`:
   ```python
   INSTALLED_APPS = [
       ...
       'terno_dbi.core',
       # Optional: include query or admin apps based on your needs
   ]
   ```
3. Include TernoDBI's URL configurations in your root `urls.py`:
   ```python
   path('api/terno/', include('terno_dbi.core.urls')), # Mounts the core API endpoints
   ```
4. Run `python manage.py migrate` to apply the TernoDBI schema alongside your existing tables.
5. You can now use TernoDBI's internal models, query optimizers, and services directly programmatically inside your Django views or Celery tasks!

*(Refer to our comprehensive [Django Integration Guide](docs/django-integration.md) for advanced overriding and customization).*

### Integrating with Custom AI Agents (LangChain, LlamaIndex, Python)
TernoDBI's uniform REST API allows any custom agent architecture to ingest data securely without needing an MCP host.

**Step-by-Step Integration:**
1. Provision a specific `query` token for your custom script using the CLI.
2. In your Agent implementation, define a tool to call `/api/query/datasources/` to discover connections.
3. Your Agent flow should dictate:
   - Call `/api/query/datasources/{id}/schema/` to fetch the context-optimized tables and columns.
   - Inject this highly structured schema context into your LLM prompt.
   - Send the LLM's generated `sql` string payload via `POST` to `/api/query/datasources/{id}/query/`.
   - Iterate based on the response structure or SQLShield validation errors gracefully.

*(Refer to our Custom Agent SDK examples for reference implementations in Python and TypeScript).*



## Documentation
Detailed guides for setting up and mastering TernoDBI:
* [Setup Guide](docs/setup.md)
* [System Architecture](docs/architecture.md)
* [MCP Integration](docs/mcp-guide.md)
* [Security & SQLShield](docs/security.md)
* [Django Integration](docs/django-integration.md)




## Contributing
We welcome contributions.

1. **Fork the repo.**
2. **Create a feature branch:** `git checkout -b feat/your-feature`
3. **Add tests & docs.**
4. **Open a PR** describing your change.

Please follow the repo's code style (Black/flake8) and include unit tests for security-critical logic.


## Community & Support
If you need help, have a question, or want to discuss a new feature:
* Open an [Issue](https://github.com/terno-ai/ternodbi/issues) for bug reports and feature requests.
* Start a [Discussion](https://github.com/terno-ai/ternodbi/discussions) for general questions or architectural feedback.

## License
TernoDBI is proudly open-source and released under the **Apache 2.0 License**. See the [LICENSE](LICENSE) file for more details.


*Built with precision for the next generation of Enterprise AI.*
