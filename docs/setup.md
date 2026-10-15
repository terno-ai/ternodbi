# Installation & Setup Guide

## Prerequisites
*   Python 3.10+
*   pip or uv
*   A supported database (Postgres, MySQL, Snowflake, BigQuery, Databricks, Oracle, or SQLite)

## Installation

### From PyPI
```bash
pip install terno-dbi
```

### From Source (Development)
```bash
git clone https://github.com/terno-ai/ternodbi
cd ternodbi
pip install -e .
```

## Configuration

TernoDBI works out-of-the-box with SQLite, but production use requires configuring environment variables.

### Environment Variables

| Variable | Description | Default |
| :--- | :--- | :--- |
| `DBI_SECRET_KEY` | Django cryptographic signing key. **Change this in prod!** | `django-insecure-change-me-in-production` |
| `DBI_DEBUG` | Enable debug mode (detailed errors). | `True` |
| `DBI_ALLOWED_HOSTS` | Comma-separated list of allowed hostnames. | `*` |
| `DATABASE_ENGINE` | Primary backend DB: `MYSQL`, `POSTGRESQL`, or empty (SQLite). | `SQLite` |
| `MCP_ENCRYPTION_KEY` | Fernet key encrypting stored credentials and OAuth tokens. **Required when `DBI_DEBUG=False`.** | Derived from `DBI_SECRET_KEY` in debug only |
| `MCP_ENCRYPTION_KEYS` | Comma-separated keys for rotation; the first encrypts, any decrypts. | — |
| `TERNO_LOCAL_PORT` | Port used to build local URLs. | `8376` |
| `TERNO_MCP_BASE_URL` | Public URL of the MCP server, used in OAuth discovery. | `http://127.0.0.1:<port>` |
| `PROVISIONER_URL` | Host that serves the browser OAuth consent page. | `http://127.0.0.1:<port>` |
| `DJANGO_PROJECT_PATH` | Share the SQLite database of another Django project. | — |
| `TERNODBI_LOG_LEVEL` | Log level. | `INFO` |
| `TERNO_DISABLED_CONNECTORS` | Comma-separated connector keys to withhold on this deployment (not listed, not connectable; existing connections keep working). | — |

API-connector OAuth credentials (`TERNO_GOOGLE_OAUTH_CLIENT_ID`, …) are listed in the [README](../README.md#api-connectors).

Generate an encryption key with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### Database Specific Configuration

#### PostgreSQL (`DATABASE_ENGINE=POSTGRESQL`)
*   `POSTGRES_DB`
*   `POSTGRES_USER`
*   `POSTGRES_PASS`
*   `POSTGRES_HOST`
*   `POSTGRES_PORT`

#### MySQL (`DATABASE_ENGINE=MYSQL`)
*   `MYSQL_DB`
*   `MYSQL_USER`
*   `MYSQL_PASS`
*   `MYSQL_HOST`
*   `MYSQL_PORT`

## Running the Server

1.  **Start the Server**: 
    ```bash
    ternodbi start
    ```
    This single command automatically runs database migrations, creates a default admin user, and starts the API server.

    *   The Health Check will be available at: `http://localhost:8376/api/health/`
    *   The Admin Console will be at: `http://localhost:8376/admin/`

## Creating Your First User & Token

1.  **Create Superuser** (if you don't want to use the default `admin/admin`):
    ```bash
    ternodbi manage createsuperuser
    ```

2.  **Issue API Token** (for Agents):
    ```bash
    ternodbi manage issue_token --name "MyFirstAgent" --type query
    ```
