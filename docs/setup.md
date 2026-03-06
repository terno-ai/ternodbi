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
| `DBI_SECRET_KEY` | Django cryptographic signing key. **Change this in prod!** | `unsafe-default` |
| `DBI_DEBUG` | Enable debug mode (detailed errors). | `True` |
| `DBI_ALLOWED_HOSTS` | Comma-separated list of allowed hostnames. | `localhost,127.0.0.1` |
| `DATABASE_ENGINE` | Primary backend DB: `MYSQL`, `POSTGRESQL`, or empty (SQLite). | `SQLite` |

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

    *   The Health Check will be available at: `http://localhost:8376/api/query/health/`
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
