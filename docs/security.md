# Security & SQLShield

Security is the primary design goal of TernoDBI. When giving AI Agents access to databases, the risk of data exfiltration or destruction is high. TernoDBI mitigates this with a defense-in-depth approach.

## 1. SQLShield: The Query Firewall

SQLShield is our proprietary SQL validation engine. Unlike regex-based filters (which are easily bypassed), SQLShield parses SQL into an Abstract Syntax Tree (AST) using `sqlglot`.

### How it works
1.  **Parsing**: The incoming query is parsed into a structural representation.
2.  **Analysis**: The AST is traversed to check for forbidden node types.
3.  **Transformation**: The query is rewritten (e.g., adding `LIMIT` clauses) before execution.

### Protections
*   **ReadOnly Enforcement**: Rejects `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `GRANT`, `TRUNCATE`.
*   **System Catalog Protection**: Prevents access to `information_schema` or `pg_catalog` unless explicitly allowed.
*   **Limit Enforcement**: Automatically appends `LIMIT 100` (or configurable max) to prevent DOS attacks via massive result sets.

## 2. Service Token Authentication

TernoDBI uses a custom implementation of API Key authentication (`ServiceToken`).

### Token Types
*   **Query Token**: Read-only access. Can be scoped to specific Datasources.
*   **Admin Token**: Full access to the Admin API (Datasource creation, Metadata updates).

### Scoping
Tokens can be restricted to specific Datasource IDs.
```python
# Only allow access to Financial DB (ID: 5)
python manage.py issue_token --name "FinBot" --type query --datasource 5
```

### Expiration
Tokens can be set to auto-expire.
```python
# Valid for 24 hours
python manage.py issue_token --name "TempAccess" --expires 1
```

## 3. Best Practices

1.  **Least Privilege**: Always create database users (on Postgres/Snowflake) with read-only permissions. Do not give TernoDBI a `root` or `admin` DB connection string.
2.  **Ephemeral Tokens**: For chat sessions, generate a short-lived token per session.
3.  **Network Isolation**: Run TernoDBI in a private subnet, accessible only by your Agent infrastructure.
