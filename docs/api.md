# API Reference

TernoDBI provides two API services: **Query Service** for read operations and **Admin Service** for write operations. Both services require authentication via API tokens.

## Authentication

All API endpoints require a valid service token passed in the `Authorization` header:

```bash
Authorization: Bearer <your-api-token>
```

Generate tokens using:
```bash
python manage.py issue_token --name "My Agent" --type query
```

---

## Query Service

Base URL: `/api/query/`

The Query Service is designed for AI agents and provides read-only access to your databases.

### Health Check

```http
GET /api/query/health/
```

Returns the service status.

**Response:**
```json
{
  "status": "ok",
  "version": "1.0.0"
}
```

---

### List Datasources

```http
GET /api/query/datasources/
```

Returns all datasources accessible to the authenticated token.

**Response:**
```json
{
  "datasources": [
    {
      "id": 1,
      "name": "Production DB",
      "db_type": "postgres"
    }
  ]
}
```

---

### Get Datasource Details

```http
GET /api/query/datasources/{id}/
```

Returns detailed information about a specific datasource.

---

### Get Schema

```http
GET /api/query/datasources/{id}/schema/
```

Returns the complete schema for a datasource, including tables, columns, and relationships. This is the primary endpoint for LLMs to understand your database structure.

**Response:**
```json
{
  "datasource": "Production DB",
  "tables": [
    {
      "id": 1,
      "name": "users",
      "public_name": "Users",
      "description": "User accounts",
      "columns": [
        {
          "name": "id",
          "public_name": "User ID",
          "data_type": "integer",
          "description": "Primary key"
        }
      ]
    }
  ]
}
```

---

### List Tables

```http
GET /api/query/datasources/{id}/tables/
```

Returns all visible tables for a datasource.

---

### List Columns

```http
GET /api/query/datasources/{id}/tables/{table_id}/columns/
```

Returns columns for a specific table.

---

### List Foreign Keys

```http
GET /api/query/datasources/{id}/foreign-keys/
```

Returns foreign key relationships for schema understanding.

---

### Execute Query

```http
POST /api/query/datasources/{id}/query/
```

Executes a SQL query with advanced pagination support.

**Request Body:**
```json
{
  "sql": "SELECT * FROM users",
  "pagination_mode": "cursor",  // "offset" (default) or "cursor"
  "per_page": 50,              // Default: 50, Max: 500
  "page": 1,                   // For offset mode
  "cursor": "...",             // For cursor mode (next/prev_cursor from response)
  "direction": "forward",      // "forward" (default) or "backward"
  "order_by": [                // Required for cursor mode
    {"column": "id", "direction": "DESC"}
  ]
}
```

**Response:**
```json
{
  "status": "success",
  "table_data": {
    "columns": ["id", "name", "email"],
    "data": [
      [1, "Alice", "alice@example.com"],
      [2, "Bob", "bob@example.com"]
    ],
    "page": 1,
    "per_page": 50,
    "row_count": 1000,         // Estimation or exact count
    "total_pages": 20,         // Only available in offset mode
    "has_next": true,
    "has_prev": false,
    "next_cursor": "eyJ2Ijox...",  // Pass this to 'cursor' param for next page
    "prev_cursor": null
  },
  "warnings": [
    "PAGINATION_WARNING: Deep offset (50000) - consider cursor pagination"
  ]
}
```

---

### Get Sample Data

```http
GET /api/query/tables/{table_id}/sample/
```

Returns sample rows from a table (useful for LLM context).

---

## Admin Service

Base URL: `/api/admin/`

The Admin Service requires `admin` type tokens and provides write access for managing datasources and metadata.

### Create Datasource

```http
POST /api/admin/datasources/
```

Creates a new datasource connection.

**Request Body:**
```json
{
  "name": "My Database",
  "db_type": "postgres",
  "connection_string": "postgresql://user:pass@host:5432/db"
}
```

---

### Update Datasource

```http
PUT /api/admin/datasources/{id}/
```

Updates datasource configuration.

---

### Delete Datasource

```http
DELETE /api/admin/datasources/{id}/delete/
```

Removes a datasource and its metadata.

---

### Sync Metadata

```http
POST /api/admin/datasources/{id}/sync/
```

Re-syncs table and column metadata from the database.

---

### Update Table Metadata

```http
PUT /api/admin/tables/{id}/
```

Updates table metadata (description, public name, visibility).

**Request Body:**
```json
{
  "public_name": "Customer Orders",
  "description": "All customer order records",
  "is_visible": true
}
```

---

### Update Column Metadata

```http
PUT /api/admin/columns/{id}/
```

Updates column metadata.

**Request Body:**
```json
{
  "public_name": "Order Date",
  "description": "When the order was placed",
  "is_visible": true
}
```

---

### Validate Connection

```http
POST /api/admin/validate/
```

Tests a connection string without creating a datasource.

**Request Body:**
```json
{
  "connection_string": "postgresql://user:pass@host:5432/db"
}
```

---

### Get Table Info

```http
GET /api/admin/datasources/{id}/tables/{table_name}/info/
```

Returns detailed table information including column metadata.

---

### Get All Tables Info

```http
GET /api/admin/datasources/{id}/tables/info/
```

Returns metadata for all tables in a datasource.

---

## Error Responses

All endpoints return standard error responses:

```json
{
  "error": "Error message",
  "code": "ERROR_CODE"
}
```

Common status codes:
- `400` - Bad Request (validation error)
- `401` - Unauthorized (invalid or missing token)
- `403` - Forbidden (insufficient permissions)
- `404` - Not Found
- `500` - Internal Server Error
