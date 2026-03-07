# Architecture Overview

TernoDBI is designed as a **Database Interface Layer** that sits between your AI Agents and your physical data warehouses. It solves the "last mile" problem of letting LLMs query databases securely.

## System Diagram

```mermaid
graph TD
    User([AI Agent / Claude]) -->|HTTP / MCP| Server[TernoDBI Server]

    subgraph Pipeline[" "]
        direction TB
        Server -->|1. Authenticate| Auth[Auth Middleware]
        Auth -->|Valid| Router{Request Type}
        
        %% Admin Path (Bypasses Shield)
        Router -->|Admin Tool| Admin[Admin Service]
        Admin -->|Metadata Ops| Connect[Unified Connector]

        %% Query Path (Protected)
        Router -->|Query Tool| Shield[SQLShield Engine]
        Shield -->|2. Parse & Validate| AST[AST Analysis]
        AST --x|Forbidden| Error[Block Request]
        AST -->|3. Transform| SafeQuery[Safe SQL Builder]
        SafeQuery -->|Execute| Connect
    end

    Connect -->|SQL Dialect| DB[(Target Warehouse)]
    
    subgraph Backends[" "]
        direction LR
        DB -.-> PG[Postgres]
        DB -.-> MySQL[MySQL]
        DB -.-> SQ[SQLite]
        DB -.-> Snow[Snowflake]
        DB -.-> BQ[BigQuery]
        DB -.-> DBX[Databricks]
        DB -.-> Ora[Oracle]
    end
    
    %% Styling
    classDef protective fill:#2e001f,stroke:#ff0055,stroke-width:2px;
    classDef admin fill:#0f172a,stroke:#6366f1,stroke-width:2px,stroke-dasharray: 5 5;
    classDef container fill:transparent,stroke:#94a3b8,stroke-width:2px,color:#fff;
    
    class Shield,AST,SafeQuery protective;
    class Admin admin;
    class Pipeline,Backends container;
```

## Core Components

### 1. The MCP Layer
TernoDBI exposes two distinct servers via the [Model Context Protocol](https://modelcontextprotocol.io/):

*   **Query Server**: Read-only. Exposes tools like `list_tables`, `list_table_columns`, and `execute_query`. Designed for safety.
*   **Admin Server**: Write-access. Exposes tools like `rename_table`, `update_table_description`, and `validate_connection`. Designed for human-in-the-loop curation.

### 2. SQLShield
The security engine. It parses every incoming SQL query into an Abstract Syntax Tree (AST) using `sqlglot`.

*   **Validation**: Rejects mutations (`INSERT`, `DROP`, `ALTER`).
*   **Transformation**: Can rewrite queries (e.g., forcing `LIMIT`, applying Row Level Security).
*   **Dialect Translation**: Converts generic SQL into database-specific dialects (e.g., handling BigQuery backticks vs Postgres quotes).

### 3. Unified Connector System
A factory-based abstraction over `SQLAlchemy` and native drivers.

*   **Single Interface**: Application code (and Agents) interact with a single `Connector` interface.
*   **Complexity Handling**: TernoDBI handles the complexity of connection pooling, cursor management, and type conversion for each backend.

### 4. Service Token Authentication
A custom authentication system designed for agents.

*   **Scopes**: Tokens can be global or restricted to specific Datasource IDs.
*   **Expiration**: Tokens can be short-lived (e.g., ephemeral tokens for a specific chat session).
*   **Audit**: Usage is tracked per-token.
