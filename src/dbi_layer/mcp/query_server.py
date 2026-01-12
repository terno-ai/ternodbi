"""
Query MCP Server for TernoDBI.

Provides read-only database operations for AI agents:
- List datasources, tables, columns
- Execute queries with SQLShield translation
- Get schema and sample data

Run with: python -m dbi_layer.mcp query
"""

import os
import sys
import json
import asyncio
import logging
from typing import Any, Dict, List

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# Setup Django for TernoDBI
# By default, uses TernoDBI standalone server settings (dbi_server.settings)
# For TernoAI integration, set:
#   - TERNO_PROJECT_PATH=/path/to/terno-ai/terno
#   - DJANGO_SETTINGS_MODULE=mysite.settings

# Add TernoDBI server to path (for standalone mode)
TERNODBI_SERVER_PATH = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'server')
if os.path.exists(TERNODBI_SERVER_PATH):
    sys.path.insert(0, os.path.abspath(TERNODBI_SERVER_PATH))

# Optional: Add custom project path (for TernoAI integration)
TERNO_PROJECT_PATH = os.environ.get('TERNO_PROJECT_PATH', '')
if TERNO_PROJECT_PATH and TERNO_PROJECT_PATH not in sys.path:
    sys.path.insert(0, TERNO_PROJECT_PATH)

# Default to TernoDBI standalone settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbi_server.settings')

import django
django.setup()

from asgiref.sync import sync_to_async
from dbi_layer.django_app import models
from dbi_layer.connectors import ConnectorFactory
from dbi_layer.services.shield import generate_mdb, generate_native_sql

logger = logging.getLogger(__name__)

server = Server("ternodbi-query")


# Helper functions to wrap sync Django ORM calls
@sync_to_async
def get_datasources():
    datasources = list(models.DataSource.objects.filter(enabled=True))
    return [
        {
            "id": ds.id,
            "name": ds.display_name,
            "type": ds.type,
            "description": ds.description or ""
        }
        for ds in datasources
    ]


@sync_to_async
def get_tables(datasource_id):
    tables = list(models.Table.objects.filter(data_source_id=datasource_id))
    return [
        {
            "id": t.id,
            "name": t.name,
            "public_name": t.public_name,
            "description": t.description or ""
        }
        for t in tables
    ]


@sync_to_async
def get_columns(table_id):
    columns = list(models.TableColumn.objects.filter(table_id=table_id))
    return [
        {
            "id": c.id,
            "name": c.name,
            "public_name": c.public_name,
            "data_type": c.data_type
        }
        for c in columns
    ]


@sync_to_async
def get_schema(datasource_id):
    datasource = models.DataSource.objects.get(id=datasource_id)
    tables = list(models.Table.objects.filter(data_source=datasource))
    
    schema = []
    for table in tables:
        columns = list(models.TableColumn.objects.filter(table=table))
        schema.append({
            "table_name": table.name,
            "public_name": table.public_name,
            "description": table.description or "",
            "columns": [
                {
                    "name": c.name,
                    "public_name": c.public_name,
                    "type": c.data_type
                }
                for c in columns
            ]
        })
    
    return {
        "datasource": datasource.display_name,
        "schema": schema,
        "table_count": len(schema)
    }


@sync_to_async
def execute_query(datasource_id, sql, limit=100):
    import sqlalchemy
    
    datasource = models.DataSource.objects.get(id=datasource_id)
    
    # Generate MDB for SQL translation
    mdb = generate_mdb(datasource)
    
    # Translate public names to native names using SQLShield
    native_sql_response = generate_native_sql(mdb, sql, datasource.dialect_name)
    
    if native_sql_response['status'] == 'error':
        return {"error": native_sql_response['error']}
    
    native_sql = native_sql_response['native_sql']
    
    # Add LIMIT if not present
    if "limit" not in native_sql.lower():
        native_sql = f"{native_sql} LIMIT {limit}"
    
    # Execute query
    connector = ConnectorFactory.create_connector(
        datasource.type,
        datasource.connection_str,
        credentials=datasource.connection_json
    )
    
    with connector.get_connection() as conn:
        result_proxy = conn.execute(sqlalchemy.text(native_sql))
        rows = result_proxy.fetchall()
        columns = list(result_proxy.keys())
        
        data = [dict(zip(columns, row)) for row in rows]
        
        return {
            "columns": columns,
            "data": data,
            "row_count": len(data),
            "translated_sql": native_sql
        }


@sync_to_async
def get_sample_data(table_id, num_rows=10):
    import sqlalchemy
    
    table = models.Table.objects.get(id=table_id)
    datasource = table.data_source
    
    connector = ConnectorFactory.create_connector(
        datasource.type,
        datasource.connection_str,
        credentials=datasource.connection_json
    )
    
    sql = f"SELECT * FROM {table.name} LIMIT {num_rows}"
    
    with connector.get_connection() as conn:
        result_proxy = conn.execute(sqlalchemy.text(sql))
        rows = result_proxy.fetchall()
        columns = list(result_proxy.keys())
        
        data = [dict(zip(columns, row)) for row in rows]
        
        return {
            "table": table.public_name,
            "columns": columns,
            "data": data,
            "row_count": len(data)
        }


@sync_to_async
def get_suggestions(datasource_id):
    suggestions = list(models.DatasourceSuggestions.objects.filter(data_source_id=datasource_id))
    return [s.suggestion for s in suggestions]


@server.list_tools()
async def list_tools() -> List[Tool]:
    """List available Query MCP tools."""
    return [
        Tool(
            name="list_datasources",
            description="List all configured database connections",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="list_tables",
            description="List all tables in a datasource with their public names",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_id": {
                        "type": "integer",
                        "description": "ID of the datasource"
                    }
                },
                "required": ["datasource_id"]
            }
        ),
        Tool(
            name="list_columns",
            description="List all columns for a table with their public names and types",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "integer",
                        "description": "ID of the table"
                    }
                },
                "required": ["table_id"]
            }
        ),
        Tool(
            name="get_schema",
            description="Get the full schema (tables and columns) for a datasource",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_id": {
                        "type": "integer",
                        "description": "ID of the datasource"
                    }
                },
                "required": ["datasource_id"]
            }
        ),
        Tool(
            name="execute_query",
            description="Execute a SQL query. Supports public table/column names which are translated to native names.",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_id": {
                        "type": "integer",
                        "description": "ID of the datasource to query"
                    },
                    "sql": {
                        "type": "string",
                        "description": "SQL query to execute (can use public names)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max rows to return (default: 100)"
                    }
                },
                "required": ["datasource_id", "sql"]
            }
        ),
        Tool(
            name="get_sample_data",
            description="Get sample rows from a table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "integer",
                        "description": "ID of the table"
                    },
                    "rows": {
                        "type": "integer",
                        "description": "Number of sample rows (default: 10)"
                    }
                },
                "required": ["table_id"]
            }
        ),
        Tool(
            name="get_suggestions",
            description="Get query suggestions for a datasource",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_id": {
                        "type": "integer",
                        "description": "ID of the datasource"
                    }
                },
                "required": ["datasource_id"]
            }
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle Query MCP tool calls."""
    try:
        result = None
        
        if name == "list_datasources":
            datasources = await get_datasources()
            result = {
                "datasources": datasources,
                "count": len(datasources)
            }
        
        elif name == "list_tables":
            datasource_id = arguments["datasource_id"]
            tables = await get_tables(datasource_id)
            result = {
                "tables": tables,
                "count": len(tables)
            }
        
        elif name == "list_columns":
            table_id = arguments["table_id"]
            columns = await get_columns(table_id)
            result = {
                "columns": columns,
                "count": len(columns)
            }
        
        elif name == "get_schema":
            datasource_id = arguments["datasource_id"]
            result = await get_schema(datasource_id)
        
        elif name == "execute_query":
            datasource_id = arguments["datasource_id"]
            sql = arguments["sql"]
            limit = arguments.get("limit", 100)
            result = await execute_query(datasource_id, sql, limit)
        
        elif name == "get_sample_data":
            table_id = arguments["table_id"]
            rows = arguments.get("rows", 10)
            result = await get_sample_data(table_id, rows)
        
        elif name == "get_suggestions":
            datasource_id = arguments["datasource_id"]
            suggestions = await get_suggestions(datasource_id)
            result = {
                "suggestions": suggestions,
                "count": len(suggestions)
            }
        
        else:
            result = {"error": f"Unknown tool: {name}"}
        
        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
    
    except models.DataSource.DoesNotExist:
        return [TextContent(type="text", text=json.dumps({"error": "Datasource not found"}))]
    except models.Table.DoesNotExist:
        return [TextContent(type="text", text=json.dumps({"error": "Table not found"}))]
    except Exception as e:
        logger.exception(f"Error in Query MCP tool {name}: {e}")
        return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


async def run_server():
    """Run the Query MCP server."""
    print("🔍 Starting TernoDBI Query MCP Server", file=sys.stderr)
    
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def main():
    """CLI entry point for Query MCP."""
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
