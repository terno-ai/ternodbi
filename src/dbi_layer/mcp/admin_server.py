"""
Admin MCP Server for TernoDBI.

Provides administrative operations for AI agents:
- Rename tables and columns (update public names)
- Manage query suggestions
- Update descriptions

Run with: python -m dbi_layer.mcp admin
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
from dbi_layer.services.validation import validate_datasource_input
from dbi_layer.services import schema_utils

logger = logging.getLogger(__name__)

server = Server("ternodbi-admin")


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
def rename_table(table_id, public_name):
    table = models.Table.objects.get(id=table_id)
    old_name = table.public_name
    table.public_name = public_name
    table.save()
    return {
        "success": True,
        "table_id": table_id,
        "old_public_name": old_name,
        "new_public_name": public_name
    }


@sync_to_async
def update_table_description(table_id, description):
    table = models.Table.objects.get(id=table_id)
    table.description = description
    table.save()
    return {
        "success": True,
        "table_id": table_id,
        "description": description
    }


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
def rename_column(column_id, public_name):
    column = models.TableColumn.objects.get(id=column_id)
    old_name = column.public_name
    column.public_name = public_name
    column.save()
    return {
        "success": True,
        "column_id": column_id,
        "old_public_name": old_name,
        "new_public_name": public_name
    }


@sync_to_async
def get_suggestions(datasource_id):
    suggestions = list(models.DatasourceSuggestions.objects.filter(data_source_id=datasource_id))
    return [
        {
            "id": s.id,
            "suggestion": s.suggestion
        }
        for s in suggestions
    ]


@sync_to_async
def add_suggestion(datasource_id, suggestion_text):
    datasource = models.DataSource.objects.get(id=datasource_id)
    suggestion = models.DatasourceSuggestions.objects.create(
        data_source=datasource,
        suggestion=suggestion_text
    )
    return {
        "success": True,
        "suggestion_id": suggestion.id,
        "suggestion": suggestion_text
    }


@sync_to_async
def delete_suggestion(suggestion_id):
    suggestion = models.DatasourceSuggestions.objects.get(id=suggestion_id)
    suggestion.delete()
    return {
        "success": True,
        "deleted_suggestion_id": suggestion_id
    }


@sync_to_async
def validate_connection_sync(db_type, connection_str, connection_json=None):
    """Validate a database connection."""
    error = validate_datasource_input(db_type, connection_str, connection_json)
    if error:
        return {"valid": False, "error": error}
    return {"valid": True, "message": "Connection validated successfully"}


@sync_to_async
def add_datasource_sync(display_name, db_type, connection_str, connection_json=None, description=""):
    """Create a new datasource and auto-sync metadata."""
    # Validate connection first
    error = validate_datasource_input(db_type, connection_str, connection_json)
    if error:
        return {"success": False, "error": f"Connection validation failed: {error}"}
    
    # Create datasource
    ds = models.DataSource.objects.create(
        display_name=display_name,
        type=db_type.lower(),
        connection_str=connection_str,
        connection_json=connection_json,
        description=description,
        dialect_name=db_type.lower(),
        enabled=True,
    )
    
    # Auto-sync metadata to discover tables and columns
    sync_result = schema_utils.sync_metadata(ds.id)
    
    return {
        "success": True,
        "datasource_id": ds.id,
        "datasource": {
            "id": ds.id,
            "name": ds.display_name,
            "type": ds.type,
            "enabled": ds.enabled,
        },
        "sync_result": {
            "tables_created": sync_result.get("tables_created", 0),
            "columns_created": sync_result.get("columns_created", 0),
        }
    }


@sync_to_async
def delete_datasource_sync(datasource_id):
    """Delete a datasource and all its metadata."""
    ds = models.DataSource.objects.get(id=datasource_id)
    name = ds.display_name
    ds.delete()
    return {
        "success": True,
        "message": f"Datasource '{name}' and all its metadata have been deleted"
    }


@sync_to_async
def get_table_info_sync(datasource_id, table_name):
    """Get detailed table information for description generation."""
    datasource = models.DataSource.objects.get(id=datasource_id, enabled=True)
    return schema_utils.get_table_info(datasource, table_name)


@sync_to_async
def get_all_tables_info_sync(datasource_id, table_names=None):
    """Get info for all tables in a datasource."""
    return schema_utils.get_datasource_tables_info(datasource_id, table_names)


@sync_to_async
def update_column_description_sync(column_id, description):
    """Update the description of a column."""
    column = models.TableColumn.objects.get(id=column_id)
    column.description = description
    column.save()
    return {
        "success": True,
        "column_id": column_id,
        "description": description
    }


@sync_to_async
def update_column_public_name_sync(column_id, public_name):
    """Update the public name of a column."""
    column = models.TableColumn.objects.get(id=column_id)
    old_name = column.public_name
    column.public_name = public_name
    column.save()
    return {
        "success": True,
        "column_id": column_id,
        "old_public_name": old_name,
        "new_public_name": public_name
    }


@sync_to_async
def sync_metadata_sync(datasource_id, overwrite=False):
    """Sync metadata from database - discover tables and columns."""
    return schema_utils.sync_metadata(datasource_id, overwrite)


@server.list_tools()
async def list_tools() -> List[Tool]:
    """List available Admin MCP tools."""
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
            description="List all tables in a datasource",
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
            name="rename_table",
            description="Update the public display name of a table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "integer",
                        "description": "ID of the table to rename"
                    },
                    "public_name": {
                        "type": "string",
                        "description": "New public display name for the table"
                    }
                },
                "required": ["table_id", "public_name"]
            }
        ),
        Tool(
            name="update_table_description",
            description="Update the description of a table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_id": {
                        "type": "integer",
                        "description": "ID of the table"
                    },
                    "description": {
                        "type": "string",
                        "description": "New description for the table"
                    }
                },
                "required": ["table_id", "description"]
            }
        ),
        Tool(
            name="list_columns",
            description="List all columns for a table",
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
            name="rename_column",
            description="Update the public display name of a column",
            inputSchema={
                "type": "object",
                "properties": {
                    "column_id": {
                        "type": "integer",
                        "description": "ID of the column to rename"
                    },
                    "public_name": {
                        "type": "string",
                        "description": "New public display name for the column"
                    }
                },
                "required": ["column_id", "public_name"]
            }
        ),
        Tool(
            name="list_suggestions",
            description="List all query suggestions for a datasource",
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
            name="add_suggestion",
            description="Add a query suggestion for a datasource",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_id": {
                        "type": "integer",
                        "description": "ID of the datasource"
                    },
                    "suggestion": {
                        "type": "string",
                        "description": "The query suggestion text"
                    }
                },
                "required": ["datasource_id", "suggestion"]
            }
        ),
        Tool(
            name="delete_suggestion",
            description="Delete a query suggestion",
            inputSchema={
                "type": "object",
                "properties": {
                    "suggestion_id": {
                        "type": "integer",
                        "description": "ID of the suggestion to delete"
                    }
                },
                "required": ["suggestion_id"]
            }
        ),
        Tool(
            name="validate_connection",
            description="Test a database connection before adding a datasource. Supported types: postgres, mysql, sqlite, bigquery, snowflake, oracle, databricks",
            inputSchema={
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "description": "Database type (postgres, mysql, sqlite, bigquery, snowflake, oracle, databricks)"
                    },
                    "connection_str": {
                        "type": "string",
                        "description": "Connection string (e.g., postgresql://user:pass@host:port/db)"
                    },
                    "connection_json": {
                        "type": "object",
                        "description": "Optional credentials JSON (required for BigQuery)"
                    }
                },
                "required": ["type", "connection_str"]
            }
        ),
        Tool(
            name="add_datasource",
            description="Add a new database connection. Validates the connection first.",
            inputSchema={
                "type": "object",
                "properties": {
                    "display_name": {
                        "type": "string",
                        "description": "Display name for the datasource"
                    },
                    "type": {
                        "type": "string",
                        "description": "Database type (postgres, mysql, sqlite, bigquery, snowflake, oracle, databricks)"
                    },
                    "connection_str": {
                        "type": "string",
                        "description": "Connection string (e.g., postgresql://user:pass@host:port/db)"
                    },
                    "connection_json": {
                        "type": "object",
                        "description": "Optional credentials JSON (required for BigQuery)"
                    },
                    "description": {
                        "type": "string",
                        "description": "Optional description of the datasource"
                    }
                },
                "required": ["display_name", "type", "connection_str"]
            }
        ),
        Tool(
            name="delete_datasource",
            description="Delete a datasource and all its metadata (tables, columns, etc.)",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_id": {
                        "type": "integer",
                        "description": "ID of the datasource to delete"
                    }
                },
                "required": ["datasource_id"]
            }
        ),
        Tool(
            name="get_table_info",
            description="Get detailed table info (columns, stats, sample data) for AI agents to generate descriptions. Returns data needed to generate table and column descriptions.",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_id": {
                        "type": "integer",
                        "description": "ID of the datasource"
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table"
                    }
                },
                "required": ["datasource_id", "table_name"]
            }
        ),
        Tool(
            name="get_all_tables_info",
            description="Get info for all tables (or specific tables) in a datasource. Use for batch description generation.",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_id": {
                        "type": "integer",
                        "description": "ID of the datasource"
                    },
                    "table_names": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of specific table names to get info for"
                    }
                },
                "required": ["datasource_id"]
            }
        ),
        Tool(
            name="update_column_description",
            description="Update the description of a column",
            inputSchema={
                "type": "object",
                "properties": {
                    "column_id": {
                        "type": "integer",
                        "description": "ID of the column"
                    },
                    "description": {
                        "type": "string",
                        "description": "New description for the column"
                    }
                },
                "required": ["column_id", "description"]
            }
        ),
        Tool(
            name="update_column_public_name",
            description="Update the public display name of a column",
            inputSchema={
                "type": "object",
                "properties": {
                    "column_id": {
                        "type": "integer",
                        "description": "ID of the column"
                    },
                    "public_name": {
                        "type": "string",
                        "description": "New public display name for the column"
                    }
                },
                "required": ["column_id", "public_name"]
            }
        ),
        Tool(
            name="sync_metadata",
            description="Discover and sync tables/columns from the database. Run this after adding a new datasource to load its schema.",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_id": {
                        "type": "integer",
                        "description": "ID of the datasource to sync"
                    },
                    "overwrite": {
                        "type": "boolean",
                        "description": "If true, update existing metadata"
                    }
                },
                "required": ["datasource_id"]
            }
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle Admin MCP tool calls."""
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
        
        elif name == "rename_table":
            table_id = arguments["table_id"]
            public_name = arguments["public_name"]
            result = await rename_table(table_id, public_name)
        
        elif name == "update_table_description":
            table_id = arguments["table_id"]
            description = arguments["description"]
            result = await update_table_description(table_id, description)
        
        elif name == "list_columns":
            table_id = arguments["table_id"]
            columns = await get_columns(table_id)
            result = {
                "columns": columns,
                "count": len(columns)
            }
        
        elif name == "rename_column":
            column_id = arguments["column_id"]
            public_name = arguments["public_name"]
            result = await rename_column(column_id, public_name)
        
        elif name == "list_suggestions":
            datasource_id = arguments["datasource_id"]
            suggestions = await get_suggestions(datasource_id)
            result = {
                "suggestions": suggestions,
                "count": len(suggestions)
            }
        
        elif name == "add_suggestion":
            datasource_id = arguments["datasource_id"]
            suggestion_text = arguments["suggestion"]
            result = await add_suggestion(datasource_id, suggestion_text)
        
        elif name == "delete_suggestion":
            suggestion_id = arguments["suggestion_id"]
            result = await delete_suggestion(suggestion_id)
        
        elif name == "validate_connection":
            db_type = arguments["type"]
            connection_str = arguments["connection_str"]
            connection_json = arguments.get("connection_json")
            result = await validate_connection_sync(db_type, connection_str, connection_json)
        
        elif name == "add_datasource":
            display_name = arguments["display_name"]
            db_type = arguments["type"]
            connection_str = arguments["connection_str"]
            connection_json = arguments.get("connection_json")
            description = arguments.get("description", "")
            result = await add_datasource_sync(display_name, db_type, connection_str, connection_json, description)
        
        elif name == "delete_datasource":
            datasource_id = arguments["datasource_id"]
            result = await delete_datasource_sync(datasource_id)
        
        elif name == "get_table_info":
            datasource_id = arguments["datasource_id"]
            table_name = arguments["table_name"]
            result = await get_table_info_sync(datasource_id, table_name)
        
        elif name == "get_all_tables_info":
            datasource_id = arguments["datasource_id"]
            table_names = arguments.get("table_names")
            result = await get_all_tables_info_sync(datasource_id, table_names)
        
        elif name == "update_column_description":
            column_id = arguments["column_id"]
            description = arguments["description"]
            result = await update_column_description_sync(column_id, description)
        
        elif name == "update_column_public_name":
            column_id = arguments["column_id"]
            public_name = arguments["public_name"]
            result = await update_column_public_name_sync(column_id, public_name)
        
        elif name == "sync_metadata":
            datasource_id = arguments["datasource_id"]
            overwrite = arguments.get("overwrite", False)
            result = await sync_metadata_sync(datasource_id, overwrite)
        
        else:
            result = {"error": f"Unknown tool: {name}"}
        
        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
    
    except models.DataSource.DoesNotExist:
        return [TextContent(type="text", text=json.dumps({"error": "Datasource not found"}))]
    except models.Table.DoesNotExist:
        return [TextContent(type="text", text=json.dumps({"error": "Table not found"}))]
    except models.TableColumn.DoesNotExist:
        return [TextContent(type="text", text=json.dumps({"error": "Column not found"}))]
    except models.DatasourceSuggestions.DoesNotExist:
        return [TextContent(type="text", text=json.dumps({"error": "Suggestion not found"}))]
    except Exception as e:
        logger.exception(f"Error in Admin MCP tool {name}: {e}")
        return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


async def run_server():
    """Run the Admin MCP server."""
    print("⚙️ Starting TernoDBI Admin MCP Server", file=sys.stderr)
    
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def main():
    """CLI entry point for Admin MCP."""
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
