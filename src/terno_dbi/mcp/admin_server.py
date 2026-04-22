import os
import sys
import json
import asyncio
import logging
from typing import Any, Dict, List
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from terno_dbi.client import TernoDBIClient


logger = logging.getLogger(__name__)

client = TernoDBIClient()

server = Server("ternodbi-admin")


@server.list_tools()
async def list_tools() -> List[Tool]:
    return [
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
    logger.info("Admin tool called: %s", name)
    logger.debug("Tool arguments: %s", arguments)
    try:
        result = None

        if name == "rename_table":
            table_id = arguments["table_id"]
            public_name = arguments["public_name"]
            result = client.update_table(table_id, public_name=public_name)

        elif name == "update_table_description":
            table_id = arguments["table_id"]
            description = arguments["description"]
            result = client.update_table(table_id, description=description)

        elif name == "rename_column":
            column_id = arguments["column_id"]
            public_name = arguments["public_name"]
            result = client.update_column(column_id, public_name=public_name)



        elif name == "validate_connection":
            db_type = arguments["type"]
            connection_str = arguments["connection_str"]
            connection_json = arguments.get("connection_json")
            result = client.validate_connection(db_type, connection_str, connection_json)

        elif name == "add_datasource":
            display_name = arguments["display_name"]
            db_type = arguments["type"]
            connection_str = arguments["connection_str"]
            connection_json = arguments.get("connection_json")
            description = arguments.get("description", "")
            result = client.create_datasource(display_name, db_type, connection_str, connection_json, description)

        elif name == "delete_datasource":
            datasource_id = arguments["datasource_id"]
            result = client.delete_datasource(datasource_id)

        elif name == "get_table_info":
            datasource_id = arguments["datasource_id"]
            table_name = arguments["table_name"]
            result = client.get_table_info(datasource_id, table_name)

        elif name == "update_column_description":
            column_id = arguments["column_id"]
            description = arguments["description"]
            result = client.update_column(column_id, description=description)

        elif name == "sync_metadata":
            datasource_id = arguments["datasource_id"]
            overwrite = arguments.get("overwrite", False)
            result = client.sync_metadata(datasource_id, overwrite=overwrite)

        else:
            result = {"error": f"Unknown tool: {name}"}

        logger.debug("Tool %s completed successfully", name)
        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

    except Exception as e:
        logger.exception("Error in Admin MCP tool %s", name)
        return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


async def run_server():
    logger.info("Starting TernoDBI Admin MCP Server")
    logger.debug("API Base URL: %s", client.base_url)
    print(f"Starting TernoDBI Admin MCP Server (API: {client.base_url})", file=sys.stderr)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())
    logger.info("Admin MCP Server stopped")


def main():
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
