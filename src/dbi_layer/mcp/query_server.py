import os
import sys
import json
import asyncio
import logging
from typing import Any, Dict, List

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from dbi_layer.client import TernoDBIClient
logger = logging.getLogger(__name__)

client = TernoDBIClient()

server = Server("ternodbi-query")


@server.list_tools()
async def list_tools() -> List[Tool]:
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
    try:
        result = None

        if name == "list_datasources":
            result = {"datasources": client.list_datasources()}
            if isinstance(result["datasources"], list):
                result["count"] = len(result["datasources"])

        elif name == "list_tables":
            datasource_id = arguments["datasource_id"]
            tables = client.list_tables(datasource_id)
            result = {
                "tables": tables,
                "count": len(tables) if isinstance(tables, list) else 0
            }

        elif name == "list_columns":
            table_id = arguments["table_id"]
            columns = client.list_columns(table_id)
            result = {
                "columns": columns,
                "count": len(columns)
            }

        elif name == "get_schema":
            datasource_id = arguments["datasource_id"]
            result = client.get_schema(datasource_id)

        elif name == "execute_query":
            datasource_id = arguments["datasource_id"]
            sql = arguments["sql"]
            limit = arguments.get("limit", 100)
            result = client.execute_query(datasource_id, sql, limit)

        elif name == "get_sample_data":
            table_id = arguments["table_id"]
            rows = arguments.get("rows", 10)
            result = client.get_sample_data(table_id, rows)

        elif name == "get_suggestions":
            datasource_id = arguments["datasource_id"]
            data = client.list_suggestions(datasource_id)
            result = {
                "suggestions": data.get("suggestions", []),
                "count": len(data.get("suggestions", []))
            }

        else:
            result = {"error": f"Unknown tool: {name}"}

        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

    except Exception as e:
        logger.exception(f"Error in Query MCP tool {name}: {e}")
        return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


async def run_server():
    print(f"Starting TernoDBI Query MCP Server (API: {client.base_url})", file=sys.stderr)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def main():
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
