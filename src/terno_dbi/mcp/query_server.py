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
                    "datasource": {
                        "type": "string",
                        "description": "Datasource name or ID"
                    }
                },
                "required": ["datasource"]
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
                    "datasource": {
                        "type": "string",
                        "description": "Datasource name or ID"
                    }
                },
                "required": ["datasource"]
            }
        ),
        Tool(
            name="execute_query",
            description="""Execute a SQL query with pagination support.

Pagination Modes:
- offset: Traditional page-based (default). Good for UI with page numbers.
- cursor: High-performance for large datasets. Use for infinite scroll/streaming.

For very large results, use cursor mode with the returned next_cursor.

Tip: To get the total row count of a table without scanning it, use 'offset' mode with a LIMIT 1 query (e.g. SELECT * FROM table LIMIT 1). The response will include 'row_count' metadata. Do not run SELECT COUNT(*).""",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource": {
                        "type": "string",
                        "description": "Datasource name or ID"
                    },
                    "sql": {
                        "type": "string",
                        "description": "SQL query to execute (can use public names)"
                    },
                    "pagination_mode": {
                        "type": "string",
                        "enum": ["offset", "cursor"],
                        "description": "Pagination strategy (default: offset)"
                    },
                    "page": {
                        "type": "integer",
                        "description": "Page number for offset mode (1-indexed, default: 1)"
                    },
                    "per_page": {
                        "type": "integer",
                        "description": "Rows per page (default: 50, max: 500)"
                    },
                    "cursor": {
                        "type": "string",
                        "description": "Cursor from previous response (for cursor mode)"
                    },
                    "direction": {
                        "type": "string",
                        "enum": ["forward", "backward"],
                        "description": "Direction for cursor pagination (default: forward)"
                    }
                },
                "required": ["datasource", "sql"]
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
            datasource = arguments["datasource"]
            tables = client.list_tables(datasource)
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
            datasource = arguments["datasource"]
            result = client.get_schema(datasource)

        elif name == "execute_query":
            datasource = arguments["datasource"]
            sql = arguments["sql"]
            pagination_mode = arguments.get("pagination_mode", "offset")
            page = arguments.get("page", 1)
            per_page = arguments.get("per_page", 50)
            cursor = arguments.get("cursor")
            direction = arguments.get("direction", "forward")
            result = client.execute_query(
                datasource,
                sql,
                pagination_mode=pagination_mode,
                page=page,
                per_page=per_page,
                cursor=cursor,
                direction=direction
            )

        elif name == "get_sample_data":
            table_id = arguments["table_id"]
            rows = arguments.get("rows", 10)
            result = client.get_sample_data(table_id, rows)

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
