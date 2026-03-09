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
            name="list_table_columns",
            description="List all columns for a table with their public names and types",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource": {
                        "type": "string",
                        "description": "Datasource name or ID"
                    },
                    "table": {
                        "type": "string",
                        "description": "Table name or ID"
                    }
                },
                "required": ["datasource", "table"]
            }
        ),
        Tool(
            name="execute_query",
            description="""Execute a SQL query with pagination support.

Pagination Modes:
- offset: Traditional page-based (default). Returns has_next/has_prev.
- cursor: High-performance keyset pagination for large datasets. Requires ORDER BY in the SQL or an explicit order_by parameter.

The response includes pagination_mode_used to indicate which mode was actually applied. If cursor mode is requested but no ordering can be determined, the system auto-falls back to offset mode.

To get total row count, set include_count=true (off by default for performance).""",
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
                    },
                    "order_by": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": {"type": "string"},
                                "direction": {"type": "string", "enum": ["ASC", "DESC"]}
                            },
                            "required": ["column"]
                        },
                        "description": "Explicit ordering columns for cursor mode. If omitted, ORDER BY is auto-detected from the SQL."
                    },
                    "include_count": {
                        "type": "boolean",
                        "description": "If true, includes total row_count in response (default: false). Expensive on large tables."
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
    logger.info("Query tool called: %s", name)
    logger.debug("Tool arguments: %s", arguments)
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

        elif name == "list_table_columns":
            datasource = arguments["datasource"]
            table = arguments["table"]
            columns = client.list_table_columns(datasource, table)
            result = {
                "columns": columns,
                "count": len(columns)
            }


        elif name == "execute_query":
            datasource = arguments["datasource"]
            sql = arguments["sql"]
            pagination_mode = arguments.get("pagination_mode", "offset")
            page = arguments.get("page", 1)
            per_page = arguments.get("per_page", 50)
            cursor = arguments.get("cursor")
            direction = arguments.get("direction", "forward")
            order_by = arguments.get("order_by")
            include_count = arguments.get("include_count", False)
            result = client.execute_query(
                datasource,
                sql,
                pagination_mode=pagination_mode,
                page=page,
                per_page=per_page,
                cursor=cursor,
                direction=direction,
                order_by=order_by,
                include_count=include_count
            )

        elif name == "get_sample_data":
            table_id = arguments["table_id"]
            rows = arguments.get("rows", 10)
            result = client.get_sample_data(table_id, rows)

        else:
            result = {"error": f"Unknown tool: {name}"}

        logger.debug("Tool %s completed successfully", name)
        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

    except Exception as e:
        logger.exception("Error in Query MCP tool %s", name)
        return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


async def run_server():
    logger.info("Starting TernoDBI Query MCP Server")
    logger.debug("API Base URL: %s", client.base_url)
    print(f"Starting TernoDBI Query MCP Server (API: {client.base_url})", file=sys.stderr)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())
    logger.info("Query MCP Server stopped")


def main():
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
