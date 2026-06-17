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
            description="""Execute a SQL query using high-performance server-side streaming.

Returns columns and data rows. Use max_rows to limit the number of rows returned.""",
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
                    "max_rows": {
                        "type": "integer",
                        "description": "Maximum number of rows to return (optional, returns all rows if not set)"
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
        Tool(
            name="find_similar_examples",
            description="Find similar prompt examples (domain knowledge, business rules) based on semantic similarity.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The natural language query or context to find similar examples for"
                    },
                    "datasource_id": {
                        "type": "integer",
                        "description": "Optional: Restrict search to a specific datasource ID"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of examples to return (default: 10)",
                        "default": 10
                    }
                },
                "required": ["query"]
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
            max_rows = arguments.get("max_rows")
            result = client.execute_query(
                datasource,
                sql,
                max_rows=max_rows,
            )

        elif name == "get_sample_data":
            table_id = arguments["table_id"]
            rows = arguments.get("rows", 10)
            result = client.get_sample_data(table_id, rows)

        elif name == "find_similar_examples":
            query_str = arguments["query"]
            limit = arguments.get("limit", 10)
            result = client.find_similar_examples(
                query=query_str,
                limit=limit
            )

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
