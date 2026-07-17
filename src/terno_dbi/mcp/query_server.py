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

server = Server(
    "ternodbi-query",
    instructions=(
        "Read-only access to configured SQL databases and their durable metadata. "
        "Typical flow: list_datasources, then list_tables/list_table_columns to see "
        "schema, then execute_query (use public names) or get_sample_data to preview "
        "rows.\n\n"
        "This server also holds durable, shared memory (list_memories/get_memory/"
        "grep_memory) — facts about this data recorded by any agent that worked with "
        "it before, not just you. Check it before answering questions about schema, "
        "joins, or business rules you don't already know; a memory's one-line "
        "description in the index is a hook, not the fact itself, so read the full "
        "entry via get_memory before relying on it. If you maintain your own separate "
        "memory system, do not let facts about this data live only there — other "
        "agents attached to this same server, including ones with no memory of their "
        "own, can only benefit from a fact if it's recorded here."
    ),
)


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

        # Tool(
        #     name="get_datasource_context",
        #     description=(
        #         "Get the complete context package for a datasource in ONE call: "
        #         "its schema (tables/columns with public names, types, descriptions) "
        #         "PLUS a memory index of persistent facts (global + datasource-scoped). "
        #         "The `memory_index` shows one line per fact — call `get_memory(name=...)` "
        #         "for the full content of any entry that looks relevant before relying on it. "
        #         "Call this first when you start working with a datasource."
        #     ),
        #     inputSchema={
        #         "type": "object",
        #         "properties": {
        #             "datasource": {
        #                 "type": "string",
        #                 "description": "Datasource name or ID"
        #             }
        #         },
        #         "required": ["datasource"]
        #     }
        # ),
        Tool(
            name="list_memories",
            description=(
                "List the memory index (name, one-line description, type, scope — "
                "not full content) of persistent facts. Optionally scope to a datasource; "
                "global memories are always included. Use `get_memory` to read a full fact."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_id": {
                        "type": "integer",
                        "description": "Optional: include this datasource's scoped memories alongside global ones"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="get_memory",
            description=(
                "Fetch the full content of one memory by its `name` (the slug shown in "
                "the memory index). The response includes `content_hash` — pass it back as "
                "`expected_hash` when you later edit or overwrite this memory."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "The memory's name/slug, e.g. 'zydus-active-users-join'"
                    },
                    "datasource_id": {
                        "type": "integer",
                        "description": "Optional: datasource scope to prefer when resolving the name"
                    }
                },
                "required": ["name"]
            }
        ),
        Tool(
            name="grep_memory",
            description=(
                "Regex-search the BODIES of memories and return matching index rows "
                "(name/description/type/scope, no bodies). Use to find a fact when you "
                "don't know its exact name."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Regular expression matched (case-insensitive) against memory content"
                    },
                    "datasource_id": {
                        "type": "integer",
                        "description": "Optional: restrict to global + this datasource's memories"
                    }
                },
                "required": ["pattern"]
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

        # elif name == "get_datasource_context":
        #     datasource = arguments["datasource"]
        #     result = client.get_datasource_context(datasource)

        elif name == "list_memories":
            result = client.list_memories(datasource_id=arguments.get("datasource_id"))

        elif name == "get_memory":
            mem_name = arguments["name"]
            datasource_id = arguments.get("datasource_id")
            result = {"memory": client.get_memory(mem_name, datasource_id=datasource_id)}

        elif name == "grep_memory":
            matches = client.grep_memory(arguments["pattern"],
                                         datasource_id=arguments.get("datasource_id"))
            result = {"matches": matches, "count": len(matches)}

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
