import os
import sys
import json
import asyncio
import logging
from typing import Any, Dict, List
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool
from terno_dbi.mcp.context import client, describe_backend
from terno_dbi.mcp.instructions import QUERY_INSTRUCTIONS
from terno_dbi.mcp.surface import GUIDE_TOOL, handle_guide, register_surface
from terno_dbi.mcp.tool_meta import (
    apply_tool_meta,
    as_error_result,
    as_tool_result,
    dispatch_in_worker,
)

logger = logging.getLogger(__name__)

# `client` is a request-scoped proxy, not a client instance. Under stdio it
# uses environment credentials; under HTTP it resolves credentials per request,
# allowing one process to safely serve multiple organisations.

server = Server(
    "ternodbi-query",
    instructions=QUERY_INSTRUCTIONS,
)

# Advertises resources/ and prompts/ in `initialize`; must run at import.
register_surface(server)


def own_tools() -> List[Tool]:
    """This server's own tools, without the shared surface.

    Separate from `list_tools` so the merged hosted server can compose both
    registries rather than carrying a third copy of these definitions.
    """
    return [
        Tool(
            name="get_org_prompt",
            description=(
                "Get this organisation's custom system-prompt addendum — text "
                "appended to the default LLM system prompt for all users in this "
                "organisation. The response includes `content_hash` — pass it back "
                "as `expected_hash` when you later edit or replace this prompt. "
                "Paginated like a file read: returns up to `limit` lines (default 2000) "
                "starting at 1-indexed `offset`; when `has_more` is true, page through "
                "with `next_offset`. `content_hash` always covers the full prompt "
                "regardless of which page you read."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "offset": {
                        "type": "integer",
                        "description": "1-indexed line number to start reading from (default 1)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of lines to return (default 2000)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="grep_org_prompt",
            description=(
                "Regex-search the organisation prompt's own text and return matching "
                "lines (1-indexed). Use to find a specific passage before editing it "
                "with edit_org_prompt."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "pattern": {
                        "type": "string",
                        "description": "Regular expression matched (case-insensitive) against the org prompt's lines"
                    }
                },
                "required": ["pattern"]
            }
        ),
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
        # `find_similar_examples` was dropped here (team decision, 2026-08-08):
        # stale, and its PromptExample + Milvus backing is scheduled for removal.
        # See docs/BACKLOG.md D4.

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


@server.list_tools()
async def list_tools() -> List[Tool]:
    return apply_tool_meta([GUIDE_TOOL, *own_tools()])


@server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]):
    """Async entrypoint; the work itself runs in a worker thread.

    Everything below `_dispatch` is synchronous — the client, and under the
    in-process transport the Django ORM, which refuses to be touched from an
    async context. See `dispatch_in_worker`.
    """
    return await dispatch_in_worker(_dispatch, name, arguments)


def _dispatch(name: str, arguments: Dict[str, Any]):
    logger.info("Query tool called: %s", name)
    logger.debug("Tool arguments: %s", arguments)
    try:
        result = None

        if name == "terno_guide":
            result = handle_guide(arguments)

        elif name == "get_org_prompt":
            result = client.get_org_prompt(
                offset=arguments.get("offset"),
                limit=arguments.get("limit"),
            )

        elif name == "grep_org_prompt":
            result = client.grep_org_prompt(arguments["pattern"])

        elif name == "list_datasources":
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
            return as_error_result(f"Unknown tool: {name}")

        logger.debug("Tool %s completed successfully", name)
        return as_tool_result(result)

    except Exception as e:
        logger.exception("Error in Query MCP tool %s", name)
        return as_error_result(str(e))


async def run_server():
    logger.info("Starting Terno Query MCP Server")
    logger.debug("API Base URL: %s", describe_backend())
    print(f"Starting Terno Query MCP Server (API: {describe_backend()})", file=sys.stderr)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())
    logger.info("Query MCP Server stopped")


def main():
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
