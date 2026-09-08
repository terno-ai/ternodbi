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


# The API-source MCP tools (data_query et al.) are gated so a production deploy
# can carry the new code while the submitted mcp.terno.ai tool manifest stays
# exactly as reviewed. Enable with the Django setting or env var
# TERNO_ENABLE_API_MCP_TOOLS once the directory listings are approved.
_API_TOOL_NAMES = frozenset({
    "get_today", "list_accounts", "list_fields", "data_query", "get_query_results",
})

# list_datasources' reviewed (stable) description. The richer, api-aware wording
# below is shown only when the API tools are enabled, so the frozen manifest is
# byte-for-byte unchanged while the tools are off.
_LIST_DATASOURCES_DESC_STABLE = "List all configured database connections"


def _api_mcp_tools_enabled() -> bool:
    """Whether the API-source MCP tools are exposed on this server. Off by default."""
    from django.conf import settings
    val = getattr(settings, "TERNO_ENABLE_API_MCP_TOOLS", None)
    if val is None:
        val = os.environ.get("TERNO_ENABLE_API_MCP_TOOLS", "")
    return str(val).strip().lower() in ("1", "true", "yes", "on")


def own_tools() -> List[Tool]:
    """This server's own tools, without the shared surface.

    Separate from `list_tools` so the merged hosted server can compose both
    registries rather than carrying a third copy of these definitions. The
    API-source tools are advertised only when TERNO_ENABLE_API_MCP_TOOLS is on.
    """
    tools = _all_own_tools()
    if not _api_mcp_tools_enabled():
        tools = [t for t in tools if t.name not in _API_TOOL_NAMES]
    return tools


def _all_own_tools() -> List[Tool]:
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
            description=(
                (
                    "List the data sources this organisation has connected, and "
                    "those it could connect but has not. Start here.\n"
                    "`datasources` holds connected sources; `available` holds the "
                    "rest, each with a `connect_url` to show the user as a "
                    "clickable link. Never ask for a password or connection string "
                    "in the conversation — that link exists so the credential never "
                    "passes through it.\n"
                    "Each entry carries `family`: use execute_query for "
                    "'database', data_query for 'api'."
                )
                if _api_mcp_tools_enabled()
                else _LIST_DATASOURCES_DESC_STABLE
            ),
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
        Tool(
            name="get_today",
            description=(
                "Current UTC date and time. Call before resolving a relative "
                "range like 'last month' into the start/end dates data_query "
                "needs. Pass a datasource's timezone to also get its local date."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "timezone": {
                        "type": "string",
                        "description": "Optional IANA timezone, e.g. 'America/New_York'."
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="list_accounts",
            description=(
                "List the accounts (properties, ad accounts, channels) you may "
                "query on a connected API datasource. Only accounts you are "
                "permitted to see are returned."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource": {"type": "string", "description": "Datasource name or ID"},
                },
                "required": ["datasource"]
            }
        ),
        Tool(
            name="list_fields",
            description=(
                "List the dimensions and metrics available on an API datasource. "
                "A source may return hundreds — pass `filter` to narrow. Metrics "
                "flagged is_non_aggregatable must not be summed across rows. The "
                "response also lists valid report_types for data_query."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource": {"type": "string", "description": "Datasource name or ID"},
                    "report_type": {"type": "string", "description": "Optional report type id"},
                    "filter": {"type": "string", "description":
                               "Optional, comma-separated. Matches field id, name, "
                               "or group, e.g. 'session,user'."},
                    "kind": {"type": "string", "enum": ["metric", "dimension"],
                             "description": "Optional: return only metrics or only "
                                            "dimensions."},
                },
                "required": ["datasource"]
            }
        ),
        Tool(
            name="data_query",
            description=(
                "Query an API datasource (GA4, Meta, Google Ads…). Returns a "
                "query_id; poll get_query_results with it. Resolve relative "
                "dates with get_today first. List dimensions before metrics."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource": {"type": "string", "description": "Datasource name or ID"},
                    "accounts": {"type": "array", "items": {"type": "string"},
                                 "description": "Account ids from list_accounts"},
                    "fields": {"type": "array", "items": {"type": "string"},
                               "description": "Field ids from list_fields, dimensions first"},
                    "report_type": {"type": "string"},
                    "settings": {"type": "object", "description": "Report-type settings, e.g. {\"video_id\": \"...\"}"},
                    "date_range": {
                        "type": "object",
                        "properties": {
                            "start": {"type": "string", "description": "YYYY-MM-DD"},
                            "end": {"type": "string", "description": "YYYY-MM-DD"},
                            "inclusive_of_today": {"type": "boolean"},
                        },
                        "required": ["start", "end"],
                    },
                    "filters": {"type": "string", "description": "e.g. 'country == US AND clicks > 100'"},
                    "compare": {
                        "type": "object",
                        "description": "Period comparison: {type, show, start?, end?}",
                    },
                    "timezone": {"type": "string"},
                    "max_rows": {"type": "integer"},
                },
                "required": ["datasource", "accounts", "fields", "date_range"]
            }
        ),
        Tool(
            name="get_query_results",
            description=(
                "Retrieve a data_query result by its query_id. Poll until "
                "status is 'completed' or 'failed'."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query_id": {"type": "string", "description": "From data_query. Copy verbatim."},
                },
                "required": ["query_id"]
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

        # Defence in depth: a gated API tool is not advertised, but a client
        # could still call it by name. When off, treat it as non-existent.
        if name in _API_TOOL_NAMES and not _api_mcp_tools_enabled():
            return as_error_result(f"Unknown tool: {name}")

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
            payload = client.list_datasources_full()
            if _api_mcp_tools_enabled():
                # Pass the envelope through rather than rebuilding it: `available`
                # and `notes` are the point of this tool now, and re-deriving
                # `count` here would let the two drift.
                result = {
                    key: value for key, value in payload.items()
                    if key != "status"
                }
                if "count" not in result and isinstance(result.get("datasources"), list):
                    result["count"] = len(result["datasources"])
            else:
                # Frozen 1.0.2 shape: connected database sources only, original
                # fields, no `available`/`notes`/`family`. Keeps the submitted
                # mcp.terno.ai response byte-for-byte identical while API tools
                # are off. (API-family sources are omitted here — there is no
                # data_query tool to query them in this manifest.)
                stable_keys = ("id", "name", "type", "description",
                               "is_erp", "dialect_name", "dialect_version")
                dbs = [
                    {k: entry.get(k) for k in stable_keys}
                    for entry in (payload.get("datasources") or [])
                    if entry.get("family", "database") == "database"
                ]
                result = {"datasources": dbs, "count": len(dbs)}

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

        elif name == "get_today":
            result = client.get_today(arguments.get("timezone"))

        elif name == "list_accounts":
            result = client.list_accounts(arguments["datasource"])

        elif name == "list_fields":
            result = client.list_fields(arguments["datasource"],
                                        report_type=arguments.get("report_type"),
                                        filter=arguments.get("filter"),
                                        kind=arguments.get("kind"))

        elif name == "data_query":
            datasource = arguments["datasource"]
            payload = {k: v for k, v in arguments.items() if k != "datasource"}
            result = client.data_query(datasource, payload)

        elif name == "get_query_results":
            result = client.get_query_results(arguments["query_id"])

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
