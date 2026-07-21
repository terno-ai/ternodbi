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
    "ternodbi-admin",
    instructions=(
        "Write access to manage datasource schema metadata (rename tables/columns, "
        "edit descriptions, add/sync/delete datasources), to write durable, shared "
        "memory (save_memory/edit_memory/delete_memory), and to set this "
        "organisation's custom system-prompt addendum (update_org_prompt/"
        "edit_org_prompt).\n\n"
        "Memory write rules: one fact per memory. Prefer edit_memory over a fresh "
        "save_memory when an existing memory is still mostly right but needs a "
        "correction — it preserves any [[name]] links other memories point at it "
        "with. Both edit_memory and a save_memory that replaces existing content "
        "require expected_hash from a get_memory call made just before — this is "
        "enforced server-side, not optional. store='org' shares a memory with every "
        "agent working on this organisation's data; store='user' (the default) is "
        "private to you. Prefer 'org' for facts that would help any agent querying "
        "this data, not just facts specific to your own preferences or this session.\n\n"
        "The same read-before-write rule applies to the org prompt: read it first "
        "with get_org_prompt (on the paired query server) and pass its content_hash "
        "as expected_hash — required for edit_org_prompt always, and for "
        "update_org_prompt whenever the prompt isn't currently blank.\n\n"
        "org_prompt vs memory — decide by REACH, not importance. org_prompt is "
        "injected into every request for every user, always; a memory is fetched "
        "only when an agent looks for it (this is delivery, not visibility — even an "
        "org-shared memory is pulled on demand, never auto-injected the way "
        "org_prompt is). So put in org_prompt ONLY the few directives that must shape "
        "every query: terminology, default units/formatting, filters that always "
        "apply. Everything else — schema quirks, join paths, domain facts, however "
        "important it feels — goes in memory. When unsure, choose memory: it is "
        "unbounded and costs nothing until read, whereas every line of org_prompt is "
        "paid on every future request. Never keep the same rule in both places: "
        "before writing to org_prompt, grep_memory for it and delete_memory whatever "
        "you are promoting into it; before saving a memory that reads like an "
        "always-apply rule, get_org_prompt to confirm it is not already there. "
        "Duplicated rules drift apart silently — keep exactly one copy of each."
    ),
)


@server.list_tools()
async def list_tools() -> List[Tool]:
    return [
        Tool(
            name="update_org_prompt",
            description=(
                "Create or fully replace this organisation's custom system-prompt "
                "addendum — text appended to the default LLM system prompt for all "
                "users in this organisation. To REPLACE an existing (non-blank) "
                "prompt you must first read it (get_org_prompt) and pass its "
                "`content_hash` as `expected_hash`. Capped at a max length per call — "
                "if rejected as too long, write a shorter initial version here, then "
                "use the uncapped edit_org_prompt to grow it further.\n\n"
                "Write as short imperative bullets (terminology, default units, "
                "must-apply filters) — one directive per line, no prose, no "
                "meta-commentary about why a rule exists. Aim for a few hundred words; "
                "the cap above is a hard backstop, not a target. For revisions, prefer "
                "edit_org_prompt over a fresh update_org_prompt — a full replace risks "
                "silently dropping an existing directive you didn't know mattered. "
                "Because this changes behavior for every user in the organisation, "
                "show the requesting user the exact wording before writing it."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "org_prompt": {
                        "type": "string",
                        "description": "New organisation prompt text (replaces the existing value)"
                    },
                    "expected_hash": {
                        "type": "string",
                        "description": "Required when replacing a non-blank org_prompt: its current content_hash from get_org_prompt"
                    }
                },
                "required": ["org_prompt"]
            }
        ),
        Tool(
            name="edit_org_prompt",
            description=(
                "Edit the organisation prompt by exact string replacement, preserving "
                "the rest of its content. `old_string` must be present and unique "
                "unless replace_all=true. Read it first (get_org_prompt) and pass its "
                "`content_hash` as `expected_hash`."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "old_string": {"type": "string", "description": "Exact text to replace"},
                    "new_string": {"type": "string", "description": "Replacement text"},
                    "expected_hash": {"type": "string", "description": "The org prompt's current content_hash from get_org_prompt (read-before-write)"},
                    "replace_all": {"type": "boolean", "description": "Replace every occurrence (default false)"}
                },
                "required": ["old_string", "new_string", "expected_hash"]
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
        Tool(
            name="save_memory",
            description=(
                "Create or fully replace a persistent memory (a single fact recalled "
                "across sessions). One fact per memory. Set `datasource_id` when the fact "
                "is specific to one database's tables/columns/joins/rules; omit it for a "
                "global fact. `store`: 'user' (private to the caller) or 'org' (shared "
                "org-wide, admin only). To REPLACE an existing memory you must first read "
                "it (get_memory) and pass its `content_hash` as `expected_hash`."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "kebab-case slug, unique within scope, e.g. 'zydus-active-users-join'"},
                    "description": {"type": "string", "description": "One-line hook shown in the memory index"},
                    "content": {"type": "string", "description": "The full fact body (add Why/How-to-apply for feedback/project types)"},
                    "memory_type": {"type": "string", "enum": ["user", "feedback", "project", "reference"], "description": "Kind of fact (default: project)"},
                    "store": {"type": "string", "enum": ["user", "org"], "description": "user = private to caller; org = shared org-wide (default user)"},
                    "datasource_id": {"type": "integer", "description": "Optional: scope this fact to one datasource"},
                    "expected_hash": {"type": "string", "description": "Required when replacing an existing memory: its current content_hash from get_memory"}
                },
                "required": ["name", "description", "content"]
            }
        ),
        Tool(
            name="edit_memory",
            description=(
                "Edit an existing memory by exact string replacement, preserving the rest "
                "of its content. `old_string` must be present and unique unless replace_all=true. "
                "Read the memory first (get_memory) and pass its `content_hash` as `expected_hash`."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "The memory's slug"},
                    "old_string": {"type": "string", "description": "Exact text to replace"},
                    "new_string": {"type": "string", "description": "Replacement text"},
                    "expected_hash": {"type": "string", "description": "The memory's current content_hash from get_memory (read-before-write)"},
                    "replace_all": {"type": "boolean", "description": "Replace every occurrence (default false)"},
                    "store": {"type": "string", "enum": ["user", "org"], "description": "Which store the memory is in (default user)"},
                    "datasource_id": {"type": "integer", "description": "Scope of the memory to edit"}
                },
                "required": ["name", "old_string", "new_string", "expected_hash"]
            }
        ),
        Tool(
            name="delete_memory",
            description="Delete a persistent memory by name within its scope.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "The memory's slug"},
                    "store": {"type": "string", "enum": ["user", "org"], "description": "Which store the memory is in (default user)"},
                    "datasource_id": {"type": "integer", "description": "Scope of the memory to delete"}
                },
                "required": ["name"]
            }
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    logger.info("Admin tool called: %s", name)
    logger.debug("Tool arguments: %s", arguments)
    try:
        result = None

        if name == "update_org_prompt":
            result = client.update_org_prompt(
                arguments["org_prompt"],
                expected_hash=arguments.get("expected_hash"),
            )

        elif name == "edit_org_prompt":
            result = client.edit_org_prompt(
                old_string=arguments["old_string"],
                new_string=arguments["new_string"],
                expected_hash=arguments["expected_hash"],
                replace_all=arguments.get("replace_all", False),
            )

        elif name == "rename_table":
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

        elif name == "save_memory":
            result = client.save_memory(
                name=arguments["name"],
                description=arguments["description"],
                content=arguments["content"],
                memory_type=arguments.get("memory_type", "project"),
                store=arguments.get("store", "user"),
                datasource_id=arguments.get("datasource_id"),
                expected_hash=arguments.get("expected_hash"),
            )

        elif name == "edit_memory":
            result = client.edit_memory(
                name=arguments["name"],
                old_string=arguments["old_string"],
                new_string=arguments["new_string"],
                expected_hash=arguments["expected_hash"],
                store=arguments.get("store", "user"),
                replace_all=arguments.get("replace_all", False),
                datasource_id=arguments.get("datasource_id"),
            )

        elif name == "delete_memory":
            result = client.delete_memory(
                name=arguments["name"],
                store=arguments.get("store", "user"),
                datasource_id=arguments.get("datasource_id"),
            )

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
