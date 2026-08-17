"""Metadata and output schemas for all MCP tools.

Kept in one table so tool titles, annotations, and schemas can be reviewed
together and applied consistently across stdio and hosted servers.
`apply_tool_meta` also fails if a tool has no entry, preventing new tools from
shipping without required metadata such as a title or `readOnlyHint`.

Output schemas are intentionally permissive. The MCP SDK validates
`structuredContent` against the schema, while backend responses and error
responses may vary in shape. The schemas therefore document expected keys
without requiring them or rejecting additional fields.
"""

import json
from typing import Any, Dict, List, Tuple
from mcp.types import CallToolResult, TextContent, Tool, ToolAnnotations

_ERROR = {
    "error": {
        "type": "string",
        "description": "Present instead of the payload when the call failed.",
    }
}


def _out(description: str, **props: Dict[str, Any]) -> Dict[str, Any]:
    """An open object schema: documents keys, requires none, allows extras."""
    return {
        "type": "object",
        "description": description,
        "properties": {**props, **_ERROR},
        "additionalProperties": True,
    }


_COUNT = {"type": "integer", "description": "Number of items returned."}
_ROWS = {
    "type": "array",
    "description": "Result rows, each an object keyed by column name.",
    "items": {"type": "object", "additionalProperties": True},
}
_INDEX_ROWS = {
    "type": "array",
    "description": "Memory index rows — name, description, type, and scope. Bodies are not included.",
    "items": {"type": "object", "additionalProperties": True},
}
_WRITE_RESULT = _out(
    "Confirmation of the write, including the updated record where the API returns one.",
    success={"type": "boolean", "description": "Whether the write was applied."},
)
_HASHED_CONTENT = {
    "content_hash": {
        "type": "string",
        "description": "Hash of the full text, to pass back as `expected_hash` on a later write.",
    }
}


# name -> (title, ToolAnnotations kwargs minus title, outputSchema)
#
# `openWorldHint` is true where the call reaches a customer database, false
# where it touches only TernoDBI's own metadata store. That distinction is the
# useful one for a client deciding how much to trust a result.
TOOL_META: Dict[str, Dict[str, Any]] = {
    # --------------------------------------------------------------- shared
    "terno_guide": {
        "title": "About this connector",
        "hints": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False),
        "output": _out(
            "Orientation text for the connector.",
            mode={"type": "string", "description": "Which guide was returned: 'tour' or 'whats_new'."},
            content={"type": "string", "description": "Markdown guide text."},
        ),
    },
    "connect_datasource": {
        "title": "Connect a database",
        "hints": dict(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False),
        "output": _out(
            "Where the user connects a database. Never contains credentials.",
            credential_required={"type": "boolean"},
            reason={"type": "string", "description": "Why this cannot be done in the conversation."},
            instruction={"type": "string", "description": "What to do with the link."},
            setup_url={"type": "string", "description": "Link for the user to open. Absent if the workspace could not be resolved."},
            setup_location={"type": "string", "description": "Prose fallback when no URL could be built."},
        ),
    },
    # ---------------------------------------------------------------- query
    "list_datasources": {
        "title": "List databases",
        "hints": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False),
        "output": _out(
            "The database connections this token may reach.",
            datasources={
                "type": "array",
                "description": "Configured datasources, each with at least an id and a display name.",
                "items": {"type": "object", "additionalProperties": True},
            },
            count=_COUNT,
        ),
    },
    "list_tables": {
        "title": "List tables",
        "hints": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True),
        "output": _out(
            "Tables visible in the datasource, with their public names.",
            tables={
                "type": "array",
                "description": "Visible tables. Write SQL against `public_name`, not the physical name.",
                "items": {"type": "object", "additionalProperties": True},
            },
            count=_COUNT,
        ),
    },
    "list_table_columns": {
        "title": "List columns in a table",
        "hints": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True),
        "output": _out(
            "Columns visible in the table, with public names and types.",
            columns={
                "type": "array",
                "description": "Visible columns, each with a public name, type, and any description.",
                "items": {"type": "object", "additionalProperties": True},
            },
            count=_COUNT,
        ),
    },
    "execute_query": {
        "title": "Run a SQL query",
        "hints": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=False, openWorldHint=True),
        "output": _out(
            "Query results.",
            columns={
                "type": "array",
                "description": "Column names, in the order values appear in each row.",
                "items": {"type": "string"},
            },
            data=_ROWS,
            row_count=_COUNT,
        ),
    },
    "get_sample_data": {
        "title": "Preview sample rows",
        "hints": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True),
        "output": _out(
            "A small sample of rows from the table.",
            columns={"type": "array", "items": {"type": "string"}},
            data=_ROWS,
        ),
    },
    "list_memories": {
        "title": "List recorded facts",
        "hints": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False),
        "output": _out(
            "The memory index. Descriptions are hooks — call get_memory for a fact itself.",
            memories=_INDEX_ROWS,
            count=_COUNT,
        ),
    },
    "get_memory": {
        "title": "Read a recorded fact",
        "hints": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False),
        "output": _out(
            "One memory in full, with the hash needed to write to it.",
            memory={
                "type": "object",
                "description": "The memory: name, description, content, type, scope, and content_hash.",
                "properties": _HASHED_CONTENT,
                "additionalProperties": True,
            },
        ),
    },
    "grep_memory": {
        "title": "Search recorded facts",
        "hints": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False),
        "output": _out(
            "Index rows whose bodies matched the pattern. Bodies are not returned.",
            matches=_INDEX_ROWS,
            count=_COUNT,
        ),
    },
    "get_org_prompt": {
        "title": "Read the organisation prompt",
        "hints": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False),
        "output": _out(
            "A page of the organisation prompt. `content_hash` always covers the full text.",
            org_prompt={"type": "string", "description": "The requested lines of the prompt."},
            has_more={"type": "boolean", "description": "Whether more lines follow."},
            next_offset={"type": "integer", "description": "Offset to pass to read the next page."},
            **_HASHED_CONTENT,
        ),
    },
    "grep_org_prompt": {
        "title": "Search the organisation prompt",
        "hints": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False),
        "output": _out(
            "Matching lines from the organisation prompt, with 1-indexed line numbers.",
            matches={
                "type": "array",
                "description": "Matching lines, each with its line number and text.",
                "items": {"type": "object", "additionalProperties": True},
            },
            count=_COUNT,
        ),
    },
    # ---------------------------------------------------------------- admin
    "get_table_info": {
        "title": "Inspect table metadata",
        "hints": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True),
        "output": _out(
            "Columns, statistics, and sample data for one table — enough to write descriptions from.",
            columns={"type": "array", "items": {"type": "object", "additionalProperties": True}},
            sample_data=_ROWS,
        ),
    },
    "validate_connection": {
        "title": "Test a database connection",
        "hints": dict(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True),
        "output": _out(
            "Whether the connection succeeded.",
            success={"type": "boolean", "description": "Whether the database accepted the connection."},
            message={"type": "string", "description": "Detail on the outcome, including the failure reason."},
        ),
    },
    "add_datasource": {
        "title": "Add a database",
        "hints": dict(readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=True),
        "output": _out(
            "The datasource that was created.",
            success={"type": "boolean"},
            datasource={"type": "object", "additionalProperties": True},
        ),
    },
    "delete_datasource": {
        "title": "Delete a database connection",
        "hints": dict(readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=True),
        "output": _WRITE_RESULT,
    },
    "sync_metadata": {
        "title": "Sync schema metadata",
        "hints": dict(readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=True),
        "output": _out(
            "What the sync discovered and changed.",
            success={"type": "boolean"},
            tables_added=_COUNT,
            columns_added=_COUNT,
        ),
    },
    "rename_table": {
        "title": "Rename a table",
        "hints": dict(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False),
        "output": _WRITE_RESULT,
    },
    "rename_column": {
        "title": "Rename a column",
        "hints": dict(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False),
        "output": _WRITE_RESULT,
    },
    "update_table_description": {
        "title": "Describe a table",
        "hints": dict(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False),
        "output": _WRITE_RESULT,
    },
    "update_column_description": {
        "title": "Describe a column",
        "hints": dict(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False),
        "output": _WRITE_RESULT,
    },
    "save_memory": {
        "title": "Save a fact",
        "hints": dict(readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False),
        "output": _out(
            "Confirmation, with the hash of the stored memory.",
            success={"type": "boolean"},
            **_HASHED_CONTENT,
        ),
    },
    "edit_memory": {
        "title": "Edit a saved fact",
        "hints": dict(readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False),
        "output": _out(
            "Confirmation, with the hash of the edited memory.",
            success={"type": "boolean"},
            **_HASHED_CONTENT,
        ),
    },
    "delete_memory": {
        "title": "Delete a saved fact",
        "hints": dict(readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=False),
        "output": _WRITE_RESULT,
    },
    "update_org_prompt": {
        "title": "Replace the organisation prompt",
        "hints": dict(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False),
        "output": _out(
            "Confirmation, with the hash of the stored prompt.",
            success={"type": "boolean"},
            **_HASHED_CONTENT,
        ),
    },
    "edit_org_prompt": {
        "title": "Edit the organisation prompt",
        "hints": dict(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False),
        "output": _out(
            "Confirmation, with the hash of the edited prompt.",
            success={"type": "boolean"},
            **_HASHED_CONTENT,
        ),
    },
}


class MissingToolMetadata(RuntimeError):
    """A tool was listed without a title, annotations, and an output schema."""


def apply_tool_meta(tools: List[Tool]) -> List[Tool]:
    """Attach title, annotations, and outputSchema to each tool.

    Raises rather than skipping: a tool missing its metadata cannot be listed in
    the connector directory, and a silent omission is exactly the failure this
    table exists to prevent.
    """
    missing = [t.name for t in tools if t.name not in TOOL_META]
    if missing:
        raise MissingToolMetadata(
            f"No entry in TOOL_META for: {', '.join(sorted(missing))}. "
            f"Add a title, annotation hints, and an output schema in "
            f"terno_dbi/mcp/tool_meta.py before shipping these tools."
        )

    decorated = []
    for tool in tools:
        meta = TOOL_META[tool.name]
        decorated.append(
            tool.model_copy(
                update={
                    "title": meta["title"],
                    "annotations": ToolAnnotations(title=meta["title"], **meta["hints"]),
                    "outputSchema": meta["output"],
                }
            )
        )
    return decorated


def as_error_result(message: str, **extra: Any) -> "CallToolResult":
    """Return a failed tool call with `isError` set.

    Using `CallToolResult` lets MCP clients correctly identify the call as failed
    instead of treating an `{"error": ...}` payload as a successful result.

    Error responses also bypass success output-schema validation, so a tool's
    success schema does not need to describe error payloads.
    """
    payload: Dict[str, Any] = {"error": message, **extra}
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(payload, indent=2, default=str))],
        structuredContent=payload,
        isError=True,
    )


def as_tool_result(result: Any) -> Tuple[List[TextContent], Dict[str, Any]]:
    """Return a handler result as `(content, structuredContent)`.

    Tools with an `outputSchema` must return structured content. Since MCP requires
    `structuredContent` to be a JSON object, non-dict backend responses are wrapped
    under `result` before being returned.
    """
    structured = result if isinstance(result, dict) else {"result": result}
    text = json.dumps(structured, indent=2, default=str)
    return [TextContent(type="text", text=text)], structured


async def dispatch_in_worker(dispatch, name: str, arguments: Dict[str, Any]):
    """Run synchronous tool dispatch outside the event loop.

    Tool handlers are async, but the client, Django views, and ORM are synchronous.
    Running them directly in the event loop causes `SynchronousOnlyOperation` and
    would block other MCP sessions while queries are running.

    The dispatch runs in a thread pool with `thread_sensitive=False` so tool calls
    can run concurrently. This avoids the need for `DJANGO_ALLOW_ASYNC_UNSAFE` and
    prevents slow queries from blocking the event loop.
    """
    from asgiref.sync import sync_to_async

    def _run():
        try:
            return dispatch(name, arguments)
        finally:
            # Each worker thread keeps its own connection. Without this they
            # accumulate and outlive their server-side timeout, which surfaces
            # later as "MySQL server has gone away" on a reused connection.
            try:
                from django.db import close_old_connections

                close_old_connections()
            except Exception:
                pass

    return await sync_to_async(_run, thread_sensitive=False)()
