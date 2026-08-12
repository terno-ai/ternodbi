"""Display titles, behaviour annotations, and output schemas for every tool.

Kept as one table rather than inline on each `Tool(...)` for three reasons: the
annotations are a directory-submission gate and reviewing them means reading
them side by side; the same table serves both stdio servers today and the merged
hosted server later; and `apply_tool_meta` can then *fail* on a tool with no
entry, so a new tool cannot quietly ship without a title or a `readOnlyHint`.

## On the output schemas being permissive

The MCP SDK validates `structuredContent` against `outputSchema` and turns a
mismatch into a tool error. A tight schema would therefore convert every
backend response shape change — and every error path, which returns
`{"error": ...}` — into a failed call.

So each schema below documents the keys a caller can expect, sets no `required`,
and leaves `additionalProperties` open. It describes what exists rather than
constraining it, which is what Phase 0.3 asked for. Tightening a schema is a
deliberate later decision with a test behind it, not a default.
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
    # Verified against a live response, not assumed: rows come back as objects
    # keyed by column name — [{"status": "paid", "n": 3}] — not as positional
    # arrays. An earlier version of this schema said `items: {type: array}`,
    # which made the SDK reject every successful `execute_query` result with an
    # output-validation error.
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
        # readOnlyHint is verified, not assumed: SQLShield parses every
        # statement, requires a single SELECT, and rejects DML/DDL anywhere in
        # the tree before execution. See docs/mcp-token-type-shield-asgi.md §2.
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
        # Non-destructive on the stated assumption that a sync does not clobber
        # human-written descriptions. Flagged in IMPLEMENTATION-PLAN.md §0.2 as
        # a judgment call — if it can overwrite them, this becomes true.
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
                    # Both spellings on purpose: `title` is where the current
                    # spec puts the display name, `annotations.title` is where
                    # older clients still read it from.
                    "title": meta["title"],
                    "annotations": ToolAnnotations(title=meta["title"], **meta["hints"]),
                    "outputSchema": meta["output"],
                }
            )
        )
    return decorated


def as_error_result(message: str, **extra: Any) -> "CallToolResult":
    """Return a failed tool call, with `isError` actually set.

    Handlers used to catch every exception and return `{"error": ...}` as a
    normal result. MCP clients read `isError` to decide whether a call failed, so
    a connection refusal looked like a successful call whose payload happened to
    mention an error — the model would go on to reason about the "result".

    Returning a `CallToolResult` also short-circuits the SDK's output
    validation, which is what you want: an error payload should not have to
    satisfy the tool's success schema. (It is also what would let those schemas
    be tightened later — they are currently permissive partly to accommodate
    error dicts flowing through the success path.)
    """
    payload: Dict[str, Any] = {"error": message, **extra}
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(payload, indent=2, default=str))],
        structuredContent=payload,
        isError=True,
    )


def as_tool_result(result: Any) -> Tuple[List[TextContent], Dict[str, Any]]:
    """Return a handler result as the (content, structuredContent) pair.

    Once a tool declares an `outputSchema`, the SDK rejects a call that returns
    no structured content — so returning text alone is no longer an option for
    any tool in `TOOL_META`.

    `structuredContent` must be a JSON object. Handlers that pass a backend
    response straight through can yield a list or a scalar, so anything that is
    not a dict is wrapped under `result` rather than allowed to fail validation
    at the transport layer.
    """
    structured = result if isinstance(result, dict) else {"result": result}
    text = json.dumps(structured, indent=2, default=str)
    return [TextContent(type="text", text=text)], structured


async def dispatch_in_worker(dispatch, name: str, arguments: Dict[str, Any]):
    """Run a synchronous tool dispatch off the event loop.

    Tool handlers are `async def`, but everything they call is synchronous: the
    client, and — under the in-process transport — the Django views and the ORM
    behind them. Django refuses sync ORM access from an async context outright
    (`SynchronousOnlyOperation`), so without this **every tool call fails** when
    the server is mounted inside Django.

    Two reasons this is a thread rather than an `DJANGO_ALLOW_ASYNC_UNSAFE`
    escape hatch:

    1. That flag suppresses the error without fixing what it warns about — the
       ORM would still block the event loop.
    2. A SQL connector runs slow queries by definition. Blocking the loop for
       the duration would stall every other session in the process, including
       the keep-alives that stop clients timing out.

    `thread_sensitive=False` so calls run concurrently in a pool rather than
    serialising through one shared thread — which would make a single slow query
    block every other tool call in the process. The trade-off is that each pool
    thread holds its own database connection, so expired ones are released after
    every call.
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
            except Exception:  # pragma: no cover - Django not configured (stdio)
                pass

    return await sync_to_async(_run, thread_sensitive=False)()
