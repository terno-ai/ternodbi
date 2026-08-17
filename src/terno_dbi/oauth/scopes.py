"""OAuth scopes and the tools they grant access to.

OAuth uses the existing `ServiceToken.scopes` values and `@require_scope`
checks: `query:read`, `query:execute`, `admin:read`, `admin:write`, and
`admin:sync`. This avoids introducing a second scope system.

`TOOL_SCOPES` is based on the scopes required by the views each tool calls.
`validate_connection` is a notable exception: it is read-only by annotation
but requires `admin:write` at the endpoint.

The query service does not use `@require_scope`, so `query:read` and
`query:execute` are enforced at the MCP layer.

OAuth tokens are currently restricted to `/mcp`, making MCP-level enforcement
sufficient. If OAuth tokens are ever allowed to access the REST API, the query
views must enforce their scopes there as well.
"""

from typing import Dict, FrozenSet, Iterable, Optional

QUERY_READ = "query:read"
QUERY_EXECUTE = "query:execute"
ADMIN_READ = "admin:read"
ADMIN_WRITE = "admin:write"
ADMIN_SYNC = "admin:sync"

SCOPE_DESCRIPTIONS: Dict[str, str] = {
    QUERY_READ: "View your datasource connections, tables and columns, saved memories, and organisation context",
    QUERY_EXECUTE: "Run read-only SQL queries against your databases",
    ADMIN_READ: "Inspect table statistics and sample rows, used to write descriptions",
    ADMIN_WRITE: "Add, edit, and delete datasource connections, table and column metadata, saved memories, and the organisation prompt",
    ADMIN_SYNC: "Refresh schema metadata from your databases",
}

ALL_SCOPES: FrozenSet[str] = frozenset(SCOPE_DESCRIPTIONS)

DEFAULT_SCOPES: FrozenSet[str] = frozenset({QUERY_READ, QUERY_EXECUTE})

WRITE_SCOPES: FrozenSet[str] = frozenset({ADMIN_WRITE, ADMIN_SYNC})

# tool name -> the scope required to use it. None means always available.
TOOL_SCOPES: Dict[str, Optional[str]] = {
    "terno_guide": None,
    # --- query service (no @require_scope on the views; gated here) ---
    "list_datasources": QUERY_READ,
    "list_tables": QUERY_READ,
    "list_table_columns": QUERY_READ,
    "get_sample_data": QUERY_READ,
    "list_memories": QUERY_READ,
    "get_memory": QUERY_READ,
    "grep_memory": QUERY_READ,
    "get_org_prompt": QUERY_READ,
    "grep_org_prompt": QUERY_READ,
    "execute_query": QUERY_EXECUTE,
    # --- admin service (transcribed from @require_scope) ---
    "get_table_info": ADMIN_READ,
    "validate_connection": ADMIN_WRITE,
    "add_datasource": ADMIN_WRITE,
    "delete_datasource": ADMIN_WRITE,
    "rename_table": ADMIN_WRITE,
    "rename_column": ADMIN_WRITE,
    "update_table_description": ADMIN_WRITE,
    "update_column_description": ADMIN_WRITE,
    "update_org_prompt": ADMIN_WRITE,
    "edit_org_prompt": ADMIN_WRITE,
    "sync_metadata": ADMIN_SYNC,
    "save_memory": ADMIN_WRITE,
    "edit_memory": ADMIN_WRITE,
    "delete_memory": ADMIN_WRITE,
}


class UnknownScope(ValueError):
    """A requested scope is not one this server issues."""


def parse_scope_string(raw: Optional[str]) -> FrozenSet[str]:
    """Parse a space-delimited OAuth `scope` parameter.

    Unknown scopes raise rather than being dropped. Silently narrowing a grant
    produces a token that looks accepted and then cannot do what the client
    asked for, which surfaces later as an unexplained empty tool list.
    """
    if not raw or not raw.strip():
        return DEFAULT_SCOPES

    requested = frozenset(raw.split())
    unknown = requested - ALL_SCOPES
    if unknown:
        raise UnknownScope(
            f"Unknown scope(s): {' '.join(sorted(unknown))}. "
            f"Supported: {' '.join(sorted(ALL_SCOPES))}"
        )
    return requested


def scope_string(scopes: Iterable[str]) -> str:
    return " ".join(sorted(scopes))


def granted_scopes(scopes: Iterable[str], *, can_write: bool) -> FrozenSet[str]:
    """Narrow a requested scope set by what the user is actually allowed.

    The OAuth scope says what the *client* asked for; the group says what the
    *user* may do. A user who is not an org admin does not get write scopes even
    if the client requested them and the user clicked allow — otherwise consent
    alone would be enough to escalate.
    """
    granted = frozenset(scopes) & ALL_SCOPES
    if not can_write:
        granted -= WRITE_SCOPES
    return granted


def tool_is_allowed(tool_name: str, scopes: FrozenSet[str]) -> bool:
    required = TOOL_SCOPES.get(tool_name)
    if required is None:
        return tool_name in TOOL_SCOPES
    return required in scopes
