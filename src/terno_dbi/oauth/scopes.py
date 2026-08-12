"""The OAuth scopes, and which tool each one gates.

## These are the scopes that already exist

`ServiceToken.scopes` and `@require_scope` were built before any of this and are
enforced across 11 views today. So the OAuth grant declares **those** names —
`query:read`, `query:execute`, `admin:read`, `admin:write`, `admin:sync` — and
maps straight onto `ServiceToken.scopes`. Inventing a parallel `ternodbi:*`
vocabulary would mean two systems to keep in agreement.

## Where the mapping comes from

`TOOL_SCOPES` is transcribed from the `@require_scope` decorators on the views
each tool actually calls, not from what the tool sounds like it should need. Two
consequences worth knowing:

- **`validate_connection` requires `admin:write`**, even though it only tests a
  connection and is annotated `readOnlyHint: true`. Testing a connection changes
  nothing in Terno, so the annotation is right — but the endpoint behind it is
  guarded by `admin:write`, so a read-only grant cannot reach it. Filtering on
  the annotation instead of on this table would list it and then 403.
- **The query service has no `@require_scope` at all.** `query:read` and
  `query:execute` are declared here and enforced *at the MCP layer*, not by the
  Django view.

## Why MCP-layer enforcement is sufficient for these tokens

An OAuth bearer can only ever reach `/mcp`: `app.terno.ai/api/` is mTLS-gated
and Anthropic cannot present a client certificate, and the `mcp.terno.ai` vhost
returns 404 for everything except `/mcp` and `/.well-known/`. So the MCP
boundary is the only reachable path, which makes it the right place to enforce
and avoids changing `require_scope` behaviour for existing REST integrations.

If that ever stops being true — if an OAuth token becomes usable against the
REST API directly — the query views need real `@require_scope` decorators before
that happens.
"""

from typing import Dict, FrozenSet, Iterable, Optional

QUERY_READ = "query:read"
QUERY_EXECUTE = "query:execute"
ADMIN_READ = "admin:read"
ADMIN_WRITE = "admin:write"
ADMIN_SYNC = "admin:sync"

# Shown on the consent screen, so write for a person deciding whether to allow
# it — not for a developer who already knows the tool names.
SCOPE_DESCRIPTIONS: Dict[str, str] = {
    QUERY_READ: "See your databases, their tables and columns, and the notes recorded about them",
    QUERY_EXECUTE: "Run read-only SQL queries against your databases",
    ADMIN_READ: "Inspect table statistics and sample data used to write descriptions",
    ADMIN_WRITE: "Change table and column descriptions, saved notes, and the organisation prompt",
    ADMIN_SYNC: "Refresh schema metadata from your databases",
}

ALL_SCOPES: FrozenSet[str] = frozenset(SCOPE_DESCRIPTIONS)

# Requested when a client asks for nothing specific. Read-only by default was
# decision 5: write is a deliberate second step, not something a user grants by
# clicking through the screen that connects them.
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
    "validate_connection": ADMIN_WRITE,      # see module docstring
    "add_datasource": ADMIN_WRITE,
    "delete_datasource": ADMIN_WRITE,
    "rename_table": ADMIN_WRITE,
    "rename_column": ADMIN_WRITE,
    "update_table_description": ADMIN_WRITE,
    "update_column_description": ADMIN_WRITE,
    "update_org_prompt": ADMIN_WRITE,
    "edit_org_prompt": ADMIN_WRITE,
    "sync_metadata": ADMIN_SYNC,
    # Memory writes live on the query service and carry no view-level scope, but
    # they mutate shared organisation state, so they sit behind admin:write
    # rather than being reachable with a read-only grant.
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
        # Either an always-available tool, or one with no mapping — treat an
        # unmapped tool as unavailable rather than open, so a new tool cannot
        # reach a read-only grant by being forgotten here.
        return tool_name in TOOL_SCOPES
    return required in scopes
