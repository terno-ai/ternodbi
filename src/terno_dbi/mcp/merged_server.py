"""The single hosted server: one endpoint carrying both tool registries.

The `query`/`admin` split exists because stdio has no notion of identity — the
only way to separate read from write was two processes holding two tokens. Over
HTTP the grant carries that, so the split becomes two Connect buttons, two OAuth
clients, and two directory listings for one product. Supermetrics runs 14 tools
spanning read and write on one endpoint, including one that spends real money.

This module composes rather than redefines: tool definitions come from
`query_server.own_tools()` and `admin_server.own_tools()`, and dispatch delegates
to the same handlers the stdio servers use. There is no third copy to keep in
sync, and `dbi-mcp query` / `dbi-mcp admin` keep working unchanged for local use.

**Write tools are omitted from `tools/list` when the grant does not allow
writes** — better than listing them and failing the call, and it keeps the
common read-only case cheaper in context.
"""

import asyncio
import logging
import sys
from typing import Any, Dict, List, Optional

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool

from terno_dbi.mcp import admin_server, query_server
from terno_dbi.mcp.context import can_write, current_scopes
from terno_dbi.oauth.scopes import TOOL_SCOPES, tool_is_allowed
from terno_dbi.mcp.instructions import MERGED_INSTRUCTIONS
from terno_dbi.mcp.surface import GUIDE_TOOL, handle_guide, register_surface
from terno_dbi.mcp.tool_meta import (
    TOOL_META,
    apply_tool_meta,
    as_error_result,
    as_tool_result,
)

logger = logging.getLogger(__name__)

SERVER_NAME = "terno"

server = Server(SERVER_NAME, instructions=MERGED_INSTRUCTIONS)
register_surface(server)


def _is_write_tool(name: str) -> bool:
    """Whether a tool mutates state, per its annotations.

    Used for the defence-in-depth check at call time. **Not** used to decide
    what `tools/list` shows — that is `TOOL_SCOPES`, because the two disagree:
    `validate_connection` is annotated `readOnlyHint: true` (it changes nothing
    in Terno) but its endpoint is guarded by `admin:write`. Filtering on the
    annotation would list it for a read-only grant and then 403.
    """
    meta = TOOL_META.get(name)
    return bool(meta) and not meta["hints"]["readOnlyHint"]


def all_tools() -> List[Tool]:
    """Every tool this server can expose, before grant filtering."""
    return [GUIDE_TOOL, *query_server.own_tools(), *admin_server.own_tools()]


def visible_tools(scopes: Optional[frozenset]) -> List[Tool]:
    """The tools a grant may use.

    `scopes=None` means stdio — no grant applies, so everything is shown.
    """
    tools = all_tools()
    if scopes is not None:
        tools = [t for t in tools if tool_is_allowed(t.name, scopes)]
    return apply_tool_meta(tools)


@server.list_tools()
async def list_tools() -> List[Tool]:
    scopes = current_scopes()
    tools = visible_tools(scopes)
    logger.debug(
        "tools/list: %d tools (scopes=%s)",
        len(tools), "unscoped" if scopes is None else sorted(scopes),
    )
    return tools


# Built once at import. Both registries are checked for name collisions here
# rather than at request time, so a collision introduced later fails loudly at
# startup instead of silently routing to whichever module happens to win.
_QUERY_NAMES = {t.name for t in query_server.own_tools()}
_ADMIN_NAMES = {t.name for t in admin_server.own_tools()}
_COLLISIONS = _QUERY_NAMES & _ADMIN_NAMES
if _COLLISIONS:
    raise RuntimeError(
        f"Tool names collide between the query and admin registries: "
        f"{sorted(_COLLISIONS)}. The merged server cannot route these."
    )


@server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]):
    # Reachable only if a client calls a tool it was never shown — a stale
    # tools/list, or a client ignoring it. Checked again here rather than
    # trusting that filtering the listing was enough.
    scopes = current_scopes()
    if scopes is not None and not tool_is_allowed(name, scopes):
        required = TOOL_SCOPES.get(name)
        logger.warning("Refused %s: grant lacks %s", name, required)
        return as_error_result(
            f"'{name}' requires the '{required}' scope, which this connection "
            f"was not granted."
            if required
            else f"'{name}' is not available to this connection.",
            required_scope=required,
        )

    if _is_write_tool(name) and not can_write():
        logger.warning("Refused write tool %s: grant is read-only", name)
        return as_error_result(
            f"'{name}' requires write access, which this connection was not granted."
        )

    if name == "terno_guide":
        return as_tool_result(handle_guide(arguments))
    if name in _QUERY_NAMES:
        return await query_server.call_tool(name, arguments)
    if name in _ADMIN_NAMES:
        return await admin_server.call_tool(name, arguments)
    return as_error_result(f"Unknown tool: {name}")


async def run_server():
    from terno_dbi.mcp.context import describe_backend

    logger.info("Starting Terno MCP server (merged)")
    print(f"Starting Terno MCP Server (API: {describe_backend()})", file=sys.stderr)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())
    logger.info("Terno MCP server stopped")


def main():
    asyncio.run(run_server())


if __name__ == "__main__":
    main()
