"""The hosted MCP server combining query and admin tools on one endpoint.

The query/admin split is useful for stdio, where separate processes and tokens
provide the permission boundary. Over HTTP, OAuth scopes provide that boundary,
so both tool sets can be exposed through a single endpoint.

This module only composes the existing servers. Tool definitions come from
`query_server.own_tools()` and `admin_server.own_tools()`, and dispatch uses
the same handlers as the stdio servers. The existing `dbi-mcp query` and
`dbi-mcp admin` commands remain unchanged.

Write tools are excluded from `tools/list` when the current grant does not
allow writes, so clients only see tools they can use.
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
from terno_dbi.oauth.scopes import (
    CREDENTIAL_TOOLS,
    TOOL_SCOPES,
    WRITE_SCOPES,
    tool_is_allowed,
)
from terno_dbi.mcp.instructions import MERGED_INSTRUCTIONS
from terno_dbi.mcp.surface import (
    CONNECT_DATASOURCE_TOOL,
    GUIDE_TOOL,
    handle_connect_datasource,
    handle_guide,
    register_surface,
)
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
    """Whether a tool mutates state, based on its annotations.

    Used as a defence-in-depth check when a tool is called. It does not control
    `tools/list`; that is handled by `TOOL_SCOPES`, since annotations and access
    requirements are not always the same. For example, `validate_connection` is
    read-only but requires the `admin:write` scope.
"""
    meta = TOOL_META.get(name)
    return bool(meta) and not meta["hints"]["readOnlyHint"]


def all_tools() -> List[Tool]:
    """Every tool this server can expose, before grant filtering."""
    return [
        GUIDE_TOOL,
        CONNECT_DATASOURCE_TOOL,
        *query_server.own_tools(),
        *admin_server.own_tools(),
    ]


def effective_scopes(
    scopes: Optional[frozenset], writable: bool = True
) -> Optional[frozenset]:
    """Return the scopes the current grant can use.

    Scopes are fixed when the token is granted, but write access is checked against
    the user's current organisation role. If an admin is demoted, their
    `admin:write` scope remains on the token but is temporarily inactive.

    Keeping this effective scope set in one place ensures `tools/list` and
    call-time permission checks use the same access rules. It also handles tools
    such as `validate_connection`, which is marked read-only but requires
    `admin:write`.
    """
    if scopes is None or writable:
        return scopes
    return scopes - WRITE_SCOPES


def visible_tools(scopes: Optional[frozenset], writable: bool = True) -> List[Tool]:
    """The tools a grant may use.

    `scopes=None` means stdio — no grant applies, so everything is shown.
    """
    tools = all_tools()
    scopes = effective_scopes(scopes, writable)
    if scopes is not None:
        tools = [t for t in tools if tool_is_allowed(t.name, scopes)]
    return apply_tool_meta(tools)


@server.list_tools()
async def list_tools() -> List[Tool]:
    scopes = current_scopes()
    writable = can_write()
    tools = visible_tools(scopes, writable)
    if scopes is not None and not writable and (scopes & WRITE_SCOPES):
        logger.info(
            "Withholding write tools: the grant carries %s but the user is no "
            "longer an Org Admin.",
            " ".join(sorted(scopes & WRITE_SCOPES)),
        )
    logger.debug(
        "tools/list: %d tools (scopes=%s, writable=%s)",
        len(tools), "unscoped" if scopes is None else sorted(scopes), writable,
    )
    return tools


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
    scopes = current_scopes()
    writable = can_write()

    if scopes is not None and name in CREDENTIAL_TOOLS:
        logger.warning("Refused %s over OAuth: takes a database credential", name)
        handoff = handle_connect_datasource({"type": (arguments or {}).get("type")})
        return as_error_result(
            f"'{name}' cannot be used here: it would mean sending a database "
            f"password through this conversation. Use the link below instead.",
            **handoff,
        )

    if scopes is not None and not tool_is_allowed(name, effective_scopes(scopes, writable)):
        required = TOOL_SCOPES.get(name)
        withdrawn = bool(required and required in WRITE_SCOPES and required in scopes)
        if withdrawn:
            logger.warning(
                "Refused %s: '%s' is in the grant but the user is no longer an "
                "Org Admin.", name, required,
            )
            message = (
                f"'{name}' needs write access. This connection was granted it, "
                f"but you are no longer an administrator of this organisation, "
                f"so write access is suspended. An administrator can restore it "
                f"in Terno — reconnecting will not."
            )
        else:
            logger.warning("Refused %s: grant lacks %s", name, required)
            message = (
                f"'{name}' requires the '{required}' scope, which this connection "
                f"was not granted."
                if required
                else f"'{name}' is not available to this connection."
            )
        return as_error_result(message, required_scope=required)

    if _is_write_tool(name) and not writable:
        logger.warning("Refused write tool %s: grant is read-only", name)
        return as_error_result(
            f"'{name}' requires write access, which this connection was not granted."
        )

    if name == "terno_guide":
        return as_tool_result(handle_guide(arguments))
    if name == "connect_datasource":
        return as_tool_result(handle_connect_datasource(arguments))
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
