"""Phase 3: HTTP transport, self-contained auth, and grant-filtered tools.

The isolation tests here are the gate on hosting this at all. Under stdio a
credential leak is impossible — one process, one user. Over HTTP one process
serves every organisation, so a leak crosses tenants.
"""

import asyncio
import json
from typing import Any, Dict, List

import pytest

from terno_dbi.mcp import merged_server
from terno_dbi.mcp.context import (
    RequestCredentials,
    can_write,
    current_client,
    current_credentials,
    request_credentials,
)
from terno_dbi.mcp.http_app import TokenRejected, build_asgi_app

WRITE_TOOLS = {
    "add_datasource", "delete_datasource", "sync_metadata", "rename_table",
    "rename_column", "update_table_description", "update_column_description",
    "save_memory", "edit_memory", "delete_memory", "update_org_prompt",
    "edit_org_prompt",
}


# ------------------------------------------------------------ merged server

def test_merged_server_carries_both_registries():
    names = {t.name for t in merged_server.all_tools()}
    assert "execute_query" in names          # query registry
    assert "add_datasource" in names         # admin registry
    assert "terno_guide" in names            # shared surface
    assert len(names) == 25


def test_merged_instructions_fit_the_cap():
    from terno_dbi.mcp.instructions import INSTRUCTIONS_CHAR_CAP, MERGED_INSTRUCTIONS

    assert len(MERGED_INSTRUCTIONS) < INSTRUCTIONS_CHAR_CAP


def test_write_tools_are_hidden_from_a_read_only_grant():
    from terno_dbi.oauth.scopes import ALL_SCOPES, DEFAULT_SCOPES

    writable = {t.name for t in merged_server.visible_tools(ALL_SCOPES)}
    readonly = {t.name for t in merged_server.visible_tools(DEFAULT_SCOPES)}

    assert WRITE_TOOLS <= writable
    assert not (WRITE_TOOLS & readonly), "write tools leaked into a read-only listing"
    assert readonly < writable
    # Reads must survive the filter intact.
    assert {"execute_query", "list_tables", "get_memory", "terno_guide"} <= readonly


def test_validate_connection_is_hidden_despite_being_read_only():
    """The case that proves listing must follow scopes, not annotations.

    `validate_connection` is annotated `readOnlyHint: true` — correctly, it
    changes nothing in Terno — but its endpoint is guarded by `@require_scope(
    'admin:write')`. Filtering the listing on the annotation would show it to a
    read-only grant, which would then get a 403 from the API.
    """
    from terno_dbi.oauth.scopes import DEFAULT_SCOPES

    tool = next(t for t in merged_server.all_tools() if t.name == "validate_connection")
    assert merged_server.apply_tool_meta([tool])[0].annotations.readOnlyHint is True
    assert not merged_server._is_write_tool("validate_connection")

    readonly = {t.name for t in merged_server.visible_tools(DEFAULT_SCOPES)}
    assert "validate_connection" not in readonly


def test_unscoped_stdio_sees_everything():
    """`None` means no grant applies, and must not be read as an empty grant."""
    assert {t.name for t in merged_server.visible_tools(None)} == {
        t.name for t in merged_server.all_tools()
    }


def test_an_empty_grant_permits_only_the_guide():
    """frozenset() is a real grant that allows nothing — the opposite of None."""
    names = {t.name for t in merged_server.visible_tools(frozenset())}
    assert names == {"terno_guide"}


def test_every_tool_has_a_scope_mapping():
    """An unmapped tool is treated as unavailable, but that is a silent
    degradation — catch it here instead."""
    from terno_dbi.oauth.scopes import TOOL_SCOPES

    for tool in merged_server.all_tools():
        assert tool.name in TOOL_SCOPES, f"{tool.name} has no entry in TOOL_SCOPES"


def test_write_classification_comes_from_the_annotations():
    """A separate hand-kept list could disagree with the hint shown to clients."""
    for name in WRITE_TOOLS:
        assert merged_server._is_write_tool(name), f"{name} not classified as write"
    for name in ("execute_query", "list_tables", "terno_guide", "get_memory"):
        assert not merged_server._is_write_tool(name)


def test_a_read_only_grant_cannot_call_a_write_tool_directly():
    """Filtering tools/list is not enough — a client may hold a stale list."""
    with request_credentials(api_key="k", can_write=False, scopes=frozenset({"query:read"})):
        result = asyncio.run(
            merged_server.call_tool("delete_datasource", {"datasource_id": 1})
        )
    assert result.isError is True, "a refusal must be reported as a failed call"
    assert "was not granted" in result.structuredContent["error"]


def test_a_write_grant_reaches_the_admin_handler():
    from unittest.mock import patch

    with request_credentials(api_key="k", can_write=True):
        with patch.object(merged_server.admin_server.client, "delete_datasource",
                          return_value={"success": True}) as mock:
            _, structured = asyncio.run(
                merged_server.call_tool("delete_datasource", {"datasource_id": 7})
            )
    assert structured == {"success": True}
    mock.assert_called_once_with(7)


# ------------------------------------------------------------- isolation

def test_concurrent_requests_do_not_share_credentials():
    """The cross-tenant leak this whole phase exists to prevent.

    Two sessions interleave deliberately: each sets its identity, yields to the
    other mid-request, then reads back what it sees.
    """
    seen: Dict[str, List[Any]] = {}

    async def session(name: str, key: str, writable: bool):
        with request_credentials(api_key=key, base_url=f"https://{name}", can_write=writable):
            await asyncio.sleep(0)  # force a switch to the other task
            creds = current_credentials()
            await asyncio.sleep(0)
            seen[name] = [creds.api_key, creds.base_url, can_write(),
                          current_client().api_key]

    async def main():
        await asyncio.gather(
            session("acme", "key-acme", True),
            session("globex", "key-globex", False),
            session("initech", "key-initech", False),
        )

    asyncio.run(main())

    assert seen["acme"] == ["key-acme", "https://acme", True, "key-acme"]
    assert seen["globex"] == ["key-globex", "https://globex", False, "key-globex"]
    assert seen["initech"] == ["key-initech", "https://initech", False, "key-initech"]


def test_tool_visibility_is_per_request_not_per_process():
    """Two concurrent sessions must get different tool lists."""
    result: Dict[str, set] = {}

    from terno_dbi.oauth.scopes import ALL_SCOPES, DEFAULT_SCOPES

    async def session(name: str, scopes, writable: bool):
        with request_credentials(api_key=f"k-{name}", can_write=writable, scopes=scopes):
            await asyncio.sleep(0)
            result[name] = {t.name for t in await merged_server.list_tools()}

    async def main():
        await asyncio.gather(
            session("writer", ALL_SCOPES, True),
            session("reader", DEFAULT_SCOPES, False),
        )

    asyncio.run(main())
    assert WRITE_TOOLS <= result["writer"]
    assert not (WRITE_TOOLS & result["reader"])


def test_credentials_do_not_leak_after_the_block_exits():
    with request_credentials(api_key="scoped", can_write=False):
        assert current_credentials().api_key == "scoped"
    assert current_credentials() is None


# ------------------------------------------------------------------- auth

def _scope(headers=None):
    return {
        "type": "http",
        "method": "POST",
        "path": "/mcp",
        "headers": headers or [],
    }


async def _capture(app, scope):
    """Drive the ASGI app and collect the response."""
    sent: List[Dict[str, Any]] = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    await app(scope, receive, send)
    start = next(m for m in sent if m["type"] == "http.response.start")
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    headers = {k.decode().lower(): v.decode() for k, v in start["headers"]}
    return start["status"], headers, json.loads(body or b"{}")


@pytest.fixture
def app():
    async def resolver(token: str):
        if token == "good":
            return {"api_key": "good", "can_write": True}
        raise TokenRejected("Invalid or expired token")

    return build_asgi_app(
        merged_server.server,
        token_resolver=resolver,
        base_url="https://mcp.terno.ai",
        strict_credentials=False,  # leave the global flag alone for other tests
    )


def test_missing_token_is_401_with_a_discovery_pointer(app):
    """The header is what makes Claude Code start OAuth instead of showing a
    bare 401 to the user."""
    status, headers, body = asyncio.run(_capture(app, _scope()))
    assert status == 401
    assert "error" in body
    auth = headers["www-authenticate"]
    assert "Bearer" in auth
    assert "https://mcp.terno.ai/.well-known/oauth-protected-resource" in auth


def test_non_bearer_authorization_is_rejected(app):
    scope = _scope([(b"authorization", b"Basic dXNlcjpwYXNz")])
    status, _, _ = asyncio.run(_capture(app, scope))
    assert status == 401


def test_invalid_token_is_401(app):
    scope = _scope([(b"authorization", b"Bearer wrong")])
    status, _, body = asyncio.run(_capture(app, scope))
    assert status == 401
    assert body["error"] == "Invalid or expired token"


def test_resolver_failure_is_500_not_a_silent_pass(app):
    """A broken auth backend must never be treated as 'no restrictions'."""

    async def broken(token: str):
        raise RuntimeError("database down")

    broken_app = build_asgi_app(
        merged_server.server, token_resolver=broken, strict_credentials=False
    )
    scope = _scope([(b"authorization", b"Bearer anything")])
    status, _, body = asyncio.run(_capture(broken_app, scope))
    assert status == 500
    assert "database down" not in json.dumps(body), "internal detail leaked to the caller"


def test_non_http_scope_is_refused(app):
    async def receive():
        return {}

    async def send(message):
        pass

    with pytest.raises(RuntimeError, match="unsupported scope"):
        asyncio.run(app({"type": "websocket", "headers": []}, receive, send))


# ---------------------------------------------------------------- isError

def test_handler_exceptions_set_is_error():
    """MCP clients read `isError` to decide whether a call failed. Without it a
    connection refusal looks like a successful call whose payload happens to
    mention an error, and the model reasons about the 'result'."""
    from unittest.mock import patch

    with patch.object(merged_server.query_server.client, "list_tables",
                      side_effect=RuntimeError("connection refused")):
        result = asyncio.run(merged_server.call_tool("list_tables", {"datasource": "d"}))

    assert result.isError is True
    assert result.structuredContent["error"] == "connection refused"
    assert result.content[0].text  # text form still populated for display


def test_unknown_tool_sets_is_error():
    result = asyncio.run(merged_server.call_tool("no_such_tool", {}))
    assert result.isError is True
    assert "Unknown tool" in result.structuredContent["error"]


def test_scope_refusal_names_the_missing_scope():
    """The model can act on this — ask the user to reconnect with write access —
    only if it is told which scope was missing."""
    with request_credentials(api_key="k", can_write=False, scopes=frozenset({"query:read"})):
        result = asyncio.run(merged_server.call_tool("sync_metadata", {"datasource_id": 1}))
    assert result.isError is True
    assert result.structuredContent["required_scope"] == "admin:sync"


def test_successful_calls_do_not_set_is_error():
    """The counterpart: a success must not be reported as a failure."""
    from unittest.mock import patch

    with patch.object(merged_server.query_server.client, "list_tables",
                      return_value=[{"public_name": "orders"}]):
        content, structured = asyncio.run(
            merged_server.call_tool("list_tables", {"datasource": "d"})
        )
    assert structured["count"] == 1


def test_error_results_skip_output_schema_validation():
    """Returning a CallToolResult short-circuits the SDK's validation, so an
    error payload need not satisfy the tool's success schema."""
    from terno_dbi.mcp.tool_meta import as_error_result

    result = as_error_result("boom", required_scope="admin:write")
    assert result.isError is True
    assert result.structuredContent == {"error": "boom", "required_scope": "admin:write"}
