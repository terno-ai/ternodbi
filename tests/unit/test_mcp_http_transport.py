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

# Write tools available to a full grant. `add_datasource` and
# `validate_connection` are excluded because they accept database credentials
# and are not exposed through OAuth.
WRITE_TOOLS = {
    "delete_datasource", "sync_metadata", "rename_table",
    "rename_column", "update_table_description", "update_column_description",
    "save_memory", "edit_memory", "delete_memory", "update_org_prompt",
    "edit_org_prompt",
    "connect_datasource",
}


# ------------------------------------------------------------ merged server

def test_merged_server_carries_both_registries():
    names = {t.name for t in merged_server.all_tools()}
    assert "execute_query" in names          # query registry
    assert "add_datasource" in names         # admin registry
    assert "terno_guide" in names            # shared surface
    assert len(names) == 26


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


def test_credential_tools_are_withheld_from_every_grant():
    """D10. `add_datasource` and `validate_connection` take `connection_str` — a
    DSN with the password in it — so a conversational "connect my Postgres" puts a
    live credential into the model's context, the client's stored history, and the
    client operator's logs. Terno controls none of those and cannot redact them.

    No scope can fix this: the leak happens on the way *in*, before the server
    sees the call. So they are absent from every OAuth listing, including a grant
    holding all five scopes.
    """
    from terno_dbi.oauth.scopes import ALL_SCOPES, CREDENTIAL_TOOLS, tool_is_allowed

    assert CREDENTIAL_TOOLS == {"add_datasource", "validate_connection"}

    full = {t.name for t in merged_server.visible_tools(ALL_SCOPES)}
    assert not (CREDENTIAL_TOOLS & full), "a credential tool is listed over OAuth"
    for name in CREDENTIAL_TOOLS:
        assert not tool_is_allowed(name, ALL_SCOPES)

    # Still present over stdio: that process is started by the operator on their
    # own machine with their own key, and removing it there would break existing
    # `dbi-mcp` users without protecting anyone new.
    assert CREDENTIAL_TOOLS <= {t.name for t in merged_server.visible_tools(None)}


def test_a_credential_tool_called_anyway_is_refused_with_a_usable_message():
    """A client working from a stale tools/list can still call it. The refusal
    must not read as a missing scope — that would send the model off to request a
    permission that would not help — and must tell it where the real path is
    without asking the user for their connection string.
    """
    import asyncio

    with request_credentials(api_key="k", can_write=True,
                             scopes=frozenset({"admin:write"})):
        result = asyncio.run(merged_server.call_tool("add_datasource", {}))

    text = " ".join(c.text for c in result.content).lower()
    assert result.isError
    assert "scope" not in text, "refusal misattributes this to a missing scope"
    # It must carry the same hand-off `connect_datasource` returns, so the model
    # has somewhere to send the user rather than inventing a workaround.
    assert "credential_required" in text
    assert "do not ask them for a connection string" in text
    assert "terno" in text


def test_a_read_only_grant_gets_no_setup_link_from_add_datasource_either():
    """`connect_datasource` requires admin:write: a read-only grant cannot add a
    datasource once connected either, so handing it the setup link would only
    reveal the org's subdomain to someone who could never act on it.

    `add_datasource`'s refusal must honour the same gate, not hand out the link
    unconditionally — otherwise calling the wrong tool name becomes a backdoor
    around the restriction just added to `connect_datasource` itself.
    """
    import asyncio

    with request_credentials(api_key="k", can_write=False,
                             scopes=frozenset({"query:read"})):
        result = asyncio.run(merged_server.call_tool("add_datasource", {}))

    text = " ".join(c.text for c in result.content).lower()
    assert result.isError
    assert "setup_url" not in text and "setup_location" not in text
    assert "write access" in text


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
    # It is now withheld from write grants too, for taking a credential — but the
    # scopes-not-annotations rule this test documents still governs the listing.


def test_unscoped_stdio_sees_everything():
    """`None` means no grant applies, and must not be read as an empty grant."""
    assert {t.name for t in merged_server.visible_tools(None)} == {
        t.name for t in merged_server.all_tools()
    }


def test_an_empty_grant_permits_only_the_unscoped_tools():
    """frozenset() is a real grant that allows nothing — the opposite of None.

    Only `terno_guide` survives it. `connect_datasource` requires admin:write:
    a read-only grant cannot add a datasource once connected either, so showing
    it the setup link would only reveal the org's subdomain to someone who could
    never act on it.
    """
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


# ------------------------------------------- D10: credentials handed over by link

def test_connect_datasource_returns_a_link_and_never_asks_for_a_credential():
    """The pattern Supermetrics uses: per-source auth is a **link**, not a tool
    parameter. Their `data_source_discovery` returns `login_link` +
    `login_note: You need to log in with this link before using this data source.`

    Withholding `add_datasource` alone stopped the leak but stranded the user —
    over a hosted connector there was then no route to a first datasource at all.
    This tool completes the loop without the secret entering the conversation.
    """
    import asyncio

    from django.test import override_settings

    with override_settings(MAIN_DOMAIN="app.terno.ai"):
        with request_credentials(api_key="k", can_write=True,
                                 scopes=frozenset({"admin:write"}),
                                 org_subdomain="acme"):
            content, structured = asyncio.run(
                merged_server.call_tool("connect_datasource", {"type": "postgres"})
            )

    assert structured["setup_url"] == "https://acme.app.terno.ai/admin/core/datasource/add/"
    assert structured["credential_required"] is True
    assert "do not ask them for a connection string" in structured["instruction"].lower()

    # No input on this tool may accept a secret.
    tool = next(t for t in merged_server.all_tools() if t.name == "connect_datasource")
    assert not ({"connection_str", "connection_json", "password"}
                & set(tool.inputSchema["properties"]))


def test_connect_datasource_requires_write_access():
    """A read-only grant cannot add a datasource once connected either, so the
    tool must not be listed or callable for one — showing it the link would only
    reveal the org's subdomain to someone who could never act on it."""
    import asyncio

    from terno_dbi.oauth.scopes import ADMIN_WRITE, DEFAULT_SCOPES, tool_is_allowed

    assert tool_is_allowed("connect_datasource", DEFAULT_SCOPES) is False
    assert tool_is_allowed("connect_datasource", DEFAULT_SCOPES | {ADMIN_WRITE}) is True

    readonly = {t.name for t in merged_server.visible_tools(DEFAULT_SCOPES)}
    assert "connect_datasource" not in readonly

    with request_credentials(api_key="k", can_write=False, scopes=DEFAULT_SCOPES):
        result = asyncio.run(merged_server.call_tool("connect_datasource", {}))
    assert result.isError


def test_the_setup_instruction_does_not_send_the_model_back_for_a_sync():
    """Both `create_datasource` (core/admin_service/views.py) and the admin form
    (`DataSourceAdmin.save_model`) already call `sync_metadata` at creation time
    — the schema is loaded before the user ever returns to the conversation.

    `sync_metadata` exists to refresh a datasource whose *own* schema changed
    later, not to finish one that was just added. Telling the model to run it
    right after connecting is one redundant write call away from wrong: it
    reads as "the schema isn't ready yet," which isn't true.
    """
    from terno_dbi.mcp.setup_link import SETUP_INSTRUCTION

    lowered = SETUP_INSTRUCTION.lower()
    assert "do not suggest sync_metadata" in lowered
    assert "already syncs the schema" in lowered


def test_the_setup_link_carries_no_token():
    """Supermetrics' `login_link` embeds a bearer token, which makes the link
    itself a credential — and it lands in the transcript, the exact place we are
    keeping secrets out of. Anyone who reads the conversation can open it.

    Ours is a plain deep link: opening it requires the user's own Terno session,
    so it grants nothing on its own and is safe in a log or a screenshot.
    """
    from django.test import override_settings

    from terno_dbi.mcp.setup_link import datasource_setup_url

    with override_settings(MAIN_DOMAIN="app.terno.ai"):
        url = datasource_setup_url("acme")

    assert url == "https://acme.app.terno.ai/admin/core/datasource/add/"
    assert "?" not in url and "token" not in url.lower()


def test_setup_link_path_matches_the_real_admin_app_label():
    """The path must be derived from `DataSource._meta`, not hardcoded a second
    time. `app_label` is not the package name — it defaults to the last dotted
    segment of `AppConfig.name` (`terno_dbi.core` -> `core`), so a hand-typed
    `/admin/terno/datasource/add/` looked plausible and 404'd on every real
    deployment. Confirmed live on staging: the actual admin route is
    `/admin/core/datasource/`.
    """
    from terno_dbi.core.models import DataSource
    from terno_dbi.mcp.setup_link import _datasource_admin_path

    assert DataSource._meta.app_label == "core"
    assert _datasource_admin_path() == "/admin/core/datasource/add/"


def test_no_setup_link_is_invented_when_the_workspace_is_unknown():
    """A dead link on this path is worse than prose: the user clicks it, gets an
    error, and gives up. Fall back to naming where to go instead."""
    from django.test import override_settings

    from terno_dbi.mcp.setup_link import datasource_setup_url, setup_handoff

    with override_settings(MAIN_DOMAIN="app.terno.ai"):
        assert datasource_setup_url(None) is None
    with override_settings(MAIN_DOMAIN=""):
        assert datasource_setup_url("acme") is None

        payload = setup_handoff("acme", reason="x")
        assert "setup_url" not in payload
        assert "Datasources" in payload["setup_location"]


def test_connect_datasource_succeeds_rather_than_erroring():
    """For a grant that may actually use it, it is the supported path, not a
    refusal. An `isError` result invites the model to look for a workaround, and
    the workaround is asking the user to paste a DSN — the thing this exists to
    prevent. (A grant without admin:write is refused instead — see
    test_connect_datasource_requires_write_access.)"""
    import asyncio

    with request_credentials(api_key="k", can_write=True, scopes=frozenset({"admin:write"})):
        result = asyncio.run(merged_server.call_tool("connect_datasource", {}))

    # Success is the (content, structuredContent) pair; a refusal is CallToolResult.
    assert isinstance(result, tuple), "connect_datasource returned an error result"


def test_org_subdomain_from_the_resolver_actually_reaches_the_request():
    """`default_token_resolver` puts `org_subdomain` in the dict it returns, but
    `build_asgi_app` never read that key back out of `resolved` when constructing
    `request_credentials` — the keyword was simply absent from the call. So
    `current_org_subdomain()` was always None over the real HTTP path, and
    `connect_datasource` always fell back to prose ("the Datasources section of
    Terno") instead of a real link, for every deployment and every request.

    Every other test in this file calls `request_credentials(org_subdomain=...)`
    directly, which is exactly why none of them caught it — that bypasses
    `build_asgi_app` entirely. This one goes through the real ASGI app, the way
    an actual MCP request does.
    """
    import terno_dbi.mcp.http_app as http_app_module

    seen = {}

    async def resolver(token: str):
        if token != "good":
            raise TokenRejected("Invalid or expired token")
        return {
            "api_key": "good",
            "can_write": False,
            "scopes": frozenset({"query:read"}),
            "org_subdomain": "acme",
        }

    # Captures exactly the kwargs `build_asgi_app` passes to `request_credentials`
    # -- this is the seam that dropped `org_subdomain` -- without needing a real
    # Streamable HTTP session handshake to reach a tool call.
    from contextlib import contextmanager

    real_request_credentials = http_app_module.request_credentials

    @contextmanager
    def spying_request_credentials(**kwargs):
        seen.update(kwargs)
        with real_request_credentials(**kwargs) as creds:
            yield creds

    http_app_module.request_credentials = spying_request_credentials
    try:
        probe_app = build_asgi_app(
            merged_server.server, token_resolver=resolver,
            base_url="https://mcp.terno.ai", strict_credentials=False,
        )
        scope = _scope([(b"authorization", b"Bearer good")])
        try:
            asyncio.run(_capture(probe_app, scope))
        except Exception:
            # Only `request_credentials`'s kwargs matter here; the credentials
            # capture above already ran by the time the real session manager
            # gets involved with the request body/headers.
            pass
    finally:
        http_app_module.request_credentials = real_request_credentials

    assert seen.get("org_subdomain") == "acme", (
        "org_subdomain resolved by the token backend never reached the "
        "request_credentials() call inside build_asgi_app"
    )
