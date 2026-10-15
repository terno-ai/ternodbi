"""Behaviour of the two endpoint profiles beyond the manifest snapshot.

Covers what a client actually experiences — calling tools in a real session,
schema validation of real responses — plus the entry points that choose a
profile: the standalone server, the `dbi-mcp` CLI, and OAuth discovery.
"""

import asyncio
import contextvars
import json
import subprocess
import sys

import httpx
import jsonschema
import pytest

from terno_dbi.mcp import merged_server, query_server
from terno_dbi.mcp.http_app import build_asgi_app
from terno_dbi.mcp.profiles import (
    PREVIEW,
    STABLE,
    current_profile,
    profile_for_resource,
    use_profile,
)
from terno_dbi.oauth.scopes import ALL_SCOPES

BASE = "https://mcp.terno.ai"
PROFILES = [STABLE, PREVIEW]

# Shapes of real successful responses (from test_mcp_tool_metadata) that every
# endpoint's schemas must accept — /mcp keeps its released (older) ones.
OBSERVED = [
    ("list_tables", {"tables": [{"id": 1434, "name": "orders"}], "count": 1}),
    ("list_table_columns", {"columns": [{"public_name": "id"}], "count": 1}),
    ("list_memories", {"memories": [], "count": 0}),
    ("grep_memory", {"matches": [], "count": 0}),
    ("get_memory", {"memory": {"name": "x", "content_hash": "abc"}}),
    ("get_org_prompt", {"org_prompt": "", "content_hash": "abc", "has_more": False}),
    ("terno_guide", {"mode": "tour", "content": "# Guide"}),
    ("execute_query", {"status": "success", "columns": ["n"], "data": [{"n": 1}], "row_count": 1}),
    ("save_memory", {"status": "success", "content_hash": "abc"}),
]
FROZEN_LIST_DATASOURCES = {"datasources": [{"id": 1, "name": "shop", "type": "postgres"}],
                           "count": 1}


def _tools(profile):
    with use_profile(profile):
        return {t.name: t for t in merged_server.visible_tools(None)}


# ------------------------------------------------------------- schemas

@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.name)
def test_every_tool_lists_with_valid_metadata(profile):
    for name, tool in _tools(profile).items():
        assert tool.title and tool.annotations is not None, name
        jsonschema.Draft202012Validator.check_schema(tool.outputSchema)
        jsonschema.Draft202012Validator.check_schema(tool.inputSchema)


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.name)
@pytest.mark.parametrize("name, payload", OBSERVED, ids=[n for n, _ in OBSERVED])
def test_real_responses_validate_on_both_endpoints(profile, name, payload):
    jsonschema.validate(payload, _tools(profile)[name].outputSchema)


def test_stable_list_datasources_schema_accepts_its_frozen_response():
    jsonschema.validate(FROZEN_LIST_DATASOURCES,
                        _tools(STABLE)["list_datasources"].outputSchema)


def test_stable_error_schema_is_a_string_and_preview_also_allows_objects():
    stable = _tools(STABLE)["execute_query"].outputSchema["properties"]["error"]
    preview = _tools(PREVIEW)["execute_query"].outputSchema["properties"]["error"]
    assert stable == {"type": "string",
                         "description": "Present instead of the payload when the call failed."}
    jsonschema.validate({"error": {"code": "x"}}, {"properties": {"error": preview}})


def test_stable_overrides_do_not_mutate_the_shared_metadata():
    """`_meta_for` builds copies; the preview metadata must be untouched after
    the stable surface has been rendered."""
    from terno_dbi.mcp.tool_meta import TOOL_META

    before = json.dumps(TOOL_META, sort_keys=True, default=str)
    _tools(STABLE)
    _tools(PREVIEW)
    assert json.dumps(TOOL_META, sort_keys=True, default=str) == before


# ------------------------------------------------------ real sessions

class _StubClient:
    def list_datasources_full(self):
        return {"status": "success", "datasources": [
            {"id": 1, "name": "shop", "type": "postgres", "description": "",
             "is_erp": False, "dialect_name": "postgresql", "dialect_version": "16",
             "family": "database", "auth_status": "connected"},
            {"id": 7, "name": "Google Ads", "type": "google_ads", "family": "api",
             "auth_status": "connected"},
        ], "available": [{"key": "meta_ads"}], "available_count": 1, "notes": ["n"]}

    def get_today(self, timezone=None):
        return {"utc_date": "2026-10-15", "utc_datetime": "2026-10-15T00:00:00Z"}


def _app(profile):
    async def resolver(token):
        return {"api_key": token, "can_write": True, "scopes": ALL_SCOPES}

    return build_asgi_app(merged_server.server, token_resolver=resolver, base_url=BASE,
                          json_response=True, strict_credentials=False, profile=profile)


async def _session(client, path):
    headers = {"authorization": "Bearer t",
               "accept": "application/json, text/event-stream"}
    init = await client.post(path, headers=headers, json={
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                   "clientInfo": {"name": "test", "version": "0"}}})
    headers["mcp-session-id"] = init.headers["mcp-session-id"]
    await client.post(path, headers=headers,
                      json={"jsonrpc": "2.0", "method": "notifications/initialized"})

    async def call(name, arguments=None):
        r = await client.post(path, headers=headers, json={
            "jsonrpc": "2.0", "id": 3, "method": "tools/call",
            "params": {"name": name, "arguments": arguments or {}}})
        return r.json()["result"]
    return call


def _call_on(profile, name):
    async def main():
        transport = httpx.ASGITransport(app=_app(profile))
        async with httpx.AsyncClient(transport=transport, base_url=BASE) as client:
            call = await _session(client, profile.path)
            return await call(name)
    return asyncio.run(main())


def test_stable_list_datasources_call_returns_the_frozen_shape(monkeypatch):
    monkeypatch.setattr(query_server, "client", _StubClient())
    result = _call_on(STABLE, "list_datasources")

    assert not result.get("isError"), result      # passed the SDK's output validation
    assert result["structuredContent"] == {
        "datasources": [{"id": 1, "name": "shop", "type": "postgres", "description": "",
                         "is_erp": False, "dialect_name": "postgresql",
                         "dialect_version": "16"}],
        "count": 1,
    }


def test_preview_list_datasources_call_returns_the_full_envelope(monkeypatch):
    monkeypatch.setattr(query_server, "client", _StubClient())
    result = _call_on(PREVIEW, "list_datasources")

    assert not result.get("isError"), result
    assert {"available", "notes"} <= set(result["structuredContent"])
    assert result["structuredContent"]["count"] == 2


def test_a_preview_tool_called_on_stable_is_unknown(monkeypatch):
    monkeypatch.setattr(query_server, "client", _StubClient())
    result = _call_on(STABLE, "get_today")

    assert result["isError"] is True
    assert "unknown tool" in result["content"][0]["text"].lower()


def test_the_same_tool_works_on_preview(monkeypatch):
    monkeypatch.setattr(query_server, "client", _StubClient())
    result = _call_on(PREVIEW, "get_today")

    assert not result.get("isError"), result
    assert result["structuredContent"]["utc_date"] == "2026-10-15"


# ------------------------------------------------- choosing a profile

@pytest.mark.parametrize("resources, expected", [
    (["https://mcp.terno.ai/mcp", "https://mcp.terno.ai/mcp/preview"], PREVIEW),
    (["https://mcp.terno.ai/mcp/preview?x=1"], PREVIEW),
    (["not a url"], STABLE),
    (["https://mcp.terno.ai/mcp/previewx"], STABLE),
    (["https://mcp.terno.ai/other/mcp/preview"], STABLE),
])
def test_profile_for_resource_edge_cases(resources, expected):
    assert profile_for_resource(resources) is expected


def test_use_profile_restores_the_previous_profile():
    with use_profile(PREVIEW):
        with use_profile(STABLE):
            assert current_profile() is STABLE
        assert current_profile() is PREVIEW
    assert current_profile() is STABLE


def test_hidden_and_advertised_scopes_partition_all_scopes():
    for profile in PROFILES:
        assert profile.hidden_scopes | profile.advertised_scopes == ALL_SCOPES
        assert not (profile.hidden_scopes & profile.advertised_scopes)
    assert STABLE.hidden_scopes == {"connector:write"}
    assert PREVIEW.hidden_scopes == frozenset()


@pytest.mark.parametrize("argv, expected", [
    (["serve"], "stable"),
    (["serve", "--profile", "preview"], "preview"),
    (["query", "--profile", "preview"], "preview"),
])
def test_cli_stdio_runs_with_the_chosen_profile(monkeypatch, argv, expected):
    from terno_dbi.mcp import __main__ as cli

    seen = {}
    for module in ("merged_server", "query_server", "admin_server"):
        monkeypatch.setattr(f"terno_dbi.mcp.{module}.main",
                            lambda: seen.setdefault("profile", current_profile().name))
    monkeypatch.setattr(sys, "argv", ["dbi-mcp", *argv])
    # Run in a copied context: the CLI sets the process-wide profile.
    contextvars.copy_context().run(cli.main)
    assert seen["profile"] == expected


def test_cli_http_builds_the_app_with_the_chosen_profile(monkeypatch):
    from terno_dbi.mcp import __main__ as cli

    seen = {}
    monkeypatch.setattr(cli, "_run_http",
                        lambda server, host, port, profile: seen.update(profile=profile))
    monkeypatch.setattr(sys, "argv", ["dbi-mcp", "serve", "--transport", "http",
                                      "--profile", "preview"])
    contextvars.copy_context().run(cli.main)
    assert seen["profile"] is PREVIEW


def test_cli_rejects_an_unknown_profile(monkeypatch):
    from terno_dbi.mcp import __main__ as cli

    monkeypatch.setattr(sys, "argv", ["dbi-mcp", "serve", "--profile", "everything"])
    with pytest.raises(SystemExit):
        cli.main()


def test_standalone_server_routes_both_endpoints():
    """Run in a subprocess: importing the module installs the in-process
    transport globally, which must not leak into other tests."""
    script = r"""
import asyncio, os, json
os.environ["DJANGO_SETTINGS_MODULE"] = "terno_dbi.server.settings"
os.environ["TERNO_MCP_BASE_URL"] = "https://mcp.terno.ai"
import httpx
from terno_dbi.server import asgi
async def main():
    t = httpx.ASGITransport(app=asgi.application)
    out = {"paths": sorted(asgi.mcp_applications)}
    async with httpx.AsyncClient(transport=t, base_url="https://mcp.terno.ai") as c:
        for p in ("/mcp", "/mcp/", "/mcp/preview", "/mcp/preview/"):
            r = await c.post(p, json={})
            out[p] = [r.status_code, r.headers.get("www-authenticate", "")]
        r = await c.get("/.well-known/oauth-protected-resource/mcp/preview")
        out["prm"] = [r.status_code, r.json()["resource"]]
    print(json.dumps(out))
asyncio.run(main())
"""
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True,
                          timeout=120)
    assert proc.returncode == 0, proc.stderr[-2000:]
    out = json.loads(proc.stdout.strip().splitlines()[-1])

    assert out["paths"] == ["/mcp", "/mcp/preview"]
    for path in ("/mcp", "/mcp/"):
        assert out[path][0] == 401
        assert out[path][1].endswith('/.well-known/oauth-protected-resource"')
    for path in ("/mcp/preview", "/mcp/preview/"):
        assert out[path][0] == 401
        assert out[path][1].endswith('/.well-known/oauth-protected-resource/mcp/preview"')
    assert out["prm"] == [200, "https://mcp.terno.ai/mcp/preview"]


# ---------------------------------------------------------- discovery

@pytest.mark.django_db
def test_preview_resource_metadata_has_the_discovery_headers(client):
    r = client.get("/.well-known/oauth-protected-resource/mcp/preview")
    assert r["Access-Control-Allow-Origin"] == "*"
    assert "max-age" in r["Cache-Control"]


@pytest.mark.django_db
def test_the_shared_authorization_server_never_advertises_connector_write(client):
    doc = client.get("/.well-known/oauth-authorization-server").json()
    assert "connector:write" not in doc["scopes_supported"]


@pytest.mark.parametrize("name", sorted(query_server._PREVIEW_ONLY_TOOL_NAMES))
def test_every_preview_only_tool_is_routed_on_preview(monkeypatch, name):
    """Regression: the merged server's routing table was built at import from
    the default (stable) channel, so preview tools were listed on
    /mcp/preview but answered "Unknown tool" when called."""
    monkeypatch.setattr(query_server, "client", _StubClient())
    result = _call_on(PREVIEW, name)
    text = json.dumps(result).lower()
    assert "unknown tool" not in text, result


@pytest.mark.parametrize("name", sorted(query_server._PREVIEW_ONLY_TOOL_NAMES))
def test_every_preview_only_tool_is_unknown_on_stable(monkeypatch, name):
    monkeypatch.setattr(query_server, "client", _StubClient())
    result = _call_on(STABLE, name)
    assert result["isError"] is True
    assert "unknown tool" in result["content"][0]["text"].lower()


# ------------------------------------------- per-request tool definitions

def test_sdk_still_has_the_hook_the_merged_server_overrides():
    """If an MCP SDK upgrade renames it, validation silently falls back to the
    shared cache; fail here instead."""
    from mcp.server.lowlevel import Server

    assert hasattr(Server, "_get_cached_tool_definition")
    assert "_get_cached_tool_definition" in type(merged_server.server).__dict__


def _definition(profile, name, scopes=ALL_SCOPES, writable=True):
    from terno_dbi.mcp.context import request_credentials

    async def get():
        return await merged_server.server._get_cached_tool_definition(name)

    with use_profile(profile), request_credentials(api_key="k", can_write=writable,
                                                   scopes=scopes):
        return asyncio.run(get())


def test_call_validation_uses_the_callers_own_definitions():
    assert _definition(STABLE, "list_datasources").title == "List databases"
    assert _definition(PREVIEW, "list_datasources").title == "List data sources"
    assert _definition(STABLE, "data_query") is None
    assert _definition(PREVIEW, "data_query") is not None
    # A grant that cannot see a tool gets no definition either.
    assert _definition(STABLE, "save_memory", scopes=frozenset({"query:read"})) is None


def test_a_preview_listing_does_not_leak_into_stable_calls(monkeypatch):
    """The order that exposed the shared cache: a preview client lists tools,
    then a /mcp client calls a preview-only tool without arguments. It must get
    "Unknown tool", not an input-validation error naming the tool's params."""
    monkeypatch.setattr(query_server, "client", _StubClient())
    apps = {STABLE.path: _app(STABLE), PREVIEW.path: _app(PREVIEW)}

    async def router(scope, receive, send):
        await apps[scope["path"]](scope, receive, send)

    from mcp.types import ListToolsRequest

    from terno_dbi.mcp.context import request_credentials

    async def main():
        # Prime the SDK's shared cache with the preview listing, as a preview
        # client's tools/list does.
        with use_profile(PREVIEW), request_credentials(api_key="k", scopes=ALL_SCOPES):
            await merged_server.server.request_handlers[ListToolsRequest](None)
        assert "data_query" in merged_server.server._tool_cache

        transport = httpx.ASGITransport(app=router)
        async with httpx.AsyncClient(transport=transport, base_url=BASE) as client:
            preview_call = await _session(client, PREVIEW.path)
            await preview_call("get_today")
            stable_call = await _session(client, STABLE.path)
            return await stable_call("data_query")

    result = asyncio.run(main())
    text = result["content"][0]["text"].lower()
    assert result["isError"] is True
    assert "unknown tool" in text and "required property" not in text
