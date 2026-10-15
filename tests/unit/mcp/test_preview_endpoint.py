"""`/mcp` and `/mcp/preview` served side by side from one server object.

These go through the real ASGI app and a real stateful MCP session, because
the profile reaches tool handlers only via the session's context — a unit test
of `own_tools()` alone would not prove that.
"""

import asyncio

import httpx
import pytest

from terno_dbi.mcp import merged_server
from terno_dbi.mcp.http_app import build_asgi_app
from terno_dbi.mcp.profiles import PREVIEW, STABLE, profile_for_resource
from terno_dbi.oauth.scopes import ALL_SCOPES

BASE = "https://mcp.terno.ai"
CONNECTOR_TOOLS = {"data_query", "list_accounts", "list_connectors", "execute_action"}


def _app(profile):
    async def resolver(token):
        return {"api_key": token, "can_write": True, "scopes": ALL_SCOPES}

    return build_asgi_app(
        merged_server.server, token_resolver=resolver, base_url=BASE,
        json_response=True, strict_credentials=False, profile=profile,
    )


async def _list_tools(client, path):
    headers = {"authorization": "Bearer t",
               "accept": "application/json, text/event-stream"}
    init = await client.post(path, headers=headers, json={
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                   "clientInfo": {"name": "test", "version": "0"}},
    })
    assert init.status_code == 200, init.text
    headers["mcp-session-id"] = init.headers["mcp-session-id"]
    await client.post(path, headers=headers,
                      json={"jsonrpc": "2.0", "method": "notifications/initialized"})
    listed = await client.post(path, headers=headers,
                               json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    return {t["name"]: t for t in listed.json()["result"]["tools"]}


def test_each_endpoint_lists_its_own_surface_in_a_real_session():
    apps = {"/mcp": _app(STABLE), "/mcp/preview": _app(PREVIEW)}

    async def router(scope, receive, send):
        await apps[scope["path"]](scope, receive, send)

    async def main():
        transport = httpx.ASGITransport(app=router)
        async with httpx.AsyncClient(transport=transport, base_url=BASE) as client:
            return await asyncio.gather(
                _list_tools(client, "/mcp"), _list_tools(client, "/mcp/preview"))

    stable, preview = asyncio.run(main())

    assert CONNECTOR_TOOLS.isdisjoint(stable)
    assert CONNECTOR_TOOLS <= set(preview)
    assert stable["list_datasources"]["title"] == "List databases"
    assert preview["list_datasources"]["title"] == "List data sources"


@pytest.mark.parametrize("profile, pointer", [
    (STABLE, f"{BASE}/.well-known/oauth-protected-resource\""),
    (PREVIEW, f"{BASE}/.well-known/oauth-protected-resource/mcp/preview\""),
])
def test_401_points_each_endpoint_at_its_own_resource_metadata(profile, pointer):
    async def main():
        transport = httpx.ASGITransport(app=_app(profile))
        async with httpx.AsyncClient(transport=transport, base_url=BASE) as client:
            return await client.post(profile.path, json={})

    response = asyncio.run(main())
    assert response.status_code == 401
    assert response.headers["www-authenticate"].endswith(pointer)


@pytest.mark.django_db
def test_preview_resource_metadata_is_served_at_its_path(client):
    doc = client.get("/.well-known/oauth-protected-resource/mcp/preview").json()
    assert doc["resource"].endswith("/mcp/preview")
    assert "connector:write" in doc["scopes_supported"]

    root = client.get("/.well-known/oauth-protected-resource").json()
    assert root["resource"].endswith("/mcp")
    assert "connector:write" not in root["scopes_supported"]


# ------------------------------------------------------------ consent screen

@pytest.mark.parametrize("resources, expected", [
    (["https://mcp.terno.ai/mcp/preview"], PREVIEW),
    (["https://mcp.terno.ai/mcp/preview/"], PREVIEW),
    (["https://mcp.terno.ai/mcp"], STABLE),
    ([], STABLE),
    (None, STABLE),
])
def test_profile_for_resource(resources, expected):
    assert profile_for_resource(resources) is expected


@pytest.mark.parametrize("query, granted", [
    ("", ["query:read"]),
    ("&resource=https://mcp.terno.ai/mcp", ["query:read"]),
    ("&resource=https://mcp.terno.ai/mcp/preview", ["query:read", "connector:write"]),
])
def test_consent_drops_scopes_the_endpoint_does_not_offer(rf, query, granted):
    """Stripped before the form is built, so the scope is neither shown nor
    granted — and requesting it is not an error, so older clients connect."""
    from unittest.mock import patch

    from django.apps import apps
    if not apps.is_installed("oauth2_provider"):
        pytest.skip("django-oauth-toolkit not installed")
    from oauth2_provider.views import AuthorizationView

    from terno_dbi.oauth import views

    view = views.make_authorization_view()()
    request = rf.get(f"/oauth/authorize/?client_id=c{query}")
    with patch.object(AuthorizationView, "validate_authorization_request",
                      return_value=(["query:read", "connector:write"], {"client_id": "c"})):
        scopes, credentials = view.validate_authorization_request(request)

    assert scopes == granted
    assert credentials == {"client_id": "c"}
