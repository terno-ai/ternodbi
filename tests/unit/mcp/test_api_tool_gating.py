"""The connector tools exist only on the `preview` endpoint.

`/mcp` (the `stable` channel) keeps its released surface: the API-source
tools, `list_connectors` and the connector
write tools are neither advertised nor callable there, and `list_datasources`
keeps its original description and response shape. `/mcp/preview` exposes them.
"""

import asyncio

import pytest

from terno_dbi.mcp import query_server
from terno_dbi.mcp.profiles import PREVIEW, STABLE, use_profile

_API_TOOLS = {"get_today", "list_accounts", "list_fields", "data_query",
              "get_query_results"}
_CONNECTOR_TOOLS = {"list_connectors"}
_WRITE_TOOLS = {"list_actions", "execute_action"}
_PREVIEW_ONLY = _API_TOOLS | _CONNECTOR_TOOLS | _WRITE_TOOLS


def _tool_names():
    return {t.name for t in query_server.own_tools()}


def _list_ds_description():
    return next(t for t in query_server.own_tools() if t.name == "list_datasources").description


def _error_text(result):
    return " ".join(b.text for b in result.content).lower()


def test_stable_is_the_default_outside_a_request():
    from terno_dbi.mcp.profiles import current_profile
    assert current_profile() is STABLE


class TestStable:
    @pytest.fixture(autouse=True)
    def _stable(self):
        with use_profile(STABLE):
            yield

    def test_connector_tools_are_not_advertised(self):
        assert _PREVIEW_ONLY.isdisjoint(_tool_names())

    def test_list_datasources_keeps_the_reviewed_description(self):
        assert _list_ds_description() == query_server._LIST_DATASOURCES_DESC_STABLE

    @pytest.mark.parametrize("name", sorted(_PREVIEW_ONLY))
    def test_connector_tool_call_is_rejected_as_unknown(self, name):
        # Defence in depth: even called by name, a hidden tool must not run.
        result = asyncio.run(query_server.call_tool(name, {}))
        assert result.isError
        assert "unknown tool" in _error_text(result)

    def test_list_datasources_response_is_frozen_to_1_0_2_shape(self, monkeypatch):
        # The frozen response carries only the original fields, database-family
        # sources only, and none of the new envelope keys.
        class _StubClient:
            def list_datasources_full(self):
                return {
                    "status": "success",
                    "datasources": [
                        {"id": 1, "name": "PG", "type": "postgres",
                         "description": "", "is_erp": False,
                         "dialect_name": "postgresql", "dialect_version": "16",
                         "family": "database", "auth_status": "connected"},
                        {"id": 7, "name": "GA4", "type": "googleanalytics4",
                         "description": "", "family": "api",
                         "auth_status": "connected"},
                    ],
                    "available": [{"key": "meta_ads", "connect_url": "x"}],
                    "available_count": 1,
                    "notes": ["something"],
                }

        monkeypatch.setattr(query_server, "client", _StubClient())
        # On success call_tool returns (content_blocks, structured_dict).
        result = asyncio.run(query_server.call_tool("list_datasources", {}))
        payload = result[1]

        assert set(payload) == {"datasources", "count"}          # no available/notes
        assert payload["count"] == 1                              # GA4 (api) omitted
        entry = payload["datasources"][0]
        assert entry["name"] == "PG"
        assert "family" not in entry and "auth_status" not in entry


class TestPreview:
    @pytest.fixture(autouse=True)
    def _preview(self):
        with use_profile(PREVIEW):
            yield

    def test_every_connector_tool_is_advertised(self):
        assert _PREVIEW_ONLY <= _tool_names()

    def test_list_datasources_uses_the_richer_description(self):
        assert _list_ds_description() != query_server._LIST_DATASOURCES_DESC_STABLE
        assert "data_query" in _list_ds_description()


def test_profiles_do_not_leak_between_concurrent_requests():
    """Both endpoints share one server object; the profile is per request."""
    seen = {}

    async def probe(profile, key, gate):
        with use_profile(profile):
            await gate.wait()
            seen[key] = _tool_names()

    async def main():
        gate = asyncio.Event()
        tasks = [asyncio.create_task(probe(STABLE, "stable", gate)),
                 asyncio.create_task(probe(PREVIEW, "preview", gate))]
        await asyncio.sleep(0)
        gate.set()
        await asyncio.gather(*tasks)

    asyncio.run(main())
    assert _PREVIEW_ONLY.isdisjoint(seen["stable"])
    assert _PREVIEW_ONLY <= seen["preview"]
