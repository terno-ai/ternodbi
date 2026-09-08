"""The API-source MCP tools are gated by TERNO_ENABLE_API_MCP_TOOLS.

A production deploy can carry the new connector code while the submitted
`mcp.terno.ai` tool manifest stays exactly as reviewed: with the flag off, the
API tools are neither advertised nor callable, and `list_datasources` keeps its
original description. The flag turns them on once the directory listings are
approved.
"""

import asyncio

import pytest

from terno_dbi.mcp import query_server

_API_TOOLS = {"get_today", "list_accounts", "list_fields", "data_query",
              "get_query_results"}


def _tool_names():
    return {t.name for t in query_server.own_tools()}


def _list_ds_description():
    return next(t for t in query_server.own_tools() if t.name == "list_datasources").description


class TestGateOff:
    @pytest.fixture(autouse=True)
    def _off(self, monkeypatch):
        monkeypatch.setenv("TERNO_ENABLE_API_MCP_TOOLS", "")

    def test_api_tools_are_not_advertised(self):
        assert _API_TOOLS.isdisjoint(_tool_names())

    def test_list_datasources_keeps_the_reviewed_description(self):
        assert _list_ds_description() == query_server._LIST_DATASOURCES_DESC_STABLE

    def test_api_tool_call_is_rejected_as_unknown(self):
        # Defence in depth: even called by name, a gated tool must not run.
        result = asyncio.run(query_server.call_tool("data_query", {}))
        assert result.isError
        text = " ".join(b.text for b in result.content).lower()
        assert "unknown tool" in text

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


class TestGateOn:
    @pytest.fixture(autouse=True)
    def _on(self, monkeypatch):
        monkeypatch.setenv("TERNO_ENABLE_API_MCP_TOOLS", "true")

    def test_api_tools_are_advertised(self):
        assert _API_TOOLS <= _tool_names()

    def test_list_datasources_uses_the_richer_description(self):
        assert _list_ds_description() != query_server._LIST_DATASOURCES_DESC_STABLE
        assert "data_query" in _list_ds_description()
