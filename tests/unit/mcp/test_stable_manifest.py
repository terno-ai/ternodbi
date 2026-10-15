"""The stable endpoint (`/mcp`) must match its published snapshot.

Any change to a tool, prompt, resource, the instructions or the OAuth discovery
documents on `/mcp` fails here. Put new tools on `preview` instead, or, to
release a change on purpose, regenerate the fixture (see `manifest_snapshot`).
"""

import json
from pathlib import Path

from terno_dbi.mcp import merged_server
from terno_dbi.mcp.profiles import PREVIEW, STABLE, use_profile
from terno_dbi.oauth.scopes import ALL_SCOPES
from tests.unit.mcp.manifest_snapshot import snapshot

FIXTURE = Path(__file__).parent / "fixtures" / "stable_manifest.json"

# What a fully-scoped OAuth grant lists on `/mcp` (the credential
# tools add_datasource / validate_connection are never listed over OAuth).
STABLE_OAUTH_TOOLS = {
    "connect_datasource", "delete_datasource", "delete_memory", "edit_memory",
    "edit_org_prompt", "execute_query", "get_memory", "get_org_prompt",
    "get_sample_data", "get_table_info", "grep_memory", "grep_org_prompt",
    "list_datasources", "list_memories", "list_table_columns", "list_tables",
    "rename_column", "rename_table", "save_memory", "sync_metadata",
    "terno_guide", "update_column_description", "update_org_prompt",
    "update_table_description",
}


def test_stable_endpoint_matches_its_snapshot():
    expected = json.loads(FIXTURE.read_text())
    with use_profile(STABLE):
        actual = json.loads(json.dumps(snapshot()))
    for section in expected:
        assert actual[section] == expected[section], f"`/mcp` {section} changed"
    assert set(actual) == set(expected)


def test_a_full_grant_on_stable_lists_the_24_released_tools():
    with use_profile(STABLE):
        names = {t.name for t in merged_server.visible_tools(ALL_SCOPES)}
    assert names == STABLE_OAUTH_TOOLS


def test_preview_is_a_superset_of_stable():
    with use_profile(PREVIEW):
        names = {t.name for t in merged_server.visible_tools(ALL_SCOPES)}
    assert STABLE_OAUTH_TOOLS < names
    assert {"data_query", "list_connectors", "execute_action"} <= names
