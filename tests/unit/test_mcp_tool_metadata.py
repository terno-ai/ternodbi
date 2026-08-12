"""Phase 0 acceptance: instruction length, tool annotations, output schemas.

These are gates for a connector-directory listing, and two of them fail
silently in production if they regress — an over-long `instructions` blob is
truncated by the client with no error, and a tool missing `readOnlyHint` is
simply treated as unsafe. Hence tests rather than a one-off check.
"""

import asyncio

import jsonschema
import pytest

from terno_dbi.mcp import admin_server, query_server, surface
from terno_dbi.mcp.instructions import (
    ADMIN_INSTRUCTIONS,
    INSTRUCTIONS_CHAR_CAP,
    QUERY_INSTRUCTIONS,
)
from terno_dbi.mcp.tool_meta import (
    TOOL_META,
    MissingToolMetadata,
    apply_tool_meta,
    as_tool_result,
)

# 10 query + 14 admin, each plus the shared `terno_guide`.
EXPECTED_QUERY_TOOLS = 11
EXPECTED_ADMIN_TOOLS = 15


def _tools(server_module):
    handler = server_module.server.request_handlers
    # Call the registered list_tools function directly rather than through the
    # request handler, which would need a full session.
    return asyncio.run(server_module.list_tools())


@pytest.fixture(scope="module")
def query_tools():
    return _tools(query_server)


@pytest.fixture(scope="module")
def admin_tools():
    return _tools(admin_server)


# --------------------------------------------------------------- 0.1 blobs

@pytest.mark.parametrize(
    "name,blob",
    [("query", QUERY_INSTRUCTIONS), ("admin", ADMIN_INSTRUCTIONS)],
)
def test_instructions_fit_the_truncation_cap(name, blob):
    assert len(blob) < INSTRUCTIONS_CHAR_CAP, (
        f"{name} instructions are {len(blob)} chars; Claude Code truncates at "
        f"{INSTRUCTIONS_CHAR_CAP} and the tail would be silently lost."
    )


@pytest.mark.parametrize(
    "name,blob,must_mention",
    [
        ("query", QUERY_INSTRUCTIONS, ["list_datasources", "execute_query"]),
        ("admin", ADMIN_INSTRUCTIONS, ["expected_hash"]),
    ],
)
def test_essentials_land_in_the_first_512_chars(name, blob, must_mention):
    """OpenAI's guidance: the first ~512 chars carry the weight."""
    head = blob[:512]
    for token in must_mention:
        assert token in head, f"{name}: '{token}' must appear in the first 512 chars"


# ------------------------------------------------- 0.2 annotations + titles

def test_tool_counts(query_tools, admin_tools):
    assert len(query_tools) == EXPECTED_QUERY_TOOLS
    assert len(admin_tools) == EXPECTED_ADMIN_TOOLS


def test_find_similar_examples_is_gone(query_tools):
    assert "find_similar_examples" not in {t.name for t in query_tools}


def test_every_tool_has_a_title_and_hints(query_tools, admin_tools):
    for tool in query_tools + admin_tools:
        assert tool.title, f"{tool.name} has no top-level title"
        assert tool.annotations is not None, f"{tool.name} has no annotations"
        assert tool.annotations.title == tool.title, (
            f"{tool.name}: title and annotations.title disagree"
        )
        assert tool.annotations.readOnlyHint is not None
        assert tool.annotations.destructiveHint is not None


def test_titles_use_the_user_s_vocabulary(query_tools, admin_tools):
    """Titles say "database"; "datasource" is our word, not the reader's."""
    for tool in query_tools + admin_tools:
        assert "datasource" not in tool.title.lower(), (
            f"{tool.name}: title '{tool.title}' leaks internal vocabulary"
        )


def test_destructive_titles_are_unmistakable(admin_tools):
    """These titles are what someone reads in a confirmation dialog, a moment
    before approving a deletion. They must not be ambiguous."""
    for tool in admin_tools:
        if tool.annotations.destructiveHint:
            assert "delete" in tool.title.lower(), (
                f"{tool.name} is destructive but its title '{tool.title}' does "
                f"not say so"
            )


def test_query_server_is_entirely_read_only(query_tools):
    for tool in query_tools:
        assert tool.annotations.readOnlyHint is True, f"{tool.name} is not read-only"
        assert tool.annotations.destructiveHint is False


def test_destructive_tools_are_flagged(admin_tools):
    destructive = {
        t.name for t in admin_tools if t.annotations.destructiveHint is True
    }
    assert destructive == {"delete_datasource", "delete_memory"}


def test_read_only_tools_are_never_destructive():
    for name, meta in TOOL_META.items():
        hints = meta["hints"]
        if hints["readOnlyHint"]:
            assert hints["destructiveHint"] is False, (
                f"{name} claims to be read-only and destructive at once"
            )


def test_apply_tool_meta_refuses_an_unknown_tool():
    from mcp.types import Tool

    orphan = Tool(name="brand_new_tool", description="x", inputSchema={"type": "object"})
    with pytest.raises(MissingToolMetadata, match="brand_new_tool"):
        apply_tool_meta([orphan])


# ------------------------------------------------------- 0.3 output schemas

def test_every_tool_has_an_output_schema(query_tools, admin_tools):
    for tool in query_tools + admin_tools:
        assert tool.outputSchema is not None, f"{tool.name} has no outputSchema"
        jsonschema.Draft7Validator.check_schema(tool.outputSchema)


def test_error_payloads_validate_against_every_schema(query_tools, admin_tools):
    """The SDK validates structuredContent and fails the call on a mismatch.

    Handlers return {"error": ...} on any exception, so if a schema rejected
    that shape, every failure would surface as a confusing validation error
    instead of the actual cause.
    """
    _, structured = as_tool_result({"error": "connection refused"})
    for tool in query_tools + admin_tools:
        jsonschema.validate(instance=structured, schema=tool.outputSchema)


def test_non_dict_results_are_wrapped(query_tools):
    """A backend returning a bare list must not break output validation."""
    content, structured = as_tool_result([1, 2, 3])
    assert structured == {"result": [1, 2, 3]}
    assert content[0].text  # text form is still populated
    by_name = {t.name: t for t in query_tools}
    jsonschema.validate(instance=structured, schema=by_name["list_tables"].outputSchema)


def test_content_and_structured_content_agree():
    import json

    payload = {"tables": [{"public_name": "orders"}], "count": 1}
    content, structured = as_tool_result(payload)
    assert structured == payload
    assert json.loads(content[0].text) == payload


# ------------------------------------------------- Phase 1: resources/prompts

def test_docs_resource_is_reachable_because_the_blobs_promise_it():
    """Both instruction blobs tell the model to read `ternodbi://docs`.

    Until it is served those are dangling pointers, which is worse than not
    mentioning it — the model is told a source exists and cannot fetch it.
    """
    import asyncio

    from terno_dbi.mcp.instructions import (
        ADMIN_INSTRUCTIONS,
        DOCS_LONG_FORM,
        QUERY_INSTRUCTIONS,
    )
    from terno_dbi.mcp.surface import DOCS_URI, RESOURCES

    assert DOCS_URI in QUERY_INSTRUCTIONS
    assert DOCS_URI in ADMIN_INSTRUCTIONS
    assert DOCS_URI in {str(r.uri) for r in RESOURCES}

    body = asyncio.run(surface.read_resource(DOCS_URI))
    assert body == DOCS_LONG_FORM
    assert len(body) > 2000, "docs resource should carry what the blobs could not"


def test_changelog_resource_resolves_to_real_content():
    """A fallback stub would mean `whats_new` silently says nothing useful."""
    import asyncio

    from terno_dbi.mcp.surface import CHANGELOG_URI, _CHANGELOG_FALLBACK

    body = asyncio.run(surface.read_resource(CHANGELOG_URI))
    assert body != _CHANGELOG_FALLBACK, "CHANGELOG.md was not found"
    assert "0.1.35" in body


def test_unknown_resource_raises():
    import asyncio

    with pytest.raises(ValueError, match="Unknown resource"):
        asyncio.run(surface.read_resource("ternodbi://nope"))


def test_prompt_definitions_are_complete():
    import asyncio

    from terno_dbi.mcp.surface import _PROMPTS

    prompts = asyncio.run(surface.list_prompts())
    assert {p.name for p in prompts} == set(_PROMPTS)
    for p in prompts:
        assert p.title and p.description


@pytest.mark.parametrize("module", [query_server, admin_server])
def test_each_server_registers_the_surface(module):
    """`register_surface()` is what advertises resources/ and prompts/ in
    `initialize`. Omitting the call on a server — the merged one being the next
    chance to do so — makes both capabilities silently absent, with the tools
    still working, so nothing else here would catch it."""
    from mcp import types

    handlers = module.server.request_handlers
    for request_type in (
        types.ListResourcesRequest,
        types.ReadResourceRequest,
        types.ListPromptsRequest,
        types.GetPromptRequest,
    ):
        assert request_type in handlers, (
            f"{module.server.name} has no handler for {request_type.__name__} — "
            f"is register_surface() called?"
        )


def test_prompts_render_a_user_message():
    import asyncio

    result = asyncio.run(surface.get_prompt("build-a-query", None))
    assert result.messages[0].role == "user"
    assert "datasource" in result.messages[0].content.text.lower()


def test_unknown_prompt_raises():
    import asyncio

    with pytest.raises(ValueError, match="Unknown prompt"):
        asyncio.run(surface.get_prompt("nope", None))


def test_guide_tool_is_on_both_servers_and_read_only(query_tools, admin_tools):
    for tools in (query_tools, admin_tools):
        guide = next(t for t in tools if t.name == "terno_guide")
        assert guide.annotations.readOnlyHint is True
        assert guide.title == "About this connector"


@pytest.mark.parametrize("mode,expected", [("tour", "tour"), ("whats_new", "whats_new"), (None, "tour")])
def test_guide_modes(mode, expected):
    from terno_dbi.mcp.surface import handle_guide

    result = handle_guide({"mode": mode} if mode else {})
    assert result["mode"] == expected
    assert len(result["content"]) > 500


def test_guide_output_matches_its_declared_schema(query_tools):
    from terno_dbi.mcp.surface import handle_guide

    schema = {t.name: t.outputSchema for t in query_tools}["terno_guide"]
    for mode in ("tour", "whats_new"):
        _, structured = as_tool_result(handle_guide({"mode": mode}))
        jsonschema.validate(instance=structured, schema=schema)


# ------------------------------------- schemas vs. the shapes actually returned

def test_row_schema_matches_the_real_response_shape(query_tools):
    """Rows come back as objects keyed by column name, not positional arrays.

    This is a regression test with a scar: `_ROWS` originally declared
    `items: {type: array}`, which made the SDK reject **every successful**
    `execute_query` result with an output-validation error. It passed unit tests
    because none of them validated a real response body, and only surfaced when
    a live query was run end to end.
    """
    by_name = {t.name: t for t in query_tools}

    real_execute_query = {
        "status": "success",
        "columns": ["status", "n"],
        "data": [{"status": "paid", "n": 3}, {"status": "refunded", "n": 1}],
        "row_count": 2,
    }
    jsonschema.validate(real_execute_query, by_name["execute_query"].outputSchema)

    real_sample = {
        "status": "success",
        "table_id": 1434,
        "columns": ["id", "customer"],
        "data": [{"id": 1, "customer": "Ada"}],
    }
    jsonschema.validate(real_sample, by_name["get_sample_data"].outputSchema)


@pytest.mark.parametrize(
    "tool_name,payload",
    [
        ("list_datasources", {"datasources": [{"id": 1, "name": "shop"}], "count": 1}),
        ("list_tables", {"tables": [{"id": 1434, "name": "orders"}], "count": 1}),
        ("list_table_columns", {"columns": [{"public_name": "id"}], "count": 1}),
        ("list_memories", {"memories": [], "count": 0}),
        ("grep_memory", {"matches": [], "count": 0}),
        ("get_memory", {"memory": {"name": "x", "content_hash": "abc"}}),
        ("get_org_prompt", {"org_prompt": "", "content_hash": "abc", "has_more": False}),
        ("terno_guide", {"mode": "tour", "content": "# Guide"}),
    ],
)
def test_observed_payloads_validate(query_tools, tool_name, payload):
    """Shapes taken from live responses against the local server."""
    by_name = {t.name: t for t in query_tools}
    jsonschema.validate(payload, by_name[tool_name].outputSchema)


def test_admin_observed_payloads_validate(admin_tools):
    by_name = {t.name: t for t in admin_tools}
    observed = {
        "validate_connection": {"status": "success", "valid": True, "message": "ok"},
        "sync_metadata": {"status": "success", "datasource_id": 15,
                          "sync_result": {"tables_created": 0, "tables_updated": 1}},
        "get_table_info": {"status": "success", "columns": [{"name": "id"}],
                           "sample_data": [{"id": 1}]},
        "save_memory": {"status": "success", "content_hash": "abc"},
    }
    for name, payload in observed.items():
        jsonschema.validate(payload, by_name[name].outputSchema)
