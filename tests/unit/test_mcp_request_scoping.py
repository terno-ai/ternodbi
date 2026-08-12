"""Phase 2 acceptance: per-request credential isolation.

This is the highest-risk change in the connector plan. Under stdio the old
module-level `TernoDBIClient()` was correct — one process, one user. Under HTTP
one process serves many organisations, and a shared client means every caller
transacts as whoever the process started as. That is a cross-tenant data leak,
not a degraded mode.

The tests that matter here are the concurrent ones. A sequential test passes
against a global singleton too, so it would prove nothing.
"""

import asyncio
from unittest.mock import patch

import pytest

from terno_dbi.mcp import context
from terno_dbi.mcp.context import (
    MissingRequestCredentials,
    client,
    current_client,
    current_credentials,
    describe_backend,
    request_credentials,
)


@pytest.fixture(autouse=True)
def _reset_strict():
    """Strict mode is process-global; never let it leak between tests."""
    before = context.is_strict()
    yield
    context.require_request_credentials(before)


# ------------------------------------------------------------ the leak itself

def test_concurrent_requests_do_not_share_credentials():
    """Two sessions interleaved on one event loop must not see each other.

    Ordering is forced with an event so the second identity is definitely set
    while the first is still mid-request — the exact window a global client
    would leak through.
    """
    second_has_set_creds = asyncio.Event()
    observed = {}

    async def first():
        with request_credentials(api_key="key-org-A", base_url="https://a.example"):
            # Yield until the other task has installed its own credentials.
            await second_has_set_creds.wait()
            observed["first"] = (client.api_key, client.base_url)

    async def second():
        with request_credentials(api_key="key-org-B", base_url="https://b.example"):
            second_has_set_creds.set()
            await asyncio.sleep(0)
            observed["second"] = (client.api_key, client.base_url)

    async def main():
        await asyncio.gather(first(), second())

    asyncio.run(main())

    assert observed["first"] == ("key-org-A", "https://a.example")
    assert observed["second"] == ("key-org-B", "https://b.example")


def test_many_concurrent_identities_stay_distinct():
    """Fan out wider than two, since a leak might only show under contention."""

    async def one(n: int):
        with request_credentials(api_key=f"key-{n}", base_url=f"https://{n}.example"):
            await asyncio.sleep(0)
            for _ in range(3):
                assert client.api_key == f"key-{n}"
                await asyncio.sleep(0)
            return client.api_key

    async def main():
        return await asyncio.gather(*(one(n) for n in range(25)))

    results = asyncio.run(main())
    assert results == [f"key-{n}" for n in range(25)]
    assert len(set(results)) == 25


def test_credentials_do_not_outlive_their_block():
    assert current_credentials() is None
    with request_credentials(api_key="temp"):
        assert current_credentials().api_key == "temp"
    assert current_credentials() is None, "credentials leaked past the context"


def test_nested_scopes_restore_the_outer_identity():
    with request_credentials(api_key="outer"):
        with request_credentials(api_key="inner"):
            assert client.api_key == "inner"
        assert client.api_key == "outer", "inner scope clobbered the outer one"


# --------------------------------------------------- stdio behaviour unchanged

def test_stdio_falls_back_to_environment():
    """The existing single-user stdio path must behave exactly as before."""
    with patch.dict(
        "os.environ",
        {"TERNODBI_API_KEY": "env-key", "TERNODBI_API_URL": "https://env.example"},
    ):
        assert current_credentials() is None
        resolved = current_client()
        assert resolved.api_key == "env-key"
        assert resolved.base_url == "https://env.example"


def test_proxy_delegates_attributes_and_methods():
    with patch.dict("os.environ", {"TERNODBI_API_KEY": "env-key"}):
        assert client.api_key == "env-key"
        assert callable(client.list_datasources)


def test_each_resolution_is_a_fresh_client():
    """No cache: a cache keyed on credentials is the likeliest way to
    reintroduce cross-tenant bleed, so its absence is asserted deliberately."""
    with request_credentials(api_key="k", base_url="https://x.example"):
        assert current_client() is not current_client()


# ----------------------------------------------------------------- strict mode

def test_strict_mode_refuses_to_fall_back():
    """Under HTTP, an unscoped request must fail loudly.

    Falling back would run the call as the server's own identity — which is the
    leak, wearing the costume of a sensible default.
    """
    context.require_request_credentials(True)
    with patch.dict("os.environ", {"TERNODBI_API_KEY": "server-own-key"}):
        with pytest.raises(MissingRequestCredentials, match="server's own identity"):
            current_client()


def test_strict_mode_still_serves_scoped_requests():
    context.require_request_credentials(True)
    with request_credentials(api_key="caller-key", base_url="https://c.example"):
        assert current_client().api_key == "caller-key"


def test_strict_mode_error_surfaces_as_a_tool_error_not_a_crash():
    """A handler hitting this must return an error result, not kill the session."""
    import asyncio as _asyncio

    from terno_dbi.mcp import query_server

    context.require_request_credentials(True)
    result = _asyncio.run(query_server.call_tool("list_datasources", {}))
    assert result.isError is True
    structured = result.structuredContent
    assert "error" in structured
    assert "request credentials" in structured["error"].lower()


# ------------------------------------------------------------------- logging

def test_describe_backend_never_leaks_the_key():
    with request_credentials(api_key="super-secret-key", base_url="https://x.example"):
        rendered = describe_backend()
        assert "super-secret-key" not in rendered
        assert rendered == "https://x.example"


def test_repr_never_leaks_the_key():
    with request_credentials(api_key="super-secret-key"):
        assert "super-secret-key" not in repr(client)
