"""Everything an MCP client sees of the `stable` endpoint, as plain JSON.

`test_stable_manifest.py` compares `snapshot()` against
`fixtures/stable_manifest.json`, so a change to `/mcp`'s tools, prompts,
resources, instructions or OAuth discovery documents has to be made on purpose.

To regenerate the fixture after an intended change:

    DJANGO_SETTINGS_MODULE=terno_dbi.server.settings PYTHONPATH=src:. \
        python tests/unit/mcp/manifest_snapshot.py > tests/unit/mcp/fixtures/stable_manifest.json

The current fixture was generated from commit 1389aca, the release that first
published this surface (`git archive 1389aca src` into a directory, then
point PYTHONPATH at it). Keep this module free of anything newer than the
fixture's source, so the same code runs on both.
"""

import asyncio
import json

RESOURCE_URL = "https://mcp.terno.ai"
AUTHORIZE_URL = "https://app.terno.ai"


def _dump(items):
    return [i.model_dump(mode="json", exclude_none=True) for i in items]


def snapshot() -> dict:
    from terno_dbi.mcp import merged_server, surface
    from terno_dbi.oauth import metadata

    # No request credentials: unscoped, so every tool's definition is included.
    tools = asyncio.run(merged_server.list_tools())
    return {
        "instructions": merged_server.server.instructions,
        "tools": sorted(_dump(tools), key=lambda t: t["name"]),
        "prompts": _dump(asyncio.run(surface.list_prompts())),
        "resources": _dump(asyncio.run(surface.list_resources())),
        "protected_resource_metadata": metadata.protected_resource_metadata(
            RESOURCE_URL, RESOURCE_URL),
        "authorization_server_metadata": metadata.authorization_server_metadata(
            RESOURCE_URL, AUTHORIZE_URL),
    }


if __name__ == "__main__":
    import django
    django.setup()
    print(json.dumps(snapshot(), indent=1, sort_keys=True, ensure_ascii=False))
