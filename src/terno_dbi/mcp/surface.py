"""Resources, prompts, and the guide tool — the non-tool server surface.

Registered onto a `Server` by `register_surface()`. Kept in one module, and
parameterised by role rather than duplicated per server, because `query` and
`admin` merge into a single hosted server: this file is then registered once
instead of needing a third copy reconciled against two others.

Three things live here:

- **`ternodbi://docs`** — the long-form guidance that does not fit in
  `instructions`. Both instruction blobs already tell the model to read it, so
  until this is served those pointers dangle.
- **`ternodbi://changelog`** — `CHANGELOG.md`, so an agent can check whether a
  capability exists in the running version rather than guessing.
- **`terno_guide`** — a tool, because an agent mid-conversation looks for a tool.
  Resources are discoverable but rarely fetched unprompted.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp.types import (
    GetPromptResult,
    Prompt,
    PromptArgument,
    PromptMessage,
    Resource,
    TextContent,
    Tool,
)

from terno_dbi.mcp.instructions import DOCS_LONG_FORM

DOCS_URI = "ternodbi://docs"
CHANGELOG_URI = "ternodbi://changelog"

_CHANGELOG_FALLBACK = (
    "# Changelog\n\nCHANGELOG.md was not packaged with this install. "
    "See https://github.com/terno-ai/ternodbi for release history.\n"
)


def _changelog_text() -> str:
    """Read `CHANGELOG.md`, from the package in an install or the repo root in a
    source checkout.

    Both locations are needed: pyproject force-includes it as
    `terno_dbi/CHANGELOG.md` for the wheel, and it lives at the repo root for
    GitHub. Falls back rather than raising — a missing changelog is a packaging
    gap, not a reason for `resources/read` to fail mid-conversation.
    """
    here = Path(__file__).resolve()
    for candidate in (
        here.parents[1] / "CHANGELOG.md",   # installed: terno_dbi/CHANGELOG.md
        here.parents[3] / "CHANGELOG.md",   # source checkout: repo root
    ):
        try:
            return candidate.read_text(encoding="utf-8")
        except OSError:
            continue
    return _CHANGELOG_FALLBACK


RESOURCES: List[Resource] = [
    Resource(
        uri=DOCS_URI,
        name="TernoDBI connector guide",
        title="Connector guide",
        description=(
            "Full guidance on reading schema, memory (stores, read-before-write), "
            "and when to use org_prompt versus memory. Referenced from the server "
            "instructions, which are length-capped and cannot carry it."
        ),
        mimeType="text/markdown",
    ),
    Resource(
        uri=CHANGELOG_URI,
        name="TernoDBI changelog",
        title="Changelog",
        description=(
            "Release history. Check here before assuming a capability exists in "
            "the version you are talking to."
        ),
        mimeType="text/markdown",
    ),
]


# Deliberately short. Supermetrics' four prompts are 78-188 characters and still
# earn their slot — a prompt is a discoverable starting point, not a system
# prompt. The value is that a user sees the connector can do this at all.
_PROMPTS: Dict[str, Dict[str, str]] = {
    "explore-datasources": {
        "title": "Explore my databases",
        "description": "List reachable databases and summarise what is in them.",
        "text": (
            "Show me the datasources I have access to and what's in them. "
            "List the databases, then for the most substantial one summarise its "
            "tables. Check memory for anything already recorded about this data "
            "before describing it."
        ),
    },
    "build-a-query": {
        "title": "Build a SQL query",
        "description": "Pick a datasource, read its schema, then write and run SQL.",
        "text": (
            "Help me build a SQL query. Ask which datasource if it is ambiguous, "
            "read the relevant table and column names first rather than guessing "
            "them, check memory for known join paths and business rules, then "
            "write the SQL and run it. Use public names."
        ),
    },
    "schema-health": {
        "title": "Review schema metadata",
        "description": "Find tables and columns missing descriptions.",
        "text": (
            "Review the schema metadata for a datasource and report what is "
            "missing: tables and columns with no description, names that are "
            "opaque enough to need one, and anything whose description looks "
            "stale against the actual data. Report first — do not write "
            "descriptions until I have seen the list."
        ),
    },
}


GUIDE_TOOL = Tool(
    name="terno_guide",
    description=(
        "Orientation for this connector: what it can do, the canonical tool "
        "sequence, and what changed in recent versions. Call at most once per "
        "conversation, when you are unsure what the connector supports — not "
        "before every task."
    ),
    inputSchema={
        "type": "object",
        "properties": {
            "mode": {
                "type": "string",
                "enum": ["tour", "whats_new"],
                "description": (
                    "'tour' for what the connector does and how to use it; "
                    "'whats_new' for the changelog. Default 'tour'."
                ),
            }
        },
        "required": [],
    },
)

_TOUR = """\
# Terno connector — orientation

Terno gives you read access to this organisation's configured SQL databases,
plus a durable shared memory of facts recorded about them.

## Reading data

1. `list_datasources` — what you can reach
2. `list_tables` / `list_table_columns` — schema, with public names
3. `execute_query` — SELECT only, written against public names
   (`get_sample_data` previews one table without writing SQL)

`execute_query` is read-only by construction: every statement is parsed, must be
a single SELECT, and any DML or DDL anywhere in the tree is rejected before
execution. There is no setting that bypasses this.

## Memory — check it before you answer

`grep_memory` or `list_memories`, then `get_memory` for the full entry. The index
returns one-line descriptions, which are hooks, not facts. Memory holds what
previous agents learned about this data: join paths, schema quirks, business
rules. Reading it is usually cheaper than rediscovering.

## Writing

Schema metadata, memory, and the organisation prompt are writable where your
grant allows it — tools you cannot use are not listed for you. Writes to
existing content require a hash from a read made immediately before, which the
server enforces.

## Full detail

Read the `ternodbi://docs` resource — especially before writing to the
organisation prompt, which affects every user in the organisation.
"""


def handle_guide(arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Return the guide payload. Kept here so every server shares one copy."""
    mode = (arguments or {}).get("mode") or "tour"
    if mode == "whats_new":
        return {"mode": "whats_new", "content": _changelog_text()}
    return {"mode": "tour", "content": _TOUR}


# Handlers are module-level rather than closures inside `register_surface` so
# they can be called directly in tests and reused by the merged server.

async def list_resources() -> List[Resource]:
    return RESOURCES


async def read_resource(uri) -> str:
    key = str(uri)
    if key == DOCS_URI:
        return DOCS_LONG_FORM
    if key == CHANGELOG_URI:
        return _changelog_text()
    raise ValueError(f"Unknown resource: {key}")


async def list_prompts() -> List[Prompt]:
    return [
        Prompt(
            name=name,
            title=spec["title"],
            description=spec["description"],
            arguments=[],
        )
        for name, spec in _PROMPTS.items()
    ]


async def get_prompt(name: str, arguments: Optional[Dict[str, str]] = None) -> GetPromptResult:
    spec = _PROMPTS.get(name)
    if spec is None:
        raise ValueError(f"Unknown prompt: {name}")
    return GetPromptResult(
        description=spec["description"],
        messages=[
            PromptMessage(
                role="user",
                content=TextContent(type="text", text=spec["text"]),
            )
        ],
    )


def register_surface(server) -> None:
    """Attach the resource and prompt handlers to `server`.

    Registering a handler is what advertises the capability in `initialize`, so
    this must run at import time on every server that should expose them.
    """
    server.list_resources()(list_resources)
    server.read_resource()(read_resource)
    server.list_prompts()(list_prompts)
    server.get_prompt()(get_prompt)
