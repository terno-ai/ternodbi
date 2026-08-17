"""Server `instructions` blobs, and the long-form text they no longer carry.

Claude Code hard-truncates server `instructions` at 2,049 characters, and OpenAI
advises that what matters lands in the first ~512. Both blobs below are written
to that budget: the opening paragraph is the contract, everything after it is
useful but survivable if a client trims it.

`DOCS_LONG_FORM` holds the material moved out of the blobs. It is not dead text
— it is the payload for the `ternodbi://docs` resource (Phase 1.3), which is
fetched on demand and is not subject to the truncation cap. Keep it in sync when
the guidance changes; it is the only remaining copy of the org_prompt-vs-memory
reasoning.

A length test guards the cap: tests/test_instruction_length.py.
"""

INSTRUCTIONS_CHAR_CAP = 2049

QUERY_INSTRUCTIONS = (
    "Read-only SQL access to this organisation's configured databases, and to "
    "the durable memory recorded about them.\n\n"
    "Canonical flow: list_datasources, then list_tables / list_table_columns "
    "for schema, then execute_query (use public names) or get_sample_data.\n\n"
    "Before answering any schema, join, or business-rule question you cannot "
    "verify from the schema alone, search memory: grep_memory or "
    "list_memories, then get_memory for the full entry. An index description "
    "is a hook, not the fact — never rely on one without reading the entry.\n\n"
    "Memory here is shared, not yours: it holds facts recorded by every agent "
    "that worked with this data before. If you keep your own memory as well, do "
    "not let facts about this data live only there — other agents attached to "
    "this server, including ones with no memory of their own, can only benefit "
    "from a fact if it is recorded here.\n\n"
    "get_org_prompt returns this organisation's system-prompt addendum plus its "
    "content_hash; grep_org_prompt searches it. Both are read-only here — "
    "writing to the org prompt is an admin operation on the paired admin "
    "server. org_prompt and memory are not interchangeable: org_prompt is "
    "injected into every request, memory is pulled on demand, and the same rule "
    "must live in exactly one of them. Read ternodbi://docs for the full "
    "guidance on which to use."
)

ADMIN_INSTRUCTIONS = (
    "Write access to schema metadata (rename tables/columns, edit "
    "descriptions, add/sync/delete datasources), to durable shared memory, and "
    "to this organisation's system-prompt addendum.\n\n"
    "Two rules that are enforced server-side, not advisory:\n"
    "1. Read before write. edit_memory, edit_org_prompt, and any save_memory "
    "or update_org_prompt that replaces existing content require "
    "expected_hash from a get_memory / get_org_prompt call made just before.\n"
    "2. One fact per memory. Prefer edit_memory over a replacing save_memory — "
    "it preserves the [[name]] links other memories point at it with.\n\n"
    "store='org' shares a memory with every agent working on this "
    "organisation's data; store='user' (the default) is private to you. Prefer "
    "'org' for facts that would help any agent querying this data, rather than "
    "facts about your own preferences or this session.\n\n"
    "org_prompt vs memory — decide by REACH, not importance. org_prompt is "
    "injected into every request for every user, so it costs tokens forever; "
    "put only always-apply directives there (terminology, default units, "
    "filters that always apply). Everything else — schema quirks, join paths, "
    "domain facts, however important they feel — goes in memory, which is "
    "unbounded and costs nothing until read. When unsure, choose memory. Never "
    "keep the same rule in both places; duplicated rules drift apart silently. "
    "Because org_prompt changes behaviour for every user in the organisation, "
    "show the user the exact wording before writing it.\n\n"
    "Full guidance, including the promotion checklist: ternodbi://docs."
)


MERGED_INSTRUCTIONS = (
    "SQL access to this organisation's databases, the durable memory recorded "
    "about them, and their schema metadata.\n\n"
    "Read flow: list_datasources, then list_tables / list_table_columns, then "
    "execute_query (SELECT only, public names) or get_sample_data.\n\n"
    "Before answering any schema, join, or business-rule question you cannot "
    "verify from the schema alone, search memory: grep_memory or list_memories, "
    "then get_memory for the full entry. An index description is a hook, not the "
    "fact. Memory is shared — it holds what every agent before you learned about "
    "this data.\n\n"
    "Writes, where your access allows them: schema descriptions and names, "
    "memory, and the organisation prompt. **You only see the tools you may "
    "use** — if a write tool is not listed, you do not have that access, so say "
    "so rather than attempting the call.\n\n"
    "Two server-enforced rules:\n"
    "1. Read before write. edit_memory, edit_org_prompt, and any save_memory or "
    "update_org_prompt replacing existing content require expected_hash from a "
    "read made just before.\n"
    "2. One fact per memory. Prefer edit_memory over a replacing save_memory — "
    "it preserves the [[name]] links other memories point at it with.\n\n"
    "store='org' shares a memory with every agent working on this "
    "organisation's data; store='user' (the default) is private to you.\n\n"
    "org_prompt vs memory — decide by REACH, not importance. org_prompt is "
    "injected into every request for every user, so it costs tokens forever: put "
    "only always-apply directives there. Everything else goes in memory, which "
    "costs nothing until read. When unsure, choose memory, and never keep the "
    "same rule in both. Because org_prompt changes behaviour for everyone, show "
    "the user the exact wording first.\n\n"
    "Full guidance: ternodbi://docs. Orientation: terno_guide."
)


# ---------------------------------------------------------------------------
# Moved out of the blobs above. Served as `ternodbi://docs` in Phase 1.3.
# ---------------------------------------------------------------------------

DOCS_LONG_FORM = """\
# TernoDBI connector guide

## Reading schema

`list_datasources` -> `list_tables` -> `list_table_columns` -> `execute_query`.
Tables and columns carry *public names* alongside their physical names; write
SQL against the public names. `get_sample_data` previews rows for one table
without writing a query.

`execute_query` is read-only by construction, not by convention: every
statement is parsed and must be a single SELECT, with any DML, DDL, `COMMAND`,
`COMMIT`, `DROP`, or `TRUNCATE` node anywhere in the tree — including inside a
CTE or subquery — rejected before execution. There is no privilege level or
datasource setting that bypasses this.

## Memory

One fact per memory. A memory has a `name` (kebab-case slug), a one-line
`description` that appears in the index, and a `content` body.

The index is a set of hooks, not facts. `list_memories` and `grep_memory`
return descriptions only; call `get_memory` before relying on anything. A
description that sounds like it confirms your assumption is the most common way
to get this wrong.

### Stores

- `store='user'` (default) — private to the calling identity.
- `store='org'` — shared with every agent working on this organisation's data.

Prefer `org` for anything about the data itself: join paths, schema quirks,
business rules, known-bad columns. Reserve `user` for preferences and
session-specific scratch notes.

Shared does not mean injected. Even an org memory is fetched on demand — an
agent that never searches memory never sees it. This is the key difference from
`org_prompt`.

### Read before write

`edit_memory` always requires `expected_hash`. So does `save_memory` when it
would replace an existing memory. Get the hash from a `get_memory` call made
immediately before the write — not from earlier in the conversation, since
another agent may have written in between. The server rejects a stale hash
rather than clobbering.

Prefer `edit_memory` to a replacing `save_memory`. A full replace silently
drops `[[name]]` links that other memories use to point at this one, and drops
any part of the body you did not know was there.

## org_prompt vs memory

Decide by **reach**, not by how important the fact feels.

| | org_prompt | memory |
|---|---|---|
| delivery | injected into every request, for every user | fetched when an agent looks for it |
| cost | paid on every future request, forever | nothing until read |
| size | keep to a few hundred words | unbounded |

So `org_prompt` gets only the directives that must shape *every* query:
terminology the organisation uses, default units and formatting, filters that
always apply (for example "exclude test accounts unless asked").

Everything else goes in memory. Schema quirks, join paths, domain facts, and
caveats all feel important — importance is not the test. The test is whether
every single request needs it.

When unsure, choose memory. An unread memory costs nothing; an unnecessary
`org_prompt` line is paid forever.

### Never both

A rule duplicated across `org_prompt` and memory will drift apart, and nothing
will flag it. Keep exactly one copy:

- Before writing to `org_prompt`: `grep_memory` for the rule, and
  `delete_memory` whatever you are promoting into the prompt.
- Before saving a memory that reads like an always-apply rule:
  `get_org_prompt` to confirm it is not already there.

### Writing org_prompt

Short imperative bullets, one directive per line. No prose, no explanation of
why a rule exists — that reasoning belongs in a memory if it is worth keeping
at all.

`update_org_prompt` replaces the whole prompt and is capped per call;
`edit_org_prompt` is uncapped and surgical. For any revision to an existing
prompt, use `edit_org_prompt`. A full replace risks dropping a directive you
did not know mattered.

Because this text changes behaviour for every user in the organisation, show
the requesting user the exact wording before writing it.

## When write tools disappear

Write access needs two things at once: the connection must have been granted
write scopes at consent, and the user must be an **Org Admin** in this
organisation *right now*. The first is fixed when the connection is made; the
second is re-checked on every request. So the tools available to you can change
mid-conversation without the user doing anything to the connection.

| what changed | effect |
|---|---|
| user loses Org Admin | write tools disappear immediately; read is unaffected |
| user gains Org Admin | **nothing changes until they reconnect** |

The second row is the one that looks like a bug and is not. A connection minted
while the user was not an admin carries no write scopes, and the consent screen
told them so — "the connection will be read-only". Granting the role later does
not silently widen a grant the user already approved.

So if a user says they have just been made an administrator and writes are still
refused, do not retry and do not report a fault: tell them to disconnect and
reconnect Terno in their client. If instead a write tool refuses with *"you are
no longer an administrator"*, reconnecting will **not** help — their role was
removed, and only an administrator can restore it in Terno.
"""
