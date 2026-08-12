<!--
  Served live as the ternodbi://changelog MCP resource (src/terno_dbi/mcp/surface.py) —
  an agent reads this to decide whether a capability exists yet, not a person
  skimming release notes. Keep entries factual, specific, and short: what broke,
  what changed, why it matters to a caller. No adjectives, no "excited to
  announce," nothing that isn't true of the code as of this version.

  Entries begin at 0.1.33; everything earlier is in git history only.
-->

# Changelog

## 0.1.35

- **Fixed: `terno-dbi` 0.1.34 could not be installed fresh.** The `mcp`
  dependency was uncapped, so a new install resolved to `mcp` 2.0, whose
  breaking changes made the server fail on import with
  `'Server' object has no attribute 'list_tools'`. Now `mcp>=1.25.0,<2.0`.
- All 24 tools carry a display `title`, behaviour `annotations`
  (`readOnlyHint` / `destructiveHint` / `idempotentHint` / `openWorldHint`), and
  an `outputSchema`. Tool responses now include `structuredContent` alongside
  the text form.
- Server `instructions` rewritten to fit the 2,049-character client truncation
  limit. The admin server previously exceeded it, so its guidance on
  `org_prompt` versus memory was being silently cut and never reached the model.
- **Removed the `find_similar_examples` tool.** Stale. Its `PromptExample` and
  Milvus backing remain for now and are scheduled for removal.
- `__version__` is read from installed package metadata; it had reported
  `0.1.0` through the 0.1.34 release.

## 0.1.34

- Connectors are now closed on every query-execution and validation path,
  fixing a connection leak.
- Error handling in API response construction made consistent.

## 0.1.33

- `ScopedDataSourceFilter` restricts datasource visibility in the memory admin.

## Before 0.1.33

Not tracked here — see `git log`. Notable prior work: persistent scoped agent
memory (CRUD + versioning), memory import/export, and organisation-prompt
management (search, targeted edit, client support).
