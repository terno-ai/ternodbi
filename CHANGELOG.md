<!--
  Served live as the ternodbi://changelog MCP resource (src/terno_dbi/mcp/surface.py) —
  an agent reads this to decide whether a capability exists yet, not a person
  skimming release notes. Keep entries factual, specific, and short: what broke,
  what changed, why it matters to a caller. No adjectives, no "excited to
  announce," nothing that isn't true of the code as of this version.

  Entries begin at 0.1.33; everything earlier is in git history only.
-->

# Changelog

## 0.1.39, 0.1.40 and 0.1.41

- New tool `connect_datasource`: returns a link to add a database in Terno
  instead of taking `connection_str` in chat. `add_datasource` and
  `validate_connection` are now withheld from every OAuth grant for the same
  reason — a credential must never pass through a conversation.
- Fixed: a demoted admin kept write access until reconnecting. Admin status is
  now re-checked on every call, not just at grant time.

## 0.1.38

Version bump only.

## 0.1.37

- Consent screen redesigned to match the Terno app: organisation, read/write
  access, and a notice when write will be withheld.

## 0.1.36

- Fixed: write access could never be granted. The check read the wrong group
  field, so `admin:write`/`admin:sync` were stripped from every token.

## 0.1.35

- Fixed: 0.1.34 could not be installed fresh (`mcp` dependency uncapped).
- Tools carry titles, behaviour annotations, and output schemas.
- Removed the stale `find_similar_examples` tool.

## 0.1.34

- Connections are now closed on every query-execution and validation path.

## 0.1.33

- `ScopedDataSourceFilter` restricts datasource visibility in the memory admin.

## Before 0.1.33

Not tracked here — see `git log`. Notable prior work: persistent scoped agent
memory (CRUD + versioning), memory import/export, and organisation-prompt
management (search, targeted edit, client support).
