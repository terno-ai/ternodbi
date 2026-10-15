# Contributing to TernoDBI

Thanks for helping. Bug reports, new connectors, documentation and fixes are
all welcome.

**Security issues:** please don't open a public issue — follow
[SECURITY.md](SECURITY.md) instead.

## Development setup

Requires Python 3.10+.

```bash
git clone https://github.com/terno-ai/ternodbi.git
cd ternodbi
python -m venv .venv && source .venv/bin/activate
pip install -e '.[dev,http]'
```

Run the server locally with `ternodbi start` (REST API and admin panel), or
with `uvicorn terno_dbi.server.asgi:application --port 8376` to include the
MCP endpoints over HTTP. Development mode needs no other configuration.

## Tests and linting

```bash
pytest tests/unit                  # what CI runs (coverage must stay ≥ 80%)
pytest tests/unit -k shield        # a subset
ruff check src tests
```

Tests use the standalone settings (`terno_dbi.server.settings`) and SQLite, so
no database server is needed. Tests that call third-party APIs mock them; never
commit real credentials, tokens or customer data, including in fixtures.

## Pull requests

1. Open an issue first for larger changes, so we can agree on the approach.
2. Branch from `main` (`git checkout -b feat/your-feature`).
3. Add tests with your change. **Security-sensitive code needs tests that prove
   the restriction holds**, not only that the happy path works — SQLShield,
   tokens and scopes, OAuth, private tables and columns, row filters,
   organisation isolation and connector writes.
4. Update the docs and [CHANGELOG.md](CHANGELOG.md) when behaviour changes.
5. Keep pull requests focused; describe what changed and why.

## Things to know

### The stable MCP surface
`tests/unit/mcp/test_stable_manifest.py` pins everything a client sees on the
stable `/mcp` endpoint — tools, prompts, resources, instructions and OAuth
discovery documents. If your change trips it:

* **New tool?** Release it on the `preview` channel first (see
  `src/terno_dbi/mcp/profiles.py` and the [MCP guide](docs/mcp-guide.md)).
* **Intended change to `/mcp`?** Regenerate the fixture as described in
  `tests/unit/mcp/manifest_snapshot.py`, and say so in the pull request.

Every MCP tool needs an entry in `src/terno_dbi/mcp/tool_meta.py` — a title,
`readOnlyHint` / `destructiveHint` annotations and an output schema — or listing
it fails.

### Adding an API connector
1. Implement the connector in `src/terno_dbi/connectors/api/sources/` and
   register it in that package's `__init__.py`.
2. Declare it as a `ConnectorSpec` in `src/terno_dbi/catalog/declarations.py`
   (catalog rows are generated from these; don't create them by hand).
3. If it uses a new OAuth provider, add it to
   `src/terno_dbi/connectors/api/auth/providers.py` with its client-ID and
   secret environment variables, and list them in the README.
4. Add tests next to the existing ones in `tests/unit/connectors/api/`.

Write actions must be explicit, separately annotated as destructive, gated by
the `connector:write` scope, and support `dry_run`. Never make a dry run the
default, and prefer reversible actions (pause, hide) to deletion.

### Code style
`ruff` with the settings in `pyproject.toml`. Match the surrounding code; write
comments that explain *why*, not *what*.

## License

By contributing, you agree that your contributions are licensed under the
[Apache 2.0 License](LICENSE).
