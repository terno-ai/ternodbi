<!--
  Served live as the ternodbi://changelog MCP resource (src/terno_dbi/mcp/surface.py) —
  an agent reads this to decide whether a capability exists yet, not a person
  skimming release notes. Keep entries factual, specific, and short: what broke,
  what changed, why it matters to a caller. No adjectives, no "excited to
  announce," nothing that isn't true of the code as of this version.

  Entries begin at 0.1.33; everything earlier is in git history only.
-->

# Changelog

## 1.0.37

- Admin checks are per organisation. They read the permissions on the user's
  groups in that organisation (`OrganisationUser.groups`) instead of the global
  `user.groups` "Org Admin" group, which made an admin of one organisation an
  admin of every organisation they joined. See `terno_dbi.core.org_permissions`.
  - Connect and manage connector sources: the owner, a superuser, or
    `core.add_datasource`.
  - Write access over MCP/OAuth: a superuser or `core.use_dbi_admin_mode`
    (new permission).
- The connector account allowlist is matched against the caller's groups in
  the datasource's organisation, not their global groups.
- Migration: each member of the global "Org Admin" group (or
  `TERNO_ORG_ADMIN_GROUP`) gets a "<org_id>:DBI Admin" group with both
  permissions in every organisation they belong to, so existing admins stay
  admins. The global group is left as it was and no longer grants anything.
  `TERNO_ORG_ADMIN_GROUP` is read only by this migration.
- `bootstrap_local` puts the admin group on the membership.

## Unreleased

- Fixed: Shopify webhooks sent to `/connectors/shopify/webhooks` (no trailing
  slash, which is how Shopify stores the URI) got a 403 instead of reaching the
  handler. The slashless path is now routed too.
- Shopify App Store install. `GET /connectors/shopify/install/` is the app URL:
  it verifies Shopify's query signature, a timestamp under one hour old and a
  `*.myshopify.com` shop, then sends the merchant straight to Shopify's consent
  screen, before any Terno sign-in (App Store requirement 2.3.2). The callback
  holds the token in the new `ShopifyPendingInstall` model (migration 0039) for
  30 minutes, bound to the browser by a cookie, and sends the merchant to sign
  in with `next=/connect?connector=shopify&shop=…&install=1`. That `/connect`
  checks admin rights as usual and attaches the store to the organisation. The
  merchant never types the store domain. `TERNO_SHOPIFY_INSTALL_RETURN_TO`
  optionally sets where they land afterwards (default `/`).
- `complete_authorization` now refuses an OAuth state with no organisation and
  no datasource, so no datasource can be created outside an organisation.
- `POST /connectors/shopify/webhooks/` takes signed Shopify webhooks; unsigned
  requests get a 401. `app/uninstalled` removes the store's tokens and any
  unclaimed install. `shop/redact` deletes the store's connections and any
  unclaimed install. `customers/redact` deletes the store's stored query
  results. `customers/data_request` is logged for manual follow-up.

## 1.0.1

- OAuth issuer moved to `https://mcp.terno.ai`. Client registration and token
  exchange are served there; authorization stays on `https://app.terno.ai`.
  Clients registered against the old issuer must register again and users must
  re-consent.
- Fixed: registration was refused for a client whose callback used a
  private-use URI scheme without a dot in the scheme, such as Cursor's
  `cursor://anysphere.cursor-mcp/oauth/callback`. One rejected entry failed the
  whole request, so the valid https and loopback callbacks sent alongside it
  were discarded too. Redirect URIs are now accepted on structure — a scheme
  plus a host — rather than on the shape of the scheme name.

## 0.1.39, 0.1.40 , 0.1.41 and 0.1.42

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
