# Security Policy

TernoDBI sits between AI agents and production data, so we take security
reports seriously and appreciate responsible disclosure.

## Reporting a vulnerability

**Please do not open a public issue, discussion or pull request for a
vulnerability.**

Report it privately through GitHub:
[**Report a vulnerability**](https://github.com/terno-ai/ternodbi/security/advisories/new)
(the *Security* tab of this repository → *Report a vulnerability*).

Please include:

* the affected version or commit,
* what an attacker can do, and under which configuration,
* steps to reproduce, ideally a minimal proof of concept,
* any suggested fix.

We will acknowledge your report, keep you updated while we investigate, and
credit you in the advisory unless you prefer otherwise. Please give us a
reasonable time to release a fix before disclosing publicly.

## Scope

Especially relevant reports include:

* **SQLShield bypasses** — executing anything other than a `SELECT`, or reading
  tables, columns or rows that a role's private-table, private-column or
  row-filter rules should hide.
* **Authentication and authorisation** — using a service token, OAuth token or
  session beyond its organisation, datasources or scopes; reaching write tools
  without the required scope and Org Admin role; connector writes on an account
  without `writes_enabled`.
* **OAuth** — flaws in client registration, the consent screen, PKCE, token
  issuance, refresh or revocation.
* **Secrets** — exposure of datasource credentials, connector OAuth tokens or
  encryption keys, including through logs, errors or MCP responses.
* **Cross-organisation access** of any kind.

Out of scope: findings that require `DBI_DEBUG=True`, the default `admin` /
`admin` account, or the development secret and encryption key in a deployment
— these are documented as development-only (see the
[Configuration](README.md#configuration) section).

## Supported versions

Security fixes are released for the latest published version of
[`terno-dbi`](https://pypi.org/project/terno-dbi/). Please upgrade before
reporting, if you can.
