"""Phase 4: scopes, discovery documents, and Dynamic Client Registration.

The DCR tests are the security-relevant ones. That endpoint is unauthenticated
by design, so its validation is the only thing standing between a stranger and
a client record with an attacker-controlled redirect URI.
"""

import json
import pathlib

import pytest

from terno_dbi.oauth.dcr import (
    InvalidRegistration,
    registration_response,
    validate_redirect_uri,
    validate_registration,
)
from terno_dbi.oauth.metadata import (
    authorization_server_metadata,
    protected_resource_metadata,
)
from terno_dbi.oauth.scopes import (
    ADMIN_SYNC,
    ADMIN_WRITE,
    ALL_SCOPES,
    DEFAULT_SCOPES,
    QUERY_EXECUTE,
    QUERY_READ,
    SCOPE_DESCRIPTIONS,
    TOOL_SCOPES,
    UnknownScope,
    granted_scopes,
    parse_scope_string,
    tool_is_allowed,
)

RESOURCE = "https://mcp.terno.ai"
ISSUER = "https://app.terno.ai"


# ----------------------------------------------------------------- scopes

def test_only_existing_scope_names_are_declared():
    """`ternodbi:read`/`ternodbi:write` were never real. The scope system was
    already built with these names and is enforced across 11 views."""
    assert ALL_SCOPES == {
        "query:read", "query:execute", "admin:read", "admin:write", "admin:sync",
    }
    assert not any(s.startswith("ternodbi:") for s in ALL_SCOPES)


def test_every_scope_has_a_consent_description():
    for scope in ALL_SCOPES:
        assert SCOPE_DESCRIPTIONS[scope].strip()


def test_default_grant_is_read_only():
    """Decision 5: write is a deliberate second step, not something granted by
    clicking through the screen that connects you."""
    assert DEFAULT_SCOPES == {QUERY_READ, QUERY_EXECUTE}
    assert not (DEFAULT_SCOPES & {ADMIN_WRITE, ADMIN_SYNC})


def test_parse_scope_string_defaults_when_absent():
    assert parse_scope_string(None) == DEFAULT_SCOPES
    assert parse_scope_string("  ") == DEFAULT_SCOPES


def test_parse_scope_string_rejects_unknown_rather_than_dropping():
    """Silently narrowing produces a token that looks accepted and then cannot
    do what was asked, surfacing later as an unexplained empty tool list."""
    with pytest.raises(UnknownScope, match="ternodbi:write"):
        parse_scope_string("query:read ternodbi:write")


def test_granted_scopes_strips_write_for_a_non_admin():
    """Consent alone must not escalate: the client asked for write, but the user
    is not an org admin."""
    requested = {QUERY_READ, ADMIN_WRITE, ADMIN_SYNC}
    assert granted_scopes(requested, can_write=True) == requested
    assert granted_scopes(requested, can_write=False) == {QUERY_READ}


def test_tool_scope_mapping_matches_the_view_decorators():
    """Transcribed from @require_scope, not from what the tool sounds like."""
    assert TOOL_SCOPES["sync_metadata"] == ADMIN_SYNC
    assert TOOL_SCOPES["get_table_info"] == "admin:read"
    assert TOOL_SCOPES["execute_query"] == QUERY_EXECUTE
    # The counterintuitive one: read-only by annotation, admin:write by endpoint.
    assert TOOL_SCOPES["validate_connection"] == ADMIN_WRITE


def test_unmapped_tool_is_denied_not_allowed():
    """A new tool forgotten in TOOL_SCOPES must fail closed."""
    assert not tool_is_allowed("some_future_tool", ALL_SCOPES)


def test_guide_needs_no_scope():
    assert tool_is_allowed("terno_guide", frozenset())


# --------------------------------------------------------------- metadata

def test_protected_resource_metadata_shape():
    doc = protected_resource_metadata(RESOURCE, ISSUER)
    assert doc["resource"] == "https://mcp.terno.ai/mcp"
    assert doc["authorization_servers"] == [ISSUER]
    assert doc["bearer_methods_supported"] == ["header"]
    assert set(doc["scopes_supported"]) == ALL_SCOPES


def test_the_401_pointer_resolves_to_this_document():
    """`http_app` puts this path in WWW-Authenticate. If the two disagree the
    client follows the pointer to a 404 and shows a bare auth error."""
    from terno_dbi.mcp.http_app import PROTECTED_RESOURCE_METADATA_PATH
    from terno_dbi.oauth import urls as oauth_urls

    served = {p.pattern._route for p in oauth_urls.urlpatterns}
    assert PROTECTED_RESOURCE_METADATA_PATH.lstrip("/") in served


def test_authorization_server_metadata_shape():
    doc = authorization_server_metadata(ISSUER)
    assert doc["issuer"] == ISSUER
    assert doc["registration_endpoint"] == f"{ISSUER}/oauth/register"
    assert doc["grant_types_supported"] == ["authorization_code", "refresh_token"]
    assert doc["response_types_supported"] == ["code"]


def test_pkce_is_s256_only():
    """`plain` gives none of the protection PKCE exists for."""
    doc = authorization_server_metadata(ISSUER)
    assert doc["code_challenge_methods_supported"] == ["S256"]


def test_public_clients_are_supported():
    """Claude Code runs on a user's machine and cannot hold a client secret."""
    doc = authorization_server_metadata(ISSUER)
    assert "none" in doc["token_endpoint_auth_methods_supported"]


def test_metadata_urls_have_no_double_slashes():
    doc = authorization_server_metadata(ISSUER + "/")
    for key, value in doc.items():
        if isinstance(value, str) and value.startswith("http"):
            assert "//" not in value[len("https://"):], f"{key} -> {value}"


# -------------------------------------------------------------------- DCR

def test_localhost_callback_on_any_port_is_accepted():
    """Claude Code binds a random port unless given --callback-port. Rejecting
    these would make DCR pointless."""
    for uri in (
        "http://localhost:54321/callback",
        "http://localhost:1/callback",
        "http://127.0.0.1:8080/callback",
        "http://[::1]:9999/callback",
    ):
        assert validate_redirect_uri(uri) == uri


def test_plaintext_http_off_loopback_is_rejected():
    """An authorization code must never travel unencrypted."""
    with pytest.raises(InvalidRegistration, match="loopback"):
        validate_redirect_uri("http://evil.example.com/callback")


def test_https_redirect_is_accepted():
    assert validate_redirect_uri("https://claude.ai/api/mcp/auth_callback")


def test_redirect_uri_with_fragment_is_rejected():
    with pytest.raises(InvalidRegistration, match="fragment"):
        validate_redirect_uri("https://claude.ai/cb#token")


@pytest.mark.parametrize("uri", ["javascript:alert(1)", "data:text/html,x", "", "   "])
def test_dangerous_or_empty_schemes_are_rejected(uri):
    with pytest.raises(InvalidRegistration):
        validate_redirect_uri(uri)


def test_minimal_claude_registration():
    reg = validate_registration({
        "client_name": "Claude",
        "redirect_uris": ["http://localhost:54321/callback"],
        "grant_types": ["authorization_code", "refresh_token"],
        "token_endpoint_auth_method": "none",
    })
    assert reg.client_name == "Claude"
    assert reg.is_public


def test_registration_requires_redirect_uris():
    with pytest.raises(InvalidRegistration, match="redirect_uris"):
        validate_registration({"client_name": "Claude"})


def test_registration_requires_authorization_code():
    with pytest.raises(InvalidRegistration, match="authorization_code"):
        validate_registration({
            "redirect_uris": ["https://claude.ai/cb"],
            "grant_types": ["refresh_token"],
        })


def test_implicit_grant_is_refused():
    with pytest.raises(InvalidRegistration, match="Unsupported grant_types"):
        validate_registration({
            "redirect_uris": ["https://claude.ai/cb"],
            "grant_types": ["authorization_code", "implicit"],
        })


def test_registration_caps_redirect_uri_count():
    with pytest.raises(InvalidRegistration, match="At most"):
        validate_registration({
            "redirect_uris": [f"https://claude.ai/cb{i}" for i in range(50)],
        })


def test_registration_rejects_unknown_scope():
    with pytest.raises(UnknownScope):
        validate_registration({
            "redirect_uris": ["https://claude.ai/cb"],
            "scope": "query:read ternodbi:write",
        })


def test_public_client_response_carries_no_secret():
    reg = validate_registration({
        "redirect_uris": ["http://localhost:1234/callback"],
        "token_endpoint_auth_method": "none",
    })
    body = registration_response(reg, "abc123", None, 1700000000)
    assert body["client_id"] == "abc123"
    assert "client_secret" not in body
    assert body["token_endpoint_auth_method"] == "none"
    json.dumps(body)  # must be serialisable as-is


def test_confidential_client_response_carries_a_secret():
    reg = validate_registration({
        "redirect_uris": ["https://claude.ai/cb"],
        "token_endpoint_auth_method": "client_secret_post",
    })
    body = registration_response(reg, "abc", "shhh", 1700000000)
    assert body["client_secret"] == "shhh"
    assert body["client_secret_expires_at"] == 0  # 0 = never, per RFC 7591


# ------------------------------------------------------- minting (DB-backed)

def test_access_token_is_a_valid_service_token_key():
    """The OAuth access token *is* the ServiceToken key, so it must satisfy the
    prefix `verify_token` requires — otherwise every bearer would be rejected
    before the hash was even looked up."""
    from terno_dbi.oauth.minting import OAUTH_KEY_PREFIX, generate_oauth_access_token

    token = generate_oauth_access_token()
    assert token.startswith(OAUTH_KEY_PREFIX)
    assert token.startswith("dbi_")          # what verify_token checks
    assert len(token) > 40


def test_access_tokens_are_unique():
    from terno_dbi.oauth.minting import generate_oauth_access_token

    assert len({generate_oauth_access_token() for _ in range(500)}) == 500


def test_key_hash_matches_what_verify_token_computes():
    """If these two ever disagree, every OAuth bearer silently stops resolving."""
    import hashlib

    from terno_dbi.oauth.minting import key_hash

    sample = "dbi_oauth_example"
    assert key_hash(sample) == hashlib.sha256(sample.encode()).hexdigest()


@pytest.fixture
def org_fixture(db):
    from django.contrib.auth.models import Group, User

    from terno_dbi.core.models import CoreOrganisation, OrganisationUser

    owner = User.objects.create(username="owner-fixture")
    org = CoreOrganisation.objects.create(
        name="Acme", subdomain="acme-fixture", owner=owner
    )
    admin_group = Group.objects.create(name="Org Admin")
    analysts = Group.objects.create(name="Analysts")

    # Mirrors production wiring, which this fixture originally got wrong: terno-ai
    # puts "Org Admin" on `User.groups` (`create_org_admin_group` does
    # `owner.groups.add(...)`), while `OrganisationUser.groups` carries the
    # per-org table-scoping groups. Putting the admin group on the membership
    # here made every write test pass against a shape production never produces.
    admin_user = User.objects.create(username="ada")
    admin_user.groups.set([admin_group])
    membership = OrganisationUser.objects.create(user=admin_user, organisation=org)
    membership.groups.set([analysts])

    plain_user = User.objects.create(username="bob")
    plain_membership = OrganisationUser.objects.create(user=plain_user, organisation=org)
    plain_membership.groups.set([analysts])

    return {"org": org, "admin": admin_user, "plain": plain_user}


@pytest.mark.django_db
def test_minted_token_resolves_through_verify_token(org_fixture):
    from terno_dbi.oauth.minting import generate_oauth_access_token, mint_service_token_for_key
    from terno_dbi.services.auth import verify_token

    key = generate_oauth_access_token()
    mint_service_token_for_key(
        key, org_fixture["admin"], org_fixture["org"],
        ["query:read", "query:execute", "admin:write"],
    )

    token = verify_token(key)
    assert token is not None
    assert token.token_type == "oauth"
    assert token.organisation == org_fixture["org"]


@pytest.mark.django_db
def test_groups_are_copied_for_read_parity(org_fixture):
    """Not a nicety: group filtering is additive, so a token with no groups
    silently sees fewer tables than the same user sees in the web app."""
    from terno_dbi.oauth.minting import generate_oauth_access_token, mint_service_token_for_key

    key = generate_oauth_access_token()
    token = mint_service_token_for_key(
        key, org_fixture["admin"], org_fixture["org"], ["query:read"]
    )
    # The per-org scoping groups, which is what `OrganisationUser.groups` holds.
    # "Org Admin" is not among them by design — it lives on `User.groups`.
    assert sorted(g.name for g in token.groups.all()) == ["Analysts"]


@pytest.mark.django_db
def test_consent_alone_cannot_grant_write(org_fixture):
    """A non-admin whose client requested write must not receive write scopes,
    however enthusiastically they clicked allow."""
    from terno_dbi.oauth.minting import (
        generate_oauth_access_token,
        mint_service_token_for_key,
        token_grant_summary,
    )

    key = generate_oauth_access_token()
    token = mint_service_token_for_key(
        key, org_fixture["plain"], org_fixture["org"],
        ["query:read", "query:execute", "admin:write", "admin:sync"],
    )
    assert set(token.scopes) == {"query:read", "query:execute"}
    assert token_grant_summary(token)["can_write"] is False


@pytest.mark.django_db
def test_org_admin_receives_write(org_fixture):
    from terno_dbi.oauth.minting import (
        generate_oauth_access_token,
        mint_service_token_for_key,
        token_grant_summary,
    )

    token = mint_service_token_for_key(
        generate_oauth_access_token(), org_fixture["admin"], org_fixture["org"],
        ["query:read", "admin:write", "admin:sync"],
    )
    assert token_grant_summary(token)["can_write"] is True


@pytest.mark.django_db
def test_minting_refuses_a_non_member(org_fixture):
    from django.contrib.auth.models import User

    from terno_dbi.oauth.minting import generate_oauth_access_token, mint_service_token_for_key

    stranger = User.objects.create(username="stranger")
    with pytest.raises(PermissionError, match="not a member"):
        mint_service_token_for_key(
            generate_oauth_access_token(), stranger, org_fixture["org"], ["query:read"]
        )


@pytest.mark.django_db
def test_revocation_stops_the_bearer_working(org_fixture):
    """The ServiceToken is what /mcp checks. If revoking the OAuth token left it
    active, a disconnected connector would keep working."""
    from terno_dbi.oauth.minting import (
        deactivate_service_token_for_key,
        generate_oauth_access_token,
        mint_service_token_for_key,
    )
    from terno_dbi.core.models import ServiceToken
    from terno_dbi.services.auth import verify_token

    key = generate_oauth_access_token()
    mint_service_token_for_key(key, org_fixture["admin"], org_fixture["org"], ["query:read"])
    assert verify_token(key) is not None

    assert deactivate_service_token_for_key(key) == 1
    assert verify_token(key) is None
    # Kept, not deleted — the audit trail is why these rows exist.
    assert ServiceToken.objects.filter(key_hash__isnull=False).exists()


@pytest.mark.django_db
def test_resolve_organisation_without_membership_returns_none(db):
    from django.contrib.auth.models import User

    from terno_dbi.oauth.minting import resolve_organisation

    assert resolve_organisation(User.objects.create(username="orphan")) is None


# ------------------------------------------------------------ consent screen

def test_consent_view_tracks_whether_dot_is_installed():
    """The module must stay importable either way.

    Asserted against the app registry rather than a fixed expectation, because
    ternodbi's settings add `oauth2_provider` conditionally when it is
    importable — so a bare assertion of None passes or fails depending on
    whether the developer happens to have DOT installed.
    """
    from django.apps import apps

    from terno_dbi.oauth import views

    view = views.make_authorization_view()
    if apps.is_installed("oauth2_provider"):
        assert view is not None
        assert view.__name__ == "TernoAuthorizationView"
    else:
        assert view is None


def test_consent_template_ships_with_the_package():
    """It is loaded by app-directories lookup at runtime, so a packaging miss
    would silently fall back to DOT's default rather than error."""
    from pathlib import Path

    import terno_dbi.oauth as pkg

    template = (
        Path(pkg.__file__).parent / "templates" / "oauth2_provider" / "authorize.html"
    )
    assert template.is_file()
    body = template.read_text()
    assert "{% csrf_token %}" in body
    assert "read_scopes" in body and "write_scopes" in body


def test_consent_rows_split_read_from_write():
    """The screen separates them; one flat list makes 'run a query' and 'delete
    a database' read as the same kind of thing."""
    from terno_dbi.oauth.metadata import scope_consent_rows
    from terno_dbi.oauth.scopes import ALL_SCOPES, WRITE_SCOPES

    rows = scope_consent_rows(ALL_SCOPES)
    read = [r for r in rows if r["scope"] not in WRITE_SCOPES]
    write = [r for r in rows if r["scope"] in WRITE_SCOPES]

    assert {r["scope"] for r in write} == set(WRITE_SCOPES)
    assert read and all(r["description"] for r in read)


def test_consent_rows_ignore_unknown_scopes():
    from terno_dbi.oauth.metadata import scope_consent_rows

    assert scope_consent_rows({"query:read", "not:a:scope"}) == [
        {"scope": "query:read", "description": scope_consent_rows({"query:read"})[0]["description"]}
    ]


# ----------------------------------------------- signup mid-flow (provisioning)

def test_subdomain_handles_the_cases_that_used_to_fall_back():
    """The web provisioner discarded these and used a generic name. All three
    are reachable here: underscores, over-length, and mixed case."""
    from terno_dbi.oauth.provisioning import (
        MAX_SUBDOMAIN_LENGTH,
        SUBDOMAIN_REGEX,
        generate_subdomain,
    )

    free = lambda c: False  # noqa: E731 - nothing taken
    for seed in ("navin_bhagat", "Acme Corp", "a" * 60, "John.Doe", "_lead_"):
        result = generate_subdomain(seed, taken=free)
        assert SUBDOMAIN_REGEX.match(result), f"{seed!r} -> {result!r} is invalid"
        assert len(result) <= MAX_SUBDOMAIN_LENGTH


def test_subdomain_falls_back_for_non_latin_names():
    """Cyrillic slugifies to an empty string. Refusing to create the
    organisation would be worse than an unlovely subdomain."""
    from terno_dbi.oauth.provisioning import SUBDOMAIN_REGEX, generate_subdomain

    result = generate_subdomain("Аналитика", taken=lambda c: False)
    assert SUBDOMAIN_REGEX.match(result)
    assert result.startswith("org")


def test_subdomain_avoids_collisions_and_reserved_names():
    from terno_dbi.oauth.provisioning import generate_subdomain

    taken = {"acme", "acme1", "admin"}
    assert generate_subdomain("Acme", taken=lambda c: c in taken) == "acme2"
    # A reserved name is skipped even when nothing is taken.
    assert generate_subdomain("admin", taken=lambda c: False) != "admin"
    assert generate_subdomain("mcp", taken=lambda c: False) != "mcp"


def test_collision_suffix_stays_within_the_length_limit():
    """Appending a counter must not push a max-length slug over the cap."""
    from terno_dbi.oauth.provisioning import (
        MAX_SUBDOMAIN_LENGTH,
        SUBDOMAIN_REGEX,
        generate_subdomain,
    )

    seen = set()
    for _ in range(12):
        result = generate_subdomain("b" * 40, taken=lambda c: c in seen)
        assert len(result) <= MAX_SUBDOMAIN_LENGTH
        assert SUBDOMAIN_REGEX.match(result)
        assert result not in seen
        seen.add(result)


def test_org_name_prefers_a_human_label():
    from terno_dbi.oauth.provisioning import default_org_name

    class U:
        username = "ada"
        email = "ada@acme.com"

        def get_full_name(self):
            return "Ada Lovelace"

    assert default_org_name(U()) == "Ada Lovelace's Organisation"

    class NoName(U):
        def get_full_name(self):
            return ""

    assert default_org_name(NoName()) == "ada's Organisation"


@pytest.mark.django_db
def test_ensure_organisation_returns_none_without_a_provisioner(settings):
    """Fails closed: the grant is refused rather than issued unscoped."""
    from django.contrib.auth.models import User

    from terno_dbi.oauth.provisioning import ensure_organisation

    settings.TERNO_ORG_PROVISIONER = None
    user = User.objects.create(username="unprovisioned")
    assert ensure_organisation(user) is None


@pytest.mark.django_db
def test_ensure_organisation_uses_the_existing_org_and_does_not_provision(settings):
    from django.contrib.auth.models import User

    from terno_dbi.core.models import CoreOrganisation, OrganisationUser
    from terno_dbi.oauth.provisioning import ensure_organisation

    settings.TERNO_ORG_PROVISIONER = f"{__name__}._provision_double"

    owner = User.objects.create(username="ens-owner")
    org = CoreOrganisation.objects.create(name="Acme", subdomain="ens-acme", owner=owner)
    member = User.objects.create(username="ens-member")
    OrganisationUser.objects.create(user=member, organisation=org)

    before = CoreOrganisation.objects.count()
    assert ensure_organisation(member) == org
    # Asserted against the database rather than a call recorder: `import_string`
    # loads this module under a different name than pytest did, so the two hold
    # separate module globals and a recorder list would always look empty.
    assert CoreOrganisation.objects.count() == before, "provisioned despite an existing org"


def _provision_double(user):
    """Stands in for terno-ai's provisioner, which ternodbi cannot import."""
    from terno_dbi.core.models import CoreOrganisation
    from terno_dbi.oauth.provisioning import default_org_name, generate_subdomain

    return CoreOrganisation.objects.create(
        name=default_org_name(user),
        subdomain=generate_subdomain(user.username),
        owner=user,
        is_active=True,
    )


@pytest.mark.django_db
def test_a_new_user_gets_their_own_org_not_a_matching_one(settings):
    """The rule that matters: a matching email domain must NOT join an existing
    organisation. Doing so would hand a stranger that org's datasources."""
    from django.contrib.auth.models import User

    from terno_dbi.core.models import CoreOrganisation, OrganisationUser
    from terno_dbi.oauth.provisioning import ensure_organisation

    settings.TERNO_ORG_PROVISIONER = f"{__name__}._provision_double"

    boss = User.objects.create(username="boss", email="boss@acme.com")
    acme = CoreOrganisation.objects.create(
        name="Acme", subdomain="acme-existing", owner=boss
    )
    OrganisationUser.objects.create(user=boss, organisation=acme)

    newcomer = User.objects.create(username="newcomer", email="new@acme.com")
    created = ensure_organisation(newcomer)

    assert created is not None
    assert created != acme, "newcomer was joined to an existing organisation"
    assert created.owner == newcomer
    assert created.subdomain != acme.subdomain
    # Acme's membership is untouched — the newcomer is not in it.
    assert not OrganisationUser.objects.filter(user=newcomer, organisation=acme).exists()


def test_confidential_client_gets_the_plaintext_secret_not_the_hash():
    """The bug that broke the first real Claude connect.

    DOT's `client_secret` is a `ClientSecretField`: the model default generates a
    128-char plaintext at instantiation and `save()` replaces it in place with a
    `pbkdf2_sha256$...` hash. Reading it *after* save returns the hash, and
    returning that to the client makes every token exchange fail with a 401 —
    the client sends the hash, DOT hashes it again, and they never match.

    A public client never noticed, because it authenticates with client_id plus
    PKCE and the secret is unused. Claude registers as **confidential** (its
    callback is `https://claude.ai/api/mcp/auth_callback`, so the exchange
    happens on Anthropic's server, which can hold a secret), which is what
    surfaced it.
    """
    reg = validate_registration({
        "client_name": "Claude",
        "redirect_uris": ["https://claude.ai/api/mcp/auth_callback"],
        "token_endpoint_auth_method": "client_secret_post",
    })
    assert not reg.is_public

    body = registration_response(reg, "cid", "the-plaintext-secret", 1700000000)
    assert body["client_secret"] == "the-plaintext-secret"
    assert not body["client_secret"].startswith("pbkdf2_"), (
        "a hashed secret was returned to the client; it will 401 at /oauth/token"
    )


def test_claude_ai_callback_is_an_accepted_redirect():
    """The real redirect_uri Claude sends, from the observed authorize request."""
    assert validate_redirect_uri("https://claude.ai/api/mcp/auth_callback")


def test_confidential_registration_survives_claudes_actual_body():
    """Verbatim from the DCR request Claude made against the local server."""
    reg = validate_registration({
        "client_name": "Claude",
        "redirect_uris": ["https://claude.ai/api/mcp/auth_callback"],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "client_secret_post",
    })
    assert reg.client_name == "Claude"
    assert reg.is_public is False


# ------------------------------------------------- multi-org choice at consent

@pytest.fixture
def two_orgs(db):
    from django.contrib.auth.models import Group, User

    from terno_dbi.core.models import CoreOrganisation, OrganisationUser

    owner = User.objects.create(username="mo-owner")
    acme = CoreOrganisation.objects.create(name="Acme", subdomain="mo-acme", owner=owner)
    globex = CoreOrganisation.objects.create(name="Globex", subdomain="mo-globex", owner=owner)
    outsider_org = CoreOrganisation.objects.create(
        name="Initech", subdomain="mo-initech", owner=owner
    )

    user = User.objects.create(username="mo-user")
    m1 = OrganisationUser.objects.create(user=user, organisation=acme)
    m1.groups.set([Group.objects.get_or_create(name="Org Admin")[0]])
    OrganisationUser.objects.create(user=user, organisation=globex)

    return {"user": user, "acme": acme, "globex": globex, "outsider": outsider_org}


@pytest.mark.django_db
def test_choices_cover_every_membership_and_label_them_by_name(two_orgs):
    """Every membership is offered, labelled by organisation name.

    The label used to carry an "— you are an admin here" suffix. It was dead code
    — it read "Org Admin" off `membership.groups`, where nothing puts it — and
    once the lookup was corrected it applied to *every* option for an admin,
    overflowing the select. Write access is stated in its own section of the
    consent screen, which is where it belongs.
    """
    from terno_dbi.oauth.org_choice import organisation_choices

    choices = dict(organisation_choices(two_orgs["user"]))
    assert set(choices) == {str(two_orgs["acme"].pk), str(two_orgs["globex"].pk)}
    assert choices[str(two_orgs["acme"].pk)] == two_orgs["acme"].name
    assert choices[str(two_orgs["globex"].pk)] == two_orgs["globex"].name
    assert not any("admin" in label.lower() for label in choices.values())


@pytest.mark.django_db
def test_a_non_member_organisation_is_refused(two_orgs):
    """The choice arrives in a hidden field in a browser POST, so it is
    attacker-controlled. Honouring it unvalidated would hand out a token for
    someone else's organisation."""
    from terno_dbi.oauth.org_choice import validate_choice

    assert validate_choice(two_orgs["user"], two_orgs["globex"].pk) == two_orgs["globex"]
    assert validate_choice(two_orgs["user"], two_orgs["outsider"].pk) is None
    assert validate_choice(two_orgs["user"], 999999) is None


@pytest.mark.django_db
@pytest.mark.parametrize("bad", ["", None, "not-a-number", "1; DROP TABLE", "-1"])
def test_malformed_choices_fail_closed(two_orgs, bad):
    from terno_dbi.oauth.org_choice import validate_choice

    assert validate_choice(two_orgs["user"], bad) is None


def test_claims_round_trip_preserves_existing_content():
    """`claims` is DOT's OIDC pass-through. Overwriting it would discard whatever
    the client sent."""
    from terno_dbi.oauth.org_choice import extract_from_claims, merge_into_claims

    merged = merge_into_claims('{"userinfo": {"email": null}}', 42)
    assert extract_from_claims(merged) == "42"
    assert "userinfo" in merged


@pytest.mark.parametrize("raw", ["", None, "not json", "[1,2,3]", '"a string"'])
def test_claims_extraction_survives_garbage(raw):
    from terno_dbi.oauth.org_choice import extract_from_claims, merge_into_claims

    assert extract_from_claims(raw) is None
    # Merging into garbage must still produce something readable back.
    assert extract_from_claims(merge_into_claims(raw, 7)) == "7"


@pytest.mark.django_db
def test_selector_is_hidden_for_a_single_membership(db):
    """One organisation is a statement, not a decision."""
    from django.contrib.auth.models import User

    from terno_dbi.core.models import CoreOrganisation, OrganisationUser
    from terno_dbi.oauth.org_choice import organisation_choices

    owner = User.objects.create(username="single-owner")
    org = CoreOrganisation.objects.create(
        name="Only", subdomain="single-only", owner=owner
    )
    user = User.objects.create(username="single-user")
    OrganisationUser.objects.create(user=user, organisation=org)

    assert len(organisation_choices(user)) == 1  # view suppresses the selector


def test_connector_login_url_is_a_real_page_not_the_bare_domain(settings):
    """`LOGIN_URL` in terno-ai is the bare domain, so DOT would drop an
    unauthenticated user on the site root with nothing to do — which is exactly
    what a new user reported. The authorize view overrides it with the actual
    login screen, on the same host, so Django's `?next=` survives."""
    from django.apps import apps

    from terno_dbi.oauth import views

    view_cls = views.make_authorization_view()
    if view_cls is None or not apps.is_installed("oauth2_provider"):
        pytest.skip("django-oauth-toolkit not installed")

    settings.PROVISIONER_URL = "https://app.terno.ai"
    settings.TERNO_CONNECTOR_LOGIN_URL = None

    url = view_cls().get_login_url()
    assert url == "https://app.terno.ai/accounts/login/"
    assert url != "https://app.terno.ai"

    settings.TERNO_CONNECTOR_LOGIN_URL = "https://app.terno.ai/accounts/signup/"
    assert view_cls().get_login_url().endswith("/accounts/signup/")


@pytest.mark.parametrize("issuer,expected", [
    ("http://127.0.0.1:8000", "http://127.0.0.1:8000/accounts/login/"),
    ("https://app.terno.ai", "https://app.terno.ai/accounts/login/"),
    ("app.terno.ai", "https://app.terno.ai/accounts/login/"),
    ("https://app.terno.ai/", "https://app.terno.ai/accounts/login/"),
])
def test_login_url_never_doubles_the_scheme(settings, issuer, expected):
    """`MAIN_DOMAIN` / the issuer is a bare host in some deployments and a full
    origin in others. Prefixing 'https://' unconditionally produced
    'https://http://127.0.0.1:8000/...', which 404s — hit while testing the real
    connect flow locally."""
    from django.apps import apps

    from terno_dbi.oauth import views

    view_cls = views.make_authorization_view()
    if view_cls is None or not apps.is_installed("oauth2_provider"):
        pytest.skip("django-oauth-toolkit not installed")

    settings.TERNO_CONNECTOR_LOGIN_URL = None
    settings.PROVISIONER_URL = issuer
    url = view_cls().get_login_url()
    assert url == expected
    assert url.count("://") == 1, f"malformed: {url}"


def test_oauth_paths_are_exempt_from_subdomain_scoping():
    """`/oauth/*` is served on app.terno.ai, which is not an org subdomain.

    `SubdomainOrganisationMiddleware` resolves `host.split('.')[0]` to an
    Organisation and 403s when there is none — 'app' is not an org. It bypasses
    *unauthenticated* requests, so the first hit to /oauth/authorize passes and
    the failure appears only on the return trip after login, which is the more
    confusing order.

    Only active in production (`ENABLE_SUBDOMAIN=True`); local development uses
    `DefaultOrganisationMiddleware`, which is why this does not reproduce
    locally.
    """
    from pathlib import Path

    mw = Path("/Users/navin/terno/terno-ai/terno/terno/middleware/subdomain_middleware.py")
    if not mw.is_file():
        pytest.skip("terno-ai checkout not present")

    body = mw.read_text()
    assert "'/oauth'" in body, "/oauth must bypass subdomain scoping"
    assert "'/.well-known'" in body, "/.well-known must bypass subdomain scoping"


def test_register_is_served_at_both_paths():
    """RFC 7591 does not mandate a registration path.

    A client that cannot parse the authorization-server metadata falls back to
    guessing `<issuer>/register`. Observed live: Claude POSTed there, hit the
    SPA catch-all, and got a CSRF 403 — surfaced to the user as "couldn't
    register with terno's sign-in service", which points nowhere near the real
    cause. Serving both paths makes the guess work.
    """
    from terno_dbi.oauth import urls as oauth_urls

    routes = {p.pattern._route for p in oauth_urls.urlpatterns}
    assert "oauth/register" in routes
    assert "register" in routes

    views_for = {
        p.pattern._route: p.callback
        for p in oauth_urls.urlpatterns
        if p.pattern._route in {"oauth/register", "register"}
    }
    assert views_for["oauth/register"] is views_for["register"], (
        "the alias must be the same view, not a copy that can drift"
    )


def test_connector_env_vars_are_plain_overrides_not_derived():
    """TERNO_MCP_BASE_URL, TERNO_CONNECTOR_LOGIN_URL and TRUSTED_REDIRECT_DOMAINS
    are set explicitly per environment (in env.sh, alongside MAIN_DOMAIN and
    PROVISIONER_URL) rather than derived from MAIN_DOMAIN/PROVISIONER_URL —
    derivation was tried and reverted because it added a hostname-rewriting
    helper for something a deploy already states directly."""
    from pathlib import Path

    settings_py = Path("/Users/navin/terno/terno-ai/terno/mysite/settings.py")
    if not settings_py.is_file():
        pytest.skip("terno-ai checkout not present")

    src = settings_py.read_text()
    assert "_mcp_host" not in src
    assert "_absolute" not in src
    assert 'TERNO_MCP_BASE_URL = os.environ.get("TERNO_MCP_BASE_URL"' in src

    web_settings = Path("/Users/navin/terno/terno-web/terno/terno/settings.py")
    if web_settings.is_file():
        web_src = web_settings.read_text()
        assert "_provisioner_host" not in web_src
        assert "TRUSTED_REDIRECT_DOMAINS = [" in web_src


def test_connector_login_hands_off_via_sso_not_straight_to_authorize():
    """terno-web must not redirect a pending authorize URL directly.

    `/oauth/authorize` is terno-ai, which keeps a separate session cookie
    (`ternoapp_sessionid` vs terno-web's `sessionid`). A direct redirect leaves
    terno-ai anonymous, so it bounces back to terno-web's login, which — already
    authenticated — bounces straight back. Observed live against staging as
    dozens of `GET /oauth/authorize/ -> 302` with the OAuth client restarting the
    handshake three times before giving up.

    `/sso-login` is the only hop that establishes the terno-ai session, and it
    already forwards a connector authorize path.
    """
    from pathlib import Path

    views = Path("/Users/navin/terno/terno-web/terno/provisioner/views.py")
    if not views.is_file():
        pytest.skip("terno-web checkout not present")

    src = views.read_text()
    assert "def _connector_sso_url" in src, (
        "the SSO hand-off for a pending connector authorize URL is gone"
    )
    assert "if is_connector_next(next_param):" in src, (
        "login_page no longer special-cases the connector redirect"
    )
    # The hand-off must go through get_subdomain_login_url, which mints the SSO
    # token; anything else cannot give terno-ai a session.
    handoff = src.split("def _connector_sso_url", 1)[1].split("\ndef ", 1)[0]
    assert "get_subdomain_login_url" in handoff

    # A user arriving from a connector directory has no organisation yet -- orgs
    # are created in `onboarding`, which runs *after* this hop. Returning None
    # here would fall back to the direct redirect and loop, so the no-org case
    # must provision rather than give up.
    assert "_provision_org_for_connector" in handoff, (
        "no-org users fall back to the direct redirect, which loops"
    )
    provision = src.split("def _provision_org_for_connector", 1)[1].split("\ndef ", 1)[0]
    assert "ensure_org_created_and_email_sent" in provision, (
        "connector-created orgs must use the same helper as web signup, or they "
        "differ from web-created ones in naming, demo data, or approval email"
    )


def test_terno_ai_sso_login_forwards_a_connector_authorize_path():
    """The receiving half of the hand-off above.

    `sso_login` must check `is_connector_next` *before* its org-subdomain
    branch, which would otherwise rewrite the target to the app home and drop
    the authorize URL.
    """
    from pathlib import Path

    views = Path("/Users/navin/terno/terno-ai/terno/terno/views.py")
    if not views.is_file():
        pytest.skip("terno-ai checkout not present")

    body = views.read_text().split("def sso_login", 1)[1].split("\ndef ", 1)[0]
    assert "is_connector_next(redirect_to)" in body

    login_at = body.index("perform_login")
    connector_at = body.index("is_connector_next(redirect_to)")
    subdomain_at = body.index("org_user.organisation.subdomain")
    assert login_at < connector_at, "session must be established before redirecting"
    assert connector_at < subdomain_at, (
        "the connector check must precede the subdomain rewrite, or the "
        "authorize URL is replaced by the app home"
    )


@pytest.mark.django_db
def test_write_gate_reads_the_group_terno_ai_actually_assigns(org_fixture):
    """The write gate must read `User.groups`, not `OrganisationUser.groups`.

    terno-ai's `create_org_admin_group` receiver does
    `instance.owner.groups.add(group)` on org creation, and both `terno/views.py`
    and `terno/permissions.py` check `user.groups.filter(name="Org Admin")`. That
    is the app's single global admin flag.

    Reading the gate off `OrganisationUser.groups` instead made `can_write`
    permanently False in production — nothing ever puts "Org Admin" there — so
    `admin:write`/`admin:sync` were stripped from *every* token and the write half
    of the connector was unreachable. It passed locally only because both the
    fixture and `bootstrap_local` set the group in the same wrong place.
    Observed on staging as `groups=[] can_write=False` for a fresh org owner.
    """
    from django.contrib.auth.models import Group, User

    from terno_dbi.core.models import OrganisationUser
    from terno_dbi.oauth.minting import (
        generate_oauth_access_token,
        mint_service_token_for_key,
        token_grant_summary,
        user_can_write,
    )

    org = org_fixture["org"]
    admin_group = Group.objects.get(name="Org Admin")
    requested = ["query:read", "admin:write", "admin:sync"]

    # Global group -> write granted, at mint time and at request time.
    admin_membership = OrganisationUser.objects.get(user=org_fixture["admin"], organisation=org)
    assert user_can_write(admin_membership) is True
    token = mint_service_token_for_key(
        generate_oauth_access_token(), org_fixture["admin"], org, requested
    )
    assert {"admin:write", "admin:sync"} <= set(token.scopes)
    summary = token_grant_summary(token)
    assert summary["is_org_admin"] is True
    assert summary["can_write"] is True

    # The membership m2m must NOT be a backdoor: it is for table scoping, and a
    # user who only has "Org Admin" there is not an admin as far as the app is
    # concerned, so the connector must not treat them as one.
    impostor = User.objects.create(username="membership-only-admin")
    impostor_membership = OrganisationUser.objects.create(user=impostor, organisation=org)
    impostor_membership.groups.set([admin_group])
    assert user_can_write(impostor_membership) is False

    impostor_token = mint_service_token_for_key(
        generate_oauth_access_token(), impostor, org, requested
    )
    assert not ({"admin:write", "admin:sync"} & set(impostor_token.scopes))
    assert token_grant_summary(impostor_token)["can_write"] is False


def test_bootstrap_local_grants_the_group_the_same_way_production_does():
    """The local harness must not diverge from production wiring.

    It previously did `membership.groups.add(admin_group)`, which granted write
    locally while production granted none — the divergence that hid the write-gate
    bug through every local test run.
    """
    from pathlib import Path

    cmd = Path(__file__).resolve().parents[2] / (
        "src/terno_dbi/core/management/commands/bootstrap_local.py"
    )
    src = cmd.read_text()
    assert "user.groups.add(admin_group)" in src
    assert "membership.groups.add(admin_group)" not in src


def test_completed_connect_does_not_leave_a_replayable_redirect():
    """A finished connector flow must not be resumable a second time.

    `login_page` resumes from its own `session["next"]`, while
    `ConnectorNextMiddleware` keeps a separate copy under
    `session["terno_connector_next"]` and re-stores it on every authorize->login
    bounce. Nothing consumed that copy, so it outlived the completed connect and
    the next `pop_next` caller replayed it.

    Observed on staging: token minted 12:54:39, user landed in the app, then at
    12:55:42 `onboarding` popped the stale URL and the consent screen reappeared
    -- a second token for a single connect.
    """
    from pathlib import Path

    views = Path("/Users/navin/terno/terno-web/terno/provisioner/views.py")
    redirect_mod = Path(
        "/Users/navin/terno/terno-web/terno/provisioner/connector_redirect.py"
    )
    if not views.is_file() or not redirect_mod.is_file():
        pytest.skip("terno-web checkout not present")

    assert "def clear_next" in redirect_mod.read_text()

    src = views.read_text()
    branch = src.split("if is_connector_next(next_param):", 1)[1].split("return redirect", 1)[0]
    assert "clear_next(request)" in branch, (
        "login_page consumes session['next'] but leaves the middleware's copy "
        "behind, so the connector flow can be replayed after it completes"
    )


CONSENT_TEMPLATE = "oauth2_provider/authorize.html"


def _render_consent(**overrides):
    """Render the consent screen with a realistic context."""
    from django.template.loader import get_template

    class _Org:
        name = "Acme Analytics"
        subdomain = "acme"
        def __str__(self):
            return f"{self.name} - {self.subdomain}"

    context = {
        "application": type("A", (), {"name": "Claude"})(),
        "form": [],
        "organisation": _Org(),
        "read_scopes": [{"description": "Run read-only SQL queries"}],
        "write_scopes": [{"description": "Refresh schema metadata"}],
        "user_can_write": True,
        "csrf_token": "t",
    }
    context.update(overrides)
    return get_template(CONSENT_TEMPLATE).render(context)


@pytest.mark.parametrize("state", ["single_org", "multi_org", "no_write_permission", "error"])
def test_consent_screen_never_leaks_template_source(state):
    """`{# ... #}` is single-line only. A multi-line one is not a comment — Django
    renders it verbatim, which is exactly how

        {# More than one membership: let the user choose. Each organisation has
           its own databases ... #}

    ended up displayed to users on the live consent screen. Anything spanning
    lines must use {% comment %}.
    """
    from django.template import TemplateSyntaxError

    overrides = {
        "single_org": {},
        "multi_org": {
            "organisation_choices": [("1", "Acme Analytics"), ("2", "Side Project")],
            "selected_organisation_id": "1",
        },
        "no_write_permission": {"user_can_write": False},
        "error": {"error": type("E", (), {"error": "invalid_request",
                                          "description": "Missing redirect_uri"})()},
    }[state]

    try:
        html = _render_consent(**overrides)
    except TemplateSyntaxError as exc:
        pytest.fail(f"consent template does not compile: {exc}")

    assert "{#" not in html and "#}" not in html, "a template comment reached the user"
    assert "{%" not in html and "%}" not in html, "an unrendered template tag reached the user"
    assert "{{" not in html, "an unrendered variable reached the user"


def test_consent_screen_names_the_organisation_readably():
    """`CoreOrganisation.__str__` is "name - subdomain", and auto-provisioned orgs
    have name == subdomain — so `{{ organisation }}` rendered "fugj - fugj" on the
    live screen, in the heading and again in the write-access warning."""
    html = _render_consent(user_can_write=False)

    assert "Acme Analytics" in html
    assert "Acme Analytics - acme" not in html, "organisation rendered via __str__"


def test_consent_screen_states_when_write_will_be_withheld():
    """A non-admin's write scopes are stripped at mint time whatever they click,
    so the screen has to say so rather than implying the request was granted."""
    withheld = _render_consent(user_can_write=False)
    granted = _render_consent(user_can_write=True)

    # Match the notice, not the substring: "read-only" also appears in the scope
    # description "Run read-only SQL queries", so a bare containment check passes
    # whether or not the warning is rendered at all.
    notice = "you are not an administrator"
    assert notice in withheld
    assert notice not in granted


def test_consent_screen_makes_no_external_requests():
    """Served by Django from inside the pip package, on a host whose SPA assets
    are content-hashed and rebuilt independently. Any external reference would be
    a broken image or a blocked font on the one screen a user must trust."""
    import re

    html = _render_consent()
    external = re.findall(r'(?:src|href)="(?!#)(https?:|//)[^"]*"', html)
    assert not external, f"consent screen loads external resources: {external}"


# ------------------------------------------------- write access over time
#
# Write needs two halves that answer different questions: the *scope* is what
# the client was granted at consent and is frozen into the token; the *group* is
# what the user may do right now and is re-read on every request. These four
# cases are the whole matrix.

@pytest.fixture
def connectable(db):
    from django.contrib.auth.models import Group, User

    from terno_dbi.core.models import CoreOrganisation, OrganisationUser

    owner = User.objects.create(username="wa-owner")
    org = CoreOrganisation.objects.create(name="Acme", subdomain="wa-acme", owner=owner)
    user = User.objects.create(username="wa-user")
    OrganisationUser.objects.create(user=user, organisation=org)
    group, _ = Group.objects.get_or_create(name="Org Admin")
    return {"org": org, "user": user, "group": group}


_ALL_REQUESTED = ["query:read", "query:execute", "admin:read", "admin:write", "admin:sync"]


def _mint(user, org):
    from terno_dbi.oauth.minting import (
        generate_oauth_access_token,
        mint_service_token_for_key,
    )

    return mint_service_token_for_key(
        generate_oauth_access_token(), user, org, _ALL_REQUESTED
    )


def _per_request(token):
    """What the transport recomputes on every request."""
    from terno_dbi.oauth.minting import token_grant_summary

    token.refresh_from_db()
    return token_grant_summary(token)


def _tool_names(token):
    from terno_dbi.mcp.merged_server import visible_tools

    summary = _per_request(token)
    return {t.name for t in visible_tools(summary["scopes"], summary["can_write"])}


@pytest.mark.django_db
def test_write_case1_non_admin_never_receives_write(connectable):
    """Consent alone cannot escalate: the scopes are stripped at mint."""
    token = _mint(connectable["user"], connectable["org"])

    assert not ({ADMIN_WRITE, ADMIN_SYNC} & set(token.scopes))
    assert _per_request(token)["can_write"] is False
    assert "delete_datasource" not in _tool_names(token)


@pytest.mark.django_db
def test_write_case2_admin_receives_write(connectable):
    connectable["user"].groups.add(connectable["group"])
    token = _mint(connectable["user"], connectable["org"])

    assert {ADMIN_WRITE, ADMIN_SYNC} <= set(token.scopes)
    assert _per_request(token)["can_write"] is True
    assert "delete_datasource" in _tool_names(token)


@pytest.mark.django_db
def test_write_case3_demotion_takes_effect_without_reconnecting(connectable):
    """The token still carries admin:write — the group is what changed.

    Because the group is re-read per request, write dies on the next call rather
    than lasting until the token expires (up to 8 hours). The listing has to
    agree: showing a tool that can only ever fail wastes the model's turn and
    reports the wrong cause.
    """
    connectable["user"].groups.add(connectable["group"])
    token = _mint(connectable["user"], connectable["org"])
    assert "delete_datasource" in _tool_names(token)

    connectable["user"].groups.remove(connectable["group"])

    assert {ADMIN_WRITE, ADMIN_SYNC} <= set(token.scopes), "scopes should be untouched"
    assert _per_request(token)["can_write"] is False
    assert "delete_datasource" not in _tool_names(token)
    # readOnlyHint: true but gated on admin:write — the annotation-based check
    # misses it, so only subtracting the scope hides it.
    assert "validate_connection" not in _tool_names(token)
    # Read access is unaffected: demotion is not disconnection.
    assert "execute_query" in _tool_names(token)


@pytest.mark.django_db
def test_write_case4_promotion_does_not_upgrade_a_live_connection(connectable):
    """The user is an admin now, but consented to a read-only connection — the
    screen said so. Upgrading it silently would grant access the user never
    approved, so it takes a reconnect."""
    token = _mint(connectable["user"], connectable["org"])
    assert _per_request(token)["can_write"] is False

    connectable["user"].groups.add(connectable["group"])

    summary = _per_request(token)
    assert summary["is_org_admin"] is True
    assert summary["can_write"] is False
    assert "delete_datasource" not in _tool_names(token)

    # Reconnecting is what grants it.
    assert {ADMIN_WRITE, ADMIN_SYNC} <= set(_mint(connectable["user"], connectable["org"]).scopes)


@pytest.mark.django_db
def test_withdrawn_write_is_reported_as_withdrawn_not_ungranted(connectable):
    """"was not granted" sends the user to reconnect, which cannot fix a
    demotion. The message has to name the real cause."""
    import asyncio

    from terno_dbi.mcp import merged_server
    from terno_dbi.mcp.context import request_credentials

    connectable["user"].groups.add(connectable["group"])
    token = _mint(connectable["user"], connectable["org"])
    connectable["user"].groups.remove(connectable["group"])

    summary = _per_request(token)
    with request_credentials(
        api_key="k", can_write=summary["can_write"], scopes=summary["scopes"]
    ):
        result = asyncio.run(merged_server.call_tool("delete_datasource", {}))

    text = " ".join(c.text for c in result.content).lower()
    assert "no longer an administrator" in text
    assert "was not granted" not in text


def test_the_promotion_asymmetry_is_documented():
    """A user promoted to Org Admin keeps getting write refusals until they
    reconnect. That is correct — their consent was for a read-only connection —
    but it reads as a fault, so both the agent-facing docs resource and the
    human-facing guide have to say so. An undocumented correct behaviour gets
    "fixed" by the next person to hit it.
    """
    from terno_dbi.mcp.instructions import DOCS_LONG_FORM

    assert "## When write tools disappear" in DOCS_LONG_FORM
    assert "reconnect" in DOCS_LONG_FORM.lower()

    guide = pathlib.Path(__file__).resolve().parents[2] / "docs/mcp-guide.md"
    if not guide.is_file():
        pytest.skip("docs/mcp-guide.md not present")
    text = guide.read_text().lower()
    assert "org admin" in text
    assert "disconnect and reconnect" in text
