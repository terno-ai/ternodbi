"""The real OAuth flow — register, consent, token, refresh — per MCP endpoint.

`/mcp` (the stable channel): its consent screen must not offer
`connector:write` and its tokens must not carry it, while `/mcp/preview` grants
it to an Org Admin. A client that still asks for it on `/mcp` (e.g. from a
cached scope list) must connect normally, just without that scope.
"""

import base64
import hashlib
import re
from urllib.parse import parse_qs, urlencode, urlparse

import pytest
from django.apps import apps

pytestmark = pytest.mark.skipif(
    not apps.is_installed("oauth2_provider"), reason="django-oauth-toolkit not installed"
)

REDIRECT = "http://127.0.0.1:33418/callback"
VERIFIER = "v" * 64
CHALLENGE = base64.urlsafe_b64encode(
    hashlib.sha256(VERIFIER.encode()).digest()).rstrip(b"=").decode()
REQUESTED = "query:read query:execute connector:write"


@pytest.fixture
def org_admin(db):
    from django.contrib.auth.models import Group, User

    from terno_dbi.core.models import CoreOrganisation, OrganisationUser

    owner = User.objects.create(username="flow-owner")
    org = CoreOrganisation.objects.create(name="Flow", subdomain="flow", owner=owner)
    user = User.objects.create(username="flow-admin")
    user.groups.add(Group.objects.get_or_create(name="Org Admin")[0])
    OrganisationUser.objects.create(user=user, organisation=org)
    return user, org


def _register(client):
    r = client.post("/oauth/register", content_type="application/json", data={
        "client_name": "Claude", "redirect_uris": [REDIRECT],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"], "token_endpoint_auth_method": "none",
        "scope": REQUESTED,
    })
    assert r.status_code == 201, r.content
    return r.json()["client_id"]


def _authorize(client, client_id, resource):
    params = {
        "response_type": "code", "client_id": client_id, "redirect_uri": REDIRECT,
        "scope": REQUESTED, "state": "s", "code_challenge": CHALLENGE,
        "code_challenge_method": "S256",
    }
    if resource:
        params["resource"] = resource
    return client.get("/oauth/authorize/?" + urlencode(params))


def _flow(client, org_admin, resource):
    """Run the whole flow; return (consent page, token response JSON)."""
    user, org = org_admin
    client.force_login(user)
    client_id = _register(client)

    page = _authorize(client, client_id, resource)
    assert page.status_code == 200, page.content

    form = {k: v for k, v in page.context["form"].initial.items() if v is not None}
    form.update({"allow": "Authorize", "organisation": str(org.pk)})
    approved = client.post("/oauth/authorize/", data=form)
    assert approved.status_code == 302, approved.content
    code = parse_qs(urlparse(approved["Location"]).query)["code"][0]

    token = client.post("/oauth/token/", data={
        "grant_type": "authorization_code", "code": code, "redirect_uri": REDIRECT,
        "client_id": client_id, "code_verifier": VERIFIER,
    })
    assert token.status_code == 200, token.content
    return page, token.json(), client_id


def _scopes_on_page(page):
    return {row["scope"] for row in page.context["read_scopes"] + page.context["write_scopes"]}


def _service_token_scopes(access_token):
    from terno_dbi.services.auth import verify_token
    return set(verify_token(access_token).scopes)


@pytest.mark.django_db
@pytest.mark.parametrize("resource", [None, "https://mcp.terno.ai/mcp"])
def test_stable_flow_never_shows_or_grants_connector_write(client, org_admin, resource):
    page, token, _ = _flow(client, org_admin, resource)

    assert _scopes_on_page(page) == {"query:read", "query:execute"}
    assert "connector:write" not in page.context["form"].initial["scope"]
    assert "Google Ads" not in page.content.decode()
    assert "connector:write" not in token["scope"].split()
    assert _service_token_scopes(token["access_token"]) == {"query:read", "query:execute"}


@pytest.mark.django_db
def test_preview_flow_grants_connector_write_to_an_org_admin(client, org_admin):
    page, token, _ = _flow(client, org_admin, "https://mcp.terno.ai/mcp/preview")

    assert "connector:write" in _scopes_on_page(page)
    assert "connector:write" in token["scope"].split()
    assert "connector:write" in _service_token_scopes(token["access_token"])


@pytest.mark.django_db
@pytest.mark.parametrize("resource", [None, "https://mcp.terno.ai/mcp/preview"])
def test_refresh_keeps_working_on_either_endpoint(client, org_admin, resource):
    _, token, client_id = _flow(client, org_admin, resource)

    refreshed = client.post("/oauth/token/", data={
        "grant_type": "refresh_token", "refresh_token": token["refresh_token"],
        "client_id": client_id,
    })
    assert refreshed.status_code == 200, refreshed.content
    assert set(refreshed.json()["scope"].split()) == set(token["scope"].split())
    from terno_dbi.services.auth import verify_token
    assert verify_token(refreshed.json()["access_token"]) is not None


@pytest.mark.django_db
@pytest.mark.xfail(strict=True, reason=(
    "Pre-existing (present at a5ebed9): TernoOAuth2Validator.save_bearer_token "
    "calls super() first, which revokes the old DOT access token, so "
    "`refresh_token_instance.access_token` is already gone when it tries to "
    "deactivate the old ServiceToken. The old bearer keeps working at /mcp."))
def test_refresh_switches_off_the_old_bearer(client, org_admin):
    _, token, client_id = _flow(client, org_admin, None)
    client.post("/oauth/token/", data={
        "grant_type": "refresh_token", "refresh_token": token["refresh_token"],
        "client_id": client_id,
    })
    from terno_dbi.services.auth import verify_token
    assert verify_token(token["access_token"]) is None


@pytest.mark.django_db
def test_registration_still_accepts_connector_write(client):
    """Clients that registered with the old scope list must not be refused."""
    assert re.match(r"^[\w-]+$", _register(client))


def test_dot_still_accepts_connector_write():
    """Hidden is not removed: DOT must accept it, or a client asking for it on
    /mcp would get invalid_scope instead of connecting."""
    from oauth2_provider.settings import oauth2_settings
    assert "connector:write" in oauth2_settings.SCOPES
