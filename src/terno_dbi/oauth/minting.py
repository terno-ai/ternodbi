"""Create a `ServiceToken` from an approved OAuth grant.

This keeps OAuth isolated from the existing authorization layer. The rest of
TernoDBI continues to work with `ServiceToken` without needing OAuth-specific
logic.

The user's organisation groups must be copied to the token to preserve the
same table visibility as the web application. Without them, restricted tables
may be hidden even though the user can access them through the web app.

Write access requires both the OAuth scope and the user's organisation role.
The scope controls what the client was granted, while the role controls what
the user is allowed to do.
"""

import hashlib
import logging
import secrets
from typing import Iterable, Tuple
from terno_dbi.oauth.scopes import granted_scopes, scope_string

logger = logging.getLogger(__name__)

ORG_ADMIN_GROUP = "Org Admin"


OAUTH_KEY_PREFIX = "dbi_oauth_"


def generate_oauth_access_token(request=None) -> str:
    return f"{OAUTH_KEY_PREFIX}{secrets.token_urlsafe(32)}"


def key_hash(plaintext_key: str) -> str:
    return hashlib.sha256(plaintext_key.encode()).hexdigest()


def user_can_write(organisation_user) -> bool:
    if organisation_user is None:
        return False
    user = getattr(organisation_user, "user", None)
    if user is None:
        return False
    return user.groups.filter(name=ORG_ADMIN_GROUP).exists()


def resolve_membership(user, organisation):
    """The `OrganisationUser` row for this pair, or None."""
    from terno_dbi.core.models import OrganisationUser

    return (
        OrganisationUser.objects
        .filter(user=user, organisation=organisation)
        .prefetch_related("groups")
        .first()
    )


def mint_token_for_grant(
    user,
    organisation,
    requested_scopes: Iterable[str],
    *,
    client_name: str = "MCP connector",
    expires_at=None,
) -> Tuple[object, str, frozenset]:
    """Create the `ServiceToken` backing an OAuth access token.

    Returns `(token, plaintext_key, granted)`. The plaintext key is the OAuth
    access token value and is never recoverable afterwards — only its hash is
    stored.
    """
    from terno_dbi.core.models import ServiceToken
    from terno_dbi.services.auth import generate_service_token

    membership = resolve_membership(user, organisation)
    if membership is None:
        raise PermissionError(
            f"{user} is not a member of {organisation}; cannot mint a connector token."
        )

    can_write = user_can_write(membership)
    granted = granted_scopes(requested_scopes, can_write=can_write)

    token, plaintext = generate_service_token(
        name=f"{client_name} — {user}",
        token_type=ServiceToken.TokenType.OAUTH,
        created_for=user,
        organisation=organisation,
        expires_at=expires_at,
        scopes=sorted(granted),
    )

    groups = list(membership.groups.all())
    if groups:
        token.groups.set(groups)

    logger.info(
        "Minted OAuth ServiceToken for user=%s org=%s scopes=[%s] groups=[%s] can_write=%s",
        user, organisation, scope_string(granted),
        ", ".join(g.name for g in groups), can_write,
    )
    return token, plaintext, granted


def token_grant_summary(token) -> dict:
    """What the transport needs to know about a resolved token.

    Kept here so the HTTP layer does not have to know how scopes and groups
    combine — it asks once and gets an answer.
    """
    scopes = frozenset(token.scopes or [])
    is_org_admin = bool(
        token.created_for_id
        and token.created_for.groups.filter(name=ORG_ADMIN_GROUP).exists()
    )
    return {
        "scopes": scopes,
        "can_write": bool(scopes & {"admin:write", "admin:sync"}) and is_org_admin,
        "is_org_admin": is_org_admin,
    }


def resolve_organisation(user, organisation=None):
    """Which organisation this grant is for.

    OAuth has no subdomain to read, unlike the web app where
    `SubdomainOrganisationMiddleware` decides. So for a user in exactly one
    organisation it is unambiguous; for a user in several, the oldest membership
    is chosen deterministically and logged.

    Choosing deterministically is a limitation, not a design: a user in two orgs
    cannot currently pick which one the connector sees. The standards-track fix
    is RFC 8707 resource indicators, or an organisation picker on the consent
    screen. Until then this must not be silent, hence the warning.
    """
    from terno_dbi.core.models import OrganisationUser

    if organisation is not None:
        return organisation

    memberships = list(
        OrganisationUser.objects.filter(user=user)
        .select_related("organisation")
        .order_by("id")
    )
    if not memberships:
        return None
    if len(memberships) > 1:
        logger.warning(
            "User %s belongs to %d organisations; connector grant defaults to '%s'. "
            "There is no way for the user to choose.",
            user, len(memberships), memberships[0].organisation,
        )
    return memberships[0].organisation


def mint_service_token_for_key(
    plaintext_key: str,
    user,
    organisation,
    requested_scopes: Iterable[str],
    *,
    client_name: str = "MCP connector",
    expires_at=None,
):
    """Create the `ServiceToken` backing an already-issued OAuth access token.

    Unlike `mint_token_for_grant`, the key is supplied rather than generated —
    it is DOT's access token, so the two are the same secret and the bearer
    resolves through `verify_token` unchanged.
    """
    from terno_dbi.core.models import ServiceToken

    membership = resolve_membership(user, organisation)
    if membership is None:
        raise PermissionError(
            f"{user} is not a member of {organisation}; cannot mint a connector token."
        )

    can_write = user_can_write(membership)
    granted = granted_scopes(requested_scopes, can_write=can_write)

    token = ServiceToken.objects.create(
        name=f"{client_name} — {user}",
        token_type=ServiceToken.TokenType.OAUTH,
        key_prefix=OAUTH_KEY_PREFIX,
        key_hash=key_hash(plaintext_key),
        created_for=user,
        organisation=organisation,
        is_active=True,
        expires_at=expires_at,
        scopes=sorted(granted),
    )

    # Read parity, not the write gate — see the module docstring.
    groups = list(membership.groups.all())
    if groups:
        token.groups.set(groups)

    logger.info(
        "OAuth ServiceToken minted: user=%s org=%s scopes=[%s] groups=[%s] can_write=%s",
        user, organisation, scope_string(granted),
        ", ".join(g.name for g in groups), can_write,
    )
    return token


def deactivate_service_token_for_key(plaintext_key: str) -> int:
    """Deactivate the ServiceToken behind an access token.

    Called on revocation and on refresh-token rotation. Deactivating rather than
    deleting keeps the audit trail — who connected, when, and with what scopes —
    which is the point of these rows existing separately from DOT's.
    """
    from terno_dbi.core.models import ServiceToken

    updated = ServiceToken.objects.filter(
        key_hash=key_hash(plaintext_key), is_active=True
    ).update(is_active=False)
    if updated:
        logger.info("Deactivated %d OAuth ServiceToken(s) on revoke/rotate", updated)
    return updated
