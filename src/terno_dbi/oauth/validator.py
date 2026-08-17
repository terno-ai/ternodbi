"""Bridge django-oauth-toolkit and `ServiceToken`.

django-oauth-toolkit handles the OAuth flow, while TernoDBI handles
organisation scoping and authorization. This class connects the two by
creating a `ServiceToken` for each approved OAuth grant.

The OAuth access token is generated as a `dbi_`-prefixed token, so it can be
resolved by the existing `verify_token` path without changing token lookup or
adding a separate OAuth-to-service-token mapping.

Token issuance fails if a `ServiceToken` cannot be created. An OAuth token
without a valid authorization context must never be issued.
"""

import logging
from oauth2_provider.oauth2_validators import OAuth2Validator
from terno_dbi.oauth.minting import (
    deactivate_service_token_for_key,
    mint_service_token_for_key,
)
from terno_dbi.oauth.org_choice import organisation_from_grant
from terno_dbi.oauth.provisioning import ensure_organisation

logger = logging.getLogger(__name__)


class MissingOrganisation(Exception):
    """The authenticated user has no organisation to scope a grant to."""


class TernoOAuth2Validator(OAuth2Validator):
    """Mints a `ServiceToken` alongside every issued access token."""

    # Claims available if an `openid` scope is added later. Harmless now.
    oidc_claim_scope = None

    def validate_code(self, client_id, code, client, request, *args, **kwargs):
        """Recover the organisation the user chose at the consent screen.

        This is the only point where both the authorization code and the `Grant`
        row still exist — DOT deletes the grant once the code is redeemed. The
        value is re-validated against the user's memberships inside
        `organisation_from_grant`, because `claims` reached the grant from a
        hidden field in a browser POST and cannot be trusted on the way back
        out.
        """
        valid = super().validate_code(client_id, code, client, request, *args, **kwargs)
        if valid:
            user = getattr(request, "user", None)
            if user is not None:
                request.terno_organisation = organisation_from_grant(user, code, client)
        return valid

    def save_bearer_token(self, token, request, *args, **kwargs):
        super().save_bearer_token(token, request, *args, **kwargs)

        access_token = token.get("access_token")
        if not access_token:
            # Refresh flows that only rotate a refresh token have nothing to
            # mint against.
            return

        user = getattr(request, "user", None)
        if user is None or not getattr(user, "is_authenticated", False):
            logger.error("OAuth token issued with no authenticated user; refusing")
            raise MissingOrganisation("No authenticated user for this grant")

        # Three sources, in order of authority:
        #
        # 1. What the user picked at the consent screen, already re-validated
        #    against their memberships (`validate_code`).
        # 2. On a refresh there is no grant and no consent screen, so inherit the
        #    organisation from the token being refreshed — a refresh must not
        #    silently move the connection to a different organisation.
        # 3. Otherwise the default, creating a personal organisation if this is
        #    the user's first contact with Terno. Never joins an existing one.
        chosen = getattr(request, "terno_organisation", None)
        organisation = chosen or self._organisation_from_refresh(request)
        if organisation is None:
            organisation = ensure_organisation(user)
        else:
            logger.info(
                "Connector grant scoped to '%s' (%s)", organisation,
                "user choice" if chosen else "inherited from refreshed token",
            )
        if organisation is None:
            logger.error(
                "OAuth grant for %s has no organisation and none could be "
                "provisioned — refusing to issue a token that would authenticate "
                "but authorize nothing.", user,
            )
            raise MissingOrganisation(
                f"{user} has no Terno organisation and one could not be created. "
                f"Sign in at the Terno web app once, then reconnect."
            )

        # On refresh, DOT issues a new access token and the previous one stops
        # being valid — deactivate its ServiceToken so a leaked old bearer
        # cannot outlive the rotation.
        previous = getattr(request, "refresh_token_instance", None)
        if previous is not None and getattr(previous, "access_token", None):
            deactivate_service_token_for_key(previous.access_token.token)

        client_name = getattr(getattr(request, "client", None), "name", "MCP connector")
        scope = token.get("scope") or ""

        mint_service_token_for_key(
            access_token,
            user=user,
            organisation=organisation,
            requested_scopes=scope.split(),
            client_name=client_name,
        )

    @staticmethod
    def _organisation_from_refresh(request):
        """The organisation of the token being refreshed, if this is a refresh."""
        previous = getattr(request, "refresh_token_instance", None)
        access = getattr(previous, "access_token", None) if previous else None
        if access is None:
            return None

        from terno_dbi.core.models import ServiceToken
        from terno_dbi.oauth.minting import key_hash

        existing = ServiceToken.objects.filter(key_hash=key_hash(access.token)).first()
        return existing.organisation if existing else None

    def revoke_token(self, token, token_type_hint, request, *args, **kwargs):
        """Deactivate the ServiceToken when the OAuth token is revoked.

        Without this the OAuth token would stop working while its ServiceToken
        stayed active — and the ServiceToken is what `/mcp` actually checks, so
        the bearer would keep working after the user disconnected the connector.
        """
        deactivate_service_token_for_key(token)
        super().revoke_token(token, token_type_hint, request, *args, **kwargs)
