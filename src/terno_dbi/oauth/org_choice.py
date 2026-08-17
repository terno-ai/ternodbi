"""Let a user choose which organisation to connect for OAuth.

Users may belong to multiple organisations, so the organisation cannot be
selected automatically. The choice must survive from the consent screen to
token exchange, which happens as a separate back-channel request without the
user's session cookie. It is therefore stored on the OAuth `Grant`.

The selected organisation is treated as untrusted input and validated against
the user's `OrganisationUser` membership both when submitted and when the
authorization code is redeemed.
"""

import json
import logging
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

CLAIM_KEY = "terno_organisation_id"


def memberships_for(user) -> List:
    """Every organisation this user belongs to, oldest first."""
    from terno_dbi.core.models import OrganisationUser

    return list(
        OrganisationUser.objects.filter(user=user)
        .select_related("organisation")
        .prefetch_related("groups")
        .order_by("id")
    )


def organisation_choices(user) -> List[Tuple[str, str]]:
    """`(id, label)` pairs for the consent screen selector."""

    choices = []
    for membership in memberships_for(user):
        org = membership.organisation
        choices.append((str(org.pk), org.name or org.subdomain or f"Organisation {org.pk}"))
    return choices


def validate_choice(user, organisation_id) -> Optional[object]:
    """Return the organisation only if `user` is genuinely a member of it.

    Returns None for a missing, malformed, or non-member id — the caller then
    falls back to the default rather than trusting the submitted value.
    """
    if organisation_id in (None, ""):
        return None
    try:
        organisation_id = int(organisation_id)
    except (TypeError, ValueError):
        logger.warning("Rejected non-numeric organisation choice %r", organisation_id)
        return None

    from terno_dbi.core.models import OrganisationUser

    membership = (
        OrganisationUser.objects
        .filter(user=user, organisation_id=organisation_id)
        .select_related("organisation")
        .first()
    )
    if membership is None:
        # Either a tampered hidden field or a membership revoked between consent
        # and redemption. Both must fail closed to the default, never honoured.
        logger.warning(
            "Rejected organisation choice %s for %s: not a member",
            organisation_id, user,
        )
        return None
    return membership.organisation


def merge_into_claims(raw_claims: str, organisation_id) -> str:
    """Add the choice to a claims blob without discarding what is already there."""
    try:
        data = json.loads(raw_claims) if raw_claims else {}
        if not isinstance(data, dict):
            data = {}
    except (TypeError, ValueError):
        data = {}
    data[CLAIM_KEY] = str(organisation_id)
    return json.dumps(data)


def extract_from_claims(raw_claims: str):
    """Pull the choice back out. Returns None when absent or unparseable."""
    if not raw_claims:
        return None
    try:
        data = json.loads(raw_claims)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    return data.get(CLAIM_KEY)


def organisation_from_grant(user, code, application):
    """The validated organisation recorded against an authorization code."""
    try:
        from oauth2_provider.models import get_grant_model
    except ImportError:
        return None

    grant = get_grant_model().objects.filter(code=code, application=application).first()
    if grant is None:
        return None
    return validate_choice(user, extract_from_claims(grant.claims))
