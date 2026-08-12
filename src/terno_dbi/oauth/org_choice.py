"""Letting a multi-organisation user choose which one to connect.

Before this, `resolve_organisation` picked the oldest membership and logged a
warning. That warning fired on the first real Claude connect against a live
server, so it was the default experience, not an edge case.

## Where the choice has to live

The consent screen is a browser POST; the token exchange that follows is a
**back-channel POST from Anthropic's server with no session cookie**. So the
Django session cannot carry the choice — it has to be persisted on the `Grant`
row that the authorization code refers to.

DOT's `Grant.claims` is a pass-through `TextField` for OIDC claims, and
`AllowForm` already round-trips it. The choice is merged in under a namespaced
key so a future OIDC use can coexist.

## The choice is untrusted input

`claims` is a hidden form field in a POST from the user's browser. Anyone can
edit it. So the organisation id is validated against `OrganisationUser` **twice**
— once when the form is submitted, once again when the code is redeemed — and a
value the user is not a member of is discarded rather than honoured. Validating
only at submission would mean a tampered hidden field became a grant for someone
else's organisation.
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
    from terno_dbi.oauth.minting import ORG_ADMIN_GROUP

    choices = []
    for membership in memberships_for(user):
        org = membership.organisation
        is_admin = any(g.name == ORG_ADMIN_GROUP for g in membership.groups.all())
        label = org.name or org.subdomain or f"Organisation {org.pk}"
        if is_admin:
            label = f"{label} — you are an admin here"
        choices.append((str(org.pk), label))
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
    except ImportError:  # pragma: no cover
        return None

    grant = get_grant_model().objects.filter(code=code, application=application).first()
    if grant is None:
        return None
    return validate_choice(user, extract_from_claims(grant.claims))
