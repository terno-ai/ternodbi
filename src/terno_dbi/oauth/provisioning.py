"""Creating an organisation for a user who arrives via OAuth without one.

A user who finds Terno through Claude has no Terno account yet. They sign in (or
sign up) through allauth, land back on the consent screen — and have no
organisation, so there is nothing to scope a grant to. Without this the grant is
refused and the connector is a dead end for exactly the users a directory
listing is meant to bring in.

## The rule: a personal organisation, never an existing one

A new user whose email domain matches an existing organisation **must not** be
added to it. Email-domain matching is not proof of employment, and silently
joining someone to an organisation would give them that organisation's
datasources. Every user provisioned here gets their own organisation, exactly as
the web signup flow does.

## Why the creation itself is a hook

The sequence — `LLMCredit`, `CoreOrganisation`, then the terno-ai `Organisation`
extension whose `save()` bootstraps the membership, staff flag, `org_owner`
group and preferences — depends on terno-ai models that this package cannot
import. So terno-ai supplies the callable and ternodbi calls it. Configure with:

    TERNO_ORG_PROVISIONER = "terno.provisioning.provision_personal_org"

With no provisioner configured, `ensure_organisation` returns None and the grant
is refused with a message telling the user to sign in to the web app once. That
is the correct degradation: it fails closed, and it is what a deployment without
terno-ai (a bare ternodbi install) should do.
"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# Mirrors terno-web's provisioner so a connector-created subdomain is
# indistinguishable from a web-created one.
RESERVED_SUBDOMAINS = {"admin", "www", "api", "support", "help", "mcp", "app"}
SUBDOMAIN_REGEX = re.compile(r"^(?!-)[a-z0-9-]{1,25}(?<!-)$")
MAX_SUBDOMAIN_LENGTH = 25


def _slug(value: str) -> str:
    from django.utils.text import slugify

    # slugify keeps underscores but the regex rejects them, which is what made
    # 8% of a production sample fall back to a generic name. Truncation matters
    # for the same reason: an over-long slug fails the regex and would otherwise
    # be discarded entirely rather than shortened.
    slug = slugify(value).replace("_", "-").strip("-")
    return slug[:MAX_SUBDOMAIN_LENGTH].strip("-")


def generate_subdomain(seed: str, taken=None) -> str:
    """A unique, valid subdomain derived from `seed`.

    `taken` is a callable returning True when a subdomain is in use; it defaults
    to querying `CoreOrganisation`, and is injectable for tests.
    """
    if taken is None:
        def taken(candidate: str) -> bool:
            from terno_dbi.core.models import CoreOrganisation

            return CoreOrganisation.objects.filter(subdomain=candidate).exists()

    base = _slug(seed or "")
    if not SUBDOMAIN_REGEX.match(base):
        # Non-Latin names slugify to an empty string, so seeding from the name
        # is not always possible. "org" plus a counter is ugly but reachable and
        # unique, which beats refusing to create the organisation.
        base = "org"

    candidate, counter = base, 1
    while candidate in RESERVED_SUBDOMAINS or taken(candidate):
        suffix = str(counter)
        candidate = f"{base[:MAX_SUBDOMAIN_LENGTH - len(suffix)].strip('-')}{suffix}"
        counter += 1
    return candidate


def default_org_name(user) -> str:
    """The display name for a personal organisation."""
    label = (
        (getattr(user, "get_full_name", lambda: "")() or "").strip()
        or (getattr(user, "username", "") or "").strip()
        or (getattr(user, "email", "") or "").split("@")[0]
    )
    return f"{label}'s Organisation" if label else "My Organisation"


def _configured_provisioner():
    from django.conf import settings
    from django.utils.module_loading import import_string

    path = getattr(settings, "TERNO_ORG_PROVISIONER", None)
    if not path:
        return None
    try:
        return import_string(path)
    except ImportError:
        logger.exception("TERNO_ORG_PROVISIONER=%r could not be imported", path)
        return None


def ensure_organisation(user, organisation=None):
    """Return this user's organisation, creating a personal one if needed.

    Returns None only when the user has none and no provisioner is configured —
    the caller must then refuse the grant rather than proceeding unscoped.
    """
    from terno_dbi.oauth.minting import resolve_organisation

    existing = resolve_organisation(user, organisation)
    if existing is not None:
        return existing

    provision = _configured_provisioner()
    if provision is None:
        logger.warning(
            "User %s has no organisation and TERNO_ORG_PROVISIONER is not set; "
            "cannot self-serve.", user,
        )
        return None

    logger.info("Provisioning a personal organisation for %s (first connector use)", user)
    created = provision(user)
    if created is None:
        logger.error("Provisioner returned None for %s", user)
    return created
