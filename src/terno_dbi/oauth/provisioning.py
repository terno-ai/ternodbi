"""Create a personal organisation for users who arrive through OAuth without one.

A user can reach TernoDBI through an OAuth client before having a Terno
organisation. In that case, the OAuth flow needs to create one before a grant
can be scoped.

Each new user gets a personal organisation. Existing organisations are never
selected based on email domain, since domain matching does not prove
membership and could grant access to another organisation's data.

Organisation creation is provided by the host application through
`TERNO_ORG_PROVISIONER`, since the required organisation models and setup logic
are not part of TernoDBI. If no provisioner is configured, no organisation is
created and the grant is refused.
"""

import logging
import re
from typing import Optional
from django.utils.text import slugify
from django.conf import settings
from django.utils.module_loading import import_string

logger = logging.getLogger(__name__)

# Mirrors terno-web's provisioner so a connector-created subdomain is
# indistinguishable from a web-created one.
RESERVED_SUBDOMAINS = {"admin", "www", "api", "support", "help", "mcp", "app"}
SUBDOMAIN_REGEX = re.compile(r"^(?!-)[a-z0-9-]{1,25}(?<!-)$")
MAX_SUBDOMAIN_LENGTH = 25


def _slug(value: str) -> str:    

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
