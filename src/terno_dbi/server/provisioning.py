"""Local stand-in for terno-ai's organisation provisioner.

terno-ai's version also creates an `LLMCredit` and the `Organisation` extension
whose `save()` bootstraps membership, staff flag, groups and preferences. None of
those models exist in a standalone install, so this creates the
`CoreOrganisation` and the `OrganisationUser` membership directly — which is all
the connector's authorization chain actually reads.

Used only by the standalone dev server. Production points
`TERNO_ORG_PROVISIONER` at `terno.provisioning.provision_personal_org`.
"""

import logging

from django.db import transaction

from terno_dbi.oauth.provisioning import default_org_name, generate_subdomain

logger = logging.getLogger(__name__)


@transaction.atomic
def provision_local_org(user):
    from terno_dbi.core.models import CoreOrganisation, OrganisationUser

    org = CoreOrganisation.objects.create(
        name=default_org_name(user),
        subdomain=generate_subdomain(getattr(user, "username", "") or "org"),
        owner=user,
        is_active=True,
    )
    OrganisationUser.objects.create(organisation=org, user=user)
    logger.info("Provisioned local organisation '%s' for %s", org.name, user)
    return org
