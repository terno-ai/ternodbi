"""Test helper: an organisation's admin group, the way a host app sets one up.

Admin rights come from permissions on the user's groups in that organisation
(`OrganisationUser.groups`), see `terno_dbi.core.org_permissions`.
"""

from terno_dbi.core.org_permissions import ADD_DATASOURCE, USE_DBI_ADMIN_MODE


def admin_group(org, name="Org Admin", perms=(ADD_DATASOURCE, USE_DBI_ADMIN_MODE)):
    """A group linked to ``org`` holding ``perms``; named "<org_id>:<name>"."""
    from django.contrib.auth.models import Group, Permission

    from terno_dbi.core.models import OrganisationGroup

    group, _ = Group.objects.get_or_create(name=f"{org.pk}:{name}")
    for perm in perms:
        app_label, _, codename = perm.partition(".")
        group.permissions.add(
            Permission.objects.get(content_type__app_label=app_label, codename=codename))
    OrganisationGroup.objects.get_or_create(organisation=org, group=group)
    return group


def make_org_admin(user, org):
    """Give ``user`` the admin group on their membership in ``org`` (created if missing)."""
    from terno_dbi.core.models import OrganisationUser

    membership, _ = OrganisationUser.objects.get_or_create(user=user, organisation=org)
    group = admin_group(org)
    membership.groups.add(group)
    return membership
