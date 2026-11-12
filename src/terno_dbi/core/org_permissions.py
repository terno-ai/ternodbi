"""Who may do what inside one organisation.

A user's roles in an organisation are the Django Groups on their membership
(``OrganisationUser.groups``), not their global ``user.groups``: the same person
can be an admin in one organisation and a plain member in another. A group
grants whatever Django permissions it holds, so the host application is free to
name and shape its groups however it likes; TernoDBI only asks which
permissions the user's groups hold in that organisation.

The permissions TernoDBI checks:

- ``core.add_datasource`` — connect and manage connector sources. The
  organisation's owner may always do this.
- ``core.use_dbi_admin_mode`` — write access (admin tools over MCP/OAuth).

An active superuser holds every permission in every organisation.
"""

ADD_DATASOURCE = "core.add_datasource"
USE_DBI_ADMIN_MODE = "core.use_dbi_admin_mode"


def _org_id(org):
    return getattr(org, "pk", org)


def _is_active_user(user) -> bool:
    return (
        user is not None
        and getattr(user, "is_authenticated", False)
        and getattr(user, "is_active", False)
    )


def membership_groups(user, org):
    """The user's groups in ``org``; none when they are not a member."""
    from django.contrib.auth.models import Group

    org_id = _org_id(org)
    if org_id is None or not _is_active_user(user):
        return Group.objects.none()
    return Group.objects.filter(
        org_user_memberships__user_id=user.pk,
        org_user_memberships__organisation_id=org_id,
    ).distinct()


def has_org_permission(user, perm: str, org) -> bool:
    """Does one of the user's groups in ``org`` hold ``perm`` ("app_label.codename")?"""
    if org is None or not _is_active_user(user):
        return False
    if user.is_superuser:
        return True
    from django.contrib.auth.models import Permission

    app_label, _, codename = perm.partition(".")
    return Permission.objects.filter(
        content_type__app_label=app_label,
        codename=codename,
        group__org_user_memberships__user_id=user.pk,
        group__org_user_memberships__organisation_id=_org_id(org),
    ).exists()


def can_manage_connectors(user, org) -> bool:
    """Connect, disconnect and configure connector sources for ``org``."""
    if org is None or not _is_active_user(user):
        return False
    if getattr(org, "owner_id", None) == user.pk:
        return True
    return has_org_permission(user, ADD_DATASOURCE, org)


def can_write(user, org) -> bool:
    """Write access (admin tools) in ``org``."""
    return has_org_permission(user, USE_DBI_ADMIN_MODE, org)
