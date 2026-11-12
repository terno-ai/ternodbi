"""Per-organisation permission checks and the migration off the global group."""

import importlib

import pytest

from terno_dbi.core import org_permissions
from tests.unit._org_roles import admin_group, make_org_admin


@pytest.fixture
def orgs(db):
    from django.contrib.auth.models import User

    from terno_dbi.core.models import CoreOrganisation, OrganisationUser

    owner = User.objects.create(username="op-owner")
    acme = CoreOrganisation.objects.create(name="Acme", subdomain="op-acme", owner=owner)
    globex = CoreOrganisation.objects.create(name="Globex", subdomain="op-globex", owner=owner)
    user = User.objects.create(username="op-user")
    OrganisationUser.objects.create(user=user, organisation=acme)
    OrganisationUser.objects.create(user=user, organisation=globex)
    return {"owner": owner, "acme": acme, "globex": globex, "user": user}


@pytest.mark.django_db
def test_permission_comes_from_groups_in_that_org_only(orgs):
    make_org_admin(orgs["user"], orgs["acme"])

    assert org_permissions.can_write(orgs["user"], orgs["acme"]) is True
    assert org_permissions.can_manage_connectors(orgs["user"], orgs["acme"]) is True
    assert org_permissions.can_write(orgs["user"], orgs["globex"]) is False
    assert org_permissions.can_manage_connectors(orgs["user"], orgs["globex"]) is False


@pytest.mark.django_db
def test_org_id_works_as_well_as_the_org(orgs):
    make_org_admin(orgs["user"], orgs["acme"])
    assert org_permissions.can_write(orgs["user"], orgs["acme"].pk) is True


@pytest.mark.django_db
def test_a_group_of_another_org_on_the_membership_still_counts_only_here(orgs):
    """The check is the membership, so a group is scoped by where it is attached."""
    from terno_dbi.core.models import OrganisationUser

    group = admin_group(orgs["acme"])
    OrganisationUser.objects.get(user=orgs["user"], organisation=orgs["globex"]).groups.add(group)
    assert org_permissions.can_write(orgs["user"], orgs["globex"]) is True
    assert org_permissions.can_write(orgs["user"], orgs["acme"]) is False


@pytest.mark.django_db
def test_global_groups_and_permissions_grant_nothing(orgs):
    from django.contrib.auth.models import Group, Permission

    global_group = Group.objects.create(name="Org Admin")
    global_group.permissions.add(
        Permission.objects.get(content_type__app_label="core", codename="use_dbi_admin_mode"))
    orgs["user"].groups.add(global_group)
    orgs["user"].user_permissions.add(
        Permission.objects.get(content_type__app_label="core", codename="add_datasource"))

    assert org_permissions.can_write(orgs["user"], orgs["acme"]) is False
    assert org_permissions.can_manage_connectors(orgs["user"], orgs["acme"]) is False


@pytest.mark.django_db
def test_owner_manages_connectors_but_write_needs_the_permission(orgs):
    from terno_dbi.core.models import OrganisationUser

    OrganisationUser.objects.create(user=orgs["owner"], organisation=orgs["acme"])
    assert org_permissions.can_manage_connectors(orgs["owner"], orgs["acme"]) is True
    assert org_permissions.can_write(orgs["owner"], orgs["acme"]) is False


@pytest.mark.django_db
def test_superuser_holds_everything_and_inactive_users_nothing(orgs):
    from django.contrib.auth.models import User

    su = User.objects.create(username="op-su", is_superuser=True)
    assert org_permissions.can_write(su, orgs["acme"]) is True
    assert org_permissions.can_manage_connectors(su, orgs["acme"]) is True

    make_org_admin(orgs["user"], orgs["acme"])
    orgs["user"].is_active = False
    assert org_permissions.can_write(orgs["user"], orgs["acme"]) is False
    assert org_permissions.can_manage_connectors(orgs["user"], orgs["acme"]) is False


@pytest.mark.django_db
def test_no_org_or_no_user_is_denied(orgs):
    assert org_permissions.can_write(orgs["user"], None) is False
    assert org_permissions.can_write(None, orgs["acme"]) is False
    assert not org_permissions.membership_groups(None, orgs["acme"]).exists()


@pytest.mark.django_db
def test_membership_groups_are_the_groups_in_that_org(orgs):
    group = make_org_admin(orgs["user"], orgs["acme"]).groups.get()
    assert list(org_permissions.membership_groups(orgs["user"], orgs["acme"])) == [group]
    assert not org_permissions.membership_groups(orgs["user"], orgs["globex"]).exists()


# --- migration 0039: existing global admins stay admins --------------------------

def _migration():
    return importlib.import_module("terno_dbi.core.migrations.0039_org_permissions")


@pytest.mark.django_db
def test_migration_moves_global_admins_into_each_of_their_orgs(orgs):
    from django.apps import apps
    from django.contrib.auth.models import Group, User

    from terno_dbi.core.models import OrganisationGroup

    legacy = Group.objects.create(name="Org Admin")
    orgs["user"].groups.add(legacy)
    bystander = User.objects.create(username="op-bystander")
    from terno_dbi.core.models import OrganisationUser
    OrganisationUser.objects.create(user=bystander, organisation=orgs["acme"])

    _migration().move_global_admins(apps, None)

    for org in (orgs["acme"], orgs["globex"]):
        assert org_permissions.can_write(orgs["user"], org) is True
        assert org_permissions.can_manage_connectors(orgs["user"], org) is True
        group = Group.objects.get(name=f"{org.pk}:DBI Admin")
        assert OrganisationGroup.objects.filter(organisation=org, group=group).exists()
    assert org_permissions.can_write(bystander, orgs["acme"]) is False
    # The old group is left alone.
    assert orgs["user"].groups.filter(pk=legacy.pk).exists()

    # Running it again changes nothing.
    _migration().move_global_admins(apps, None)
    assert Group.objects.filter(name__endswith=":DBI Admin").count() == 2


@pytest.mark.django_db
def test_migration_does_nothing_without_global_admins(orgs):
    from django.apps import apps
    from django.contrib.auth.models import Group

    _migration().move_global_admins(apps, None)
    Group.objects.create(name="Org Admin")
    _migration().move_global_admins(apps, None)
    assert not Group.objects.filter(name__endswith=":DBI Admin").exists()
