"""Admin rights move from the global "Org Admin" group to per-organisation groups.

Before this release TernoDBI treated a user in the global ``user.groups`` group
"Org Admin" (or ``settings.TERNO_ORG_ADMIN_GROUP``) as an admin in every
organisation they belong to. Admin checks now read the permissions on the
user's groups *in that organisation* (``OrganisationUser.groups``), see
``terno_dbi.core.org_permissions``.

So nobody loses access, every membership of a user in the old global group gets
a per-organisation group "<org_id>:DBI Admin" holding ``core.add_datasource``
and ``core.use_dbi_admin_mode``. The old global group is left as it was; it no
longer grants anything. Nothing happens when the group is missing or empty.
"""

from django.conf import settings
from django.db import migrations

LEGACY_ADMIN_GROUP = getattr(settings, "TERNO_ORG_ADMIN_GROUP", "Org Admin")
ORG_ADMIN_GROUP_NAME = "{org_id}:DBI Admin"


def _permission(apps, model, codename, name):
    ContentType = apps.get_model("contenttypes", "ContentType")
    Permission = apps.get_model("auth", "Permission")
    # Permission rows are normally created after migrate finishes, so make sure
    # they exist now.
    content_type, _ = ContentType.objects.get_or_create(app_label="core", model=model)
    permission, _ = Permission.objects.get_or_create(
        content_type=content_type, codename=codename, defaults={"name": name},
    )
    return permission


def move_global_admins(apps, schema_editor):
    Group = apps.get_model("auth", "Group")
    OrganisationUser = apps.get_model("core", "OrganisationUser")
    OrganisationGroup = apps.get_model("core", "OrganisationGroup")

    legacy = Group.objects.filter(name=LEGACY_ADMIN_GROUP).first()
    if legacy is None:
        return
    user_ids = list(legacy.user_set.values_list("id", flat=True))
    if not user_ids:
        return

    permissions = [
        _permission(apps, "datasource", "add_datasource", "Can add data source"),
        _permission(apps, "coreorganisation", "use_dbi_admin_mode", "Can use DBI admin mode"),
    ]
    for membership in OrganisationUser.objects.filter(user_id__in=user_ids):
        org_id = membership.organisation_id
        group, created = Group.objects.get_or_create(name=ORG_ADMIN_GROUP_NAME.format(org_id=org_id))
        if created:
            group.permissions.set(permissions)
        OrganisationGroup.objects.get_or_create(organisation_id=org_id, group=group)
        membership.groups.add(group)


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0038_connector_request"),
        ("auth", "0012_alter_user_first_name_max_length"),
        ("contenttypes", "0002_remove_content_type_name"),
    ]

    operations = [
        migrations.AlterModelOptions(
            name="coreorganisation",
            options={"permissions": [("use_dbi_admin_mode", "Can use DBI admin mode")]},
        ),
        migrations.RunPython(move_global_admins, migrations.RunPython.noop),
    ]
