"""Seed the connector catalog and attach existing datasources to it.

Runs the same idempotent upsert `refresh_catalog()` performs on every deploy,
then links each pre-existing `DataSource` to its catalog entry by type. The
`post_migrate` signal also refreshes the catalog immediately afterwards, so the
seed here is belt-and-suspenders — but the *attach* must happen in a migration,
because the catalog is empty at this point in a fresh migrate (post_migrate has
not run yet) and unlinked API rows (e.g. GA4) would otherwise be miscategorised
as databases.

Imports the live declarations rather than snapshotting them: the write is an
idempotent upsert of whatever is declared when it runs, filtered to the columns
this historical model actually has, so adding future code-owned fields never
breaks this migration.
"""

from django.db import migrations


def seed_and_attach(apps, schema_editor):
    from terno_dbi.catalog.declarations import DECLARED_CONNECTORS, canonical_key

    ConnectorCatalog = apps.get_model('core', 'ConnectorCatalog')
    DataSource = apps.get_model('core', 'DataSource')

    valid = {f.name for f in ConnectorCatalog._meta.get_fields()}
    for spec in DECLARED_CONNECTORS:
        defaults = {'enabled': spec.default_enabled, **spec.code_owned_fields()}
        defaults = {k: v for k, v in defaults.items() if k in valid}
        ConnectorCatalog.objects.update_or_create(key=spec.key, defaults=defaults)

    ids = dict(ConnectorCatalog.objects.values_list('key', 'id'))
    types = (
        DataSource.objects
        .filter(catalog__isnull=True)
        .values_list('type', flat=True)
        .distinct()
    )
    for db_type in types:
        catalog_id = ids.get(canonical_key(db_type))
        if catalog_id is None:
            continue
        DataSource.objects.filter(
            type=db_type, catalog__isnull=True,
        ).update(catalog_id=catalog_id)


def unseed(apps, schema_editor):
    """Detach before deleting — the FK is PROTECT, so order matters."""
    ConnectorCatalog = apps.get_model('core', 'ConnectorCatalog')
    DataSource = apps.get_model('core', 'DataSource')

    DataSource.objects.filter(catalog__isnull=False).update(catalog=None)
    ConnectorCatalog.objects.all().delete()


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0026_connectorcatalog_datasource_auth_error_and_more'),
    ]

    operations = [
        migrations.RunPython(seed_and_attach, unseed),
    ]
