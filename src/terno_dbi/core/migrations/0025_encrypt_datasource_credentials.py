"""Encrypt existing DataSource credentials at rest.

Migration 0024 changed `connection_str` and `connection_json` to encrypted
fields, so new values are encrypted automatically. This migration updates
older rows that were stored as plaintext.

The fields still support reading existing plaintext values, so both plaintext
and encrypted rows can be read during the migration. Saving a row encrypts
the value, and already-encrypted values are left unchanged.

An encryption key must be configured through `MCP_ENCRYPTION_KEY` or
`MCP_ENCRYPTION_KEYS`. If there are values to encrypt and no key is available,
the migration fails instead of leaving the values as plaintext.

The reverse migration is intentionally a no-op. The encrypted fields can
continue reading both encrypted and remaining plaintext values, so rolling
back this migration does not break reads. Converting the encrypted values back
to plaintext would require reverting the field change from migration 0024 and
running a separate decryption step.
"""

from django.db import migrations


def encrypt_existing(apps, schema_editor):
    DataSource = apps.get_model("core", "DataSource")
    for ds in DataSource.objects.all().iterator():
        DataSource.objects.filter(pk=ds.pk).update(
            connection_str=ds.connection_str,
            connection_json=ds.connection_json,
        )


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0024_alter_datasource_connection_json_and_more"),
    ]

    operations = [
        migrations.RunPython(encrypt_existing, migrations.RunPython.noop),
    ]
