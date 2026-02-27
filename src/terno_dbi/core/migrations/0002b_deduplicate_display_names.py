# Deduplicate DataSource display_name values
#
# On existing databases, multiple DataSources may share the default
# display_name ("Datasource 1"). This migration renames duplicates
# before 0003 adds the UNIQUE constraint.
#
# Strategy: Keep the first occurrence (lowest ID) unchanged,
# rename others by appending the datasource ID.
# e.g. "Datasource 1" → "Datasource 1 (42)"

import logging
from django.db import migrations, connection

logger = logging.getLogger(__name__)


def deduplicate_display_names(apps, schema_editor):
    """
    Find duplicate display_name values in terno_datasource and rename them.
    """
    with connection.cursor() as cursor:
        # Find all display_name values that appear more than once
        cursor.execute("""
            SELECT display_name, COUNT(*) as cnt
            FROM terno_datasource
            GROUP BY display_name
            HAVING cnt > 1
        """)
        duplicates = cursor.fetchall()

        if not duplicates:
            logger.info("No duplicate display_name values found, skipping dedup")
            return

        for display_name, count in duplicates:
            logger.info(
                "Deduplicating display_name '%s' (%d occurrences)",
                display_name, count
            )
            # Get all IDs with this display_name, ordered by ID
            cursor.execute(
                "SELECT id FROM terno_datasource WHERE display_name = %s ORDER BY id",
                [display_name]
            )
            ids = [row[0] for row in cursor.fetchall()]

            # Keep the first one unchanged, rename the rest
            for ds_id in ids[1:]:
                new_name = f"{display_name} ({ds_id})"
                cursor.execute(
                    "UPDATE terno_datasource SET display_name = %s WHERE id = %s",
                    [new_name, ds_id]
                )
                logger.info(
                    "Renamed DataSource %d: '%s' → '%s'",
                    ds_id, display_name, new_name
                )


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0002_organization_support'),
    ]

    operations = [
        migrations.RunPython(
            deduplicate_display_names,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
