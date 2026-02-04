# TernoDBI Organization Support Migration
# Creates organization-related tables for multi-tenancy support.
# Uses conditional creation to handle existing installations.

from django.db import migrations, models, connection
import django.db.models.deletion


def table_exists(table_name):
    """Check if a table exists in the database."""
    with connection.cursor() as cursor:
        tables = connection.introspection.table_names(cursor)
        return table_name in tables


def create_org_tables_if_needed(apps, schema_editor):
    """
    Create organization tables only if they don't already exist.
    """
    creation_order = [
        ('CoreOrganisation', 'core_organisation'),
        ('OrganisationUser', 'core_organisationuser'),
        ('OrganisationGroup', 'core_organisationgroup'),
    ]
    
    for model_name, table_name in creation_order:
        if table_exists(table_name):
            print(f"  ✓ Table '{table_name}' exists, skipping creation")
        else:
            print(f"  → Creating table '{table_name}'")
            try:
                model = apps.get_model('core', model_name)
                schema_editor.create_model(model)
            except Exception as e:
                print(f"  ✗ Error creating {table_name}: {e}")
                raise


def add_datasource_org_fk(apps, schema_editor):
    """
    Add organisation FK to DataSource table if it doesn't exist.
    Uses schema_editor for cross-database compatibility (SQLite, PostgreSQL, MySQL).
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Check if column exists using Django's cross-database introspection API
    with connection.cursor() as cursor:
        columns = [
            col.name for col in
            connection.introspection.get_table_description(cursor, 'terno_datasource')
        ]
    
    if 'organisation_id' in columns:
        logger.info("Column 'organisation_id' exists in terno_datasource, skipping")
        return
    
    logger.info("Adding 'organisation_id' column to terno_datasource")
    
    # Use schema_editor.add_field() - Django handles SQL dialect automatically
    DataSource = apps.get_model('core', 'DataSource')
    CoreOrganisation = apps.get_model('core', 'CoreOrganisation')
    
    field = models.ForeignKey(
        CoreOrganisation,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='datasources',
        db_column='organisation_id'
    )
    field.set_attributes_from_name('organisation')
    schema_editor.add_field(DataSource, field)
    
    logger.info("Successfully added 'organisation_id' column")


class Migration(migrations.Migration):
    
    dependencies = [
        ('auth', '0012_alter_user_first_name_max_length'),
        ('core', '0001_initial'),
    ]
    
    operations = [
        # State operations for new models
        migrations.SeparateDatabaseAndState(
            state_operations=[
                # CoreOrganisation
                migrations.CreateModel(
                    name='CoreOrganisation',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('name', models.CharField(max_length=255)),
                        ('subdomain', models.CharField(max_length=100, unique=True)),
                        ('owner', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='core_organisations', to='auth.user')),
                        ('verified', models.BooleanField(default=True)),
                        ('is_active', models.BooleanField(default=False)),
                        ('created_at', models.DateTimeField(auto_now_add=True, blank=True, null=True)),
                        ('updated_at', models.DateTimeField(auto_now=True, blank=True, null=True)),
                    ],
                    options={'db_table': 'core_organisation'},
                ),
                
                # OrganisationUser
                migrations.CreateModel(
                    name='OrganisationUser',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('organisation', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='organisation_users', to='core.coreorganisation')),
                        ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='auth.user')),
                        ('created_at', models.DateTimeField(auto_now_add=True, blank=True, null=True)),
                        ('updated_at', models.DateTimeField(auto_now=True, blank=True, null=True)),
                    ],
                    options={'db_table': 'core_organisationuser'},
                ),
                
                # OrganisationGroup
                migrations.CreateModel(
                    name='OrganisationGroup',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('organisation', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='organisation_groups', to='core.coreorganisation')),
                        ('group', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='auth.group')),
                        ('created_at', models.DateTimeField(auto_now_add=True, blank=True, null=True)),
                        ('updated_at', models.DateTimeField(auto_now=True, blank=True, null=True)),
                    ],
                    options={'db_table': 'core_organisationgroup'},
                ),
                
                # Add organisation FK to DataSource
                migrations.AddField(
                    model_name='datasource',
                    name='organisation',
                    field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='datasources', to='core.coreorganisation', help_text='Organisation this datasource belongs to'),
                ),
                
                # Constraints
                migrations.AddConstraint(
                    model_name='organisationuser',
                    constraint=models.UniqueConstraint(fields=['organisation', 'user'], name='core_unique_org_user'),
                ),
            ],
            database_operations=[],
        ),
        
        # Conditional table creation
        migrations.RunPython(
            create_org_tables_if_needed,
            reverse_code=migrations.RunPython.noop,
        ),
        
        # Add FK column to DataSource
        migrations.RunPython(
            add_datasource_org_fk,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
