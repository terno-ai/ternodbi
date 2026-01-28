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
    """
    # Check if column exists
    with connection.cursor() as cursor:
        if connection.vendor == 'sqlite':
            cursor.execute("PRAGMA table_info(terno_datasource);")
            columns = [row[1] for row in cursor.fetchall()]
        else:
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'terno_datasource' AND column_name = 'organisation_id'
            """)
            columns = [row[0] for row in cursor.fetchall()]
    
    if 'organisation_id' in columns:
        print("  ✓ Column 'organisation_id' exists in terno_datasource, skipping")
    else:
        print("  → Adding 'organisation_id' column to terno_datasource")
        if connection.vendor == 'sqlite':
            cursor = connection.cursor()
            cursor.execute("""
                ALTER TABLE terno_datasource 
                ADD COLUMN organisation_id BIGINT NULL 
                REFERENCES core_organisation(id) ON DELETE CASCADE
            """)
        else:
            cursor = connection.cursor()
            cursor.execute("""
                ALTER TABLE terno_datasource 
                ADD COLUMN organisation_id BIGINT NULL,
                ADD CONSTRAINT fk_datasource_org 
                FOREIGN KEY (organisation_id) REFERENCES core_organisation(id) ON DELETE CASCADE
            """)


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
