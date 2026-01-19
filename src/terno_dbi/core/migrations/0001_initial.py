# TernoDBI Initial Migration - Conditional Table Creation
#
# This migration handles two scenarios:
# 1. Existing TernoAI: Tables already exist → skip creation
# 2. New TernoDBI: Tables don't exist → create them
#
# Uses SeparateDatabaseAndState to:
# - Update Django's state (knows about models)
# - Conditionally create database tables (only if they don't exist)

from django.db import migrations, models, connection
import django.db.models.deletion


def table_exists(table_name):
    """Check if a table exists in the database."""
    with connection.cursor() as cursor:
        tables = connection.introspection.table_names(cursor)
        return table_name in tables


def create_tables_if_needed(apps, schema_editor):
    """
    Create database tables only if they don't already exist.
    
    For existing TernoAI installations: tables exist, skip creation.
    For new TernoDBI installations: create all tables.
    """
    # Order matters! Create tables with no FK dependencies first
    creation_order = [
        ('DataSource', 'terno_datasource'),
        ('Table', 'terno_table'),
        ('TableColumn', 'terno_tablecolumn'),
        ('DatasourceSuggestions', 'terno_datasourcesuggestions'),
        ('ForeignKey', 'terno_foreignkey'),
        ('PrivateTableSelector', 'terno_privatetableselector'),
        ('GroupTableSelector', 'terno_grouptableselector'),
        ('PrivateColumnSelector', 'terno_privatecolumnselector'),
        ('GroupColumnSelector', 'terno_groupcolumnselector'),
        ('TableRowFilter', 'terno_tablerowfilter'),
        ('GroupTableRowFilter', 'terno_grouptablerowfilter'),
        ('ServiceToken', 'dbi_servicetoken'),
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


def add_constraints_if_needed(apps, schema_editor):
    """Add constraints if they don't already exist."""
    from django.db import connection
    
    constraints_to_add = [
        ('terno_table', 'unique_table_public_name_per_datasource'),
        ('terno_tablecolumn', 'unique_column_public_name_per_table'),
    ]
    
    for table_name, constraint_name in constraints_to_add:
        # Check if constraint already exists (SQLite-specific check)
        try:
            with connection.cursor() as cursor:
                # For SQLite, constraints are embedded in table schema
                # For other DBs, you'd check information_schema
                pass  # Constraints are created when create_model is called
        except Exception:
            pass


class Migration(migrations.Migration):
    
    initial = True
    
    dependencies = [
        ('auth', '0012_alter_user_first_name_max_length'),
        ('contenttypes', '0002_remove_content_type_name'),
    ]
    
    operations = [
        # =====================================================================
        # Use SeparateDatabaseAndState: 
        # - state_operations: Update Django's internal model registry
        # - database_operations: Empty (we'll use RunPython to conditionally create)
        # =====================================================================
        migrations.SeparateDatabaseAndState(
            state_operations=[
                # DataSource
                migrations.CreateModel(
                    name='DataSource',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('display_name', models.CharField(default='Datasource 1', max_length=40)),
                        ('type', models.CharField(
                            choices=[
                                ('generic', 'Generic'), ('oracle', 'Oracle'),
                                ('mysql', 'MySQL'), ('postgres', 'Postgres'),
                                ('bigquery', 'BigQuery'), ('databricks', 'DataBricks'),
                                ('snowflake', 'Snowflake'),
                            ],
                            default='generic', max_length=20
                        )),
                        ('is_erp', models.BooleanField(default=False, help_text='Flag to indicate if the datasource is an ERP system.')),
                        ('connection_str', models.TextField(help_text='Connection string for the datasource', max_length=1000)),
                        ('connection_json', models.JSONField(blank=True, help_text='JSON key file contents for authentication', null=True)),
                        ('description', models.TextField(blank=True, default='', help_text='Give description of your datasource/schema.', max_length=1024, null=True)),
                        ('enabled', models.BooleanField(default=True)),
                        ('dialect_name', models.CharField(blank=True, default='', help_text='Auto-generated on save', max_length=20, null=True)),
                        ('dialect_version', models.CharField(blank=True, default='', help_text='Auto-generated on save', max_length=20, null=True)),
                    ],
                    options={'db_table': 'terno_datasource'},
                ),
                
                # Table
                migrations.CreateModel(
                    name='Table',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('name', models.CharField(max_length=255)),
                        ('public_name', models.CharField(blank=True, max_length=255, null=True)),
                        ('description', models.CharField(blank=True, max_length=500, null=True)),
                        ('complete_description', models.BooleanField(default=False, help_text='Denotes if description is generated for the table and all its columns.')),
                        ('category', models.CharField(blank=True, max_length=255, null=True)),
                        ('sample_rows', models.JSONField(blank=True, null=True)),
                        ('description_updated_at', models.DateTimeField(blank=True, null=True)),
                        ('data_source', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='core.datasource')),
                    ],
                    options={'db_table': 'terno_table'},
                ),
                
                # TableColumn
                migrations.CreateModel(
                    name='TableColumn',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('name', models.CharField(max_length=255)),
                        ('public_name', models.CharField(blank=True, max_length=255, null=True)),
                        ('data_type', models.CharField(blank=True, max_length=50)),
                        ('description', models.CharField(blank=True, max_length=300, null=True)),
                        ('unique_categories', models.JSONField(blank=True, default=dict, null=True)),
                        ('table', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='core.table')),
                    ],
                    options={'db_table': 'terno_tablecolumn'},
                ),
                
                migrations.CreateModel(
                    name='DatasourceSuggestions',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('suggestion', models.TextField(blank=True, null=True)),
                        ('data_source', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='core.datasource')),
                    ],
                    options={'db_table': 'terno_datasourcesuggestions'},
                ),
                
                # ForeignKey
                migrations.CreateModel(
                    name='ForeignKey',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('constrained_table', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='contrained_table', to='core.table')),
                        ('constrained_columns', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='contrained_columns', to='core.tablecolumn')),
                        ('referred_table', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='referred_table', to='core.table')),
                        ('referred_columns', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='referred_columns', to='core.tablecolumn')),
                        ('referred_schema', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='core.datasource')),
                    ],
                    options={'db_table': 'terno_foreignkey'},
                ),
                
                # PrivateTableSelector
                migrations.CreateModel(
                    name='PrivateTableSelector',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('data_source', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='core.datasource')),
                        ('tables', models.ManyToManyField(blank=True, related_name='private_tables', to='core.table')),
                    ],
                    options={'db_table': 'terno_privatetableselector'},
                ),
                
                # GroupTableSelector
                migrations.CreateModel(
                    name='GroupTableSelector',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('group', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='auth.group')),
                        ('tables', models.ManyToManyField(blank=True, related_name='include_tables', to='core.table')),
                        ('exclude_tables', models.ManyToManyField(blank=True, related_name='exclude_tables', to='core.table')),
                    ],
                    options={'db_table': 'terno_grouptableselector'},
                ),
                
                # PrivateColumnSelector
                migrations.CreateModel(
                    name='PrivateColumnSelector',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('data_source', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='core.datasource')),
                        ('columns', models.ManyToManyField(blank=True, related_name='private_columns', to='core.tablecolumn')),
                    ],
                    options={'db_table': 'terno_privatecolumnselector'},
                ),
                
                # GroupColumnSelector
                migrations.CreateModel(
                    name='GroupColumnSelector',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('group', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='auth.group')),
                        ('columns', models.ManyToManyField(blank=True, related_name='include_columns', to='core.tablecolumn')),
                        ('exclude_columns', models.ManyToManyField(blank=True, related_name='exclude_columns', to='core.tablecolumn')),
                    ],
                    options={'db_table': 'terno_groupcolumnselector'},
                ),
                
                # TableRowFilter
                migrations.CreateModel(
                    name='TableRowFilter',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('filter_str', models.CharField(max_length=300)),
                        ('data_source', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='core.datasource')),
                        ('table', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='core.table')),
                    ],
                    options={'db_table': 'terno_tablerowfilter'},
                ),
                
                # GroupTableRowFilter
                migrations.CreateModel(
                    name='GroupTableRowFilter',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('filter_str', models.CharField(max_length=300)),
                        ('data_source', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='core.datasource')),
                        ('table', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='core.table')),
                        ('group', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='auth.group')),
                    ],
                    options={'db_table': 'terno_grouptablerowfilter'},
                ),
                
                # ServiceToken (NEW in TernoDBI)
                migrations.CreateModel(
                    name='ServiceToken',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('key_hash', models.CharField(db_index=True, help_text='SHA-256 hash of the token key', max_length=128, unique=True)),
                        ('key_prefix', models.CharField(help_text='First 8 chars of token for identification', max_length=10)),
                        ('name', models.CharField(help_text='Friendly name for the token', max_length=100)),
                        ('token_type', models.CharField(choices=[('admin', 'Admin Service'), ('query', 'Query Service')], default='query', max_length=10)),
                        ('created_at', models.DateTimeField(auto_now_add=True)),
                        ('expires_at', models.DateTimeField(blank=True, help_text='Token expiry time. Null means never expires.', null=True)),
                        ('last_used', models.DateTimeField(blank=True, help_text='Last time this token was used', null=True)),
                        ('is_active', models.BooleanField(default=True, help_text='Set to False to revoke the token')),
                        ('created_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='created_tokens', to='auth.user')),
                        ('datasources', models.ManyToManyField(blank=True, help_text='If empty, token has global access. Otherwise limited to these datasources.', related_name='service_tokens', to='core.datasource')),
                    ],
                    options={'db_table': 'dbi_servicetoken', 'ordering': ['-created_at']},
                ),
                
                # Constraints
                migrations.AddConstraint(
                    model_name='table',
                    constraint=models.UniqueConstraint(
                        condition=models.Q(('public_name__isnull', True), _negated=True),
                        fields=('data_source', 'public_name'),
                        name='unique_table_public_name_per_datasource'
                    ),
                ),
                migrations.AddConstraint(
                    model_name='tablecolumn',
                    constraint=models.UniqueConstraint(
                        condition=models.Q(('public_name__isnull', True), _negated=True),
                        fields=('table', 'public_name'),
                        name='unique_column_public_name_per_table'
                    ),
                ),
            ],
            database_operations=[
                # Empty! We handle database creation in RunPython below
            ],
        ),
        
        # =====================================================================
        # Conditional table creation - only creates tables that don't exist
        # =====================================================================
        migrations.RunPython(
            create_tables_if_needed,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
