import django.db.models.deletion
from django.db import migrations, models, connection


def table_exists(table_name):
    with connection.cursor() as cursor:
        tables = connection.introspection.table_names(cursor)
        return table_name in tables


def create_prompt_example_table_if_needed(apps, schema_editor):
    table_name = "terno_promptexample"

    if not table_exists(table_name):
        model = apps.get_model("core", "PromptExample")
        schema_editor.create_model(model)


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0011_llmconfiguration'),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.CreateModel(
                    name='PromptExample',
                    fields=[
                        ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                        ('example_type', models.CharField(
                            choices=[
                                ('query_sql', 'Query - SQL'),
                                ('question_plan', 'Question - Plan'),
                                ('input_response', 'Input Response'),
                                ('lib_script', 'Library Script')
                            ],
                            default='query_sql',
                            help_text='Type of example, e.g., Query - SQL or Question - Plan.',
                            max_length=50
                        )),
                        ('key', models.CharField(max_length=255)),
                        ('value', models.TextField()),
                        ('created_at', models.DateTimeField(auto_now_add=True)),
                        ('updated_at', models.DateTimeField(auto_now=True)),
                        ('organisation', models.ForeignKey(
                            blank=True,
                            null=True,
                            on_delete=django.db.models.deletion.CASCADE,
                            related_name='prompt_examples',
                            to='core.coreorganisation'
                        )),
                    ],
                    options={
                        'verbose_name': 'Prompt Example',
                        'verbose_name_plural': 'Prompt Examples',
                        'db_table': 'terno_promptexample',
                     },
                ),
            ],
            database_operations=[],
        ),

        migrations.RunPython(
            create_prompt_example_table_if_needed,
            reverse_code=migrations.RunPython.noop,
        ),
    ]
