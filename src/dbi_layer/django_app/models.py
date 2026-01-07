"""
Django models for DBI Layer.

These models are migrated from TernoAI. They use the same db_table names
to preserve existing data when TernoAI imports from DBI Layer.
"""

from django.db import models
from django.utils.translation import gettext_lazy as _
from django.contrib.auth.models import Group


class DataSource(models.Model):
    """Database connection configuration."""
    
    class DBType(models.TextChoices):
        default = "generic", _("Generic")
        Oracle = "oracle", _("Oracle")
        MSSQL = "mysql", _("MySQL")
        postgres = "postgres", _("Postgres")
        bigquery = "bigquery", _("BigQuery")
        databricks = "databricks", _("DataBricks")
        snowflake = "snowflake", _("Snowflake")
    
    display_name = models.CharField(max_length=40, default='Datasource 1')
    type = models.CharField(max_length=20, choices=DBType,
                            default=DBType.default)
    is_erp = models.BooleanField(
        default=False,
        help_text="Flag to indicate if the datasource is an ERP system."
    )
    connection_str = models.TextField(
        max_length=1000, help_text="Connection string for the datasource")
    connection_json = models.JSONField(
        null=True, blank=True,
        help_text="JSON key file contents for authentication")
    description = models.TextField(
        max_length=1024, null=True, blank=True, default='',
        help_text="Give description of your datasource/schema.")
    enabled = models.BooleanField(default=True)
    dialect_name = models.CharField(
        max_length=20, null=True, blank=True, default='',
        help_text="Auto-generated on save")
    dialect_version = models.CharField(
        max_length=20, null=True, blank=True, default='',
        help_text="Auto-generated on save")

    class Meta:
        db_table = 'terno_datasource'  # Keep same table for data preservation

    def __str__(self):
        return self.display_name


class Table(models.Model):
    """Model to represent a table in the data source."""
    
    name = models.CharField(max_length=255)
    public_name = models.CharField(max_length=255, null=True, blank=True)
    data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE)
    description = models.CharField(max_length=500, null=True, blank=True)
    complete_description = models.BooleanField(
        default=False,
        help_text="Denotes if description is generated for the table and all its columns."
    )
    category = models.CharField(max_length=255, null=True, blank=True)
    sample_rows = models.JSONField(null=True, blank=True)
    description_updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = 'terno_table'
        constraints = [
            models.UniqueConstraint(
                fields=["data_source", "public_name"],
                condition=~models.Q(public_name__isnull=True),
                name="unique_table_public_name_per_datasource"
            )
        ]

    def __str__(self):
        return f"{self.data_source.display_name} - {self.name}"


class TableColumn(models.Model):
    """Model to represent a column in a table."""
    
    name = models.CharField(max_length=255)
    public_name = models.CharField(max_length=255, null=True, blank=True)
    table = models.ForeignKey(Table, on_delete=models.CASCADE)
    data_type = models.CharField(max_length=50, blank=True)
    description = models.CharField(max_length=300, null=True, blank=True)
    unique_categories = models.JSONField(default=dict, null=True, blank=True)

    class Meta:
        db_table = 'terno_tablecolumn'
        constraints = [
            models.UniqueConstraint(
                fields=["table", "public_name"],
                condition=~models.Q(public_name__isnull=True),
                name="unique_column_public_name_per_table"
            )
        ]

    def __str__(self):
        return f"{self.table} - {self.name}"


class ForeignKey(models.Model):
    """Foreign key relationship between tables."""
    
    constrained_table = models.ForeignKey(
        Table, on_delete=models.CASCADE,
        related_name='contrained_table',
        null=True, blank=True
    )
    constrained_columns = models.ForeignKey(
        TableColumn, on_delete=models.CASCADE,
        related_name='contrained_columns'
    )
    referred_table = models.ForeignKey(
        Table, on_delete=models.CASCADE,
        related_name='referred_table'
    )
    referred_columns = models.ForeignKey(
        TableColumn, on_delete=models.CASCADE,
        related_name='referred_columns'
    )
    referred_schema = models.ForeignKey(
        DataSource, on_delete=models.CASCADE,
        null=True, blank=True
    )

    class Meta:
        db_table = 'terno_foreignkey'


class PrivateTableSelector(models.Model):
    """Model for user to select private tables (hidden globally)."""
    
    data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE)
    tables = models.ManyToManyField(
        Table, blank=True,
        related_name='private_tables'
    )

    class Meta:
        db_table = 'terno_privatetableselector'

    def __str__(self):
        return f'{self.data_source}'


class GroupTableSelector(models.Model):
    """Per-group table access control."""
    
    group = models.ForeignKey(Group, on_delete=models.CASCADE)
    tables = models.ManyToManyField(
        Table, blank=True,
        related_name='include_tables'
    )
    exclude_tables = models.ManyToManyField(
        Table, blank=True,
        related_name='exclude_tables'
    )

    class Meta:
        db_table = 'terno_grouptableselector'

    def __str__(self) -> str:
        return f'{self.group.name}'


class PrivateColumnSelector(models.Model):
    """Model for user to select private columns (hidden globally)."""
    
    data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE)
    columns = models.ManyToManyField(
        TableColumn, blank=True,
        related_name='private_columns'
    )

    class Meta:
        db_table = 'terno_privatecolumnselector'

    def __str__(self):
        return f'{self.data_source}'


class GroupColumnSelector(models.Model):
    """Per-group column access control."""
    
    group = models.ForeignKey(Group, on_delete=models.CASCADE)
    columns = models.ManyToManyField(
        TableColumn, blank=True,
        related_name='include_columns'
    )
    exclude_columns = models.ManyToManyField(
        TableColumn, blank=True,
        related_name='exclude_columns'
    )

    class Meta:
        db_table = 'terno_groupcolumnselector'

    def __str__(self) -> str:
        return f'{self.group.name}'


class GroupTableRowFilter(models.Model):
    """Row-level filters per group."""
    
    data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE)
    table = models.ForeignKey(Table, on_delete=models.CASCADE)
    group = models.ForeignKey(Group, on_delete=models.CASCADE)
    filter_str = models.CharField(max_length=300)

    class Meta:
        db_table = 'terno_grouptablerowfilter'


class TableRowFilter(models.Model):
    """Global row filters for tables."""
    
    data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE)
    table = models.ForeignKey(Table, on_delete=models.CASCADE)
    filter_str = models.CharField(max_length=300)

    class Meta:
        db_table = 'terno_tablerowfilter'
