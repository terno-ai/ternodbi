"""
Django models for DBI Layer.

These provide abstract base classes that TernoAI inherits from.
TernoAI owns the concrete models and database tables.
"""

from django.db import models
from django.utils.translation import gettext_lazy as _
from django.contrib.auth.models import Group, User


# =============================================================================
# Abstract Base Classes for Organisation (TernoAI inherits these)
# =============================================================================

class OrganisationBase(models.Model):
    """
    Abstract base class for Organisation.
    
    TernoAI creates concrete Organisation that inherits from this.
    This class defines the core fields for multi-tenancy.
    """
    name = models.CharField(max_length=255)
    subdomain = models.CharField(max_length=100, unique=True)
    owner = models.ForeignKey(
        User, 
        on_delete=models.CASCADE, 
        related_name='organisation'  # Match original TernoAI model
    )
    verified = models.BooleanField(default=True)
    is_active = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, blank=True, null=True)

    class Meta:
        abstract = True

    def __str__(self):
        return f"{self.name} - {self.subdomain}"


class OrganisationUserBase(models.Model):
    """
    Abstract base class for Organisation-User relationship.
    
    TernoAI creates concrete OrganisationUser that inherits from this.
    Child class must define: organisation = ForeignKey(Organisation, ...)
    """
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, blank=True, null=True)

    class Meta:
        abstract = True

    def __str__(self):
        return f"{self.user.username}"


class OrganisationDataSourceBase(models.Model):
    """
    Abstract base class for Organisation-DataSource relationship.
    
    TernoAI creates concrete OrganisationDataSource that inherits from this.
    Child class must define:
        - organisation = ForeignKey(Organisation, ...)
        - datasource = ForeignKey(DataSource, ...)
    """
    # Note: datasource FK must be defined in child class to avoid cross-app reference issues
    created_at = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, blank=True, null=True)
    is_external = models.BooleanField(default=False)

    class Meta:
        abstract = True

    def __str__(self):
        return f"OrganisationDataSource {self.id}"


class OrganisationGroupBase(models.Model):
    """
    Abstract base class for Organisation-Group relationship.
    
    TernoAI creates concrete OrganisationGroup that inherits from this.
    Child class must define: organisation = ForeignKey(Organisation, ...)
    """
    group = models.ForeignKey(Group, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, blank=True, null=True)

    class Meta:
        abstract = True

    def __str__(self):
        return f"{self.group.name}"


# =============================================================================
# Concrete DataSource Models (owned by TernoDBI)
# =============================================================================


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


class DatasourceSuggestions(models.Model):
    """Query suggestions for a datasource."""
    data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE)
    suggestion = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'terno_datasourcesuggestions'

    def __str__(self):
        return f"{self.data_source.display_name}: {self.suggestion[:50] if self.suggestion else ''}"



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


class ServiceToken(models.Model):
    """
    API tokens for authenticating service requests.
    
    Two token types:
    - admin: Full access (create tokens, rename tables, hide columns, etc.)
    - query: Read-only access (list datasources, tables, execute queries)
    """
    
    class TokenType(models.TextChoices):
        ADMIN = 'admin', _('Admin Service')
        QUERY = 'query', _('Query Service')
    
    # Token key (stored as hash, prefix shown for identification)
    key_hash = models.CharField(
        max_length=128, 
        unique=True, 
        db_index=True,
        help_text="SHA-256 hash of the token key"
    )
    key_prefix = models.CharField(
        max_length=10,
        help_text="First 8 chars of token for identification"
    )
    
    # Token metadata
    name = models.CharField(
        max_length=100,
        help_text="Friendly name for the token"
    )
    token_type = models.CharField(
        max_length=10,
        choices=TokenType.choices,
        default=TokenType.QUERY
    )
    
    # Scope (null = global access to all datasources)
    datasources = models.ManyToManyField(
        DataSource,
        blank=True,
        related_name='service_tokens',
        help_text="If empty, token has global access. Otherwise limited to these datasources."
    )
    
    # Audit fields
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='created_tokens'
    )
    expires_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Token expiry time. Null means never expires."
    )
    last_used = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Last time this token was used"
    )
    is_active = models.BooleanField(
        default=True,
        help_text="Set to False to revoke the token"
    )
    
    class Meta:
        db_table = 'dbi_servicetoken'
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.name} ({self.token_type}) - {self.key_prefix}..."
    
    @classmethod
    def generate_key(cls):
        """Generate a new random token key."""
        import secrets
        return f"dbi_sk_{secrets.token_hex(24)}"
    
    @classmethod
    def hash_key(cls, key):
        """Hash a token key for storage."""
        import hashlib
        return hashlib.sha256(key.encode()).hexdigest()
    
    def has_access_to(self, datasource):
        """Check if token has access to a specific datasource."""
        # Global access if no datasources specified
        if not self.datasources.exists():
            return True
        return self.datasources.filter(id=datasource.id).exists()
