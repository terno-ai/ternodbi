from django.db import models
from django.db.models import Q
from django.utils.translation import gettext_lazy as _
from django.contrib.auth.models import Group, User
import logging

logger = logging.getLogger(__name__)


class CoreOrganisation(models.Model):
    name = models.CharField(max_length=255)
    subdomain = models.CharField(max_length=100, unique=True)
    owner = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='core_organisations'
    )
    verified = models.BooleanField(default=True)
    is_active = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'core_organisation'

    def __str__(self):
        return f"{self.name} - {self.subdomain}"


class OrganisationUser(models.Model):
    organisation = models.ForeignKey(
        CoreOrganisation,
        on_delete=models.CASCADE,
        related_name='organisation_users'
    )
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'core_organisationuser'
        constraints = [
            models.UniqueConstraint(
                fields=['organisation', 'user'],
                name='core_unique_org_user'
            )
        ]

    def __str__(self):
        return f"{self.user.username}"


class OrganisationGroup(models.Model):
    organisation = models.ForeignKey(
        CoreOrganisation,
        on_delete=models.CASCADE,
        related_name='organisation_groups'
    )
    group = models.ForeignKey(Group, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'core_organisationgroup'

    def __str__(self):
        return f"{self.group.name}"


class DataSource(models.Model):

    class DBType(models.TextChoices):
        default = "generic", _("Generic")
        Oracle = "oracle", _("Oracle")
        MSSQL = "mysql", _("MySQL")
        postgres = "postgres", _("Postgres")
        bigquery = "bigquery", _("BigQuery")
        databricks = "databricks", _("DataBricks")
        snowflake = "snowflake", _("Snowflake")

    display_name = models.CharField(max_length=40, default='Datasource 1', unique=True)
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
    organisation = models.ForeignKey(
        CoreOrganisation,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='datasources',
        help_text="Organisation this datasource belongs to"
    )
    is_global = models.BooleanField(
        default=False,
        help_text="If True, this datasource is accessible by all organisations (read-only)."
    )

    class Meta:
        db_table = 'terno_datasource'

    def __str__(self):
        return self.display_name


class Table(models.Model):
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
    estimated_row_count = models.BigIntegerField(
        null=True, blank=True,
        help_text="Approximate row count fetched during metadata sync."
    )
    is_hidden = models.BooleanField(
        default=False,
        help_text="If True, this table is globally hidden from all API consumers and the AI agent."
    )
    description_updated_at = models.DateTimeField(blank=True, null=True)
    metadata_updated_at = models.DateTimeField(auto_now=True, blank=True, null=True)

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
    name = models.CharField(max_length=255)
    public_name = models.CharField(max_length=255, null=True, blank=True)
    table = models.ForeignKey(Table, on_delete=models.CASCADE)
    data_type = models.CharField(max_length=50, blank=True)
    description = models.CharField(max_length=300, null=True, blank=True)
    unique_categories = models.JSONField(default=dict, null=True, blank=True)
    primary_key = models.BooleanField(default=False)
    is_hidden = models.BooleanField(
        default=False,
        help_text="If True, this column is globally hidden from all API consumers and the AI agent."
    )
    metadata_updated_at = models.DateTimeField(auto_now=True, blank=True, null=True)

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
    data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE)
    table = models.ForeignKey(Table, on_delete=models.CASCADE)
    group = models.ForeignKey(Group, on_delete=models.CASCADE)
    filter_str = models.CharField(max_length=300)

    class Meta:
        db_table = 'terno_grouptablerowfilter'


class TableRowFilter(models.Model):
    data_source = models.ForeignKey(DataSource, on_delete=models.CASCADE)
    table = models.ForeignKey(Table, on_delete=models.CASCADE)
    filter_str = models.CharField(max_length=300)

    class Meta:
        db_table = 'terno_tablerowfilter'


class ServiceToken(models.Model):
    class TokenType(models.TextChoices):
        ADMIN = 'admin', _('Admin Service')
        QUERY = 'query', _('Query Service')

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
    name = models.CharField(
        max_length=100,
        help_text="Friendly name for the token"
    )
    token_type = models.CharField(
        max_length=10,
        choices=TokenType.choices,
        default=TokenType.QUERY
    )
    scopes = models.JSONField(
        default=list,
        blank=True,
        help_text="List of scopes this token grants. E.g. ['query:read', 'query:execute', 'admin:read']"
    )

    organisation = models.ForeignKey(
        CoreOrganisation,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='service_tokens',
        help_text="If set, token can access all datasources in this organisation"
    )

    # Granular datasource override
    datasources = models.ManyToManyField(
        DataSource,
        blank=True,
        related_name='service_tokens',
        help_text="If set, overrides org scope with explicit datasource list"
    )

    groups = models.ManyToManyField(
        Group,
        blank=True,
        related_name='service_tokens',
        help_text="Groups inherited by this token to evaluate GroupTableSelector/GroupColumnSelector"
    )

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
        import secrets
        return f"dbi_sk_{secrets.token_hex(24)}"

    @classmethod
    def hash_key(cls, key):
        """Hash a token key for storage."""
        import hashlib
        return hashlib.sha256(key.encode()).hexdigest()

    def get_accessible_datasources(self):
        """
        Returns QuerySet of datasources this token can access.
        Priority:
        1. Explicit datasource links (most restrictive)
        2. Organisation scope (all DS in org)
        3. No restrictions (supertoken - configurable)
        """
        from terno_dbi.core import conf

        if self.datasources.exists():
            return self.datasources.filter(enabled=True)
        elif self.organisation:
            return DataSource.objects.filter(
                Q(organisation=self.organisation) | Q(is_global=True),
                enabled=True
            )
        else:
            if conf.get('ALLOW_SUPERTOKEN'):
                logger.warning("Supertoken access granted to token '%s' (no org/ds scope)", self.name)
                return DataSource.objects.filter(enabled=True)
            else:
                return DataSource.objects.none()

    def has_access_to_datasource(self, datasource):
        """Check if token has access to a specific datasource."""
        return self.get_accessible_datasources().filter(id=datasource.id).exists()

    def has_access_to_table(self, table):
        if not self.has_access_to_datasource(table.data_source):
            return False

        # Check explicit private table selector
        from terno_dbi.core.models import PrivateTableSelector
        pts = PrivateTableSelector.objects.filter(data_source=table.data_source).first()
        if pts and pts.tables.filter(id=table.id).exists():
            return False

        return True

    def has_access_to_column(self, column):
        if not self.has_access_to_table(column.table):
            return False

        # Check explicit private column selector
        from terno_dbi.core.models import PrivateColumnSelector
        pcs = PrivateColumnSelector.objects.filter(data_source=column.table.data_source).first()
        if pcs and pcs.columns.filter(id=column.id).exists():
            return False

        return True

    def has_scope(self, required_scope: str) -> bool:
        """
        Check if the token has a required scope.
        Supports wildcard matching, e.g. 'query:*' matches 'query:read'.
        """
        if not self.scopes:
            # Legacy tokens without scopes: fall back to token_type check
            if required_scope.startswith('query:') and self.token_type == self.TokenType.QUERY:
                return True
            if required_scope.startswith('admin:') and self.token_type == self.TokenType.ADMIN:
                return True
            return False

        for scope in self.scopes:
            if scope == required_scope:
                return True
            # Wildcard support: 'query:*' matches 'query:read'
            if scope.endswith(':*'):
                prefix = scope[:-1]  # 'query:'
                if required_scope.startswith(prefix):
                    return True
        return False
