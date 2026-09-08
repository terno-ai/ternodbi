from django.db import models
from django.db.models import Q
from django.utils.translation import gettext_lazy as _
from django.contrib.auth.models import Group, User
from django.core.exceptions import ValidationError
import logging
import secrets
import hashlib
import reversion
from cryptography.fernet import Fernet
from django.conf import settings
from terno_dbi.core.fields import EncryptedTextField, EncryptedJSONField

logger = logging.getLogger(__name__)


def _get_fernet():
    return Fernet(settings.MCP_ENCRYPTION_KEY)


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
    org_prompt = models.TextField(
        blank=True, default="",
        help_text="Custom text appended to the default LLM system prompt for all users in this organisation."
    )
    created_at = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, blank=True, null=True)

    class Meta:
        db_table = 'core_organisation'

    @property
    def org_prompt_hash(self):
        """SHA-256 of the current org_prompt — the read-before-write token."""
        return hashlib.sha256((self.org_prompt or "").encode("utf-8")).hexdigest()

    def __str__(self):
        return f"{self.name} - {self.subdomain}"


class OrganisationUser(models.Model):
    organisation = models.ForeignKey(
        CoreOrganisation,
        on_delete=models.CASCADE,
        related_name='organisation_users'
    )
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    active_token = models.ForeignKey(
        'ServiceToken',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='org_users',
        help_text="Active sandbox ServiceToken for this user+org pair"
    )
    encrypted_token_key = models.BinaryField(
        null=True,
        blank=True,
        help_text="Fernet-encrypted raw key for the active sandbox token"
    )
    groups = models.ManyToManyField(
        Group,
        related_name='org_user_memberships',
        blank=True,
        help_text="Per-org roles/capabilities for this user+org membership"
    )
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

    def encrypt_token_key(self, raw_key):
        """Encrypt raw token key for secure storage using Fernet."""
        self.encrypted_token_key = _get_fernet().encrypt(raw_key.encode())

    def decrypt_token_key(self):
        """Decrypt stored token key using Fernet."""
        return _get_fernet().decrypt(bytes(self.encrypted_token_key)).decode()


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


class ConnectorCatalog(models.Model):
    """Catalog of every source TernoDBI can connect to.

    Global by design, with no organisation FK. A row is an available connector;
    `DataSource` represents an organisation's connected instance.

    This table is a projection of `catalog.declarations`, not the source of truth.
    `refresh_catalog()` updates code-owned fields on deploy while preserving
    deployment-specific settings such as `enabled`, ordering, and display overrides.
    """

    class AuthType(models.TextChoices):
        OAUTH = "oauth", _("OAuth")
        MANUAL = "manual", _("Credentials")

    class Family(models.TextChoices):
        DATABASE = "database", _("Database")
        API = "api", _("API")

    key = models.SlugField(max_length=64, unique=True)

    # ---- database-owned: safe to edit, survives a refresh -----------------
    enabled = models.BooleanField(
        default=True,
        help_text="Turn a source off for this deployment. A catalog refresh "
                  "never changes this.")
    sort_order = models.IntegerField(default=100)
    most_popular = models.BooleanField(default=False)
    display_name_override = models.CharField(max_length=80, blank=True, default="")
    description_override = models.TextField(blank=True, default="")

    # ---- code-owned: overwritten by refresh_catalog(), read-only in admin --
    display_name = models.CharField(max_length=80)
    provider = models.CharField(max_length=60, blank=True, default="")
    category = models.CharField(max_length=40, blank=True, default="")
    description = models.TextField(blank=True, default="")
    icon_url = models.URLField(blank=True, default="")
    scopes_label = models.CharField(max_length=200, blank=True, default="")

    family = models.CharField(max_length=16, choices=Family)
    auth_type = models.CharField(max_length=16, choices=AuthType)

    # Manual connectors only: the shape of the credentials form. Never values.
    fields_spec = models.JSONField(default=list, blank=True)

    has_account_list = models.BooleanField(default=False)
    has_fields = models.BooleanField(default=False)
    has_report_types = models.BooleanField(default=False)
    is_date_range_required = models.BooleanField(default=False)
    report_types = models.JSONField(default=list, blank=True)
    default_report_type = models.CharField(max_length=64, blank=True, default="")

    account_label_singular = models.CharField(max_length=40, default="Account")
    account_label_plural = models.CharField(max_length=40, default="Accounts")

    class Meta:
        db_table = 'terno_connector_catalog'
        ordering = ('sort_order', 'display_name')
        verbose_name_plural = 'Connector catalog'

    def __str__(self):
        return self.name

    @property
    def name(self) -> str:
        return self.display_name_override or self.display_name

    @property
    def summary(self) -> str:
        return self.description_override or self.description

    @property
    def is_api(self) -> bool:
        return self.family == self.Family.API

    def required_settings(self, report_type: str) -> list:
        """Settings a report type needs before it can run at all.

        Distinct from filters: these select which upstream call is made, so a
        missing one is an error rather than an empty result.
        """
        for report in self.report_types:
            if report.get("id") == report_type:
                return [s for s in report.get("settings", []) if s.get("required", True)]
        return []


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
    connection_str = EncryptedTextField(
        help_text=(
            "Connection string for the datasource.<br><br>"
            "<b>Examples:</b><br>"
            "&bull; <b>Postgres:</b> <code>postgresql://user:password@host:port/dbname</code><br>"
            "&bull; <b>MySQL:</b> <code>mysql+pymysql://user:password@host:port/dbname</code><br>"
            "&bull; <b>Oracle:</b> <code>oracle+oracledb://user:password@host:port/?service_name=service_name</code><br>"
            "&bull; <b>Snowflake:</b> <code>snowflake://user:password@account_identifier/dbname/schema_name?warehouse=warehouse_name</code><br>"
            "&bull; <b>BigQuery:</b> <code>bigquery://project_id/dataset_id</code> <i>(Requires Connection JSON)</i><br>"
            "&bull; <b>DataBricks:</b> <code>databricks://token:dapi_token@host:port?http_path=/sql/1.0/endpoints/12345</code>"
        )
    )
    connection_json = EncryptedJSONField(
        null=True, blank=True,
        help_text=(
            "JSON key file contents for authentication.<br><br>"
            "<b>Examples:</b><br>"
            "&bull; <b>BigQuery:</b> Paste the entire contents of your Google Cloud Service Account JSON key file here."
        )
    )
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

    class AuthStatus(models.TextChoices):
        NOT_AUTHENTICATED = "not_authenticated", _("Not authenticated")
        CONNECTED = "connected", _("Connected")
        EXPIRED = "expired", _("Needs reconnect")
        ERROR = "error", _("Error")

    catalog = models.ForeignKey(
        ConnectorCatalog,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name='datasources',
        help_text=(
            "Which catalog entry this connection is an instance of. "
            "PROTECT, not CASCADE — retiring a catalog entry must never "
            "silently delete a customer's configured datasource."
        ),
    )
    auth_status = models.CharField(
        max_length=32, choices=AuthStatus, default=AuthStatus.CONNECTED,
        help_text="Whether this connection can currently reach its source.",
    )
    auth_error = models.TextField(blank=True, default="")
    last_synced = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'terno_datasource'

    def __str__(self):
        return self.display_name

    @property
    def decrypted_connection_str(self):
        """The plaintext connection string. Use only where a connection is opened
        — never for display, serialization or logging (those get the ciphertext
        via `connection_str`). Legacy plaintext rows pass through unchanged."""
        from terno_dbi.services import secrets
        return secrets.decrypt_str(self.connection_str)

    @property
    def decrypted_connection_json(self):
        """The plaintext credentials dict (e.g. a BigQuery service account). Same
        rule as `decrypted_connection_str`: connect-path use only."""
        from terno_dbi.services import secrets
        return secrets.decrypt_dict(self.connection_json)

    @property
    def family(self) -> str:
        """Selects the execution strategy.

        Falls back to 'database' for rows predating the catalog, which were all
        databases by definition.
        """
        return self.catalog.family if self.catalog_id else ConnectorCatalog.Family.DATABASE

    @property
    def is_api(self) -> bool:
        return self.family == ConnectorCatalog.Family.API

    @property
    def needs_reconnect(self) -> bool:
        return self.auth_status in (
            self.AuthStatus.EXPIRED, self.AuthStatus.NOT_AUTHENTICATED,
        )

    def clean(self):
        super().clean()
        if self.catalog_id and self.catalog.family == ConnectorCatalog.Family.API:
            if not self.catalog.enabled:
                raise ValidationError(
                    f"{self.catalog.name} is not enabled on this deployment."
                )
        elif not (self.connection_str or "").strip():
            raise ValidationError(
                {"connection_str": "A database datasource needs a connection string."}
            )


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
        OAUTH = 'oauth', _('OAuth Connector')

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
        related_name='created_tokens',
        help_text="Audit only: who/what actually minted this token (an admin, "
                  "the system, etc). Not used for authorization."
    )
    created_for = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='tokens_created_for',
        help_text="Whose identity this token acts as — the memory author for "
                  "user-store writes, and what visibility is scoped to. This, "
                  "not created_by, is what authorization reads."
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
        return f"dbi_sk_{secrets.token_hex(24)}"

    @classmethod
    def hash_key(cls, key):
        """Hash a token key for storage."""
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
            # Check if show_demo_data is enabled for the organisation
            show_demo_data = False
            if hasattr(self.organisation, 'terno_organisation'):
                try:
                    show_demo_data = self.organisation.terno_organisation.show_demo_data
                except Exception:
                    pass
            elif hasattr(self.organisation, 'show_demo_data'):
                show_demo_data = self.organisation.show_demo_data

            if show_demo_data:
                return DataSource.objects.filter(
                    Q(organisation=self.organisation) | Q(is_global=True),
                    enabled=True
                )
            else:
                return DataSource.objects.filter(
                    organisation=self.organisation,
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

        from terno_dbi.core.models import PrivateTableSelector
        pts = PrivateTableSelector.objects.filter(data_source=table.data_source).first()
        if pts and pts.tables.filter(id=table.id).exists():
            return False

        return True

    def has_access_to_column(self, column):
        if not self.has_access_to_table(column.table):
            return False

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
            if required_scope.startswith('query:') and self.token_type == self.TokenType.QUERY:
                return True
            if required_scope.startswith('admin:') and self.token_type == self.TokenType.ADMIN:
                return True
            return False

        for scope in self.scopes:
            if scope == required_scope:
                return True
            if scope.endswith(':*'):
                prefix = scope[:-1]
                if required_scope.startswith(prefix):
                    return True
        return False


class LLMConfiguration(models.Model):

    LLM_TYPES = [
        ('openai', 'OpenAI'),
        ('gemini', 'Gemini'),
        ('anthropic', 'Anthropic'),
        ('ollama', 'Ollama'),
        ('custom', 'CustomLLM'),
        ('terno', 'TernoLLM'),
    ]

    organisation = models.ForeignKey(
        CoreOrganisation,
        on_delete=models.CASCADE,
        related_name="llm_configs"
    )

    llm_type = models.CharField(max_length=64, choices=LLM_TYPES)
    api_key = models.CharField(max_length=512)
    model_name = models.CharField(max_length=256, blank=True, null=True)
    temperature = models.FloatField(blank=True, null=True)
    custom_system_message = models.TextField(blank=True, null=True)
    max_tokens = models.IntegerField(blank=True, null=True)
    top_p = models.FloatField(blank=True, null=True)
    top_k = models.FloatField(blank=True, null=True)
    custom_parameters = models.JSONField(blank=True, null=True)
    enabled = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "dbi_llm_configuration"

    def clean(self):
        super().clean()

        if self.custom_parameters:
            if not isinstance(self.custom_parameters, dict):
                raise ValidationError("custom_parameters must be a JSON object")

        # Only ONE enabled per organisation
        if self.enabled:
            existing = LLMConfiguration.objects.filter(
                organisation=self.organisation,
                enabled=True
            ).exclude(id=self.id)

            if existing.exists():
                raise ValidationError("Only one enabled LLM per organisation allowed")

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.organisation} - {self.llm_type}"


class PromptExample(models.Model):

    organisation = models.ForeignKey(
        CoreOrganisation, on_delete=models.CASCADE, null=True, blank=True,
        related_name='prompt_examples')
    created_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='prompt_examples',
        help_text="Owner of this memory. NULL = org-level shared knowledge."
    )
    is_shared = models.BooleanField(
        default=False,
        help_text="If True, this memory is visible to all users in the organisation. "
                  "Only org admins can set this."
    )
    key = models.CharField(
        max_length=255,
        help_text="The question or query key used for semantic matching."
    )
    value = models.TextField(
        help_text="The domain knowledge, business rule, or contextual answer."
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "terno_promptexample"
        verbose_name = "Prompt Example"
        verbose_name_plural = "Prompt Examples"

    def __str__(self):
        owner = self.created_by.username if self.created_by else "org-shared"
        return f"[{owner}] {self.key[:50]}"


@reversion.register()
class Memory(models.Model):
    """
    Two independent axes:

    * ``store`` — who can see it:
        - ``user`` — private to :attr:`created_by`.
        - ``org``  — shared across the whole organisation; writing it needs an
          admin-scoped token.
    * ``data_source`` — the scope axis:
        - NULL -> global: the fact applies regardless of which datasource is queried.
        - set  -> specific to that database's schema/rules.
    """

    class Store(models.TextChoices):
        USER = 'user', _('User (private to creator)')
        ORG = 'org', _('Organisation (shared)')

    class MemoryType(models.TextChoices):
        USER = 'user', _('User')
        FEEDBACK = 'feedback', _('Feedback')
        PROJECT = 'project', _('Project')
        REFERENCE = 'reference', _('Reference')

    organisation = models.ForeignKey(
        CoreOrganisation, on_delete=models.CASCADE,
        related_name='memories')
    store = models.CharField(
        max_length=10, choices=Store.choices, default=Store.USER,
        help_text="user = private to creator; org = shared across the organisation."
    )
    created_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='memories',
        help_text="The author of this memory (always set at creation). "
                  "Null only if that user was later deleted."
    )
    data_source = models.ForeignKey(
        DataSource, on_delete=models.CASCADE, null=True, blank=True,
        related_name='memories',
        help_text="scope axis: NULL = global (applies to any datasource); "
                  "set = specific to this datasource's schema/rules."
    )
    name = models.SlugField(
        max_length=100,
        help_text="kebab-case slug, unique within its scope; the lookup key, "
                  "e.g. 'zydus-active-users-join'."
    )
    description = models.CharField(
        max_length=255,
        help_text="One-line hook shown in the memory index."
    )
    memory_type = models.CharField(
        max_length=20, choices=MemoryType.choices,
        default=MemoryType.PROJECT
    )
    content = models.TextField(
        help_text="The full fact body (plus Why/How-to-apply for feedback/project)."
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "terno_memory"
        verbose_name = "Memory"
        verbose_name_plural = "Memories"
        permissions = [
            ("write_org_memory", "Can write organisation-wide memories"),
        ]
        constraints = [
            # org store: one name per (org, datasource) — a shared namespace.
            models.UniqueConstraint(
                fields=['organisation', 'data_source', 'name'],
                condition=models.Q(store='org'),
                name='uniq_org_memory_scope_name'
            ),
            # user store: one name per (org, owner, datasource).
            models.UniqueConstraint(
                fields=['organisation', 'created_by', 'data_source', 'name'],
                condition=models.Q(store='user'),
                name='uniq_user_memory_scope_name'
            ),
        ]
        indexes = [
            models.Index(fields=['organisation', 'store', 'data_source']),
        ]

    @property
    def scope(self):
        """Scope string: 'global' or 'datasource:<id>'."""
        return f"datasource:{self.data_source_id}" if self.data_source_id else "global"

    @property
    def content_hash(self):
        """SHA-256 of the current content — the read-before-write token."""
        return hashlib.sha256((self.content or "").encode("utf-8")).hexdigest()

    def __str__(self):
        return f"[{self.store}/{self.scope}] {self.name}"


class ApiQueryJob(models.Model):
    """An asynchronous `data_query` job for an API source.

    Long-running queries are executed as jobs so `data_query` can return a job ID
    without waiting for the provider request to finish. `get_query_results` polls
    the job for its result.

    Jobs are scoped to their owning organisation, so a job ID alone can never be
    used to access another organisation's result.
    """

    class Status(models.TextChoices):
        PENDING = "pending", _("Pending")
        RUNNING = "running", _("Running")
        COMPLETED = "completed", _("Completed")
        FAILED = "failed", _("Failed")

    id = models.CharField(max_length=40, primary_key=True)
    organisation = models.ForeignKey(
        CoreOrganisation, on_delete=models.CASCADE, related_name="api_query_jobs",
        null=True, blank=True,
    )
    data_source = models.ForeignKey(
        DataSource, on_delete=models.CASCADE, related_name="api_query_jobs",
    )
    status = models.CharField(
        max_length=16, choices=Status, default=Status.PENDING,
    )
    spec = models.JSONField(default=dict)
    result = models.JSONField(null=True, blank=True)
    error = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "terno_api_query_job"
        ordering = ("-created_at",)
        indexes = [
            models.Index(fields=["organisation", "status"]),
        ]

    def __str__(self):
        return f"{self.id} [{self.status}]"

    @property
    def is_terminal(self) -> bool:
        return self.status in (self.Status.COMPLETED, self.Status.FAILED)


class ConnectorOAuthState(models.Model):
    """Transient state binding one in-flight OAuth authorization to its result.

    Created when a connect flow starts, consumed when the provider redirects
    back. Holds the PKCE verifier and the target datasource/org so the callback
    can finish without trusting anything in the redirect except the opaque
    `state` token (which it looks up here).

    Distinct from `terno_dbi.oauth` state — that is TernoDBI's own provider side.
    This is the client side, authorizing *to* Google/Meta.
    """

    state = models.CharField(max_length=128, unique=True, db_index=True)
    connector_key = models.CharField(max_length=64)
    code_verifier = models.CharField(max_length=128, blank=True, default="")
    redirect_uri = models.CharField(max_length=500)
    organisation = models.ForeignKey(
        CoreOrganisation, on_delete=models.CASCADE,
        related_name="connector_oauth_states", null=True, blank=True,
    )
    data_source = models.ForeignKey(
        DataSource, on_delete=models.CASCADE,
        related_name="oauth_states", null=True, blank=True,
        help_text="Set when reconnecting an existing datasource; null on first connect.",
    )
    return_to = models.CharField(max_length=500, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()

    class Meta:
        db_table = "terno_connector_oauth_state"
        indexes = [models.Index(fields=["expires_at"])]

    def __str__(self):
        return f"{self.connector_key} state {self.state[:8]}…"

    @property
    def is_expired(self) -> bool:
        from django.utils import timezone
        return timezone.now() >= self.expires_at


class GroupAccountAllowlist(models.Model):
    """Define which API accounts each group can query on a connected source.

    For API sources, account access is the main authorization boundary: GA4
    properties, ad accounts, and channels can be restricted even when they share
    one OAuth connection.

    If a datasource has an allowlist, callers may access only accounts granted to
    their groups; no matching account means no access. If no allowlist exists, the
    datasource is unrestricted within the owning organisation.
    """

    group = models.ForeignKey(
        Group, on_delete=models.CASCADE, related_name="account_allowlists",
    )
    data_source = models.ForeignKey(
        DataSource, on_delete=models.CASCADE, related_name="account_allowlists",
    )
    account_id = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "terno_group_account_allowlist"
        constraints = [
            models.UniqueConstraint(
                fields=["group", "data_source", "account_id"],
                name="uniq_group_datasource_account",
            ),
        ]
        indexes = [models.Index(fields=["data_source"])]

    def __str__(self):
        return f"{self.group.name} → {self.data_source_id}:{self.account_id}"
