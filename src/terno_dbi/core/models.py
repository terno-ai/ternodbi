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
        max_length=1000, 
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
    connection_json = models.JSONField(
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
