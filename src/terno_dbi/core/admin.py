import json
import logging
from django import forms
from django.contrib import admin
from django.apps import apps
import reversion.admin
from .models import LLMConfiguration
from django.db.models import Q

from terno_dbi.core import models
from django.contrib import messages
from django.http import HttpResponse
from django.shortcuts import redirect, render
from django.urls import path
from terno_dbi.services import memory as memory_service
from terno_dbi.services.schema_utils import sync_metadata
from terno_dbi.core.models import ServiceToken
from django.utils.html import format_html
from terno_dbi.services.auth import generate_service_token

logger = logging.getLogger(__name__)

PARENT_APP_INSTALLED = apps.is_installed('terno')

if not PARENT_APP_INSTALLED:
    from .models import (
        DataSource, Table, TableColumn, ForeignKey,
        PrivateTableSelector, GroupTableSelector,
        PrivateColumnSelector, GroupColumnSelector,
        GroupTableRowFilter, TableRowFilter,
        CoreOrganisation, OrganisationUser, OrganisationGroup, 
    )

    class TableColumnInline(admin.TabularInline):
        model = TableColumn
        extra = 0
        fields = ('name', 'public_name', 'data_type', 'description', 'is_hidden')
        readonly_fields = ('name', 'data_type')

    class ForeignKeyInline(admin.TabularInline):
        model = ForeignKey
        fk_name = 'constrained_table'
        extra = 0
        fields = ('constrained_columns', 'referred_table', 'referred_columns')
        raw_id_fields = ('constrained_columns', 'referred_table', 'referred_columns')

    @admin.register(DataSource)
    class DataSourceAdmin(reversion.admin.VersionAdmin):
        list_display = ('display_name', 'type', 'enabled', 'dialect_name', 'dialect_version', 'organisation')
        list_filter = ('type', 'enabled', 'dialect_name', 'organisation')
        search_fields = ('display_name', 'description', 'organisation__name')
        readonly_fields = ('dialect_name', 'dialect_version')
        fields = (
            'display_name',
            'type',
            'organisation',
            'enabled',
            'connection_str',
            'connection_json',
            'description',
            'dialect_name',
            'dialect_version',
        )
        actions = ['trigger_sync_metadata']

        def get_changeform_initial_data(self, request):
            initial = super().get_changeform_initial_data(request)
            org_user = OrganisationUser.objects.filter(user=request.user).first()
            if org_user:
                initial['organisation'] = org_user.organisation_id
            return initial

        def save_model(self, request, obj, form, change):

            super().save_model(request, obj, form, change)
            logger.info("DataSource saved via admin: id=%s, name='%s', change=%s", obj.id, obj.display_name, change)

            if not change and obj.enabled:
                try:
                    sync_result = sync_metadata(obj.id)

                    if 'error' in sync_result:
                        logger.warning("Metadata sync failed for '%s': %s", obj.display_name, sync_result['error'])
                        messages.warning(
                            request,
                            f"Datasource saved, but metadata sync failed: {sync_result['error']}"
                        )
                    else:
                        tables_created = sync_result.get('tables_created', 0)
                        columns_created = sync_result.get('columns_created', 0)
                        logger.info("Metadata synced for '%s': %d tables, %d columns", obj.display_name, tables_created, columns_created)
                        messages.success(
                            request,
                            f"Metadata synced: {tables_created} tables and {columns_created} columns discovered."
                        )
                except Exception as e:
                    logger.error("Metadata sync exception for '%s': %s", obj.display_name, str(e))
                    messages.warning(
                        request,
                        f"Datasource saved, but metadata sync failed: {str(e)}"
                    )

        def trigger_sync_metadata(self, request, queryset):
            logger.info("Metadata sync triggered via admin for %d datasources", queryset.count())
            for ds in queryset:
                if not ds.enabled:
                    logger.debug("Skipped disabled datasource: '%s'", ds.display_name)
                    messages.warning(request, f"Skipped '{ds.display_name}' - datasource is not enabled.")
                    continue
                try:
                    sync_result = sync_metadata(ds.id, overwrite=False)  
                    if 'error' in sync_result:
                        logger.error("Sync failed for '%s': %s", ds.display_name, sync_result['error'])
                        messages.error(request, f"'{ds.display_name}': {sync_result['error']}")
                    else:
                        tables_created = sync_result.get('tables_created', 0)
                        tables_updated = sync_result.get('tables_updated', 0)
                        columns_created = sync_result.get('columns_created', 0)
                        logger.info("Sync completed for '%s': %d new tables, %d updated, %d columns", ds.display_name, tables_created, tables_updated, columns_created)
                        messages.success(
                            request,
                            f"'{ds.display_name}': Synced {tables_created} new tables, "
                            f"{tables_updated} updated, {columns_created} new columns."
                        )
                except Exception as e:
                    logger.exception("Sync exception for '%s'", ds.display_name)
                    messages.error(request, f"'{ds.display_name}': Sync failed - {str(e)}")

        trigger_sync_metadata.short_description = "Sync metadata (discover tables & columns)"

    @admin.register(Table)
    class TableAdmin(reversion.admin.VersionAdmin):
        list_display = ('name', 'public_name', 'data_source', 'is_hidden', 'estimated_row_count', 'column_count')
        list_filter = ('data_source', 'is_hidden')
        search_fields = ('name', 'public_name', 'description')
        inlines = [TableColumnInline, ForeignKeyInline]

        def column_count(self, obj):
            return obj.tablecolumn_set.count()
        column_count.short_description = 'Columns'

    @admin.register(TableColumn)
    class TableColumnAdmin(reversion.admin.VersionAdmin):
        list_display = ('name', 'public_name', 'table', 'data_type', 'is_hidden')
        list_filter = ('table__data_source', 'data_type', 'is_hidden')
        search_fields = ('name', 'public_name', 'description')
        raw_id_fields = ('table',)

    @admin.register(ForeignKey)
    class ForeignKeyAdmin(reversion.admin.VersionAdmin):
        list_display = ('constrained_table', 'constrained_columns', 'referred_table', 'referred_columns')
        list_filter = ('constrained_table__data_source',)
        raw_id_fields = ('constrained_table', 'constrained_columns', 'referred_table', 'referred_columns')


    @admin.register(PrivateTableSelector)
    class PrivateTableSelectorAdmin(admin.ModelAdmin):
        list_display = ('data_source', 'table_count')
        filter_horizontal = ('tables',)

        def table_count(self, obj):
            return obj.tables.count()
        table_count.short_description = 'Tables'

    @admin.register(GroupTableSelector)
    class GroupTableSelectorAdmin(admin.ModelAdmin):
        list_display = ('group', 'table_count')
        filter_horizontal = ('tables',)

        def table_count(self, obj):
            return obj.tables.count()
        table_count.short_description = 'Tables'

    @admin.register(PrivateColumnSelector)
    class PrivateColumnSelectorAdmin(admin.ModelAdmin):
        list_display = ('data_source', 'column_count')
        filter_horizontal = ('columns',)

        def column_count(self, obj):
            return obj.columns.count()
        column_count.short_description = 'Columns'

    @admin.register(GroupColumnSelector)
    class GroupColumnSelectorAdmin(admin.ModelAdmin):
        list_display = ('group', 'column_count')
        filter_horizontal = ('columns',)

        def column_count(self, obj):
            return obj.columns.count()
        column_count.short_description = 'Columns'

    @admin.register(TableRowFilter)
    class TableRowFilterAdmin(admin.ModelAdmin):
        list_display = ('table', 'data_source', 'filter_str')
        list_filter = ('data_source',)
        raw_id_fields = ('table',)

    @admin.register(GroupTableRowFilter)
    class GroupTableRowFilterAdmin(admin.ModelAdmin):
        list_display = ('group', 'table', 'data_source', 'filter_str')
        list_filter = ('data_source', 'group')
        raw_id_fields = ('table',)

    @admin.register(CoreOrganisation)
    class CoreOrganisationAdmin(reversion.admin.VersionAdmin):
        list_display = ('name', 'created_at', 'updated_at')
        search_fields = ('name',)
        readonly_fields = ('created_at', 'updated_at')

    @admin.register(OrganisationUser)
    class OrganisationUserAdmin(admin.ModelAdmin):
        list_display = ('user', 'organisation')
        list_filter = ('organisation',)
        search_fields = ('user__username', 'user__email', 'organisation__name')
        raw_id_fields = ('user', 'organisation')
        filter_horizontal = ('groups',)

    @admin.register(OrganisationGroup)
    class OrganisationGroupAdmin(admin.ModelAdmin):
        list_display = ('group', 'organisation')
        list_filter = ('organisation',)
        search_fields = ('group__name', 'organisation__name')
        raw_id_fields = ('group', 'organisation')

    @admin.register(ServiceToken)
    class ServiceTokenAdmin(admin.ModelAdmin):
        list_display = ('name', 'key_prefix', 'token_type', 'organisation', 'created_for', 'is_active', 'last_used', 'created_at')
        list_filter = ('token_type', 'is_active', 'organisation', 'created_at')
        search_fields = ('name', 'key_prefix', 'organisation__name', 'created_for__username')
        readonly_fields = ('key_hash', 'key_prefix', 'last_used', 'created_at', 'created_by')
        fields = ('name', 'token_type', 'organisation', 'created_for', 'datasources', 'groups', 'is_active',
                  'expires_at', 'key_hash', 'key_prefix', 'last_used', 'created_at', 'created_by')

        filter_horizontal = ('datasources', 'groups')
        autocomplete_fields = ('organisation', 'created_for')

        def save_model(self, request, obj, form, change):
            if not change:

                organisation = form.cleaned_data.get('organisation')
                created_for = form.cleaned_data.get('created_for') or request.user

                token, full_key = generate_service_token(
                    name=obj.name,
                    token_type=obj.token_type,
                    created_by=request.user,
                    created_for=created_for,
                    organisation=organisation
                )

                obj.id = token.id
                obj.key_hash = token.key_hash
                obj.key_prefix = token.key_prefix
                obj.created_at = token.created_at

                datasources = form.cleaned_data.get('datasources')
                if datasources:
                    token.datasources.set(datasources)

                groups = form.cleaned_data.get('groups')
                if groups:
                    token.groups.set(groups)

                messages.set_level(request, messages.SUCCESS)
                messages.success(request, format_html(
                    "<strong>Token Created Successfully!</strong><br>"
                    "Authentication Key: <code>{}</code><br>"
                    "<span style='color:red'>WARNING: Copy this now. It will never be shown again.</span>",
                    full_key
                ))
            else:
                super().save_model(request, obj, form, change)


class OrganisationFilterMixin:
    organisation_related_field_names = []
    organisation_foreignkey_field_names = {}
    organisation_manytomany_field_names = {}

    def get_user_organisation(self, request):
        organisation = models.CoreOrganisation.objects.get(pk=request.org_id)
        if not models.OrganisationUser.objects.filter(
                user=request.user,
                organisation=organisation).exists():
            return None
        return organisation

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        if not request.user.is_superuser:
            user_organisation = self.get_user_organisation(request)
            if user_organisation and self.organisation_related_field_names:
                # Use dynamic filtering based on the organisation field specified in the admin class
                combined_q = Q()

                for field_name in self.organisation_related_field_names:
                    combined_q = combined_q | Q(**{f"{field_name}_id": user_organisation.pk})

                qs = qs.filter(combined_q)
            else:
                qs = qs.none()
        return qs

    def formfield_for_foreignkey(self, db_field, request, **kwargs):
        if not request.user.is_superuser:
            user_organisation = self.get_user_organisation(request)
            if user_organisation and db_field.name in self.organisation_foreignkey_field_names:
                field_filter = self.organisation_foreignkey_field_names.get(db_field.name)
                if field_filter:
                    filter_kwargs = {
                        f"{field_filter}_id": user_organisation.pk
                    }

                    kwargs["queryset"] = db_field.related_model.objects.filter(**filter_kwargs)
            else:
                kwargs["queryset"] = db_field.related_model.objects.none()
        return super().formfield_for_foreignkey(db_field, request, **kwargs)

    def formfield_for_manytomany(self, db_field, request, **kwargs):
        if not request.user.is_superuser:
            user_organisation = self.get_user_organisation(request)
            if user_organisation and db_field.name in self.organisation_manytomany_field_names:
                field_filter = self.organisation_manytomany_field_names.get(db_field.name)
                if field_filter:
                    filter_kwargs = {
                        f"{field_filter}_id": user_organisation.pk
                    }

                    kwargs["queryset"] = db_field.related_model.objects.filter(**filter_kwargs)
            else:
                kwargs["queryset"] = db_field.related_model.objects.none()
        return super().formfield_for_manytomany(db_field, request, **kwargs)

if not PARENT_APP_INSTALLED:
    @admin.register(LLMConfiguration)
    class LLMConfigurationAdmin(admin.ModelAdmin):

        list_display = ('organisation', 'llm_type', 'masked_api_key', 'model_name', 'enabled')
        list_filter = ('llm_type', 'enabled', 'organisation')
        search_fields = ('llm_type', 'model_name')

        fieldsets = (
            ('Basic Configuration', {
                'fields': ('organisation', 'llm_type', 'api_key', 'enabled'),
            }),
            ('Advanced Configuration (Optional)', {
                'classes': ('collapse',),
                'fields': ('model_name', 'temperature', 'top_p', 'top_k', 'max_tokens', 'custom_parameters'),
            }),
        )

        # Mask API key
        def masked_api_key(self, obj):
            if obj.api_key:
                return obj.api_key[:6] + "****"
            return ""
        masked_api_key.short_description = "API Key"

        # Make API key readonly after creation
        def get_readonly_fields(self, request, obj=None):
            if obj:
                return ('api_key',)
            return ()

        def save_model(self, request, obj, form, change):
            if obj.enabled:
                # Disable other enabled LLMs for same org
                LLMConfiguration.objects.filter(
                    organisation=obj.organisation,
                    enabled=True
                ).exclude(id=obj.id).update(enabled=False)

            super().save_model(request, obj, form, change)


@admin.register(models.PromptExample)
class PromptExampleAdmin(OrganisationFilterMixin, admin.ModelAdmin):
    list_display = ('key', 'value', 'created_by', 'is_shared', 'created_at', 'updated_at')
    list_filter = ('is_shared',)
    organisation_related_field_names = ['organisation']
    exclude = ['organisation']

    def save_model(self, request, obj, form, change):
        org_id = request.org_id
        org = models.CoreOrganisation.objects.get(pk=org_id)
        obj.organisation = org
        if not change and not obj.created_by_id:
            obj.is_shared = True
        super().save_model(request, obj, form, change)


KEEP_ORIGINAL_DATASOURCE = "__keep__"
GLOBAL_DATASOURCE = "__global__"


def _can_write_org_memory(user):
    """Whether ``user`` may create/edit/delete org-store (shared) memories.

    Deliberately a generic Django permission (``core.write_org_memory``)
    rather than a hardcoded group name — ternodbi ships standalone and must
    not assume a host app's own group-naming convention (e.g. terno-ai's
    global "Org Admin" group) exists. The host grants this permission to
    whichever group/role it wants; ternodbi only ever checks ``has_perm``.
    """
    return user.is_superuser or user.has_perm('core.write_org_memory')


def _importable_organisations(user):
    """Organisations a user may import memories into: every org for a
    superuser, otherwise only orgs they belong to (via ``OrganisationUser``).
    """
    if user.is_superuser:
        return models.CoreOrganisation.objects.order_by('name')
    return models.CoreOrganisation.objects.filter(
        organisation_users__user=user
    ).distinct().order_by('name')


class MemoryImportForm(forms.Form):
    file = forms.FileField(label="Memories JSON export")
    organisation = forms.ModelChoiceField(
        queryset=models.CoreOrganisation.objects.none(),
        label="Import into organisation",
        help_text="Every imported memory is attributed to you, inside this "
                   "organisation — regardless of which org the file was "
                   "originally exported from.",
    )
    datasource = forms.ChoiceField(
        label="Attach imported memories to",
        help_text="Choose a datasource to attach every imported memory to, or "
                   "import them as global. \"Keep from file\" tries to match "
                   "each row's own datasource by name and falls back to global "
                   "if it can't be found locally.",
    )

    def __init__(self, *args, organisations_qs=None, **kwargs):
        super().__init__(*args, **kwargs)
        from django.db.models import Q
        from terno_dbi.core.models import DataSource

        organisations_qs = (
            organisations_qs if organisations_qs is not None
            else models.CoreOrganisation.objects.none()
        )
        self.fields['organisation'].queryset = organisations_qs

        choices = [
            (KEEP_ORIGINAL_DATASOURCE, "Keep from file (match by name)"),
            (GLOBAL_DATASOURCE, "Global (no datasource)"),
        ]
        datasources = DataSource.objects.filter(
            Q(organisation__in=organisations_qs) | Q(is_global=True)
        ).distinct().order_by('display_name')
        choices += [
            (str(ds.pk), f"{ds.display_name}"
                          f"{'' if ds.is_global else f' ({ds.organisation.name})'}")
            for ds in datasources
        ]
        self.fields['datasource'].choices = choices

        self.datasource_organisation_map = {
            KEEP_ORIGINAL_DATASOURCE: "",
            GLOBAL_DATASOURCE: "",
        }
        self.datasource_organisation_map.update({
            str(ds.pk): "" if ds.is_global else str(ds.organisation_id)
            for ds in datasources
        })


@admin.register(models.Memory)
class MemoryAdmin(OrganisationFilterMixin, reversion.admin.VersionAdmin):
    list_display = ('name', 'description', 'memory_type', 'store',
                    'data_source', 'created_by', 'updated_at')
    list_filter = ('memory_type', 'store', 'data_source')
    search_fields = ('name', 'description', 'content')
    organisation_related_field_names = ['organisation']
    exclude = ['organisation']
    actions = ['export_selected_memories']
    change_list_template = 'admin/core/memory/change_list.html'

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        if request.user.is_superuser:
            return qs
        # Everyone (Org Admin or not) only ever sees org-wide memories plus
        # their *own* personal ones — nobody browses other members' personal
        # memory here, matching what the agent itself is allowed to touch.
        return qs.filter(
            Q(store=models.Memory.Store.ORG)
            | Q(store=models.Memory.Store.USER, created_by_id=request.user.id)
        )

    def has_change_permission(self, request, obj=None):
        if not super().has_change_permission(request, obj):
            return False
        if obj is None or request.user.is_superuser:
            return True
        if obj.store == models.Memory.Store.ORG:
            return _can_write_org_memory(request.user)
        return obj.created_by_id == request.user.id

    def has_delete_permission(self, request, obj=None):
        if not super().has_delete_permission(request, obj):
            return False
        if obj is None or request.user.is_superuser:
            return True
        if obj.store == models.Memory.Store.ORG:
            return _can_write_org_memory(request.user)
        return obj.created_by_id == request.user.id

    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj=obj, **kwargs)
        if not _can_write_org_memory(request.user) and 'store' in form.base_fields:
            # Non-Org-Admins can only ever create/edit their own personal
            # memories through this form — org-store is read-only for them.
            form.base_fields['store'].choices = [
                (models.Memory.Store.USER, models.Memory.Store.USER.label),
            ]
            form.base_fields['store'].initial = models.Memory.Store.USER
        return form

    def save_model(self, request, obj, form, change):
        org_id = request.org_id
        org = models.CoreOrganisation.objects.get(pk=org_id)
        obj.organisation = org
        if not change and not obj.created_by_id:
            obj.created_by = request.user
        super().save_model(request, obj, form, change)

    @admin.action(description="Export selected memories to JSON")
    def export_selected_memories(self, request, queryset):
        rows = [
            memory_service.export_row(mem)
            for mem in queryset.select_related('organisation', 'created_by', 'data_source')
        ]
        payload = json.dumps(
            {"version": 1, "count": len(rows), "memories": rows},
            indent=2, ensure_ascii=False,
        )
        response = HttpResponse(payload, content_type='application/json')
        response['Content-Disposition'] = 'attachment; filename="memories_export.json"'
        return response

    def get_urls(self):
        custom = [
            path('import-json/', self.admin_site.admin_view(self.import_json_view),
                 name='terno_dbi_memory_import_json'),
        ]
        return custom + super().get_urls()

    def import_json_view(self, request):
        allowed_orgs = _importable_organisations(request.user)
        if not allowed_orgs.exists():
            messages.error(request, "You are not a member of any organisation.")
            return redirect('..')

        if request.method == 'POST':
            form = MemoryImportForm(request.POST, request.FILES, organisations_qs=allowed_orgs)
            if form.is_valid():
                target_org = form.cleaned_data['organisation']

                try:
                    payload = json.load(form.cleaned_data['file'])
                except (json.JSONDecodeError, UnicodeDecodeError):
                    messages.error(request, "Uploaded file is not valid JSON.")
                    return redirect('.')

                if isinstance(payload, list):
                    rows = payload
                elif isinstance(payload, dict):
                    rows = payload.get('memories', [])
                else:
                    messages.error(
                        request,
                        "Uploaded file must be a JSON object with a 'memories' list, "
                        "or a plain JSON list of memory rows.",
                    )
                    return redirect('.')

                if not rows:
                    messages.warning(
                        request,
                        "No memory rows found in the uploaded file — it parsed as valid "
                        "JSON but contained an empty list (or an empty 'memories' key). "
                        "Nothing was imported.",
                    )
                    return redirect('..')

                datasource_choice = form.cleaned_data['datasource']
                if datasource_choice == KEEP_ORIGINAL_DATASOURCE:
                    force_datasource = memory_service.NOT_SET
                elif datasource_choice == GLOBAL_DATASOURCE:
                    force_datasource = None
                else:
                    from terno_dbi.core.models import DataSource
                    force_datasource = int(datasource_choice)
                    ds = DataSource.objects.filter(id=force_datasource).first()
                    if ds is None or not (ds.is_global or ds.organisation_id == target_org.id):
                        messages.error(
                            request,
                            "Selected datasource does not belong to the selected "
                            "organisation.",
                        )
                        return redirect('.')

                counts = {"created": 0, "skipped": 0, "error": 0}
                notes = []
                for row in rows:
                    action, detail = memory_service.import_row(
                        row, target_organisation_id=target_org.id,
                        importing_user_id=request.user.id,
                        can_write_org_memory=_can_write_org_memory(request.user),
                        force_datasource_id=force_datasource,
                    )
                    counts[action] = counts.get(action, 0) + 1
                    if detail:
                        notes.append(f"{row.get('name', '?')}: {detail}")

                messages.success(
                    request,
                    f"Import done — {len(rows)} rows read into '{target_org.name}', "
                    f"created={counts['created']} skipped={counts['skipped']} "
                    f"errors={counts['error']}",
                )
                for note in notes[:20]:
                    messages.warning(request, note)
                if len(notes) > 20:
                    messages.warning(request, f"...and {len(notes) - 20} more (see server logs).")
                return redirect('..')
        else:
            form = MemoryImportForm(organisations_qs=allowed_orgs)

        return render(
            request, 'admin/core/memory/import_json.html',
            {
                'form': form, 'opts': self.model._meta,
                'datasource_organisation_map_json': json.dumps(form.datasource_organisation_map),
            },
        )
