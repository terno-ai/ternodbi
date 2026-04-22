import logging
from django.contrib import admin
from django.apps import apps
import reversion.admin
from django.contrib import messages
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
        list_display = ('display_name', 'type', 'enabled', 'dialect_name', 'dialect_version')
        list_filter = ('type', 'enabled', 'dialect_name')
        search_fields = ('display_name', 'description')
        readonly_fields = ('dialect_name', 'dialect_version')
        fieldsets = (
            (None, {
                'fields': ('display_name', 'description', 'enabled')
            }),
            ('Connection', {
                'fields': ('type', 'connection_str', 'connection_json'),
                'classes': ('collapse',),
            }),
            ('Dialect Info', {
                'fields': ('dialect_name', 'dialect_version'),
                'classes': ('collapse',),
            }),
        )
        actions = ['trigger_sync_metadata']

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

    @admin.register(OrganisationGroup)
    class OrganisationGroupAdmin(admin.ModelAdmin):
        list_display = ('group', 'organisation')
        list_filter = ('organisation',)
        search_fields = ('group__name', 'organisation__name')
        raw_id_fields = ('group', 'organisation')


    @admin.register(ServiceToken)
    class ServiceTokenAdmin(admin.ModelAdmin):
        list_display = ('name', 'key_prefix', 'token_type', 'organisation', 'is_active', 'last_used', 'created_at')
        list_filter = ('token_type', 'is_active', 'organisation', 'created_at')
        search_fields = ('name', 'key_prefix', 'organisation__name')
        readonly_fields = ('key_hash', 'key_prefix', 'last_used', 'created_at', 'created_by')
        fields = ('name', 'token_type', 'organisation', 'datasources', 'is_active', 'expires_at', 
                  'key_hash', 'key_prefix', 'last_used', 'created_at', 'created_by')

        filter_horizontal = ('datasources',)
        raw_id_fields = ('organisation',)

        def save_model(self, request, obj, form, change):
            if not change:

                organisation = form.cleaned_data.get('organisation')

                token, full_key = generate_service_token(
                    name=obj.name,
                    token_type=obj.token_type,
                    created_by=request.user,
                    organisation=organisation
                )

                obj.id = token.id
                obj.key_hash = token.key_hash
                obj.key_prefix = token.key_prefix
                obj.created_at = token.created_at

                datasources = form.cleaned_data.get('datasources')
                if datasources:
                    token.datasources.set(datasources)

                messages.set_level(request, messages.SUCCESS)
                messages.success(request, format_html(
                    "<strong>Token Created Successfully!</strong><br>"
                    "Authentication Key: <code>{}</code><br>"
                    "<span style='color:red'>WARNING: Copy this now. It will never be shown again.</span>",
                    full_key
                ))
            else:
                super().save_model(request, obj, form, change)
