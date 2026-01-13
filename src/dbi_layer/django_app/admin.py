"""
Django Admin registrations for TernoDBI models.

Provides a web interface for managing datasources, tables, columns, and access control.
Access at: http://localhost:8000/admin/

NOTE: These registrations are conditional - they only apply when running TernoDBI
standalone. When used with TernoAI, TernoAI provides its own admin configurations.
"""

from django.contrib import admin
from django.apps import apps

# Check if TernoAI's terno app is installed - if so, skip registration
# because TernoAI has its own admin.py that registers these models
TERNO_AI_INSTALLED = apps.is_installed('terno')

if not TERNO_AI_INSTALLED:
    from .models import (
        DataSource, Table, TableColumn, ForeignKey,
        PrivateTableSelector, GroupTableSelector,
        PrivateColumnSelector, GroupColumnSelector,
        GroupTableRowFilter, TableRowFilter,
        DatasourceSuggestions,
    )

    class TableColumnInline(admin.TabularInline):
        """Inline display of columns within a table."""
        model = TableColumn
        extra = 0
        fields = ('name', 'public_name', 'data_type', 'description')
        readonly_fields = ('name', 'data_type')

    class ForeignKeyInline(admin.TabularInline):
        """Inline display of foreign keys within a table."""
        model = ForeignKey
        fk_name = 'constrained_table'
        extra = 0
        fields = ('constrained_columns', 'referred_table', 'referred_columns')

    @admin.register(DataSource)
    class DataSourceAdmin(admin.ModelAdmin):
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

    @admin.register(Table)
    class TableAdmin(admin.ModelAdmin):
        list_display = ('name', 'public_name', 'data_source', 'column_count')
        list_filter = ('data_source',)
        search_fields = ('name', 'public_name', 'description')
        inlines = [TableColumnInline, ForeignKeyInline]
        
        def column_count(self, obj):
            return obj.tablecolumn_set.count()
        column_count.short_description = 'Columns'

    @admin.register(TableColumn)
    class TableColumnAdmin(admin.ModelAdmin):
        list_display = ('name', 'public_name', 'table', 'data_type')
        list_filter = ('table__data_source', 'data_type')
        search_fields = ('name', 'public_name', 'description')
        raw_id_fields = ('table',)

    @admin.register(ForeignKey)
    class ForeignKeyAdmin(admin.ModelAdmin):
        list_display = ('constrained_table', 'constrained_columns', 'referred_table', 'referred_columns')
        list_filter = ('constrained_table__data_source',)
        raw_id_fields = ('constrained_table', 'constrained_columns', 'referred_table', 'referred_columns')

    @admin.register(DatasourceSuggestions)
    class DatasourceSuggestionsAdmin(admin.ModelAdmin):
        list_display = ('suggestion', 'data_source')
        list_filter = ('data_source',)
        search_fields = ('suggestion',)

    # Access Control Models
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

    from dbi_layer.django_app.models import ServiceToken
    from django.contrib import messages
    from django.utils.html import format_html
    from dbi_layer.services.auth import generate_service_token

    @admin.register(ServiceToken)
    class ServiceTokenAdmin(admin.ModelAdmin):
        list_display = ('name', 'key_prefix', 'token_type', 'is_active', 'last_used', 'created_at')
        list_filter = ('token_type', 'is_active', 'created_at')
        search_fields = ('name', 'key_prefix')
        readonly_fields = ('key_hash', 'key_prefix', 'last_used', 'created_at', 'created_by')
        fields = ('name', 'token_type', 'datasources', 'is_active', 'expires_at', 
                  'key_hash', 'key_prefix', 'last_used', 'created_at', 'created_by')
        
        filter_horizontal = ('datasources',)

        def save_model(self, request, obj, form, change):
            if not change:  # Creating new token
                # We interpret the save as a request to generate a new token
                # The obj already has data from the form, but isn't saved yet.
                
                # Use our secure generation utility
                token, full_key = generate_service_token(
                    name=obj.name, 
                    token_type=obj.token_type,
                    created_by=request.user
                )
                
                # Copy generated fields back to the form object to satisfy Django admin
                obj.id = token.id
                obj.key_hash = token.key_hash
                obj.key_prefix = token.key_prefix
                obj.created_at = token.created_at
                
                # Handle M2M separately after save, but here we just need to ensure the obj reference is correct
                # Django admin will call save_m2m() next, using 'obj'.
                
                # SHOW THE KEY TO USER (One time only)
                messages.set_level(request, messages.SUCCESS)
                messages.success(request, format_html(
                    "<strong>Token Created Successfully!</strong><br>"
                    "Authentication Key: <code>{}</code><br>"
                    "<span style='color:red'>WARNING: Copy this now. It will never be shown again.</span>",
                    full_key
                ))
            else:
                # Update existing (revocation, renaming, etc)
                super().save_model(request, obj, form, change)
