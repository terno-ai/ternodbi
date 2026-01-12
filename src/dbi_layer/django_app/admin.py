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
