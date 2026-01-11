"""
Admin Service URL Configuration.

REST API endpoints for datasource and schema management.
No authentication required - consuming apps should add their own auth layer.
"""

from django.urls import path
from . import views

app_name = "admin_service"

urlpatterns = [
    # DataSource Management
    path("datasources/", views.create_datasource, name="create_datasource"),
    path("datasources/<int:datasource_id>/", views.update_datasource, name="update_datasource"),
    path("datasources/<int:datasource_id>/delete/", views.delete_datasource, name="delete_datasource"),
    
    # Suggestions Management
    path("datasources/<int:datasource_id>/suggestions/", views.list_suggestions, name="list_suggestions"),
    path("datasources/<int:datasource_id>/suggestions/add/", views.add_suggestion, name="add_suggestion"),
    path("suggestions/<int:suggestion_id>/", views.delete_suggestion, name="delete_suggestion"),
    
    # Table Management
    path("tables/<int:table_id>/", views.update_table, name="update_table"),
    
    # Column Management
    path("columns/<int:column_id>/", views.update_column, name="update_column"),
    
    # Validation
    path("validate/", views.validate_connection, name="validate_connection"),
]
