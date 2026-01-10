"""
Admin Service URL Configuration.

All endpoints require a valid admin token.
"""

from django.urls import path
from . import views

app_name = "admin_service"

urlpatterns = [
    # Token Management
    path("tokens/", views.list_tokens, name="list_tokens"),
    path("tokens/create/", views.create_token, name="create_token"),
    path("tokens/<int:token_id>/revoke/", views.revoke_token, name="revoke_token"),
    path("tokens/<int:token_id>/", views.delete_token, name="delete_token"),
    
    # DataSource Management
    path("datasources/create/", views.add_datasource, name="create_datasource"),
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
]
