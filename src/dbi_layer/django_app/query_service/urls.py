"""
Query Service URL Configuration.

Simple REST API endpoints - no authentication required.
Consuming apps should add their own auth layer.
"""

from django.urls import path
from . import views

app_name = "query_service"

urlpatterns = [
    # Health & Info
    path("health/", views.health, name="health"),
    path("info/", views.info, name="info"),
    
    # DataSources
    path("datasources/", views.list_datasources, name="list_datasources"),
    path("datasources/<int:datasource_id>/", views.get_datasource, name="get_datasource"),
    
    # Tables
    path("datasources/<int:datasource_id>/tables/", views.list_tables, name="list_tables"),
    path("datasources/<int:datasource_id>/tables/<int:table_id>/columns/", 
         views.list_columns, name="list_columns"),
    
    # Foreign Keys
    path("datasources/<int:datasource_id>/foreign-keys/", views.list_foreign_keys, name="list_foreign_keys"),
    
    # Query Execution
    path("datasources/<int:datasource_id>/query/", views.execute_query, name="execute_query"),
    path("datasources/<int:datasource_id>/export/", views.export_query, name="export_query"),
    
    # Legacy endpoints (without datasource_id in URL)
    path("query/", views.execute_query, name="execute_query_legacy"),
    path("export/", views.export_query, name="export_query_legacy"),
]
