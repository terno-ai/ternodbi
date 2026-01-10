"""
Query Service URL Configuration.

All endpoints require a valid service token (admin or query) OR session auth.
"""

from django.urls import path
from . import views

app_name = "query_service"

urlpatterns = [
    # Health & Info
    path("health/", views.health, name="health"),
    path("info/", views.info, name="info"),
    
    # DataSources
    path("datasources/", views.get_datasources, name="get_datasources"),
    path("datasources/<str:ds_id>/", views.get_datasource_name, name="get_datasource_name"),
    
    # Tables
    path("datasources/<int:datasource_id>/tables/", views.get_tables, name="get_tables"),
    path("datasources/<int:datasource_id>/tables/<int:table_id>/columns/", 
         views.list_columns, name="list_columns"),
    
    # Foreign Keys
    path("datasources/<int:datasource_id>/foreign-keys/", views.list_foreign_keys, name="list_foreign_keys"),
    
    # Query Execution
    path("datasources/<int:datasource_id>/query/", views.execute_sql, name="execute_sql"),
    path("datasources/<int:datasource_id>/export/", views.export_sql_result, name="export_sql_result"),
]

