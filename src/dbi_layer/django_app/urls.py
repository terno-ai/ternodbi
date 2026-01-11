"""
URL configuration for DBI Layer Django App.

Include these URLs in your project:
    urlpatterns = [
        path("dbi/", include("dbi_layer.django_app.urls")),
    ]

This provides:
    /dbi/query/...   - Query service (list datasources, tables, execute queries)
    /dbi/admin/...   - Admin service (create, update, delete datasources)
    /dbi/...         - Direct endpoints (health, info, validation)
"""

from django.urls import path, include
from . import views

app_name = "dbi_layer"

urlpatterns = [
    # Core services
    path("admin/", include("dbi_layer.django_app.admin_service.urls")),
    path("query/", include("dbi_layer.django_app.query_service.urls")),
    
    # Direct endpoints
    path("health/", views.health, name="health"),
    path("info/", views.info, name="info"),
    path("datasources/", views.list_datasources, name="list_datasources"),
    path("datasources/<int:datasource_id>/", views.get_datasource, name="get_datasource"),
    path("datasources/<int:datasource_id>/tables/", views.list_tables, name="list_tables"),
    path("datasources/<int:datasource_id>/tables/<int:table_id>/columns/", views.list_columns, name="list_columns"),
    path("datasources/<int:datasource_id>/query/", views.execute_query, name="execute_query"),
    path("validate/", views.validate_connection, name="validate_connection"),
]
