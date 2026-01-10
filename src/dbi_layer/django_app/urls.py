"""
URL configuration for DBI Layer Django App.

Include these URLs in your project:
    urlpatterns = [
        path("dbi/", include("dbi_layer.django_app.urls")),
    ]

This provides:
    /dbi/admin/...   - Admin service (requires admin token)
    /dbi/query/...   - Query service (requires query/admin token)
    /dbi/...         - Legacy unauthenticated endpoints (deprecated)
"""

from django.urls import path, include
from . import views

app_name = "dbi_layer"

urlpatterns = [
    # New authenticated services
    path("admin/", include("dbi_layer.django_app.admin_service.urls")),
    path("query/", include("dbi_layer.django_app.query_service.urls")),
    
    # Legacy unauthenticated endpoints (for backward compatibility)
    # These should be deprecated in favor of /query/ endpoints
    path("health/", views.health, name="health"),
    path("info/", views.info, name="info"),
    path("datasources/", views.list_datasources, name="list_datasources"),
    path("datasources/<int:datasource_id>/", views.get_datasource, name="get_datasource"),
    path("datasources/<int:datasource_id>/tables/", views.list_tables, name="list_tables"),
    path("datasources/<int:datasource_id>/tables/<int:table_id>/columns/", views.list_columns, name="list_columns"),
    path("datasources/<int:datasource_id>/query/", views.execute_query, name="execute_query"),
    path("validate/", views.validate_connection, name="validate_connection"),
]
