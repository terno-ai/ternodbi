from django.urls import path, include
from . import views

app_name = "terno_dbi"

urlpatterns = [
    path("admin/", include("terno_dbi.core.admin_service.urls")),
    path("query/", include("terno_dbi.core.query_service.urls")),

    path("health/", views.health, name="health"),
    path("info/", views.info, name="info"),
    path("datasources/", views.list_datasources, name="list_datasources"),
    path("datasources/<int:datasource_id>/", views.get_datasource, name="get_datasource"),
    path("datasources/<int:datasource_id>/tables/", views.list_tables, name="list_tables"),
    path("datasources/<int:datasource_id>/tables/<int:table_id>/columns/", views.list_columns, name="list_columns"),
    path("datasources/<int:datasource_id>/query/", views.execute_query, name="execute_query"),
    path("validate/", views.validate_connection, name="validate_connection"),
]
