from django.urls import path
from . import views

app_name = "query_service"

urlpatterns = [
    path("health/", views.health, name="health"),
    path("info/", views.info, name="info"),
    path("datasources/", views.list_datasources, name="list_datasources"),
    path("datasources/<int:datasource_id>/", views.get_datasource, name="get_datasource"),
    path("datasources/<int:datasource_id>/tables/", views.list_tables, name="list_tables"),
    path("datasources/<int:datasource_id>/tables/<int:table_id>/columns/",
         views.list_columns, name="list_columns"),
    path("datasources/<int:datasource_id>/foreign-keys/", views.list_foreign_keys, name="list_foreign_keys"),
    path("datasources/<int:datasource_id>/query/", views.execute_query, name="execute_query"),
    path("datasources/<int:datasource_id>/export/", views.export_query, name="export_query"),
    path("tables/<int:table_id>/sample/", views.get_sample_data, name="get_sample_data"),
    path("datasources/<int:datasource_id>/schema/", views.get_schema, name="get_schema"),
    path("tables/<int:table_id>/columns/", views.get_table_columns, name="get_table_columns"),
    path("query/", views.execute_query, name="execute_query_legacy"),
    path("export/", views.export_query, name="export_query_legacy"),
]
