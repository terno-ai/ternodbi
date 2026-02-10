from django.urls import path
from . import views

app_name = "query_service"

urlpatterns = [
    path("health/", views.health, name="health"),
    path("info/", views.info, name="info"),
    path("datasources/", views.list_datasources, name="list_datasources"),
    path("datasources/<str:datasource_identifier>/", views.get_datasource, name="get_datasource"),
    path("datasources/<str:datasource_identifier>/tables/", views.list_tables, name="list_tables"),
    path("datasources/<str:datasource_identifier>/tables/<str:table_identifier>/columns/",
         views.list_table_columns, name="list_table_columns"),
    path("datasources/<str:datasource_identifier>/foreign-keys/", views.list_foreign_keys, name="list_foreign_keys"),
    path("datasources/<str:datasource_identifier>/query/", views.execute_query, name="execute_query"),
    path("datasources/<str:datasource_identifier>/export/", views.export_query, name="export_query"),
    path("tables/<int:table_id>/sample/", views.get_sample_data, name="get_sample_data"),
    path("datasources/<str:datasource_identifier>/schema/", views.get_schema, name="get_schema"),
    path("tables/<int:table_id>/columns/", views.get_table_columns, name="get_table_columns"),
    path("query/", views.execute_query, name="execute_query_legacy"),
    path("export/", views.export_query, name="export_query_legacy"),
]
