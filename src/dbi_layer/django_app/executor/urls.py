from django.urls import path
from .views import list_all_tables, list_tables, execute_sql, list_datasources, get_db_schema, list_table_columns, get_tables

urlpatterns = [
    path("api/list_tables/", list_tables, name="list_tables"),
    path("api/list_table_columns/", list_table_columns, name="list_table_columns"),
    path("api/execute_sql/", execute_sql, name="execute_sql"),
    path("api/list_datasources/", list_datasources, name="list_datasources"),
    path("api/get_db_schema/", get_db_schema, name="get_db_schema"),
    path("api/list_all_tables/", list_all_tables, name="list_all_tables"),
    path("api/get_tables/", get_tables, name="get_tables"), 
]
