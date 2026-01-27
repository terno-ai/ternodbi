from django.urls import path
from . import views

app_name = "admin_service"

urlpatterns = [
    path("datasources/", views.create_datasource, name="create_datasource"),
    path("datasources/<str:datasource_identifier>/", views.update_datasource, name="update_datasource"),
    path("datasources/<str:datasource_identifier>/delete/", views.delete_datasource, name="delete_datasource"),
    path("tables/<int:table_id>/", views.update_table, name="update_table"),
    path("columns/<int:column_id>/", views.update_column, name="update_column"),
    path("validate/", views.validate_connection, name="validate_connection"),
    path("datasources/<str:datasource_identifier>/sync/", views.sync_metadata, name="sync_metadata"),
    path("datasources/<str:datasource_identifier>/tables/<str:table_name>/info/", views.get_table_info, name="get_table_info"),
    path("datasources/<str:datasource_identifier>/tables/info/", views.get_all_tables_info, name="get_all_tables_info"),
]
