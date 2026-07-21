from django.urls import path
from . import views

app_name = "query_service"

urlpatterns = [
    path("organisation/prompt/", views.get_org_prompt, name="get_org_prompt"),
    path("organisation/prompt/grep/", views.grep_org_prompt, name="grep_org_prompt"),
    path("datasources/", views.list_datasources, name="list_datasources"),
    path("datasources/<str:datasource_identifier>/tables/", views.list_tables, name="list_tables"),
    path("datasources/<str:datasource_identifier>/tables/<str:table_identifier>/columns/",
         views.list_table_columns, name="list_table_columns"),
    path("datasources/<str:datasource_identifier>/foreign-keys/", views.list_foreign_keys, name="list_foreign_keys"),
    path("datasources/<str:datasource_identifier>/query/", views.execute_query, name="execute_query"),
    path("datasources/<str:datasource_identifier>/stream/", views.stream_query, name="stream_query"),
    path("datasources/<str:datasource_identifier>/export/", views.export_query, name="export_query"),
    path("tables/<int:table_id>/sample/", views.get_sample_data, name="get_sample_data"),
    path("query/", views.execute_query, name="execute_query_legacy"),
    path("similar-examples/", views.get_similar_examples_for_agent, name="similar_examples"),
    path("add-examples/", views.add_prompt_example, name="add_examples"),
    path("export/", views.export_query, name="export_query_legacy"),
    # path("datasources/<str:datasource_identifier>/context/",
    #      views.get_datasource_context, name="get_datasource_context"),
    path("memory/", views.list_memories, name="list_memories"),
    path("memory/save/", views.save_memory, name="save_memory"),
    path("memory/grep/", views.grep_memory, name="grep_memory"),
    path("memory/<str:name>/", views.get_memory, name="get_memory"),
    path("memory/<str:name>/edit/", views.edit_memory, name="edit_memory"),
    path("memory/<str:name>/delete/", views.delete_memory, name="delete_memory"),
]
