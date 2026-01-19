from django.urls import path, include
from . import views

app_name = "terno_dbi"

urlpatterns = [
    path("admin/", include("terno_dbi.core.admin_service.urls")),
    path("query/", include("terno_dbi.core.query_service.urls")),

    path("health/", views.health, name="health"),
    path("info/", views.info, name="info"),

]
