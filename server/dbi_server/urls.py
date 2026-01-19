from django.contrib import admin
from django.urls import path, include
from dbi_layer.django_app.views import landing_page

urlpatterns = [
    path('', landing_page, name='home'),
    path('admin/', admin.site.urls),
    path('api/', include('dbi_layer.django_app.urls')),
]
