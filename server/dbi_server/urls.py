from django.contrib import admin
from django.urls import path, include
from terno_dbi.core.views import landing_page

urlpatterns = [
    path('', landing_page, name='home'),
    path('admin/', admin.site.urls),
    path('api/', include('terno_dbi.core.urls')),
]
