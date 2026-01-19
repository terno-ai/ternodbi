from django.contrib import admin
from django.urls import path, include
from terno_dbi.core.views import landing_page, doc_view
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('', landing_page, name='home'),
    path('docs/', doc_view, name='docs_home'),
    path('docs/<str:page>/', doc_view, name='docs'),
    path('admin/', admin.site.urls),
    path('api/', include('terno_dbi.core.urls')),
]


if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
