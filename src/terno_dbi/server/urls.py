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

# Discovery documents and DCR. Always mounted: the 401 from /mcp points at the
# protected-resource document regardless of whether OAuth is fully configured.
urlpatterns += [path('', include('terno_dbi.oauth.urls'))]

# The authorize/token/revoke endpoints themselves need django-oauth-toolkit.
if settings.OAUTH_ENABLED:
    urlpatterns += [
        path('oauth/', include('oauth2_provider.urls', namespace='oauth2_provider')),
    ]

if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
