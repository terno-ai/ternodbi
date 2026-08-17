"""OAuth discovery and registration routes.

These routes are mounted at the project root because the `.well-known` paths
are defined by the OAuth RFCs and clients expect them there.

The discovery and registration endpoints are served on both the resource and
authorization hosts so clients can complete discovery regardless of which
host they start from.
"""

from django.urls import path
from terno_dbi.oauth import views

app_name = "terno_dbi_oauth"


urlpatterns = [
    path(
        ".well-known/oauth-protected-resource",
        views.oauth_protected_resource,
        name="oauth-protected-resource",
    ),
    path(
        ".well-known/oauth-authorization-server",
        views.oauth_authorization_server,
        name="oauth-authorization-server",
    ),
    path("oauth/register", views.register, name="oauth-register"),
    path("register", views.register, name="oauth-register-alias"),
]

_authorization_view = views.make_authorization_view()
if _authorization_view is not None:
    urlpatterns.append(
        path("oauth/authorize/", _authorization_view.as_view(), name="authorize")
    )
