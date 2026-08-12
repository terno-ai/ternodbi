"""OAuth discovery and registration routes.

Mount in terno-ai's `mysite/urls.py` at the project root, not under a prefix —
`.well-known` paths are fixed by RFC and clients will not look anywhere else:

    urlpatterns = [
        ...,
        path("", include("terno_dbi.oauth.urls")),
        path("oauth/", include("oauth2_provider.urls", namespace="oauth2_provider")),
    ]

Both documents are served on both hosts. They are small, static, and cacheable,
and serving each only on "its" vhost means a client that guesses the other host
gets a 404 instead of an answer.
"""

from django.urls import path

from terno_dbi.oauth import views

app_name = "terno_dbi_oauth"

# The two discovery documents are served unconditionally. They describe *where*
# the authorization server is; they do not need one to be installed. The 401
# from /mcp points at the protected-resource document whatever the deployment,
# so gating it behind django-oauth-toolkit would leave that pointer dangling in
# exactly the setup most likely to hit it — a fresh local install.
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
    # RFC 7591. Sits alongside DOT's /oauth/authorize and /oauth/token, which
    # DOT's own urlconf provides. Returns 503 when DOT is absent, which is the
    # honest answer: registration genuinely is unavailable.
    path("oauth/register", views.register, name="oauth-register"),
    # Alias. RFC 7591 does not mandate a path, and a client that cannot read the
    # authorization-server metadata falls back to guessing `<issuer>/register`.
    # Observed live: Claude POSTed here and got a CSRF 403 from the SPA
    # catch-all, which reads as "couldn't register with terno's sign-in service"
    # rather than pointing at the metadata.
    path("register", views.register, name="oauth-register-alias"),
]

# The consent screen, overriding DOT's. Registered before DOT's own urlconf is
# included, since Django takes the first match. Skipped entirely when DOT is not
# installed, so this module stays importable either way.
_authorization_view = views.make_authorization_view()
if _authorization_view is not None:
    urlpatterns.append(
        path("oauth/authorize/", _authorization_view.as_view(), name="authorize")
    )
