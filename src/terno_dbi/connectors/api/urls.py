"""Browser-facing routes for connecting API sources.

Kept separate from the token-authenticated API views so host applications can
mount the session-based connect flow under their own browser authentication.

Both standalone and embedded deployments use the same `/connect` and OAuth
callback routes. The callback path is fixed because it is registered with the
OAuth provider and must bypass tenant/subdomain middleware when it lands on the
canonical host.
"""

from django.urls import path
from . import web

app_name = "connectors_api"

urlpatterns = [
    path("connect", web.connect, name="connect"),
    path("connectors/oauth/callback/", web.oauth_callback, name="oauth_callback"),
    # Connector gallery (session-authenticated browser API).
    path("connectors/api/", web.list_api_connectors, name="list_api_connectors"),
    path("connectors/api/<slug:connector_key>/disconnect/",
         web.disconnect_connector, name="disconnect_connector"),
]
