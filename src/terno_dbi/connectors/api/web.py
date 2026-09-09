"""Browser-facing routes for connecting API sources.

These session-authenticated routes handle the browser OAuth flow: starting a
connection, receiving the provider callback, and falling back to the existing
manual datasource form.

The organisation comes from the request's subdomain but is always checked
against the signed-in user's memberships. The subdomain is untrusted input and
must not be enough to connect a source to another organisation.
"""

from __future__ import annotations
import logging
from django.conf import settings
from django.http import HttpResponse, HttpResponseBadRequest, JsonResponse
from django.shortcuts import redirect
from django.utils.http import url_has_allowed_host_and_scheme
from django.views.decorators.http import require_http_methods

logger = logging.getLogger(__name__)

CALLBACK_PATH = "/connectors/oauth/callback/"


def _org_from_subdomain(request):
    """Resolve the organisation named by the request host's first label.

    Untrusted — the caller must still confirm the signed-in user belongs to it.
    """
    from terno_dbi.core.models import CoreOrganisation

    host = request.get_host().split(":")[0]
    label = host.split(".")[0]
    return CoreOrganisation.objects.filter(subdomain=label).first()


def _authorised_org(request):
    """Return the organisation this request is allowed to act on, or `None`.

    The organisation ID comes from `request.org_id` when provided by the host
    application, with a subdomain fallback for standalone deployments. In either
    case, the ID is validated against the user's organisation memberships before
    use; request data is never trusted on its own.
    """
    from terno_dbi.oauth.org_choice import validate_choice

    if not getattr(request, "user", None) or not request.user.is_authenticated:
        return None

    org_id = getattr(request, "org_id", None)
    if org_id is None:
        org = _org_from_subdomain(request)
        org_id = org.id if org else None
    if org_id is None:
        return None
    # Reuse the membership check the OAuth provider side already trusts.
    return validate_choice(request.user, org_id)


def _login_redirect(request):
    login_url = getattr(settings, "LOGIN_URL", "/accounts/login/")
    return redirect(f"{login_url}?next={request.get_full_path()}")


def _is_org_admin(user, org) -> bool:
    """Whether `user` may manage (connect/disconnect) sources for `org`.

    Connecting a source exposes the connector's data to the whole organisation on
    that user's provider credentials, so it is an admin action — mirroring how a
    database source is added. The admin set is: a member of the configurable
    "Org Admin" group (the same group that grants the agent its admin scope), the
    organisation's owner, or a Django superuser. Querying a connected source
    stays open to every member (narrowed only by the account allowlist).
    """
    if getattr(user, "is_superuser", False):
        return True
    if getattr(org, "owner_id", None) == getattr(user, "id", None):
        return True
    group = getattr(settings, "TERNO_ORG_ADMIN_GROUP", "Org Admin")
    return user.groups.filter(name=group).exists()


def _callback_uri(request) -> str:
    """The single redirect URI registered with the OAuth provider.

    The request host is the org subdomain (e.g. ``acme.terno.ai``), but a
    provider like Google forbids wildcard redirect URIs — so the callback must
    be one canonical host. When ``MAIN_DOMAIN`` is set (an embedding, multi-tenant
    deployment such as terno-ai) we use it and rely on ``return_to`` to bounce the
    user back to their subdomain afterwards. The standalone single-host server
    (and local dev) has no ``MAIN_DOMAIN`` and falls back to the request host.
    """
    main_domain = getattr(settings, "MAIN_DOMAIN", "") or ""
    if not settings.DEBUG and main_domain:
        return f"https://{main_domain}{CALLBACK_PATH}"
    scheme = "http" if settings.DEBUG else "https"
    return f"{scheme}://{request.get_host()}{CALLBACK_PATH}"


def connect(request):
    """Entry point for a connect link. Dispatches by the connector's auth type."""
    from terno_dbi.connectors.api.model.errors import ApiError
    from terno_dbi.connectors.api.auth.oauth import start_authorization
    from terno_dbi.core.models import ConnectorCatalog
    from terno_dbi.mcp.setup_link import datasource_setup_url

    connector_key = request.GET.get("connector", "")
    catalog = ConnectorCatalog.objects.filter(key=connector_key, enabled=True).first()
    if catalog is None:
        return HttpResponseBadRequest("Unknown or disabled connector.")

    if not getattr(request, "user", None) or not request.user.is_authenticated:
        return _login_redirect(request)

    org = _authorised_org(request)
    if org is None:
        return HttpResponse("You are not a member of this organisation.", status=403)

    if not _is_org_admin(request.user, org):
        return HttpResponse(
            "Only organisation admins can connect a data source. Ask an admin "
            "to connect it — once connected, everyone in the org can query it.",
            status=403,
        )

    return_to = request.GET.get("return_to", "")
    if return_to and not url_has_allowed_host_and_scheme(
        return_to, allowed_hosts={request.get_host()},
        require_https=not settings.DEBUG,
    ):
        return_to = ""   # ignore an off-site return target

    if catalog.auth_type == ConnectorCatalog.AuthType.MANUAL:
        # Credentials never pass through OAuth; the user fills the admin form.
        url = datasource_setup_url(org.subdomain)
        return redirect(url or "/admin/")

    try:
        started = start_authorization(
            connector_key=connector_key,
            redirect_uri=_callback_uri(request),
            organisation=org,
            return_to=return_to,
        )
    except ApiError as exc:
        return HttpResponse(exc.message, status=400)

    return redirect(started["authorization_url"])


def oauth_callback(request):
    """The provider redirects here with `code` and `state`."""
    from terno_dbi.connectors.api.model.errors import ApiError
    from terno_dbi.connectors.api.auth.oauth import complete_authorization
    from terno_dbi.core.models import ConnectorOAuthState

    error = request.GET.get("error")
    if error:
        return HttpResponse(f"Authorization was declined: {error}", status=400)

    state = request.GET.get("state", "")
    code = request.GET.get("code", "")
    if not (state and code):
        return HttpResponseBadRequest("Missing state or code.")

    # Look up the return target before the state is consumed.
    st = ConnectorOAuthState.objects.filter(state=state).first()
    return_to = st.return_to if st else ""

    try:
        data_source = complete_authorization(state=state, code=code)
    except ApiError as exc:
        return HttpResponse(exc.message, status=400)

    if return_to:
        # Signal success to the returning page (the connector gallery toasts on
        # ?connected and reopens). Preserve any existing query string.
        from urllib.parse import quote
        sep = "&" if "?" in return_to else "?"
        return redirect(f"{return_to}{sep}connected={quote(data_source.display_name)}")
    return JsonResponse({
        "status": "connected",
        "datasource": data_source.display_name,
        "message": "Connected. Return to your conversation and continue.",
    })


def _connector_status(ds) -> str:
    """Map a DataSource's auth_status onto the card status the gallery renders."""
    from terno_dbi.core.models import DataSource

    if ds is None:
        return "not_connected"
    return {
        DataSource.AuthStatus.CONNECTED: "connected",
        DataSource.AuthStatus.EXPIRED: "expired",
        DataSource.AuthStatus.ERROR: "error",
        DataSource.AuthStatus.NOT_AUTHENTICATED: "not_connected",
    }.get(ds.auth_status, "not_connected")


def _connector_cards(org) -> list:
    """A card per enabled API connector, carrying this org's connection state.

    The *offer* is the ConnectorCatalog (family=api, enabled); the *state* is the
    org's DataSource for each. Every connector declared and enabled in ternodbi
    appears here automatically — there is no per-connector code. A connected row
    wins over an unauthenticated leftover for the same source.
    """
    from terno_dbi.core.models import ConnectorCatalog, DataSource

    catalogs = list(ConnectorCatalog.objects.filter(family="api", enabled=True))
    ds_by_key = {}
    for ds in (DataSource.objects
               .filter(organisation=org, catalog__family="api")
               .select_related("catalog")):
        key = ds.catalog.key
        if key not in ds_by_key or ds.auth_status == DataSource.AuthStatus.CONNECTED:
            ds_by_key[key] = ds

    cards = []
    for cat in catalogs:
        ds = ds_by_key.get(cat.key)
        cards.append({
            "key": cat.key,
            "name": cat.name,
            "provider": cat.provider,
            "category": cat.category,
            "description": cat.summary,
            "icon_url": cat.icon_url,
            "most_popular": cat.most_popular,
            "status": _connector_status(ds),
            "datasource_id": ds.id if ds else None,
            "last_error": (getattr(ds, "auth_error", "") if ds else "") or "",
        })
    return cards


@require_http_methods(["GET"])
def list_api_connectors(request):
    """GET: the API connectors this organisation can use, with connection state.

    Session-authenticated (the signed-in user's browser), scoped to the org the
    request resolves to and validated against membership — the same trust model
    as `connect`. Drives the frontend connector gallery.
    """
    org = _authorised_org(request)
    if org is None:
        if not getattr(request, "user", None) or not request.user.is_authenticated:
            return _login_redirect(request)
        return HttpResponse("You are not a member of this organisation.", status=403)
    # can_manage tells the gallery whether to show Connect/Disconnect controls;
    # the connect and disconnect endpoints enforce it server-side regardless.
    return JsonResponse({
        "connectors": _connector_cards(org),
        "can_manage": _is_org_admin(request.user, org),
    })


@require_http_methods(["POST"])
def disconnect_connector(request, connector_key):
    """POST: disconnect an API source for this organisation.

    Clears the stored OAuth tokens and marks the DataSource not-authenticated.
    The row is *kept* on purpose — deleting it would cascade away the source's
    memory, schema metadata and history, which a disconnect must not destroy. A
    not-authenticated API source is filtered out of the connected datasource
    listings (so it leaves the sidebar and the agent's queryable set) and offered
    again as a reconnect in the connector gallery. All rows for the connector are
    cleared (a source connected more than once before the reuse fix may have
    duplicates). CSRF-protected by middleware.
    """
    from terno_dbi.core.models import DataSource

    org = _authorised_org(request)
    if org is None:
        return HttpResponse("Not permitted.", status=403)

    if not _is_org_admin(request.user, org):
        return HttpResponse(
            "Only organisation admins can disconnect a data source.", status=403)

    rows = list(DataSource.objects.filter(
        organisation=org, catalog__key=connector_key, catalog__family="api",
    ))
    if not rows:
        return JsonResponse({"error": "Not connected."}, status=404)

    for ds in rows:
        ds.connection_json = {}
        ds.auth_status = DataSource.AuthStatus.NOT_AUTHENTICATED
        fields = ["connection_json", "auth_status"]
        if hasattr(ds, "auth_error"):
            ds.auth_error = ""
            fields.append("auth_error")
        ds.save(update_fields=fields)
    logger.info("disconnected API connector %r for org %s (%d row(s))",
                connector_key, org.id, len(rows))
    return JsonResponse({"status": "disconnected", "key": connector_key})


__all__ = [
    "connect",
    "oauth_callback",
    "list_api_connectors",
    "disconnect_connector",
]
