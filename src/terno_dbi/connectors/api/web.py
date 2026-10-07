"""Browser-facing routes for connecting API sources.

These session-authenticated routes handle the browser OAuth flow: starting a
connection, receiving the provider callback, and falling back to the existing
manual datasource form.

The organisation comes from the request's subdomain but is always checked
against the signed-in user's memberships. The subdomain is untrusted input and
must not be enough to connect a source to another organisation.
"""

from __future__ import annotations
import json
import logging
from django.conf import settings
from django.http import HttpResponse, HttpResponseBadRequest, JsonResponse
from django.shortcuts import redirect
from django.utils.html import escape
from django.utils.http import url_has_allowed_host_and_scheme
from django.views.decorators.http import require_http_methods
from terno_dbi.connectors.api.auth.providers import login_methods
from terno_dbi.connectors.api import management_service
from terno_dbi.connectors.api.management_service import (
    is_org_admin,
    ConnectorManagementError,
    ConnectorPermissionDenied,
    ConnectorNotConnected,
)

logger = logging.getLogger(__name__)

CALLBACK_PATH = "/connectors/oauth/callback/"

# Callback `error` values that are not the user declining, mapped to what to do.
_CALLBACK_ERRORS = {
    # Zoho: the signed-in account has no organisation in the product the scopes
    # ask for, so Zoho offered no Accept button at all.
    "no_org": "This Zoho account has no Zoho CRM organisation. Sign in with an "
              "account that uses Zoho CRM, then connect again.",
}


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

    if not is_org_admin(request.user, org):
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

    requested_method = request.GET.get("login_method", "")
    allowed_methods = login_methods(connector_key)
    login_method = requested_method if requested_method in allowed_methods else ""

    try:
        started = start_authorization(
            connector_key=connector_key,
            redirect_uri=_callback_uri(request),
            organisation=org,
            return_to=return_to,
            instance=request.GET.get("shop", ""),
            login_method=login_method,
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
        message = _CALLBACK_ERRORS.get(error) or f"Authorization was declined: {escape(error)}"
        return HttpResponse(message, status=400)

    state = request.GET.get("state", "")
    code = request.GET.get("code", "")
    if not (state and code):
        return HttpResponseBadRequest("Missing state or code.")

    # Look up the return target before the state is consumed.
    st = ConnectorOAuthState.objects.filter(state=state).first()
    return_to = st.return_to if st else ""
    connector_key = st.connector_key if st else ""

    try:
        data_source = complete_authorization(
            state=state, code=code, callback_params=request.GET)
    except ApiError as exc:
        return HttpResponse(exc.message, status=400)

    if return_to:
        from urllib.parse import quote
        sep = "&" if "?" in return_to else "?"
        suffix = f"connected={quote(data_source.display_name)}"
        if connector_key:
            suffix += f"&select_accounts={quote(connector_key)}"
        return redirect(f"{return_to}{sep}{suffix}")
    return JsonResponse({
        "status": "connected",
        "datasource": data_source.display_name,
        "message": "Connected. Return to your conversation and continue.",
    })


@require_http_methods(["GET"])
def list_api_connectors(request):
    """GET: the API connectors this organisation can use, with connection state.

    Session-authenticated (the signed-in user's browser), scoped to the org the
    request resolves to and validated against membership — the same trust model
    as `connect`. Drives the frontend connector gallery. The work lives in
    `management_service`; this view only resolves identity and serialises.
    """
    org = _authorised_org(request)
    if org is None:
        if not getattr(request, "user", None) or not request.user.is_authenticated:
            return _login_redirect(request)
        return HttpResponse("You are not a member of this organisation.", status=403)
    return JsonResponse(management_service.list_connectors(request.user, org))


@require_http_methods(["POST"])
def disconnect_connector(request, connector_key):
    """POST: disconnect an API source for this organisation (admin-only).

    CSRF-protected by middleware. See `management_service.disconnect_connector`
    for what disconnect does (tokens cleared, row deliberately kept).
    """
    org = _authorised_org(request)
    if org is None:
        return HttpResponse("Not permitted.", status=403)
    try:
        return JsonResponse(
            management_service.disconnect_connector(request.user, org, connector_key))
    except ConnectorManagementError as exc:
        return _management_error_response(exc)


@require_http_methods(["GET", "POST"])
def connector_accounts(request, connector_key):
    """Manage which of a connected source's accounts are queryable.

    GET lists every visible account with its enabled flag (the account-picker
    modal); POST saves per-account deltas or a whole set. Org-admin gated and
    CSRF-protected. The work lives in `management_service`; this view only
    resolves identity, parses the body, and serialises.
    """
    org = _authorised_org(request)
    if org is None:
        return HttpResponse("Not permitted.", status=403)
    try:
        if request.method == "POST":
            try:
                body = json.loads(request.body or "{}")
            except (json.JSONDecodeError, ValueError):
                return HttpResponseBadRequest("Invalid JSON.")
            return JsonResponse(
                management_service.save_accounts(request.user, org, connector_key, body))
        return JsonResponse(
            management_service.list_accounts(request.user, org, connector_key))
    except ConnectorManagementError as exc:
        return _management_error_response(exc)


@require_http_methods(["POST"])
def connector_toggle(request, connector_key):
    """POST ``{"enabled": bool}``: enable/disable a connected API source for
    querying (admin-only). The connection and its tokens are kept — this only
    flips whether the agent may query it. Addressed by connector key, never a
    datasource id. CSRF-protected."""
    org = _authorised_org(request)
    if org is None:
        return HttpResponse("Not permitted.", status=403)
    try:
        body = json.loads(request.body or "{}")
    except (json.JSONDecodeError, ValueError):
        return HttpResponseBadRequest("Invalid JSON.")
    try:
        return JsonResponse(management_service.set_connector_enabled(
            request.user, org, connector_key, bool(body.get("enabled"))))
    except ConnectorManagementError as exc:
        return _management_error_response(exc)


def _management_error_response(exc):
    """Map a management-service error onto this browser view's HTTP response."""
    if isinstance(exc, ConnectorPermissionDenied):
        return HttpResponse(str(exc), status=403)
    if isinstance(exc, ConnectorNotConnected):
        return JsonResponse({"error": str(exc)}, status=404)
    # ConnectorBadRequest or any other expected management error.
    return JsonResponse({"error": str(exc)}, status=400)


__all__ = [
    "connect",
    "oauth_callback",
    "list_api_connectors",
    "disconnect_connector",
    "connector_accounts",
    "connector_toggle",
]
