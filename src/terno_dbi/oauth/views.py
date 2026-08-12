"""Discovery documents and Dynamic Client Registration.

These are Django views, unlike `/mcp`. They are reached through the normal URL
resolver and middleware, which is what you want here — they are ordinary
request/response endpoints with no streaming.

`register` creates the client record. It is deliberately thin: all validation
lives in `dcr.py`, so the rules can be tested without a database or a request.

## The DOT dependency is soft on purpose

`django-oauth-toolkit` provides the `Application` model this writes to, but the
discovery documents do not need it. Importing DOT lazily means
`/.well-known/...` still serves correctly in a deployment where DOT is not yet
installed — useful while Phase 4 is landing, and it keeps this module importable
in ternodbi's own test suite, which has no DOT.
"""

import json
import logging
import time

from django.conf import settings
from django.http import HttpResponseNotAllowed, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from terno_dbi.oauth.dcr import (
    InvalidRegistration,
    registration_response,
    validate_registration,
)
from terno_dbi.oauth.metadata import (
    authorization_server_metadata,
    protected_resource_metadata,
)
from terno_dbi.oauth.scopes import UnknownScope

logger = logging.getLogger(__name__)

# Registrations per IP per window. The endpoint is unauthenticated by design
# (RFC 7591), so it is a row-creation vector; this bounds it without needing a
# dependency on a rate-limit package.
REGISTRATION_RATE_LIMIT = 10
REGISTRATION_RATE_WINDOW_SECONDS = 3600


def _resource_url() -> str:
    return getattr(settings, "TERNO_MCP_BASE_URL", "https://mcp.terno.ai")


def _auth_server_url() -> str:
    return getattr(settings, "PROVISIONER_URL", "https://app.terno.ai")


def _absolute_issuer() -> str:
    """The issuer as an absolute origin.

    Settings sometimes hold a bare host ('app.terno.ai') and sometimes a full
    origin ('http://127.0.0.1:8000'). Prefixing a scheme unconditionally turns
    the second into 'https://http://127.0.0.1:8000', which 404s — a real bug hit
    while testing locally.
    """
    value = (_auth_server_url() or "app.terno.ai").rstrip("/")
    if value.startswith(("http://", "https://")):
        return value
    return f"https://{value}"


def _cors(response: JsonResponse) -> JsonResponse:
    """Discovery documents are fetched cross-origin by clients in a browser."""
    response["Access-Control-Allow-Origin"] = "*"
    response["Cache-Control"] = "public, max-age=3600"
    return response


def oauth_protected_resource(request):
    """RFC 9728 — served on the resource host, `mcp.terno.ai`.

    The 401 from `/mcp` points here via `WWW-Authenticate`; without this the
    client follows that pointer to a 404 and shows the user a bare auth error
    instead of starting sign-in.
    """
    return _cors(JsonResponse(
        protected_resource_metadata(_resource_url(), _auth_server_url())
    ))


def oauth_authorization_server(request):
    """RFC 8414 — served on the auth host, `app.terno.ai`."""
    return _cors(JsonResponse(authorization_server_metadata(_auth_server_url())))


def _client_ip(request) -> str:
    forwarded = request.META.get("HTTP_X_FORWARDED_FOR", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR", "unknown")


def _rate_limited(request) -> bool:
    from django.core.cache import cache

    key = f"dbi_dcr_rate_{_client_ip(request)}"
    try:
        count = cache.get_or_set(key, 0, REGISTRATION_RATE_WINDOW_SECONDS)
        count = cache.incr(key)
    except ValueError:
        # The key expired between get_or_set and incr.
        cache.set(key, 1, REGISTRATION_RATE_WINDOW_SECONDS)
        count = 1
    return count > REGISTRATION_RATE_LIMIT


@csrf_exempt
def register(request):
    """RFC 7591 Dynamic Client Registration.

    Unauthenticated by design: a client that has never spoken to this server has
    nothing to authenticate with. That is why the rate limit matters.
    """
    if request.method == "OPTIONS":
        return _cors(JsonResponse({}))
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])

    if _rate_limited(request):
        logger.warning("DCR rate limit hit for %s", _client_ip(request))
        return JsonResponse(
            {"error": "temporarily_unavailable",
             "error_description": "Too many registration requests. Try again later."},
            status=429,
        )

    try:
        body = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        return JsonResponse(
            {"error": "invalid_client_metadata",
             "error_description": "Request body must be valid JSON"},
            status=400,
        )

    try:
        registration = validate_registration(body)
    except UnknownScope as exc:
        return JsonResponse(
            {"error": "invalid_client_metadata", "error_description": str(exc)},
            status=400,
        )
    except InvalidRegistration as exc:
        return JsonResponse(
            {"error": exc.error, "error_description": str(exc)}, status=400
        )

    try:
        from oauth2_provider.models import get_application_model
    except ImportError:  # pragma: no cover - deployment without DOT
        logger.error("DCR requested but django-oauth-toolkit is not installed")
        return JsonResponse(
            {"error": "temporarily_unavailable",
             "error_description": "Client registration is not enabled on this server."},
            status=503,
        )

    Application = get_application_model()
    app = Application(
        name=registration.client_name,
        redirect_uris=" ".join(registration.redirect_uris),
        client_type=(
            Application.CLIENT_PUBLIC if registration.is_public
            else Application.CLIENT_CONFIDENTIAL
        ),
        authorization_grant_type=Application.GRANT_AUTHORIZATION_CODE,
        # The consent screen is the whole point — it is where the user sees what
        # is being requested. Never skip it for a self-registered client.
        skip_authorization=False,
    )
    # CAPTURE THE SECRET BEFORE SAVING. `client_secret` is a DOT
    # `ClientSecretField`: the model default generates a 128-char plaintext at
    # instantiation, and `save()` replaces it in place with a
    # `pbkdf2_sha256$...` hash. Reading it after `save()` therefore returns the
    # hash, and returning that to the client makes every token exchange fail
    # with a 401 — the client authenticates with the hash, DOT hashes it again,
    # and the two never match.
    #
    # This is not hypothetical: it is exactly how the first real Claude connect
    # attempt failed. A public client does not care (it authenticates with
    # client_id plus PKCE and the secret is never consulted), which is why the
    # bug hid until a confidential client appeared.
    plaintext_secret = app.client_secret
    app.save()

    logger.info(
        "DCR: registered client '%s' (%s) from %s",
        registration.client_name,
        "public" if registration.is_public else "confidential",
        _client_ip(request),
    )

    return JsonResponse(
        registration_response(
            registration,
            client_id=app.client_id,
            # Never returned for a public client, which has no use for it.
            client_secret=None if registration.is_public else plaintext_secret,
            issued_at=int(time.time()),
        ),
        status=201,
    )


def make_authorization_view():
    """Build the consent view, or return None where DOT is not usable.

    Deliberately not a module-level import. `oauth2_provider.views` pulls in its
    models, which raise `RuntimeError` — not `ImportError` — when the package is
    installed but absent from `INSTALLED_APPS`. That is exactly the state of
    ternodbi's own test settings, so an import guard here would break importing
    this module at all.
    """
    from django.apps import apps

    if not apps.is_installed("oauth2_provider"):
        return None

    from django import forms as django_forms
    from oauth2_provider.forms import AllowForm
    from oauth2_provider.views import AuthorizationView

    from terno_dbi.oauth.org_choice import (
        merge_into_claims,
        organisation_choices,
        validate_choice,
    )

    class TernoAllowForm(AllowForm):
        """DOT's consent form plus an organisation selector.

        The choice is merged into `claims`, which DOT already round-trips onto
        the `Grant`. That is the only place it can live: the token exchange that
        follows is a back-channel POST with no session.

        `clean` re-validates the submitted id against the user's memberships.
        The field is a browser POST and `claims` is a hidden input, so both are
        attacker-controlled — an unvalidated value here would become a grant for
        an organisation the user has no access to.
        """

        organisation = django_forms.ChoiceField(
            required=False, label="Organisation", choices=()
        )

        def __init__(self, *args, user=None, **kwargs):
            super().__init__(*args, **kwargs)
            self._user = user
            if user is not None:
                self.fields["organisation"].choices = organisation_choices(user)

        def clean(self):
            cleaned = super().clean()
            chosen = cleaned.get("organisation")
            if self._user is None or not chosen:
                return cleaned

            organisation = validate_choice(self._user, chosen)
            if organisation is None:
                # Fail closed to the default rather than erroring — a revoked
                # membership between render and submit is legitimate, and the
                # server picks the default in that case.
                logger.warning(
                    "Discarding organisation choice %r for %s", chosen, self._user
                )
                return cleaned

            cleaned["claims"] = merge_into_claims(cleaned.get("claims"), organisation.pk)
            return cleaned

    class TernoAuthorizationView(AuthorizationView):
        """DOT's consent screen, plus the context our template needs.

        Three additions, each because the default omits something the user needs
        in order to consent meaningfully:

        - **the organisation** being connected, since a user in more than one
          cannot choose and should at least see which it is (BACKLOG D17);
        - **read and write scopes separated**, rather than one flat list;
        - **whether write will actually be granted** — a non-admin's write
          scopes are stripped at mint time no matter what they approve, so
          showing the request without that caveat would mislead.
        """

        def get_login_url(self):
            """Where an unauthenticated user is sent to sign in or sign up.

            Not `settings.LOGIN_URL`, which is the bare domain — a user who
            clicks Connect in Claude with no account lands on the site root with
            no idea what to do. Signup lives in terno-web, reached at
            `/accounts/…` on this same host (nginx routes it to the provisioner),
            so point at the login screen there. Django appends `?next=` with the
            authorize URL, and the provisioner carries it through signup.

            Overridden here rather than changing `LOGIN_URL` globally, which
            every other login in the app also depends on.
            """
            configured = getattr(settings, "TERNO_CONNECTOR_LOGIN_URL", None)
            if configured:
                return configured
            return f"{_absolute_issuer()}/accounts/login/"

        def get_form_class(self):
            return TernoAllowForm

        def get_form_kwargs(self):
            kwargs = super().get_form_kwargs()
            kwargs["user"] = self.request.user
            return kwargs

        def get_context_data(self, **kwargs):
            context = super().get_context_data(**kwargs)

            from terno_dbi.oauth.metadata import scope_consent_rows
            from terno_dbi.oauth.minting import resolve_membership, user_can_write
            from terno_dbi.oauth.org_choice import organisation_choices
            from terno_dbi.oauth.provisioning import ensure_organisation
            from terno_dbi.oauth.scopes import WRITE_SCOPES

            requested = set(context.get("scopes") or [])
            rows = scope_consent_rows(requested)
            context["read_scopes"] = [r for r in rows if r["scope"] not in WRITE_SCOPES]
            context["write_scopes"] = [r for r in rows if r["scope"] in WRITE_SCOPES]

            # Provision here rather than at token issuance so the consent
            # screen can name the organisation the user is about to connect —
            # and so a failure to create one surfaces before they approve,
            # not after.
            organisation = ensure_organisation(self.request.user)
            context["organisation"] = organisation
            context["user_can_write"] = (
                user_can_write(resolve_membership(self.request.user, organisation))
                if organisation
                else False
            )

            # Only offer the selector when there is a genuine choice. A single
            # membership is stated, not presented as a decision.
            choices = organisation_choices(self.request.user)
            context["organisation_choices"] = choices if len(choices) > 1 else []
            context["selected_organisation_id"] = str(organisation.pk) if organisation else ""
            return context

    return TernoAuthorizationView
