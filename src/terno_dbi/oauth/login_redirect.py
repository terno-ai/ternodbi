"""Preserve the OAuth authorize URL across signup and email verification.

When Clients starts OAuth without an existing session, the authorize URL is
passed through login as `next`. Signup and email verification may span multiple
requests, so `next` is otherwise lost and the user is redirected to `/` instead
of returning to the OAuth consent screen.

Store the URL in the session when first seen and consume it when selecting the
post-auth redirect target.

Only relative paths to the OAuth authorize endpoint are accepted to prevent
open redirects. This is intentionally narrow and is not a general redirect
mechanism.
"""

import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

SESSION_KEY = "terno_connector_next"

# Only these are worth carrying. Restricting by path means an unrelated `next`
# cannot be captured and replayed by this code.
AUTHORIZE_PATHS = ("/oauth/authorize",)


def is_connector_next(url: str) -> bool:
    """Whether `url` is a safe, relative link to our authorize endpoint."""
    if not url or not isinstance(url, str):
        return False

    if url.startswith("//"):
        return False
    if not url.startswith("/"):
        return False

    parsed = urlparse(url)
    if parsed.scheme or parsed.netloc:
        return False

    return any(parsed.path.startswith(p) for p in AUTHORIZE_PATHS)


def remember_next(request) -> bool:
    """Stash a connector authorize URL from this request, if it carries one.

    Checks GET then POST: the login form posts back with the value in either,
    depending on how allauth rendered it.
    """
    if not hasattr(request, "session"):
        return False

    candidate = request.GET.get("next") or (
        request.POST.get("next") if request.method == "POST" else None
    )
    if not is_connector_next(candidate):
        if candidate and "/oauth/authorize" in candidate:
            logger.warning(
                "Ignoring connector redirect %r: expected a relative path. "
                "If this is absolute, check that the proxy forwards "
                "X-Forwarded-Proto and that SECURE_PROXY_SSL_HEADER is set.",
                candidate,
            )
        return False

    if request.session.get(SESSION_KEY) != candidate:
        request.session[SESSION_KEY] = candidate
        logger.debug("Remembered connector redirect: %s", candidate)
    return True


def peek_next(request):
    if not hasattr(request, "session"):
        return None
    stored = request.session.get(SESSION_KEY)
    return stored if is_connector_next(stored) else None


def pop_next(request):
    """Return the stashed URL and clear it.

    Cleared on use so a stale entry cannot hijack an unrelated later login in
    the same browser session.
    """
    if not hasattr(request, "session"):
        return None
    stored = request.session.pop(SESSION_KEY, None)
    if not is_connector_next(stored):
        return None
    logger.info("Resuming connector authorization at %s", stored)
    return stored


class ConnectorNextMiddleware:
    """Captures the authorize URL on every request that carries one.

    A middleware rather than an adapter hook because the value has to be
    captured *early* — on the first redirect to login — while the adapter hooks
    only run much later, after the value has already been lost.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        remember_next(request)
        return self.get_response(request)


class ConnectorRedirectMixin:
    """Mixin for allauth adapters to preserve pending OAuth authorization redirects.

    Overrides the redirect decision points so a pending connector authorization
    takes priority over the default landing page. Falls back to `super()` when no
    redirect is stashed, leaving normal logins and signups unchanged.
    """

    def get_login_redirect_url(self, request):
        return pop_next(request) or super().get_login_redirect_url(request)

    def get_signup_redirect_url(self, request):
        return pop_next(request) or super().get_signup_redirect_url(request)

    def get_email_confirmation_redirect_url(self, request):
        return pop_next(request) or super().get_email_confirmation_redirect_url(request)
