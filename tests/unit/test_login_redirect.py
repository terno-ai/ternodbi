"""Carrying the OAuth authorize URL across signup and email verification.

The bug: a user clicking Connect in Claude, then signing up, landed on the Terno
home page instead of the consent screen — because allauth adapters read `next`
from `request.GET` and the request completing signup does not carry it. With
mandatory email verification the confirming request is a brand-new browser hit
with no querystring at all.
"""

import pytest

from terno_dbi.oauth.login_redirect import (
    SESSION_KEY,
    ConnectorNextMiddleware,
    ConnectorRedirectMixin,
    is_connector_next,
    peek_next,
    pop_next,
    remember_next,
)

AUTHORIZE = "/oauth/authorize/?response_type=code&client_id=abc&scope=query%3Aread"


class FakeRequest:
    def __init__(self, get=None, post=None, method="GET", session=None):
        self.GET = get or {}
        self.POST = post or {}
        self.method = method
        self.session = session if session is not None else {}


# ------------------------------------------------------------- validation

def test_the_authorize_url_is_accepted():
    assert is_connector_next(AUTHORIZE)
    assert is_connector_next("/oauth/authorize")


@pytest.mark.parametrize("hostile", [
    "https://evil.example/steal",       # absolute, off-site
    "//evil.example/steal",             # protocol-relative — leaves the site
    "http://localhost:8000/oauth/authorize",  # absolute, even to us
    "javascript:alert(1)",
    "/accounts/profile/",               # relative but not the authorize endpoint
    "oauth/authorize",                  # not rooted
    "", None, 123,
])
def test_everything_else_is_refused(hostile):
    """A redirect target from a request parameter is an open-redirect vector.
    Only relative paths at the authorize endpoint are carried."""
    assert not is_connector_next(hostile)


# ---------------------------------------------------------------- stashing

def test_remember_then_pop():
    request = FakeRequest(get={"next": AUTHORIZE})
    assert remember_next(request) is True
    assert request.session[SESSION_KEY] == AUTHORIZE
    assert pop_next(request) == AUTHORIZE
    assert SESSION_KEY not in request.session, "must be cleared on use"


def test_pop_is_single_use():
    """A stale entry must not hijack an unrelated later login in the same
    browser session."""
    request = FakeRequest(get={"next": AUTHORIZE})
    remember_next(request)
    assert pop_next(request) == AUTHORIZE
    assert pop_next(request) is None


def test_next_is_read_from_post_too():
    """allauth's login form posts back; the value may be in either place."""
    request = FakeRequest(post={"next": AUTHORIZE}, method="POST")
    assert remember_next(request) is True
    assert peek_next(request) == AUTHORIZE


def test_unrelated_next_is_not_captured():
    request = FakeRequest(get={"next": "/dashboard/"})
    assert remember_next(request) is False
    assert request.session == {}


def test_a_hostile_value_already_in_the_session_is_ignored():
    """Defence in depth: validated on the way out as well as on the way in."""
    request = FakeRequest(session={SESSION_KEY: "https://evil.example/"})
    assert peek_next(request) is None
    assert pop_next(request) is None


def test_missing_session_is_survivable():
    class NoSession:
        GET = {}
        POST = {}
        method = "GET"

    assert remember_next(NoSession()) is False
    assert pop_next(NoSession()) is None


# -------------------------------------------------------------- middleware

def test_middleware_captures_on_the_way_through():
    seen = {}

    def get_response(request):
        seen["session"] = dict(request.session)
        return "response"

    request = FakeRequest(get={"next": AUTHORIZE})
    assert ConnectorNextMiddleware(get_response)(request) == "response"
    assert seen["session"][SESSION_KEY] == AUTHORIZE


# ------------------------------------------------------------------ mixin

class _Base:
    def get_login_redirect_url(self, request):
        return "/"

    def get_signup_redirect_url(self, request):
        return "/"

    def get_email_confirmation_redirect_url(self, request):
        return "/"


class _Adapter(ConnectorRedirectMixin, _Base):
    pass


@pytest.mark.parametrize("hook", [
    "get_login_redirect_url",
    "get_signup_redirect_url",
    "get_email_confirmation_redirect_url",
])
def test_mixin_resumes_authorization_at_every_hook(hook):
    """All three matter. Email confirmation is the one that was broken, since
    that request arrives with no querystring."""
    request = FakeRequest(get={"next": AUTHORIZE})
    remember_next(request)
    assert getattr(_Adapter(), hook)(request) == AUTHORIZE


@pytest.mark.parametrize("hook", [
    "get_login_redirect_url",
    "get_signup_redirect_url",
    "get_email_confirmation_redirect_url",
])
def test_mixin_leaves_ordinary_logins_alone(hook):
    """Nothing stashed means the app's normal landing page, unchanged."""
    assert getattr(_Adapter(), hook)(FakeRequest()) == "/"


def test_mixin_must_precede_the_adapter_in_the_mro():
    """Listed second, the default implementation would win and this would
    silently do nothing."""
    assert _Adapter.__mro__.index(ConnectorRedirectMixin) < _Adapter.__mro__.index(_Base)


# ------------------------------------- the copy in terno-web must not drift

def test_the_provisioner_copy_matches():
    """The provisioner cannot import this module — terno-web has no terno-dbi
    dependency, and adding the whole connector stack for one redirect would be a
    poor trade. So the logic is duplicated, and the duplicate must agree."""
    from pathlib import Path

    copy = Path("/Users/navin/terno/terno-web/terno/provisioner/connector_redirect.py")
    if not copy.is_file():
        pytest.skip("terno-web checkout not present")

    body = copy.read_text()
    for token in ("SESSION_KEY = \"terno_connector_next\"",
                  "AUTHORIZE_PATHS = (\"/oauth/authorize\",)",
                  'url.startswith("//")',
                  "def pop_next", "class ConnectorNextMiddleware"):
        assert token in body, f"provisioner copy is missing: {token}"


# --------------------------------- the cross-app hop (provisioner -> terno-ai)

AUTHORIZE_FULL = (
    "/oauth/authorize/?response_type=code&client_id=abc"
    "&redirect_uri=https%3A%2F%2Fclaude.ai%2Fapi%2Fmcp%2Fauth_callback"
    "&code_challenge=xyz&scope=query%3Aread+query%3Aexecute"
)


def test_the_authorize_url_survives_url_encoding():
    """The provisioner encodes it into the SSO URL's querystring, and terno-ai
    unquotes it. It carries its own querystring, so without encoding the
    parameters would be parsed as parameters of the SSO URL and lost."""
    from urllib.parse import quote, unquote

    encoded = quote(AUTHORIZE_FULL, safe="")
    assert "?" not in encoded and "&" not in encoded, "would collide with the SSO querystring"
    assert is_connector_next(unquote(encoded))
    assert unquote(encoded) == AUTHORIZE_FULL


@pytest.mark.parametrize("hostile", [
    "https://evil.example/",
    "//evil.example/",
    "/admin/",              # a real terno path, but not the connector flow
    "app",                  # the ordinary enum value
    "add-ds",
])
def test_sso_only_honours_connector_paths(hostile):
    """terno-ai's sso_login checks `is_connector_next` before redirecting. The
    enum values must fall through to the existing behaviour, and anything
    off-site must be refused outright."""
    assert not is_connector_next(hostile)


def test_sso_login_guard_is_the_shared_validator():
    """terno-ai imports this exact function, so the two cannot disagree about
    what is safe to redirect to."""
    from pathlib import Path

    view = Path("/Users/navin/terno/terno-ai/terno/terno/views.py")
    if not view.is_file():
        pytest.skip("terno-ai checkout not present")

    body = view.read_text()
    assert "from terno_dbi.oauth.login_redirect import is_connector_next" in body
    assert "if redirect_to and is_connector_next(redirect_to):" in body


def test_sso_redirect_to_round_trips_without_corruption():
    """The provisioner encodes the authorize path into the SSO querystring and
    terno-ai reads it from `request.GET`, which Django has already decoded once.

    An extra `unquote()` there — which the original `sso_login` did — is a second
    decode: `redirect_uri=https%3A%2F%2F…` collapses to unencoded form, and any
    parameter whose decoded value contains %, & or + is mangled outright.
    """
    from urllib.parse import parse_qs, quote, unquote, urlparse

    authorize = (
        "/oauth/authorize/?response_type=code&client_id=abc"
        "&redirect_uri=https%3A%2F%2Fclaude.ai%2Fapi%2Fmcp%2Fauth_callback"
        "&scope=query%3Aread+query%3Aexecute"
    )
    url = f"http://127.0.0.1:8000/sso-login?redirect_to={quote(authorize, safe='')}"

    received = parse_qs(urlparse(url).query)["redirect_to"][0]   # == request.GET
    assert received == authorize
    assert is_connector_next(received)
    assert "%3A%2F%2F" in received, "redirect_uri must stay encoded"

    # And the double-decode that used to happen does corrupt it.
    assert unquote(received) != authorize
    assert "%3A%2F%2F" not in unquote(received)


def test_sso_login_does_not_double_decode():
    from pathlib import Path

    view = Path("/Users/navin/terno/terno-ai/terno/terno/views.py")
    if not view.is_file():
        pytest.skip("terno-ai checkout not present")

    body = view.read_text()
    assert "if redirect_to and is_connector_next(redirect_to):" in body
    assert "is_connector_next(unquote(redirect_to))" not in body


def test_an_absolute_authorize_url_is_refused_but_logged(caplog):
    """The failure mode that cost a debugging session.

    Django's `AccessMixin.handle_no_permission()` only shortens `next` to a
    relative path when the login URL's scheme *and* host match the request. A
    proxy that does not forward `X-Forwarded-Proto` makes Django see `http`,
    disagree with an `https` login URL, and emit a full absolute URL — which is
    correctly refused here, but was refused *silently*.
    """
    import logging

    absolute = "http://example.ngrok-free.dev/oauth/authorize/?response_type=code"
    assert not is_connector_next(absolute)

    request = FakeRequest(get={"next": absolute})
    with caplog.at_level(logging.WARNING):
        assert remember_next(request) is False

    messages = [r.getMessage() for r in caplog.records]
    assert any("X-Forwarded-Proto" in m for m in messages), (
        f"an absolute authorize URL must say why it was ignored; got {messages}"
    )


def test_an_unrelated_next_is_refused_quietly(caplog):
    """Only authorize URLs are worth a warning; ordinary redirects are not our
    business and must not fill the logs."""
    import logging

    request = FakeRequest(get={"next": "/dashboard/"})
    with caplog.at_level(logging.WARNING):
        assert remember_next(request) is False
    assert not caplog.records
