"""The OAuth authorization-code flow for connecting an API source.

Two halves:

- `start_authorization` builds the provider consent URL and records the state
  needed to finish (PKCE verifier, target org/datasource). The user opens the
  URL and grants access.
- `complete_authorization` runs on the provider's redirect back: it exchanges
  the code for tokens, encrypts them, and stores them on a `DataSource`.

The token-exchange HTTP call is injected (`http_post`) so the flow is testable
without a live provider. Tokens are never logged and never returned to the
caller — only the resulting datasource is.
"""

from __future__ import annotations
import base64
import hashlib
import logging
import secrets as _secrets
import time
from datetime import timedelta
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import urlencode
from django.utils import timezone
import requests
from terno_dbi.services.secrets import decrypt_dict, encrypt_dict

logger = logging.getLogger(__name__)

STATE_TTL_MINUTES = 10


def _pkce_pair() -> Tuple[str, str]:
    verifier = _secrets.token_urlsafe(96)[:128]
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).rstrip(b"=").decode()
    return verifier, challenge


def start_authorization(
    *,
    connector_key: str,
    redirect_uri: str,
    organisation=None,
    data_source=None,
    return_to: str = "",
) -> Dict[str, Any]:
    """Begin a connect flow. Returns `{authorization_url, state}`.

    Raises `ApiError` if the connector has no configured provider, so the caller
    can tell the user "this deployment isn't set up for X" rather than sending
    them to a broken consent screen.
    """

    from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
    from terno_dbi.connectors.api.auth.providers import get_provider
    from terno_dbi.core.models import ConnectorOAuthState

    provider = get_provider(connector_key)
    if provider is None:
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"{connector_key} does not support OAuth connection.",
        )
    if not provider.is_configured():
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"This deployment has no OAuth credentials for {connector_key}. "
            f"Set {provider.client_id_env} and {provider.client_secret_env}.",
        )

    verifier, challenge = _pkce_pair() if provider.use_pkce else ("", "")
    state = _secrets.token_urlsafe(32)

    ConnectorOAuthState.objects.create(
        state=state,
        connector_key=connector_key,
        code_verifier=verifier,
        redirect_uri=redirect_uri,
        organisation=organisation,
        data_source=data_source,
        return_to=return_to,
        expires_at=timezone.now() + timedelta(minutes=STATE_TTL_MINUTES),
    )

    params = {
        "response_type": "code",
        "client_id": provider.client_id(),
        "redirect_uri": redirect_uri,
        "state": state,
    }
    if provider.scope:
        params["scope"] = provider.scope
    if provider.use_pkce:
        params["code_challenge"] = challenge
        params["code_challenge_method"] = "S256"
    params.update(provider.extra_authorize_params)

    return {
        "authorization_url": f"{provider.authorization_url}?{urlencode(params)}",
        "state": state,
    }


def _default_post(url: str, data: Dict[str, str]) -> Dict[str, Any]:
    resp = requests.post(url, data=data, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _store_tokens(data_source, token_response: Dict[str, Any]) -> None:
    """Encrypt and persist the token bundle onto the datasource.

    Preserves an existing refresh token when the provider omits one on refresh
    (Google returns it only on the first consent).
    """
    from terno_dbi.core.models import DataSource
    existing = decrypt_dict(data_source.connection_json) or {}
    bundle: Dict[str, Any] = dict(existing)

    if token_response.get("access_token"):
        bundle["ACCESS_TOKEN"] = token_response["access_token"]
    if token_response.get("refresh_token"):
        bundle["REFRESH_TOKEN"] = token_response["refresh_token"]
    expires_in = token_response.get("expires_in")
    if expires_in:
        bundle["TOKEN_EXPIRES_AT"] = str(time.time() + float(expires_in))

    data_source.connection_json = encrypt_dict(bundle)
    data_source.auth_status = DataSource.AuthStatus.CONNECTED
    data_source.auth_error = ""
    data_source.save(update_fields=[
        "connection_json", "auth_status", "auth_error",
    ])


def complete_authorization(
    *,
    state: str,
    code: str,
    http_post: Optional[Callable[[str, Dict[str, str]], Dict[str, Any]]] = None,
):
    """Finish a flow: exchange the code, store encrypted tokens, return the DS.

    Creates the `DataSource` on a first connect, or updates the one referenced by
    the state on a reconnect.
    """
    # Resolved at call time (not as a default arg) so tests can monkeypatch it.
    http_post = http_post or _default_post
    from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
    from terno_dbi.connectors.api.auth.providers import get_provider
    from terno_dbi.core.models import ConnectorCatalog, ConnectorOAuthState, DataSource

    st = ConnectorOAuthState.objects.filter(state=state).first()
    if st is None or st.is_expired:
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            "This connect link has expired or was already used. Start again.",
        )

    provider = get_provider(st.connector_key)
    exchange = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": st.redirect_uri,
        "client_id": provider.client_id(),
        "client_secret": provider.client_secret(),
    }
    if provider.use_pkce and st.code_verifier:
        exchange["code_verifier"] = st.code_verifier

    try:
        token_response = http_post(provider.token_url, exchange)
    except Exception as exc:   # noqa: BLE001
        logger.warning("Token exchange failed for %s: %s", st.connector_key, exc)
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            "Could not complete the connection with the provider. Try again.",
        )

    token_response = _post_process(st.connector_key, provider, token_response)

    data_source = st.data_source
    if data_source is None:
        catalog = ConnectorCatalog.objects.filter(key=st.connector_key).first()
        # Reuse an existing datasource for this connector so reconnecting does not
        # create duplicates. Create a new datasource only when none exists.
        data_source = (
            DataSource.objects.filter(
                organisation=st.organisation, catalog=catalog,
            ).order_by("id").first()
            if catalog else None
        )
        if data_source is None:
            data_source = DataSource.objects.create(
                display_name=_unique_name(catalog, st.organisation),
                type=st.connector_key,
                connection_str="",
                organisation=st.organisation,
                catalog=catalog,
                auth_status=DataSource.AuthStatus.NOT_AUTHENTICATED,
            )

    _store_tokens(data_source, token_response)
    st.delete()
    return data_source


def _post_process(connector_key, provider, token_response):
    """Provider-specific fix-ups. Meta exchanges for a long-lived token."""
    if connector_key != "meta_ads":
        return token_response
    access = token_response.get("access_token")
    if not access:
        return token_response
    try:
        resp = requests.get(
            provider.token_url,
            params={
                "grant_type": "fb_exchange_token",
                "client_id": provider.client_id(),
                "client_secret": provider.client_secret(),
                "fb_exchange_token": access,
            },
            timeout=15,
        )
        resp.raise_for_status()
        return {**token_response, **resp.json()}
    except Exception as exc:
        logger.warning("Meta long-lived exchange failed: %s", exc)
        return token_response


def _unique_name(catalog, organisation) -> str:
    from terno_dbi.core.models import DataSource

    base = catalog.name if catalog else "Connected source"
    name = base
    i = 2
    while DataSource.objects.filter(display_name=name).exists():
        name = f"{base} {i}"
        i += 1
    return name


def refresh_access_token(
    data_source,
    http_post: Optional[Callable[[str, Dict[str, str]], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Use the stored refresh token to get a new access token, and persist it.

    Plugs into `tokens.ensure_fresh_token` as its `refresh_fn`. Marks the
    datasource `expired` if there is no refresh token or the exchange fails, so
    the agent is handed a reconnect link instead of a raw 401.
    """
    from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
    from terno_dbi.connectors.api.auth.providers import get_provider
    from terno_dbi.core.models import DataSource

    http_post = http_post or _default_post
    tokens = decrypt_dict(data_source.connection_json) or {}
    refresh_token = tokens.get("REFRESH_TOKEN")
    provider = get_provider(data_source.type)

    if not refresh_token or provider is None:
        _mark_expired(data_source, "No refresh token; reconnect the source.")
        raise ApiError(ErrorCode.AUTH_EXPIRED,
                       f"{data_source.type} needs reconnecting.")

    try:
        token_response = http_post(provider.token_url, {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": provider.client_id(),
            "client_secret": provider.client_secret(),
        })
    except Exception as exc:   # noqa: BLE001
        _mark_expired(data_source, "Token refresh failed; reconnect the source.")
        raise ApiError(ErrorCode.AUTH_EXPIRED,
                       f"{data_source.type} needs reconnecting.") from exc

    _store_tokens(data_source, token_response)
    from terno_dbi.services.secrets import decrypt_dict as _d
    return _d(data_source.connection_json)


def _mark_expired(data_source, message: str) -> None:
    from terno_dbi.core.models import DataSource
    data_source.auth_status = DataSource.AuthStatus.EXPIRED
    data_source.auth_error = message
    data_source.save(update_fields=["auth_status", "auth_error"])


def make_ensure_token(data_source) -> Callable[[], None]:
    """The `token_refresher` a connector calls from `access_token()`.

    A zero-arg callable that refreshes the datasource's access token once, under
    a lock, only when it is due — see `tokens.ensure_fresh_token`. Bound to a
    connector by its factory (e.g. `make_ga4_connector`), so the model layer
    stays free of OAuth specifics while every provider call still gets a fresh
    token.
    """
    from terno_dbi.connectors.api.auth.tokens import ensure_fresh_token

    def ensure() -> None:
        ensure_fresh_token(
            data_source,
            refresh_fn=lambda: refresh_access_token(data_source),
            read_tokens=lambda: decrypt_dict(data_source.connection_json) or {},
        )

    return ensure


__all__ = [
    "complete_authorization",
    "make_ensure_token",
    "refresh_access_token",
    "start_authorization",
]
