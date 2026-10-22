"""OAuth 1.0a for an API connector whose provider has no OAuth 2.0 (X Ads).

The X Ads API accepts only OAuth 1.0a user-context requests: every call is
signed with HMAC-SHA1 under the app's consumer secret and the user's token
secret. Connecting is three-legged:

1. `fetch_request_token` — POST the provider's request-token URL with our
   callback; it answers with a short-lived request token and its secret.
2. The user opens the authorize URL with `oauth_token=<request token>` and
   approves. The provider redirects to the callback with `oauth_token` (the
   request token, which serves as the flow's `state`) and `oauth_verifier`
   (its `code`) — or with `denied` if the user refused.
3. `fetch_access_token` — POST the access-token URL, signed with the request
   token; it answers with the user's access token and secret. X's do not
   expire, so there is nothing to refresh.

Signing follows RFC 5849 §3.4.1. The HTTP call is injected (`http_post`) so the
flow is testable without a live provider; it receives the full URL and the
headers and returns the form-encoded response body decoded to a dict.
"""

from __future__ import annotations
import base64
import hashlib
import hmac
import secrets as _secrets
import time
from typing import Any, Callable, Dict, Mapping, Optional, Tuple
from urllib.parse import parse_qsl, quote, urlsplit, urlunsplit


def _enc(value: Any) -> str:
    """RFC 3986 percent-encoding: everything except the unreserved characters."""
    return quote(str(value), safe="")


def encode_query(params: Mapping[str, Any]) -> str:
    """A query string encoded exactly as it is signed.

    `requests` would encode a space as `+`, which the signature base string
    does not; building the query here keeps what is sent and what is signed the
    same bytes.
    """
    return "&".join(f"{_enc(k)}={_enc(v)}" for k, v in params.items())


def signature(method: str, url: str, params: Mapping[str, Any],
              consumer_secret: str, token_secret: str = "") -> str:
    """The HMAC-SHA1 signature of a request (RFC 5849 §3.4).

    `params` are the protocol (`oauth_*`) and body parameters; any query on
    `url` is signed too.
    """
    parts = urlsplit(url)
    base_url = urlunsplit(
        (parts.scheme.lower(), parts.netloc.lower(), parts.path, "", ""))
    pairs = [*parse_qsl(parts.query, keep_blank_values=True), *params.items()]
    normalised = "&".join(
        f"{k}={v}" for k, v in sorted((_enc(k), _enc(v)) for k, v in pairs))
    base = "&".join((method.upper(), _enc(base_url), _enc(normalised)))
    key = f"{_enc(consumer_secret)}&{_enc(token_secret)}"
    digest = hmac.new(key.encode(), base.encode(), hashlib.sha1).digest()
    return base64.b64encode(digest).decode()


def authorization_header(
    method: str,
    url: str,
    *,
    consumer_key: str,
    consumer_secret: str,
    token: str = "",
    token_secret: str = "",
    oauth_params: Optional[Mapping[str, str]] = None,
    nonce: Optional[str] = None,
    timestamp: Optional[int] = None,
) -> str:
    """The `Authorization: OAuth ...` header value for one request.

    `url` carries the request's whole query string. `oauth_params` adds
    protocol parameters a step needs (`oauth_callback`, `oauth_verifier`).
    `nonce` and `timestamp` are fixed only by tests.
    """
    oauth = {
        "oauth_consumer_key": consumer_key,
        "oauth_nonce": nonce or _secrets.token_hex(16),
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": str(int(time.time()) if timestamp is None else timestamp),
        "oauth_version": "1.0",
        **(oauth_params or {}),
    }
    if token:
        oauth["oauth_token"] = token
    oauth["oauth_signature"] = signature(
        method, url, oauth, consumer_secret, token_secret)
    return "OAuth " + ", ".join(
        f'{_enc(k)}="{_enc(v)}"' for k, v in sorted(oauth.items()))


def _default_post(url: str, headers: Dict[str, str]) -> Dict[str, str]:
    import requests
    resp = requests.post(url, headers=headers, timeout=15)
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:
        detail = (resp.text or "").strip()[:500]
        raise requests.HTTPError(
            f"{exc} — response: {detail}", response=resp) from exc
    return dict(parse_qsl(resp.text or ""))


def fetch_request_token(
    provider, callback: str,
    http_post: Optional[Callable[[str, Dict[str, str]], Dict[str, str]]] = None,
) -> Tuple[str, str]:
    """Step 1: a request token and its secret, bound to `callback`.

    `x_auth_access_type=write` asks for read and write access, so write
    features need no reconnect. It never reaches Direct Messages, even when
    the app is allowed them, and it cannot exceed the app's own permission
    setting.
    """
    url = f"{provider.request_token_url}?{encode_query({'x_auth_access_type': 'write'})}"
    header = authorization_header(
        "POST", url,
        consumer_key=provider.client_id(),
        consumer_secret=provider.client_secret(),
        oauth_params={"oauth_callback": callback},
    )
    out = (http_post or _default_post)(url, {"Authorization": header})
    # A provider that did not accept the callback would send the user to a
    # PIN screen rather than back to us.
    if out.get("oauth_callback_confirmed") != "true" or not (
            out.get("oauth_token") and out.get("oauth_token_secret")):
        raise ValueError("request token refused: the callback was not confirmed")
    return out["oauth_token"], out["oauth_token_secret"]


def fetch_access_token(
    provider, request_token: str, request_secret: str, verifier: str,
    http_post: Optional[Callable[[str, Dict[str, str]], Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Step 3: swap the approved request token for the user's access token.

    Returned in the shape `_store_tokens` reads: the token, its secret, and the
    connected account's `@handle` as the connection's identity.
    """
    url = provider.token_url
    header = authorization_header(
        "POST", url,
        consumer_key=provider.client_id(),
        consumer_secret=provider.client_secret(),
        token=request_token,
        token_secret=request_secret,
        oauth_params={"oauth_verifier": verifier},
    )
    out = (http_post or _default_post)(url, {"Authorization": header})
    if not (out.get("oauth_token") and out.get("oauth_token_secret")):
        raise ValueError("access token refused: no token in the response")
    response: Dict[str, Any] = {
        "access_token": out["oauth_token"],
        "access_token_secret": out["oauth_token_secret"],
    }
    if out.get("screen_name"):
        response["connected_email"] = f"@{out['screen_name']}"
    return response


__all__ = [
    "authorization_header",
    "encode_query",
    "fetch_access_token",
    "fetch_request_token",
    "signature",
]
