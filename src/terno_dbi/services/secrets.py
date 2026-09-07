"""Encrypt sensitive credentials before storing them in the database.

Database passwords, service-account private keys, and OAuth tokens should not
be stored as plaintext. This module uses Fernet encryption with support for
multiple encryption keys, versioned encrypted values, and idempotent
encryption.

MCP_ENCRYPTION_KEYS can contain multiple comma-separated keys. The first key is
used for encryption, while any configured key can be used for decryption. This
allows keys to be rotated without immediately re-encrypting every stored value.

Encrypted values include a version marker so they can be distinguished from
legacy plaintext values during migration.

Encrypting an already-encrypted value is a no-op, which prevents values from
being encrypted more than once.

The older MCP_ENCRYPTION_KEY setting is also supported for backwards
compatibility.
"""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any, Dict, Optional

from cryptography.fernet import Fernet, MultiFernet
from django.conf import settings

ENVELOPE_VERSION = 1
_ENVELOPE_MARKER = "__enc__"
_STR_PREFIX = "enc:1:"


class SecretsUnavailable(RuntimeError):
    """No encryption key is configured — refuse to store secrets in plaintext."""


@lru_cache(maxsize=1)
def _fernet() -> MultiFernet:
    keys = []
    multi = getattr(settings, "MCP_ENCRYPTION_KEYS", None)
    if multi:
        if isinstance(multi, str):
            keys = [k.strip() for k in multi.split(",") if k.strip()]
        else:
            keys = list(multi)
    single = getattr(settings, "MCP_ENCRYPTION_KEY", None)
    if single and single not in keys:
        keys.append(single)
    if not keys:
        raise SecretsUnavailable(
            "Set MCP_ENCRYPTION_KEY (or MCP_ENCRYPTION_KEYS) before storing "
            "credentials."
        )
    return MultiFernet([Fernet(_as_bytes(k)) for k in keys])


def _as_bytes(key) -> bytes:
    return key.encode() if isinstance(key, str) else key


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def is_encrypted(value: Any) -> bool:
    """True if ``value`` is an encrypted dict envelope (not legacy plaintext)."""
    return isinstance(value, dict) and value.get(_ENVELOPE_MARKER) == ENVELOPE_VERSION


def is_encrypted_str(value: Any) -> bool:
    """True if ``value`` is an encrypted string envelope."""
    return isinstance(value, str) and value.startswith(_STR_PREFIX)


# ---------------------------------------------------------------------------
# Dict values (e.g. connection_json, OAuth token bundles)
# ---------------------------------------------------------------------------

def encrypt_dict(payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Encrypt a JSON-serialisable dict into a storage envelope. Idempotent."""
    if payload is None:
        return None
    if is_encrypted(payload):
        return payload
    blob = json.dumps(payload, separators=(",", ":")).encode()
    token = _fernet().encrypt(blob).decode()
    return {_ENVELOPE_MARKER: ENVELOPE_VERSION, "ct": token}


def decrypt_dict(value: Optional[Any]) -> Optional[Any]:
    """Return the plaintext dict from a value that may be encrypted or not.

    A legacy plaintext dict is returned as-is, so a reader can be deployed
    *before* the data is encrypted (and keeps working if a migration is rolled
    back). ``None`` passes through.
    """
    if value is None:
        return None
    if not is_encrypted(value):
        return value
    token = value["ct"].encode()
    return json.loads(_fernet().decrypt(token).decode())


# ---------------------------------------------------------------------------
# Text values (e.g. connection_str)
# ---------------------------------------------------------------------------

def encrypt_str(value: Optional[str]) -> Optional[str]:
    """Encrypt a string into a prefixed envelope. Idempotent; ``None``/"" pass through."""
    if value is None or value == "":
        return value
    if is_encrypted_str(value):
        return value
    token = _fernet().encrypt(value.encode()).decode()
    return _STR_PREFIX + token


def decrypt_str(value: Optional[str]) -> Optional[str]:
    """Return plaintext from a value that may be an encrypted string or legacy plaintext."""
    if value is None or value == "":
        return value
    if not is_encrypted_str(value):
        return value
    token = value[len(_STR_PREFIX):].encode()
    return _fernet().decrypt(token).decode()


# ---------------------------------------------------------------------------
# Rotation / maintenance
# ---------------------------------------------------------------------------

def rotate(value: Any) -> Any:
    """Re-wrap an encrypted value under the primary key (after a key rotation)."""
    if is_encrypted(value):
        return {**value, "ct": _fernet().rotate(value["ct"].encode()).decode()}
    if is_encrypted_str(value):
        token = value[len(_STR_PREFIX):].encode()
        return _STR_PREFIX + _fernet().rotate(token).decode()
    return value


def reset_cache() -> None:
    """Drop the cached key ring — call after changing keys in a test/runtime."""
    _fernet.cache_clear()


__all__ = [
    "SecretsUnavailable",
    "is_encrypted",
    "is_encrypted_str",
    "encrypt_dict",
    "decrypt_dict",
    "encrypt_str",
    "decrypt_str",
    "rotate",
    "reset_cache",
]
