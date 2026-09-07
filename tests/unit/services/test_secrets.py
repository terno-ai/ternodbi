"""Encryption-at-rest utility: envelopes, passthrough, idempotence, fail-closed."""

import pytest
from cryptography.fernet import Fernet

from terno_dbi.services import secrets


@pytest.fixture(autouse=True)
def key(settings):
    settings.MCP_ENCRYPTION_KEY = Fernet.generate_key().decode()
    settings.MCP_ENCRYPTION_KEYS = None
    secrets.reset_cache()
    yield
    secrets.reset_cache()


class TestDictEnvelope:
    def test_round_trip(self):
        env = secrets.encrypt_dict({"private_key": "KEY", "n": 1})
        assert secrets.is_encrypted(env)
        assert "KEY" not in env["ct"]              # the secret is not in the clear
        assert secrets.decrypt_dict(env) == {"private_key": "KEY", "n": 1}

    def test_legacy_plaintext_is_passed_through(self):
        # A row written before encryption is a bare dict — read it unchanged.
        assert secrets.decrypt_dict({"a": 1}) == {"a": 1}

    def test_none_passes_through(self):
        assert secrets.encrypt_dict(None) is None
        assert secrets.decrypt_dict(None) is None

    def test_encrypt_is_idempotent(self):
        env = secrets.encrypt_dict({"a": 1})
        assert secrets.encrypt_dict(env) == env      # already-encrypted: no double wrap


class TestStringEnvelope:
    def test_round_trip(self):
        ct = secrets.encrypt_str("postgresql://u:pw@h/db")
        assert secrets.is_encrypted_str(ct)
        assert "pw" not in ct
        assert secrets.decrypt_str(ct) == "postgresql://u:pw@h/db"

    def test_legacy_plaintext_is_passed_through(self):
        assert secrets.decrypt_str("postgresql://plain") == "postgresql://plain"

    def test_empty_and_none_pass_through(self):
        assert secrets.encrypt_str("") == ""
        assert secrets.encrypt_str(None) is None
        assert secrets.decrypt_str("") == ""

    def test_encrypt_is_idempotent(self):
        ct = secrets.encrypt_str("secret")
        assert secrets.encrypt_str(ct) == ct


class TestKeyRing:
    def test_any_ring_key_decrypts_after_rotation(self, settings):
        old = Fernet.generate_key().decode()
        settings.MCP_ENCRYPTION_KEY = old
        settings.MCP_ENCRYPTION_KEYS = None
        secrets.reset_cache()
        ct = secrets.encrypt_str("v")

        # Prepend a new key: the new key encrypts, the old key still decrypts.
        new = Fernet.generate_key().decode()
        settings.MCP_ENCRYPTION_KEYS = f"{new},{old}"
        secrets.reset_cache()
        assert secrets.decrypt_str(ct) == "v"

    def test_missing_key_fails_closed(self, settings):
        settings.MCP_ENCRYPTION_KEY = None
        settings.MCP_ENCRYPTION_KEYS = None
        secrets.reset_cache()
        with pytest.raises(secrets.SecretsUnavailable):
            secrets.encrypt_str("x")
