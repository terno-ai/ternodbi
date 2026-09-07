"""At-rest encryption for credential fields — encrypt on write, *not* auto-decrypt.

``DataSource.connection_str`` and ``connection_json`` hold database passwords and
service-account private keys. These field subclasses encrypt on the way to the
database, but deliberately do **not** decrypt on the way out: reading the
attribute returns the stored ciphertext (a legacy plaintext row returns its
plaintext). This is secure-by-default — the admin, serializers, logs and any
casual reader see ciphertext, so a secret cannot leak through a surface that
forgot to mask it.

The plaintext is obtained only where a connection is actually opened, via the
explicit accessors ``DataSource.decrypted_connection_str`` /
``decrypted_connection_json`` (thin ``secrets.decrypt_*`` wrappers). A consumer
that forgets to decrypt fails to connect — a loud, safe failure — rather than
silently exposing the credential.

Writes are idempotent: a value already in envelope form is not re-encrypted, so
loading a row (ciphertext) and saving it back keeps it wrapped exactly once.

Encryption is non-deterministic, so these fields must not be used in a
``.filter()`` on their value — nothing queries a credential by its content.
"""

from django.db import models


class EncryptedTextField(models.TextField):
    """A ``TextField`` encrypted on write; reads return the stored ciphertext."""

    def get_prep_value(self, value):
        from terno_dbi.services import secrets
        value = super().get_prep_value(value)
        return secrets.encrypt_str(value)


class EncryptedJSONField(models.JSONField):
    """A ``JSONField`` stored as an encrypted envelope; reads return the envelope."""

    def get_prep_value(self, value):
        from terno_dbi.services import secrets
        if value is not None and not secrets.is_encrypted(value):
            value = secrets.encrypt_dict(value)
        return super().get_prep_value(value)
