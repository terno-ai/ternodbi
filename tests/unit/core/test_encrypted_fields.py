"""DataSource credentials: ciphertext at rest AND on read; plaintext only via
the explicit accessors used at connect time (Option B, secure-by-default)."""

import pytest
from cryptography.fernet import Fernet
from django.db import connection

from terno_dbi.core.models import DataSource
from terno_dbi.services import secrets


@pytest.fixture(autouse=True)
def key(settings):
    settings.MCP_ENCRYPTION_KEY = Fernet.generate_key().decode()
    settings.MCP_ENCRYPTION_KEYS = None
    secrets.reset_cache()
    yield
    secrets.reset_cache()


def _raw(column, pk):
    with connection.cursor() as cur:
        cur.execute(
            f"SELECT {column} FROM {DataSource._meta.db_table} WHERE id = %s", [pk]
        )
        return cur.fetchone()[0]


@pytest.mark.django_db
class TestEncryptedAtRest:
    def test_connection_str_reads_back_as_ciphertext(self):
        ds = DataSource.objects.create(
            display_name="pg", type="postgres",
            connection_str="postgresql://u:supersecret@h:5432/db",
        )
        ds.refresh_from_db()
        # The field does NOT auto-decrypt: the attribute is ciphertext, so no
        # casual reader (admin, serializer, log) sees the secret.
        assert ds.connection_str.startswith("enc:1:")
        assert "supersecret" not in ds.connection_str
        # The raw column matches (same ciphertext).
        assert _raw("connection_str", ds.id) == ds.connection_str
        # Plaintext is available only via the explicit accessor.
        assert ds.decrypted_connection_str == "postgresql://u:supersecret@h:5432/db"

    def test_connection_json_reads_back_as_envelope(self):
        ds = DataSource.objects.create(
            display_name="bq", type="bigquery", connection_str="bigquery://p/d",
            connection_json={"private_key": "-----BEGIN PRIVATE KEY-----ABC"},
        )
        ds.refresh_from_db()
        assert secrets.is_encrypted(ds.connection_json)          # envelope, not the key
        assert "BEGIN PRIVATE KEY" not in str(ds.connection_json)
        assert ds.decrypted_connection_json == {"private_key": "-----BEGIN PRIVATE KEY-----ABC"}

    def test_null_connection_json_stays_null(self):
        ds = DataSource.objects.create(
            display_name="nulljson", type="postgres",
            connection_str="postgresql://u:p@h/db", connection_json=None,
        )
        assert _raw("connection_json", ds.id) is None
        ds.refresh_from_db()
        assert ds.connection_json is None
        assert ds.decrypted_connection_json is None

    def test_legacy_plaintext_row_reads_and_decrypts(self):
        # A row written before encryption is raw plaintext. The attribute returns
        # it as-is; the accessor passes it through unchanged.
        ds = DataSource.objects.create(
            display_name="legacy", type="postgres", connection_str="seed",
        )
        with connection.cursor() as cur:
            cur.execute(
                f"UPDATE {DataSource._meta.db_table} "
                f"SET connection_str = %s WHERE id = %s",
                ["postgresql://plain:text@h/db", ds.id],
            )
        ds.refresh_from_db()
        assert ds.connection_str == "postgresql://plain:text@h/db"          # raw
        assert ds.decrypted_connection_str == "postgresql://plain:text@h/db"  # passthrough

    def test_resave_encrypts_a_legacy_row(self):
        ds = DataSource.objects.create(
            display_name="reencrypt", type="postgres", connection_str="seed",
        )
        with connection.cursor() as cur:
            cur.execute(
                f"UPDATE {DataSource._meta.db_table} "
                f"SET connection_str = %s WHERE id = %s",
                ["postgresql://legacy@h/db", ds.id],
            )
        ds.refresh_from_db()
        # The backfill pattern: write the raw value back; get_prep_value encrypts.
        DataSource.objects.filter(pk=ds.pk).update(connection_str=ds.connection_str)
        assert _raw("connection_str", ds.id).startswith("enc:1:")
        ds.refresh_from_db()
        assert ds.decrypted_connection_str == "postgresql://legacy@h/db"

    def test_saving_an_encrypted_value_is_idempotent(self):
        ds = DataSource.objects.create(
            display_name="idem", type="postgres",
            connection_str="postgresql://u:pw@h/db",
        )
        ds.refresh_from_db()
        ciphertext = ds.connection_str
        # Re-saving the (already-encrypted) attribute must not double-wrap.
        DataSource.objects.filter(pk=ds.pk).update(connection_str=ds.connection_str)
        ds.refresh_from_db()
        assert ds.decrypted_connection_str == "postgresql://u:pw@h/db"
        assert ds.connection_str.startswith("enc:1:")
