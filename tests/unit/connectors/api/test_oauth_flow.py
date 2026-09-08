"""The OAuth connect flow (§5.3): start, complete, refresh — HTTP injected."""

import time

import pytest
from django.contrib.auth.models import User

from terno_dbi.catalog.refresh import refresh_catalog
from terno_dbi.connectors.api.auth import oauth
from terno_dbi.connectors.api.model.errors import ApiError
from terno_dbi.core.models import (
    ConnectorCatalog,
    ConnectorOAuthState,
    CoreOrganisation,
    DataSource,
)
from terno_dbi.services import secrets


@pytest.fixture(autouse=True)
def google_creds(monkeypatch):
    monkeypatch.setenv("TERNO_GOOGLE_OAUTH_CLIENT_ID", "client-abc")
    monkeypatch.setenv("TERNO_GOOGLE_OAUTH_CLIENT_SECRET", "secret-xyz")
    secrets.reset_cache()
    yield
    secrets.reset_cache()


@pytest.fixture
def org(db):
    user = User.objects.create_user("oauthuser", "o@x.com", "pw")
    return CoreOrganisation.objects.create(name="Acme", subdomain="acme", owner=user)


@pytest.fixture
def catalog(db):
    refresh_catalog()


@pytest.mark.django_db
class TestStart:
    def test_builds_a_consent_url_and_records_state(self, org, catalog):
        out = oauth.start_authorization(
            connector_key="googleanalytics4",
            redirect_uri="https://acme.app.terno.ai/callback",
            organisation=org,
        )
        assert "accounts.google.com" in out["authorization_url"]
        assert "code_challenge=" in out["authorization_url"]      # PKCE
        assert "analytics.readonly" in out["authorization_url"]
        assert ConnectorOAuthState.objects.filter(state=out["state"]).exists()

    def test_unconfigured_provider_is_refused(self, org, catalog, monkeypatch):
        monkeypatch.delenv("TERNO_GOOGLE_OAUTH_CLIENT_ID", raising=False)
        with pytest.raises(ApiError) as exc:
            oauth.start_authorization(
                connector_key="googleanalytics4",
                redirect_uri="https://x/callback", organisation=org,
            )
        assert "credentials" in exc.value.message.lower()

    def test_unknown_connector_is_refused(self, org, catalog):
        with pytest.raises(ApiError):
            oauth.start_authorization(
                connector_key="not_a_connector",
                redirect_uri="https://x/callback", organisation=org,
            )


@pytest.mark.django_db
class TestComplete:
    def _start(self, org):
        return oauth.start_authorization(
            connector_key="googleanalytics4",
            redirect_uri="https://acme.app.terno.ai/callback",
            organisation=org,
        )

    def test_exchange_stores_encrypted_tokens_and_creates_datasource(self, org, catalog):
        started = self._start(org)

        def fake_post(url, data):
            assert data["grant_type"] == "authorization_code"
            assert data["code"] == "the-code"
            return {
                "access_token": "at-1", "refresh_token": "rt-1",
                "expires_in": 3600,
            }

        ds = oauth.complete_authorization(
            state=started["state"], code="the-code", http_post=fake_post,
        )
        assert ds.auth_status == DataSource.AuthStatus.CONNECTED
        assert ds.organisation_id == org.id

        # Tokens are encrypted at rest, never plaintext.
        assert secrets.is_encrypted(ds.connection_json)
        bundle = secrets.decrypt_dict(ds.connection_json)
        assert bundle["ACCESS_TOKEN"] == "at-1"
        assert bundle["REFRESH_TOKEN"] == "rt-1"
        assert float(bundle["TOKEN_EXPIRES_AT"]) > time.time()

        # State is single-use.
        assert not ConnectorOAuthState.objects.filter(state=started["state"]).exists()

    def test_expired_state_is_refused(self, org, catalog):
        started = self._start(org)
        st = ConnectorOAuthState.objects.get(state=started["state"])
        from django.utils import timezone
        from datetime import timedelta
        st.expires_at = timezone.now() - timedelta(minutes=1)
        st.save()
        with pytest.raises(ApiError):
            oauth.complete_authorization(
                state=started["state"], code="x", http_post=lambda u, d: {},
            )

    def test_provider_failure_surfaces_cleanly(self, org, catalog):
        started = self._start(org)

        def boom(url, data):
            raise RuntimeError("network down")

        with pytest.raises(ApiError) as exc:
            oauth.complete_authorization(
                state=started["state"], code="x", http_post=boom,
            )
        assert "provider" in exc.value.message.lower()


@pytest.mark.django_db
class TestRefresh:
    def _connected_ds(self, org):
        refresh_catalog()
        ds = DataSource.objects.create(
            display_name="GA4", type="googleanalytics4", connection_str="",
            organisation=org,
            catalog=ConnectorCatalog.objects.get(key="googleanalytics4"),
        )
        ds.connection_json = secrets.encrypt_dict({
            "ACCESS_TOKEN": "old", "REFRESH_TOKEN": "rt-keep",
            "TOKEN_EXPIRES_AT": str(time.time() - 1),
        })
        ds.save()
        return ds

    def test_refresh_updates_access_token_and_keeps_refresh_token(self, org):
        ds = self._connected_ds(org)

        def fake_post(url, data):
            assert data["grant_type"] == "refresh_token"
            assert data["refresh_token"] == "rt-keep"
            return {"access_token": "new-at", "expires_in": 3600}  # no new RT

        bundle = oauth.refresh_access_token(ds, http_post=fake_post)
        assert bundle["ACCESS_TOKEN"] == "new-at"
        assert bundle["REFRESH_TOKEN"] == "rt-keep"     # preserved

    def test_missing_refresh_token_marks_expired(self, org):
        refresh_catalog()
        ds = DataSource.objects.create(
            display_name="GA4b", type="googleanalytics4", connection_str="",
            organisation=org,
            catalog=ConnectorCatalog.objects.get(key="googleanalytics4"),
        )
        ds.connection_json = secrets.encrypt_dict({"ACCESS_TOKEN": "only"})
        ds.save()

        with pytest.raises(ApiError):
            oauth.refresh_access_token(ds, http_post=lambda u, d: {})
        ds.refresh_from_db()
        assert ds.auth_status == DataSource.AuthStatus.EXPIRED
