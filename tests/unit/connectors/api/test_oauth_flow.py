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

    def test_consent_is_scoped_to_just_this_connector(self, org, catalog):
        # Connecting GA4 must ask only for the GA4 scope — not Ads/YouTube/GSC.
        # include_granted_scopes would make Google merge every previously-granted
        # scope into this consent screen, so it must not be sent.
        from urllib.parse import urlparse, parse_qs

        out = oauth.start_authorization(
            connector_key="googleanalytics4",
            redirect_uri="https://acme.app.terno.ai/callback",
            organisation=org,
        )
        qs = parse_qs(urlparse(out["authorization_url"]).query)
        # `openid email` (non-sensitive) are added to record the connecting
        # account; the connector's own scope stays GA4-only.
        assert qs["scope"] == [
            "openid email https://www.googleapis.com/auth/analytics.readonly"]
        assert "include_granted_scopes" not in qs
        for foreign in ("adwords", "webmasters", "youtube"):
            assert foreign not in out["authorization_url"]

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


def _id_token(email):
    import base64 as _b64, json as _json
    body = _b64.urlsafe_b64encode(_json.dumps({"email": email}).encode()).rstrip(b"=").decode()
    return f"h.{body}.s"


class TestStoreTokensExtras:
    def test_store_tokens_persists_connected_email(self, org, catalog):
        ds = DataSource.objects.create(
            display_name="GA4e", type="googleanalytics4", connection_str="",
            organisation=org,
            catalog=ConnectorCatalog.objects.get(key="googleanalytics4"),
        )
        oauth._store_tokens(ds, {
            "access_token": "at", "refresh_token": "rt",
            "id_token": _id_token("who@x.com"), "expires_in": 3600,
        })
        bundle = secrets.decrypt_dict(ds.connection_json)
        assert bundle["CONNECTED_EMAIL"] == "who@x.com"
        assert ds.auth_status == DataSource.AuthStatus.CONNECTED

    def test_refresh_without_id_token_keeps_existing_email(self, org, catalog):
        ds = DataSource.objects.create(
            display_name="GA4f", type="googleanalytics4", connection_str="",
            organisation=org,
            catalog=ConnectorCatalog.objects.get(key="googleanalytics4"),
        )
        ds.connection_json = secrets.encrypt_dict({"CONNECTED_EMAIL": "keep@x.com"})
        ds.save()
        oauth._store_tokens(ds, {"access_token": "at2"})   # refresh: no id_token
        assert secrets.decrypt_dict(ds.connection_json)["CONNECTED_EMAIL"] == "keep@x.com"


class TestUniqueName:
    def test_appends_a_suffix_on_collision(self, org, catalog):
        cat = ConnectorCatalog.objects.get(key="googleanalytics4")
        DataSource.objects.create(
            display_name=cat.name, type="googleanalytics4", connection_str="",
            organisation=org, catalog=cat)
        assert oauth._unique_name(cat, org) == f"{cat.name} 2"


class TestPostProcess:
    def test_non_meta_is_a_passthrough(self):
        tr = {"access_token": "a"}
        assert oauth._post_process("google_ads", None, tr) is tr

    def test_meta_exchanges_for_a_long_lived_token(self, monkeypatch):
        from terno_dbi.connectors.api.auth.providers import get_provider

        class _Resp:
            def raise_for_status(self): pass
            def json(self): return {"access_token": "long-lived"}

        monkeypatch.setattr(oauth.requests, "get", lambda *a, **k: _Resp())
        out = oauth._post_process("meta_ads", get_provider("meta_ads"),
                                  {"access_token": "short"})
        assert out["access_token"] == "long-lived"

    def test_meta_exchange_failure_falls_back(self, monkeypatch):
        from terno_dbi.connectors.api.auth.providers import get_provider

        def _boom(*a, **k):
            raise RuntimeError("network down")

        monkeypatch.setattr(oauth.requests, "get", _boom)
        out = oauth._post_process("meta_ads", get_provider("meta_ads"),
                                  {"access_token": "short"})
        assert out["access_token"] == "short"     # original kept


@pytest.mark.django_db
class TestZohoRegionalServer:
    """Zoho names the user's data centre only at the callback (`accounts-server`);
    the token exchange and every refresh must go there — and nowhere else, since
    that POST carries our client secret."""

    @pytest.fixture(autouse=True)
    def zoho_creds(self, monkeypatch):
        monkeypatch.setenv("TERNO_ZOHO_CLIENT_ID", "zoho-id")
        monkeypatch.setenv("TERNO_ZOHO_CLIENT_SECRET", "zoho-secret")

    def _start(self, org):
        return oauth.start_authorization(
            connector_key="zoho_crm",
            redirect_uri="https://acme.app.terno.ai/callback",
            organisation=org,
        )

    def test_consent_starts_at_the_us_server_asking_for_offline_access(self, org, catalog):
        from urllib.parse import urlparse, parse_qs

        url = self._start(org)["authorization_url"]
        assert url.startswith("https://accounts.zoho.com/oauth/v2/auth?")
        qs = parse_qs(urlparse(url).query)
        assert qs["access_type"] == ["offline"]
        assert qs["prompt"] == ["consent"]
        scopes = set(qs["scope"][0].split(","))
        assert {"ZohoCRM.modules.READ", "ZohoCRM.coql.READ",
                "ZohoCRM.modules.CREATE", "ZohoCRM.modules.UPDATE"} <= scopes
        # Record deletion is never requested.
        assert not {s for s in scopes if s.endswith((".DELETE", ".ALL"))}
        assert "code_challenge" not in qs

    def test_exchange_goes_to_the_named_data_centre(self, org, catalog):
        started = self._start(org)
        posted = {}

        def fake_post(url, data):
            posted["url"] = url
            return {"access_token": "at", "refresh_token": "rt",
                    "api_domain": "https://www.zohoapis.eu", "expires_in": 3600}

        ds = oauth.complete_authorization(
            state=started["state"], code="c", http_post=fake_post,
            callback_params={"location": "eu",
                             "accounts-server": "https://accounts.zoho.eu"},
        )
        assert posted["url"] == "https://accounts.zoho.eu/oauth/v2/token"
        bundle = secrets.decrypt_dict(ds.connection_json)
        assert bundle["INSTANCE"] == "accounts.zoho.eu"
        assert bundle["API_DOMAIN"] == "https://www.zohoapis.eu"

    def test_missing_server_means_the_one_consent_started_on(self, org, catalog):
        started = self._start(org)
        posted = {}

        def fake_post(url, data):
            posted["url"] = url
            return {"access_token": "at", "api_domain": "https://www.zohoapis.com"}

        oauth.complete_authorization(
            state=started["state"], code="c", http_post=fake_post)
        assert posted["url"] == "https://accounts.zoho.com/oauth/v2/token"

    @pytest.mark.parametrize("server", [
        "https://evil.example.com",
        "https://accounts.zoho.com.evil.io",
        "https://accounts.zoho.com@evil.example.com",
        "accounts.zoho.eu.evil.io",
    ])
    def test_a_forged_server_never_receives_the_client_secret(self, org, catalog, server):
        started = self._start(org)

        def fake_post(url, data):
            raise AssertionError(f"token exchange must not be sent to {url}")

        with pytest.raises(ApiError) as exc:
            oauth.complete_authorization(
                state=started["state"], code="c", http_post=fake_post,
                callback_params={"accounts-server": server},
            )
        assert "unrecognised server" in exc.value.message

    def test_refresh_goes_to_the_saved_data_centre(self, org, catalog):
        ds = DataSource.objects.create(
            display_name="Zoho", type="zoho_crm", connection_str="",
            organisation=org,
            catalog=ConnectorCatalog.objects.get(key="zoho_crm"),
        )
        ds.connection_json = secrets.encrypt_dict({
            "ACCESS_TOKEN": "old", "REFRESH_TOKEN": "rt",
            "INSTANCE": "accounts.zoho.in",
            "API_DOMAIN": "https://www.zohoapis.in",
            "TOKEN_EXPIRES_AT": str(time.time() - 1),
        })
        ds.save()
        posted = {}

        def fake_post(url, data):
            posted["url"] = url
            return {"access_token": "new", "api_domain": "https://www.zohoapis.in",
                    "expires_in": 3600}

        bundle = oauth.refresh_access_token(ds, http_post=fake_post)
        assert posted["url"] == "https://accounts.zoho.in/oauth/v2/token"
        assert bundle["ACCESS_TOKEN"] == "new"
        assert bundle["REFRESH_TOKEN"] == "rt"


@pytest.mark.django_db
class TestPipedriveBasicAuth:
    """Pipedrive authenticates the client at its token endpoint with HTTP Basic,
    takes no scope at consent (the app's scopes are set in its Developer Hub),
    and names the company's own API host in the token response."""

    @pytest.fixture(autouse=True)
    def pipedrive_creds(self, monkeypatch):
        monkeypatch.setenv("TERNO_PIPEDRIVE_CLIENT_ID", "pd-id")
        monkeypatch.setenv("TERNO_PIPEDRIVE_CLIENT_SECRET", "pd-secret")

    def _start(self, org):
        return oauth.start_authorization(
            connector_key="pipedrive",
            redirect_uri="https://acme.app.terno.ai/callback",
            organisation=org,
        )

    def test_consent_url_carries_no_scope_and_no_pkce(self, org, catalog):
        from urllib.parse import urlparse, parse_qs

        url = self._start(org)["authorization_url"]
        assert url.startswith("https://oauth.pipedrive.com/oauth/authorize?")
        qs = parse_qs(urlparse(url).query)
        assert qs["client_id"] == ["pd-id"]
        assert "scope" not in qs
        assert "code_challenge" not in qs

    def test_exchange_sends_the_secret_only_as_basic_auth(self, org, catalog):
        started = self._start(org)
        posted = {}

        def fake_post(url, data, auth=None):
            posted.update(url=url, data=data, auth=auth)
            return {"access_token": "at", "refresh_token": "rt",
                    "scope": "base,deals:read,users:read",
                    "api_domain": "https://acme.pipedrive.com", "expires_in": 3599}

        ds = oauth.complete_authorization(
            state=started["state"], code="c", http_post=fake_post)
        assert posted["url"] == "https://oauth.pipedrive.com/oauth/token"
        assert posted["auth"] == ("pd-id", "pd-secret")
        assert posted["data"] == {
            "grant_type": "authorization_code", "code": "c",
            "redirect_uri": "https://acme.app.terno.ai/callback"}
        bundle = secrets.decrypt_dict(ds.connection_json)
        assert bundle["API_DOMAIN"] == "https://acme.pipedrive.com"
        assert bundle["GRANTED_SCOPES"] == "base,deals:read,users:read"

    def test_refresh_sends_the_secret_only_as_basic_auth(self, org, catalog):
        ds = DataSource.objects.create(
            display_name="Pipedrive", type="pipedrive", connection_str="",
            organisation=org,
            catalog=ConnectorCatalog.objects.get(key="pipedrive"),
        )
        ds.connection_json = secrets.encrypt_dict({
            "ACCESS_TOKEN": "old", "REFRESH_TOKEN": "rt",
            "API_DOMAIN": "https://acme.pipedrive.com",
            "TOKEN_EXPIRES_AT": str(time.time() - 1),
        })
        ds.save()
        posted = {}

        def fake_post(url, data, auth=None):
            posted.update(url=url, data=data, auth=auth)
            return {"access_token": "new", "refresh_token": "rt",
                    "api_domain": "https://acme.pipedrive.com", "expires_in": 3599}

        bundle = oauth.refresh_access_token(ds, http_post=fake_post)
        assert posted["url"] == "https://oauth.pipedrive.com/oauth/token"
        assert posted["auth"] == ("pd-id", "pd-secret")
        assert posted["data"] == {"grant_type": "refresh_token", "refresh_token": "rt"}
        assert bundle["ACCESS_TOKEN"] == "new"

    def test_default_post_passes_basic_auth_to_requests(self, monkeypatch):
        seen = {}

        class _Resp:
            def raise_for_status(self): pass
            def json(self): return {"access_token": "at"}

        def fake_post(url, data=None, auth=None, timeout=None):
            seen.update(url=url, data=data, auth=auth)
            return _Resp()

        monkeypatch.setattr(oauth.requests, "post", fake_post)
        oauth._default_post("https://oauth.pipedrive.com/oauth/token",
                            {"grant_type": "refresh_token"}, auth=("i", "s"))
        assert seen["auth"] == ("i", "s")
