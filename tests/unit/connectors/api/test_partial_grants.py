"""Partial OAuth grants: a required permission left unticked on the consent screen."""

import json
from io import StringIO
from urllib.parse import parse_qs, urlsplit

import pytest
from django.contrib.auth.models import User
from django.core.management import call_command
from django.test import RequestFactory

from terno_dbi.catalog.refresh import refresh_catalog
from terno_dbi.connectors.api import management_service as svc
from terno_dbi.connectors.api import web
from terno_dbi.connectors.api.auth import oauth
from terno_dbi.connectors.api.auth.grants import missing_required_scopes
from terno_dbi.connectors.api.auth.providers import get_provider
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.core.models import CoreOrganisation, DataSource
from terno_dbi.services import secrets

GA = "https://www.googleapis.com/auth/analytics.readonly"
GA_LABEL = "See and download your Google Analytics data"


@pytest.fixture(autouse=True)
def creds(monkeypatch):
    monkeypatch.setenv("TERNO_GOOGLE_OAUTH_CLIENT_ID", "client-abc")
    monkeypatch.setenv("TERNO_GOOGLE_OAUTH_CLIENT_SECRET", "secret-xyz")
    monkeypatch.setenv("TERNO_META_APP_ID", "meta-id")
    monkeypatch.setenv("TERNO_META_APP_SECRET", "meta-secret")
    secrets.reset_cache()
    yield
    secrets.reset_cache()


@pytest.fixture
def org(db):
    refresh_catalog()
    user = User.objects.create_user("grantuser", "g@x.com", "pw")
    return CoreOrganisation.objects.create(name="Acme", subdomain="acme", owner=user)


def _connect_ga4(org, scope, return_to=""):
    started = oauth.start_authorization(
        connector_key="googleanalytics4",
        redirect_uri="https://acme.app.terno.ai/callback",
        organisation=org, return_to=return_to,
    )
    response = {"access_token": "at", "refresh_token": "rt", "expires_in": 3600}
    if scope is not None:
        response["scope"] = scope
    return started, (lambda url, data, **kw: response)


class TestRequiredScopes:
    def test_ga4_without_analytics_is_missing_it(self):
        assert missing_required_scopes(get_provider("googleanalytics4"),
                                       "openid email") == [GA]

    def test_unknown_grant_is_never_missing(self):
        assert missing_required_scopes(get_provider("googleanalytics4"), "") == []

    def test_identity_scopes_are_never_required(self):
        granted = f"openid https://www.googleapis.com/auth/userinfo.email {GA}"
        assert missing_required_scopes(get_provider("googleanalytics4"), granted) == []

    def test_youtube_monetary_is_optional(self):
        granted = ("https://www.googleapis.com/auth/yt-analytics.readonly "
                   "https://www.googleapis.com/auth/youtube.readonly")
        assert missing_required_scopes(get_provider("youtube"), granted) == []

    def test_providers_without_granular_consent_are_not_checked(self):
        assert missing_required_scopes(get_provider("shopify"), "read_orders") == []

    def test_meta_grants_are_comma_separated(self):
        assert missing_required_scopes(get_provider("meta_ads"),
                                       "public_profile") == ["ads_read"]
        assert missing_required_scopes(get_provider("meta_ads"),
                                       "public_profile,ads_read") == []


@pytest.mark.django_db
class TestConnect:
    def test_missing_scope_saves_source_as_needing_reconnect(self, org):
        started, post = _connect_ga4(org, "openid email")
        ds = oauth.complete_authorization(state=started["state"], code="c",
                                          http_post=post)
        assert ds.auth_status == DataSource.AuthStatus.EXPIRED
        assert GA_LABEL in ds.auth_error
        assert ds.needs_reconnect

    def test_full_grant_connects(self, org):
        started, post = _connect_ga4(org, f"openid email {GA}")
        ds = oauth.complete_authorization(state=started["state"], code="c",
                                          http_post=post)
        assert ds.auth_status == DataSource.AuthStatus.CONNECTED
        assert ds.auth_error == ""

    def test_unreported_grant_connects(self, org):
        started, post = _connect_ga4(org, None)
        ds = oauth.complete_authorization(state=started["state"], code="c",
                                          http_post=post)
        assert ds.auth_status == DataSource.AuthStatus.CONNECTED

    def test_reconnect_does_not_reuse_the_old_grant(self, org):
        # The first connect recorded a partial grant; the reconnect's provider
        # reports nothing. The stale grant must not block it.
        started, post = _connect_ga4(org, "openid email")
        oauth.complete_authorization(state=started["state"], code="c", http_post=post)
        started, post = _connect_ga4(org, None)
        ds = oauth.complete_authorization(state=started["state"], code="c",
                                          http_post=post)
        assert ds.auth_status == DataSource.AuthStatus.CONNECTED
        assert "GRANTED_SCOPES" not in secrets.decrypt_dict(ds.connection_json)

    def test_refresh_keeps_the_grant(self, org):
        started, post = _connect_ga4(org, f"openid email {GA}")
        ds = oauth.complete_authorization(state=started["state"], code="c",
                                          http_post=post)
        oauth.refresh_access_token(
            ds, http_post=lambda url, data, **kw: {"access_token": "new", "expires_in": 3600})
        bundle = secrets.decrypt_dict(ds.connection_json)
        assert GA in bundle["GRANTED_SCOPES"]
        assert ds.auth_status == DataSource.AuthStatus.CONNECTED

    def test_reconnect_with_full_grant_clears_the_problem(self, org):
        started, post = _connect_ga4(org, "openid email")
        oauth.complete_authorization(state=started["state"], code="c", http_post=post)
        started, post = _connect_ga4(org, f"openid email {GA}")
        ds = oauth.complete_authorization(state=started["state"], code="c",
                                          http_post=post)
        assert ds.auth_status == DataSource.AuthStatus.CONNECTED


class _Resp:
    def __init__(self, body, status=200):
        self._body, self.status_code = body, status

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


class TestMetaGrants:
    def _get(self, permissions, fail=False):
        def get(url, params=None, headers=None, timeout=None):
            if "permissions" in url:
                if fail:
                    return _Resp({}, status=500)
                return _Resp({"data": permissions})
            return _Resp({"access_token": "long", "expires_in": 5184000})
        return get

    def test_declined_permission_is_recorded(self, monkeypatch):
        monkeypatch.setattr(oauth.requests, "get", self._get([
            {"permission": "public_profile", "status": "granted"},
            {"permission": "ads_read", "status": "declined"},
        ]))
        out = oauth._post_process("meta_ads", get_provider("meta_ads"),
                                  {"access_token": "short"})
        assert out["scope"] == "public_profile"
        assert out["access_token"] == "long"
        assert missing_required_scopes(get_provider("meta_ads"), out["scope"]) == ["ads_read"]

    def test_failed_lookup_leaves_grant_unknown(self, monkeypatch):
        monkeypatch.setattr(oauth.requests, "get", self._get([], fail=True))
        out = oauth._post_process("meta_ads", get_provider("meta_ads"),
                                  {"access_token": "short"})
        assert "scope" not in out

    def test_facebook_pages_records_grants_too(self, monkeypatch):
        monkeypatch.setattr(oauth.requests, "get", self._get([
            {"permission": "pages_show_list", "status": "granted"},
            {"permission": "pages_read_engagement", "status": "granted"},
        ]))
        out = oauth._post_process("facebook_pages", get_provider("facebook_pages"),
                                  {"access_token": "t"})
        assert out["scope"] == "pages_show_list,pages_read_engagement"

    def test_lookup_keeps_the_token_out_of_the_url_and_logs(self, monkeypatch, caplog):
        seen = {}

        def get(url, params=None, headers=None, timeout=None):
            seen.update(url=url, params=params, headers=headers)
            raise RuntimeError(f"boom {url}")

        monkeypatch.setattr(oauth.requests, "get", get)
        oauth._meta_grants({"access_token": "secret-token"})
        assert "secret-token" not in seen["url"]
        assert not seen["params"]
        assert seen["headers"]["Authorization"] == "Bearer secret-token"
        assert "secret-token" not in caplog.text

    def test_meta_reprompts_declined_permissions(self):
        assert get_provider("meta_ads").extra_authorize_params["auth_type"] == "rerequest"


class TestInstagramPublic:
    def test_needs_only_profile_and_page(self):
        provider = get_provider("instagram_public")
        assert missing_required_scopes(
            provider, "public_profile,instagram_basic,pages_show_list") == []
        assert missing_required_scopes(
            provider, "public_profile,pages_show_list") == ["instagram_basic"]

    def test_insights_still_need_insights(self):
        assert "instagram_manage_insights" in missing_required_scopes(
            get_provider("instagram_insights"), "instagram_basic,pages_show_list")


class TestInstagramLoginGrants:
    def test_permissions_list_becomes_scope(self):
        out = oauth._instagram_login_grants(
            {"access_token": "t", "permissions": ["instagram_business_basic"]})
        assert out["scope"] == "instagram_business_basic"
        provider = get_provider("instagram_insights", "instagram")
        assert missing_required_scopes(provider, out["scope"]) == [
            "instagram_business_manage_insights"]


def _callback(params):
    request = RequestFactory().get("/connectors/oauth/callback/", params)
    return web.oauth_callback(request)


@pytest.mark.django_db
class TestCallback:
    def test_missing_scope_returns_to_app_with_the_reason(self, org, monkeypatch):
        started, post = _connect_ga4(org, "openid email", return_to="/data-connectors")
        monkeypatch.setattr(oauth, "_default_post", post)
        resp = _callback({"state": started["state"], "code": "c"})
        assert resp.status_code == 302
        qs = parse_qs(urlsplit(resp.url).query)
        assert urlsplit(resp.url).path == "/data-connectors"
        assert qs["connector_error"] == ["googleanalytics4"]
        assert qs["reason"] == ["missing_permission"]
        assert "select_accounts" not in qs

    def test_declined_returns_to_app(self, org):
        started, _ = _connect_ga4(org, None, return_to="/data-connectors")
        resp = _callback({"state": started["state"], "error": "access_denied"})
        assert resp.status_code == 302
        qs = parse_qs(urlsplit(resp.url).query)
        assert qs["connector_error"] == ["googleanalytics4"]
        assert qs["reason"] == ["declined"]

    def test_expired_link_returns_to_app(self, org):
        from datetime import timedelta
        from django.utils import timezone
        from terno_dbi.core.models import ConnectorOAuthState
        started, _ = _connect_ga4(org, None, return_to="/data-connectors")
        ConnectorOAuthState.objects.filter(state=started["state"]).update(
            expires_at=timezone.now() - timedelta(minutes=1))
        resp = _callback({"state": started["state"], "code": "c"})
        assert resp.status_code == 302
        assert parse_qs(urlsplit(resp.url).query)["reason"] == ["link_expired"]

    def test_provider_codes_pass_through_but_free_text_does_not(self, org):
        started, _ = _connect_ga4(org, None, return_to="/data-connectors")
        resp = _callback({"state": started["state"], "error": "no_org"})
        assert parse_qs(urlsplit(resp.url).query)["reason"] == ["no_org"]
        started, _ = _connect_ga4(org, None, return_to="/data-connectors")
        resp = _callback({"state": started["state"],
                          "error": "Your account is locked, call 555"})
        qs = parse_qs(urlsplit(resp.url).query)
        assert qs["reason"] == ["failed"]
        assert "555" not in resp.url

    def test_without_return_to_shows_the_message(self, org):
        resp = _callback({"error": "access_denied"})
        assert resp.status_code == 400
        assert b"declined" in resp.content


def _ga4_source(org, scope, status=DataSource.AuthStatus.CONNECTED):
    ds = oauth.connector_datasource(org, "googleanalytics4")
    bundle = {"ACCESS_TOKEN": "at", "TOKEN_EXPIRES_AT": "9999999999"}
    if scope is not None:
        bundle["GRANTED_SCOPES"] = scope
    ds.connection_json = secrets.encrypt_dict(bundle)
    ds.auth_status = status
    ds.save()
    return ds


@pytest.mark.django_db
class TestExistingSources:
    def test_query_path_marks_source_and_asks_to_reconnect(self, org):
        from terno_dbi.connectors.api.sources.ga4 import GA4Connector
        ds = _ga4_source(org, "openid email")
        called = []
        conn = GA4Connector(ds, http=lambda *a: called.append(a) or {})
        with pytest.raises(ApiError) as exc:
            conn.ensure_required_grants()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert exc.value.retriable is False
        assert GA_LABEL in exc.value.message
        ds.refresh_from_db()
        assert ds.auth_status == DataSource.AuthStatus.EXPIRED
        assert not called

    def test_unknown_grant_is_not_blocked(self, org):
        from terno_dbi.connectors.api.sources.ga4 import GA4Connector
        ds = _ga4_source(org, None)
        GA4Connector(ds, http=lambda *a: {}).ensure_required_grants()

    def test_account_picker_error_says_reconnect(self, org):
        _ga4_source(org, "openid email")
        with pytest.raises(svc.ConnectorBadRequest) as exc:
            svc.list_accounts(org.owner, org, "googleanalytics4")
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert exc.value.reconnect
        body = json.loads(web._management_error_response(exc.value).content)
        assert body["code"] == "AUTH_EXPIRED" and body["reconnect"] is True
        assert GA_LABEL in body["error"]

    def test_card_names_missing_permissions(self, org):
        _ga4_source(org, "openid email", status=DataSource.AuthStatus.EXPIRED)
        cards = {c["key"]: c for c in svc._connector_cards(org)}
        assert cards["googleanalytics4"]["missing_permissions"] == [GA_LABEL]
        assert cards["googleanalytics4"]["status"] == "expired"

    def test_audit_command_dry_run_then_apply(self, org):
        ds = _ga4_source(org, "openid email")
        out = StringIO()
        call_command("audit_connector_grants", stdout=out)
        assert f"{ds.id}\tgoogleanalytics4" in out.getvalue()
        ds.refresh_from_db()
        assert ds.auth_status == DataSource.AuthStatus.CONNECTED

        call_command("audit_connector_grants", "--apply", stdout=StringIO())
        ds.refresh_from_db()
        assert ds.auth_status == DataSource.AuthStatus.EXPIRED
        assert GA_LABEL in ds.auth_error


class TestGoogleScopeErrors:
    def test_ga4_insufficient_scope_asks_to_reconnect(self):
        from terno_dbi.connectors.api.sources.ga4 import _ga4_error
        err = _ga4_error(_Resp({"error": {
            "code": 403, "status": "PERMISSION_DENIED",
            "message": "Request had insufficient authentication scopes.",
            "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
                         "reason": "ACCESS_TOKEN_SCOPE_INSUFFICIENT"}]}}, 403))
        assert err.code == ErrorCode.AUTH_EXPIRED
        assert "Reconnect" in err.message

    def test_ga4_other_403_names_the_reason(self):
        from terno_dbi.connectors.api.sources.ga4 import _ga4_error
        err = _ga4_error(_Resp({"error": {
            "code": 403, "status": "PERMISSION_DENIED",
            "details": [{"reason": "SERVICE_DISABLED"}]}}, 403))
        assert err.code == ErrorCode.UPSTREAM_ERROR
        assert "SERVICE_DISABLED" in err.message


SHEETS = "https://www.googleapis.com/auth/spreadsheets.readonly"
DRIVE = "https://www.googleapis.com/auth/drive.readonly"
DRIVE_META = "https://www.googleapis.com/auth/drive.metadata.readonly"


class TestRequestedScopes:
    def test_scope_added_after_connect_does_not_block(self):
        # Connected when the connector asked only for the Sheets scope; it has
        # since started needing Drive. The user never declined Drive.
        from terno_dbi.connectors.api.auth.grants import missing_scopes
        bundle = {"GRANTED_SCOPES": f"openid email {SHEETS}",
                  "REQUESTED_SCOPES": f"openid email {SHEETS}"}
        assert missing_scopes("google_sheets", bundle) == []

    def test_declined_requested_scope_blocks(self):
        from terno_dbi.connectors.api.auth.grants import missing_scopes
        bundle = {"GRANTED_SCOPES": f"openid email {SHEETS}",
                  "REQUESTED_SCOPES": f"openid email {SHEETS} {DRIVE}"}
        assert missing_scopes("google_sheets", bundle) == [DRIVE]

    def test_unrecorded_request_blocks_only_an_empty_grant(self):
        # Sources connected before requests were recorded: an older, narrower
        # scope set must not be read as a decline.
        from terno_dbi.connectors.api.auth.grants import missing_scopes
        assert missing_scopes("google_sheets", {
            "GRANTED_SCOPES": f"openid email {SHEETS} {DRIVE_META}"}) == []
        assert missing_scopes("google_sheets", {
            "GRANTED_SCOPES": "openid email"}) == [DRIVE, SHEETS]


@pytest.mark.django_db
class TestFreshGrantBookkeeping:
    def test_connect_records_what_was_asked(self, org):
        started, post = _connect_ga4(org, f"openid email {GA}")
        ds = oauth.complete_authorization(state=started["state"], code="c",
                                          http_post=post)
        bundle = secrets.decrypt_dict(ds.connection_json)
        assert GA in bundle["REQUESTED_SCOPES"]

    def test_switching_login_method_drops_the_old_one(self, org, monkeypatch):
        # Connected with Instagram Login, reconnected with Facebook Login: the
        # grant must be judged against Facebook's provider, not Instagram's.
        ds = oauth.connector_datasource(org, "instagram_insights")
        ds.connection_json = secrets.encrypt_dict(
            {"ACCESS_TOKEN": "old", "LOGIN_METHOD": "instagram",
             "GRANTED_SCOPES": "instagram_business_basic"})
        ds.save()
        oauth._store_tokens(ds, {
            "access_token": "new",
            "scope": "public_profile,instagram_basic,instagram_manage_insights,"
                     "pages_show_list"}, fresh_grant=True)
        bundle = secrets.decrypt_dict(ds.connection_json)
        assert "LOGIN_METHOD" not in bundle
        assert ds.auth_status == DataSource.AuthStatus.CONNECTED


@pytest.mark.django_db
class TestProviderScopeRefusal:
    def test_scope_403_marks_the_source_for_reconnect(self, org):
        from terno_dbi.connectors.api.sources.ga4 import GA4Connector, _ga4_error
        ds = _ga4_source(org, None)

        def http(*args):
            raise _ga4_error(_Resp({"error": {
                "code": 403, "details": [{"reason": "ACCESS_TOKEN_SCOPE_INSUFFICIENT"}]}}, 403))

        with pytest.raises(ApiError) as exc:
            GA4Connector(ds, http=http).list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        ds.refresh_from_db()
        assert ds.auth_status == DataSource.AuthStatus.EXPIRED

    def test_other_errors_leave_the_source_alone(self, org):
        from terno_dbi.connectors.api.sources.ga4 import GA4Connector, _ga4_error
        ds = _ga4_source(org, None)

        def http(*args):
            raise _ga4_error(_Resp({"error": {"code": 500, "message": "x"}}, 500))

        with pytest.raises(ApiError):
            GA4Connector(ds, http=http).list_accounts()
        ds.refresh_from_db()
        assert ds.auth_status == DataSource.AuthStatus.CONNECTED

    def test_non_retriable_error_does_not_say_try_again(self):
        from terno_dbi.connectors.api.sources.ga4 import _ga4_error
        err = _ga4_error(_Resp({"error": {"code": 400, "message": "Bad dimension"}}, 400))
        assert "Bad dimension" in err.message and "Try again" not in err.message


@pytest.mark.django_db
class TestWritesAndResponses:
    def test_write_action_checks_grants_first(self, org):
        from terno_dbi.connectors.api.pipeline import dispatch
        from terno_dbi.core.models import ConnectorAccountSelection
        ds = _ga4_source(org, "openid email")
        ConnectorAccountSelection.objects.create(
            data_source=ds, account_id="1", enabled=True, writes_enabled=True)

        class _Conn:
            datasource = ds
            key = "googleanalytics4"

            def ensure_required_grants(self):
                raise ApiError(ErrorCode.AUTH_EXPIRED, "reconnect", retriable=False)

            def execute_action(self, *a, **k):
                raise AssertionError("must not reach the provider")

        with pytest.raises(ApiError) as exc:
            dispatch.run_write_action(_Conn(), "x", "1")
        assert exc.value.code == ErrorCode.AUTH_EXPIRED

    def test_error_fields(self):
        assert svc.ConnectorBadRequest("m", code="AUTH_EXPIRED").error_fields() == {
            "code": "AUTH_EXPIRED", "reconnect": True}
        assert svc.ConnectorBadRequest("m", code="UPSTREAM_ERROR").error_fields() == {
            "code": "UPSTREAM_ERROR"}
        assert svc.ConnectorBadRequest("m").error_fields() == {}

    def test_reason_with_trailing_newline_is_not_echoed(self, org):
        started, _ = _connect_ga4(org, None, return_to="/data-connectors")
        resp = _callback({"state": started["state"], "error": "no_org\n"})
        assert parse_qs(urlsplit(resp.url).query)["reason"] == ["failed"]


@pytest.mark.django_db
class TestReconnectLinkInErrors:
    @pytest.fixture(autouse=True)
    def _single_host(self, settings):
        settings.ENABLE_SUBDOMAIN = False
        settings.MAIN_DOMAIN = "https://app.example.com"

    def test_reconnect_errors_carry_the_link(self, org):
        from terno_dbi.mcp.setup_link import add_reconnect_url
        ds = _ga4_source(org, "openid email")
        error = add_reconnect_url(
            {"code": ErrorCode.AUTH_EXPIRED, "message": "m"}, ds)
        assert "connector=googleanalytics4" in error["details"]["reconnect_url"]

    def test_other_errors_do_not(self, org):
        from terno_dbi.mcp.setup_link import add_reconnect_url
        ds = _ga4_source(org, "openid email")
        error = add_reconnect_url(
            {"code": ErrorCode.UPSTREAM_ERROR, "message": "m"}, ds)
        assert "details" not in error

    def test_failed_query_job_tells_the_caller_where_to_reconnect(self, org):
        from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
        from terno_dbi.connectors.api.pipeline import jobs
        from terno_dbi.connectors.api.sources.ga4 import GA4Connector
        from terno_dbi.core.models import ApiQueryJob
        ds = _ga4_source(org, "openid email")
        job = ApiQueryJob.objects.create(organisation=org, data_source=ds)
        spec = QuerySpec(accounts=["1"], fields=["sessions"],
                         date_range=DateRange("2026-01-01", "2026-01-31"))
        jobs._execute_job(job.id, ds, spec,
                          connector_factory=lambda d: GA4Connector(d, http=lambda *a: {}),
                          permitted_accounts=None, rate_limit=None)
        job.refresh_from_db()
        assert job.error["code"] == ErrorCode.AUTH_EXPIRED
        assert GA_LABEL in job.error["message"]
        assert "connector=googleanalytics4" in job.error["details"]["reconnect_url"]
