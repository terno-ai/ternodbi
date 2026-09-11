"""Connect/setup link building across deployment shapes.

The connect link must be absolute so an agent can drop it into chat. Two shapes
must both work: multi-tenant (per-org subdomain) and single-host (local /
self-hosted, one host for every org). The single-host case is the one that
regressed to a null URL on local.
"""

import pytest
from django.test import override_settings

from terno_dbi.mcp.setup_link import connect_url, datasource_setup_url


class _Catalog:
    key = "google_ads"
    auth_type = "oauth"


class _ManualCatalog:
    key = "mysql"
    auth_type = "manual"


class TestAuthTypeShape:
    @override_settings(ENABLE_SUBDOMAIN=True, MAIN_DOMAIN="app.ternoapp.com")
    def test_oauth_uses_connect_endpoint(self):
        assert connect_url("navin1", _Catalog()) == (
            "https://navin1.app.ternoapp.com/connect?connector=google_ads"
        )

    @override_settings(ENABLE_SUBDOMAIN=True, MAIN_DOMAIN="app.ternoapp.com")
    def test_manual_uses_the_credentials_modal(self):
        assert connect_url("navin1", _ManualCatalog()) == (
            "https://navin1.app.ternoapp.com/data-connectors/datasource/mysql"
        )

    @override_settings(ENABLE_SUBDOMAIN=False, MAIN_DOMAIN="http://127.0.0.1:8000")
    def test_manual_on_single_host(self):
        assert connect_url(None, _ManualCatalog()) == (
            "http://127.0.0.1:8000/data-connectors/datasource/mysql"
        )

    @override_settings(ENABLE_SUBDOMAIN=False, MAIN_DOMAIN="http://127.0.0.1:8000",
                       TERNO_MANUAL_CONNECT_PATH="/custom/setup")
    def test_manual_path_is_configurable(self):
        assert connect_url(None, _ManualCatalog()) == (
            "http://127.0.0.1:8000/custom/setup/mysql"
        )


class TestMultiTenant:
    @override_settings(ENABLE_SUBDOMAIN=True, MAIN_DOMAIN="app.ternoapp.com")
    def test_uses_the_org_subdomain(self):
        assert connect_url("navin1", _Catalog()) == (
            "https://navin1.app.ternoapp.com/connect?connector=google_ads"
        )

    @override_settings(ENABLE_SUBDOMAIN=True, MAIN_DOMAIN="app.ternoapp.com")
    def test_no_subdomain_yields_no_link(self):
        # Multi-tenant genuinely cannot address the org without its subdomain.
        assert connect_url(None, _Catalog()) is None


class TestSingleHost:
    @override_settings(ENABLE_SUBDOMAIN=False, MAIN_DOMAIN="http://127.0.0.1:8000")
    def test_uses_main_domain_verbatim_with_scheme(self):
        # Local: one host, no subdomain, keep http scheme.
        assert connect_url(None, _Catalog()) == (
            "http://127.0.0.1:8000/connect?connector=google_ads"
        )

    @override_settings(ENABLE_SUBDOMAIN=False, MAIN_DOMAIN="http://127.0.0.1:8000")
    def test_subdomain_is_ignored_on_single_host(self):
        assert connect_url("navin1", _Catalog()) == (
            "http://127.0.0.1:8000/connect?connector=google_ads"
        )

    @override_settings(ENABLE_SUBDOMAIN=False, MAIN_DOMAIN="terno.example.com")
    def test_bare_host_defaults_to_https(self):
        assert connect_url(None, _Catalog()) == (
            "https://terno.example.com/connect?connector=google_ads"
        )

    @override_settings(ENABLE_SUBDOMAIN=False, MAIN_DOMAIN="http://127.0.0.1:8000")
    def test_setup_url_also_single_host(self):
        url = datasource_setup_url(None)
        assert url is not None
        assert url.startswith("http://127.0.0.1:8000/")


class TestUnconfigured:
    @override_settings(ENABLE_SUBDOMAIN=False, MAIN_DOMAIN="")
    def test_no_domain_yields_no_link(self):
        assert connect_url(None, _Catalog()) is None
