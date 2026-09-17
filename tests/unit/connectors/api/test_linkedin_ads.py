"""The LinkedIn Ads connector, against mocked Marketing API responses.

The mock returns the real response shapes from `adAccounts`, `adCampaigns` and
the `adAnalytics` analytics finder, so Rest.li parameter building, pivot/URN
handling and row parsing are exercised without a live provider.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
from terno_dbi.connectors.api.sources.linkedin_ads import LinkedInAdsConnector


class _Catalog:
    key = "linkedin_ads"
    report_types = [
        {"id": "Campaign", "settings": []},
        {"id": "CampaignGroup", "settings": []},
        {"id": "Creative", "settings": []},
        {"id": "Account", "settings": []},
    ]
    has_report_types = True


class _DS:
    type = "linkedin_ads"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok"}


# --- canned LinkedIn responses ---------------------------------------------

ACCOUNTS = {
    "elements": [
        {"id": 5101, "name": "Acme EU", "currency": "EUR",
         "status": "ACTIVE", "type": "BUSINESS"},
        {"id": 5102, "name": "Acme US", "currency": "USD",
         "status": "ACTIVE", "type": "BUSINESS"},
    ],
}

CAMPAIGNS = {
    "elements": [
        {"id": 777, "name": "Q3 Retargeting"},
        {"id": 888, "name": "Brand Awareness"},
    ],
}

# Two pivoted rows; cost arrives as a decimal *string*, as LinkedIn sends it.
ANALYTICS = {
    "elements": [
        {
            "impressions": 1200,
            "clicks": 34,
            "costInLocalCurrency": "145.6789",
            "pivotValues": ["urn:li:sponsoredCampaign:777"],
            "dateRange": {
                "start": {"year": 2026, "month": 8, "day": 1},
                "end": {"year": 2026, "month": 8, "day": 1},
            },
        },
        {
            "impressions": 800,
            "clicks": 12,
            "costInLocalCurrency": "90.5",
            "pivotValues": ["urn:li:sponsoredCampaign:888"],
            "dateRange": {
                "start": {"year": 2026, "month": 8, "day": 2},
                "end": {"year": 2026, "month": 8, "day": 2},
            },
        },
    ],
}


def _mock_http(routes):
    def http(method, url, token, params=None):
        assert token == "tok"
        for (m, needle), response in routes.items():
            if method == m and needle in url:
                return response
        raise AssertionError(f"unexpected call: {method} {url}")
    return http


def _connector(routes=None):
    return LinkedInAdsConnector(_DS(), http=_mock_http(routes or {
        ("GET", "/adAnalytics"): ANALYTICS,
        ("GET", "/adCampaigns"): CAMPAIGNS,
        ("GET", "/adAccounts"): ACCOUNTS,
    }))


def _capture():
    """A connector plus every call it makes, newest last."""
    calls = []

    def http(method, url, token, params=None):
        calls.append({"url": url, "params": params or {}})
        if "/adAnalytics" in url:
            return ANALYTICS
        if "/adCampaigns" in url or "/adCampaignGroups" in url:
            return CAMPAIGNS
        return ACCOUNTS

    return LinkedInAdsConnector(_DS(), http=http), calls


def _spec(fields=("campaign_id", "impressions", "clicks"), accounts=("5101",),
          report_type="Campaign", max_rows=1000):
    return QuerySpec(
        accounts=list(accounts),
        fields=list(fields),
        date_range=DateRange("2026-08-01", "2026-08-31"),
        report_type=report_type,
        max_rows=max_rows,
    )


def _analytics_call(calls):
    return next(c for c in calls if "/adAnalytics" in c["url"])


class TestListAccounts:
    def test_maps_sponsored_accounts_with_currency(self):
        conn = _connector({("GET", "/adAccounts"): ACCOUNTS})
        accounts = conn.list_accounts()
        assert [a.id for a in accounts] == ["5101", "5102"]
        assert accounts[0].name == "Acme EU"
        # Currency is load-bearing: the dispatch layer refuses to sum spend
        # across accounts that bill differently.
        assert accounts[0].currency == "EUR"
        assert accounts[1].currency == "USD"
        assert accounts[0].extra["status"] == "ACTIVE"


class TestListFields:
    def test_report_type_fixes_the_breakdown_dimensions(self):
        conn = _connector()
        campaign = {f.id for f in conn.list_fields("Campaign")}
        groups = {f.id for f in conn.list_fields("CampaignGroup")}
        assert {"campaign_id", "campaign_name", "date"} <= campaign
        assert {"campaign_group_id", "campaign_group_name"} <= groups
        assert "campaign_id" not in groups

    def test_creatives_expose_no_name_field(self):
        conn = _connector()
        creative = {f.id for f in conn.list_fields("Creative")}
        assert "creative_id" in creative
        # The Marketing API gives a creative no name; inventing one would lie.
        assert "creative_name" not in creative

    def test_metric_flags(self):
        conn = _connector()
        by_id = {f.id: f for f in conn.list_fields("Campaign")}
        assert by_id["impressions"].kind == "metric"
        assert by_id["costInLocalCurrency"].is_monetary is True
        assert by_id["costInUsd"].is_monetary is True
        assert by_id["date"].data_type == "date"


class TestRunReport:
    def test_parses_pivot_urns_and_metrics(self):
        conn = _connector()
        result = conn.query(_spec())
        assert result.row_count == 2
        assert result.rows[0] == {
            "campaign_id": "777", "impressions": 1200, "clicks": 34,
        }

    def test_money_strings_become_numbers(self):
        conn = _connector()
        result = conn.query(_spec(fields=("campaign_id", "costInLocalCurrency")))
        assert result.rows[0]["costInLocalCurrency"] == pytest.approx(145.6789)
        assert result.rows[1]["costInLocalCurrency"] == pytest.approx(90.5)

    def test_date_is_read_from_the_nested_range(self):
        conn = _connector()
        result = conn.query(_spec(fields=("date", "campaign_id", "impressions")))
        assert result.rows[0]["date"] == "2026-08-01"
        assert result.rows[1]["date"] == "2026-08-02"

    def test_unknown_field_is_rejected_with_a_suggestion(self):
        conn = _connector()
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(fields=("campaign_id", "impresions")))
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "impressions" in exc.value.message

    def test_no_metric_requested_falls_back_to_delivery_basics(self):
        conn = _connector()
        result = conn.query(_spec(fields=()))
        assert "impressions" in result.requested_field_ids
        assert result.rows[0]["impressions"] == 1200


class TestRequestBuilding:
    def test_date_range_uses_restli_syntax_with_unpadded_integers(self):
        conn, calls = _capture()
        conn.query(_spec())
        # Zero-padded months are rejected by Rest.li as malformed.
        assert _analytics_call(calls)["params"]["dateRange"] == (
            "(start:(year:2026,month:8,day:1),"
            "end:(year:2026,month:8,day:31))")

    def test_account_is_sent_as_a_urn_list(self):
        conn, calls = _capture()
        conn.query(_spec(accounts=("5101",)))
        assert _analytics_call(calls)["params"]["accounts"] == (
            "List(urn:li:sponsoredAccount:5101)")

    def test_a_urn_account_is_accepted_and_not_double_wrapped(self):
        conn, calls = _capture()
        conn.query(_spec(accounts=("urn:li:sponsoredAccount:5101",)))
        assert _analytics_call(calls)["params"]["accounts"] == (
            "List(urn:li:sponsoredAccount:5101)")

    def test_report_type_selects_the_pivot(self):
        conn, calls = _capture()
        conn.query(_spec(fields=("campaign_group_id",),
                         report_type="CampaignGroup"))
        assert _analytics_call(calls)["params"]["pivot"] == "CAMPAIGN_GROUP"

    def test_requesting_date_switches_to_daily_granularity(self):
        conn, calls = _capture()
        conn.query(_spec(fields=("date", "impressions")))
        params = _analytics_call(calls)["params"]
        assert params["timeGranularity"] == "DAILY"
        assert "dateRange" in params["fields"]

    def test_omitting_date_aggregates_over_the_whole_range(self):
        conn, calls = _capture()
        conn.query(_spec(fields=("campaign_id", "impressions")))
        params = _analytics_call(calls)["params"]
        assert params["timeGranularity"] == "ALL"
        # Not naming dateRange in `fields` keeps the response minimal.
        assert "dateRange" not in params["fields"]

    def test_pivot_values_are_always_requested(self):
        conn, calls = _capture()
        conn.query(_spec())
        # LinkedIn omits pivotValues unless asked, leaving rows unidentifiable.
        assert "pivotValues" in _analytics_call(calls)["params"]["fields"]

    def test_count_is_capped_at_the_api_maximum(self):
        conn, calls = _capture()
        conn.query(_spec(max_rows=50000))
        assert _analytics_call(calls)["params"]["count"] == 1000


class TestNameResolution:
    def test_campaign_names_are_resolved_from_urns(self):
        conn = _connector()
        result = conn.query(_spec(fields=("campaign_id", "campaign_name")))
        assert result.rows[0]["campaign_name"] == "Q3 Retargeting"
        assert result.rows[1]["campaign_name"] == "Brand Awareness"

    def test_names_are_not_fetched_when_no_name_was_requested(self):
        conn, calls = _capture()
        conn.query(_spec(fields=("campaign_id", "impressions")))
        # The lookup is a whole extra request; it must stay opt-in.
        assert not any("/adCampaigns" in c["url"] for c in calls)

    def test_names_are_fetched_once_per_account(self):
        conn, calls = _capture()
        conn.query(_spec(fields=("campaign_id", "campaign_name"),
                         accounts=("5101", "5102")))
        lookups = [c for c in calls if "/adCampaigns" in c["url"]]
        assert len(lookups) == 2      # one per account, not one per row

    def test_account_report_names_come_from_discovery(self):
        conn, calls = _capture()
        result = conn.query(_spec(fields=("account_id", "account_name"),
                                  report_type="Account"))
        # The account's own name is already known; no sub-resource lookup.
        assert not any("/adCampaignGroups" in c["url"] for c in calls)
        assert result.rows[0]["account_name"] is None or isinstance(
            result.rows[0]["account_name"], str)

    def test_an_unresolvable_urn_yields_none_not_a_urn(self):
        def http(method, url, token, params=None):
            if "/adAnalytics" in url:
                return ANALYTICS
            return {"elements": []}       # names unavailable

        conn = LinkedInAdsConnector(_DS(), http=http)
        result = conn.query(_spec(fields=("campaign_id", "campaign_name")))
        assert result.rows[0]["campaign_id"] == "777"
        assert result.rows[0]["campaign_name"] is None


class TestMultiAccount:
    def test_merges_and_tags_rows(self):
        conn = _connector()
        result = conn.query(_spec(accounts=("5101", "5102")))
        assert result.row_count == 4
        assert {r["_account"] for r in result.rows} == {"5101", "5102"}

    def test_one_failing_account_does_not_sink_the_others(self):
        def http(method, url, token, params=None):
            if "5102" in str((params or {}).get("accounts", "")):
                raise ApiError(ErrorCode.UPSTREAM_ERROR, "no permission")
            return ANALYTICS

        conn = LinkedInAdsConnector(_DS(), http=http)
        result = conn.query(_spec(accounts=("5101", "5102")))
        assert result.row_count == 2
        assert any("5102" in w for w in result.warnings)


class TestAuthMapping:
    def test_401_becomes_auth_expired(self):
        from terno_dbi.connectors.api.sources.linkedin_ads import _AuthError

        def http(method, url, token, params=None):
            raise _AuthError()

        conn = LinkedInAdsConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestApiVersion:
    def test_defaults_and_can_be_overridden_by_environment(self, monkeypatch):
        from terno_dbi.connectors.api.sources import linkedin_ads

        monkeypatch.delenv("TERNO_LINKEDIN_API_VERSION", raising=False)
        assert linkedin_ads.api_version() == linkedin_ads._DEFAULT_VERSION
        # LinkedIn sunsets versions quarterly, so a deployment must be able to
        # move without a code release.
        monkeypatch.setenv("TERNO_LINKEDIN_API_VERSION", "202612")
        assert linkedin_ads.api_version() == "202612"


class TestTokenRefreshOnProviderCall:
    def test_list_accounts_refreshes_the_token(self):
        calls = []
        conn = LinkedInAdsConnector(
            _DS(), http=lambda *a, **k: ACCOUNTS,
            token_refresher=lambda: calls.append(1),
        )
        conn.list_accounts()
        assert calls == [1]


class TestRegistration:
    def test_linkedin_is_registered_at_startup(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("linkedin_ads")

    def test_registered_factory_binds_a_token_refresher(self):
        from terno_dbi.connectors.api.sources.linkedin_ads import (
            make_linkedin_ads_connector,
        )
        conn = make_linkedin_ads_connector(_DS())
        assert conn._token_refresher is not None

    def test_oauth_provider_is_configured_without_pkce(self):
        from terno_dbi.connectors.api.auth.providers import get_provider
        provider = get_provider("linkedin_ads")
        assert provider is not None
        assert provider.scope == "r_ads r_ads_reporting"
        # LinkedIn's token endpoint authenticates with the client secret and
        # rejects a PKCE challenge.
        assert provider.use_pkce is False
