"""The two Instagram connectors, against mocked Instagram Graph API responses.

Covers discovery (Facebook Page -> linked IG Business account), the Insights
reports (account time-series pivot, media with nested insights) and the Public
Data reports (Business Discovery profile/media, hashtag two-step), plus the
required-setting guard — all without a live provider or app approval.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
from terno_dbi.connectors.api.sources.instagram_insights import (
    InstagramInsightsConnector,
)
from terno_dbi.connectors.api.sources.instagram_public import (
    InstagramPublicConnector,
)


_REPORT_TYPES = {
    "instagram_insights": [
        {"id": "AccountInsights", "settings": []},
        {"id": "AccountTotals", "settings": []},
        {"id": "Media", "settings": []},
    ],
    "instagram_public": [
        {"id": "Profile",
         "settings": [{"setting_id": "username", "required": True}]},
        {"id": "Media",
         "settings": [{"setting_id": "username", "required": True}]},
        {"id": "Hashtag",
         "settings": [{"setting_id": "hashtag", "required": True}]},
    ],
}


class _Catalog:
    def __init__(self, ds_type):
        self.key = ds_type
        self.report_types = _REPORT_TYPES[ds_type]
        self.has_report_types = True


class _DS:
    def __init__(self, ds_type, login_method=""):
        self.type = ds_type
        self.catalog = _Catalog(ds_type)
        self.connection_json = {"ACCESS_TOKEN": "tok"}
        if login_method:
            self.connection_json["LOGIN_METHOD"] = login_method


# A Facebook Pages reply: one page with a linked IG account, one without.
ACCOUNTS = {
    "data": [
        {"name": "CloudXLab Page",
         "instagram_business_account": {"id": "178414", "username": "cloudxlab",
                                        "name": "CloudXLab"}},
        {"name": "No-IG Page"},
    ],
}


def _spec(fields, report_type, accounts=("178414",), settings=None):
    return QuerySpec(
        accounts=list(accounts), fields=list(fields),
        date_range=DateRange("2026-08-01", "2026-08-31"),
        report_type=report_type, settings=settings or {},
    )


# --------------------------------------------------------------------------
# Discovery (shared)
# --------------------------------------------------------------------------

class TestDiscovery:
    def test_lists_linked_ig_business_accounts(self):
        def http(method, url, token, params=None):
            assert "me/accounts" in url
            return ACCOUNTS

        conn = InstagramInsightsConnector(_DS("instagram_insights"), http=http)
        accts = conn.list_accounts()
        assert [a.id for a in accts] == ["178414"]      # page without IG skipped
        assert accts[0].name == "@cloudxlab"

    def test_public_connector_shares_discovery(self):
        conn = InstagramPublicConnector(
            _DS("instagram_public"),
            http=lambda m, u, t, p=None: ACCOUNTS)
        assert [a.id for a in conn.list_accounts()] == ["178414"]


# --------------------------------------------------------------------------
# Instagram Insights
# --------------------------------------------------------------------------

class TestInsights:
    def test_account_insights_pivots_series_to_daily_rows(self):
        insights = {"data": [
            {"name": "reach", "period": "day", "values": [
                {"value": 100, "end_time": "2026-08-01T07:00:00+0000"},
                {"value": 150, "end_time": "2026-08-02T07:00:00+0000"}]},
            {"name": "follower_count", "period": "day", "values": [
                {"value": 5, "end_time": "2026-08-01T07:00:00+0000"},
                {"value": 8, "end_time": "2026-08-02T07:00:00+0000"}]},
        ]}

        captured = {}

        def http(method, url, token, params=None):
            captured["url"] = url
            captured["params"] = params
            return insights

        conn = InstagramInsightsConnector(_DS("instagram_insights"), http=http)
        res = conn.query(_spec(["date", "reach", "follower_count"],
                               "AccountInsights"))
        assert "178414/insights" in captured["url"]
        assert captured["params"]["period"] == "day"
        # v22 daily series requires metric_type=time_series (fixes empty results).
        assert captured["params"]["metric_type"] == "time_series"
        assert res.rows == [
            {"date": "2026-08-01", "reach": 100, "follower_count": 5},
            {"date": "2026-08-02", "reach": 150, "follower_count": 8},
        ]

    def test_account_totals_returns_one_aggregate_row(self):
        def http(method, url, token, params=None):
            assert params["metric_type"] == "total_value"
            return {"data": [
                {"name": "reach", "total_value": {"value": 8078}},
                {"name": "profile_views", "total_value": {"value": 42}}]}

        conn = InstagramInsightsConnector(_DS("instagram_insights"), http=http)
        res = conn.query(_spec(["reach", "profile_views"], "AccountTotals"))
        assert res.rows == [{"reach": 8078, "profile_views": 42}]

    def test_media_report_fetches_per_media_insights(self):
        def http(method, url, token, params=None):
            if url.endswith("/media"):
                # The media list carries only plain fields, no nested insights.
                assert "insights" not in params["fields"]
                return {"data": [{"id": "m1", "media_type": "IMAGE",
                                  "like_count": 10, "comments_count": 2}]}
            if url.endswith("/m1/insights"):
                assert params["metric"] == "reach,saved"
                return {"data": [
                    {"name": "reach", "values": [{"value": 300}]},
                    {"name": "saved", "values": [{"value": 7}]}]}
            raise AssertionError(url)

        conn = InstagramInsightsConnector(_DS("instagram_insights"), http=http)
        res = conn.query(_spec(["id", "like_count", "reach", "saved"], "Media"))
        assert res.rows == [
            {"id": "m1", "like_count": 10, "reach": 300, "saved": 7},
        ]

    def test_account_insights_skips_a_failing_metric_not_the_whole_pull(self):
        # A single deprecated/unsupported metric must not sink the report: the
        # connector retries per-metric and keeps what works, with a warning.
        from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode

        def http(method, url, token, params=None):
            metric = params.get("metric", "")
            if "," in metric or metric == "follower_count":
                # The combined call and the bad metric both hard-error.
                raise ApiError(ErrorCode.UPSTREAM_ERROR, "unavailable")
            return {"data": [{"name": metric, "values": [
                {"value": 5, "end_time": "2026-08-01T07:00:00+0000"}]}]}

        conn = InstagramInsightsConnector(_DS("instagram_insights"), http=http)
        res = conn.query(_spec(["date", "reach", "follower_count"],
                               "AccountInsights"))
        # reach survived; follower_count was skipped (null) with a warning.
        assert res.rows == [
            {"date": "2026-08-01", "reach": 5, "follower_count": None},
        ]
        assert any("follower_count" in w for w in res.warnings)

    def test_unknown_field_rejected(self):
        conn = InstagramInsightsConnector(
            _DS("instagram_insights"), http=lambda *a, **k: {"data": []})
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(["date", "not_a_metric"], "AccountInsights"))
        assert exc.value.code == ErrorCode.INVALID_FIELD


# --------------------------------------------------------------------------
# Instagram Public Data
# --------------------------------------------------------------------------

class TestPublic:
    def test_business_discovery_profile(self):
        captured = {}

        def http(method, url, token, params=None):
            captured["params"] = params
            return {"business_discovery": {
                "username": "nasa", "followers_count": 90000000,
                "media_count": 4000}, "id": "178414"}

        conn = InstagramPublicConnector(_DS("instagram_public"), http=http)
        res = conn.query(_spec(
            ["username", "followers_count", "media_count"], "Profile",
            settings={"username": "@nasa"}))
        # @ is stripped; the username is threaded into business_discovery(...).
        assert "business_discovery.username(nasa)" in captured["params"]["fields"]
        assert res.rows == [{"username": "nasa", "followers_count": 90000000,
                             "media_count": 4000}]

    def test_business_discovery_media(self):
        def http(method, url, token, params=None):
            return {"business_discovery": {"media": {"data": [
                {"id": "p1", "like_count": 5, "comments_count": 1,
                 "timestamp": "2026-08-01T00:00:00+0000"},
                {"id": "p2", "like_count": 9, "comments_count": 3,
                 "timestamp": "2026-08-02T00:00:00+0000"}]}}}

        conn = InstagramPublicConnector(_DS("instagram_public"), http=http)
        res = conn.query(_spec(
            ["id", "like_count"], "Media", settings={"username": "nasa"}))
        assert res.rows == [{"id": "p1", "like_count": 5},
                            {"id": "p2", "like_count": 9}]

    def test_hashtag_two_step_search_then_top_media(self):
        calls = []

        def http(method, url, token, params=None):
            calls.append(url)
            if "ig_hashtag_search" in url:
                assert params["q"] == "coffee"
                return {"data": [{"id": "hash123"}]}
            if "hash123/top_media" in url:
                return {"data": [{"id": "t1", "like_count": 500,
                                  "comments_count": 12}]}
            raise AssertionError(url)

        conn = InstagramPublicConnector(_DS("instagram_public"), http=http)
        res = conn.query(_spec(
            ["id", "like_count"], "Hashtag", settings={"hashtag": "#coffee"}))
        assert any("ig_hashtag_search" in u for u in calls)
        assert any("hash123/top_media" in u for u in calls)
        assert res.rows == [{"id": "t1", "like_count": 500}]

    def test_missing_username_setting_is_actionable(self):
        conn = InstagramPublicConnector(
            _DS("instagram_public"), http=lambda *a, **k: {})
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(["username"], "Profile", settings={}))
        assert exc.value.code == ErrorCode.MISSING_SETTING


# --------------------------------------------------------------------------
# Instagram Login flavour (Insights only)
# --------------------------------------------------------------------------

class TestInstagramLogin:
    def test_discovery_uses_graph_instagram_me(self):
        def http(method, url, token, params=None):
            assert url == "https://graph.instagram.com/me"      # not /me/accounts
            return {"user_id": "178414", "username": "cloudxlab"}

        conn = InstagramInsightsConnector(
            _DS("instagram_insights", login_method="instagram"), http=http)
        accts = conn.list_accounts()
        assert [a.id for a in accts] == ["178414"]
        assert accts[0].name == "@cloudxlab"

    def test_insights_calls_graph_instagram_host(self):
        captured = {}

        def http(method, url, token, params=None):
            captured["url"] = url
            return {"data": [{"name": "reach", "values": [
                {"value": 42, "end_time": "2026-08-01T07:00:00+0000"}]}]}

        conn = InstagramInsightsConnector(
            _DS("instagram_insights", login_method="instagram"), http=http)
        res = conn.query(_spec(["date", "reach"], "AccountInsights"))
        assert captured["url"] == "https://graph.instagram.com/178414/insights"
        assert res.rows == [{"date": "2026-08-01", "reach": 42}]

    def test_facebook_login_still_uses_graph_facebook_host(self):
        captured = {}

        def http(method, url, token, params=None):
            captured["url"] = url
            return {"data": []}

        conn = InstagramInsightsConnector(_DS("instagram_insights"), http=http)
        conn.query(_spec(["date", "reach"], "AccountInsights"))
        assert captured["url"].startswith("https://graph.facebook.com/")


class TestProviderResolution:
    def test_login_method_selects_instagram_login_provider(self):
        from terno_dbi.connectors.api.auth.providers import (
            get_provider, login_methods,
        )
        default = get_provider("instagram_insights")
        fb = get_provider("instagram_insights", "facebook")
        ig_login = get_provider("instagram_insights", "instagram")
        assert default.name == "meta"                     # Facebook Login default
        assert fb.name == "meta"
        assert ig_login.name == "instagram_login"
        assert ig_login.authorization_url.startswith("https://www.instagram.com/")
        assert login_methods("instagram_insights") == ["instagram", "facebook"]
        # Public Data offers a single login path.
        assert login_methods("instagram_public") == []
        # An unknown method falls back to the default provider.
        assert get_provider("instagram_insights", "nonsense").name == "meta"
