"""The X Ads connector, against mocked Ads API (v12) responses.

Exercises the stats path without a live app — active entities, 20-id batches,
7-day windows aligned to the account's midnight, DAY/HOUR/TOTAL series folded
into rows, names and parents from the entity lists, campaign-level reads summed
when no entity column is asked — plus the entity listings, the OAuth 1.0a
signing on the wire, and how X's refusals map to error codes.
"""

from urllib.parse import parse_qsl, unquote, urlsplit

import pytest

from terno_dbi.connectors.api.auth import oauth1
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
from terno_dbi.connectors.api.sources import x_ads
from terno_dbi.connectors.api.sources.x_ads import XAdsConnector

BASE = "https://ads-api.x.com/12/"


class _Catalog:
    key = "x_ads"
    report_types = [
        {"id": "Performance", "settings": [
            {"setting_id": "placement", "required": False}]},
        {"id": "Campaigns", "settings": []},
        {"id": "LineItems", "settings": []},
        {"id": "PromotedTweets", "settings": []},
    ]
    has_report_types = True


class _DS:
    type = "x_ads"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok", "ACCESS_TOKEN_SECRET": "tok-secret"}


@pytest.fixture(autouse=True)
def consumer_keys(monkeypatch):
    monkeypatch.setenv("TERNO_X_CONSUMER_KEY", "ck")
    monkeypatch.setenv("TERNO_X_CONSUMER_SECRET", "cs")


class _Recorder:
    """Answers each call from `respond(path, params)` and records it."""

    def __init__(self, respond):
        self.respond = respond
        self.calls = []

    def __call__(self, method, url, params, creds):
        assert method == "GET"
        assert creds == x_ads.Credentials("ck", "cs", "tok", "tok-secret")
        path = url.split(BASE, 1)[1]
        self.calls.append((path, dict(params)))
        return self.respond(path, params)

    def paths(self, prefix):
        return [(p, q) for p, q in self.calls if p.startswith(prefix)]


def _connector(respond):
    rec = _Recorder(respond)
    return XAdsConnector(_DS(), http=rec), rec


def _spec(fields, report_type="Performance", start="2026-08-01",
          end="2026-08-07", accounts=("18ce54d4x5t",), settings=None,
          max_rows=1000):
    return QuerySpec(
        accounts=list(accounts), fields=list(fields),
        date_range=DateRange(start, end), report_type=report_type,
        settings=settings or {}, max_rows=max_rows,
    )


def _stats(by_id):
    """A stats response: entity id -> {metric key: series}."""
    return {"data_type": "stats", "data": [
        {"id": eid, "id_data": [{"segment": None, "metrics": metrics}]}
        for eid, metrics in by_id.items()]}


def _active(*ids, placements=("ALL_ON_TWITTER",), start=None, end=None):
    return {"data": [{"entity_id": eid,
                      "activity_start_time": start, "activity_end_time": end,
                      "placements": list(placements)} for eid in ids]}


def _respond(stats=None, active=None, tz="America/Los_Angeles", lists=None):
    """A respond() for the stats path."""
    lists = lists or {}

    def respond(path, params):
        if path == "accounts/18ce54d4x5t":
            return {"data": {"id": "18ce54d4x5t", "timezone": tz}}
        if path.endswith("/active_entities"):
            return active if active is not None else _active("c1")
        if path.startswith("stats/accounts/"):
            return stats(params) if callable(stats) else stats
        for name, items in lists.items():
            if path.endswith(f"/{name}"):
                return {"data": items, "next_cursor": None}
        raise AssertionError(f"unexpected call {path}")
    return respond


class TestListAccounts:
    def test_lists_accounts_with_currency_and_timezone(self):
        def respond(path, params):
            if path == "accounts":
                assert params["count"] == 1000
                if "cursor" not in params:
                    return {"data": [
                        {"id": "a1", "name": "Acme", "timezone": "Europe/London"},
                        {"id": "gone", "name": "Old", "deleted": True},
                    ], "next_cursor": "c2"}
                return {"data": [{"id": "a2", "business_name": "Beta Ltd"}],
                        "next_cursor": None}
            if path == "accounts/a1/funding_instruments":
                return {"data": [{"id": "fi", "currency": "GBP"}]}
            return {"data": []}

        conn, _ = _connector(respond)
        accounts = conn.list_accounts()
        assert [(a.id, a.name, a.currency, a.timezone) for a in accounts] == [
            ("a1", "Acme", "GBP", "Europe/London"),
            ("a2", "Beta Ltd", None, None),
        ]

    def test_a_currency_failure_still_lists_the_account(self):
        def respond(path, params):
            if path == "accounts":
                return {"data": [{"id": "a1", "name": "Acme"}]}
            raise ApiError(ErrorCode.ACCOUNT_FORBIDDEN, "no")

        conn, _ = _connector(respond)
        assert [(a.id, a.currency) for a in conn.list_accounts()] == [("a1", None)]

    def test_unconfigured_consumer_keys_are_reported(self, monkeypatch):
        monkeypatch.delenv("TERNO_X_CONSUMER_KEY")
        conn, _ = _connector(lambda path, params: pytest.fail("no call"))
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert "TERNO_X_CONSUMER_KEY" in exc.value.message

    def test_a_missing_token_secret_needs_a_reconnect(self):
        class _NoSecret(_DS):
            connection_json = {"ACCESS_TOKEN": "tok"}

        conn = XAdsConnector(_NoSecret(), http=lambda *a: pytest.fail("no call"))
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestPerformance:
    def test_daily_rows_per_campaign_with_names(self):
        stats = _stats({"c1": {"impressions": [100, None, 40, 0, 0, 0, 0],
                               "billed_charge_local_micro": [2500000, None,
                                                             1000000, 0, 0, 0, 0]}})
        conn, rec = _connector(_respond(stats=stats, lists={"campaigns": [
            {"id": "c1", "name": "Launch"}]}))
        result = conn.query(_spec(
            ["date", "campaign_id", "campaign_name", "impressions", "spend"]))

        _, active = rec.paths("stats/accounts/18ce54d4x5t/active_entities")[0]
        assert active["entity"] == "CAMPAIGN"
        # Midnight in Los Angeles (PDT, UTC-7), end exclusive.
        assert active["start_time"] == "2026-08-01T07:00:00Z"
        assert active["end_time"] == "2026-08-08T07:00:00Z"
        _, params = [c for c in rec.calls if c[0] == "stats/accounts/18ce54d4x5t"][0]
        assert params == {
            "entity": "CAMPAIGN", "entity_ids": "c1",
            "start_time": "2026-08-01T07:00:00Z",
            "end_time": "2026-08-08T07:00:00Z", "granularity": "DAY",
            "metric_groups": "BILLING,ENGAGEMENT", "placement": "ALL_ON_TWITTER"}
        _, names = rec.paths("accounts/18ce54d4x5t/campaigns")[0]
        assert names["with_deleted"] == "true"
        # The day it did not serve (all null) is left out; zeros are kept.
        assert [r["date"] for r in result.rows] == [
            "2026-08-01", "2026-08-03", "2026-08-04", "2026-08-05",
            "2026-08-06", "2026-08-07"]
        assert result.rows[0] == {"date": "2026-08-01", "campaign_id": "c1",
                                  "campaign_name": "Launch", "impressions": 100,
                                  "spend": 2.5}

    def test_no_entity_column_sums_the_campaigns(self):
        stats = _stats({"c1": {"impressions": [100], "clicks": [3]},
                        "c2": {"impressions": [50], "clicks": None}})
        conn, rec = _connector(_respond(stats=stats, active=_active("c1", "c2")))
        result = conn.query(_spec(["impressions", "clicks"]))
        _, params = [c for c in rec.calls if c[0] == "stats/accounts/18ce54d4x5t"][0]
        assert params["entity"] == "CAMPAIGN"
        assert params["granularity"] == "TOTAL"
        assert result.rows == [{"impressions": 150, "clicks": 3}]
        # No entity column: names are never fetched.
        assert not rec.paths("accounts/18ce54d4x5t/campaigns")

    def test_long_ranges_are_read_in_7_day_windows_and_totals_summed(self):
        stats = lambda params: _stats({"c1": {"impressions": [10]}})   # noqa: E731
        conn, rec = _connector(_respond(stats=stats, tz="UTC"))
        result = conn.query(_spec(["campaign_id", "impressions"],
                                  start="2026-08-01", end="2026-08-20"))
        windows = [(q["start_time"], q["end_time"]) for p, q in rec.calls
                   if p == "stats/accounts/18ce54d4x5t"]
        assert windows == [
            ("2026-08-01T00:00:00Z", "2026-08-08T00:00:00Z"),
            ("2026-08-08T00:00:00Z", "2026-08-15T00:00:00Z"),
            ("2026-08-15T00:00:00Z", "2026-08-21T00:00:00Z"),
        ]
        assert result.rows == [{"campaign_id": "c1", "impressions": 30}]

    def test_ids_are_batched_by_20_and_filtered_by_activity(self):
        ids = [f"c{i}" for i in range(25)]
        active = _active(*ids, start="2026-08-01T00:00:00Z",
                         end="2026-08-03T00:00:00Z")
        active["data"].append({"entity_id": "late",
                               "activity_start_time": "2026-08-10T00:00:00Z",
                               "activity_end_time": "2026-08-12T00:00:00Z",
                               "placements": ["ALL_ON_TWITTER"]})
        conn, rec = _connector(_respond(stats=_stats({}), active=active, tz="UTC"))
        conn.query(_spec(["campaign_id", "impressions"],
                         start="2026-08-01", end="2026-08-14"))
        batches = [(q["start_time"][:10], q["entity_ids"].split(","))
                   for p, q in rec.calls if p == "stats/accounts/18ce54d4x5t"]
        assert batches == [("2026-08-01", ids[:20]), ("2026-08-01", ids[20:]),
                           ("2026-08-08", ["late"])]

    def test_entities_on_another_placement_are_skipped(self):
        active = _active("c1", placements=("PUBLISHER_NETWORK",))
        conn, rec = _connector(_respond(stats=_stats({}), active=active))
        result = conn.query(_spec(["campaign_id", "impressions"]))
        assert result.rows == []
        assert not [c for c in rec.calls if c[0] == "stats/accounts/18ce54d4x5t"]

    def test_placement_setting_is_passed_and_validated(self):
        conn, rec = _connector(_respond(
            stats=_stats({}), active=_active("c1", placements=())))
        conn.query(_spec(["impressions"], settings={"placement": "publisher_network"}))
        _, params = [c for c in rec.calls if c[0] == "stats/accounts/18ce54d4x5t"][0]
        assert params["placement"] == "PUBLISHER_NETWORK"
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(["impressions"], settings={"placement": "MOON"}))
        assert exc.value.code == ErrorCode.INVALID_SETTING

    def test_hourly_buckets_are_labelled_in_account_time(self):
        stats = _stats({"c1": {"impressions": [5, 7] + [None] * 166}})
        conn, rec = _connector(_respond(stats=stats, tz="Asia/Kolkata"))
        result = conn.query(_spec(["hour", "impressions"]))
        _, params = [c for c in rec.calls if c[0] == "stats/accounts/18ce54d4x5t"][0]
        assert params["granularity"] == "HOUR"
        assert params["start_time"] == "2026-07-31T18:30:00Z"
        assert result.rows == [{"hour": "2026-08-01T00:00:00", "impressions": 5},
                               {"hour": "2026-08-01T01:00:00", "impressions": 7}]

    def test_promoted_posts_carry_their_line_item_and_campaign(self):
        stats = _stats({"pt1": {"engagements": [9]}})
        conn, rec = _connector(_respond(
            stats=stats, active=_active("pt1"), lists={
                "promoted_tweets": [{"id": "pt1", "line_item_id": "li1",
                                     "tweet_id": "1690"}],
                "line_items": [{"id": "li1", "name": "UK", "campaign_id": "c1"}],
                "campaigns": [{"id": "c1", "name": "Launch"}],
            }))
        result = conn.query(_spec(
            ["tweet_id", "line_item_name", "campaign_name", "engagements"]))
        _, active = rec.paths("stats/accounts/18ce54d4x5t/active_entities")[0]
        assert active["entity"] == "PROMOTED_TWEET"
        assert result.rows == [{"tweet_id": "1690", "line_item_name": "UK",
                                "campaign_name": "Launch", "engagements": 9}]

    def test_line_item_ids_alone_need_no_lists(self):
        conn, rec = _connector(_respond(
            stats=_stats({"li1": {"impressions": [1]}}), active=_active("li1")))
        result = conn.query(_spec(["line_item_id", "impressions"]))
        assert result.rows == [{"line_item_id": "li1", "impressions": 1}]
        assert not [p for p, _ in rec.calls if p.endswith(("/line_items", "/campaigns"))]

    def test_conversions_read_the_metric_series(self):
        stats = _stats({"c1": {"conversion_purchases": {
            "metric": [4], "sale_amount": [120000000], "order_quantity": [5]}}})
        conn, rec = _connector(_respond(stats=stats))
        result = conn.query(_spec(["conversion_purchases"]))
        _, params = [c for c in rec.calls if c[0] == "stats/accounts/18ce54d4x5t"][0]
        assert params["metric_groups"] == "WEB_CONVERSION"
        assert result.rows == [{"conversion_purchases": 4}]

    def test_nothing_active_asks_for_no_stats(self):
        conn, rec = _connector(_respond(stats=_stats({}), active={"data": []}))
        assert conn.query(_spec(["impressions"])).rows == []
        assert not [c for c in rec.calls if c[0] == "stats/accounts/18ce54d4x5t"]

    def test_defaults_to_spend_impressions_engagements(self):
        conn, _ = _connector(_respond(stats=_stats({})))
        result = conn.query(_spec([]))
        assert result.requested_field_ids == ["spend", "impressions", "engagements"]

    def test_date_and_hour_cannot_combine(self):
        conn, _ = _connector(lambda path, params: pytest.fail("no call"))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(["date", "hour", "impressions"]))
        assert exc.value.code == ErrorCode.INVALID_FILTER

    def test_unknown_field_is_refused(self):
        conn, _ = _connector(lambda path, params: pytest.fail("no call"))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(["impresions"]))
        assert exc.value.code == ErrorCode.INVALID_FIELD


class TestEntities:
    def test_campaigns_rename_ids_and_convert_micros(self):
        conn, rec = _connector(lambda path, params: {"data": [
            {"id": "c1", "name": "Launch", "entity_status": "ACTIVE",
             "daily_budget_amount_local_micro": 50000000,
             "total_budget_amount_local_micro": None}], "next_cursor": None})
        result = conn.query(_spec(
            ["campaign_id", "campaign_name", "entity_status", "daily_budget",
             "total_budget"], report_type="Campaigns"))
        assert rec.calls[0][0] == "accounts/18ce54d4x5t/campaigns"
        assert result.rows == [{"campaign_id": "c1", "campaign_name": "Launch",
                                "entity_status": "ACTIVE", "daily_budget": 50.0,
                                "total_budget": None}]

    def test_line_item_placements_are_joined(self):
        conn, _ = _connector(lambda path, params: {"data": [
            {"id": "li1", "placements": ["ALL_ON_TWITTER", "PUBLISHER_NETWORK"],
             "bid_amount_local_micro": 1500000}]})
        result = conn.query(_spec(["line_item_id", "placements", "bid_amount"],
                                  report_type="LineItems"))
        assert result.rows == [{"line_item_id": "li1",
                                "placements": "ALL_ON_TWITTER,PUBLISHER_NETWORK",
                                "bid_amount": 1.5}]

    def test_pages_stop_at_max_rows(self):
        def respond(path, params):
            n = 0 if "cursor" not in params else 2
            return {"data": [{"id": f"pt{n}"}, {"id": f"pt{n + 1}"}],
                    "next_cursor": "more"}

        conn, rec = _connector(respond)
        result = conn.query(_spec(["promoted_tweet_id"],
                                  report_type="PromotedTweets", max_rows=3))
        assert [r["promoted_tweet_id"] for r in result.rows] == ["pt0", "pt1", "pt2"]
        assert len(rec.calls) == 2


class TestTransport:
    def _resp(self, status, body=None, headers=None, text=""):
        class _Resp:
            status_code = status

            def json(self):
                if body is None:
                    raise ValueError("not json")
                return body
        r = _Resp()
        r.headers = headers or {}
        r.text = text
        return r

    def _capture(self, monkeypatch, resp):
        import requests
        seen = {}

        def fake_request(method, url, headers=None, timeout=None):
            seen.update(method=method, url=url, headers=headers)
            return resp

        monkeypatch.setattr(requests, "request", fake_request)
        return seen

    def test_requests_are_signed_exactly_as_sent(self, monkeypatch):
        seen = self._capture(monkeypatch, self._resp(200, {"data": []}))
        creds = x_ads.Credentials("ck", "cs", "tok", "tok-secret")
        x_ads._default_http("GET", f"{BASE}stats/accounts/a1",
                            {"entity_ids": "c1,c2", "metric_groups": "ENGAGEMENT"},
                            creds)
        assert seen["url"] == (f"{BASE}stats/accounts/a1"
                               "?entity_ids=c1%2Cc2&metric_groups=ENGAGEMENT")
        header = seen["headers"]["Authorization"]
        params = {unquote(k): unquote(v.strip('"')) for k, v in
                  (p.split("=", 1) for p in header[len("OAuth "):].split(", "))}
        sig = params.pop("oauth_signature")
        assert params["oauth_token"] == "tok"
        assert params["oauth_consumer_key"] == "ck"
        assert sig == oauth1.signature("GET", seen["url"], params, "cs", "tok-secret")
        # The signed query and the sent query are the same pairs.
        assert dict(parse_qsl(urlsplit(seen["url"]).query)) == {
            "entity_ids": "c1,c2", "metric_groups": "ENGAGEMENT"}

    def test_401_is_auth_expired(self, monkeypatch):
        self._capture(monkeypatch, self._resp(401, {"errors": [
            {"code": "UNAUTHORIZED_ACCESS", "message": "no"}]}))
        conn = XAdsConnector(_DS())
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED

    def test_an_unapproved_app_says_so(self, monkeypatch):
        self._capture(monkeypatch, self._resp(403, {"errors": [
            {"code": "UNAUTHORIZED_CLIENT_APPLICATION",
             "message": "The client application making this request does not "
                        "have access to Twitter Ads API"}]}))
        with pytest.raises(ApiError) as exc:
            x_ads._default_http("GET", f"{BASE}accounts", {},
                                x_ads.Credentials("ck", "cs", "t", "s"))
        assert exc.value.retriable is False
        assert "not approved for the X Ads API" in exc.value.message

    def test_no_role_on_the_account_is_forbidden(self, monkeypatch):
        self._capture(monkeypatch, self._resp(404, {"errors": [
            {"code": "ACCOUNT_NOT_FOUND", "message": "Account not found"}]}))
        with pytest.raises(ApiError) as exc:
            x_ads._default_http("GET", f"{BASE}accounts/a1", {},
                                x_ads.Credentials("ck", "cs", "t", "s"))
        assert exc.value.code == ErrorCode.ACCOUNT_FORBIDDEN

    def test_429_prefers_the_account_reset(self, monkeypatch):
        from datetime import datetime, timezone
        now = int(datetime.now(timezone.utc).timestamp())
        self._capture(monkeypatch, self._resp(429, {"errors": [
            {"code": "TOO_MANY_REQUESTS", "message": "slow down"}]}, headers={
            "x-account-rate-limit-reset": str(now + 60),
            "x-rate-limit-reset": str(now + 900)}))
        with pytest.raises(ApiError) as exc:
            x_ads._default_http("GET", f"{BASE}accounts", {},
                                x_ads.Credentials("ck", "cs", "t", "s"))
        assert exc.value.code == ErrorCode.RATE_LIMITED
        assert 40 <= exc.value.retry_after_seconds <= 60

    def test_html_503_honours_retry_after(self, monkeypatch):
        self._capture(monkeypatch, self._resp(
            503, None, headers={"retry-after": "30"}, text="<html>Over capacity"))
        with pytest.raises(ApiError) as exc:
            x_ads._default_http("GET", f"{BASE}accounts", {},
                                x_ads.Credentials("ck", "cs", "t", "s"))
        assert exc.value.code == ErrorCode.UPSTREAM_ERROR
        assert exc.value.retriable is True
        assert exc.value.retry_after_seconds == 30


class _Writer:
    """Answers GET and PUT from `respond(method, path, params)` and records them."""

    def __init__(self, respond):
        self.respond = respond
        self.calls = []

    def __call__(self, method, url, params, creds):
        assert creds == x_ads.Credentials("ck", "cs", "tok", "tok-secret")
        path = url.split(BASE, 1)[1]
        self.calls.append((method, path, dict(params)))
        return self.respond(method, path, params)


_CAMPAIGN = {"id": "8wku2", "name": "Launch", "entity_status": "ACTIVE",
             "daily_budget_amount_local_micro": 50000000,
             "total_budget_amount_local_micro": None, "currency": "USD"}


def _entity_respond(entity=_CAMPAIGN):
    def respond(method, path, params):
        if method == "GET":
            return {"data": dict(entity)}
        return {"data": {**entity, **{k: (int(v) if k.endswith("_micro") else v)
                                      for k, v in params.items()}}}
    return respond


def _writer(respond):
    rec = _Writer(respond)
    return XAdsConnector(_DS(), http=rec), rec


class TestWriteActions:
    def test_lists_the_actions(self):
        conn, _ = _writer(_entity_respond())
        assert [a.id for a in conn.list_actions()] == [
            "pause_campaign", "resume_campaign", "pause_ad_group",
            "resume_ad_group", "set_campaign_budget"]
        assert all(a.destructive for a in conn.list_actions())

    def test_pause_campaign_reads_then_puts(self):
        conn, rec = _writer(_entity_respond())
        result = conn.execute_action("pause_campaign", "18ce54d4x5t",
                                     {"campaign_id": "8wku2"})
        assert rec.calls == [
            ("GET", "accounts/18ce54d4x5t/campaigns/8wku2", {}),
            ("PUT", "accounts/18ce54d4x5t/campaigns/8wku2",
             {"entity_status": "PAUSED"}),
        ]
        assert result.before == {"id": "8wku2", "name": "Launch",
                                 "entity_status": "ACTIVE"}
        assert result.after["entity_status"] == "PAUSED"
        assert result.summary == "Campaign Launch paused."

    def test_resume_ad_group_targets_the_line_item(self):
        line_item = {"id": "a1b2c", "name": "UK", "entity_status": "PAUSED"}
        conn, rec = _writer(_entity_respond(line_item))
        result = conn.execute_action("resume_ad_group", "18ce54d4x5t",
                                     {"line_item_id": "a1b2c"})
        assert rec.calls[1] == ("PUT", "accounts/18ce54d4x5t/line_items/a1b2c",
                                {"entity_status": "ACTIVE"})
        assert result.summary == "Ad group UK resumed."

    def test_budget_is_sent_in_micros(self):
        conn, rec = _writer(_entity_respond())
        result = conn.execute_action("set_campaign_budget", "18ce54d4x5t", {
            "campaign_id": "8wku2", "daily_budget": 75.5, "total_budget": 1000})
        assert rec.calls[1] == ("PUT", "accounts/18ce54d4x5t/campaigns/8wku2", {
            "daily_budget_amount_local_micro": "75500000",
            "total_budget_amount_local_micro": "1000000000"})
        assert result.before == {"id": "8wku2", "name": "Launch",
                                 "daily_budget": 50.0, "total_budget": None,
                                 "currency": "USD"}
        assert result.after["daily_budget"] == 75.5
        assert result.after["total_budget"] == 1000.0
        assert result.summary == ("Campaign Launch's budget set: daily 75.5 USD, "
                                  "total 1000 USD.")

    def test_budget_can_change_one_figure(self):
        conn, rec = _writer(_entity_respond())
        conn.execute_action("set_campaign_budget", "18ce54d4x5t",
                            {"campaign_id": "8wku2", "total_budget": 300})
        assert rec.calls[1][2] == {"total_budget_amount_local_micro": "300000000"}

    @pytest.mark.parametrize("params", [
        {"campaign_id": "8wku2"},
        {"campaign_id": "8wku2", "daily_budget": 0},
        {"campaign_id": "8wku2", "daily_budget": -5},
        {"campaign_id": "8wku2", "daily_budget": "50"},
        {"campaign_id": "8wku2", "daily_budget": True},
    ])
    def test_bad_budgets_are_refused_before_any_call(self, params):
        conn, rec = _writer(_entity_respond())
        with pytest.raises(ApiError) as exc:
            conn.execute_action("set_campaign_budget", "18ce54d4x5t", params)
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert rec.calls == []

    @pytest.mark.parametrize("action,params", [
        ("pause_campaign", {"campaign_id": "8wku2"}),
        ("resume_campaign", {"campaign_id": "8wku2"}),
        ("pause_ad_group", {"line_item_id": "8wku2"}),
        ("set_campaign_budget", {"campaign_id": "8wku2", "daily_budget": 10}),
    ])
    def test_dry_run_reads_but_never_puts(self, action, params):
        conn, rec = _writer(_entity_respond())
        result = conn.execute_action(action, "18ce54d4x5t", params, dry_run=True)
        assert [m for m, _, _ in rec.calls] == ["GET"]
        assert result.summary.startswith("Would ")
        assert result.details == {"dry_run": True, "applied": False}
        assert result.before["id"] == "8wku2"

    def test_dry_run_budget_describes_the_new_figures(self):
        conn, _ = _writer(_entity_respond())
        result = conn.execute_action("set_campaign_budget", "18ce54d4x5t",
                                     {"campaign_id": "8wku2", "daily_budget": 10},
                                     dry_run=True)
        assert result.summary == "Would set campaign Launch's budget: daily 10 USD."

    def test_an_unknown_campaign_is_a_param_error(self):
        def respond(method, path, params):
            raise ApiError(ErrorCode.ACCOUNT_FORBIDDEN, "no",
                           details={"status": 404, "code": "NOT_FOUND"})

        conn, rec = _writer(respond)
        with pytest.raises(ApiError) as exc:
            conn.execute_action("pause_campaign", "18ce54d4x5t",
                                {"campaign_id": "zzz9"})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "zzz9 was not found" in exc.value.message
        assert [m for m, _, _ in rec.calls] == ["GET"]

    @pytest.mark.parametrize("params", [{}, {"campaign_id": ""},
                                        {"campaign_id": "8wk/u2"},
                                        {"campaign_id": "../x"}])
    def test_bad_ids_are_refused(self, params):
        conn, rec = _writer(_entity_respond())
        with pytest.raises(ApiError) as exc:
            conn.execute_action("pause_campaign", "18ce54d4x5t", params)
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert rec.calls == []

    def test_unknown_action(self):
        conn, _ = _writer(_entity_respond())
        with pytest.raises(ApiError) as exc:
            conn.execute_action("delete_campaign", "18ce54d4x5t", {})
        assert exc.value.code == ErrorCode.UNKNOWN_ACTION


class TestWriteTransport:
    def _capture(self, monkeypatch, status, body):
        import requests
        seen = {}

        class _Resp:
            status_code = status
            headers = {}
            text = ""

            def json(self):
                return body

        def fake_request(method, url, headers=None, timeout=None):
            seen.update(method=method, url=url, headers=headers)
            return _Resp()

        monkeypatch.setattr(requests, "request", fake_request)
        return seen

    def test_a_put_is_signed_with_its_query(self, monkeypatch):
        seen = self._capture(monkeypatch, 200, {"data": {}})
        creds = x_ads.Credentials("ck", "cs", "tok", "tok-secret")
        x_ads._default_http("PUT", f"{BASE}accounts/a1/campaigns/c1",
                            {"entity_status": "PAUSED"}, creds)
        assert seen["method"] == "PUT"
        assert seen["url"] == f"{BASE}accounts/a1/campaigns/c1?entity_status=PAUSED"
        header = seen["headers"]["Authorization"]
        params = {unquote(k): unquote(v.strip('"')) for k, v in
                  (p.split("=", 1) for p in header[len("OAuth "):].split(", "))}
        sig = params.pop("oauth_signature")
        assert sig == oauth1.signature("PUT", seen["url"], params, "cs", "tok-secret")

    def test_a_refused_change_explains_the_role_and_permission(self, monkeypatch):
        self._capture(monkeypatch, 403, {"errors": [
            {"code": "UNAUTHORIZED_ACCESS", "message": "Read-only application "
                                                       "cannot POST."}]})
        with pytest.raises(ApiError) as exc:
            x_ads._default_http("PUT", f"{BASE}accounts/a1/campaigns/c1",
                                {"entity_status": "PAUSED"},
                                x_ads.Credentials("ck", "cs", "t", "s"))
        assert exc.value.code == ErrorCode.ACCOUNT_FORBIDDEN
        assert "Read and write" in exc.value.message
        assert "Ad manager" in exc.value.message

    def test_a_rejected_value_is_a_param_error(self, monkeypatch):
        self._capture(monkeypatch, 400, {"errors": [
            {"code": "INVALID_PARAMETER",
             "message": "Budget is below the minimum."}]})
        with pytest.raises(ApiError) as exc:
            x_ads._default_http("PUT", f"{BASE}accounts/a1/campaigns/c1",
                                {"daily_budget_amount_local_micro": "1"},
                                x_ads.Credentials("ck", "cs", "t", "s"))
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "Budget is below the minimum" in exc.value.message
