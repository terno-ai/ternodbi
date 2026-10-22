"""The X Analytics connector, against mocked X API v2 responses.

Exercises the account's one-row discovery, the Posts and Mentions timelines
(date range to UTC instants, paging, the 30-day split for owner-only metrics,
reposts left out, media and author expansions), the Profile snapshot, and how
X's refusals map to error codes.
"""

from datetime import datetime, timezone

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
from terno_dbi.connectors.api.sources import x_analytics
from terno_dbi.connectors.api.sources.x_analytics import XAnalyticsConnector

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


class _Catalog:
    key = "x_analytics"
    report_types = [
        {"id": "Posts", "settings": []},
        {"id": "Mentions", "settings": []},
        {"id": "Profile", "settings": []},
    ]
    has_report_types = True


class _DS:
    type = "x_analytics"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok"}


class _Recorder:
    """Answers each call from `respond(path, params)` and records it."""

    def __init__(self, respond):
        self.respond = respond
        self.calls = []

    def __call__(self, method, url, token, params=None):
        assert method == "GET"
        assert token == "tok"
        path = url.split("https://api.x.com/2/", 1)[1]
        self.calls.append((path, dict(params or {})))
        return self.respond(path, params or {})


@pytest.fixture(autouse=True)
def frozen_now(monkeypatch):
    monkeypatch.setattr(x_analytics, "_now", lambda: NOW)


def _connector(respond):
    rec = _Recorder(respond)
    return XAnalyticsConnector(_DS(), http=rec), rec


def _spec(fields, report_type="Posts", start="2026-09-01", end="2026-09-07",
          accounts=("42",), max_rows=1000):
    return QuerySpec(
        accounts=list(accounts), fields=list(fields),
        date_range=DateRange(start, end), report_type=report_type,
        max_rows=max_rows,
    )


def _post(pid, created="2026-09-02T10:00:00.000Z", **extra):
    return {"id": pid, "created_at": created, "text": f"post {pid}",
            "author_id": "42",
            "public_metrics": {"impression_count": 100, "like_count": 5,
                               "retweet_count": 2, "reply_count": 1,
                               "quote_count": 0, "bookmark_count": 3},
            **extra}


_ME = {"data": {"id": "42", "username": "acme", "name": "Acme",
                "public_metrics": {"followers_count": 1200,
                                   "following_count": 80, "tweet_count": 950,
                                   "listed_count": 12, "like_count": 300,
                                   "media_count": 40},
                "verified": False, "created_at": "2015-01-01T00:00:00.000Z"}}


class TestListAccounts:
    def test_the_connected_user_is_the_one_account(self):
        conn, rec = _connector(lambda path, params: _ME)
        accounts = conn.list_accounts()
        assert rec.calls[0][0] == "users/me"
        assert "public_metrics" in rec.calls[0][1]["user.fields"]
        assert [(a.id, a.name) for a in accounts] == [("42", "Acme (@acme)")]
        assert accounts[0].as_dict()["username"] == "acme"

    def test_no_user_lists_nothing(self):
        conn, _ = _connector(lambda path, params: {"data": {}})
        assert conn.list_accounts() == []


class TestPosts:
    def test_range_becomes_utc_instants_and_reposts_are_left_out(self):
        conn, rec = _connector(lambda path, params: {"data": [_post("1")],
                                                     "meta": {}})
        result = conn.query(_spec(["post_id", "date", "impressions", "likes"]))
        path, params = rec.calls[0]
        assert path == "users/42/tweets"
        assert params["start_time"] == "2026-09-01T00:00:00Z"
        # The range's last day is included in full.
        assert params["end_time"] == "2026-09-08T00:00:00Z"
        assert params["exclude"] == "retweets"
        assert params["max_results"] == 100
        assert "non_public_metrics" not in params["tweet.fields"]
        assert result.rows == [{"post_id": "1", "date": "2026-09-02",
                                "impressions": 100, "likes": 5}]
        assert any("Reposts are not included" in n for n in result.notes)

    def test_end_is_capped_just_before_now(self):
        conn, rec = _connector(lambda path, params: {"meta": {}})
        conn.query(_spec(["post_id"], start="2026-09-29", end="2026-09-30"))
        assert rec.calls[0][1]["end_time"] == "2026-09-30T11:59:30Z"

    def test_a_range_wholly_in_the_future_asks_nothing(self):
        conn, rec = _connector(lambda path, params: pytest.fail("no call"))
        result = conn.query(_spec(["post_id"], start="2026-10-05", end="2026-10-06"))
        assert result.rows == [] and rec.calls == []

    def test_pages_until_max_rows(self):
        def respond(path, params):
            if "pagination_token" not in params:
                return {"data": [_post("1"), _post("2")],
                        "meta": {"next_token": "p2"}}
            return {"data": [_post("3"), _post("4")], "meta": {"next_token": "p3"}}

        conn, rec = _connector(respond)
        result = conn.query(_spec(["post_id"], max_rows=3))
        assert [r["post_id"] for r in result.rows] == ["1", "2", "3"]
        assert len(rec.calls) == 2
        assert rec.calls[1][1]["pagination_token"] == "p2"

    def test_post_type_url_and_text(self):
        reply = _post("2", referenced_tweets=[{"type": "replied_to", "id": "9"}])
        quote = _post("3", referenced_tweets=[{"type": "quoted", "id": "8"}])
        conn, _ = _connector(lambda path, params: {
            "data": [_post("1"), reply, quote]})
        result = conn.query(_spec(["post_id", "post_type", "url", "text"]))
        assert [r["post_type"] for r in result.rows] == ["post", "reply", "quote"]
        assert result.rows[0]["url"] == "https://x.com/i/web/status/1"
        assert result.rows[0]["text"] == "post 1"

    def test_video_views_sum_the_posts_media(self):
        with_video = _post("1", attachments={"media_keys": ["7_1", "3_2"]})

        def respond(path, params):
            assert params["expansions"] == "attachments.media_keys"
            assert params["media.fields"] == "type,public_metrics"
            assert "attachments" in params["tweet.fields"]
            return {"data": [with_video, _post("2")], "includes": {"media": [
                {"media_key": "7_1", "type": "video",
                 "public_metrics": {"view_count": 400}},
                {"media_key": "3_2", "type": "photo", "public_metrics": {}},
            ]}}

        conn, _ = _connector(respond)
        result = conn.query(_spec(["post_id", "video_views"]))
        assert [r["video_views"] for r in result.rows] == [400, None]

    def test_owner_metrics_split_the_range_at_30_days(self):
        def respond(path, params):
            if "non_public_metrics" in params["tweet.fields"]:
                return {"data": [_post(
                    "new", created="2026-09-20T09:00:00.000Z",
                    non_public_metrics={"url_link_clicks": 7,
                                        "user_profile_clicks": 4},
                    organic_metrics={"impression_count": 90})]}
            return {"data": [_post("old", created="2026-08-15T09:00:00.000Z")]}

        conn, rec = _connector(respond)
        result = conn.query(_spec(
            ["post_id", "url_link_clicks", "profile_clicks",
             "organic_impressions"], start="2026-08-01", end="2026-09-29"))
        recent, older = rec.calls[0][1], rec.calls[1][1]
        # Recent posts first (newest first), with the owner-only metrics.
        assert "organic_metrics" in recent["tweet.fields"]
        assert recent["start_time"] == "2026-08-31T13:00:00Z"
        assert recent["end_time"] == "2026-09-30T00:00:00Z"
        assert older["start_time"] == "2026-08-01T00:00:00Z"
        assert older["end_time"] == "2026-08-31T13:00:00Z"
        assert "non_public_metrics" not in older["tweet.fields"]
        assert result.rows == [
            {"post_id": "new", "url_link_clicks": 7, "profile_clicks": 4,
             "organic_impressions": 90},
            {"post_id": "old", "url_link_clicks": None, "profile_clicks": None,
             "organic_impressions": None},
        ]
        assert any("last 30 days" in n for n in result.notes)

    def test_engagements_are_an_owner_metric(self):
        conn, rec = _connector(lambda path, params: {"data": [_post(
            "1", non_public_metrics={"engagements": 31})]})
        result = conn.query(_spec(["post_id", "engagements"],
                                  start="2026-09-20", end="2026-09-25"))
        assert "non_public_metrics" in rec.calls[0][1]["tweet.fields"]
        assert result.rows == [{"post_id": "1", "engagements": 31}]

    def test_renamed_repost_and_post_counts_are_read(self):
        renamed = _post("1")
        renamed["public_metrics"] = {"repost_count": 6, "like_count": 1}
        conn, _ = _connector(lambda path, params: {"data": [renamed]})
        result = conn.query(_spec(["reposts", "likes"]))
        assert result.rows == [{"reposts": 6, "likes": 1}]

        me = {"data": dict(_ME["data"], public_metrics={"post_count": 951})}
        conn, _ = _connector(lambda path, params: me)
        result = conn.query(_spec(["posts"], report_type="Profile"))
        assert result.rows == [{"posts": 951}]

    def test_a_recent_range_needs_one_request(self):
        conn, rec = _connector(lambda path, params: {"data": []})
        result = conn.query(_spec(["post_id", "url_link_clicks"],
                                  start="2026-09-20", end="2026-09-25"))
        assert len(rec.calls) == 1
        assert "non_public_metrics" in rec.calls[0][1]["tweet.fields"]
        assert not any("last 30 days" in n for n in result.notes)

    def test_partial_errors_become_a_warning(self):
        conn, _ = _connector(lambda path, params: {
            "data": [_post("1")],
            "errors": [{"title": "Authorization Error",
                        "detail": "Sorry, you are not authorized to see the "
                                  "field non_public_metrics."}]})
        result = conn.query(_spec(["post_id"]))
        assert result.rows == [{"post_id": "1"}]
        assert "not authorized" in result.warnings[0]

    def test_unknown_field_is_refused_with_a_suggestion(self):
        conn, _ = _connector(lambda path, params: pytest.fail("no call"))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(["impresions"]))
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "impressions" in exc.value.message


class TestMentions:
    def test_mentions_carry_the_author(self):
        def respond(path, params):
            assert path == "users/42/mentions"
            assert params["expansions"] == "author_id"
            assert "exclude" not in params
            return {"data": [dict(_post("5"), author_id="77")],
                    "includes": {"users": [
                        {"id": "77", "username": "fan", "name": "A Fan",
                         "public_metrics": {"followers_count": 15}}]}}

        conn, _ = _connector(respond)
        result = conn.query(_spec(
            ["post_id", "author_username", "author_name", "author_followers",
             "likes"], report_type="Mentions"))
        assert result.rows == [{"post_id": "5", "author_username": "fan",
                                "author_name": "A Fan", "author_followers": 15,
                                "likes": 5}]

    def test_owner_metrics_are_not_offered_on_mentions(self):
        conn, _ = _connector(lambda path, params: pytest.fail("no call"))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(["url_link_clicks"], report_type="Mentions"))
        assert exc.value.code == ErrorCode.INVALID_FIELD


class TestProfile:
    def test_counts_are_read_from_public_metrics(self):
        conn, _ = _connector(lambda path, params: _ME)
        result = conn.query(_spec(
            ["username", "followers", "following", "posts", "media", "verified"],
            report_type="Profile"))
        assert result.rows == [{"username": "acme", "followers": 1200,
                                "following": 80, "posts": 950, "media": 40,
                                "verified": False}]

    def test_defaults_without_fields(self):
        conn, _ = _connector(lambda path, params: _ME)
        result = conn.query(_spec([], report_type="Profile"))
        assert result.requested_field_ids == ["username", "followers",
                                              "following", "posts"]

    def test_another_account_id_is_refused(self):
        conn, _ = _connector(lambda path, params: _ME)
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(["followers"], report_type="Profile",
                             accounts=["99"]))
        assert exc.value.code == ErrorCode.ACCOUNT_FORBIDDEN


class TestTransport:
    def _resp(self, status, body=None, headers=None):
        class _Resp:
            status_code = status
            text = ""

            def json(self):
                return body or {}
        r = _Resp()
        r.headers = headers or {}
        return r

    def _get(self, monkeypatch, resp):
        import requests
        seen = {}

        def fake_request(method, url, headers=None, params=None, json=None,
                         timeout=None):
            seen.update(headers=headers, params=params)
            if json is not None:
                seen["json"] = json
            return resp

        monkeypatch.setattr(requests, "request", fake_request)
        return seen

    def test_sends_the_bearer_token(self, monkeypatch):
        seen = self._get(monkeypatch, self._resp(200, {"data": {}}))
        x_analytics._default_http("GET", "https://api.x.com/2/users/me", "tok",
                                  {"a": "1"})
        assert seen == {"headers": {"Authorization": "Bearer tok"},
                        "params": {"a": "1"}}

    def test_401_is_auth_expired(self, monkeypatch):
        self._get(monkeypatch, self._resp(401))
        conn = XAnalyticsConnector(_DS())
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED

    def test_429_waits_for_the_reset(self, monkeypatch):
        reset = int(datetime.now(timezone.utc).timestamp()) + 120
        self._get(monkeypatch, self._resp(
            429, {"title": "Too Many Requests"},
            {"x-rate-limit-reset": str(reset)}))
        with pytest.raises(ApiError) as exc:
            x_analytics._default_http("GET", "https://api.x.com/2/users/me", "t")
        assert exc.value.code == ErrorCode.RATE_LIMITED
        assert 100 <= exc.value.retry_after_seconds <= 120

    @pytest.mark.parametrize("status,code", [
        (402, ErrorCode.QUOTA_EXCEEDED), (403, ErrorCode.UPSTREAM_ERROR)])
    def test_plan_refusals_are_not_retried(self, monkeypatch, status, code):
        self._get(monkeypatch, self._resp(status, {
            "title": "Client Forbidden",
            "detail": "This request must be made using an approved developer "
                      "account."}))
        with pytest.raises(ApiError) as exc:
            x_analytics._default_http("GET", "https://api.x.com/2/users/me", "t")
        assert exc.value.code == code
        assert exc.value.retriable is False
        assert "approved developer account" in exc.value.message

    def test_server_errors_are_retriable(self, monkeypatch):
        self._get(monkeypatch, self._resp(503))
        with pytest.raises(ApiError) as exc:
            x_analytics._default_http("GET", "https://api.x.com/2/users/me", "t")
        assert exc.value.code == ErrorCode.UPSTREAM_ERROR
        assert exc.value.retriable is True


class _Writer:
    """Answers reads and writes from `respond(method, path, params, body)`."""

    def __init__(self, respond=None):
        self.respond = respond or (lambda method, path, params, body: {"data": {}})
        self.calls = []

    def __call__(self, method, url, token, params=None, body=None):
        assert token == "tok"
        path = url.split("https://api.x.com/2/", 1)[1]
        self.calls.append((method, path, body))
        return self.respond(method, path, params or {}, body)


def _writer(respond=None, scopes=None):
    class _Scoped(_DS):
        connection_json = {"ACCESS_TOKEN": "tok",
                           **({"GRANTED_SCOPES": scopes} if scopes else {})}

    rec = _Writer(respond)
    return XAnalyticsConnector(_Scoped(), http=rec), rec


class TestWriteActions:
    def test_lists_the_actions(self):
        conn, _ = _writer()
        assert [a.id for a in conn.list_actions()] == [
            "create_post", "reply_to_post", "delete_post", "like_post",
            "unlike_post", "hide_reply"]
        undoable = {a.id for a in conn.list_actions() if not a.destructive}
        assert undoable == {"like_post", "unlike_post", "hide_reply"}

    def test_create_post(self):
        conn, rec = _writer(lambda m, p, q, b: {"data": {"id": "77", "text": b["text"]}})
        result = conn.execute_action("create_post", "42", {"text": "Hello"})
        assert rec.calls == [("POST", "tweets", {"text": "Hello"})]
        assert result.after == {"post_id": "77", "text": "Hello",
                                "url": "https://x.com/i/web/status/77"}

    def test_reply_names_the_post(self):
        conn, rec = _writer(lambda m, p, q, b: {"data": {"id": "78", "text": "hi"}})
        result = conn.execute_action("reply_to_post", "42",
                                     {"post_id": "9", "text": "hi"})
        assert rec.calls == [("POST", "tweets", {
            "text": "hi", "reply": {"in_reply_to_tweet_id": "9"}})]
        assert result.after["in_reply_to"] == "9"

    def test_delete_reads_the_post_first(self):
        def respond(method, path, params, body):
            if method == "GET":
                assert params == {"tweet.fields": "author_id,created_at"}
                return {"data": {"id": "5", "author_id": "42", "text": "old",
                                 "created_at": "2026-09-01T00:00:00.000Z"}}
            return {"data": {"deleted": True}}

        conn, rec = _writer(respond)
        result = conn.execute_action("delete_post", "42", {"post_id": "5"})
        assert [(m, p) for m, p, _ in rec.calls] == [("GET", "tweets/5"),
                                                     ("DELETE", "tweets/5")]
        assert result.before == {"post_id": "5", "text": "old",
                                 "created_at": "2026-09-01T00:00:00.000Z"}
        assert result.after == {"post_id": "5", "deleted": True}

    def test_delete_refuses_another_accounts_post(self):
        conn, rec = _writer(lambda m, p, q, b: {"data": {"id": "5", "author_id": "99"}})
        with pytest.raises(ApiError) as exc:
            conn.execute_action("delete_post", "42", {"post_id": "5"})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert [m for m, _, _ in rec.calls] == ["GET"]

    def test_delete_of_a_missing_post_is_refused(self):
        conn, _ = _writer(lambda m, p, q, b: {"errors": [
            {"title": "Not Found Error", "resource_id": "5"}]})
        with pytest.raises(ApiError) as exc:
            conn.execute_action("delete_post", "42", {"post_id": "5"})
        assert "not found" in exc.value.message

    def test_like_and_unlike_act_as_the_account(self):
        conn, rec = _writer()
        conn.execute_action("like_post", "42", {"post_id": "9"})
        result = conn.execute_action("unlike_post", "42", {"post_id": 9})
        assert rec.calls == [("POST", "users/42/likes", {"tweet_id": "9"}),
                             ("DELETE", "users/42/likes/9", None)]
        assert result.after == {"post_id": "9", "liked": False}

    def test_hide_and_unhide_a_reply(self):
        conn, rec = _writer()
        conn.execute_action("hide_reply", "42", {"reply_id": "6"})
        result = conn.execute_action("hide_reply", "42",
                                     {"reply_id": "6", "hidden": False})
        assert rec.calls == [("PUT", "tweets/6/hidden", {"hidden": True}),
                             ("PUT", "tweets/6/hidden", {"hidden": False})]
        assert result.summary == "Unhid reply 6."
        with pytest.raises(ApiError) as exc:
            conn.execute_action("hide_reply", "42", {"reply_id": "6", "hidden": "no"})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS

    @pytest.mark.parametrize("action,params", [
        ("create_post", {"text": "Hello"}),
        ("reply_to_post", {"post_id": "9", "text": "hi"}),
        ("like_post", {"post_id": "9"}),
        ("unlike_post", {"post_id": "9"}),
        ("hide_reply", {"reply_id": "6"}),
    ])
    def test_dry_run_changes_nothing(self, action, params):
        conn, rec = _writer()
        result = conn.execute_action(action, "42", params, dry_run=True)
        assert rec.calls == []
        assert result.summary.startswith("Would ")
        assert result.details == {"dry_run": True, "applied": False}

    def test_dry_run_delete_still_checks_the_post(self):
        conn, rec = _writer(lambda m, p, q, b: {"data": {"id": "5", "author_id": "42",
                                                         "text": "old"}})
        result = conn.execute_action("delete_post", "42", {"post_id": "5"},
                                     dry_run=True)
        assert [m for m, _, _ in rec.calls] == ["GET"]
        assert result.before["text"] == "old"
        assert result.details["applied"] is False

    def test_a_read_only_connection_must_reconnect(self):
        conn, rec = _writer(scopes="tweet.read users.read offline.access")
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_post", "42", {"text": "Hello"})
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert "tweet.write" in exc.value.message
        assert rec.calls == []

    def test_each_action_needs_only_its_own_scope(self):
        conn, rec = _writer(scopes="tweet.read users.read like.write")
        conn.execute_action("like_post", "42", {"post_id": "9"})
        with pytest.raises(ApiError):
            conn.execute_action("hide_reply", "42", {"reply_id": "6"})
        assert len(rec.calls) == 1

    @pytest.mark.parametrize("params", [{}, {"post_id": "abc"}, {"post_id": ""},
                                        {"post_id": True}])
    def test_bad_post_ids_are_refused(self, params):
        conn, rec = _writer()
        with pytest.raises(ApiError) as exc:
            conn.execute_action("like_post", "42", params)
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert rec.calls == []

    def test_empty_text_is_refused(self):
        conn, _ = _writer()
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_post", "42", {"text": "  "})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS

    def test_unknown_action(self):
        conn, _ = _writer()
        with pytest.raises(ApiError) as exc:
            conn.execute_action("follow_user", "42", {})
        assert exc.value.code == ErrorCode.UNKNOWN_ACTION


class TestWriteTransport:
    def _capture(self, monkeypatch, status, body):
        import requests
        seen = {}

        class _Resp:
            status_code = status
            text = ""
            headers = {}

            def json(self):
                return body

        def fake_request(method, url, headers=None, params=None, json=None,
                         timeout=None):
            seen.update(method=method, url=url, json=json)
            return _Resp()

        monkeypatch.setattr(requests, "request", fake_request)
        return seen

    def test_a_write_sends_its_body_as_json(self, monkeypatch):
        seen = self._capture(monkeypatch, 201, {"data": {"id": "77"}})
        x_analytics._default_http("POST", "https://api.x.com/2/tweets", "t",
                                  None, {"text": "Hello"})
        assert seen == {"method": "POST", "url": "https://api.x.com/2/tweets",
                        "json": {"text": "Hello"}}

    def test_a_refused_write_carries_xs_reason(self, monkeypatch):
        self._capture(monkeypatch, 403, {
            "title": "Forbidden",
            "detail": "You are not allowed to create a Tweet with duplicate content."})
        with pytest.raises(ApiError) as exc:
            x_analytics._default_http("POST", "https://api.x.com/2/tweets", "t",
                                      None, {"text": "Hello"})
        assert exc.value.retriable is False
        assert exc.value.message == ("X refused the change: You are not allowed "
                                     "to create a Tweet with duplicate content.")
