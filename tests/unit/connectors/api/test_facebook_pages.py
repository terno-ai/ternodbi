"""Facebook Pages connector, against mocked Meta Graph API responses.

Covers discovery (managed Pages + per-Page tokens from /me/accounts), each read
report (Pages, Posts, VisitorPosts, PostComments, Ratings), the Page-token
routing for Page-scoped edges, nested author flattening, and the required
`post_id` guard — all without a live provider or app approval.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
from terno_dbi.connectors.api.sources.facebook_pages import (
    FacebookPagesConnector,
)


_REPORT_TYPES = [
    {"id": "Pages", "settings": []},
    {"id": "Posts", "settings": []},
    {"id": "VisitorPosts", "settings": []},
    {"id": "PostComments",
     "settings": [{"setting_id": "post_id", "required": True}]},
    {"id": "Ratings", "settings": []},
]


class _Catalog:
    def __init__(self):
        self.key = "facebook_pages"
        self.report_types = _REPORT_TYPES
        self.has_report_types = True


class _DS:
    def __init__(self):
        self.type = "facebook_pages"
        self.catalog = _Catalog()
        self.connection_json = {"ACCESS_TOKEN": "user-tok"}


# A /me/accounts reply: two managed Pages, each with its own Page token.
ACCOUNTS = {
    "data": [
        {"id": "100", "name": "CloudXLab", "category": "Education",
         "followers_count": 5000, "fan_count": 4800,
         "link": "https://facebook.com/cloudxlab", "about": "Learn big data",
         "access_token": "page-100-tok"},
        {"id": "200", "name": "Terno", "category": "Software",
         "followers_count": 120, "fan_count": 110,
         "link": "https://facebook.com/terno", "about": "AI analytics",
         "access_token": "page-200-tok"},
    ],
}


def _spec(fields, report_type, accounts=("100",), settings=None):
    return QuerySpec(
        accounts=list(accounts), fields=list(fields),
        date_range=DateRange("2026-08-01", "2026-08-31"),
        report_type=report_type, settings=settings or {},
    )


class TestDiscovery:
    def test_lists_managed_pages(self):
        def http(method, url, token, params=None):
            assert "me/accounts" in url
            assert token == "user-tok"         # discovery uses the user token
            assert "access_token" in params["fields"]
            return ACCOUNTS

        conn = FacebookPagesConnector(_DS(), http=http)
        accts = conn.list_accounts()
        assert [a.id for a in accts] == ["100", "200"]
        assert accts[0].name == "CloudXLab"

    def test_discovery_is_cached(self):
        calls = {"n": 0}

        def http(method, url, token, params=None):
            calls["n"] += 1
            return ACCOUNTS

        conn = FacebookPagesConnector(_DS(), http=http)
        conn.list_accounts()
        conn.list_accounts()
        assert calls["n"] == 1                 # /me/accounts fetched once


class TestPages:
    def test_pages_report_rows_from_discovery(self):
        conn = FacebookPagesConnector(
            _DS(), http=lambda m, u, t, p=None: ACCOUNTS)
        res = conn.query(_spec(
            ["id", "name", "followers_count"], "Pages", accounts=["100", "200"]))
        assert res.rows == [
            {"id": "100", "name": "CloudXLab", "followers_count": 5000},
            {"id": "200", "name": "Terno", "followers_count": 120},
        ]

    def test_pages_report_respects_account_selection(self):
        conn = FacebookPagesConnector(
            _DS(), http=lambda m, u, t, p=None: ACCOUNTS)
        res = conn.query(_spec(["id", "name"], "Pages", accounts=["200"]))
        assert res.rows == [{"id": "200", "name": "Terno"}]


class TestPosts:
    def test_posts_use_page_token(self):
        captured = {}

        def http(method, url, token, params=None):
            if "me/accounts" in url:
                return ACCOUNTS
            captured["url"] = url
            captured["token"] = token
            return {"data": [
                {"id": "100_1", "created_time": "2026-08-01T10:00:00+0000",
                 "message": "Hello", "permalink_url": "https://fb.com/100_1",
                 "status_type": "added_photos"},
            ]}

        conn = FacebookPagesConnector(_DS(), http=http)
        res = conn.query(_spec(
            ["id", "message", "status_type"], "Posts"))
        assert "100/posts" in captured["url"]
        assert captured["token"] == "page-100-tok"   # Page token, not user token
        assert res.rows == [
            {"id": "100_1", "message": "Hello", "status_type": "added_photos"}]


class TestVisitorPostsAndComments:
    def test_visitor_posts_flatten_author(self):
        def http(method, url, token, params=None):
            if "me/accounts" in url:
                return ACCOUNTS
            assert "100/visitor_posts" in url
            return {"data": [
                {"id": "v1", "created_time": "2026-08-02T09:00:00+0000",
                 "from": {"name": "Asha", "id": "u1"},
                 "message": "Great course!",
                 "permalink_url": "https://fb.com/v1"},
            ]}

        conn = FacebookPagesConnector(_DS(), http=http)
        res = conn.query(_spec(
            ["id", "from_name", "message"], "VisitorPosts"))
        assert res.rows == [
            {"id": "v1", "from_name": "Asha", "message": "Great course!"}]

    def test_post_comments_require_post_id(self):
        conn = FacebookPagesConnector(
            _DS(), http=lambda m, u, t, p=None: ACCOUNTS)
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(["id", "message"], "PostComments"))
        assert exc.value.code == ErrorCode.MISSING_SETTING

    def test_post_comments_fetch_and_flatten(self):
        captured = {}

        def http(method, url, token, params=None):
            if "me/accounts" in url:
                return ACCOUNTS
            captured["url"] = url
            captured["token"] = token
            return {"data": [
                {"id": "c1", "created_time": "2026-08-03T11:00:00+0000",
                 "from": {"name": "Ravi", "id": "u2"},
                 "message": "When is the next batch?", "like_count": 3},
            ]}

        conn = FacebookPagesConnector(_DS(), http=http)
        res = conn.query(_spec(
            ["id", "from_name", "message", "like_count"], "PostComments",
            settings={"post_id": "100_1"}))
        assert "100_1/comments" in captured["url"]
        assert captured["token"] == "page-100-tok"
        assert res.rows == [{
            "id": "c1", "from_name": "Ravi",
            "message": "When is the next batch?", "like_count": 3}]


class TestRatings:
    def test_ratings_flatten_reviewer(self):
        def http(method, url, token, params=None):
            if "me/accounts" in url:
                return ACCOUNTS
            assert "100/ratings" in url
            return {"data": [
                {"created_time": "2026-08-04T12:00:00+0000",
                 "recommendation_type": "positive",
                 "review_text": "Highly recommend",
                 "reviewer": {"name": "Meera", "id": "u3"}},
            ]}

        conn = FacebookPagesConnector(_DS(), http=http)
        res = conn.query(_spec(
            ["recommendation_type", "review_text", "reviewer_name"], "Ratings"))
        assert res.rows == [{
            "recommendation_type": "positive",
            "review_text": "Highly recommend", "reviewer_name": "Meera"}]


class TestReadOnly:
    def test_exposes_no_write_actions(self):
        conn = FacebookPagesConnector(
            _DS(), http=lambda m, u, t, p=None: ACCOUNTS)
        assert conn.list_actions() == []
