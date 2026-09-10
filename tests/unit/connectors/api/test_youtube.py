"""The YouTube connector, against mocked YouTube Data + Analytics responses.

The mock returns the real shapes from `channels.list` (Data API v3) and the
column-oriented Analytics `reports` response, so account listing, per-report
dimension/metric selection, and parsing are exercised without a live provider.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.sources.youtube import YouTubeConnector
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec


class _Catalog:
    key = "youtube"
    report_types = [
        {"id": "ChannelTotals", "settings": []},
        {"id": "Geo", "settings": []},
        {"id": "LatestVideos", "settings": []},
        {"id": "VideoTotals", "settings": [
            {"setting_id": "video_id", "required": True, "label": "Video ID"}]},
        {"id": "Revenue", "settings": []},
        {"id": "Members", "settings": []},
    ]
    has_report_types = True


class _DS:
    type = "youtube"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok"}


CHANNELS = {
    "items": [
        {"id": "UC_channel_1", "snippet": {"title": "Cooking"},
         "statistics": {"videoCount": "42", "subscriberCount": "1500",
                        "viewCount": "900000"}},
        {"id": "UC_channel_2", "snippet": {"title": "Travel"},
         "statistics": {"videoCount": "7"}},
    ],
}

# Column-oriented Analytics response: headers name the columns, rows are arrays.
GEO_REPORT = {
    "columnHeaders": [
        {"name": "country", "columnType": "DIMENSION", "dataType": "STRING"},
        {"name": "views", "columnType": "METRIC", "dataType": "INTEGER"},
        {"name": "estimatedMinutesWatched", "columnType": "METRIC",
         "dataType": "INTEGER"},
    ],
    "rows": [
        ["US", "1000", "5000"],
        ["IN", "800", "4200"],
    ],
}


def _mock_http(routes):
    def http(method, url, token, body=None):
        assert token == "tok"
        for (m, needle), response in routes.items():
            if method == m and needle in url:
                return response
        raise AssertionError(f"unexpected call: {method} {url}")
    return http


def _connector(routes):
    return YouTubeConnector(_DS(), http=_mock_http(routes))


class TestListAccounts:
    def test_maps_channels(self):
        conn = _connector({("GET", "/channels"): CHANNELS})
        accounts = conn.list_accounts()
        assert {a.id for a in accounts} == {"UC_channel_1", "UC_channel_2"}
        assert accounts[0].name == "Cooking"

    def test_channel_statistics_are_surfaced(self):
        # "How many videos" is answerable straight from the channel resource.
        conn = _connector({("GET", "/channels"): CHANNELS})
        first = conn.list_accounts()[0].as_dict()
        assert first["video_count"] == 42          # coerced to int
        assert first["subscriber_count"] == 1500
        assert first["view_count"] == 900000

    def test_requests_the_statistics_part(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return CHANNELS

        conn = YouTubeConnector(_DS(), http=http)
        conn.list_accounts()
        assert "statistics" in captured["params"]["part"]


class TestListFields:
    def test_report_type_fixes_dimensions_and_scopes_metrics(self):
        conn = _connector({})
        geo = {f.id for f in conn.list_fields("Geo")}
        demo = {f.id: f for f in conn.list_fields("Demographic")}
        assert "country" in geo
        assert "views" in geo
        # Demographic exposes the viewer-percentage metric, not raw views.
        assert "viewerPercentage" in demo
        assert demo["ageGroup"].kind == "dimension"

    def test_averages_are_non_aggregatable(self):
        conn = _connector({})
        by_id = {f.id: f for f in conn.list_fields("ChannelTotals")}
        assert by_id["averageViewDuration"].is_non_aggregatable is True
        assert by_id["views"].is_non_aggregatable is False


class TestRunReport:
    def _spec(self, report_type="Geo", fields=("views", "estimatedMinutesWatched"),
              accounts=("UC_channel_1",), settings=None):
        return QuerySpec(
            accounts=list(accounts), fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type=report_type, settings=settings or {},
        )

    def test_builds_query_with_ids_dates_and_dimensions(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["url"] = url
            captured["params"] = body
            return GEO_REPORT

        conn = YouTubeConnector(_DS(), http=http)
        conn.query(self._spec())
        p = captured["params"]
        assert p["ids"] == "channel==UC_channel_1"
        assert p["dimensions"] == "country"          # fixed by the Geo report
        assert set(p["metrics"].split(",")) == {"views", "estimatedMinutesWatched"}
        assert p["startDate"] == "2026-08-01" and p["endDate"] == "2026-08-31"
        assert "reports" in captured["url"]

    def test_parses_column_oriented_rows(self):
        conn = _connector({
            ("GET", "/channels"): CHANNELS,
            ("GET", "reports"): GEO_REPORT,
        })
        result = conn.query(self._spec())
        assert result.row_count == 2
        assert result.rows[0] == {
            "country": "US", "views": 1000, "estimatedMinutesWatched": 5000,
        }

    def test_video_totals_applies_the_video_filter(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"columnHeaders": [
                {"name": "views", "columnType": "METRIC"}], "rows": [["42"]]}

        conn = YouTubeConnector(_DS(), http=http)
        conn.query(self._spec(
            report_type="VideoTotals", fields=("views",),
            settings={"video_id": "abc123"}))
        assert captured["params"]["filters"] == "video==abc123"
        assert "dimensions" not in captured["params"]   # VideoTotals has none

    def test_top_videos_sorts_and_limits(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return {"columnHeaders": [{"name": "video", "columnType": "DIMENSION"},
                                      {"name": "views", "columnType": "METRIC"}],
                    "rows": []}

        conn = YouTubeConnector(_DS(), http=http)
        spec = self._spec(report_type="LatestVideos", fields=("views",))
        conn.query(spec)
        assert captured["params"]["sort"] == "-views"
        assert captured["params"]["maxResults"] == spec.max_rows

    def test_no_metric_defaults_to_the_reports_first(self):
        captured = {}

        def http(method, url, token, body=None):
            captured["params"] = body
            return GEO_REPORT

        conn = YouTubeConnector(_DS(), http=http)
        conn.query(self._spec(fields=()))
        assert captured["params"]["metrics"] == "views"

    def test_unknown_field_is_rejected(self):
        conn = _connector({("GET", "reports"): GEO_REPORT})
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec(fields=("viewz",)))
        assert exc.value.code == ErrorCode.INVALID_FIELD

    def test_multi_channel_tags_rows(self):
        conn = _connector({
            ("GET", "/channels"): CHANNELS,
            ("GET", "reports"): GEO_REPORT,
        })
        result = conn.query(self._spec(accounts=("UC_channel_1", "UC_channel_2")))
        assert result.row_count == 4
        assert all("_account" in r for r in result.rows)


class TestRevenue:
    REVENUE = {
        "columnHeaders": [
            {"name": "estimatedRevenue", "columnType": "METRIC"},
            {"name": "estimatedAdRevenue", "columnType": "METRIC"},
        ],
        "rows": [["123.45", "100.10"]],
    }

    def test_revenue_metrics_are_monetary(self):
        conn = _connector({})
        by_id = {f.id: f for f in conn.list_fields("Revenue")}
        assert by_id["estimatedRevenue"].is_monetary is True
        assert by_id["cpm"].is_non_aggregatable is True

    def test_revenue_query_and_parse(self):
        conn = _connector({
            ("GET", "/channels"): CHANNELS,
            ("GET", "reports"): self.REVENUE,
        })
        spec = QuerySpec(
            accounts=["UC_channel_1"],
            fields=["estimatedRevenue", "estimatedAdRevenue"],
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="Revenue",
        )
        result = conn.query(spec)
        assert result.rows[0] == {
            "estimatedRevenue": 123.45, "estimatedAdRevenue": 100.10,
        }


class TestMembers:
    MEMBERS = {
        "items": [
            {"snippet": {
                "memberDetails": {"displayName": "Alice", "channelId": "UCa"},
                "membershipsDetails": {
                    "highestAccessibleLevelDisplayName": "Gold",
                    "membershipsDuration": {"memberSince": "2025-01-01T00:00:00Z",
                                            "totalDurationMonths": 8},
                }}},
            {"snippet": {
                "memberDetails": {"displayName": "Bob", "channelId": "UCb"},
                "membershipsDetails": {
                    "highestAccessibleLevelDisplayName": "Silver",
                    "membershipsDuration": {"memberSince": "2026-06-01T00:00:00Z",
                                            "totalDurationMonths": 3},
                }}},
        ],
    }

    def _spec(self):
        return QuerySpec(
            accounts=["UC_channel_1"], fields=[],
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type="Members",
        )

    def test_members_hit_the_data_api_not_analytics(self):
        seen = {}

        def http(method, url, token, body=None):
            seen["url"] = url
            return self.MEMBERS

        conn = YouTubeConnector(_DS(), http=http)
        conn.query(self._spec())
        assert "/members" in seen["url"]

    def test_members_are_parsed(self):
        conn = _connector({("GET", "/members"): self.MEMBERS})
        result = conn.query(self._spec())
        assert result.row_count == 2
        assert result.rows[0] == {
            "member_name": "Alice", "member_channel_id": "UCa",
            "level": "Gold", "member_since": "2025-01-01T00:00:00Z",
            "total_months": 8,
        }

    def test_members_fields_listed(self):
        conn = _connector({})
        ids = {f.id for f in conn.list_fields("Members")}
        assert "member_name" in ids and "level" in ids


class TestDeclinedScopes:
    """A user can decline the monetary / memberships scopes on Google's granular
    consent screen. Those reports must then fail with an actionable message, not
    an opaque provider 403 — but only when we actually know the scope was
    declined."""

    def _ds(self, granted):
        class _D:
            type = "youtube"
            catalog = _Catalog()
            connection_json = {"ACCESS_TOKEN": "tok", "GRANTED_SCOPES": granted}
        return _D()

    def _spec(self, report_type):
        return QuerySpec(
            accounts=["UC_channel_1"], fields=[],
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type=report_type)

    def test_revenue_blocked_when_monetary_scope_declined(self):
        granted = ("https://www.googleapis.com/auth/yt-analytics.readonly "
                   "https://www.googleapis.com/auth/youtube.readonly")
        conn = YouTubeConnector(self._ds(granted), http=lambda *a, **k: {})
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec("Revenue"))
        assert "revenue" in exc.value.message.lower()
        assert "reconnect" in exc.value.message.lower()

    def test_members_blocked_when_memberships_scope_declined(self):
        granted = "https://www.googleapis.com/auth/yt-analytics.readonly"
        conn = YouTubeConnector(self._ds(granted), http=lambda *a, **k: {})
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec("Members"))
        assert "member" in exc.value.message.lower()

    def test_not_blocked_when_scopes_unknown(self):
        # No GRANTED_SCOPES recorded (older connection): must not false-block —
        # let the call proceed and rely on provider error surfacing.
        class _D:
            type = "youtube"
            catalog = _Catalog()
            connection_json = {"ACCESS_TOKEN": "tok"}   # no GRANTED_SCOPES
        conn = YouTubeConnector(_D(), http=lambda *a, **k: {"rows": []})
        # Reaches the API (returns empty) rather than raising a scope error.
        result = conn.query(self._spec("Members"))
        assert result.row_count == 0

    def test_allowed_when_scope_present(self):
        granted = ("https://www.googleapis.com/auth/yt-analytics.readonly "
                   "https://www.googleapis.com/auth/yt-analytics-monetary.readonly")
        conn = YouTubeConnector(
            self._ds(granted),
            http=lambda *a, **k: {"columnHeaders": [
                {"name": "estimatedRevenue", "columnType": "METRIC"}],
                "rows": [["10.0"]]})
        result = conn.query(self._spec("Revenue"))
        assert result.rows[0]["estimatedRevenue"] == 10.0


class TestErrorSurfacing:
    class _Resp:
        def __init__(self, status, payload=None, text=""):
            self.status_code = status
            self._payload = payload
            self.text = text

        def json(self):
            if self._payload is None:
                raise ValueError("no json")
            return self._payload

    def test_error_reason_is_surfaced(self):
        from terno_dbi.connectors.api.sources.youtube import _yt_error
        err = _yt_error(self._Resp(403, {
            "error": {"message": "Forbidden",
                      "errors": [{"reason": "insufficientPermissions"}]}}))
        assert err.retriable is False
        assert "insufficientPermissions" in err.message

    def test_401_becomes_auth_expired(self):
        from terno_dbi.connectors.api.sources.youtube import _AuthError

        def http(method, url, token, body=None):
            raise _AuthError()

        conn = YouTubeConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestRegistration:
    def test_youtube_is_registered_at_startup(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("youtube")

    def test_factory_binds_a_token_refresher(self):
        from terno_dbi.connectors.api.sources.youtube import make_youtube_connector
        conn = make_youtube_connector(_DS())
        assert conn._token_refresher is not None
