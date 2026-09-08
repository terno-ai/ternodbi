"""Settings validation (§6.1.1).

A missing required setting must fail as a coded, actionable error before any
provider is called — not as an opaque upstream failure the agent can't recover
from.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.settings_validation import validate_settings


class FakeCatalog:
    """Stands in for a ConnectorCatalog row — only report_types is read."""

    def __init__(self, report_types):
        self.report_types = report_types
        self.has_report_types = bool(report_types)


YOUTUBE = FakeCatalog([
    {"id": "ChannelTotals", "settings": []},
    {"id": "VideoTotals", "settings": [
        {"setting_id": "video_id", "label": "Video ID", "required": True},
    ]},
])

NO_REPORT_TYPES = FakeCatalog([])


def _code(catalog, report_type, settings):
    with pytest.raises(ApiError) as exc:
        validate_settings(catalog, report_type, settings)
    return exc.value.code


class TestReportTypeSources:
    def test_valid_report_with_no_settings(self):
        validate_settings(YOUTUBE, "ChannelTotals", {})   # no raise

    def test_valid_report_with_required_setting(self):
        validate_settings(YOUTUBE, "VideoTotals", {"video_id": "z6m"})

    def test_missing_report_type(self):
        assert _code(YOUTUBE, None, {}) == ErrorCode.INVALID_REPORT_TYPE

    def test_missing_report_type_names_the_options_and_the_right_tool(self):
        # An agent must be able to self-correct from the error alone — so it
        # lists the valid ids and points at list_fields (list_datasources takes
        # no datasource arg and cannot show these).
        with pytest.raises(ApiError) as exc:
            validate_settings(YOUTUBE, None, {})
        assert set(exc.value.details["available"]) == {"ChannelTotals", "VideoTotals"}
        assert "ChannelTotals" in exc.value.message
        assert "list_datasources" not in exc.value.message

    def test_unknown_report_type(self):
        assert _code(YOUTUBE, "NoSuch", {}) == ErrorCode.INVALID_REPORT_TYPE

    def test_missing_required_setting(self):
        assert _code(YOUTUBE, "VideoTotals", {}) == ErrorCode.MISSING_SETTING

    def test_blank_required_setting_is_missing(self):
        assert _code(YOUTUBE, "VideoTotals", {"video_id": "  "}) == ErrorCode.MISSING_SETTING

    def test_unknown_setting_is_rejected(self):
        # An unknown key must never be forwarded to the provider.
        assert _code(YOUTUBE, "ChannelTotals", {"bogus": "x"}) == ErrorCode.INVALID_SETTING


class TestSourcesWithoutReportTypes:
    def test_no_settings_is_fine(self):
        validate_settings(NO_REPORT_TYPES, None, {})

    def test_settings_rejected_when_source_has_none(self):
        assert _code(NO_REPORT_TYPES, None, {"x": 1}) == ErrorCode.INVALID_SETTING
