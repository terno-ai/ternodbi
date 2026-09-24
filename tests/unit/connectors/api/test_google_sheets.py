"""The Google Sheets connector, against mocked Sheets v4 and Drive v3 responses.

The mock returns the real response shapes from Drive's `files.list`, Sheets'
`values.get` and `spreadsheets.get`, so range building, header slugging and row
mapping are exercised without a live provider.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
from terno_dbi.connectors.api.sources.google_sheets import GoogleSheetsConnector

_SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.metadata.readonly"


class _Catalog:
    key = "google_sheets"
    report_types = [
        {"id": "Values", "settings": [
            {"setting_id": "sheet_name", "required": False},
            {"setting_id": "header_row", "required": False},
            {"setting_id": "range", "required": False},
        ]},
        {"id": "Tabs", "settings": []},
    ]
    has_report_types = True


class _DS:
    type = "google_sheets"
    catalog = _Catalog()
    connection_json = {
        "ACCESS_TOKEN": "tok",
        "GRANTED_SCOPES": f"{_SHEETS_SCOPE} {_DRIVE_SCOPE}",
    }


# --- canned responses -------------------------------------------------------

FILES = {
    "files": [
        {"id": "sheet1", "name": "Q3 Budget",
         "modifiedTime": "2026-08-14T09:30:00.000Z"},
        {"id": "sheet2", "name": "Headcount"},
    ],
}

# Header row plus three data rows; the last row is short, as Sheets omits
# trailing empty cells entirely.
VALUES = {
    "values": [
        ["Order Date", "Customer Name", "Revenue (£)"],
        ["2026-08-01", "Acme", 1200.5],
        ["2026-08-02", "Globex", 900],
        ["2026-08-03", "Initech"],
    ],
}

TABS = {
    "sheets": [
        {"properties": {"sheetId": 0, "title": "Data", "index": 0,
                        "sheetType": "GRID",
                        "gridProperties": {"rowCount": 1000, "columnCount": 26}}},
        {"properties": {"sheetId": 77, "title": "Notes", "index": 1,
                        "sheetType": "GRID", "hidden": True,
                        "gridProperties": {"rowCount": 50, "columnCount": 5}}},
    ],
}


def _http_ok(method, url, token, params=None):
    if "/drive/v3/files" in url:
        return FILES
    if "/values/" in url:
        return VALUES
    return TABS


def _connector(http=None, ds=None):
    return GoogleSheetsConnector(ds or _DS(), http=http or _http_ok)


def _capture(response_fn=None):
    calls = []

    def http(method, url, token, params=None):
        calls.append({"url": url, "params": params or {}})
        return (response_fn or _http_ok)(method, url, token, params)

    return GoogleSheetsConnector(_DS(), http=http), calls


def _spec(fields=(), accounts=("sheet1",), report_type="Values",
          settings=None, max_rows=1000):
    return QuerySpec(
        accounts=list(accounts),
        fields=list(fields),
        date_range=DateRange("2026-08-01", "2026-08-31"),
        report_type=report_type,
        settings=dict(settings or {}),
        max_rows=max_rows,
    )


class TestListAccounts:
    def test_spreadsheets_are_the_accounts(self):
        accounts = _connector().list_accounts()
        assert [(a.id, a.name) for a in accounts] == [
            ("sheet1", "Q3 Budget"), ("sheet2", "Headcount")]
        assert accounts[0].extra["modified_time"].startswith("2026-08-14")

    def test_searches_shared_drives_too(self):
        conn, calls = _capture()
        conn.list_accounts()
        params = calls[0]["params"]
        # A team's reporting sheet usually lives in a shared drive.
        assert params["corpora"] == "allDrives"
        assert params["includeItemsFromAllDrives"] == "true"
        assert "mimeType = 'application/vnd.google-apps.spreadsheet'" in params["q"]


class TestHeaderSlugging:
    def test_human_headers_become_typeable_ids(self):
        fields = _connector().list_fields("Values")
        by_id = {f.id: f for f in fields}
        assert "order_date" in by_id
        assert "customer_name" in by_id
        # Symbols are stripped, but the display name keeps the original text.
        assert "revenue" in by_id
        assert by_id["revenue"].name == "Revenue (£)"

    def test_duplicate_headers_are_disambiguated(self):
        def http(method, url, token, params=None):
            if "/drive/v3/files" in url:
                return {"files": [{"id": "s", "name": "s"}]}
            return {"values": [["Total", "Total", "Total"]]}

        fields = _connector(http).list_fields("Values")
        # A silent collision would drop two of the three columns.
        assert [f.id for f in fields] == ["total", "total_2", "total_3"]

    def test_blank_headers_fall_back_to_column_letters(self):
        def http(method, url, token, params=None):
            if "/drive/v3/files" in url:
                return {"files": [{"id": "s", "name": "s"}]}
            return {"values": [["Name", "", "  "]]}

        fields = _connector(http).list_fields("Values")
        assert [f.id for f in fields] == ["name", "column_b", "column_c"]


class TestRunValues:
    def test_rows_are_keyed_by_slugged_header(self):
        result = _connector().query(_spec())
        assert result.row_count == 3
        assert result.rows[0] == {
            "order_date": "2026-08-01", "customer_name": "Acme",
            "revenue": 1200.5,
        }

    def test_short_rows_are_padded_not_truncated(self):
        result = _connector().query(_spec())
        # Sheets omits trailing empties; the column must still be present.
        assert result.rows[2] == {
            "order_date": "2026-08-03", "customer_name": "Initech",
            "revenue": None,
        }

    def test_requested_fields_select_and_order_columns(self):
        result = _connector().query(_spec(fields=("revenue", "order_date")))
        assert result.requested_field_ids == ["revenue", "order_date"]
        assert list(result.rows[0].keys()) == ["revenue", "order_date"]

    def test_unknown_column_is_rejected_with_a_suggestion(self):
        with pytest.raises(ApiError) as exc:
            _connector().query(_spec(fields=("revenu",)))
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "revenue" in exc.value.message

    def test_values_are_requested_unformatted(self):
        conn, calls = _capture()
        conn.query(_spec())
        params = next(c for c in calls if "/values/" in c["url"])["params"]
        # So 1234.5 arrives as a number, not "£1,234.50".
        assert params["valueRenderOption"] == "UNFORMATTED_VALUE"

    def test_result_states_that_the_date_range_was_ignored(self):
        result = _connector().query(_spec())
        assert any("no date dimension" in n for n in result.notes)

    def test_max_rows_truncates(self):
        result = _connector().query(_spec(max_rows=2))
        assert result.row_count == 2


class TestRangeBuilding:
    def _range_of(self, calls):
        return next(c for c in calls if "/values/" in c["url"])["url"].split("/values/")[1]

    def test_defaults_to_the_first_visible_tab(self):
        conn, calls = _capture()
        conn.query(_spec())
        assert self._range_of(calls) == "A:ZZ"

    def test_named_tab_is_quoted(self):
        conn, calls = _capture()
        conn.query(_spec(settings={"sheet_name": "Q3 Data"}))
        # A tab name with a space is invalid unquoted.
        assert self._range_of(calls) == "'Q3 Data'"

    def test_apostrophe_in_a_tab_name_is_doubled(self):
        conn, calls = _capture()
        conn.query(_spec(settings={"sheet_name": "Bob's Tab"}))
        assert self._range_of(calls) == "'Bob''s Tab'"

    def test_explicit_range_is_combined_with_the_tab(self):
        conn, calls = _capture()
        conn.query(_spec(settings={"sheet_name": "Data", "range": "B2:F"}))
        assert self._range_of(calls) == "'Data'!B2:F"

    def test_range_that_already_names_a_tab_passes_through(self):
        conn, calls = _capture()
        conn.query(_spec(settings={"sheet_name": "Data", "range": "Other!A1:C9"}))
        assert self._range_of(calls) == "Other!A1:C9"


class TestHeaderRowSetting:
    def test_header_row_shifts_which_row_names_the_columns(self):
        def http(method, url, token, params=None):
            if "/drive/v3/files" in url:
                return FILES
            return {"values": [["ignore", "this"], ["Name", "Qty"], ["a", 2]]}

        result = _connector(http).query(_spec(settings={"header_row": "2"}))
        assert result.rows == [{"name": "a", "qty": 2}]

    def test_non_numeric_header_row_is_rejected(self):
        with pytest.raises(ApiError) as exc:
            _connector().query(_spec(settings={"header_row": "two"}))
        assert exc.value.code == ErrorCode.INVALID_SETTING

    def test_zero_header_row_is_rejected(self):
        with pytest.raises(ApiError) as exc:
            _connector().query(_spec(settings={"header_row": "0"}))
        assert exc.value.code == ErrorCode.INVALID_SETTING


class TestColumnNameTolerance:
    """An agent that reads the label off `list_fields` instead of the id is
    asking for a column that exists; refusing on spelling is the documented
    id-vs-label trap, so both spellings resolve to the same column."""

    def test_exact_header_text_is_accepted(self):
        result = _connector().query(_spec(fields=("Customer Name", "Revenue (£)")))
        # Returned under the canonical ids, whatever spelling was asked for.
        assert result.rows[0] == {"customer_name": "Acme", "revenue": 1200.5}

    def test_case_insensitive_label_is_accepted(self):
        result = _connector().query(_spec(fields=("customer name",)))
        assert result.rows[0] == {"customer_name": "Acme"}

    def test_requested_field_ids_report_canonical_ids_not_labels(self):
        result = _connector().query(_spec(fields=("Customer Name",)))
        # The header must describe the rows it ships with.
        assert result.requested_field_ids == ["customer_name"]
        assert set(result.rows[0]) == {"customer_name"}

    def test_canonical_id_still_wins(self):
        result = _connector().query(_spec(fields=("customer_name",)))
        assert result.rows[0] == {"customer_name": "Acme"}

    def test_a_genuinely_absent_column_is_still_rejected(self):
        with pytest.raises(ApiError) as exc:
            _connector().query(_spec(fields=("Nope",)))
        assert exc.value.code == ErrorCode.INVALID_FIELD


class TestFieldsAreTaggedBySpreadsheet:
    def test_each_column_names_its_sheet(self):
        """Every spreadsheet has a different schema, so a merged catalogue is
        meaningless unless each column says which sheet it belongs to."""
        seen = {}

        def http(method, url, token, params=None):
            if "/drive/v3/files" in url:
                return FILES
            if "sheet1" in url:
                return {"values": [["Order Date", "Revenue"]]}
            return {"values": [["Headcount", "Team"]]}

        fields = _connector(http).list_fields("Values")
        by_group = {}
        for f in fields:
            by_group.setdefault(f.group, []).append(f.id)
        assert by_group["Q3 Budget"] == ["order_date", "revenue"]
        assert by_group["Headcount"] == ["headcount", "team"]

    def test_same_column_in_two_sheets_is_kept_separately(self):
        def http(method, url, token, params=None):
            if "/drive/v3/files" in url:
                return FILES
            return {"values": [["Date"]]}

        fields = _connector(http).list_fields("Values")
        # Collapsing these would hide one sheet's schema behind another's.
        assert [(f.id, f.group) for f in fields] == [
            ("date", "Q3 Budget"), ("date", "Headcount")]


class TestRunTabs:
    def test_lists_tabs_with_grid_sizes(self):
        result = _connector().query(_spec(report_type="Tabs"))
        assert result.row_count == 2
        assert result.rows[0]["title"] == "Data"
        assert result.rows[0]["rowCount"] == 1000
        # Sheets omits `hidden` for a visible tab; it must read as False.
        assert result.rows[0]["hidden"] is False
        assert result.rows[1]["hidden"] is True


class TestMultiAccount:
    def test_merges_and_tags_rows(self):
        result = _connector().query(_spec(accounts=("sheet1", "sheet2")))
        assert result.row_count == 6
        assert {r["_account"] for r in result.rows} == {"sheet1", "sheet2"}

    def test_one_unreadable_sheet_does_not_sink_the_others(self):
        def http(method, url, token, params=None):
            if "sheet2" in url:
                raise ApiError(ErrorCode.UPSTREAM_ERROR, "no access")
            return _http_ok(method, url, token, params)

        result = _connector(http).query(_spec(accounts=("sheet1", "sheet2")))
        assert result.row_count == 3
        assert any("sheet2" in w for w in result.warnings)


class TestGranularConsent:
    """Google lets a user tick individual permissions; a declined one must
    surface as an actionable message, not an opaque 403."""

    def _ds_with(self, scopes):
        class DS(_DS):
            connection_json = {"ACCESS_TOKEN": "tok", "GRANTED_SCOPES": scopes}
        return DS()

    def test_missing_drive_scope_names_the_permission(self):
        conn = _connector(ds=self._ds_with(_SHEETS_SCOPE))
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert "drive.metadata.readonly" in exc.value.message

    def test_missing_sheets_scope_names_the_permission(self):
        conn = _connector(ds=self._ds_with(_DRIVE_SCOPE))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec())
        assert "spreadsheets.readonly" in exc.value.message

    def test_unknown_granted_set_does_not_block(self):
        conn = _connector(ds=self._ds_with(""))
        # Scopes recorded before the field existed are unknown, not refused.
        assert conn.list_accounts()


class TestRegistration:
    def test_sheets_is_registered_at_startup(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("google_sheets")

    def test_oauth_requests_both_scopes(self):
        from terno_dbi.connectors.api.auth.providers import get_provider
        scope = get_provider("google_sheets").scope
        assert _SHEETS_SCOPE in scope
        assert _DRIVE_SCOPE in scope
