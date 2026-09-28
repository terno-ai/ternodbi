"""The Google Drive connector, against mocked Drive API v3 responses.

The mock returns the real response shapes from `drives.list` and `files.list`,
so query building, the field mask and row parsing are exercised without a live
provider — the same approach as the GA4 and Search Console tests.
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
from terno_dbi.connectors.api.sources.google_drive import (
    MY_DRIVE,
    GoogleDriveConnector,
)

_MODIFIED = [
    {"setting_id": "modified_after", "required": False},
    {"setting_id": "modified_before", "required": False},
]

_REPORTS = [
    {"id": "Files", "settings": [
        {"setting_id": "folder_id", "required": False},
        {"setting_id": "name_contains", "required": False},
        {"setting_id": "mime_type", "required": False},
        *_MODIFIED,
    ]},
    {"id": "Folders", "settings": [
        {"setting_id": "folder_id", "required": False},
        {"setting_id": "name_contains", "required": False},
        *_MODIFIED,
    ]},
    {"id": "SharedWithMe", "settings": list(_MODIFIED)},
    {"id": "Trashed", "settings": list(_MODIFIED)},
    {"id": "FileContent", "settings": [
        {"setting_id": "file_id", "required": True},
        {"setting_id": "max_chars", "required": False},
    ]},
]


class _Catalog:
    key = "google_drive"
    report_types = _REPORTS
    has_report_types = True


class _DS:
    type = "google_drive"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "tok"}


# --- canned Drive responses ------------------------------------------------

DRIVES = {
    "drives": [
        {"id": "0ABCdef", "name": "Marketing"},
        {"id": "0AXYZ12", "name": "Finance"},
    ],
}

FILES = {
    "files": [
        {
            "id": "f1",
            "name": "Q3 plan.pdf",
            "mimeType": "application/pdf",
            "modifiedTime": "2026-08-14T09:30:00.000Z",
            "size": "20480",
            "webViewLink": "https://drive.google.com/file/d/f1/view",
            "owners": [{"displayName": "Ada", "emailAddress": "ada@example.com"}],
            "parents": ["folderA", "folderB"],
        },
        {
            # A Google-native doc: no size, no extension, no md5.
            "id": "f2",
            "name": "Roadmap",
            "mimeType": "application/vnd.google-apps.document",
            "modifiedTime": "2026-08-02T11:00:00.000Z",
            "webViewLink": "https://docs.google.com/document/d/f2/edit",
            "owners": [],
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


def _connector(routes):
    return GoogleDriveConnector(_DS(), http=_mock_http(routes))


def _capture(response=None):
    """A connector plus the params of every call it makes."""
    calls = []

    def http(method, url, token, params=None):
        calls.append({"method": method, "url": url, "params": params or {}})
        if "/drives" in url:
            return DRIVES
        return response if response is not None else FILES

    return GoogleDriveConnector(_DS(), http=http), calls


def _spec(fields=("id", "name", "size"), accounts=(MY_DRIVE,),
          report_type="Files", settings=None, date_range=None, max_rows=1000):
    return QuerySpec(
        accounts=list(accounts),
        fields=list(fields),
        date_range=date_range or DateRange("2026-08-01", "2026-08-31"),
        report_type=report_type,
        settings=dict(settings or {}),
        max_rows=max_rows,
    )


class TestListAccounts:
    def test_my_drive_is_listed_first_then_shared_drives(self):
        conn = _connector({("GET", "/drives"): DRIVES})
        accounts = conn.list_accounts()
        assert [a.id for a in accounts] == [MY_DRIVE, "0ABCdef", "0AXYZ12"]
        assert accounts[0].name == "My Drive"
        assert accounts[1].name == "Marketing"

    def test_my_drive_is_present_with_no_shared_drives(self):
        conn = _connector({("GET", "/drives"): {}})
        accounts = conn.list_accounts()
        assert [a.id for a in accounts] == [MY_DRIVE]

    def test_a_403_on_shared_drives_still_yields_my_drive(self):
        """`drives.list` needs `drive.readonly`; `files.list` does not.

        On a `drive.metadata.readonly` grant Google answers 403 here. My Drive
        remains fully queryable, so discovery must degrade rather than fail —
        otherwise the whole connector looks broken on its intended scope.
        """
        def http(method, url, token, params=None):
            if "/drives" in url:
                raise ApiError(
                    ErrorCode.UPSTREAM_ERROR,
                    "Google Drive API error (403 insufficientPermissions): "
                    "Request had insufficient authentication scopes.",
                    details={"status": 403, "reason": "insufficientPermissions"},
                )
            return FILES

        conn = GoogleDriveConnector(_DS(), http=http)
        assert [a.id for a in conn.list_accounts()] == [MY_DRIVE]

    def test_a_non_403_failure_still_propagates(self):
        """Only a scope refusal is tolerated; a real fault must not be hidden."""
        def http(method, url, token, params=None):
            raise ApiError(ErrorCode.UPSTREAM_ERROR, "boom",
                           details={"status": 500, "reason": "backendError"})

        conn = GoogleDriveConnector(_DS(), http=http)
        with pytest.raises(ApiError):
            conn.list_accounts()


class TestListFields:
    def test_exposes_the_fixed_catalogue(self):
        conn = _connector({})
        by_id = {f.id: f for f in conn.list_fields()}
        assert by_id["name"].kind == "dimension"
        assert by_id["modifiedTime"].data_type == "datetime"
        assert by_id["size"].kind == "metric"
        assert by_id["size"].data_type == "integer"
        # A revision counter is an identifier, not a quantity.
        assert by_id["version"].is_non_aggregatable is True
        assert by_id["size"].is_non_aggregatable is False


class TestRunReport:
    def test_parses_rows_by_field_id(self):
        conn = _connector({("GET", "/files"): FILES})
        result = conn.query(_spec(fields=("id", "name", "mimeType", "size")))
        assert result.row_count == 2
        assert result.rows[0] == {
            "id": "f1", "name": "Q3 plan.pdf",
            "mimeType": "application/pdf", "size": 20480,
        }
        # Drive reports no size for a native doc; that must stay None, not 0.
        assert result.rows[1]["size"] is None
        assert result.requested_field_ids == ["id", "name", "mimeType", "size"]

    def test_flattens_owners_and_parents(self):
        conn = _connector({("GET", "/files"): FILES})
        result = conn.query(_spec(fields=("ownerName", "ownerEmail", "parents")))
        assert result.rows[0]["ownerName"] == "Ada"
        assert result.rows[0]["ownerEmail"] == "ada@example.com"
        assert result.rows[0]["parents"] == "folderA,folderB"
        # An empty owners list must not raise, and must not invent a value.
        assert result.rows[1]["ownerName"] is None

    def test_no_fields_requested_returns_a_useful_default_set(self):
        conn = _connector({("GET", "/files"): FILES})
        result = conn.query(_spec(fields=()))
        assert result.requested_field_ids == [
            "id", "name", "mimeType", "modifiedTime", "size", "webViewLink",
        ]
        assert result.rows[0]["webViewLink"].endswith("/view")

    def test_field_mask_requests_only_what_was_asked_for(self):
        conn, calls = _capture()
        conn.query(_spec(fields=("name", "ownerName")))
        mask = calls[0]["params"]["fields"]
        assert mask.startswith("nextPageToken,files(")
        # `id` always rides along; nothing unrequested does.
        assert "id" in mask and "name" in mask
        assert "owners(displayName,emailAddress)" in mask
        assert "md5Checksum" not in mask

    def test_unknown_field_is_rejected_with_a_suggestion(self):
        conn = _connector({("GET", "/files"): FILES})
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(fields=("nmae",)))
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "name" in exc.value.message


class TestQueryBuilding:
    def test_files_report_excludes_folders_and_trash(self):
        conn, calls = _capture()
        conn.query(_spec())
        q = calls[0]["params"]["q"]
        assert "mimeType != 'application/vnd.google-apps.folder'" in q
        assert "trashed = false" in q

    def test_folders_report_selects_only_folders(self):
        conn, calls = _capture()
        conn.query(_spec(report_type="Folders"))
        assert "mimeType = 'application/vnd.google-apps.folder'" \
            in calls[0]["params"]["q"]

    def test_trashed_report_selects_trash(self):
        conn, calls = _capture()
        conn.query(_spec(report_type="Trashed"))
        assert "trashed = true" in calls[0]["params"]["q"]

    def test_settings_become_query_clauses(self):
        conn, calls = _capture()
        conn.query(_spec(settings={
            "folder_id": "folderA",
            "name_contains": "budget",
            "mime_type": "application/pdf",
        }))
        q = calls[0]["params"]["q"]
        assert "'folderA' in parents" in q
        assert "name contains 'budget'" in q
        assert "mimeType = 'application/pdf'" in q

    def test_apostrophe_in_a_setting_is_escaped(self):
        conn, calls = _capture()
        conn.query(_spec(settings={"name_contains": "O'Brien"}))
        # Unescaped, the literal would end early and the query would 400.
        assert r"name contains 'O\'Brien'" in calls[0]["params"]["q"]

    def test_date_range_is_not_a_filter(self):
        conn, calls = _capture()
        conn.query(_spec(date_range=DateRange("2026-08-01", "2026-08-31")))
        # A drive is current state. Filtering modifiedTime by a range the
        # caller passed only because the API demands one is what made a folder
        # of long-settled files look empty.
        assert "modifiedTime" not in calls[0]["params"]["q"]

    def test_result_says_the_date_range_was_ignored(self):
        conn = _connector({("GET", "/files"): FILES})
        result = conn.query(_spec(date_range=DateRange("2026-08-01",
                                                       "2026-08-31")))
        note = " ".join(result.notes)
        assert "ignored" in note
        assert "modified_after" in note

    def test_modified_settings_filter_modified_time(self):
        conn, calls = _capture()
        conn.query(_spec(settings={"modified_after": "2026-08-01",
                                   "modified_before": "2026-08-31"}))
        q = calls[0]["params"]["q"]
        assert "modifiedTime >= '2026-08-01T00:00:00'" in q
        assert "modifiedTime <= '2026-08-31T23:59:59'" in q

    def test_one_sided_modified_window_is_allowed(self):
        conn, calls = _capture()
        conn.query(_spec(settings={"modified_after": "2026-08-01"}))
        q = calls[0]["params"]["q"]
        assert "modifiedTime >= '2026-08-01T00:00:00'" in q
        assert "modifiedTime <=" not in q

    def test_a_modified_window_says_what_it_omits(self):
        conn = _connector({("GET", "/files"): FILES})
        result = conn.query(_spec(settings={"modified_after": "2026-08-01"}))
        note = " ".join(result.notes)
        assert "2026-08-01" in note
        assert "modified_after" in note

    def test_a_malformed_modified_date_is_rejected_before_the_call(self):
        conn, calls = _capture()
        # What an agent sends when it passes get_today() whole instead of
        # get_today()['utc_date']. Drive answers that with a bare
        # "Invalid Value" naming only `q`, which is unactionable.
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(settings={
                "modified_before": {"utc_date": "2026-09-17"}}))
        assert exc.value.code == ErrorCode.INVALID_FILTER
        assert "modified_before" in exc.value.message
        assert calls == []


class TestCorpusSelection:
    def test_my_drive_uses_the_user_corpus(self):
        conn, calls = _capture()
        conn.query(_spec(accounts=(MY_DRIVE,)))
        params = calls[0]["params"]
        assert params["corpora"] == "user"
        assert "driveId" not in params

    def test_shared_drive_scopes_by_drive_id(self):
        conn, calls = _capture()
        conn.query(_spec(accounts=("0ABCdef",)))
        params = calls[0]["params"]
        assert params["corpora"] == "drive"
        assert params["driveId"] == "0ABCdef"
        assert params["includeItemsFromAllDrives"] == "true"
        assert params["supportsAllDrives"] == "true"

    def test_shared_with_me_pins_the_user_corpus(self):
        conn, calls = _capture()
        # Even named against a shared drive: shared-with-me lives only in the
        # user corpus, so a driveId there would return nothing.
        conn.query(_spec(accounts=("0ABCdef",), report_type="SharedWithMe"))
        params = calls[0]["params"]
        assert params["corpora"] == "user"
        assert "driveId" not in params


class TestMultiAccount:
    def test_merges_and_tags_rows_across_drives(self):
        conn = _connector({("GET", "/files"): FILES})
        result = conn.query(_spec(accounts=(MY_DRIVE, "0ABCdef")))
        assert result.row_count == 4
        assert {r["_account"] for r in result.rows} == {MY_DRIVE, "0ABCdef"}

    def test_single_account_rows_are_not_tagged(self):
        conn = _connector({("GET", "/files"): FILES})
        result = conn.query(_spec())
        assert all("_account" not in r for r in result.rows)

    def test_one_failing_drive_does_not_sink_the_others(self):
        def http(method, url, token, params=None):
            if params and params.get("driveId") == "0AXYZ12":
                raise ApiError(ErrorCode.UPSTREAM_ERROR, "no access")
            return FILES

        conn = GoogleDriveConnector(_DS(), http=http)
        result = conn.query(_spec(accounts=(MY_DRIVE, "0AXYZ12")))
        assert result.row_count == 2               # My Drive's rows survive
        assert any("0AXYZ12" in w for w in result.warnings)


class TestPagination:
    def test_follows_next_page_token_until_max_rows(self):
        pages = [
            {"files": [{"id": "a"}], "nextPageToken": "p2"},
            {"files": [{"id": "b"}], "nextPageToken": "p3"},
            {"files": [{"id": "c"}]},
        ]
        seen = []

        def http(method, url, token, params=None):
            seen.append((params or {}).get("pageToken"))
            return pages[len(seen) - 1]

        conn = GoogleDriveConnector(_DS(), http=http)
        result = conn.query(_spec(fields=("id",)))
        assert [r["id"] for r in result.rows] == ["a", "b", "c"]
        assert seen == [None, "p2", "p3"]

    def test_stops_at_max_rows_without_another_request(self):
        calls = []

        def http(method, url, token, params=None):
            calls.append(params)
            return {"files": [{"id": "a"}, {"id": "b"}], "nextPageToken": "p2"}

        conn = GoogleDriveConnector(_DS(), http=http)
        result = conn.query(_spec(fields=("id",), max_rows=2))
        assert result.row_count == 2
        assert len(calls) == 1
        # The page size asks for no more than is still wanted.
        assert calls[0]["pageSize"] == 2


class TestAuthMapping:
    def test_401_becomes_auth_expired(self):
        from terno_dbi.connectors.api.sources.google_drive import _AuthError

        def http(method, url, token, params=None):
            raise _AuthError()

        conn = GoogleDriveConnector(_DS(), http=http)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestScopeGuard:
    """A `drive.file` grant is the quiet failure: Google answers `files.list`
    with 200 and an empty list, so a mis-scoped connection is indistinguishable
    from an empty Drive unless the connector says so."""

    @staticmethod
    def _with_scopes(scopes):
        class _Scoped(_DS):
            connection_json = {"ACCESS_TOKEN": "tok", "GRANTED_SCOPES": scopes}
        calls = []
        conn = GoogleDriveConnector(
            _Scoped(), http=lambda *a, **k: calls.append(a) or FILES)
        return conn, calls

    def test_drive_file_only_is_rejected_with_an_actionable_message(self):
        conn, calls = self._with_scopes(
            "https://www.googleapis.com/auth/drive.file")
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert "drive.file" in exc.value.message
        assert "drive.readonly" in exc.value.message
        assert calls == []

    def test_a_query_is_refused_on_the_same_grant(self):
        conn, calls = self._with_scopes(
            "https://www.googleapis.com/auth/drive.file")
        with pytest.raises(ApiError) as exc:
            conn.query(_spec())
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert calls == []

    def test_drive_readonly_is_accepted(self):
        conn, _ = self._with_scopes(
            "https://www.googleapis.com/auth/drive.readonly")
        assert conn.query(_spec()).row_count == 2

    @pytest.mark.parametrize("scope", [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.metadata.readonly",
    ])
    def test_other_drive_scopes_are_refused(self, scope):
        # Only the scope this connector requests is accepted.
        conn, calls = self._with_scopes(scope)
        with pytest.raises(ApiError) as exc:
            conn.query(_spec())
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert calls == []

    def test_an_unknown_granted_set_does_not_block(self):
        # A source connected before scopes were recorded must keep working.
        conn = _connector({("GET", "/files"): FILES})
        assert conn.query(_spec()).row_count == 2


class TestTokenRefreshOnProviderCall:
    def test_list_accounts_refreshes_the_token(self):
        calls = []
        conn = GoogleDriveConnector(
            _DS(), http=lambda *a, **k: DRIVES,
            token_refresher=lambda: calls.append(1),
        )
        conn.list_accounts()
        assert calls == [1]


class TestRegistration:
    def test_drive_is_registered_at_startup(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("google_drive")

    def test_registered_factory_binds_a_token_refresher(self):
        from terno_dbi.connectors.api.sources.google_drive import (
            make_google_drive_connector,
        )
        conn = make_google_drive_connector(_DS())
        assert conn._token_refresher is not None

    def test_oauth_scope_covers_shared_drives_and_content(self):
        from terno_dbi.connectors.api.auth.providers import get_provider
        provider = get_provider("google_drive")
        assert provider is not None
        # `drive.metadata.readonly` cannot serve `drives.list`, so shared drives
        # are invisible on it; `drive.readonly` is the narrowest scope that
        # covers every call this connector makes, contents included. The
        # `openid email` prefix is every Google provider's, for CONNECTED_EMAIL.
        assert provider.scope == (
            "openid email https://www.googleapis.com/auth/drive.readonly")
        assert provider.scope.endswith("drive.readonly")
        assert "drive.metadata.readonly" not in provider.scope


# --- file content ------------------------------------------------------------

_DOC_MIME = "application/vnd.google-apps.document"
_SHEET_MIME = "application/vnd.google-apps.spreadsheet"


def _content_connector(meta, body=b"hello", scopes=None):
    """A connector whose metadata call answers `meta` and whose download
    answers `body`, recording every download."""
    class _Scoped(_DS):
        connection_json = {"ACCESS_TOKEN": "tok",
                           **({"GRANTED_SCOPES": scopes} if scopes else {})}
    downloads = []

    def http(method, url, token, params=None):
        return meta

    def download(url, token, params, max_bytes):
        downloads.append({"url": url, "params": params or {}})
        if isinstance(body, Exception):
            raise body
        return body

    return GoogleDriveConnector(_Scoped(), http=http, download=download), downloads


def _content_spec(file_id="f1", accounts=(MY_DRIVE,), fields=(), **settings):
    return _spec(fields=fields, accounts=accounts, report_type="FileContent",
                 settings={"file_id": file_id, **settings})


class TestFileContent:
    def test_a_google_doc_is_exported_as_plain_text(self):
        conn, downloads = _content_connector(
            {"id": "f1", "name": "Plan", "mimeType": _DOC_MIME},
            body="Q3 plan \u2014 draft".encode())
        result = conn.query(_content_spec())
        assert downloads[0]["url"].endswith("/files/f1/export")
        assert downloads[0]["params"] == {"mimeType": "text/plain"}
        row = result.rows[0]
        assert row["content"] == "Q3 plan \u2014 draft"
        assert row["name"] == "Plan"
        assert row["truncated"] is False

    def test_a_google_sheet_exports_csv_and_says_only_the_first_tab(self):
        conn, downloads = _content_connector(
            {"id": "f1", "name": "Sales", "mimeType": _SHEET_MIME},
            body=b"Region,Revenue\nEU,10\n")
        result = conn.query(_content_spec())
        assert downloads[0]["params"] == {"mimeType": "text/csv"}
        assert result.rows[0]["content"].startswith("Region,Revenue")
        assert any("first tab" in n for n in result.notes)

    def test_an_uploaded_text_file_is_downloaded(self):
        conn, downloads = _content_connector(
            {"id": "f1", "name": "a.csv", "mimeType": "text/csv", "size": "5"})
        conn.query(_content_spec())
        assert downloads[0]["url"].endswith("/files/f1")
        assert downloads[0]["params"]["alt"] == "media"

    def test_a_utf8_bom_is_stripped(self):
        conn, _ = _content_connector(
            {"id": "f1", "mimeType": "text/plain"}, body=b"\xef\xbb\xbfhi")
        assert conn.query(_content_spec()).rows[0]["content"] == "hi"

    def test_content_is_truncated_at_max_chars(self):
        conn, _ = _content_connector(
            {"id": "f1", "mimeType": "text/plain"}, body=b"abcdef")
        result = conn.query(_content_spec(max_chars="4"))
        assert result.rows[0]["content"] == "abcd"
        assert result.rows[0]["truncated"] is True
        assert result.rows[0]["characters"] == 4
        assert any("max_chars" in n for n in result.notes)

    @pytest.mark.parametrize("value", ["0", "abc", "999999999"])
    def test_a_bad_max_chars_is_refused(self, value):
        conn, downloads = _content_connector(
            {"id": "f1", "mimeType": "text/plain"})
        with pytest.raises(ApiError) as exc:
            conn.query(_content_spec(max_chars=value))
        assert exc.value.code == ErrorCode.INVALID_SETTING
        assert downloads == []

    @pytest.mark.parametrize("mime", [
        "application/pdf",
        "image/png",
        "application/vnd.google-apps.folder",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ])
    def test_a_binary_file_is_refused_without_downloading(self, mime):
        conn, downloads = _content_connector(
            {"id": "f1", "name": "x", "mimeType": mime})
        with pytest.raises(ApiError) as exc:
            conn.query(_content_spec())
        assert exc.value.code == ErrorCode.INVALID_SETTING
        assert mime in exc.value.message
        assert downloads == []

    def test_binary_bytes_behind_a_text_type_are_refused(self):
        conn, _ = _content_connector(
            {"id": "f1", "mimeType": "text/plain"}, body=b"PK\x03\x04\x00\x00")
        with pytest.raises(ApiError) as exc:
            conn.query(_content_spec())
        assert exc.value.code == ErrorCode.INVALID_SETTING

    def test_an_oversized_upload_is_refused_before_downloading(self):
        conn, downloads = _content_connector(
            {"id": "f1", "mimeType": "text/plain", "size": str(11 * 1024 * 1024)})
        with pytest.raises(ApiError) as exc:
            conn.query(_content_spec())
        assert "10 MB" in exc.value.message
        assert downloads == []

    def test_a_shared_drive_file_is_read_with_its_drive_as_account(self):
        conn, downloads = _content_connector(
            {"id": "f1", "mimeType": "text/plain", "driveId": "0ABCdef"})
        conn.query(_content_spec(accounts=("0ABCdef",)))
        assert len(downloads) == 1

    @pytest.mark.parametrize("drive_id, account", [
        ("0ABCdef", MY_DRIVE),     # shared-drive file asked for via My Drive
        (None, "0ABCdef"),         # My Drive file asked for via a shared drive
        ("0AXYZ12", "0ABCdef"),    # a different shared drive
    ])
    def test_a_file_outside_the_account_is_forbidden(self, drive_id, account):
        # The allowlist is enforced on accounts; a bare file id must not read
        # past it.
        meta = {"id": "f1", "mimeType": "text/plain"}
        if drive_id:
            meta["driveId"] = drive_id
        conn, downloads = _content_connector(meta)
        with pytest.raises(ApiError) as exc:
            conn.query(_content_spec(accounts=(account,)))
        assert exc.value.code == ErrorCode.ACCOUNT_FORBIDDEN
        assert downloads == []

    def test_exactly_one_account_is_required(self):
        conn, _ = _content_connector({"id": "f1", "mimeType": "text/plain"})
        with pytest.raises(ApiError) as exc:
            conn.query(_content_spec(accounts=(MY_DRIVE, "0ABCdef")))
        assert exc.value.code == ErrorCode.INVALID_SETTING

    @pytest.mark.parametrize("file_id", ["f1/export", "../drives", "a?alt=media"])
    def test_a_file_id_that_is_not_an_id_is_refused(self, file_id):
        conn, _ = _content_connector({"id": "f1", "mimeType": "text/plain"})
        with pytest.raises(ApiError) as exc:
            conn.query(_content_spec(file_id=file_id))
        assert exc.value.code == ErrorCode.INVALID_SETTING

    def test_file_id_is_a_required_setting(self):
        conn, _ = _content_connector({"id": "f1", "mimeType": "text/plain"})
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(fields=(), report_type="FileContent"))
        assert exc.value.code == ErrorCode.MISSING_SETTING

    def test_requested_fields_limit_the_row(self):
        conn, _ = _content_connector({"id": "f1", "name": "a", "mimeType": "text/plain"})
        row = conn.query(_content_spec(fields=("name", "content"))).rows[0]
        assert set(row) == {"name", "content"}

    def test_an_unknown_field_is_refused(self):
        conn, _ = _content_connector({"id": "f1", "mimeType": "text/plain"})
        with pytest.raises(ApiError) as exc:
            conn.query(_content_spec(fields=("size",)))
        assert exc.value.code == ErrorCode.INVALID_FIELD

    def test_list_fields_is_per_report(self):
        conn, _ = _content_connector({})
        assert "content" in {f.id for f in conn.list_fields("FileContent")}
        assert "content" not in {f.id for f in conn.list_fields("Files")}

    def test_a_drive_file_only_grant_is_refused(self):
        conn, downloads = _content_connector(
            {"id": "f1", "mimeType": "text/plain"},
            scopes="https://www.googleapis.com/auth/drive.file")
        with pytest.raises(ApiError) as exc:
            conn.query(_content_spec())
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert downloads == []

    def test_a_401_on_download_maps_to_auth_expired(self):
        from terno_dbi.connectors.api.sources.google_drive import _AuthError
        conn, _ = _content_connector(
            {"id": "f1", "mimeType": _DOC_MIME}, body=_AuthError())
        with pytest.raises(ApiError) as exc:
            conn.query(_content_spec())
        assert exc.value.code == ErrorCode.AUTH_EXPIRED

    def test_the_catalog_declares_the_report(self):
        from terno_dbi.catalog.declarations import DECLARED_CONNECTORS
        drive = next(c for c in DECLARED_CONNECTORS if c.key == "google_drive")
        report = next(r for r in drive.report_types if r.id == "FileContent")
        settings = {s.setting_id: s.required for s in report.settings}
        assert settings == {"file_id": True, "max_chars": False}


class TestDefaultDownload:
    """The real transport's size ceiling, against a fake `requests`."""

    def _fake_requests(self, monkeypatch, status=200, chunks=(b"ok",)):
        import sys
        import types

        class Resp:
            status_code = status
            text = ""

            def json(self):
                return {"error": {"message": "nope"}}

            def iter_content(self, chunk_size):
                return iter(chunks)

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        fake = types.SimpleNamespace(get=lambda *a, **k: Resp())
        monkeypatch.setitem(sys.modules, "requests", fake)

    def test_bytes_are_joined(self, monkeypatch):
        from terno_dbi.connectors.api.sources.google_drive import _default_download
        self._fake_requests(monkeypatch, chunks=(b"ab", b"cd"))
        assert _default_download("u", "t", None, 10) == b"abcd"

    def test_a_stream_past_the_ceiling_is_abandoned(self, monkeypatch):
        from terno_dbi.connectors.api.sources.google_drive import _default_download
        self._fake_requests(monkeypatch, chunks=(b"abcdef", b"ghij"))
        with pytest.raises(ApiError) as exc:
            _default_download("u", "t", None, 8)
        assert exc.value.code == ErrorCode.INVALID_SETTING

    def test_an_error_status_is_surfaced(self, monkeypatch):
        from terno_dbi.connectors.api.sources.google_drive import _default_download
        self._fake_requests(monkeypatch, status=403)
        with pytest.raises(ApiError) as exc:
            _default_download("u", "t", None, 8)
        assert exc.value.details["status"] == 403
