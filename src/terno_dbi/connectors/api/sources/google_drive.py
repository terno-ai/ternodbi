"""Google Drive connector.

Implements the `ApiConnector` interface against the Drive API v3 directly:

- `list_accounts()` — `GET /drives` (the shared drives the credential belongs
  to), plus a synthetic "My Drive" entry for the user's own corpus. Drive has no
  account concept of its own, so the *corpus* is what a caller picks between.
- `list_fields()`   — a *static* catalogue of file metadata. Drive exposes a
  fixed resource shape with no metadata endpoint to discover, so this mirrors
  Search Console rather than GA4.
- `_run()`          — `GET /files`, with the report type choosing the base `q`
  clause (files, folders, shared-with-me, trashed). The `FileContent` report
  instead reads one file's contents: `GET /files/{id}?alt=media` for an
  uploaded text file, `GET /files/{id}/export` for a Google Doc, Sheet or Slides
  deck, which have no bytes of their own to download.

The scope is `drive.readonly`, which covers all three calls this connector
makes — `files.list`, `drives.list`, and file content. The narrower
`drive.metadata.readonly` does *not* serve `drives.list`, which is why shared
drives are invisible on that grant.

That matters beyond the scope constant: Google now lets a user tick individual
permissions at the consent screen, so a granted set is not guaranteed to be the
requested one. `list_accounts` therefore treats a 403 from `drives.list` as "no
shared drives on this grant" and still returns My Drive, rather than failing
discovery outright.

A drive is current state, not a time series, so `date_range` is *not* a filter
here — every report type declares `is_date_range_required=False` and this
connector ignores the range, the way the Sheets connector does. Filtering
`modifiedTime` by whatever range a caller happened to pass is what made a Drive
query look empty: an agent that supplies "last 30 days" out of habit would see
a folder of long-settled files as an empty folder. A modification window is
still available, but only when asked for by name, through the `modified_after`
and `modified_before` settings.
"""

from __future__ import annotations
import logging
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

_BASE = "https://www.googleapis.com/drive/v3"

# The user's own corpus. Drive gives it no id (it is "whatever this credential
# owns"), so we mint a stable one — it is what a caller passes as an account.
MY_DRIVE = "myDrive"

_FOLDER_MIME = "application/vnd.google-apps.folder"

_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly"

# Drive caps a page at 1000 regardless of what we ask for.
_MAX_PAGE_SIZE = 1000

# -- file content -----------------------------------------------------------

_CONTENT_REPORT = "FileContent"

# Google-native files have no stored bytes; Drive exports them instead. Each is
# exported to the plain-text form an agent can read. A Sheet exports only its
# first tab as CSV — the Sheets connector is the tool for the rest.
_EXPORT_MIME: Dict[str, str] = {
    "application/vnd.google-apps.document": "text/plain",
    "application/vnd.google-apps.spreadsheet": "text/csv",
    "application/vnd.google-apps.presentation": "text/plain",
}

# Uploaded files whose bytes are text, beyond the whole `text/*` family.
_TEXT_MIME = frozenset({
    "application/json",
    "application/xml",
    "application/javascript",
    "application/x-yaml",
    "application/yaml",
    "application/csv",
    "application/sql",
    "application/x-sh",
})

# Drive refuses to export anything over 10 MB, and a download is held in memory
# whole, so the same ceiling applies to both.
_MAX_DOWNLOAD_BYTES = 10 * 1024 * 1024

# What a caller gets back unless it asks for less. Enough for a long document,
# small enough not to swamp an agent's context.
_DEFAULT_MAX_CHARS = 50_000
_MAX_MAX_CHARS = 200_000


# -- field catalogue --------------------------------------------------------

@dataclass(frozen=True)
class _DriveField:
    """One exposed field: its public shape, its Drive field-mask fragment, and
    how to pull it out of a `files` resource.

    The mask fragment matters: Drive returns *only* `id, name, mimeType` unless
    asked, so the mask is built from the fields a query actually requested
    rather than a fixed superset.
    """

    field: Field
    mask: str
    extract: Callable[[Dict[str, Any]], Any]


def _plain(key: str):
    return lambda f: f.get(key)


def _owner(attr: str):
    def extract(f):
        owners = f.get("owners") or []
        return (owners[0] or {}).get(attr) if owners else None
    return extract


def _int_or_none(key: str):
    def extract(f):
        raw = f.get(key)
        if raw is None:
            return None
        try:
            return int(raw)
        except (TypeError, ValueError):
            return raw
    return extract


def _dim(fid, name, mask, desc="", data_type="string", extract=None):
    return _DriveField(
        Field(fid, name, "dimension", desc, data_type=data_type),
        mask, extract or _plain(mask),
    )


_FIELDS: List[_DriveField] = [
    _dim("id", "File ID", "id", "The file's Drive ID."),
    _dim("name", "Name", "name", "The file name as shown in Drive."),
    _dim("mimeType", "MIME type", "mimeType",
         "The file's MIME type. Google-native files use "
         "'application/vnd.google-apps.*' (e.g. .document, .spreadsheet)."),
    _dim("fileExtension", "File extension", "fileExtension",
         "Extension of the uploaded file; absent for Google-native files."),
    _dim("createdTime", "Created", "createdTime",
         "When the file was created (RFC 3339).", data_type="datetime"),
    _dim("modifiedTime", "Last modified", "modifiedTime",
         "When the file was last modified (RFC 3339). The modified_after / "
         "modified_before settings filter on this field.",
         data_type="datetime"),
    _dim("ownerName", "Owner", "owners(displayName,emailAddress)",
         "Display name of the file's first owner.",
         extract=_owner("displayName")),
    _dim("ownerEmail", "Owner email", "owners(displayName,emailAddress)",
         "Email address of the file's first owner.",
         extract=_owner("emailAddress")),
    _dim("lastModifyingUser", "Last modified by",
         "lastModifyingUser(displayName)",
         "Display name of the last user to modify the file.",
         extract=lambda f: (f.get("lastModifyingUser") or {}).get("displayName")),
    _dim("webViewLink", "Link", "webViewLink",
         "URL that opens the file in a browser."),
    _dim("parents", "Parent folder IDs", "parents",
         "IDs of the containing folders, comma-separated.",
         extract=lambda f: ",".join(f.get("parents") or []) or None),
    _dim("driveId", "Shared drive ID", "driveId",
         "ID of the shared drive the file lives in; absent in My Drive."),
    _dim("shared", "Shared", "shared",
         "Whether the file has been shared with anyone.", data_type="boolean"),
    _dim("starred", "Starred", "starred", data_type="boolean"),
    _dim("trashed", "Trashed", "trashed",
         "Whether the file is in the trash.", data_type="boolean"),
    _dim("description", "Description", "description"),
    _dim("md5Checksum", "MD5 checksum", "md5Checksum",
         "Checksum of the uploaded content; absent for Google-native files."),
    _DriveField(
        Field("size", "Size (bytes)", "metric",
              "Stored size in bytes. Google-native files report no size.",
              data_type="integer"),
        "size", _int_or_none("size"),
    ),
    _DriveField(
        Field("version", "Version", "metric",
              "Monotonically increasing revision counter. A version number is "
              "an identifier, not a quantity — never sum it.",
              data_type="integer", is_non_aggregatable=True),
        "version", _int_or_none("version"),
    ),
]

_BY_ID: Dict[str, _DriveField] = {f.field.id: f for f in _FIELDS}

# What a caller gets when they name no fields: enough to identify and locate a
# file without dragging the whole resource back.
_DEFAULT_FIELDS = ["id", "name", "mimeType", "modifiedTime", "size", "webViewLink"]

# The `FileContent` report's own columns: one row, the file and its text.
_CONTENT_FIELDS: List[Field] = [
    Field("id", "File ID", "dimension", "The file's Drive ID."),
    Field("name", "Name", "dimension", "The file name as shown in Drive."),
    Field("mimeType", "MIME type", "dimension", "The file's MIME type."),
    Field("content", "Content", "dimension",
          "The file's text. Google Docs and Slides are exported as plain text, "
          "a Google Sheet's first tab as CSV."),
    Field("characters", "Characters returned", "metric",
          "Length of `content` in characters.", data_type="integer",
          is_non_aggregatable=True),
    Field("truncated", "Truncated", "dimension",
          "Whether `content` was cut at max_chars.", data_type="boolean"),
]
_CONTENT_FIELD_IDS = [f.id for f in _CONTENT_FIELDS]


# -- report types -----------------------------------------------------------

@dataclass(frozen=True)
class _Report:
    """One Drive report: the `q` clause that defines it and its corpus policy."""

    clause: str
    # Shared-with-me lives only in the user's corpus; asking for it inside a
    # shared drive returns nothing, so those reports pin the corpus to `user`.
    user_corpus_only: bool = False


_REPORTS: Dict[str, _Report] = {
    "Files": _Report(f"mimeType != '{_FOLDER_MIME}' and trashed = false"),
    "Folders": _Report(f"mimeType = '{_FOLDER_MIME}' and trashed = false"),
    "SharedWithMe": _Report("sharedWithMe = true and trashed = false",
                            user_corpus_only=True),
    "Trashed": _Report("trashed = true"),
}
_DEFAULT_REPORT = "Files"


def _report_for(report_type: Optional[str]) -> _Report:
    return _REPORTS.get(report_type or "", _REPORTS[_DEFAULT_REPORT])


# -- transport --------------------------------------------------------------

def _default_http(method: str, url: str, token: str,
                  params: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    resp = requests.request(
        method, url, headers={"Authorization": f"Bearer {token}"},
        params=params or {}, timeout=30)
    if resp.status_code == 401:
        raise _AuthError()
    if resp.status_code >= 400:
        raise _drive_error(resp, params)
    return resp.json()


def _default_download(url: str, token: str, params: Optional[Dict],
                      max_bytes: int) -> bytes:
    """The raw bytes at `url`, refusing anything larger than `max_bytes`.

    Streamed so an oversized file is abandoned at the ceiling rather than read
    whole — the metadata check in `_run_content` catches most, but a Google
    export has no size until it is produced.
    """
    import requests
    with requests.get(url, headers={"Authorization": f"Bearer {token}"},
                      params=params or {}, timeout=60, stream=True) as resp:
        if resp.status_code == 401:
            raise _AuthError()
        if resp.status_code >= 400:
            raise _drive_error(resp, params)
        chunks: List[bytes] = []
        total = 0
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            total += len(chunk)
            if total > max_bytes:
                raise _too_large(max_bytes)
            chunks.append(chunk)
        return b"".join(chunks)


def _too_large(max_bytes: int) -> ApiError:
    return ApiError(
        ErrorCode.INVALID_SETTING,
        f"This file is larger than {max_bytes // (1024 * 1024)} MB, the most "
        f"FileContent reads. Open it in Drive instead.",
        details={"max_bytes": max_bytes},
        retriable=False,
    )


def _drive_error(resp, params: Optional[Dict[str, Any]] = None) -> ApiError:
    """Surface Google's own reason rather than a generic failure.

    Drive answers a rejected parameter with the generic "Invalid Value" and
    names the culprit only in `location`. Without it a 400 says that something
    was wrong but not what, so the location is folded into the message and
    carried in `details`.

    A rejected `q` is logged in full: the expression is assembled from a report
    clause, caller settings and a date window, so seeing the final string is
    the only way to tell which layer produced the bad syntax.
    """
    message, reason, location = "", "", ""
    try:
        err = (resp.json() or {}).get("error", {})
        message = err.get("message", "")
        errors = err.get("errors") or []
        if errors:
            reason = errors[0].get("reason", "")
            location = errors[0].get("location", "")
    except ValueError:
        message = (resp.text or "")[:200]
    if location == "q" and params:
        logger.warning("Google Drive rejected this q expression: %r",
                       params.get("q"))
    label = f"{resp.status_code} {reason}".strip()
    where = f" at '{location}'" if location else ""
    return ApiError(
        ErrorCode.UPSTREAM_ERROR,
        f"Google Drive API error ({label}){where}: "
        f"{message or 'unknown error'}",
        retriable=resp.status_code == 429 or resp.status_code >= 500,
        # Carried so a caller can branch on the status without parsing the
        # message — `list_accounts` needs to tell a scope refusal from a fault.
        details={"status": resp.status_code, "reason": reason,
                 "location": location},
    )


class _AuthError(Exception):
    """Internal marker for a 401 from Google, mapped to AUTH_EXPIRED."""


def _quote(value: Any) -> str:
    """A single-quoted literal for a Drive `q` expression.

    Drive's query language escapes with a backslash, so an apostrophe in a file
    name ("Q3 O'Brien notes") would otherwise terminate the literal early and
    turn a search into a syntax error.
    """
    text = str(value)
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_FILE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,256}$")


def _date_literal(value: Any, setting: str, suffix: str) -> str:
    """A validated `YYYY-MM-DDThh:mm:ss` literal for a `modifiedTime` clause.

    Validated rather than interpolated, because Drive answers a malformed `q`
    with a bare "Invalid Value" that names only the parameter. An agent that
    passed `get_today()` whole instead of `get_today()['utc_date']` then gets a
    400 it cannot act on; this turns that into an error naming the setting and
    the value.
    """
    text = str(value).strip()
    if not _DATE_RE.match(text):
        raise ApiError(
            ErrorCode.INVALID_FILTER,
            f"{setting} must be a date as YYYY-MM-DD; got {value!r}.",
            details={"setting": setting, "value": text[:120]},
            retriable=False,
        )
    return f"'{text}T{suffix}'"


class GoogleDriveConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None,
                 download: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http
        self._download_http = download or _default_download

    # -- transport ----------------------------------------------------------

    def _call(self, method: str, url: str,
              params: Optional[Dict] = None) -> Dict[str, Any]:
        return self._guarded(
            lambda token: self._http(method, url, token, params))

    def _download(self, url: str, params: Optional[Dict] = None) -> bytes:
        return self._guarded(
            lambda token: self._download_http(
                url, token, params, _MAX_DOWNLOAD_BYTES))

    def _guarded(self, request: Callable[[str], Any]) -> Any:
        """Run one provider request, mapping every failure to an `ApiError`."""
        try:
            return request(self.access_token())
        except _AuthError:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} access was rejected; reconnect the source.",
            )
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("Drive request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Google Drive returned an error. Try again.",
            )

    # -- scope --------------------------------------------------------------

    def _require_drive_read(self, feature: str) -> None:
        """Fail loudly when the grant cannot see the user's files.

        Unlike `ApiConnector.require_scope`, the message explains `drive.file`,
        because the wrong grant is not an error at Google: `drive.file` answers `files.list` with 200 and an empty
        list, since it can only see files this app created or the user picked.
        Without this check a mis-scoped connection is indistinguishable from an
        empty Drive. Silent when the granted set is unknown — see
        `granted_scopes`.
        """
        granted = self.granted_scopes()
        if granted and _DRIVE_SCOPE not in granted:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{feature} needs the '{_DRIVE_SCOPE}' permission. This "
                f"connection was granted only: {', '.join(sorted(granted))}. "
                f"A 'drive.file' grant can see only files this app created or "
                f"you picked explicitly, so no files are listable. Reconnect "
                f"{self.key} and allow Drive read access.",
                details={"granted": sorted(granted),
                         "required": _DRIVE_SCOPE},
                retriable=False,
            )

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        """My Drive plus every shared drive the credential belongs to.

        My Drive is always listed, and listed first: it exists for every account
        and is where most files live. Shared drives are additive, and a
        deployment with none simply gets the one entry.

        `drives.list` needs a broader grant than `files.list`: Google serves it
        only to `drive` or `drive.readonly`, while `files.list` is happy with
        `drive.metadata.readonly`. A connection whose granted set is unknown may
        still hold a metadata-only grant, and there it answers 403, which must
        not take discovery down with it — My Drive is still fully queryable, and that is where most
        files are. So a 403 here means "no shared drives on this grant", not a
        failure. Any other error is a real fault and propagates.
        """
        self._require_drive_read("Listing your drives")
        accounts: List[Account] = [Account(
            id=MY_DRIVE, name="My Drive",
            extra={"corpus": "user"},
        )]
        try:
            data = self._call("GET", f"{_BASE}/drives", {"pageSize": 100})
        except ApiError as exc:
            if exc.details.get("status") != 403:
                raise
            logger.info(
                "%s cannot enumerate shared drives on this grant (403); "
                "listing My Drive only.", self.key)
            return accounts
        for drive in data.get("drives", []):
            did = drive.get("id")
            if not did:
                continue
            accounts.append(Account(
                id=did, name=drive.get("name") or did,
                extra={"corpus": "drive"},
            ))
        return accounts

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        if report_type == _CONTENT_REPORT:
            return list(_CONTENT_FIELDS)
        return [f.field for f in _FIELDS]

    # -- query --------------------------------------------------------------

    def _build_query(self, spec: QuerySpec, report: _Report) -> str:
        """The Drive `q` expression for this request.

        Built from two layers, ANDed: the report type's own clause and the
        caller's optional settings. The query's `date_range` is not one of
        them — see the module docstring.
        """
        clauses = [report.clause]
        settings = spec.settings or {}

        folder_id = str(settings.get("folder_id") or "").strip()
        if folder_id:
            clauses.append(f"{_quote(folder_id)} in parents")

        name_contains = str(settings.get("name_contains") or "").strip()
        if name_contains:
            clauses.append(f"name contains {_quote(name_contains)}")

        mime_type = str(settings.get("mime_type") or "").strip()
        if mime_type:
            clauses.append(f"mimeType = {_quote(mime_type)}")

        # `spec.date_range` is deliberately not read here. A drive is current
        # state, so a range the caller did not mean as a filter must not remove
        # files from the answer; a modification window is opt-in by name.
        after = settings.get("modified_after")
        if after not in (None, ""):
            clauses.append(
                "modifiedTime >= "
                + _date_literal(after, "modified_after", "00:00:00"))

        before = settings.get("modified_before")
        if before not in (None, ""):
            clauses.append(
                "modifiedTime <= "
                + _date_literal(before, "modified_before", "23:59:59"))

        return " and ".join(clauses)

    @staticmethod
    def _corpus_params(account: str, report: _Report) -> Dict[str, Any]:
        """Scope the listing to one corpus.

        My Drive and a shared drive are different corpora, not different
        filters, so this is the parameter set rather than another `q` clause.
        """
        if report.user_corpus_only or account == MY_DRIVE:
            return {"corpora": "user"}
        return {
            "corpora": "drive",
            "driveId": account,
            "includeItemsFromAllDrives": "true",
            "supportsAllDrives": "true",
        }

    def _run(self, spec: QuerySpec) -> QueryResult:
        if spec.report_type == _CONTENT_REPORT:
            return self._run_content(spec)
        self._require_drive_read("Listing Drive files")
        report = _report_for(spec.report_type)

        unknown = [f for f in spec.fields if f not in _BY_ID]
        if unknown:
            raise invalid_field(unknown[0], list(_BY_ID.keys()))

        requested = list(spec.fields) or list(_DEFAULT_FIELDS)
        # Ask Drive for exactly the columns requested. `id` rides along
        # unconditionally: it costs nothing and every other identifier in the
        # response is ambiguous without it.
        mask = sorted({_BY_ID[f].mask for f in requested} | {"id"})
        query = self._build_query(spec, report)

        base_params: Dict[str, Any] = {
            "q": query,
            "fields": f"nextPageToken,files({','.join(mask)})",
            "orderBy": "modifiedTime desc",
        }
        multi = len(spec.accounts) > 1

        def fetch(account):
            params = dict(base_params, **self._corpus_params(account, report))
            return self._fetch_pages(params, requested, account,
                                     max_rows=spec.max_rows, multi=multi)

        # Partial success: one shared drive the credential has lost access to
        # must not sink a query across the rest.
        rows, warnings = gather_accounts(spec.accounts, fetch)

        return QueryResult(
            requested_field_ids=requested,
            rows=rows,
            row_count=len(rows),
            notes=_notes(spec),
            warnings=warnings,
        )

    def _fetch_pages(self, params: Dict[str, Any], requested: List[str],
                     account: str, *, max_rows: int,
                     multi: bool) -> List[Dict[str, Any]]:
        """Page through `files.list` until `max_rows` is satisfied.

        Drive pages at 1000 and ignores a larger `pageSize`, so a caller asking
        for more than that gets it only by following `nextPageToken`. The page
        size shrinks to what is still wanted, so the last page is not oversized.
        """
        rows: List[Dict[str, Any]] = []
        page_token = None
        while len(rows) < max_rows:
            page_params = dict(params)
            page_params["pageSize"] = min(_MAX_PAGE_SIZE, max_rows - len(rows))
            if page_token:
                page_params["pageToken"] = page_token
            data = self._call("GET", f"{_BASE}/files", page_params)
            for item in data.get("files", []):
                record: Dict[str, Any] = {}
                if multi:
                    record["_account"] = account
                for fid in requested:
                    record[fid] = _BY_ID[fid].extract(item)
                rows.append(record)
                if len(rows) >= max_rows:
                    break
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        return rows

    # -- file content -------------------------------------------------------

    def _run_content(self, spec: QuerySpec) -> QueryResult:
        """One file's text, as a single row.

        The file must live in the one account the query names — My Drive for
        the user's own and shared-with-me files, or the shared drive that holds
        it. A file id alone would otherwise read past the account allowlist the
        dispatch layer has just enforced on `spec.accounts`.
        """
        self._require_drive_read("Reading file contents")

        unknown = [f for f in spec.fields if f not in _CONTENT_FIELD_IDS]
        if unknown:
            raise invalid_field(unknown[0], _CONTENT_FIELD_IDS)
        requested = list(spec.fields) or list(_CONTENT_FIELD_IDS)

        if len(spec.accounts) != 1:
            raise ApiError(
                ErrorCode.INVALID_SETTING,
                f"{_CONTENT_REPORT} reads one file, so it takes exactly one "
                f"account: '{MY_DRIVE}' or the shared drive holding the file.",
                details={"accounts": list(spec.accounts)},
                retriable=False,
            )
        account = spec.accounts[0]
        settings = spec.settings or {}
        file_id = str(settings.get("file_id") or "").strip()
        if not _FILE_ID_RE.match(file_id):
            # Checked, not escaped: the id becomes a URL path segment, and a
            # "/" in it would address a different Drive endpoint.
            raise ApiError(
                ErrorCode.INVALID_SETTING,
                f"file_id {file_id[:120]!r} is not a Drive file id. Use the "
                f"`id` column from the Files or SharedWithMe report.",
                details={"setting_id": "file_id"},
                retriable=False,
            )
        max_chars = _max_chars(settings.get("max_chars"))

        meta = self._call("GET", f"{_BASE}/files/{file_id}", {
            "fields": "id,name,mimeType,size,driveId",
            "supportsAllDrives": "true",
        })
        _check_account(meta, account)

        mime = meta.get("mimeType") or ""
        notes: List[str] = []
        if mime in _EXPORT_MIME:
            export_as = _EXPORT_MIME[mime]
            raw = self._download(f"{_BASE}/files/{file_id}/export",
                                 {"mimeType": export_as})
            notes.append(f"Exported from Google's format as {export_as}.")
            if mime == "application/vnd.google-apps.spreadsheet":
                notes.append(
                    "A Google Sheet exports only its first tab. Use the Google "
                    "Sheets source to read other tabs or a cell range.")
        elif _is_text(mime):
            size = _int_or_none("size")(meta)
            if isinstance(size, int) and size > _MAX_DOWNLOAD_BYTES:
                raise _too_large(_MAX_DOWNLOAD_BYTES)
            raw = self._download(f"{_BASE}/files/{file_id}",
                                 {"alt": "media", "supportsAllDrives": "true"})
        else:
            raise _unsupported(meta)

        if b"\x00" in raw[:8192]:
            # Labelled as text but holding binary — decoding would hand the
            # caller mojibake that reads as the file's real contents.
            raise _unsupported(meta)
        text = raw.decode("utf-8-sig", errors="replace")
        truncated = len(text) > max_chars
        if truncated:
            text = text[:max_chars]
            notes.append(
                f"Content was cut at {max_chars:,} characters. Pass a larger "
                f"max_chars (up to {_MAX_MAX_CHARS:,}) to read more.")

        values = {
            "id": meta.get("id") or file_id,
            "name": meta.get("name"),
            "mimeType": mime,
            "content": text,
            "characters": len(text),
            "truncated": truncated,
        }
        return QueryResult(
            requested_field_ids=requested,
            rows=[{fid: values[fid] for fid in requested}],
            row_count=1,
            notes=notes,
        )


_NO_DATE_NOTE = (
    "A drive is current state, not a time series: Google Drive has no date "
    "dimension, so any date_range supplied was ignored and these rows are the "
    "drive as it stands now. To restrict by modification time, pass the "
    "modified_after / modified_before settings instead."
)


def _notes(spec: QuerySpec) -> List[str]:
    """What the caller has to know to read these rows correctly.

    Either the range was ignored (the usual case, and worth saying so plainly
    because the caller *did* pass one), or a modification window was asked for
    by name — in which case the omission it causes is stated instead, since an
    untouched file drops out of a `modifiedTime` filter silently and a short
    list otherwise reads as "this folder holds nothing else".
    """
    settings = spec.settings or {}
    after = settings.get("modified_after") or None
    before = settings.get("modified_before") or None
    if not (after or before):
        return [_NO_DATE_NOTE]
    window = " and ".join(filter(None, [
        f"modified on or after {after}" if after else None,
        f"modified on or before {before}" if before else None,
    ]))
    return [
        f"Only files {window} are included. Files last changed outside that "
        f"window exist but are not listed here — widen or drop "
        f"modified_after / modified_before to see them. The query's "
        f"date_range is not a filter on this source and was ignored."
    ]


def _max_chars(value: Any) -> int:
    if value in (None, ""):
        return _DEFAULT_MAX_CHARS
    try:
        n = int(value)
    except (TypeError, ValueError):
        n = 0
    if not 1 <= n <= _MAX_MAX_CHARS:
        raise ApiError(
            ErrorCode.INVALID_SETTING,
            f"max_chars must be a whole number from 1 to {_MAX_MAX_CHARS:,}; "
            f"got {value!r}.",
            details={"setting_id": "max_chars", "value": str(value)[:120]},
            retriable=False,
        )
    return n


def _check_account(meta: Dict[str, Any], account: str) -> None:
    """Refuse a file outside the queried account (see `_run_content`)."""
    drive_id = meta.get("driveId")
    if account == MY_DRIVE and not drive_id:
        return
    if drive_id and drive_id == account:
        return
    where = f"shared drive {drive_id!r}" if drive_id else f"'{MY_DRIVE}'"
    raise ApiError(
        ErrorCode.ACCOUNT_FORBIDDEN,
        f"File {meta.get('name') or meta.get('id')!r} is in {where}, not in "
        f"account {account!r}. Query it with that account instead.",
        details={"account": account, "file_drive": drive_id or MY_DRIVE},
        retriable=False,
    )


def _is_text(mime: str) -> bool:
    return (mime.startswith("text/") or mime in _TEXT_MIME
            or mime.endswith("+json") or mime.endswith("+xml"))


def _unsupported(meta: Dict[str, Any]) -> ApiError:
    mime = meta.get("mimeType") or "unknown"
    return ApiError(
        ErrorCode.INVALID_SETTING,
        f"Cannot read the contents of {meta.get('name') or meta.get('id')!r} "
        f"({mime}). FileContent reads Google Docs, Sheets and Slides, and "
        f"plain-text files such as .txt, .csv, .json and .md.",
        details={"mimeType": mime},
        retriable=False,
    )


def make_google_drive_connector(datasource) -> GoogleDriveConnector:
    """Build a Drive connector wired to refresh its own OAuth token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return GoogleDriveConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["GoogleDriveConnector", "MY_DRIVE", "make_google_drive_connector"]
