"""Google Drive connector.

Implements the `ApiConnector` interface against the Drive API v3 directly:

- `list_accounts()` — `GET /drives` (the shared drives the credential belongs
  to), plus a synthetic "My Drive" entry for the user's own corpus. Drive has no
  account concept of its own, so the *corpus* is what a caller picks between.
- `list_fields()`   — a *static* catalogue of file metadata. Drive exposes a
  fixed resource shape with no metadata endpoint to discover, so this mirrors
  Search Console rather than GA4.
- `_run()`          — `GET /files`, with the report type choosing the base `q`
  clause (files, folders, shared-with-me, trashed).

The scope is `drive.readonly`, which covers all three calls this connector
makes — `files.list`, `drives.list`, and file content. The narrower
`drive.metadata.readonly` does *not* serve `drives.list`, which is why shared
drives are invisible on that grant.

That matters beyond the scope constant: Google now lets a user tick individual
permissions at the consent screen, so a granted set is not guaranteed to be the
requested one. `list_accounts` therefore treats a 403 from `drives.list` as "no
shared drives on this grant" and still returns My Drive, rather than failing
discovery outright.

The date range filters `modifiedTime`, which makes a Drive result narrower than
it looks: a file untouched since before the range is absent, not missing. That
is easy to read as "this folder holds nothing else", so every result says so in
its `notes` and names the range that produced it.
"""

from __future__ import annotations
import logging
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

# Drive caps a page at 1000 regardless of what we ask for.
_MAX_PAGE_SIZE = 1000


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
         "When the file was last modified (RFC 3339). This is the field a "
         "date range filters on.", data_type="datetime"),
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


class GoogleDriveConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http

    # -- transport ----------------------------------------------------------

    def _call(self, method: str, url: str,
              params: Optional[Dict] = None) -> Dict[str, Any]:
        try:
            return self._http(method, url, self.access_token(), params)
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

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        """My Drive plus every shared drive the credential belongs to.

        My Drive is always listed, and listed first: it exists for every account
        and is where most files live. Shared drives are additive, and a
        deployment with none simply gets the one entry.

        `drives.list` needs a broader grant than the rest of this connector:
        Google serves it only to `drive` or `drive.readonly`, both *restricted*
        scopes, while `files.list` is happy with `drive.metadata.readonly`. On a
        metadata-only grant it answers 403, which must not take discovery down
        with it — My Drive is still fully queryable, and that is where most
        files are. So a 403 here means "no shared drives on this grant", not a
        failure. Any other error is a real fault and propagates.
        """
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
        return [f.field for f in _FIELDS]

    # -- query --------------------------------------------------------------

    def _build_query(self, spec: QuerySpec, report: _Report) -> str:
        """The Drive `q` expression for this request.

        Built from three layers, all ANDed: the report type's own clause, the
        caller's optional settings, and the date range when one is bounded.
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

        # Drive has no date parameter of its own, so the range becomes a
        # modifiedTime window. `_range_note` warns that this narrows the answer.
        clauses.append(
            f"modifiedTime >= '{spec.date_range.start}T00:00:00' and "
            f"modifiedTime <= '{spec.date_range.end}T23:59:59'"
        )

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
            notes=[_range_note(spec)],
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


def _range_note(spec: QuerySpec) -> str:
    """State that the result is a window, not the whole drive.

    Drive has no date dimension, so the range becomes a `modifiedTime` filter
    and an untouched file drops out silently. A caller who asked for "last 30
    days" out of habit would otherwise read the result as the full contents of
    a folder, so the omission is made explicit rather than left to be inferred.
    """
    return (
        f"Only files modified between {spec.date_range.start} and "
        f"{spec.date_range.end} are included. Files last changed outside that "
        f"window exist but are not listed here — widen date_range to see them."
    )


def make_google_drive_connector(datasource) -> GoogleDriveConnector:
    """Build a Drive connector wired to refresh its own OAuth token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return GoogleDriveConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["GoogleDriveConnector", "MY_DRIVE", "make_google_drive_connector"]
