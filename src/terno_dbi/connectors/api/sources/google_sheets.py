"""Google Sheets connector.

Implements the `ApiConnector` interface against the Sheets API v4, with one
borrowed call to Drive:

- `list_accounts()` — Drive `files.list` filtered to spreadsheets. The Sheets
  API can read a spreadsheet but cannot *find* one, so discovery goes through
  Drive. A spreadsheet is the account here: it is the unit a caller selects, and
  the unit the account allowlist (§7) can restrict.
- `list_fields()`   — the header row of a spreadsheet, turned into field ids.
  Unlike every other source, the catalogue belongs to the *data* rather than the
  provider, so it is read per spreadsheet and cached for the request.
- `_run()`          — `spreadsheets.values.get` for the `Values` report, and
  `spreadsheets.get` for the `Tabs` report.

Two scopes, because the split is real: `spreadsheets.readonly` reads the cells,
`drive.metadata.readonly` finds the files. A user who grants only one of them
gets a clear error naming the missing permission rather than a bare 403 — see
`require_scope`.

Sheets has no time dimension: a spreadsheet is current state, not a time series.
The date range is accepted and ignored, and every result says so in its `notes`.
"""

from __future__ import annotations
import logging
import re
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec
from terno_dbi.connectors.api.sources._multi import gather_accounts

logger = logging.getLogger(__name__)

_SHEETS_BASE = "https://sheets.googleapis.com/v4/spreadsheets"
_DRIVE_BASE = "https://www.googleapis.com/drive/v3"

_SPREADSHEET_MIME = "application/vnd.google-apps.spreadsheet"

_SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.metadata.readonly"

# Column ZZ is 702 columns — past any sheet used as a table, and far cheaper to
# request than the 18,278-column maximum.
_LAST_COLUMN = "ZZ"

# `list_fields` has no account context, so it has to guess which spreadsheets to
# describe, and each one costs a request. Cap the guess; a caller who wants a
# specific sheet's columns names it as the account in the query itself.
_MAX_HEADER_PROBE = 10

_VALUES = "Values"
_TABS = "Tabs"

_TAB_FIELDS: List[Field] = [
    Field("sheetId", "Sheet ID", "dimension",
          "Numeric ID of the tab, stable across renames."),
    Field("title", "Tab name", "dimension",
          "The tab's name — this is what the 'sheet_name' setting takes."),
    Field("index", "Position", "dimension",
          "Zero-based position of the tab in the spreadsheet.",
          data_type="integer"),
    Field("sheetType", "Type", "dimension", "GRID, OBJECT or DATA_SOURCE."),
    Field("hidden", "Hidden", "dimension",
          "Whether the tab is hidden in the UI.", data_type="boolean"),
    Field("rowCount", "Rows", "metric",
          "Rows allocated in the grid — its size, not the number of filled "
          "rows.", data_type="integer", is_non_aggregatable=True),
    Field("columnCount", "Columns", "metric",
          "Columns allocated in the grid.", data_type="integer",
          is_non_aggregatable=True),
]


# -- header handling --------------------------------------------------------

_NON_ID = re.compile(r"[^a-z0-9]+")


def _column_letter(index: int) -> str:
    """A1-notation column letter for a zero-based index (0 -> A, 26 -> AA)."""
    letters = ""
    index += 1
    while index:
        index, remainder = divmod(index - 1, 26)
        letters = chr(ord("A") + remainder) + letters
    return letters


def _slug(header: Any, position: int) -> str:
    """A stable field id for a header cell.

    Headers are human text ("Order Date", "Revenue (£)"), which no agent can
    type back reliably. They are lowercased and reduced to `[a-z0-9_]`; a blank
    or symbol-only header falls back to its column letter, so a sheet with gaps
    still produces addressable columns.
    """
    text = _NON_ID.sub("_", str(header or "").strip().lower()).strip("_")
    return text or f"column_{_column_letter(position).lower()}"


def _header_fields(header_row: List[Any],
                   group: Optional[str] = None) -> List[Field]:
    """Turn a header row into fields, with duplicate ids disambiguated.

    Two columns called "Total" would otherwise collide and silently drop one, so
    the second becomes `total_2`. The display name keeps the original text.

    `group` is the spreadsheet's name. It matters because every spreadsheet has
    a different schema, so a catalogue merged across several is meaningless
    unless each column says which sheet it belongs to — without it an agent will
    request one sheet's columns from another and get nothing back.
    """
    fields: List[Field] = []
    seen: Dict[str, int] = {}
    for position, raw in enumerate(header_row):
        base = _slug(raw, position)
        seen[base] = seen.get(base, 0) + 1
        fid = base if seen[base] == 1 else f"{base}_{seen[base]}"
        label = str(raw).strip() or _column_letter(position)
        fields.append(Field(
            fid,
            label,
            "dimension",
            f"Column {_column_letter(position)}"
            + (f" of '{group}'." if group else " of the sheet."),
            group=group,
        ))
    return fields


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
        raise _sheets_error(resp)
    return resp.json()


def _sheets_error(resp) -> ApiError:
    """Surface Google's own reason rather than a generic failure."""
    message, reason = "", ""
    try:
        err = (resp.json() or {}).get("error", {})
        message = err.get("message", "")
        errors = err.get("errors") or []
        if errors:
            reason = errors[0].get("reason", "")
    except ValueError:
        message = (resp.text or "")[:200]
    label = f"{resp.status_code} {reason}".strip()
    return ApiError(
        ErrorCode.UPSTREAM_ERROR,
        f"Google Sheets API error ({label}): {message or 'unknown error'}",
        retriable=resp.status_code == 429 or resp.status_code >= 500,
        details={"status": resp.status_code, "reason": reason},
    )


class _AuthError(Exception):
    """Internal marker for a 401 from Google, mapped to AUTH_EXPIRED."""


def _quote_tab(name: str) -> str:
    """Quote a tab name for A1 notation.

    A name with a space or a symbol ("Q3 Data") is invalid unquoted, and an
    apostrophe inside it is escaped by doubling — A1 notation, not SQL.
    """
    return "'" + str(name).replace("'", "''") + "'"


class GoogleSheetsConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http
        # A header row is read by both `list_fields` and `_run` within one
        # request; cache per spreadsheet so the sheet is fetched once.
        self._header_cache: Dict[str, List[Field]] = {}

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
            logger.warning("Sheets request failed: %s", exc)
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "Google Sheets returned an error. Try again.",
            )

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        """Every spreadsheet the credential can open, via Drive.

        `corpora=allDrives` so spreadsheets in shared drives are found too — a
        team's reporting sheet usually lives in one, and omitting it would make
        the connector look empty for exactly the orgs most likely to use it.

        Finding files is the *Drive* permission, so a user who granted only the
        Sheets scope is told which permission is missing instead of being handed
        a bare 403 from an API they never knowingly refused.
        """
        self.require_scope(_DRIVE_SCOPE, "Listing your spreadsheets")
        data = self._call("GET", f"{_DRIVE_BASE}/files", {
            "q": f"mimeType = '{_SPREADSHEET_MIME}' and trashed = false",
            "fields": "files(id,name,modifiedTime,webViewLink)",
            "orderBy": "modifiedTime desc",
            "pageSize": 200,
            "corpora": "allDrives",
            "includeItemsFromAllDrives": "true",
            "supportsAllDrives": "true",
        })
        accounts: List[Account] = []
        for item in data.get("files", []):
            sid = item.get("id")
            if not sid:
                continue
            extra = {}
            if item.get("modifiedTime"):
                extra["modified_time"] = item["modifiedTime"]
            if item.get("webViewLink"):
                extra["web_view_link"] = item["webViewLink"]
            accounts.append(Account(
                id=sid, name=item.get("name") or sid, extra=extra))
        return accounts

    def _headers_for(self, spreadsheet_id: str,
                     tab: Optional[str] = None,
                     header_row: int = 1,
                     group: Optional[str] = None) -> List[Field]:
        """The header row of one spreadsheet, as fields.

        Reads a single row rather than the sheet: discovery must stay cheap
        enough to run across several spreadsheets. With no tab named, A1
        notation resolves to the first visible tab — the same default `_run`
        uses, so what is discovered is what is queried.
        """
        cache_key = f"{spreadsheet_id}|{tab or ''}|{header_row}"
        cached = self._header_cache.get(cache_key)
        if cached is not None:
            return cached

        cell_range = f"A{header_row}:{_LAST_COLUMN}{header_row}"
        if tab:
            cell_range = f"{_quote_tab(tab)}!{cell_range}"
        data = self._call(
            "GET", f"{_SHEETS_BASE}/{spreadsheet_id}/values/{cell_range}",
            {"majorDimension": "ROWS"})
        values = data.get("values") or []
        fields = _header_fields(values[0] if values else [], group=group)
        self._header_cache[cache_key] = fields
        return fields

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        """Tab metadata for `Tabs`; the sheets' own columns for `Values`.

        The `Values` catalogue is the union of header fields across the most
        recently modified spreadsheets, first definition winning: two sheets
        with a `name` column share one field, and a column unique to one sheet
        still appears, so the union describes everything queryable. Whether a
        *given* sheet has a requested column is settled in `_run`, against that
        sheet's own header row.

        The probe is capped because each spreadsheet costs a request — this
        mirrors GA4, which merges metadata across properties the same way.
        """
        if report_type == _TABS:
            return list(_TAB_FIELDS)
        self.require_scope(_SHEETS_SCOPE, "Reading spreadsheet columns")
        merged: Dict[tuple, Field] = {}
        for account in self.list_accounts()[:_MAX_HEADER_PROBE]:
            for field in self._headers_for(account.id, group=account.name):
                # Keyed by (id, sheet): the same `date` column in two
                # spreadsheets is two different columns, and collapsing them
                # would hide one sheet's schema behind another's.
                merged.setdefault((field.id, field.group), field)
        return list(merged.values())

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        self.require_scope(_SHEETS_SCOPE, "Reading spreadsheet data")
        if spec.report_type == _TABS:
            return self._run_tabs(spec)
        return self._run_values(spec)

    @staticmethod
    def _value_range(settings: Dict[str, Any]) -> str:
        """The A1 range to read for the `Values` report.

        An explicit `range` wins and is passed through as given — a caller who
        writes 'Data!B2:F' means it. Otherwise the whole named tab, or, with no
        tab either, columns A..ZZ of the first visible tab.
        """
        tab = str(settings.get("sheet_name") or "").strip()
        explicit = str(settings.get("range") or "").strip()
        if explicit:
            if "!" in explicit or not tab:
                return explicit
            return f"{_quote_tab(tab)}!{explicit}"
        if tab:
            return _quote_tab(tab)
        return f"A:{_LAST_COLUMN}"

    @staticmethod
    def _header_row_index(settings: Dict[str, Any]) -> int:
        """Zero-based index of the header row within the fetched range."""
        raw = settings.get("header_row")
        if raw in (None, ""):
            return 0
        try:
            number = int(raw)
        except (TypeError, ValueError):
            raise ApiError(
                ErrorCode.INVALID_SETTING,
                f"header_row must be a row number, got {raw!r}.",
                details={"setting_id": "header_row"},
            )
        if number < 1:
            raise ApiError(
                ErrorCode.INVALID_SETTING,
                f"header_row must be 1 or greater, got {number}.",
                details={"setting_id": "header_row"},
            )
        return number - 1

    def _run_values(self, spec: QuerySpec) -> QueryResult:
        settings = spec.settings or {}
        cell_range = self._value_range(settings)
        header_index = self._header_row_index(settings)
        multi = len(spec.accounts) > 1

        # Column order follows the caller's request, but each sheet answers for
        # itself which columns exist — so requested ids are resolved per account
        # inside `fetch`, and this only records what to return.
        requested = list(spec.fields)

        # Which tab each spreadsheet actually answered with. Recorded because a
        # request that names no tab silently gets the first visible one, and a
        # spreadsheet with fifteen tabs then looks like it holds four columns.
        tabs_read: Dict[str, str] = {}

        def fetch(account):
            return self._fetch_values(
                account, cell_range, header_index, requested,
                max_rows=spec.max_rows, multi=multi, tabs_read=tabs_read)

        rows, warnings = gather_accounts(spec.accounts, fetch)

        # Always report the columns the rows are actually keyed by. A caller may
        # have asked by label ("Picked By") while the rows carry the canonical
        # id ("picked_by"); echoing the request back would then describe the
        # result wrongly. Falls back to the request only when there are no rows
        # to read the truth from.
        columns = _ordered_keys(rows) or requested
        notes = [_NO_DATE_NOTE]
        if not str((spec.settings or {}).get("sheet_name") or "").strip():
            named = ", ".join(sorted(set(tabs_read.values())))
            if named:
                notes.append(
                    f"No tab was named, so the first visible one was read "
                    f"({named}). A spreadsheet can hold many tabs with entirely "
                    f"different columns — run the 'Tabs' report to list them, "
                    f"then pass sheet_name to read a specific one."
                )
        return QueryResult(
            requested_field_ids=columns,
            rows=rows,
            row_count=len(rows),
            notes=notes,
            warnings=warnings,
        )

    def _fetch_values(self, spreadsheet_id: str, cell_range: str,
                      header_index: int, requested: List[str], *,
                      max_rows: int, multi: bool,
                      tabs_read: Optional[Dict[str, str]] = None
                      ) -> List[Dict[str, Any]]:
        """Read one spreadsheet's range and map its rows to field ids.

        `UNFORMATTED_VALUE` returns numbers as numbers, so a currency column
        arrives as 1234.5 rather than "£1,234.50" and a consumer can do
        arithmetic on it without parsing. Trailing empty cells are omitted by
        the API, so short rows are padded rather than losing their tail columns.
        """
        data = self._call(
            "GET", f"{_SHEETS_BASE}/{spreadsheet_id}/values/{cell_range}",
            {
                "majorDimension": "ROWS",
                "valueRenderOption": "UNFORMATTED_VALUE",
                "dateTimeRenderOption": "FORMATTED_STRING",
            })
        # The response echoes the range it resolved ("Unassigned!A1:ZZ1000"),
        # which is the only way to learn which tab a tab-less request hit.
        if tabs_read is not None:
            resolved = str(data.get("range") or "")
            if "!" in resolved:
                tabs_read[spreadsheet_id] = resolved.rsplit("!", 1)[0].strip("'")

        values = data.get("values") or []
        if len(values) <= header_index:
            return []

        fields = _header_fields(values[header_index])
        by_id = {f.id: position for position, f in enumerate(fields)}
        lookup = _column_lookup(fields)

        # Resolve every requested name to its canonical id. A caller that read
        # the *label* off list_fields ("Picked By") rather than the id
        # ("picked_by") is asking for a column that exists, so answer it —
        # refusing on spelling is the trap both vendors warn about.
        columns: List[str] = []
        for name in requested:
            resolved = lookup.get(name) or lookup.get(str(name).strip().lower())
            if resolved is None:
                raise invalid_field(name, list(by_id.keys()))
            columns.append(resolved)
        if not columns:
            columns = [f.id for f in fields]

        rows: List[Dict[str, Any]] = []
        for raw_row in values[header_index + 1:]:
            if len(rows) >= max_rows:
                break
            record: Dict[str, Any] = {}
            if multi:
                record["_account"] = spreadsheet_id
            for column in columns:
                position = by_id[column]
                value = raw_row[position] if position < len(raw_row) else None
                record[column] = value if value != "" else None
            rows.append(record)
        return rows

    def _run_tabs(self, spec: QuerySpec) -> QueryResult:
        """The tabs of each requested spreadsheet, with their grid sizes.

        The companion to `Values`: a caller needs a tab's name before it can
        pass one as `sheet_name`, and guessing "Sheet1" is wrong often enough to
        be worth a call.
        """
        known = {f.id for f in _TAB_FIELDS}
        unknown = [f for f in spec.fields if f not in known]
        if unknown:
            raise invalid_field(unknown[0], sorted(known))

        columns = list(spec.fields) or [f.id for f in _TAB_FIELDS]
        multi = len(spec.accounts) > 1

        def fetch(account):
            data = self._call("GET", f"{_SHEETS_BASE}/{account}", {
                "fields": "sheets.properties(sheetId,title,index,sheetType,"
                          "hidden,gridProperties(rowCount,columnCount))",
            })
            rows = []
            for sheet in data.get("sheets", []):
                properties = sheet.get("properties") or {}
                grid = properties.get("gridProperties") or {}
                source = {
                    "sheetId": properties.get("sheetId"),
                    "title": properties.get("title"),
                    "index": properties.get("index"),
                    "sheetType": properties.get("sheetType"),
                    # Sheets omits `hidden` entirely when a tab is visible.
                    "hidden": bool(properties.get("hidden", False)),
                    "rowCount": grid.get("rowCount"),
                    "columnCount": grid.get("columnCount"),
                }
                record: Dict[str, Any] = {}
                if multi:
                    record["_account"] = account
                for column in columns:
                    record[column] = source[column]
                rows.append(record)
            return rows

        rows, warnings = gather_accounts(spec.accounts, fetch)
        return QueryResult(
            requested_field_ids=columns,
            rows=rows,
            row_count=len(rows),
            notes=[_NO_DATE_NOTE],
            warnings=warnings,
        )


_NO_DATE_NOTE = (
    "A spreadsheet is current state, not a time series: Google Sheets has no "
    "date dimension, so any date_range supplied was ignored and these rows are "
    "the sheet as it stands now."
)


def _column_lookup(fields: List[Field]) -> Dict[str, str]:
    """Every accepted spelling of a column -> its canonical id.

    A sheet's columns are named by humans, so the id ("picked_by"), the header
    text as written ("Picked By") and its lowercased form all have to resolve to
    the same column. Ids are inserted last so they win any collision with a
    label — the canonical name must never lose to a coincidence.
    """
    lookup: Dict[str, str] = {}
    for field in fields:
        lookup.setdefault(field.name, field.id)
        lookup.setdefault(field.name.strip().lower(), field.id)
    for field in fields:
        lookup[field.id] = field.id
    return lookup


def _ordered_keys(rows: List[Dict[str, Any]]) -> List[str]:
    """Column ids across `rows`, in first-seen order, without `_account`."""
    keys: List[str] = []
    for row in rows:
        for key in row:
            if key != "_account" and key not in keys:
                keys.append(key)
    return keys


def make_google_sheets_connector(datasource) -> GoogleSheetsConnector:
    """Build a Sheets connector wired to refresh its own OAuth token when due."""
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return GoogleSheetsConnector(
        datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["GoogleSheetsConnector", "make_google_sheets_connector"]
