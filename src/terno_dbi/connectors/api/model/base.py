"""Base class for API connectors.

API connectors are intentionally separate from `connectors.base.BaseConnector`,
which is designed around SQLAlchemy engines, connection pools, and schema
reflection. API and SQL connectors share the catalog and tool interface, not
their implementation.

Concrete connectors provide `list_accounts()`, `list_fields()`, and `_run()`.
Common settings validation and token handling live here, while cross-cutting
concerns such as rate limiting, caching, dispatch, and token refresh belong to
the dispatch layer.
"""

from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.settings_validation import validate_settings
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec

logger = logging.getLogger(__name__)


class ApiConnector(ABC):
    """Represent one API source connected to an organisation.

    The source is built from its `DataSource`. Catalog metadata comes from
    `datasource.catalog`, while OAuth tokens are stored in the encrypted
    `connection_json`.

    Token freshness is handled here through `access_token()`, which uses the
    injected `token_refresher` to refresh only when needed. This keeps discovery,
    queries, and async jobs from having to manage token expiry themselves.

    The refresher is injected by the source factory rather than imported here,
    keeping the model layer independent of the OAuth implementation.
    """

    def __init__(self, datasource, token_refresher=None):
        self.datasource = datasource
        self.catalog = datasource.catalog
        self._token_refresher = token_refresher

    # -- identity -----------------------------------------------------------

    @property
    def key(self) -> str:
        return self.catalog.key if self.catalog else self.datasource.type

    # -- discovery ----------------------------------------------------------

    @abstractmethod
    def list_accounts(self) -> List[Account]:
        """Accounts/properties/channels reachable with this connection.

        Account-level RBAC (Phase 3) filters this to the caller's permitted set;
        a connector returns everything the credential can see.
        """

    @abstractmethod
    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        """Dimensions and metrics available, optionally scoped to a report type.

        Fetched from the provider (GA4's getMetadata is per-property) and cached;
        never static, because custom dimensions differ per account.
        """

    # -- query --------------------------------------------------------------

    def query(self, spec: QuerySpec) -> QueryResult:
        """Validate the query, then run it through the concrete connector.

        This remains the single entry point so connectors cannot bypass validation.
        Account-level authorization is enforced by the dispatch layer, not by
        individual connectors.
        """
        validate_settings(self.catalog, spec.report_type, spec.settings)
        return self._run(spec)

    @abstractmethod
    def _run(self, spec: QuerySpec) -> QueryResult:
        """Lower a validated QuerySpec to the provider's request and run it."""

    # -- auth ---------------------------------------------------------------

    def _tokens(self) -> Dict[str, Any]:
        """The decrypted token bundle from `connection_json`.

        For an API source, `connection_json` holds the OAuth tokens as an
        encrypted envelope (`services.secrets`). Legacy plaintext is still read,
        so a datasource written before encryption keeps working. Kept here so
        every connector reads tokens exactly one way.
        """
        from terno_dbi.services.secrets import decrypt_dict

        raw = self.datasource.connection_json
        if not raw:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} is not connected. Reconnect the source and retry.",
            )
        tokens = decrypt_dict(raw)
        # A dict is required. `decrypt_dict` returns the value unchanged for
        # anything it cannot decrypt (e.g. a legacy plaintext string), so guard
        # against a non-dict here rather than letting `.get()` blow up later.
        if not tokens or not isinstance(tokens, dict):
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} credentials are unreadable; reconnect the source.",
            )
        return tokens

    def access_token(self) -> str:
        """A currently-valid access token for a provider call.

        Refreshes first if the injected refresher says the token is due — a cheap
        no-op when it is not. Because every provider request reads its token here,
        this is the single point that keeps freshness true everywhere.
        """
        if self._token_refresher is not None:
            self._token_refresher()
        token = self._tokens().get("ACCESS_TOKEN") or self._tokens().get("access_token")
        if not token:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} has no usable access token; reconnect the source.",
            )
        return token


__all__ = ["ApiConnector"]
