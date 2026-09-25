"""Shopify connector (GraphQL Admin API).

Shopify differs from the other connectors:

- **One store per connection.** OAuth is per-store, and the store domain is saved
  on the datasource at connect time (`INSTANCE`). `list_accounts()` returns that
  single shop.
- **GraphQL Admin API only** — the REST Admin API is legacy; new apps must use
  GraphQL. `_run` runs a fixed GraphQL query per report type, paginates with the
  cursor, and projects each node to the requested columns.
- **Expiring offline access token** — the Admin API no longer accepts
  non-expiring offline tokens, so the connection uses an expiring one (1-hour
  access token + refresh token) and refreshes like the other OAuth connectors.
- Auth is the `X-Shopify-Access-Token` header (not a bearer).
- Money is already in the shop currency (a decimal string), not micros.

Reports are CRM/commerce objects (orders, products, customers), filtered to the
date range by `created_at` via Shopify's search syntax.
"""

from __future__ import annotations
import logging
from datetime import timezone as _tz
from typing import Any, Callable, Dict, List, Optional

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import Account, Field, QueryResult, QuerySpec

logger = logging.getLogger(__name__)

_API_VERSION = "2026-07"
_PAGE = 180                 # GraphQL connection page size (max 250; 100 is safe)
_MAX_TOTAL = 100000          # safety cap on rows per query


def _money(node: Dict[str, Any], key: str):
    """Extract `<key>.shopMoney.amount` from a MoneyBag field."""
    bag = (node.get(key) or {}).get("shopMoney") or {}
    return bag.get("amount")


def _nested(node: Dict[str, Any], outer: str, inner: str):
    return (node.get(outer) or {}).get(inner)


class _Report:
    """A report type: its GraphQL root connection, node selection, and the
    per-column extractors that map a node to the connector's flat field ids."""

    def __init__(self, root: str, node_selection: str, fields: List[Field],
                 extractors: Dict[str, Callable[[Dict[str, Any]], Any]],
                 date_field: Optional[str] = "created_at",
                 child_path: Optional[str] = None,
                 child_extractors: Optional[Dict[str, Callable]] = None):
        self.root = root
        self.node_selection = node_selection
        self.fields = fields
        self.catalogue = {f.id: f for f in fields}
        self.extractors = extractors
        self.date_field = date_field
        self.child_path = child_path
        self.child_extractors = child_extractors or {}


_ORDERS = _Report(
    root="orders",
    node_selection="""
      name createdAt processedAt
      displayFinancialStatus displayFulfillmentStatus
      totalPriceSet { shopMoney { amount } }
      subtotalPriceSet { shopMoney { amount } }
      totalTaxSet { shopMoney { amount } }
      totalDiscountsSet { shopMoney { amount } }
      customer { displayName email }
    """,
    fields=[
        Field("order_name", "Order", "dimension", "Order number (e.g. #1001)."),
        Field("created_at", "Created", "dimension", "When the order was placed.",
              data_type="date"),
        Field("financial_status", "Financial status", "dimension"),
        Field("fulfillment_status", "Fulfillment status", "dimension"),
        Field("total_price", "Total", "metric", "Order total in shop currency.",
              data_type="number", is_monetary=True),
        Field("subtotal_price", "Subtotal", "metric", data_type="number",
              is_monetary=True),
        Field("total_tax", "Tax", "metric", data_type="number", is_monetary=True),
        Field("total_discounts", "Discounts", "metric", data_type="number",
              is_monetary=True),
        Field("customer_name", "Customer", "dimension"),
        Field("customer_email", "Customer email", "dimension"),
    ],
    extractors={
        "order_name": lambda n: n.get("name"),
        "created_at": lambda n: n.get("createdAt"),
        "financial_status": lambda n: n.get("displayFinancialStatus"),
        "fulfillment_status": lambda n: n.get("displayFulfillmentStatus"),
        "total_price": lambda n: _money(n, "totalPriceSet"),
        "subtotal_price": lambda n: _money(n, "subtotalPriceSet"),
        "total_tax": lambda n: _money(n, "totalTaxSet"),
        "total_discounts": lambda n: _money(n, "totalDiscountsSet"),
        "customer_name": lambda n: _nested(n, "customer", "displayName"),
        "customer_email": lambda n: _nested(n, "customer", "email"),
    },
)

_PRODUCTS = _Report(
    root="products",
    node_selection="""
      title productType vendor status createdAt totalInventory
      priceRangeV2 { minVariantPrice { amount } maxVariantPrice { amount } }
    """,
    fields=[
        Field("title", "Title", "dimension", "Product title."),
        Field("product_type", "Type", "dimension"),
        Field("vendor", "Vendor", "dimension"),
        Field("status", "Status", "dimension", "ACTIVE, ARCHIVED or DRAFT."),
        Field("created_at", "Created", "dimension", data_type="date"),
        Field("total_inventory", "Inventory", "metric", data_type="integer"),
        Field("min_price", "Min price", "metric", data_type="number",
              is_monetary=True, is_non_aggregatable=True),
        Field("max_price", "Max price", "metric", data_type="number",
              is_monetary=True, is_non_aggregatable=True),
    ],
    extractors={
        "title": lambda n: n.get("title"),
        "product_type": lambda n: n.get("productType"),
        "vendor": lambda n: n.get("vendor"),
        "status": lambda n: n.get("status"),
        "created_at": lambda n: n.get("createdAt"),
        "total_inventory": lambda n: n.get("totalInventory"),
        "min_price": lambda n: (n.get("priceRangeV2") or {}).get(
            "minVariantPrice", {}).get("amount"),
        "max_price": lambda n: (n.get("priceRangeV2") or {}).get(
            "maxVariantPrice", {}).get("amount"),
    },
)

_CUSTOMERS = _Report(
    root="customers",
    node_selection="""
      displayName email createdAt numberOfOrders state
      amountSpent { amount }
    """,
    fields=[
        Field("customer_name", "Customer", "dimension"),
        Field("email", "Email", "dimension"),
        Field("created_at", "Created", "dimension", data_type="date"),
        Field("state", "State", "dimension",
              "Account state (ENABLED, DISABLED, INVITED, DECLINED)."),
        Field("number_of_orders", "Orders", "metric", data_type="integer"),
        Field("amount_spent", "Amount spent", "metric", data_type="number",
              is_monetary=True),
    ],
    extractors={
        "customer_name": lambda n: n.get("displayName"),
        "email": lambda n: n.get("email"),
        "created_at": lambda n: n.get("createdAt"),
        "state": lambda n: n.get("state"),
        "number_of_orders": lambda n: n.get("numberOfOrders"),
        "amount_spent": lambda n: (n.get("amountSpent") or {}).get("amount"),
    },
)

_DRAFT_ORDERS = _Report(
    root="draftOrders",
    node_selection="""
      name createdAt status
      totalPriceSet { shopMoney { amount } }
      customer { displayName email }
    """,
    fields=[
        Field("name", "Draft", "dimension", "Draft order name (e.g. #D1)."),
        Field("created_at", "Created", "dimension", data_type="date"),
        Field("status", "Status", "dimension",
              "OPEN, INVOICE_SENT or COMPLETED."),
        Field("total_price", "Total", "metric", data_type="number",
              is_monetary=True),
        Field("customer_name", "Customer", "dimension"),
        Field("customer_email", "Customer email", "dimension"),
    ],
    extractors={
        "name": lambda n: n.get("name"),
        "created_at": lambda n: n.get("createdAt"),
        "status": lambda n: n.get("status"),
        "total_price": lambda n: _money(n, "totalPriceSet"),
        "customer_name": lambda n: _nested(n, "customer", "displayName"),
        "customer_email": lambda n: _nested(n, "customer", "email"),
    },
)


_VARIANTS = _Report(
    root="productVariants",
    node_selection="""
      sku title price inventoryQuantity
      product { title }
    """,
    fields=[
        Field("sku", "SKU", "dimension"),
        Field("variant_title", "Variant", "dimension"),
        Field("product_title", "Product", "dimension"),
        Field("price", "Price", "metric", data_type="number", is_monetary=True,
              is_non_aggregatable=True),
        Field("inventory_quantity", "Inventory", "metric", data_type="integer"),
    ],
    extractors={
        "sku": lambda n: n.get("sku"),
        "variant_title": lambda n: n.get("title"),
        "product_title": lambda n: _nested(n, "product", "title"),
        "price": lambda n: n.get("price"),
        "inventory_quantity": lambda n: n.get("inventoryQuantity"),
    },
    date_field=None,
)

_COLLECTIONS = _Report(
    root="collections",
    node_selection="""
      title handle updatedAt
      productsCount { count }
    """,
    fields=[
        Field("title", "Collection", "dimension"),
        Field("handle", "Handle", "dimension"),
        Field("updated_at", "Updated", "dimension", data_type="date"),
        Field("products_count", "Products", "metric", data_type="integer"),
    ],
    extractors={
        "title": lambda n: n.get("title"),
        "handle": lambda n: n.get("handle"),
        "updated_at": lambda n: n.get("updatedAt"),
        "products_count": lambda n: (n.get("productsCount") or {}).get("count"),
    },
    date_field=None,
)

_ABANDONED = _Report(
    root="abandonedCheckouts",
    node_selection="""
      name createdAt completedAt
      totalPriceSet { shopMoney { amount } }
      customer { displayName email }
    """,
    fields=[
        Field("name", "Checkout", "dimension", "Abandoned checkout name."),
        Field("created_at", "Created", "dimension", data_type="date"),
        Field("completed_at", "Completed", "dimension",
              "Set if later recovered/completed.", data_type="date"),
        Field("total_price", "Total", "metric", data_type="number",
              is_monetary=True),
        Field("customer_name", "Customer", "dimension"),
        Field("customer_email", "Customer email", "dimension"),
    ],
    extractors={
        "name": lambda n: n.get("name"),
        "created_at": lambda n: n.get("createdAt"),
        "completed_at": lambda n: n.get("completedAt"),
        "total_price": lambda n: _money(n, "totalPriceSet"),
        "customer_name": lambda n: _nested(n, "customer", "displayName"),
        "customer_email": lambda n: _nested(n, "customer", "email"),
    },
)

# Discounts: codeDiscountNodes wraps a `codeDiscount` union — inline fragments
# pull the fields shared across the code-discount types. Not time series.
_DISCOUNT_SELECTION = """
  codeDiscount {
    __typename
    ... on DiscountCodeBasic { title status startsAt endsAt asyncUsageCount
      summary codes(first:1){ nodes { code } } }
    ... on DiscountCodeBxgy { title status startsAt endsAt asyncUsageCount
      summary codes(first:1){ nodes { code } } }
    ... on DiscountCodeFreeShipping { title status startsAt endsAt asyncUsageCount
      summary codes(first:1){ nodes { code } } }
    ... on DiscountCodeApp { title status startsAt endsAt asyncUsageCount
      codes(first:1){ nodes { code } } }
  }
"""


def _disc(node, key):
    return (node.get("codeDiscount") or {}).get(key)


def _disc_code(node):
    nodes = ((node.get("codeDiscount") or {}).get("codes") or {}).get("nodes") or []
    return nodes[0].get("code") if nodes else None


_DISCOUNTS = _Report(
    root="codeDiscountNodes",
    node_selection=_DISCOUNT_SELECTION,
    fields=[
        Field("title", "Discount", "dimension"),
        Field("code", "Code", "dimension", "The discount code customers enter."),
        Field("status", "Status", "dimension", "ACTIVE, EXPIRED or SCHEDULED."),
        Field("summary", "Summary", "dimension", "Human-readable discount value."),
        Field("starts_at", "Starts", "dimension", data_type="date"),
        Field("ends_at", "Ends", "dimension", data_type="date"),
        Field("usage_count", "Times used", "metric", data_type="integer"),
    ],
    extractors={
        "title": lambda n: _disc(n, "title"),
        "code": _disc_code,
        "status": lambda n: _disc(n, "status"),
        "summary": lambda n: _disc(n, "summary"),
        "starts_at": lambda n: _disc(n, "startsAt"),
        "ends_at": lambda n: _disc(n, "endsAt"),
        "usage_count": lambda n: _disc(n, "asyncUsageCount"),
    },
    date_field=None,
)

# Best-sellers: order line items. Each order expands into one row per line item,
# carrying the order's name/date onto every line.
_LINE_ITEMS = _Report(
    root="orders",
    node_selection="""
      name createdAt
      lineItems(first:100) { edges { node {
        title quantity sku
        product { title }
        variant { sku }
        originalTotalSet { shopMoney { amount } }
        discountedTotalSet { shopMoney { amount } }
      } } }
    """,
    fields=[
        Field("order_name", "Order", "dimension"),
        Field("created_at", "Order date", "dimension", data_type="date"),
        Field("line_title", "Line item", "dimension", "Title on the order line."),
        Field("product_title", "Product", "dimension"),
        Field("sku", "SKU", "dimension"),
        Field("quantity", "Quantity", "metric", data_type="integer"),
        Field("original_total", "Line total", "metric", data_type="number",
              is_monetary=True),
        Field("discounted_total", "Line total (after discounts)", "metric",
              data_type="number", is_monetary=True),
    ],
    extractors={
        "order_name": lambda n: n.get("name"),
        "created_at": lambda n: n.get("createdAt"),
    },
    child_path="lineItems",
    child_extractors={
        "line_title": lambda ln: ln.get("title"),
        "product_title": lambda ln: _nested(ln, "product", "title"),
        "sku": lambda ln: ln.get("sku") or _nested(ln, "variant", "sku"),
        "quantity": lambda ln: ln.get("quantity"),
        "original_total": lambda ln: _money(ln, "originalTotalSet"),
        "discounted_total": lambda ln: _money(ln, "discountedTotalSet"),
    },
)

_REPORTS: Dict[str, _Report] = {
    "Orders": _ORDERS,
    "Products": _PRODUCTS,
    "Customers": _CUSTOMERS,
    "DraftOrders": _DRAFT_ORDERS,
    "ProductVariants": _VARIANTS,
    "Collections": _COLLECTIONS,
    "AbandonedCheckouts": _ABANDONED,
    "Discounts": _DISCOUNTS,
    "OrderLineItems": _LINE_ITEMS,
}
_DEFAULT_REPORT = "Orders"


def _report_for(report_type: Optional[str]) -> _Report:
    return _REPORTS.get(report_type or _DEFAULT_REPORT, _ORDERS)


def _default_http(method: str, url: str, headers: Dict[str, str],
                  json_body: Optional[Dict] = None) -> Dict[str, Any]:
    import requests
    resp = requests.request(method, url, headers=headers, json=json_body,
                            timeout=60)
    if resp.status_code == 401:
        raise _AuthError()
    if resp.status_code >= 400:
        raise ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"Shopify API error ({resp.status_code}).",
            retriable=resp.status_code == 429 or resp.status_code >= 500,
        )
    return resp.json()


class _AuthError(Exception):
    """Internal marker for a 401 from Shopify, mapped to AUTH_EXPIRED."""


class ShopifyConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http

    # -- transport ----------------------------------------------------------

    def _shop(self) -> str:
        shop = self._tokens().get("INSTANCE") or self._tokens().get("SHOP_DOMAIN")
        if not shop:
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} is missing its store domain; reconnect the source.",
                retriable=False,
            )
        return shop

    def _graphql(self, query: str, variables: Optional[Dict] = None) -> Dict[str, Any]:
        url = f"https://{self._shop()}/admin/api/{_API_VERSION}/graphql.json"
        headers = {
            "X-Shopify-Access-Token": self.access_token(),
            "Content-Type": "application/json",
        }
        try:
            resp = self._http("POST", url, headers,
                              {"query": query, "variables": variables or {}})
        except _AuthError:
            raise ApiError(ErrorCode.AUTH_EXPIRED,
                           f"{self.key} access was rejected; reconnect the source.")
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("Shopify request failed: %s", exc)
            raise ApiError(ErrorCode.UPSTREAM_ERROR,
                           "Shopify returned an error. Try again.")
        if resp.get("errors"):
            msg = resp["errors"][0].get("message") if isinstance(
                resp["errors"], list) and resp["errors"] else str(resp["errors"])
            raise ApiError(ErrorCode.UPSTREAM_ERROR, f"Shopify GraphQL error: {msg}")
        return resp.get("data") or {}

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        domain = self._shop()
        try:
            data = self._graphql("{ shop { name currencyCode myshopifyDomain } }")
            shop = data.get("shop") or {}
            return [Account(
                id=shop.get("myshopifyDomain") or domain,
                name=shop.get("name") or domain,
                currency=shop.get("currencyCode"),
            )]
        except ApiError:
            logger.info("Shopify shop lookup failed; using store domain as the "
                        "account for %s", domain)
            return [Account(id=domain, name=domain, currency=None)]

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        return list(_report_for(report_type).fields)

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = spec.report_type if spec.report_type in _REPORTS else _DEFAULT_REPORT
        report = _report_for(report_type)

        unknown = [f for f in spec.fields if f not in report.catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(report.catalogue.keys()))

        requested = list(spec.fields) or list(report.catalogue.keys())
        # Time-series reports filter by their date field; catalog reports
        # (variants, collections) ignore the date range.
        search = None
        if report.date_field:
            search = (f"{report.date_field}:>={spec.date_range.start} "
                      f"{report.date_field}:<={spec.date_range.end}")
        query = _build_query(report)

        max_total = min(spec.max_rows, _MAX_TOTAL)
        rows: List[Dict[str, Any]] = []
        after: Optional[str] = None
        while len(rows) < max_total:
            data = self._graphql(query, {
                "q": search,
                "first": min(max_total - len(rows), _PAGE),
                "after": after,
            })
            conn = data.get(report.root) or {}
            edges = conn.get("edges") or []
            for edge in edges:
                node = edge.get("node") or {}
                if report.child_path:
                    rows.extend(_expand_children(report, node, requested))
                else:
                    rows.append({c: _coerce(c, report.extractors[c](node), report)
                                 for c in requested})
            page = conn.get("pageInfo") or {}
            after = page.get("endCursor")
            if not page.get("hasNextPage") or not edges:
                break

        note = ("Records are filtered by created_at within the selected date range."
                if report.date_field
                else "This report is a catalog snapshot; the date range is ignored.")
        return QueryResult(
            requested_field_ids=requested,
            rows=rows[:max_total],
            row_count=len(rows[:max_total]),
            notes=[note],
        )


def _expand_children(report: _Report, node: Dict[str, Any],
                     requested: List[str]) -> List[Dict[str, Any]]:
    """One row per child (e.g. order line item), merging parent + child fields."""
    edges = ((node.get(report.child_path) or {}).get("edges")) or []
    out: List[Dict[str, Any]] = []
    for edge in edges:
        child = edge.get("node") or {}
        row: Dict[str, Any] = {}
        for c in requested:
            if c in report.child_extractors:
                row[c] = _coerce(c, report.child_extractors[c](child), report)
            elif c in report.extractors:
                row[c] = _coerce(c, report.extractors[c](node), report)
            else:
                row[c] = None
        out.append(row)
    return out


def _build_query(report: _Report) -> str:
    return (
        "query($q:String,$first:Int!,$after:String){"
        f"  {report.root}(first:$first, query:$q, after:$after){{"
        "    edges { cursor node {" + report.node_selection + "} }"
        "    pageInfo { hasNextPage endCursor }"
        "  }"
        "}"
    )


def _coerce(field_id, raw, report: _Report):
    if raw is None:
        return None
    field = report.catalogue.get(field_id)
    if field and field.kind == "metric":
        try:
            f = float(raw)
            return int(f) if f.is_integer() else f
        except (TypeError, ValueError):
            return raw
    return raw


def make_shopify_connector(datasource) -> ShopifyConnector:
    """Build a Shopify connector.

    Shopify's Admin API no longer accepts non-expiring offline tokens, so the
    connection uses an *expiring* offline token (1-hour access token + refresh
    token). The refresher renews it before each call, exactly like the other
    OAuth connectors.
    """
    from terno_dbi.connectors.api.auth.oauth import make_ensure_token
    return ShopifyConnector(datasource, token_refresher=make_ensure_token(datasource))


__all__ = ["ShopifyConnector", "make_shopify_connector"]
