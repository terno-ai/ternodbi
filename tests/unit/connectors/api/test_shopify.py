"""The Shopify connector, against a mocked GraphQL Admin API.

Covers the single-store account, GraphQL pagination, money/field projection,
and the per-store OAuth wiring (validated shop domain, templated URLs).
"""

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.sources.shopify import ShopifyConnector
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec


class _Catalog:
    key = "shopify"
    report_types = [
        {"id": "Orders", "settings": []},
        {"id": "OrderLineItems", "settings": []},
        {"id": "DraftOrders", "settings": []},
        {"id": "AbandonedCheckouts", "settings": []},
        {"id": "Products", "settings": []},
        {"id": "ProductVariants", "settings": []},
        {"id": "Collections", "settings": []},
        {"id": "Customers", "settings": []},
        {"id": "Discounts", "settings": []},
    ]
    has_report_types = True


class _DS:
    type = "shopify"
    catalog = _Catalog()
    connection_json = {"ACCESS_TOKEN": "shpat_x", "INSTANCE": "acme.myshopify.com"}


def _orders_page(edges, has_next=False, cursor="cur1"):
    return {"data": {"orders": {
        "edges": edges,
        "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
    }}}


def _order_edge(name, total, cursor="c"):
    return {"cursor": cursor, "node": {
        "name": name, "createdAt": "2026-08-01T10:00:00Z",
        "displayFinancialStatus": "PAID", "displayFulfillmentStatus": "FULFILLED",
        "totalPriceSet": {"shopMoney": {"amount": total}},
        "customer": {"displayName": "Jane", "email": "jane@x.com"},
    }}


def _connector(responses):
    """responses: list of dicts returned in sequence from each GraphQL POST."""
    seq = list(responses)
    calls = []

    def http(method, url, headers, body=None):
        assert headers["X-Shopify-Access-Token"] == "shpat_x"
        assert "acme.myshopify.com/admin/api/" in url
        calls.append(body)
        return seq.pop(0)

    conn = ShopifyConnector(_DS(), http=http)
    conn._calls = calls
    return conn


class TestListAccounts:
    def test_returns_the_single_store(self):
        conn = _connector([{"data": {"shop": {
            "name": "Acme", "currencyCode": "USD",
            "myshopifyDomain": "acme.myshopify.com"}}}])
        accounts = conn.list_accounts()
        assert len(accounts) == 1
        assert accounts[0].id == "acme.myshopify.com"
        assert accounts[0].currency == "USD"

    def test_falls_back_to_the_store_domain_when_shop_lookup_403s(self):
        # A 403 reading the Shop object must not break account listing — the
        # store is already known from the connection's instance domain.
        def http(method, url, headers, body=None):
            raise ApiError(ErrorCode.UPSTREAM_ERROR, "Shopify API error (403).")
        conn = ShopifyConnector(_DS(), http=http)
        accounts = conn.list_accounts()
        assert len(accounts) == 1
        assert accounts[0].id == "acme.myshopify.com"
        assert accounts[0].name == "acme.myshopify.com"
        assert accounts[0].currency is None


class TestRunReport:
    def _spec(self, fields=("order_name", "total_price"), report_type="Orders"):
        return QuerySpec(
            accounts=["acme.myshopify.com"], fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type=report_type,
        )

    def test_projects_fields_and_coerces_money(self):
        conn = _connector([_orders_page([_order_edge("#1001", "42.50")])])
        result = conn.query(self._spec())
        assert result.rows[0] == {"order_name": "#1001", "total_price": 42.5}

    def test_created_at_filter_in_search_query(self):
        conn = _connector([_orders_page([_order_edge("#1", "1.0")])])
        conn.query(self._spec())
        variables = conn._calls[0]["variables"]
        assert variables["q"] == "created_at:>=2026-08-01 created_at:<=2026-08-31"

    def test_cursor_pagination_follows_hasNextPage(self):
        conn = _connector([
            _orders_page([_order_edge("#1", "1.0", "cA")], has_next=True, cursor="cA"),
            _orders_page([_order_edge("#2", "2.0", "cB")], has_next=False, cursor="cB"),
        ])
        result = conn.query(self._spec(fields=("order_name",)))
        assert [r["order_name"] for r in result.rows] == ["#1", "#2"]
        assert conn._calls[1]["variables"]["after"] == "cA"

    def test_unknown_field_rejected(self):
        conn = _connector([_orders_page([])])
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec(fields=("order_name", "nope")))
        assert exc.value.code == ErrorCode.INVALID_FIELD

    def test_graphql_error_is_surfaced(self):
        conn = _connector([{"errors": [{"message": "Throttled"}]}])
        with pytest.raises(ApiError) as exc:
            conn.query(self._spec())
        assert "Throttled" in exc.value.message

    def test_customers_report_uses_customers_root(self):
        def http(method, url, headers, body=None):
            assert "customers(" in body["query"]
            return {"data": {"customers": {"edges": [
                {"node": {"displayName": "Jane", "amountSpent": {"amount": "99.0"},
                          "numberOfOrders": 3}}],
                "pageInfo": {"hasNextPage": False, "endCursor": None}}}}
        conn = ShopifyConnector(_DS(), http=http)
        result = conn.query(self._spec(
            fields=("customer_name", "amount_spent", "number_of_orders"),
            report_type="Customers"))
        assert result.rows[0] == {"customer_name": "Jane", "amount_spent": 99.0,
                                  "number_of_orders": 3}


class TestExtraReports:
    def _spec(self, fields, report_type):
        return QuerySpec(
            accounts=["acme.myshopify.com"], fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type=report_type,
        )

    def test_product_variants_scalar_price_and_no_date_filter(self):
        captured = {}

        def http(method, url, headers, body=None):
            captured["vars"] = body["variables"]
            assert "productVariants(" in body["query"]
            return {"data": {"productVariants": {
                "edges": [{"node": {
                    "sku": "SKU1", "title": "Large", "price": "19.99",
                    "inventoryQuantity": 7, "product": {"title": "Tee"}}}],
                "pageInfo": {"hasNextPage": False, "endCursor": None}}}}
        conn = ShopifyConnector(_DS(), http=http)
        result = conn.query(self._spec(
            ("sku", "price", "inventory_quantity", "product_title"),
            "ProductVariants"))
        assert result.rows[0] == {"sku": "SKU1", "price": 19.99,
                                  "inventory_quantity": 7, "product_title": "Tee"}
        # Catalog report -> no date filter sent.
        assert captured["vars"]["q"] is None

    def test_collections_count_object(self):
        def http(method, url, headers, body=None):
            assert "collections(" in body["query"]
            return {"data": {"collections": {
                "edges": [{"node": {"title": "Sale", "handle": "sale",
                                    "updatedAt": "2026-08-01T00:00:00Z",
                                    "productsCount": {"count": 12}}}],
                "pageInfo": {"hasNextPage": False, "endCursor": None}}}}
        conn = ShopifyConnector(_DS(), http=http)
        result = conn.query(self._spec(("title", "products_count"), "Collections"))
        assert result.rows[0] == {"title": "Sale", "products_count": 12}

    def test_draft_orders_are_date_filtered(self):
        captured = {}

        def http(method, url, headers, body=None):
            captured["vars"] = body["variables"]
            assert "draftOrders(" in body["query"]
            return {"data": {"draftOrders": {
                "edges": [{"node": {"name": "#D1", "status": "OPEN",
                                    "createdAt": "2026-08-02T00:00:00Z",
                                    "totalPriceSet": {"shopMoney": {"amount": "50.0"}},
                                    "customer": {"displayName": "Jo"}}}],
                "pageInfo": {"hasNextPage": False, "endCursor": None}}}}
        conn = ShopifyConnector(_DS(), http=http)
        result = conn.query(self._spec(("name", "status", "total_price"), "DraftOrders"))
        assert result.rows[0] == {"name": "#D1", "status": "OPEN", "total_price": 50.0}
        assert captured["vars"]["q"] == "created_at:>=2026-08-01 created_at:<=2026-08-31"


class TestAdvancedReports:
    def _spec(self, fields, report_type):
        return QuerySpec(
            accounts=["acme.myshopify.com"], fields=list(fields),
            date_range=DateRange("2026-08-01", "2026-08-31"),
            report_type=report_type,
        )

    def test_abandoned_checkouts(self):
        def http(method, url, headers, body=None):
            assert "abandonedCheckouts(" in body["query"]
            return {"data": {"abandonedCheckouts": {
                "edges": [{"node": {"name": "#C1", "createdAt": "2026-08-02T0:0:0Z",
                                    "totalPriceSet": {"shopMoney": {"amount": "80.0"}},
                                    "customer": {"email": "a@b.com"}}}],
                "pageInfo": {"hasNextPage": False, "endCursor": None}}}}
        conn = ShopifyConnector(_DS(), http=http)
        r = conn.query(self._spec(("name", "total_price", "customer_email"),
                                  "AbandonedCheckouts"))
        assert r.rows[0] == {"name": "#C1", "total_price": 80.0,
                             "customer_email": "a@b.com"}

    def test_discounts_union_inline_fragments(self):
        def http(method, url, headers, body=None):
            assert "codeDiscountNodes(" in body["query"]
            assert "DiscountCodeBasic" in body["query"]   # inline fragment present
            return {"data": {"codeDiscountNodes": {
                "edges": [{"node": {"codeDiscount": {
                    "__typename": "DiscountCodeBasic", "title": "10% off",
                    "status": "ACTIVE", "asyncUsageCount": 42,
                    "codes": {"nodes": [{"code": "SAVE10"}]}}}}],
                "pageInfo": {"hasNextPage": False, "endCursor": None}}}}
        conn = ShopifyConnector(_DS(), http=http)
        r = conn.query(self._spec(("title", "code", "status", "usage_count"),
                                  "Discounts"))
        assert r.rows[0] == {"title": "10% off", "code": "SAVE10",
                             "status": "ACTIVE", "usage_count": 42}

    def test_order_line_items_expand_one_row_per_line(self):
        def http(method, url, headers, body=None):
            assert "lineItems(first:100)" in body["query"]
            return {"data": {"orders": {"edges": [{"node": {
                "name": "#1001", "createdAt": "2026-08-05T0:0:0Z",
                "lineItems": {"edges": [
                    {"node": {"title": "Tee", "quantity": 2, "sku": "T1",
                              "product": {"title": "Tee"},
                              "originalTotalSet": {"shopMoney": {"amount": "40.0"}}}},
                    {"node": {"title": "Cap", "quantity": 1, "sku": "C1",
                              "product": {"title": "Cap"},
                              "originalTotalSet": {"shopMoney": {"amount": "15.0"}}}},
                ]}}}],
                "pageInfo": {"hasNextPage": False, "endCursor": None}}}}
        conn = ShopifyConnector(_DS(), http=http)
        r = conn.query(self._spec(
            ("order_name", "product_title", "quantity", "original_total"),
            "OrderLineItems"))
        assert r.row_count == 2                       # one row per line item
        assert r.rows[0] == {"order_name": "#1001", "product_title": "Tee",
                             "quantity": 2, "original_total": 40.0}
        assert r.rows[1]["product_title"] == "Cap"


class TestMissingStore:
    def test_missing_instance_is_a_reconnect_error(self):
        class _NoShopDS(_DS):
            connection_json = {"ACCESS_TOKEN": "shpat_x"}
        conn = ShopifyConnector(_NoShopDS(), http=lambda *a, **k: {})
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED


class TestRegistration:
    def test_shopify_is_registered(self):
        from terno_dbi.connectors.api import registry
        assert registry.is_supported("shopify")

    def test_factory_wires_a_refresher(self):
        from terno_dbi.connectors.api.sources.shopify import make_shopify_connector
        conn = make_shopify_connector(_DS())
        assert callable(conn._token_refresher)


class TestPerStoreOAuth:
    def test_shop_domain_is_validated_and_templated(self, monkeypatch):
        monkeypatch.setenv("TERNO_SHOPIFY_CLIENT_ID", "cid")
        monkeypatch.setenv("TERNO_SHOPIFY_CLIENT_SECRET", "sec")
        from terno_dbi.connectors.api.auth.oauth import _validated_instance
        from terno_dbi.connectors.api.auth.providers import get_provider
        p = get_provider("shopify")
        # bare name normalises; full host passes; junk rejected.
        assert _validated_instance(p, "shopify", "acme") == "acme.myshopify.com"
        assert _validated_instance(p, "shopify", "acme.myshopify.com") == "acme.myshopify.com"
        for bad in ("evil.com", "acme.myshopify.com.evil.com", "", "a b"):
            with pytest.raises(ApiError):
                _validated_instance(p, "shopify", bad)

    def test_fixed_url_provider_ignores_instance(self):
        from terno_dbi.connectors.api.auth.oauth import _validated_instance
        from terno_dbi.connectors.api.auth.providers import get_provider
        # A non-instance provider always yields "" and never rejects.
        assert _validated_instance(get_provider("meta_ads"), "meta_ads", "x") == ""

    def test_requests_an_expiring_offline_token(self):
        # Non-expiring offline tokens are rejected by the Admin API; the exchange
        # must send expiring=1 so Shopify returns a refreshable token.
        from terno_dbi.connectors.api.auth.providers import get_provider
        assert get_provider("shopify").extra_token_params == {"expiring": "1"}
        assert get_provider("google_ads").extra_token_params == {}
