"""The Stripe connector, against mocked Stripe API v1 responses.

Exercises the Connect wiring on the wire (platform key, Stripe-Account,
pinned Stripe-Version), list paging with `starting_after`, `created` ranges in
the query's timezone, minor-to-major money in two-, zero- and three-decimal
currencies, the MRR arithmetic, error mapping, and every write action — each
checked before anything is sent, and a dry run never sending a write.
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import DateRange, QuerySpec
from terno_dbi.connectors.api.sources import stripe
from terno_dbi.connectors.api.sources.stripe import StripeConnector

BASE = "https://api.stripe.com/v1"
ACCT = "acct_1Acme"


class _Catalog:
    key = "stripe"
    report_types = [
        {"id": r, "settings": []}
        for r in ("Charges", "BalanceTransactions", "Refunds", "Disputes",
                  "Payouts", "Customers", "Invoices", "ActiveSubscriptions",
                  "Products", "Prices")
    ] + [{"id": "Subscriptions",
          "settings": [{"setting_id": "status", "required": False}]}]
    has_report_types = True


class _DS:
    id = 7
    type = "stripe"
    catalog = _Catalog()

    def __init__(self, account=ACCT):
        self.connection_json = {"STRIPE_USER_ID": account,
                                "GRANTED_SCOPES": "read_write"}


@pytest.fixture(autouse=True)
def platform_keys(monkeypatch):
    monkeypatch.setenv("TERNO_STRIPE_CLIENT_ID", "ca_platform")
    monkeypatch.setenv("TERNO_STRIPE_SECRET_KEY", "sk_test_platform")


class _Recorder:
    """Answers each call from `respond(method, path, params)` and records it."""

    def __init__(self, respond):
        self.respond = respond
        self.calls = []

    def __call__(self, method, url, params, headers):
        assert headers["Authorization"] == "Bearer sk_test_platform"
        assert headers["Stripe-Account"] == ACCT
        assert headers["Stripe-Version"] == stripe._API_VERSION
        path = url.split(BASE, 1)[1]
        self.calls.append((method, path, list(params),
                           headers.get("Idempotency-Key")))
        return self.respond(method, path, params)

    def writes(self):
        return [c for c in self.calls if c[0] != "GET"]


def _connector(respond, ds=None):
    rec = _Recorder(respond)
    return StripeConnector(ds or _DS(), http=rec), rec


def _page(items, has_more=False):
    return {"object": "list", "data": items, "has_more": has_more}


def _spec(fields, report_type="Charges", start="2026-08-01", end="2026-08-31",
          settings=None, max_rows=1000, tz="UTC"):
    return QuerySpec(
        accounts=[ACCT], fields=list(fields), date_range=DateRange(start, end),
        report_type=report_type, settings=settings or {}, max_rows=max_rows,
        timezone=tz,
    )


def _ts(*args, tz=timezone.utc):
    return int(datetime(*args, tzinfo=tz).timestamp())


def _charge(cid, amount=1099, currency="usd", **kw):
    charge = {"id": cid, "object": "charge", "created": _ts(2026, 8, 3, 9),
              "status": "succeeded", "paid": True, "captured": True,
              "refunded": False, "amount": amount, "amount_captured": amount,
              "amount_refunded": 0, "currency": currency,
              "customer": "cus_1", "description": "Pro plan",
              "balance_transaction": "txn_1",
              "billing_details": {"email": "ada@example.com",
                                  "address": {"country": "GB"}},
              "payment_method_details": {"type": "card",
                                         "card": {"brand": "visa",
                                                  "country": "US"}},
              "outcome": {"risk_level": "normal"}, "payment_intent": "pi_1"}
    charge.update(kw)
    return charge


# --- discovery -------------------------------------------------------------


class TestListAccounts:
    def test_lists_the_connected_account(self):
        conn, rec = _connector(lambda m, p, q: {
            "id": ACCT, "country": "GB", "default_currency": "gbp",
            "email": "billing@acme.com",
            "business_profile": {"name": "Acme Ltd"},
            "settings": {"dashboard": {"display_name": "Acme",
                                       "timezone": "Europe/London"}}})
        [account] = conn.list_accounts()
        assert rec.calls == [("GET", "/account", [], None)]
        assert account.as_dict() == {
            "id": ACCT, "name": "Acme", "currency": "GBP",
            "timezone": "Europe/London", "country": "GB", "livemode": False}

    def test_a_failed_lookup_still_lists_the_account(self):
        def respond(m, p, q):
            raise ApiError(ErrorCode.UPSTREAM_ERROR, "boom")
        conn, _ = _connector(respond)
        [account] = conn.list_accounts()
        assert (account.id, account.name) == (ACCT, ACCT)

    def test_revoked_access_is_not_papered_over(self):
        def respond(m, p, q):
            raise ApiError(ErrorCode.AUTH_EXPIRED, "revoked")
        conn, _ = _connector(respond)
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED

    def test_a_connection_without_an_account_needs_a_reconnect(self):
        conn, rec = _connector(lambda m, p, q: {}, ds=_DS(account=""))
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert exc.value.code == ErrorCode.AUTH_EXPIRED
        assert rec.calls == []

    def test_a_malformed_account_id_is_never_sent_as_a_header(self):
        conn, rec = _connector(lambda m, p, q: {},
                               ds=_DS(account="acct_1\r\nX-Evil: 1"))
        with pytest.raises(ApiError):
            conn.list_accounts()
        assert rec.calls == []

    def test_an_unconfigured_platform_key_is_reported(self, monkeypatch):
        monkeypatch.delenv("TERNO_STRIPE_SECRET_KEY")
        conn, _ = _connector(lambda m, p, q: {})
        with pytest.raises(ApiError) as exc:
            conn.list_accounts()
        assert "TERNO_STRIPE_SECRET_KEY" in exc.value.message

    def test_fields_are_per_report(self):
        conn, _ = _connector(lambda m, p, q: {})
        ids = {f.id for f in conn.list_fields("Subscriptions")}
        assert {"mrr", "current_period_end", "price_ids"} <= ids
        assert "fee" in {f.id for f in conn.list_fields("Charges")}
        mrr = next(f for f in conn.list_fields("Subscriptions") if f.id == "mrr")
        assert mrr.is_monetary and mrr.kind == "metric"


# --- reports ---------------------------------------------------------------


class TestCharges:
    def test_rows_are_projected_with_money_in_major_units(self):
        conn, rec = _connector(lambda m, p, q: _page([
            _charge("ch_2", amount_refunded=500, refunded=False)]))
        result = conn.query(_spec(
            ["id", "created", "amount", "amount_refunded", "currency",
             "card_brand", "billing_country", "customer_id"]))
        assert result.rows == [{
            "id": "ch_2", "created": "2026-08-03T09:00:00+00:00",
            "amount": 10.99, "amount_refunded": 5.0, "currency": "USD",
            "card_brand": "visa", "billing_country": "GB",
            "customer_id": "cus_1"}]
        method, path, params, _ = rec.calls[0]
        assert (method, path) == ("GET", "/charges")
        assert ("created[gte]", str(_ts(2026, 8, 1))) in params
        assert ("created[lt]", str(_ts(2026, 9, 1))) in params
        assert ("limit", "100") in params
        assert not any(k == "expand[]" for k, _ in params)
        assert "Amounts are in major currency units (10.99, not 1099 cents)." \
            in result.notes

    def test_the_range_runs_midnight_to_midnight_in_the_query_timezone(self):
        conn, rec = _connector(lambda m, p, q: _page([]))
        conn.query(_spec(["id"], start="2026-08-01", end="2026-08-01",
                         tz="Europe/Berlin"))
        params = rec.calls[0][2]
        berlin = ZoneInfo("Europe/Berlin")
        assert ("created[gte]", str(_ts(2026, 8, 1, tz=berlin))) in params
        assert ("created[lt]", str(_ts(2026, 8, 2, tz=berlin))) in params

    def test_fees_expand_the_balance_transaction_only_when_asked(self):
        charge = _charge("ch_1", amount=10000, currency="eur",
                         balance_transaction={"id": "txn_1", "fee": 320,
                                              "net": 10480, "currency": "usd"})
        conn, rec = _connector(lambda m, p, q: _page([charge]))
        result = conn.query(_spec(["id", "amount", "currency", "fee", "net",
                                   "settlement_currency"]))
        assert ("expand[]", "data.balance_transaction") in rec.calls[0][2]
        assert result.rows == [{"id": "ch_1", "amount": 100.0, "currency": "EUR",
                                "fee": 3.2, "net": 104.8,
                                "settlement_currency": "USD"}]
        # EUR charged, USD settled: the totals must not be mixed.
        assert any("2 currencies (EUR, USD)" in w for w in result.warnings)

    def test_zero_and_three_decimal_currencies(self):
        conn, _ = _connector(lambda m, p, q: _page([
            _charge("ch_jpy", amount=500, currency="jpy"),
            _charge("ch_kwd", amount=12345, currency="kwd"),
            _charge("ch_isk", amount=50000, currency="isk")]))
        result = conn.query(_spec(["id", "amount"]))
        assert [r["amount"] for r in result.rows] == [500, 12.345, 500.0]
        assert any("3 currencies" in w for w in result.warnings)

    def test_pages_follow_starting_after_and_stop_at_max_rows(self):
        pages = {None: _page([_charge("ch_3"), _charge("ch_2")], has_more=True),
                 "ch_2": _page([_charge("ch_1")], has_more=True)}

        def respond(m, p, q):
            return pages[dict(q).get("starting_after")]
        conn, rec = _connector(respond)
        result = conn.query(_spec(["id"], max_rows=3))
        assert [r["id"] for r in result.rows] == ["ch_3", "ch_2", "ch_1"]
        assert len(rec.calls) == 2
        assert ("starting_after", "ch_2") in rec.calls[1][2]
        assert ("limit", "3") in rec.calls[0][2]
        assert any("max_rows (3)" in w for w in result.warnings)

    def test_default_fields(self):
        conn, _ = _connector(lambda m, p, q: _page([_charge("ch_1")]))
        result = conn.query(_spec([]))
        assert result.requested_field_ids == list(stripe._CHARGES.default)

    def test_unknown_field_is_refused_with_a_suggestion(self):
        conn, rec = _connector(lambda m, p, q: _page([]))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(["ammount"]))
        assert exc.value.code == ErrorCode.INVALID_FIELD
        assert "amount" in exc.value.details["suggestions"]
        assert rec.calls == []

    def test_another_account_is_refused(self):
        conn, rec = _connector(lambda m, p, q: _page([]))
        spec = QuerySpec(accounts=["acct_other"], fields=["id"],
                         date_range=DateRange("2026-08-01", "2026-08-31"),
                         report_type="Charges")
        with pytest.raises(ApiError) as exc:
            conn.query(spec)
        assert exc.value.code == ErrorCode.ACCOUNT_FORBIDDEN
        assert rec.calls == []

    def test_relative_dates_are_refused(self):
        conn, _ = _connector(lambda m, p, q: _page([]))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(["id"], start="last_30_days"))
        assert exc.value.code == ErrorCode.INVALID_FILTER


def _sub(sid, items, status="active", currency="usd", **kw):
    sub = {"id": sid, "object": "subscription", "status": status,
           "created": _ts(2026, 8, 5), "customer": "cus_1",
           "currency": currency, "cancel_at_period_end": False,
           "items": {"data": items, "has_more": False}}
    sub.update(kw)
    return sub


def _item(unit_amount, interval="month", count=1, quantity=1, usage="licensed",
          price_id="price_1", product="prod_1", period_end=None):
    return {"quantity": quantity,
            "current_period_start": _ts(2026, 8, 5),
            "current_period_end": period_end or _ts(2026, 9, 5),
            "price": {"id": price_id, "product": product,
                      "unit_amount": unit_amount,
                      "recurring": {"interval": interval,
                                    "interval_count": count,
                                    "usage_type": usage}}}


class TestSubscriptions:
    def test_mrr_puts_every_interval_on_a_monthly_footing(self):
        subs = [
            _sub("sub_m", [_item(2000, quantity=3)]),              # 3 x 20/mo
            _sub("sub_y", [_item(120000, interval="year")]),        # 1,200/yr
            _sub("sub_q", [_item(9000, count=3)]),                  # 90/quarter
            _sub("sub_w", [_item(1000, interval="week")]),          # 10/week
            _sub("sub_2", [_item(1000), _item(500, price_id="price_2",
                                              product="prod_2")]),
        ]
        conn, _ = _connector(lambda m, p, q: _page(subs))
        result = conn.query(_spec(["id", "mrr", "quantity", "price_ids",
                                   "product_ids", "interval"],
                                  report_type="Subscriptions"))
        assert {r["id"]: r["mrr"] for r in result.rows} == {
            "sub_m": 60.0, "sub_y": 100.0, "sub_q": 30.0, "sub_w": 43.33,
            "sub_2": 15.0}
        two = next(r for r in result.rows if r["id"] == "sub_2")
        assert two["price_ids"] == "price_1,price_2"
        assert two["product_ids"] == "prod_1,prod_2"
        assert two["quantity"] == 2

    def test_metered_prices_leave_mrr_empty_and_say_so(self):
        conn, _ = _connector(lambda m, p, q: _page([
            _sub("sub_1", [_item(5, usage="metered", quantity=None)]),
            _sub("sub_2", [_item(None)])]))
        result = conn.query(_spec(["id", "mrr"], report_type="Subscriptions"))
        assert [r["mrr"] for r in result.rows] == [None, None]
        assert any("2 subscription(s) have a metered or tiered price" in w
                   for w in result.warnings)

    def test_status_defaults_to_all_and_can_be_chosen(self):
        conn, rec = _connector(lambda m, p, q: _page([]))
        conn.query(_spec(["id"], report_type="Subscriptions"))
        conn.query(_spec(["id"], report_type="Subscriptions",
                         settings={"status": "canceled"}))
        assert ("status", "all") in rec.calls[0][2]
        assert ("status", "canceled") in rec.calls[1][2]

    def test_an_unknown_status_is_refused(self):
        conn, rec = _connector(lambda m, p, q: _page([]))
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(["id"], report_type="Subscriptions",
                             settings={"status": "cancelled"}))
        assert exc.value.code == ErrorCode.INVALID_SETTING
        assert rec.calls == []

    def test_billing_periods_come_from_the_items(self):
        conn, _ = _connector(lambda m, p, q: _page([
            _sub("sub_1", [_item(1000, period_end=_ts(2026, 9, 5))])]))
        result = conn.query(_spec(["current_period_end"],
                                  report_type="Subscriptions"))
        assert result.rows == [{"current_period_end": "2026-09-05T00:00:00+00:00"}]

    def test_customer_email_expands_the_customer(self):
        sub = _sub("sub_1", [_item(1000)],
                   customer={"id": "cus_1", "email": "ada@example.com"})
        conn, rec = _connector(lambda m, p, q: _page([sub]))
        result = conn.query(_spec(["customer_id", "customer_email"],
                                  report_type="Subscriptions"))
        assert ("expand[]", "data.customer") in rec.calls[0][2]
        assert result.rows == [{"customer_id": "cus_1",
                                "customer_email": "ada@example.com"}]

    def test_active_subscriptions_are_a_snapshot_of_active_and_past_due(self):
        def respond(m, p, q):
            return _page([_sub(f"sub_{dict(q)['status']}", [_item(1000)],
                               status=dict(q)["status"])])
        conn, rec = _connector(respond)
        result = conn.query(_spec(["id", "status", "mrr"],
                                  report_type="ActiveSubscriptions"))
        assert [r["id"] for r in result.rows] == ["sub_active", "sub_past_due"]
        assert [dict(c[2])["status"] for c in rec.calls] == ["active", "past_due"]
        assert not any(k.startswith("created") for c in rec.calls
                       for k, _ in c[2])
        assert "the date range is ignored" in result.notes[0]


class TestOtherReports:
    def test_invoices_read_the_subscription_from_parent_and_sum_taxes(self):
        invoice = {"id": "in_1", "number": "ACME-0001", "status": "paid",
                   "created": _ts(2026, 8, 2), "currency": "usd",
                   "customer": "cus_1", "customer_email": "ada@example.com",
                   "subtotal": 10000, "total": 11500,
                   "total_taxes": [{"amount": 1000}, {"amount": 500}],
                   "total_discount_amounts": [],
                   "amount_paid": 11500, "amount_due": 11500,
                   "amount_remaining": 0,
                   "status_transitions": {"paid_at": _ts(2026, 8, 2, 1)},
                   "parent": {"type": "subscription_details",
                              "subscription_details": {"subscription": "sub_1"}}}
        conn, rec = _connector(lambda m, p, q: _page([invoice]))
        result = conn.query(_spec(["id", "subscription_id", "tax", "discount",
                                   "total", "paid_at"], report_type="Invoices"))
        assert rec.calls[0][1] == "/invoices"
        assert result.rows == [{"id": "in_1", "subscription_id": "sub_1",
                                "tax": 15.0, "discount": 0.0, "total": 115.0,
                                "paid_at": "2026-08-02T01:00:00+00:00"}]

    def test_balance_transactions_keep_their_sign(self):
        conn, _ = _connector(lambda m, p, q: _page([
            {"id": "txn_1", "type": "payout", "amount": -50000, "fee": 0,
             "net": -50000, "currency": "usd", "source": "po_1",
             "created": _ts(2026, 8, 9)}]))
        result = conn.query(_spec(["type", "amount", "net", "source_id"],
                                  report_type="BalanceTransactions"))
        assert result.rows == [{"type": "payout", "amount": -500.0,
                                "net": -500.0, "source_id": "po_1"}]

    def test_catalog_reports_ignore_the_date_range(self):
        conn, rec = _connector(lambda m, p, q: _page([
            {"id": "price_1", "product": "prod_1", "unit_amount": 2500,
             "currency": "usd", "type": "recurring",
             "recurring": {"interval": "month", "interval_count": 1}}]))
        result = conn.query(_spec(["id", "product_id", "unit_amount",
                                   "interval"], report_type="Prices"))
        assert rec.calls[0][1] == "/prices"
        assert not any(k.startswith("created") for k, _ in rec.calls[0][2])
        assert result.rows == [{"id": "price_1", "product_id": "prod_1",
                                "unit_amount": 25.0, "interval": "month"}]


# --- transport -------------------------------------------------------------


class _Resp:
    def __init__(self, status, body=None, headers=None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}

    def json(self):
        if self._body is None:
            raise ValueError("not json")
        return self._body


def _send(monkeypatch, resp, method="GET"):
    seen = {}

    def fake_request(m, url, headers=None, timeout=None, **kw):
        seen.update(method=m, url=url, headers=headers, **kw)
        return resp

    monkeypatch.setattr("requests.request", fake_request)
    out = stripe._default_http(method, f"{BASE}/charges",
                               [("limit", "1")], {"Authorization": "Bearer k"})
    return out, seen


def _refused(monkeypatch, status, error=None, method="GET", headers=None):
    with pytest.raises(ApiError) as exc:
        _send(monkeypatch, _Resp(status, {"error": error or {}}, headers),
              method)
    return exc.value


class TestTransport:
    def test_form_encoding_brackets_nested_values(self):
        assert stripe._form({
            "limit": 100, "created": {"gte": 1, "lt": 2},
            "expand": ["data.customer", "data.balance_transaction"],
            "metadata": {"plan": "pro", "seats": 3},
            "cancel_at_period_end": False, "skipped": None,
        }) == [
            ("limit", "100"), ("created[gte]", "1"), ("created[lt]", "2"),
            ("expand[]", "data.customer"),
            ("expand[]", "data.balance_transaction"),
            ("metadata[plan]", "pro"), ("metadata[seats]", "3"),
            ("cancel_at_period_end", "false"),
        ]

    def test_reads_send_a_query_and_writes_a_form(self, monkeypatch):
        out, seen = _send(monkeypatch, _Resp(200, {"ok": True}))
        assert out == {"ok": True}
        assert seen["params"] == [("limit", "1")] and "data" not in seen
        _, seen = _send(monkeypatch, _Resp(200, {}), method="POST")
        assert seen["data"] == [("limit", "1")] and "params" not in seen

    def test_revoked_access_is_auth_expired(self, monkeypatch):
        err = _refused(monkeypatch, 403, {
            "code": "account_invalid", "type": "invalid_request_error",
            "message": "The provided key does not have access to account"})
        assert err.code == ErrorCode.AUTH_EXPIRED
        assert "Reconnect" in err.message

    def test_a_bad_platform_key_names_the_setting(self, monkeypatch):
        err = _refused(monkeypatch, 401, {"type": "invalid_request_error"})
        assert err.code == ErrorCode.UPSTREAM_ERROR
        assert "TERNO_STRIPE_SECRET_KEY" in err.message
        assert err.retriable is False

    def test_429_is_rate_limited(self, monkeypatch):
        err = _refused(monkeypatch, 429, {"code": "rate_limit"},
                       headers={"Retry-After": "2"})
        assert err.code == ErrorCode.RATE_LIMITED
        assert err.retry_after_seconds == 2

    def test_a_refused_write_is_a_param_error(self, monkeypatch):
        err = _refused(monkeypatch, 400, {
            "code": "charge_already_refunded", "param": "charge",
            "message": "Charge ch_1 has already been refunded."}, method="POST")
        assert err.code == ErrorCode.INVALID_ACTION_PARAMS
        assert "already been refunded" in err.message
        assert err.details["param"] == "charge"

    def test_5xx_is_retriable(self, monkeypatch):
        with pytest.raises(ApiError) as exc:
            _send(monkeypatch, _Resp(502, None))
        assert exc.value.code == ErrorCode.UPSTREAM_ERROR
        assert exc.value.retriable is True

    def test_a_network_failure_is_wrapped(self):
        def boom(*a):
            raise ConnectionError("dns")
        conn = StripeConnector(_DS(), http=boom)
        with pytest.raises(ApiError) as exc:
            conn.query(_spec(["id"]))
        assert exc.value.code == ErrorCode.UPSTREAM_ERROR
        assert exc.value.retriable is True


# --- write actions ---------------------------------------------------------


_CUSTOMER = {"id": "cus_1", "object": "customer", "email": "old@example.com",
             "name": "Ada", "description": None, "phone": None,
             "metadata": {"plan": "free", "seats": "1"}}


def _writes_respond(objects, created=None):
    """GETs answer from `objects` by path; writes echo or return `created`."""
    def respond(method, path, params):
        if method == "GET":
            if path in objects:
                return objects[path]
            raise ApiError(ErrorCode.UPSTREAM_ERROR, "missing",
                           details={"status": 404, "code": "resource_missing"})
        if created is not None:
            return created(method, path, dict(params))
        return {**objects.get(path, {}), "id": path.rsplit("/", 1)[-1]}
    return respond


class TestCustomerActions:
    def test_lists_the_actions(self):
        conn, _ = _connector(_writes_respond({}))
        actions = {a.id: a for a in conn.list_actions()}
        assert list(actions) == ["create_customer", "update_customer",
                                 "create_refund", "cancel_subscription",
                                 "reactivate_subscription"]
        assert actions["create_customer"].destructive is False
        assert actions["create_refund"].destructive is True

    def test_create_posts_a_form_with_a_stable_idempotency_key(self):
        conn, rec = _connector(_writes_respond(
            {}, created=lambda m, p, q: {"id": "cus_new", **q}))
        params = {"email": " ada@example.com ", "name": "Ada",
                  "metadata": {"plan": "pro"}}
        result = conn.execute_action("create_customer", ACCT, params)
        again = conn.execute_action("create_customer", ACCT, params)
        (_, path, body, key), (_, _, _, key2) = rec.writes()
        assert path == "/customers"
        assert body == [("email", "ada@example.com"), ("name", "Ada"),
                        ("metadata[plan]", "pro")]
        assert key.startswith("terno-") and key == key2
        assert result.summary == "Created customer cus_new with: email, name, metadata."
        assert result.after["id"] == "cus_new"
        assert again.details["idempotency_key"] == key

    def test_create_needs_an_email_or_a_name(self):
        conn, rec = _connector(_writes_respond({}))
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_customer", ACCT, {"phone": "+44 1"})
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert rec.calls == []

    def test_dry_run_create_sends_nothing(self):
        conn, rec = _connector(_writes_respond({}))
        result = conn.execute_action("create_customer", ACCT,
                                     {"email": "ada@example.com"}, dry_run=True)
        assert rec.calls == []
        assert result.summary.startswith("[dry-run — not applied]")
        assert result.details["applied"] is False

    def test_update_reads_then_posts_only_what_changes(self):
        conn, rec = _connector(_writes_respond({"/customers/cus_1": _CUSTOMER}))
        result = conn.execute_action("update_customer", ACCT, {
            "customer_id": "cus_1", "email": "new@example.com",
            "description": None, "metadata": {"plan": "pro", "seats": None}})
        assert [c[:2] for c in rec.calls] == [("GET", "/customers/cus_1"),
                                              ("POST", "/customers/cus_1")]
        assert rec.writes()[0][2] == [
            ("email", "new@example.com"), ("description", ""),
            ("metadata[plan]", "pro"), ("metadata[seats]", "")]
        assert rec.writes()[0][3] is None     # an update is safe to repeat
        assert result.before == {"id": "cus_1", "email": "old@example.com",
                                 "description": None,
                                 "metadata": {"plan": "free", "seats": "1"}}
        assert result.after == {"id": "cus_1", "email": "new@example.com",
                                "description": None,
                                "metadata": {"plan": "pro", "seats": None}}

    def test_update_dry_run_reads_but_never_posts(self):
        conn, rec = _connector(_writes_respond({"/customers/cus_1": _CUSTOMER}))
        result = conn.execute_action("update_customer", ACCT, {
            "customer_id": "cus_1", "name": "Ada Lovelace"}, dry_run=True)
        assert rec.writes() == []
        assert result.before["name"] == "Ada"

    def test_a_missing_or_deleted_customer_is_a_param_error(self):
        conn, rec = _connector(_writes_respond(
            {"/customers/cus_gone": {"id": "cus_gone", "deleted": True}}))
        for cid in ("cus_gone", "cus_none"):
            with pytest.raises(ApiError) as exc:
                conn.execute_action("update_customer", ACCT,
                                    {"customer_id": cid, "name": "x"})
            assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
            assert cid in exc.value.message
        assert rec.writes() == []

    @pytest.mark.parametrize("params", [
        {"customer_id": "cus_1/../charges", "name": "x"},
        {"customer_id": "ch_1", "name": "x"},
        {"customer_id": "cus_1"},
        {"customer_id": "cus_1", "email": "not-an-email"},
        {"customer_id": "cus_1", "name": ""},
        {"customer_id": "cus_1", "metadata": {"k" * 41: "v"}},
        {"customer_id": "cus_1", "metadata": {"k": "v" * 501}},
        {"customer_id": "cus_1", "metadata": {"k": ["v"]}},
        {"customer_id": "cus_1", "address": {"country": "GB"}},
    ])
    def test_bad_params_are_refused_before_any_call(self, params):
        conn, rec = _connector(_writes_respond({"/customers/cus_1": _CUSTOMER}))
        with pytest.raises(ApiError) as exc:
            conn.execute_action("update_customer", ACCT, params)
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert rec.calls == []

    def test_unknown_action(self):
        conn, _ = _connector(_writes_respond({}))
        with pytest.raises(ApiError) as exc:
            conn.execute_action("delete_customer", ACCT, {"customer_id": "cus_1"})
        assert exc.value.code == ErrorCode.UNKNOWN_ACTION

    def test_another_account_is_refused(self):
        conn, rec = _connector(_writes_respond({}))
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_customer", "acct_other",
                                {"email": "ada@example.com"})
        assert exc.value.code == ErrorCode.ACCOUNT_FORBIDDEN
        assert rec.calls == []


def _refund_respond(charge, refunds=(), new_refund=None):
    def respond(method, path, params):
        if (method, path) == ("GET", f"/charges/{charge['id']}"):
            return charge
        if (method, path) == ("GET", "/payment_intents/pi_1"):
            assert ("expand[]", "latest_charge") in params
            return {"id": "pi_1", "status": "succeeded", "latest_charge": charge}
        if (method, path) == ("GET", "/refunds"):
            assert ("charge", charge["id"]) in params
            return _page(list(refunds))
        if (method, path) == ("POST", "/refunds"):
            return new_refund or {"id": "re_new", "status": "succeeded",
                                  "amount": int(dict(params).get(
                                      "amount", charge["amount_captured"]
                                      - charge["amount_refunded"]))}
        raise AssertionError(f"unexpected {method} {path}")
    return respond


class TestRefunds:
    def test_a_partial_refund_is_sent_in_minor_units(self):
        conn, rec = _connector(_refund_respond(_charge("ch_1", amount=5000)))
        result = conn.execute_action("create_refund", ACCT, {
            "charge_id": "ch_1", "amount": 12.5,
            "reason": "requested_by_customer"})
        (_, path, body, key), = rec.writes()
        assert body == [("charge", "ch_1"), ("amount", "1250"),
                        ("reason", "requested_by_customer")]
        assert key.startswith("terno-")
        assert result.summary == ("Refunded 12.5 USD of charge ch_1 (in part): "
                                  "refund re_new, status succeeded.")
        assert result.before["amount_refunded"] == 0.0
        assert result.after["amount_refunded"] == 12.5

    def test_no_amount_refunds_the_rest(self):
        charge = _charge("ch_1", amount=5000, amount_refunded=1250)
        conn, rec = _connector(_refund_respond(charge))
        result = conn.execute_action("create_refund", ACCT, {"charge_id": "ch_1"})
        assert rec.writes()[0][2] == [("charge", "ch_1")]
        assert "37.5 USD of charge ch_1 (the rest)" in result.summary
        assert result.after["amount_refunded"] == 50.0

    def test_a_zero_decimal_refund(self):
        conn, rec = _connector(_refund_respond(
            _charge("ch_1", amount=3000, currency="jpy")))
        conn.execute_action("create_refund", ACCT,
                            {"charge_id": "ch_1", "amount": 1000})
        assert ("amount", "1000") in rec.writes()[0][2]

    def test_by_payment_intent_refunds_its_latest_charge(self):
        conn, rec = _connector(_refund_respond(_charge("ch_1")))
        conn.execute_action("create_refund", ACCT, {"payment_intent_id": "pi_1"})
        assert rec.writes()[0][2] == [("charge", "ch_1")]

    @pytest.mark.parametrize("params, needle", [
        ({"charge_id": "ch_1", "amount": 60}, "left to refund"),
        ({"charge_id": "ch_1", "amount": 10.005}, "decimal places"),
        ({"charge_id": "ch_1", "amount": -1}, "positive amount"),
        ({"charge_id": "ch_1", "amount": True}, "positive amount"),
        ({"charge_id": "ch_1", "reason": "changed_mind"}, "reason"),
        ({"charge_id": "ch_1", "payment_intent_id": "pi_1"}, "exactly one"),
        ({}, "exactly one"),
        ({"charge_id": "cus_1"}, "Stripe id"),
    ])
    def test_bad_refunds_are_refused_before_any_write(self, params, needle):
        conn, rec = _connector(_refund_respond(_charge("ch_1", amount=5000)))
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_refund", ACCT, params)
        assert exc.value.code == ErrorCode.INVALID_ACTION_PARAMS
        assert needle in exc.value.message
        assert rec.writes() == []

    @pytest.mark.parametrize("charge, needle", [
        (_charge("ch_1", amount_refunded=1099, refunded=True), "fully refunded"),
        (_charge("ch_1", status="failed", paid=False), "not been paid"),
        (_charge("ch_1", captured=False, amount_captured=0), "not been paid"),
    ])
    def test_unrefundable_charges_are_refused(self, charge, needle):
        conn, rec = _connector(_refund_respond(charge))
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_refund", ACCT, {"charge_id": "ch_1"})
        assert needle in exc.value.message
        assert rec.writes() == []

    def test_a_payment_intent_without_a_charge_is_refused(self):
        def respond(method, path, params):
            return {"id": "pi_1", "status": "requires_payment_method",
                    "latest_charge": None}
        conn, rec = _connector(respond)
        with pytest.raises(ApiError) as exc:
            conn.execute_action("create_refund", ACCT,
                                {"payment_intent_id": "pi_1"})
        assert "no charge to refund" in exc.value.message
        assert rec.writes() == []

    def test_dry_run_checks_the_charge_but_never_refunds(self):
        conn, rec = _connector(_refund_respond(_charge("ch_1", amount=5000)))
        result = conn.execute_action("create_refund", ACCT,
                                     {"charge_id": "ch_1", "amount": 20},
                                     dry_run=True)
        assert rec.writes() == []
        assert [c[1] for c in rec.calls] == ["/charges/ch_1"]
        assert result.summary == ("[dry-run — not applied] Would refund 20 USD "
                                  "of charge ch_1 (in part).")

    def test_a_replayed_key_is_reported_as_the_earlier_refund(self):
        charge = _charge("ch_1", amount=5000, amount_refunded=1000)
        earlier = {"id": "re_old", "status": "succeeded", "amount": 1000}
        conn, _ = _connector(_refund_respond(charge, refunds=[earlier],
                                             new_refund=earlier))
        result = conn.execute_action("create_refund", ACCT,
                                     {"charge_id": "ch_1", "amount": 10})
        assert "nothing new was refunded" in result.summary
        assert result.details["replayed"] is True
        assert result.after == result.before


def _sub_respond(sub):
    def respond(method, path, params):
        if method == "GET":
            return sub
        if method == "DELETE":
            return {**sub, "status": "canceled"}
        return {**sub, **{k: (v == "true") if v in ("true", "false") else v
                          for k, v in params}}
    return respond


class TestSubscriptionActions:
    def test_cancel_defaults_to_the_end_of_the_period(self):
        conn, rec = _connector(_sub_respond(_sub("sub_1", [_item(1000)])))
        result = conn.execute_action("cancel_subscription", ACCT,
                                     {"subscription_id": "sub_1"})
        assert rec.writes() == [("POST", "/subscriptions/sub_1",
                                 [("cancel_at_period_end", "true")], None)]
        assert result.before["cancel_at_period_end"] is False
        assert result.after["cancel_at_period_end"] is True
        assert "end of its current period (2026-09-05T00:00:00+00:00)" \
            in result.summary

    def test_cancel_now_deletes(self):
        conn, rec = _connector(_sub_respond(_sub("sub_1", [_item(1000)])))
        result = conn.execute_action("cancel_subscription", ACCT, {
            "subscription_id": "sub_1", "at_period_end": False})
        assert [c[:2] for c in rec.writes()] == [("DELETE", "/subscriptions/sub_1")]
        assert result.after["status"] == "canceled"

    def test_an_already_scheduled_cancel_changes_nothing(self):
        conn, rec = _connector(_sub_respond(
            _sub("sub_1", [_item(1000)], cancel_at_period_end=True)))
        result = conn.execute_action("cancel_subscription", ACCT,
                                     {"subscription_id": "sub_1"})
        assert rec.writes() == []
        assert result.details == {"changed": False}

    def test_an_ended_subscription_cannot_be_cancelled_or_reactivated(self):
        conn, rec = _connector(_sub_respond(
            _sub("sub_1", [_item(1000)], status="canceled")))
        for action in ("cancel_subscription", "reactivate_subscription"):
            with pytest.raises(ApiError) as exc:
                conn.execute_action(action, ACCT, {"subscription_id": "sub_1"})
            assert "already ended" in exc.value.message
        assert rec.writes() == []

    def test_cancel_dry_run_never_writes(self):
        conn, rec = _connector(_sub_respond(_sub("sub_1", [_item(1000)])))
        result = conn.execute_action("cancel_subscription", ACCT, {
            "subscription_id": "sub_1", "at_period_end": False}, dry_run=True)
        assert rec.writes() == []
        assert "Would set subscription sub_1 to cancel immediately" in result.summary

    def test_at_period_end_must_be_a_boolean(self):
        conn, rec = _connector(_sub_respond(_sub("sub_1", [_item(1000)])))
        with pytest.raises(ApiError):
            conn.execute_action("cancel_subscription", ACCT, {
                "subscription_id": "sub_1", "at_period_end": "no"})
        assert rec.calls == []

    def test_reactivate_clears_a_period_end_cancellation(self):
        conn, rec = _connector(_sub_respond(
            _sub("sub_1", [_item(1000)], cancel_at_period_end=True)))
        result = conn.execute_action("reactivate_subscription", ACCT,
                                     {"subscription_id": "sub_1"})
        assert rec.writes()[0][2] == [("cancel_at_period_end", "false")]
        assert result.after["cancel_at_period_end"] is False

    def test_reactivate_clears_a_dated_cancellation(self):
        conn, rec = _connector(_sub_respond(
            _sub("sub_1", [_item(1000)], cancel_at=_ts(2026, 12, 1))))
        conn.execute_action("reactivate_subscription", ACCT,
                            {"subscription_id": "sub_1"})
        assert rec.writes()[0][2] == [("cancel_at", "")]

    def test_reactivate_with_nothing_scheduled_changes_nothing(self):
        conn, rec = _connector(_sub_respond(_sub("sub_1", [_item(1000)])))
        result = conn.execute_action("reactivate_subscription", ACCT,
                                     {"subscription_id": "sub_1"})
        assert rec.writes() == []
        assert result.details == {"changed": False}
