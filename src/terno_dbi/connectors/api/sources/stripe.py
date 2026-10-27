"""Stripe connector (REST API v1, through Stripe Connect).

Implements the `ApiConnector` interface against the Stripe API directly:

- `list_accounts()`  — `GET /account`: the one connected Stripe account.
- `list_fields()`    — a *curated* catalogue per report type.
- `_run()`           — one list endpoint per report type (`/charges`,
                       `/balance_transactions`, `/subscriptions`, …).
- `execute_action()` — create and update customers, refund a charge, cancel or
                       reactivate a subscription.

Stripe differs from the other connectors:
  * the user connects their account to this deployment's Connect platform
    (OAuth, `read_write` scope). The token response names the connected
    account (`stripe_user_id`), and every request is made with the platform's
    own secret key plus a `Stripe-Account` header naming it — the access token
    Stripe also returns is deprecated and is not kept. Nothing expires; the
    account can revoke access from its Dashboard, and then answers
    `account_invalid`;
  * one connection reaches exactly one account;
  * every report is a list endpoint, newest first, paged with `starting_after`.
    Time-series reports filter on `created` server-side;
  * money is in the currency's minor unit (cents; yen as they are; dinars in
    thousandths) and each object carries its own currency, so one report can
    span several. Amounts are returned in major units;
  * the API version is pinned (`Stripe-Version`) so response shapes do not
    follow the account's Dashboard default;
  * Stripe has no validate-only mode for writes, so a dry run checks the target
    and the values and stops there.
"""

from __future__ import annotations
import hashlib
import json
import logging
import re
from dataclasses import dataclass, field as dc_field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from terno_dbi.connectors.api.model.base import ApiConnector
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode, invalid_field
from terno_dbi.connectors.api.model.types import (
    Account, Action, ActionResult, Field, QueryResult, QuerySpec,
)

logger = logging.getLogger(__name__)

_BASE = "https://api.stripe.com/v1"
_API_VERSION = "2026-09-30.endive"
_PAGE = 100                 # the most one list page returns
_MAX_TOTAL = 100000         # safety cap on rows per query

# Currencies whose API amounts are not in hundredths. ISK and UGX are
# zero-decimal in life but two-decimal in the API, so they are not listed.
_ZERO_DECIMAL = frozenset({
    "bif", "clp", "djf", "gnf", "jpy", "kmf", "krw", "mga", "pyg", "rwf",
    "vnd", "vuv", "xaf", "xof", "xpf",
})
_THREE_DECIMAL = frozenset({"bhd", "jod", "kwd", "omr", "tnd"})

# Months in one billing interval unit, to put every price on a monthly footing.
_MONTHS_IN = {"day": 12 / 365, "week": 12 / 52, "month": 1, "year": 12}

_SUBSCRIPTION_STATUSES = (
    "all", "active", "past_due", "unpaid", "canceled", "incomplete",
    "incomplete_expired", "trialing", "paused", "ended",
)


# --- columns ---------------------------------------------------------------


@dataclass(frozen=True)
class _Col:
    """One output column: its field, how to read it, and what it needs.

    `money` columns read a minor-unit amount and are converted with the
    currency `currency` reads; `time` columns read a Unix timestamp. `expand`
    is the `expand[]` path the column needs on the list request.
    """

    field: Field
    get: Callable[[Dict[str, Any]], Any]
    kind: str = "plain"          # plain | money | time
    currency: Callable[[Dict[str, Any]], Any] = lambda o: o.get("currency")
    expand: Optional[str] = None


def _path(*keys: str) -> Callable[[Dict[str, Any]], Any]:
    def get(obj):
        for key in keys:
            if not isinstance(obj, dict):
                return None
            obj = obj.get(key)
        return obj
    return get


def _id_of(value) -> Optional[str]:
    """The id of a reference, whether Stripe sent it as an id or expanded it."""
    if isinstance(value, dict):
        return value.get("id")
    return value


def _ref(*keys: str) -> Callable[[Dict[str, Any]], Any]:
    get = _path(*keys)
    return lambda obj: _id_of(get(obj))


def _upper(*keys: str) -> Callable[[Dict[str, Any]], Any]:
    get = _path(*keys)
    return lambda obj: get(obj).upper() if isinstance(get(obj), str) else None


def _dim(fid, name, description="", get=None, data_type="string",
         expand=None) -> _Col:
    return _Col(Field(fid, name, "dimension", description, data_type=data_type),
                get or _path(fid), expand=expand)


def _flag(fid, name, description="", get=None) -> _Col:
    return _dim(fid, name, description, get, data_type="boolean")


def _time(fid, name, description="", get=None) -> _Col:
    return _Col(Field(fid, name, "dimension", description, data_type="datetime"),
                get or _path(fid), kind="time")


def _money(fid, name, description="", get=None, currency=None,
           non_aggregatable=False, expand=None) -> _Col:
    return _Col(
        Field(fid, name, "metric", description, data_type="number",
              is_monetary=True, is_non_aggregatable=non_aggregatable),
        get or _path(fid), kind="money",
        currency=currency or _path("currency"), expand=expand)


def _count(fid, name, description="", get=None) -> _Col:
    return _Col(Field(fid, name, "metric", description, data_type="integer"),
                get or _path(fid))


def _currency_col(description="Three-letter ISO currency code.") -> _Col:
    return _dim("currency", "Currency", description, _upper("currency"))


# --- reports ---------------------------------------------------------------


@dataclass(frozen=True)
class _Report:
    """A report type: the list endpoint behind it and its columns.

    `dated` reports filter on `created`; the others are a snapshot and ignore
    the date range. `statuses` are listed one after another (a Stripe list takes
    one status); `status_setting` lets the caller choose it instead.
    """

    path: str
    cols: List[_Col]
    default: Tuple[str, ...]
    dated: bool = True
    statuses: Tuple[str, ...] = ()
    status_setting: bool = False
    catalogue: Dict[str, _Col] = dc_field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "catalogue", {c.field.id: c for c in self.cols})


_BT = "data.balance_transaction"

_CHARGES = _Report(
    path="/charges",
    cols=[
        _dim("id", "Charge ID", "The charge's id (ch_… or py_…); refund it with "
             "create_refund."),
        _time("created", "Created", "When the charge was made."),
        _dim("status", "Status", "succeeded, pending or failed."),
        _flag("paid", "Paid", "Whether the charge succeeded."),
        _flag("captured", "Captured"),
        _flag("refunded", "Fully refunded"),
        _flag("disputed", "Disputed"),
        _money("amount", "Amount", "Amount charged, in the charge currency."),
        _money("amount_captured", "Amount captured"),
        _money("amount_refunded", "Amount refunded",
               "Amount refunded so far, in the charge currency."),
        _money("fee", "Stripe fee",
               "Stripe's fees for the charge, in the settlement currency.",
               get=_path("balance_transaction", "fee"),
               currency=_path("balance_transaction", "currency"), expand=_BT),
        _money("net", "Net",
               "Amount after Stripe's fees, in the settlement currency (before "
               "any refunds).",
               get=_path("balance_transaction", "net"),
               currency=_path("balance_transaction", "currency"), expand=_BT),
        _dim("settlement_currency", "Settlement currency",
             "The currency fee and net are in — the account's balance currency, "
             "which can differ from the charge currency.",
             get=_upper("balance_transaction", "currency"), expand=_BT),
        _currency_col("The charge (presentment) currency."),
        _dim("customer_id", "Customer ID", get=_ref("customer")),
        _dim("customer_email", "Customer email",
             "The email on the customer record.",
             get=_path("customer", "email"), expand="data.customer"),
        _dim("billing_email", "Billing email",
             "The email given with the payment method.",
             get=_path("billing_details", "email")),
        _dim("billing_country", "Billing country",
             "Two-letter country of the billing address.",
             get=_path("billing_details", "address", "country")),
        _dim("description", "Description"),
        _dim("payment_method_type", "Payment method",
             "card, us_bank_account, sepa_debit, …",
             get=_path("payment_method_details", "type")),
        _dim("card_brand", "Card brand",
             get=_path("payment_method_details", "card", "brand")),
        _dim("card_country", "Card country",
             "Two-letter country where the card was issued.",
             get=_path("payment_method_details", "card", "country")),
        _dim("failure_code", "Failure code"),
        _dim("failure_message", "Failure message"),
        _dim("risk_level", "Risk level", "normal, elevated or highest.",
             get=_path("outcome", "risk_level")),
        _dim("payment_intent_id", "Payment intent ID",
             get=_ref("payment_intent")),
    ],
    default=("id", "created", "status", "amount", "amount_refunded", "currency",
             "customer_id", "description"),
)

_BALANCE_TRANSACTIONS = _Report(
    path="/balance_transactions",
    cols=[
        _dim("id", "Balance transaction ID"),
        _time("created", "Created"),
        _time("available_on", "Available on",
              "When the funds become available in the balance."),
        _dim("type", "Type",
             "charge, payment, refund, payment_refund, adjustment, payout, "
             "stripe_fee, application_fee, …"),
        _dim("reporting_category", "Reporting category",
             "Stripe's grouping for reconciliation: charge, refund, fee, "
             "dispute, payout, …"),
        _dim("status", "Status", "available or pending."),
        _dim("description", "Description"),
        _dim("source_id", "Source ID",
             "The object that caused the transaction (a charge, refund, "
             "payout, …).", get=_ref("source")),
        _money("amount", "Gross amount",
               "Negative for money leaving the balance."),
        _money("fee", "Fee", "Stripe's fees on the transaction."),
        _money("net", "Net", "Gross amount minus fees."),
        _currency_col("The balance currency."),
    ],
    default=("id", "created", "type", "amount", "fee", "net", "currency"),
)

_REFUNDS = _Report(
    path="/refunds",
    cols=[
        _dim("id", "Refund ID"),
        _time("created", "Created"),
        _money("amount", "Amount"),
        _currency_col(),
        _dim("status", "Status",
             "pending, requires_action, succeeded, failed or canceled."),
        _dim("reason", "Reason",
             "duplicate, fraudulent, requested_by_customer or "
             "expired_uncaptured_charge."),
        _dim("charge_id", "Charge ID", get=_ref("charge")),
        _dim("payment_intent_id", "Payment intent ID",
             get=_ref("payment_intent")),
    ],
    default=("id", "created", "amount", "currency", "status", "reason",
             "charge_id"),
)

_DISPUTES = _Report(
    path="/disputes",
    cols=[
        _dim("id", "Dispute ID"),
        _time("created", "Created"),
        _money("amount", "Amount", "Amount disputed."),
        _currency_col(),
        _dim("status", "Status",
             "warning_needs_response, needs_response, under_review, won, lost, "
             "…"),
        _dim("reason", "Reason",
             "fraudulent, product_not_received, duplicate, …"),
        _dim("charge_id", "Charge ID", get=_ref("charge")),
        _dim("payment_intent_id", "Payment intent ID",
             get=_ref("payment_intent")),
        _time("evidence_due_by", "Evidence due by",
              get=_path("evidence_details", "due_by")),
        _flag("is_charge_refundable", "Charge refundable"),
    ],
    default=("id", "created", "amount", "currency", "status", "reason",
             "charge_id"),
)

_PAYOUTS = _Report(
    path="/payouts",
    cols=[
        _dim("id", "Payout ID"),
        _time("created", "Created"),
        _time("arrival_date", "Arrival date",
              "When the payout is expected to reach the bank."),
        _money("amount", "Amount"),
        _currency_col(),
        _dim("status", "Status", "paid, pending, in_transit, canceled or failed."),
        _dim("type", "Type", "bank_account or card."),
        _dim("method", "Method", "standard or instant."),
        _flag("automatic", "Automatic",
              "True for payouts on the account's schedule."),
        _dim("description", "Description"),
        _dim("failure_code", "Failure code"),
    ],
    default=("id", "created", "arrival_date", "amount", "currency", "status"),
)

_CUSTOMERS = _Report(
    path="/customers",
    cols=[
        _dim("id", "Customer ID", "The customer's id (cus_…); change it with "
             "update_customer."),
        _time("created", "Created"),
        _dim("email", "Email"),
        _dim("name", "Name"),
        _dim("description", "Description"),
        _dim("phone", "Phone"),
        _dim("country", "Country", "Two-letter country of the address.",
             get=_path("address", "country")),
        _currency_col("The currency the customer is billed in, once set."),
        _money("balance", "Balance",
               "Credit (negative) or amount owed (positive) carried to the "
               "customer's next invoice."),
        _flag("delinquent", "Delinquent",
              "True when the latest invoice or charge failed."),
    ],
    default=("id", "created", "email", "name", "country"),
)


def _sum_amounts(key: str) -> Callable[[Dict[str, Any]], Any]:
    """The total of `amount` over a list of amounts (an invoice's taxes)."""
    def get(obj):
        items = obj.get(key)
        if not isinstance(items, list):
            return None
        return sum(i.get("amount") or 0 for i in items if isinstance(i, dict))
    return get


_INVOICES = _Report(
    path="/invoices",
    cols=[
        _dim("id", "Invoice ID"),
        _dim("number", "Number", "The invoice number customers see."),
        _time("created", "Created"),
        _dim("status", "Status", "draft, open, paid, uncollectible or void."),
        _dim("billing_reason", "Billing reason",
             "subscription_create, subscription_cycle, manual, …"),
        _dim("collection_method", "Collection method",
             "charge_automatically or send_invoice."),
        _dim("customer_id", "Customer ID", get=_ref("customer")),
        _dim("customer_email", "Customer email"),
        _dim("customer_name", "Customer name"),
        _dim("subscription_id", "Subscription ID",
             "Set when a subscription generated the invoice.",
             get=_ref("parent", "subscription_details", "subscription")),
        _currency_col(),
        _time("due_date", "Due date"),
        _time("period_start", "Period start"),
        _time("period_end", "Period end"),
        _time("paid_at", "Paid at", get=_path("status_transitions", "paid_at")),
        _money("subtotal", "Subtotal", "Before discounts and tax."),
        _money("discount", "Discounts", get=_sum_amounts("total_discount_amounts")),
        _money("tax", "Tax", get=_sum_amounts("total_taxes")),
        _money("total", "Total"),
        _money("amount_due", "Amount due"),
        _money("amount_paid", "Amount paid"),
        _money("amount_remaining", "Amount remaining"),
        _count("attempt_count", "Payment attempts"),
        _dim("hosted_invoice_url", "Invoice URL"),
    ],
    default=("id", "number", "created", "status", "customer_id", "total",
             "amount_paid", "currency"),
)


def _items(sub: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [i for i in ((sub.get("items") or {}).get("data") or [])
            if isinstance(i, dict)]


def _first_item(key: str) -> Callable[[Dict[str, Any]], Any]:
    """A billing-period field, which lives on the items since 2025-03-31."""
    def get(sub):
        items = _items(sub)
        return items[0].get(key) if items else None
    return get


def _price_of(item: Dict[str, Any]) -> Dict[str, Any]:
    price = item.get("price")
    return price if isinstance(price, dict) else {}


def _joined(get_one: Callable[[Dict[str, Any]], Any]):
    def get(sub):
        values = [get_one(i) for i in _items(sub)]
        return ",".join(str(v) for v in values if v) or None
    return get


def _mrr(sub: Dict[str, Any]) -> Optional[float]:
    """Monthly recurring revenue at list price, in minor units.

    Each item's unit price times its quantity, put on a monthly footing. None
    when an item has no flat unit price (metered or tiered), rather than a total
    that silently leaves it out.
    """
    items = _items(sub)
    if not items:
        return None
    total = 0.0
    for item in items:
        price = _price_of(item)
        recurring = price.get("recurring") or {}
        unit = price.get("unit_amount")
        months = _MONTHS_IN.get(recurring.get("interval"))
        if (unit is None or months is None
                or recurring.get("usage_type") == "metered"):
            return None
        quantity = item.get("quantity")
        total += unit * (1 if quantity is None else quantity) / (
            months * (recurring.get("interval_count") or 1))
    return total


def _subscription_cols() -> List[_Col]:
    return [
        _dim("id", "Subscription ID", "The subscription's id (sub_…); cancel "
             "it with cancel_subscription."),
        _time("created", "Created"),
        _time("start_date", "Start date"),
        _dim("status", "Status",
             "active, past_due, unpaid, canceled, incomplete, "
             "incomplete_expired, trialing or paused."),
        _dim("customer_id", "Customer ID", get=_ref("customer")),
        _dim("customer_email", "Customer email",
             get=_path("customer", "email"), expand="data.customer"),
        _flag("cancel_at_period_end", "Cancels at period end"),
        _time("cancel_at", "Cancels at"),
        _time("canceled_at", "Canceled at"),
        _time("ended_at", "Ended at"),
        _time("trial_end", "Trial ends"),
        _dim("cancellation_reason", "Cancellation reason",
             "cancellation_requested, payment_failed, payment_disputed, …",
             get=_path("cancellation_details", "reason")),
        _dim("collection_method", "Collection method"),
        _currency_col(),
        _time("current_period_start", "Current period start",
              get=_first_item("current_period_start")),
        _time("current_period_end", "Current period end",
              get=_first_item("current_period_end")),
        _dim("price_ids", "Price IDs", "Comma-separated.",
             get=_joined(lambda i: _price_of(i).get("id"))),
        _dim("product_ids", "Product IDs", "Comma-separated.",
             get=_joined(lambda i: _id_of(_price_of(i).get("product")))),
        _dim("interval", "Billing interval", "day, week, month or year.",
             get=lambda s: next((_path("recurring", "interval")(_price_of(i))
                                 for i in _items(s)), None)),
        _dim("interval_count", "Interval count",
             "Intervals between bills: 3 with month bills quarterly.",
             data_type="integer",
             get=lambda s: next((_path("recurring", "interval_count")(_price_of(i))
                                 for i in _items(s)), None)),
        _count("quantity", "Quantity", "Total quantity across items.",
               get=lambda s: sum(i.get("quantity") or 0 for i in _items(s))),
        _money("mrr", "MRR",
               "Monthly recurring revenue at list price: each item's unit "
               "price × quantity, normalised to a month. Before discounts and "
               "tax; empty for metered or tiered prices. Computed for every "
               "status — filter on status before totalling.",
               get=_mrr),
    ]


_SUBSCRIPTIONS = _Report(
    path="/subscriptions",
    cols=_subscription_cols(),
    default=("id", "created", "status", "customer_id", "mrr", "currency"),
    status_setting=True,
)

# Stripe counts active and past-due subscriptions toward MRR, not trials.
_ACTIVE_SUBSCRIPTIONS = _Report(
    path="/subscriptions",
    cols=_subscription_cols(),
    default=("id", "status", "customer_id", "mrr", "currency",
             "current_period_end"),
    dated=False,
    statuses=("active", "past_due"),
)

_PRODUCTS = _Report(
    path="/products",
    cols=[
        _dim("id", "Product ID"),
        _dim("name", "Name"),
        _flag("active", "Active"),
        _dim("description", "Description"),
        _time("created", "Created"),
        _time("updated", "Updated"),
        _dim("default_price_id", "Default price ID", get=_ref("default_price")),
    ],
    default=("id", "name", "active", "default_price_id"),
    dated=False,
)

_PRICES = _Report(
    path="/prices",
    cols=[
        _dim("id", "Price ID"),
        _dim("product_id", "Product ID", get=_ref("product")),
        _dim("nickname", "Nickname"),
        _flag("active", "Active"),
        _currency_col(),
        _money("unit_amount", "Unit amount",
               "Empty for tiered or custom-amount prices.",
               non_aggregatable=True),
        _dim("type", "Type", "one_time or recurring."),
        _dim("interval", "Billing interval", get=_path("recurring", "interval")),
        _dim("interval_count", "Interval count", data_type="integer",
             get=_path("recurring", "interval_count")),
        _dim("usage_type", "Usage type", "licensed or metered.",
             get=_path("recurring", "usage_type")),
        _dim("billing_scheme", "Billing scheme", "per_unit or tiered."),
        _dim("lookup_key", "Lookup key"),
        _time("created", "Created"),
    ],
    default=("id", "product_id", "active", "unit_amount", "currency", "type",
             "interval"),
    dated=False,
)

_REPORTS: Dict[str, _Report] = {
    "Charges": _CHARGES,
    "BalanceTransactions": _BALANCE_TRANSACTIONS,
    "Refunds": _REFUNDS,
    "Disputes": _DISPUTES,
    "Payouts": _PAYOUTS,
    "Customers": _CUSTOMERS,
    "Invoices": _INVOICES,
    "Subscriptions": _SUBSCRIPTIONS,
    "ActiveSubscriptions": _ACTIVE_SUBSCRIPTIONS,
    "Products": _PRODUCTS,
    "Prices": _PRICES,
}
_DEFAULT_REPORT = "Charges"


# --- write actions ---------------------------------------------------------


def _id_prop(noun: str, prefix: str, column: str) -> Dict[str, Any]:
    return {"type": "string", "pattern": f"^{prefix}_[A-Za-z0-9]+$",
            "description": f"The {noun}'s id ({prefix}_…), from the {column} "
                           f"column of a report."}


_CUSTOMER_PROPS: Dict[str, Any] = {
    "email": {"type": ["string", "null"], "description": "Email address."},
    "name": {"type": ["string", "null"], "description": "Full name or business name."},
    "description": {"type": ["string", "null"],
                    "description": "An internal note shown in the Dashboard."},
    "phone": {"type": ["string", "null"], "description": "Phone number."},
    "metadata": {"type": "object",
                 "additionalProperties": {"type": ["string", "null"]},
                 "description": "Key-value pairs to store on the customer. On "
                                "an update, a null value removes that key and "
                                "keys not named are left as they are."},
}
_CUSTOMER_FIELDS = ("email", "name", "description", "phone")

_ACTIONS: List[Action] = [
    Action(
        "create_customer", "Create customer",
        "Create a customer with an email, a name, or both. Repeating the exact "
        "same request within 24 hours returns the customer it created instead "
        "of a duplicate.",
        schema={"type": "object", "properties": _CUSTOMER_PROPS,
                "anyOf": [{"required": ["email"]}, {"required": ["name"]}],
                "additionalProperties": False},
        destructive=False,
    ),
    Action(
        "update_customer", "Update customer",
        "Change a customer's email, name, description, phone or metadata. Only "
        "the fields given change; null clears one. Customers are never deleted.",
        schema={"type": "object",
                "properties": {"customer_id": _id_prop("customer", "cus", "id"),
                               **_CUSTOMER_PROPS},
                "required": ["customer_id"],
                "minProperties": 2,
                "additionalProperties": False},
    ),
    Action(
        "create_refund", "Refund a payment",
        "Refund all or part of a successful charge, back to the customer's "
        "payment method. Money leaves the account and it cannot be undone — "
        "confirm the amount first. Name the charge (charge_id) or its payment "
        "intent (payment_intent_id). Repeating the exact same request within "
        "24 hours returns the first refund instead of refunding twice.",
        schema={"type": "object",
                "properties": {
                    "charge_id": {"type": "string",
                                  "pattern": "^(ch|py)_[A-Za-z0-9]+$",
                                  "description": "The charge's id (ch_… or "
                                                 "py_…), from the Charges "
                                                 "report."},
                    "payment_intent_id": _id_prop(
                        "payment intent", "pi", "payment_intent_id"),
                    "amount": {"type": "number", "exclusiveMinimum": 0,
                               "description": "How much to refund, in the "
                                              "charge currency's major unit "
                                              "(10.5 is 10.50). Leave out to "
                                              "refund everything not yet "
                                              "refunded."},
                    "reason": {"type": "string",
                               "enum": ["duplicate", "fraudulent",
                                        "requested_by_customer"]},
                },
                "oneOf": [{"required": ["charge_id"]},
                          {"required": ["payment_intent_id"]}],
                "additionalProperties": False},
    ),
    Action(
        "cancel_subscription", "Cancel subscription",
        "Cancel a subscription — by default at the end of the period already "
        "paid for, which reactivate_subscription can undo until then. With "
        "at_period_end false it ends now, with no refund or proration credit, "
        "and cannot be undone.",
        schema={"type": "object",
                "properties": {
                    "subscription_id": _id_prop("subscription", "sub", "id"),
                    "at_period_end": {"type": "boolean", "default": True,
                                      "description": "False cancels "
                                                     "immediately."},
                },
                "required": ["subscription_id"],
                "additionalProperties": False},
    ),
    Action(
        "reactivate_subscription", "Reactivate subscription",
        "Undo a scheduled cancellation, so the subscription keeps renewing and "
        "billing. A subscription that has already ended cannot be reactivated.",
        schema={"type": "object",
                "properties": {
                    "subscription_id": _id_prop("subscription", "sub", "id"),
                },
                "required": ["subscription_id"],
                "additionalProperties": False},
    ),
]
_ACTIONS_BY_ID: Dict[str, Action] = {a.id: a for a in _ACTIONS}

_ENDED = ("canceled", "incomplete_expired")


# --- money and time ----------------------------------------------------------


def _minor_per_major(currency) -> int:
    code = str(currency or "").lower()
    if code in _ZERO_DECIMAL:
        return 1
    if code in _THREE_DECIMAL:
        return 1000
    return 100


def _major(amount, currency):
    """A minor-unit amount in major units: 1099 USD is 10.99, 500 JPY is 500."""
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        return None
    factor = _minor_per_major(currency)
    if factor == 1:
        return amount if isinstance(amount, int) else round(amount, 2)
    return round(amount / factor, 3 if factor == 1000 else 2)


def _to_minor(amount: float, currency) -> int:
    """A major-unit amount in minor units, refusing fractions the currency lacks."""
    factor = _minor_per_major(currency)
    minor = round(amount * factor)
    if abs(amount * factor - minor) > 1e-6:
        raise ApiError(
            ErrorCode.INVALID_ACTION_PARAMS,
            f"'amount' {amount} has more decimal places than "
            f"{str(currency).upper()} allows.",
            retriable=False, details={"amount": amount})
    return minor


def _zone(name: Optional[str]):
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        return ZoneInfo(name) if name else timezone.utc
    except (ZoneInfoNotFoundError, ValueError):
        return timezone.utc


def _bounds(start: str, end: str, tz) -> Tuple[int, int]:
    """The range as `[start, end)` Unix times, midnight to midnight in `tz`."""
    try:
        first, last = date.fromisoformat(start), date.fromisoformat(end)
    except (TypeError, ValueError):
        raise ApiError(
            ErrorCode.INVALID_FILTER,
            f"date_range must be absolute 'YYYY-MM-DD' dates, got {start!r} to "
            f"{end!r}. Resolve relative ranges (e.g. 'last 30 days') with "
            f"get_today first.",
            retriable=False,
        )
    if first > last:
        raise ApiError(
            ErrorCode.INVALID_FILTER,
            f"date_range.start ({start}) is after date_range.end ({end}).",
            retriable=False,
        )
    lo = datetime.combine(first, time(), tzinfo=tz)
    hi = datetime.combine(last + timedelta(days=1), time(), tzinfo=tz)
    return int(lo.timestamp()), int(hi.timestamp())


def _iso(ts, tz) -> Optional[str]:
    if isinstance(ts, bool) or not isinstance(ts, (int, float)):
        return None
    return datetime.fromtimestamp(ts, tz).isoformat()


def _cell(col: _Col, obj: Dict[str, Any], tz):
    raw = col.get(obj)
    if col.kind == "money":
        return _major(raw, col.currency(obj))
    if col.kind == "time":
        return _iso(raw, tz)
    return raw


# --- transport -------------------------------------------------------------


def _scalar(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _form(params: Dict[str, Any], prefix: str = "") -> List[Tuple[str, str]]:
    """Stripe's bracketed form encoding: `metadata[plan]`, `expand[]`, …"""
    out: List[Tuple[str, str]] = []
    for key, value in params.items():
        name = f"{prefix}[{key}]" if prefix else str(key)
        if value is None:
            continue
        if isinstance(value, dict):
            out.extend(_form(value, name))
        elif isinstance(value, (list, tuple)):
            out.extend((f"{name}[]", _scalar(v)) for v in value)
        else:
            out.append((name, _scalar(value)))
    return out


def _default_http(method: str, url: str, params: List[Tuple[str, str]],
                  headers: Dict[str, str]) -> Dict[str, Any]:
    import requests
    body = {"data": params} if method == "POST" else {"params": params}
    resp = requests.request(method, url, headers=headers, timeout=60, **body)
    if resp.status_code >= 400:
        raise _stripe_error(resp, method)
    return resp.json()


def _stripe_error(resp, method: str = "GET") -> ApiError:
    """A Stripe refusal, by its error code first and its HTTP status second."""
    status = resp.status_code
    error: Dict[str, Any] = {}
    try:
        body = resp.json()
        if isinstance(body, dict) and isinstance(body.get("error"), dict):
            error = body["error"]
    except ValueError:
        pass
    code = str(error.get("code") or "")
    message = str(error.get("message") or "").strip()[:300]
    details = {"status": status, "code": code}
    if error.get("param"):
        details["param"] = error["param"]

    if code == "account_invalid":
        return ApiError(
            ErrorCode.AUTH_EXPIRED,
            "Stripe no longer gives this deployment access to the connected "
            "account — access was revoked from the Stripe Dashboard, or the "
            "account was closed. Reconnect the source.",
            retriable=False, details=details)
    if status == 401:
        return ApiError(
            ErrorCode.UPSTREAM_ERROR,
            "Stripe rejected this deployment's platform key. "
            "TERNO_STRIPE_SECRET_KEY must be the secret key of the Connect "
            "platform the account was connected to, in the same mode (live or "
            "test).",
            retriable=False, details=details)
    if status == 429:
        return ApiError(
            ErrorCode.RATE_LIMITED,
            f"Stripe's rate limit was reached{': ' + message if message else ''}.",
            retry_after_seconds=_retry_after(resp), details=details)
    if method != "GET" and status in (400, 402, 404, 409):
        return ApiError(
            ErrorCode.INVALID_ACTION_PARAMS,
            f"Stripe refused the change: {message or code or 'invalid request'}",
            retriable=False, details=details)
    if status == 403:
        return ApiError(
            ErrorCode.ACCOUNT_FORBIDDEN,
            f"Stripe refused access{': ' + message if message else ''}.",
            retriable=False, details=details)
    if status >= 500:
        return ApiError(
            ErrorCode.UPSTREAM_ERROR,
            f"Stripe is temporarily unavailable (HTTP {status}).",
            retry_after_seconds=_retry_after(resp), details=details)
    return ApiError(
        ErrorCode.UPSTREAM_ERROR,
        f"Stripe API error ({status}): {message or code or 'unknown error'}",
        retriable=False, details=details)


def _retry_after(resp) -> Optional[int]:
    headers = getattr(resp, "headers", None) or {}
    try:
        return int(headers.get("Retry-After"))
    except (TypeError, ValueError):
        return None


_ACCOUNT_RE = re.compile(r"^acct_[A-Za-z0-9]+$")


class StripeConnector(ApiConnector):
    def __init__(self, datasource, http: Optional[Callable] = None,
                 token_refresher: Optional[Callable] = None):
        super().__init__(datasource, token_refresher=token_refresher)
        self._http = http or _default_http

    # -- transport ----------------------------------------------------------

    def _platform_key(self) -> str:
        from terno_dbi.connectors.api.auth.providers import get_provider
        provider = get_provider("stripe")
        key = provider.client_secret() if provider else ""
        if not key:
            raise ApiError(
                ErrorCode.UPSTREAM_ERROR,
                "This deployment has no Stripe platform key. Set "
                "TERNO_STRIPE_SECRET_KEY.",
                retriable=False,
            )
        return key

    def _account_id(self) -> str:
        """The connected account. It is sent as a header, so its shape is checked."""
        account = str(self._tokens().get("STRIPE_USER_ID") or "")
        if not _ACCOUNT_RE.match(account):
            raise ApiError(
                ErrorCode.AUTH_EXPIRED,
                f"{self.key} does not name a connected Stripe account; "
                f"reconnect the source.",
            )
        return account

    def _call(self, method: str, path: str, params: Optional[Dict] = None,
              idempotency_key: str = "") -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self._platform_key()}",
            "Stripe-Account": self._account_id(),
            "Stripe-Version": _API_VERSION,
        }
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        try:
            return self._http(method, f"{_BASE}{path}", _form(params or {}),
                              headers)
        except ApiError:
            raise
        except Exception as exc:   # noqa: BLE001
            logger.warning("Stripe request failed: %s", exc)
            raise ApiError(ErrorCode.UPSTREAM_ERROR,
                           "Stripe could not be reached. Try again.")

    def _list(self, path: str, params: Dict[str, Any],
              limit: int) -> Iterator[Dict[str, Any]]:
        """Every object a list endpoint returns, newest first, page by page."""
        query = {**params, "limit": min(_PAGE, max(1, limit))}
        while True:
            page = self._call("GET", path, query)
            data = page.get("data") or []
            yield from data
            if not page.get("has_more") or not data:
                return
            query["starting_after"] = data[-1].get("id")

    # -- discovery ----------------------------------------------------------

    def list_accounts(self) -> List[Account]:
        account_id = self._account_id()
        livemode = not self._platform_key().startswith(("sk_test_", "rk_test_"))
        try:
            account = self._call("GET", "/account")
        except ApiError as exc:
            if exc.code == ErrorCode.AUTH_EXPIRED:
                raise
            logger.info("Stripe account lookup failed; using the account id "
                        "for %s", account_id)
            return [Account(id=account_id, name=account_id,
                            extra={"livemode": livemode})]
        dashboard = (account.get("settings") or {}).get("dashboard") or {}
        name = (dashboard.get("display_name")
                or (account.get("business_profile") or {}).get("name")
                or account.get("email") or account_id)
        currency = account.get("default_currency")
        return [Account(
            id=account.get("id") or account_id,
            name=name,
            currency=currency.upper() if isinstance(currency, str) else None,
            timezone=dashboard.get("timezone"),
            extra={"country": account.get("country"), "livemode": livemode},
        )]

    def list_fields(self, report_type: Optional[str] = None) -> List[Field]:
        report = _REPORTS.get(report_type or _DEFAULT_REPORT, _CHARGES)
        return [c.field for c in report.cols]

    def _check_account(self, account: Optional[str]) -> None:
        connected = self._account_id()
        if account and str(account) != connected:
            raise ApiError(
                ErrorCode.ACCOUNT_FORBIDDEN,
                f"This connection reaches Stripe account {connected}, not "
                f"{account}.",
                retriable=False, details={"account_id": str(account)},
            )

    # -- query --------------------------------------------------------------

    def _run(self, spec: QuerySpec) -> QueryResult:
        report_type = (spec.report_type if spec.report_type in _REPORTS
                       else _DEFAULT_REPORT)
        report = _REPORTS[report_type]
        for account in spec.accounts:
            self._check_account(account)

        unknown = [f for f in spec.fields if f not in report.catalogue]
        if unknown:
            raise invalid_field(unknown[0], list(report.catalogue))
        requested = list(dict.fromkeys(spec.fields)) or list(report.default)
        cols = [report.catalogue[f] for f in requested]
        tz = _zone(spec.timezone)

        params: Dict[str, Any] = {}
        expand = sorted({c.expand for c in cols if c.expand})
        if expand:
            params["expand"] = expand
        if report.dated:
            gte, lt = _bounds(spec.date_range.start, spec.date_range.end, tz)
            params["created"] = {"gte": gte, "lt": lt}
            notes = [f"Records are filtered on when they were created, within "
                     f"the selected date range in {getattr(tz, 'key', 'UTC')}."]
        else:
            notes = ["This report is the account's current state; the date "
                     "range is ignored."]
        statuses = report.statuses
        if report.status_setting:
            statuses = (_status_setting(spec.settings),)

        max_total = min(spec.max_rows, _MAX_TOTAL)
        objects: List[Dict[str, Any]] = []
        for status in statuses or (None,):
            query = {**params, "status": status} if status else params
            for obj in self._list(report.path, query, max_total - len(objects)):
                objects.append(obj)
                if len(objects) >= max_total:
                    break
            if len(objects) >= max_total:
                break

        rows = [{c.field.id: _cell(c, obj, tz) for c in cols} for obj in objects]

        warnings: List[str] = []
        if len(objects) >= max_total:
            warnings.append(
                f"Stopped at max_rows ({max_total}); more records may match. "
                f"Narrow the date range or raise max_rows before counting or "
                f"totalling.")
        money = [c for c in cols if c.kind == "money"]
        if money:
            notes.append("Amounts are in major currency units (10.99, not "
                         "1099 cents).")
            currencies = sorted({str(c.currency(obj)).upper() for obj in objects
                                 for c in money if c.currency(obj)})
            if len(currencies) > 1:
                warnings.append(
                    f"These rows are in {len(currencies)} currencies "
                    f"({', '.join(currencies)}); do not total monetary fields "
                    f"across them — group by currency.")
        if "mrr" in requested:
            warnings.extend(_mrr_warnings(objects))
        return QueryResult(requested_field_ids=requested, rows=rows,
                           row_count=len(rows), notes=notes, warnings=warnings)

    # -- write actions ------------------------------------------------------

    def list_actions(self) -> List[Action]:
        return list(_ACTIONS)

    def execute_action(
        self, action_id: str, account: str,
        params: Optional[Dict[str, Any]] = None, dry_run: bool = False,
    ) -> ActionResult:
        """Perform one write action. Account authorisation happens upstream.

        Every parameter is checked before anything is sent. Updates, refunds
        and cancellations read their target first, which confirms it exists in
        this account and records its state as `before`; a dry run stops there.
        """
        action = _ACTIONS_BY_ID.get(action_id)
        if action is None:
            raise ApiError(
                ErrorCode.UNKNOWN_ACTION,
                f"Unknown action {action_id!r}. Available: "
                f"{', '.join(_ACTIONS_BY_ID)}.",
                retriable=False,
                details={"action": action_id, "available": sorted(_ACTIONS_BY_ID)},
            )
        params = params or {}
        extra = sorted(set(params) - set(action.schema["properties"]))
        if extra:
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"{action_id} does not accept: {', '.join(extra)}.",
                retriable=False, details={"unexpected": extra},
            )
        self._check_account(account)
        handler = getattr(self, f"_{action_id}")
        return handler(account, params, bool(dry_run))

    def _idempotency_key(self, action_id: str, account: str,
                         body: Dict[str, Any]) -> str:
        """The same create, repeated, is one create — even across a retry.

        Stripe replays the original response for a key it has seen in the last
        24 hours, so an agent retrying after a timeout cannot refund twice.
        """
        source = json.dumps([getattr(self.datasource, "id", None), account,
                             action_id, body], sort_keys=True, default=str)
        return "terno-" + hashlib.sha256(source.encode()).hexdigest()[:48]

    def _fetch(self, noun: str, path: str,
               params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """The object an action targets, or a clear error if it is not here."""
        try:
            obj = self._call("GET", path, params)
        except ApiError as exc:
            if exc.details.get("status") != 404:
                raise
            obj = {}
        if not obj.get("id") or obj.get("deleted"):
            object_id = path.rsplit("/", 1)[-1]
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"No {noun} {object_id} was found in this Stripe account.",
                retriable=False, details={"id": object_id},
            )
        return obj

    def _create_customer(self, account, params, dry_run) -> ActionResult:
        given, body = _customer_change(params, creating=True)
        names = ", ".join(given)
        if dry_run:
            return _would("create_customer", account,
                          f"Would create a customer with: {names}.",
                          after=given, checked="parameter names and formats")
        key = self._idempotency_key("create_customer", account, body)
        created = self._call("POST", "/customers", body, idempotency_key=key)
        customer_id = created.get("id")
        return ActionResult(
            action="create_customer", account=account,
            summary=f"Created customer {customer_id} with: {names}.",
            after={"id": customer_id, **given},
            details={"customer_id": customer_id, "idempotency_key": key})

    def _update_customer(self, account, params, dry_run) -> ActionResult:
        customer_id = _require_id(params, "customer_id", ("cus",))
        given, body = _customer_change(
            {k: v for k, v in params.items() if k != "customer_id"},
            creating=False)
        current = self._fetch("customer", f"/customers/{customer_id}")
        before: Dict[str, Any] = {"id": customer_id}
        for name in given:
            if name == "metadata":
                existing = current.get("metadata") or {}
                before["metadata"] = {k: existing.get(k) for k in given["metadata"]}
            else:
                before[name] = current.get(name)
        after = {"id": customer_id, **given}
        names = ", ".join(given)
        if dry_run:
            return _would("update_customer", account,
                          f"Would update customer {customer_id}: {names}.",
                          before=before, after=after,
                          checked="parameter formats and that the customer "
                                  "exists")
        self._call("POST", f"/customers/{customer_id}", body)
        return ActionResult(
            action="update_customer", account=account,
            summary=f"Updated customer {customer_id}: {names}.",
            before=before, after=after, details={"customer_id": customer_id})

    def _refund_target(self, params) -> Dict[str, Any]:
        """The charge to refund, named directly or through its payment intent."""
        if ("charge_id" in params) == ("payment_intent_id" in params):
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                "Give exactly one of 'charge_id' or 'payment_intent_id'.",
                retriable=False)
        if "charge_id" in params:
            charge_id = _require_id(params, "charge_id", ("ch", "py"))
            return self._fetch("charge", f"/charges/{charge_id}")
        intent_id = _require_id(params, "payment_intent_id", ("pi",))
        intent = self._fetch("payment intent", f"/payment_intents/{intent_id}",
                             {"expand": ["latest_charge"]})
        charge = intent.get("latest_charge")
        if not isinstance(charge, dict) or not charge.get("id"):
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"Payment intent {intent_id} has no charge to refund (its "
                f"status is {intent.get('status')}).",
                retriable=False, details={"payment_intent_id": intent_id})
        return charge

    def _create_refund(self, account, params, dry_run) -> ActionResult:
        reason = params.get("reason")
        if reason is not None and reason not in (
                "duplicate", "fraudulent", "requested_by_customer"):
            raise _bad_value("reason", "duplicate, fraudulent or "
                             "requested_by_customer", reason)
        amount = params.get("amount")
        if amount is not None and (isinstance(amount, bool)
                                   or not isinstance(amount, (int, float))
                                   or amount <= 0):
            raise _bad_value("amount", "a positive amount in the charge "
                             "currency, e.g. 10.5 for 10.50", amount)
        charge = self._refund_target(params)
        charge_id = charge["id"]
        currency = charge.get("currency")
        refundable = (charge.get("amount_captured") or 0) - (
            charge.get("amount_refunded") or 0)
        if charge.get("status") != "succeeded" or not charge.get("captured"):
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"Charge {charge_id} has not been paid (status "
                f"{charge.get('status')}), so there is nothing to refund.",
                retriable=False, details={"charge_id": charge_id})
        if refundable <= 0:
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"Charge {charge_id} has already been fully refunded.",
                retriable=False, details={"charge_id": charge_id})
        minor = refundable if amount is None else _to_minor(amount, currency)
        if minor > refundable:
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"Charge {charge_id} has {_money_text(refundable, currency)} "
                f"left to refund, less than the {_money_text(minor, currency)} "
                f"asked for.",
                retriable=False, details={"charge_id": charge_id})

        before = {"charge_id": charge_id,
                  "amount": _major(charge.get("amount"), currency),
                  "amount_refunded": _major(charge.get("amount_refunded"), currency),
                  "currency": str(currency or "").upper()}
        scope = ("in part" if minor < refundable
                 else "the rest" if charge.get("amount_refunded") else "in full")
        what = f"{_money_text(minor, currency)} of charge {charge_id} ({scope})"
        if dry_run:
            return _would("create_refund", account, f"Would refund {what}.",
                          before=before,
                          checked="that the charge was paid and has that much "
                                  "left to refund")
        body: Dict[str, Any] = {"charge": charge_id}
        if amount is not None:
            body["amount"] = minor
        if reason:
            body["reason"] = reason
        key = self._idempotency_key("create_refund", account, body)
        # Read first so a replayed key — the same refund asked for again within
        # 24 hours — is reported as the earlier refund, not as a new one.
        earlier = {r.get("id") for r in self._list(
            "/refunds", {"charge": charge_id}, _PAGE)}
        refund = self._call("POST", "/refunds", body, idempotency_key=key)
        if refund.get("id") in earlier:
            return ActionResult(
                action="create_refund", account=account,
                summary=f"This refund was already made within the last 24 "
                        f"hours (refund {refund.get('id')}, status "
                        f"{refund.get('status')}); nothing new was refunded.",
                before=before, after=before,
                details={"refund_id": refund.get("id"), "replayed": True,
                         "idempotency_key": key})
        refunded = (refund.get("amount") if isinstance(refund.get("amount"), int)
                    else minor)
        after = {**before,
                 "amount_refunded": _major((charge.get("amount_refunded") or 0)
                                           + refunded, currency),
                 "refund_id": refund.get("id"),
                 "refund_status": refund.get("status")}
        return ActionResult(
            action="create_refund", account=account,
            summary=f"Refunded {what}: refund {refund.get('id')}, status "
                    f"{refund.get('status')}.",
            before=before, after=after,
            details={"refund_id": refund.get("id"), "idempotency_key": key})

    def _subscription(self, params) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
        subscription_id = _require_id(params, "subscription_id", ("sub",))
        sub = self._fetch("subscription", f"/subscriptions/{subscription_id}")
        return subscription_id, sub, _subscription_state(sub)

    def _cancel_subscription(self, account, params, dry_run) -> ActionResult:
        at_period_end = params.get("at_period_end", True)
        if not isinstance(at_period_end, bool):
            raise _bad_value("at_period_end", "true or false", at_period_end)
        subscription_id, sub, before = self._subscription(params)
        if sub.get("status") in _ENDED:
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"Subscription {subscription_id} has already ended.",
                retriable=False, details={"subscription_id": subscription_id})
        period_end = before.get("current_period_end")
        if at_period_end and sub.get("cancel_at_period_end"):
            return ActionResult(
                action="cancel_subscription", account=account,
                summary=f"Subscription {subscription_id} was already set to "
                        f"cancel at the end of its period ({period_end}); "
                        f"nothing changed.",
                before=before, after=before, details={"changed": False})
        outcome = (f"cancel at the end of its current period ({period_end})"
                   if at_period_end else "cancel immediately")
        if dry_run:
            return _would("cancel_subscription", account,
                          f"Would set subscription {subscription_id} to "
                          f"{outcome}.",
                          before=before,
                          checked="that the subscription exists and has not "
                                  "ended")
        if at_period_end:
            updated = self._call("POST", f"/subscriptions/{subscription_id}",
                                 {"cancel_at_period_end": True})
            summary = (f"Subscription {subscription_id} will cancel at the end "
                       f"of its current period ({period_end}).")
        else:
            updated = self._call("DELETE", f"/subscriptions/{subscription_id}")
            summary = f"Subscription {subscription_id} canceled immediately."
        return ActionResult(
            action="cancel_subscription", account=account, summary=summary,
            before=before, after=_subscription_state(updated or sub),
            details={"subscription_id": subscription_id})

    def _reactivate_subscription(self, account, params, dry_run) -> ActionResult:
        subscription_id, sub, before = self._subscription(params)
        if sub.get("status") in _ENDED:
            raise ApiError(
                ErrorCode.INVALID_ACTION_PARAMS,
                f"Subscription {subscription_id} has already ended and cannot "
                f"be reactivated; start a new subscription instead.",
                retriable=False, details={"subscription_id": subscription_id})
        if not (sub.get("cancel_at_period_end") or sub.get("cancel_at")):
            return ActionResult(
                action="reactivate_subscription", account=account,
                summary=f"Subscription {subscription_id} is not set to "
                        f"cancel; nothing changed.",
                before=before, after=before, details={"changed": False})
        if dry_run:
            return _would("reactivate_subscription", account,
                          f"Would remove subscription {subscription_id}'s "
                          f"scheduled cancellation.",
                          before=before,
                          checked="that the subscription exists and is set to "
                                  "cancel")
        # A period-end cancellation is undone with its flag; a dated one by
        # clearing the date.
        body = ({"cancel_at_period_end": False} if sub.get("cancel_at_period_end")
                else {"cancel_at": ""})
        updated = self._call("POST", f"/subscriptions/{subscription_id}", body)
        return ActionResult(
            action="reactivate_subscription", account=account,
            summary=f"Subscription {subscription_id} will keep renewing; its "
                    f"scheduled cancellation was removed.",
            before=before, after=_subscription_state(updated or sub),
            details={"subscription_id": subscription_id})


def _status_setting(settings: Dict[str, Any]) -> str:
    status = settings.get("status") or "all"
    if status not in _SUBSCRIPTION_STATUSES:
        raise ApiError(
            ErrorCode.INVALID_SETTING,
            f"'status' must be one of: {', '.join(_SUBSCRIPTION_STATUSES)}; got "
            f"{status!r}.",
            retriable=False, details={"setting_id": "status"})
    return status


def _mrr_warnings(subs: List[Dict[str, Any]]) -> List[str]:
    warnings = []
    unpriced = sum(1 for s in subs if _items(s) and _mrr(s) is None)
    if unpriced:
        warnings.append(
            f"{unpriced} subscription(s) have a metered or tiered price, so "
            f"their mrr is empty rather than a list-price figure.")
    partial = sum(1 for s in subs if (s.get("items") or {}).get("has_more"))
    if partial:
        warnings.append(
            f"{partial} subscription(s) have more items than Stripe lists with "
            f"the subscription; their mrr and quantity count only the items "
            f"listed.")
    return warnings


def _subscription_state(sub: Dict[str, Any]) -> Dict[str, Any]:
    tz = timezone.utc
    return {
        "id": sub.get("id"),
        "status": sub.get("status"),
        "customer_id": _id_of(sub.get("customer")),
        "cancel_at_period_end": sub.get("cancel_at_period_end"),
        "cancel_at": _iso(sub.get("cancel_at"), tz),
        "current_period_end": _iso(_first_item("current_period_end")(sub), tz),
    }


def _customer_change(params: Dict[str, Any],
                     creating: bool) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """The fields as the caller gave them, and the body Stripe is sent."""
    given: Dict[str, Any] = {}
    body: Dict[str, Any] = {}
    for name in _CUSTOMER_FIELDS:
        if name not in params:
            continue
        value = params[name]
        if value is None and not creating:
            given[name], body[name] = None, ""      # an empty string clears it
            continue
        if not isinstance(value, str) or not value.strip():
            raise _bad_value(name, "a non-empty string"
                             + ("" if creating else ", or null to clear it"),
                             value)
        value = value.strip()
        if name == "email" and ("@" not in value or any(c.isspace() for c in value)):
            raise _bad_value(name, "an email address", value)
        given[name] = body[name] = value
    if "metadata" in params:
        given["metadata"], body["metadata"] = _metadata(params["metadata"], creating)
    if not given:
        raise ApiError(
            ErrorCode.INVALID_ACTION_PARAMS,
            "Give at least one of: email, name, description, phone, metadata.",
            retriable=False)
    if creating and not (given.get("email") or given.get("name")):
        raise ApiError(
            ErrorCode.INVALID_ACTION_PARAMS,
            "A new customer needs an email or a name.",
            retriable=False, details={"missing": ["email", "name"]})
    return given, body


def _metadata(value, creating: bool) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Validate metadata within Stripe's limits: 50 keys, 40-character keys,
    500-character values. On an update a null value removes the key."""
    if not isinstance(value, dict) or not value:
        raise _bad_value("metadata", "a non-empty object of keys to values", value)
    if len(value) > 50:
        raise _bad_value("metadata", "at most 50 keys", f"{len(value)} keys")
    given: Dict[str, Any] = {}
    body: Dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key or len(key) > 40 or "[" in key:
            raise _bad_value("metadata", "keys of 1-40 characters without "
                             "brackets", key)
        if item is None and not creating:
            given[key], body[key] = None, ""
            continue
        if isinstance(item, bool) or not isinstance(item, (str, int, float)):
            raise _bad_value(f"metadata.{key}", "a string", item)
        text = str(item)
        if len(text) > 500:
            raise _bad_value(f"metadata.{key}", "at most 500 characters",
                             f"{len(text)} characters")
        given[key] = body[key] = text
    return given, body


def _require_id(params: Dict[str, Any], key: str,
                prefixes: Tuple[str, ...]) -> str:
    """An object id, checked strictly since it is placed in the request path."""
    value = params.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ApiError(ErrorCode.INVALID_ACTION_PARAMS, f"'{key}' is required.",
                       retriable=False, details={"param": key})
    value = value.strip()
    if not re.match(rf"^({'|'.join(prefixes)})_[A-Za-z0-9]+$", value):
        example = " or ".join(f"'{p}_…'" for p in prefixes)
        raise ApiError(ErrorCode.INVALID_ACTION_PARAMS,
                       f"'{key}' must be a Stripe id like {example}, got "
                       f"{value!r}.",
                       retriable=False, details={key: value})
    return value


def _bad_value(name: str, expected: str, value) -> ApiError:
    return ApiError(
        ErrorCode.INVALID_ACTION_PARAMS,
        f"{name!r} must be {expected}, got {value!r}.",
        retriable=False, details={"param": name},
    )


def _money_text(minor, currency) -> str:
    return f"{_major(minor, currency):g} {str(currency or '').upper()}".strip()


def _would(action, account, summary, *, before=None, after=None,
           checked: str) -> ActionResult:
    """A dry run's result: what would happen, with nothing applied."""
    return ActionResult(
        action=action, account=account,
        summary=f"[dry-run — not applied] {summary}",
        before=before, after=after,
        details={"dry_run": True, "applied": False,
                 "validated": f"{checked}; Stripe applies its own rules only "
                              f"when the change is made"})


def make_stripe_connector(datasource) -> StripeConnector:
    """Build a Stripe connector.

    No token refresher: requests use the platform's key and the connected
    account's id, neither of which expires.
    """
    return StripeConnector(datasource)


__all__ = ["StripeConnector", "make_stripe_connector"]
