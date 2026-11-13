"""Upgraded Smart+ write actions.

From 2027-01-01 TikTok only creates Sales, App Promotion and Lead Generation
campaigns through the Smart+ endpoints.

Smart+ requests are large and nested, so each action takes the endpoint's
request fields as documented (see `smart_plus_spec`) rather than a curated
subset. Unknown fields, missing required fields and wrong basic types are
rejected before sending; conditional fields are left to TikTok.

The connector sets `advertiser_id`, `request_id` (generated unless given, for
retries) and, on creates, `operation_status=DISABLE`: Smart+ objects are
enabled by default and would start spending immediately.
"""

from __future__ import annotations
import json
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.model.types import Action, ActionResult
from terno_dbi.connectors.api.sources.tiktok.ads.smart_plus_spec import SPEC

_MAX_BATCH = 20   # ids per status call

# Reads done before writing: kind -> (endpoint, filter key, id key,
# name key, page size).
_GETTERS: Dict[str, Tuple[str, str, str, str, int]] = {
    "campaign": ("smart_plus/campaign/get/", "campaign_ids", "campaign_id",
                 "campaign_name", 1000),
    "adgroup": ("smart_plus/adgroup/get/", "adgroup_ids", "adgroup_id",
                "adgroup_name", 1000),
    "ad": ("smart_plus/ad/get/", "smart_plus_ad_ids", "smart_plus_ad_id",
           "ad_name", 100),
}
_NOUN = {"campaign": "Smart+ campaign", "adgroup": "Smart+ ad group",
         "ad": "Smart+ ad"}
# Response fields to return, by endpoint.
_RESULT_KEYS: Dict[str, Tuple[str, ...]] = {
    "smart_plus/campaign/create/": ("campaign_id",),
    "smart_plus/adgroup/create/": ("adgroup_id",),
    "smart_plus/ad/create/": ("smart_plus_ad_id",),
    "smart_plus/campaign/copy/task/create/": ("task_id", "adgroup_error_list"),
    "smart_plus/value_rule_set/create_or_update/": ("value_rule_set_id",),
    "smart_plus/ad/preview/": ("preview_link", "iframe"),
}


@dataclass(frozen=True)
class SpecAction:
    """One Smart+ action and the fields the connector controls."""

    id: str
    name: str
    description: str
    path: str
    # Set by the connector; callers can't pass them.
    forced: Tuple[Tuple[str, Any], ...] = ()
    # Endpoint takes an idempotency `request_id`.
    request_id: bool = False
    # Entities to read first: (kind, path to their ids in the params,
    # e.g. "budget[].adgroup_id").
    entity: Optional[str] = None
    ids_from: Tuple[str, ...] = ()
    destructive: bool = True


def _status(kind: str, op: str, ids_key: str, verb: str, text: str,
            *, destructive: bool = True) -> SpecAction:
    path = {"campaign": "smart_plus/campaign/status/update/",
            "adgroup": "smart_plus/adgroup/status/update/",
            "ad": "smart_plus/ad/status/update/"}[kind]
    return SpecAction(
        f"smart_plus_{verb}_{'ad_group' if kind == 'adgroup' else kind}",
        f"{verb.capitalize()} {_NOUN[kind].lower()}", text, path,
        forced=(("operation_status", op),), entity=kind,
        ids_from=(ids_key,), destructive=destructive)


SPEC_ACTIONS: List[SpecAction] = [
    # campaigns
    SpecAction(
        "smart_plus_create_campaign", "Create Smart+ campaign",
        "Create an Upgraded Smart+ campaign (objective APP_PROMOTION, "
        "WEB_CONVERSIONS or LEAD_GENERATION). Takes the official "
        "smart_plus/campaign/create/ fields. Created PAUSED; enable with "
        "smart_plus_enable_campaign. Budget optimisation is on by default.",
        "smart_plus/campaign/create/",
        forced=(("operation_status", "DISABLE"),), request_id=True),
    SpecAction(
        "smart_plus_update_campaign", "Update Smart+ campaign",
        "Change a Smart+ campaign's name, budget, budget auto-adjust strategy, "
        "promotional setting or PO number.",
        "smart_plus/campaign/update/", entity="campaign",
        ids_from=("campaign_id",)),
    _status("campaign", "DISABLE", "campaign_ids", "pause",
            "Pause Smart+ campaigns (up to 20). Reversible."),
    _status("campaign", "ENABLE", "campaign_ids", "enable",
            "Enable Smart+ campaigns (up to 20). This STARTS SPEND — a "
            "deliberate, separately confirmed step."),
    _status("campaign", "DELETE", "campaign_ids", "delete",
            "PERMANENTLY delete Smart+ campaigns (up to 20); TikTok cannot "
            "restore them. Prefer pause."),
    SpecAction(
        "smart_plus_copy_campaign", "Copy Smart+ campaign",
        "Copy a Smart+ campaign (asynchronously) with its ad groups and ads, "
        "optionally changing name, budget, schedule and per-ad-group/ad "
        "settings. The copy is created PAUSED. Returns a task_id; check it "
        "with the SmartPlusCopyTask report.",
        "smart_plus/campaign/copy/task/create/",
        forced=(("operation_status", "DISABLE"),), request_id=True,
        entity="campaign", ids_from=("campaign_id",)),
    # ad groups
    SpecAction(
        "smart_plus_create_ad_group", "Create Smart+ ad group",
        "Create an Upgraded Smart+ ad group (optimisation, bidding, "
        "targeting_spec, budget, schedule, placements …) with the official "
        "smart_plus/adgroup/create/ fields. Created PAUSED.",
        "smart_plus/adgroup/create/",
        forced=(("operation_status", "DISABLE"),), request_id=True,
        entity="campaign", ids_from=("campaign_id",)),
    SpecAction(
        "smart_plus_update_ad_group", "Update Smart+ ad group",
        "Change a Smart+ ad group's name, bids, targeting_spec, schedule, "
        "keywords, budget and other updatable fields. Objects given replace "
        "the current ones.",
        "smart_plus/adgroup/update/", entity="adgroup",
        ids_from=("adgroup_id",)),
    SpecAction(
        "smart_plus_set_ad_group_budgets", "Set Smart+ ad group budgets",
        "Set the budgets of Smart+ ad groups now (budget: [{adgroup_id, "
        "budget}]) or as a scheduled change (scheduled_budget). Affects spend.",
        "smart_plus/adgroup/budget/update/", entity="adgroup",
        ids_from=("budget[].adgroup_id", "scheduled_budget[].adgroup_id")),
    _status("adgroup", "DISABLE", "adgroup_ids", "pause",
            "Pause Smart+ ad groups (up to 20). Reversible."),
    _status("adgroup", "ENABLE", "adgroup_ids", "enable",
            "Enable Smart+ ad groups (up to 20). STARTS SPEND."),
    _status("adgroup", "DELETE", "adgroup_ids", "delete",
            "PERMANENTLY delete Smart+ ad groups (up to 20)."),
    # ads
    SpecAction(
        "smart_plus_create_ad", "Create Smart+ ad",
        "Create an Upgraded Smart+ ad (creative_list, ad_text_list, "
        "call_to_action_list, landing pages, ad_configuration …) with the "
        "official smart_plus/ad/create/ fields. Created PAUSED.",
        "smart_plus/ad/create/",
        forced=(("operation_status", "DISABLE"),),
        entity="adgroup", ids_from=("adgroup_id",)),
    SpecAction(
        "smart_plus_update_ad", "Update Smart+ ad",
        "Change a Smart+ ad's creatives, texts, calls to action, pages, "
        "disclaimer or configuration. Lists given replace the current ones; "
        "an edited ad goes back through review.",
        "smart_plus/ad/update/", entity="ad", ids_from=("smart_plus_ad_id",)),
    _status("ad", "DISABLE", "smart_plus_ad_ids", "pause",
            "Pause Smart+ ads (up to 20). Reversible."),
    _status("ad", "ENABLE", "smart_plus_ad_ids", "enable",
            "Enable Smart+ ads (up to 20). STARTS SPEND once approved."),
    _status("ad", "DELETE", "smart_plus_ad_ids", "delete",
            "PERMANENTLY delete Smart+ ads (up to 20)."),
    SpecAction(
        "smart_plus_pause_creatives", "Pause Smart+ creatives",
        "Pause individual creatives (ad_material_ids) inside a Smart+ ad.",
        "smart_plus/ad/material_status/update/",
        forced=(("operation_status", "DISABLE"),),
        entity="ad", ids_from=("smart_plus_ad_id",)),
    SpecAction(
        "smart_plus_enable_creatives", "Enable Smart+ creatives",
        "Enable individual creatives (ad_material_ids) inside a Smart+ ad. "
        "Can start spend.",
        "smart_plus/ad/material_status/update/",
        forced=(("operation_status", "ENABLE"),),
        entity="ad", ids_from=("smart_plus_ad_id",)),
    SpecAction(
        "smart_plus_appeal_ad", "Appeal Smart+ ad rejection",
        "Appeal a Smart+ ad's review rejection, with a reason and optional "
        "attachment links.",
        "smart_plus/ad/appeal/", entity="ad", ids_from=("smart_plus_ad_id",),
        destructive=False),
    SpecAction(
        "smart_plus_preview_ad", "Preview Smart+ ad",
        "Get a preview link for a Smart+ ad: preview_type ADS_CREATION with "
        "creation fields, or AD with an existing smart_plus_ad_id. Changes "
        "nothing.",
        "smart_plus/ad/preview/", destructive=False),
    # value rule sets (Value-Based Optimization rules)
    SpecAction(
        "smart_plus_create_value_rule_set", "Create Smart+ value rule set",
        "Create a value rule set (bid adjustments by gender, age, location, "
        "OS or audience) for Smart+ ad groups.",
        "smart_plus/value_rule_set/create_or_update/",
        forced=(("operation_type", "CREATE"),)),
    SpecAction(
        "smart_plus_update_value_rule_set", "Update Smart+ value rule set",
        "Update a value rule set (value_rule_set.value_rule_set_id).",
        "smart_plus/value_rule_set/create_or_update/",
        forced=(("operation_type", "UPDATE"),)),
    SpecAction(
        "smart_plus_delete_value_rule_set", "Delete Smart+ value rule set",
        "PERMANENTLY delete a value rule set (value_rule_set."
        "value_rule_set_id).",
        "smart_plus/value_rule_set/create_or_update/",
        forced=(("operation_type", "DELETE"),)),
]
SPEC_ACTIONS_BY_ID: Dict[str, SpecAction] = {a.id: a for a in SPEC_ACTIONS}


# --- schema -----------------------------------------------------------------

_SCALAR = {"string": {"type": "string"}, "number": {"type": "number"},
           "float": {"type": "number"}, "integer": {"type": "integer"},
           "boolean": {"type": "boolean"}}
_FLAG = {"R": "Required.",
         "C": "Conditionally required — see TikTok's reference.",
         "O": "Optional."}


def _prop_schema(spec: Dict[str, Any]) -> Dict[str, Any]:
    t = spec["t"]
    if t == "object":
        schema = _object_schema(spec.get("f") or {}, ())
    elif t == "object[]":
        schema = {"type": "array",
                  "items": _object_schema(spec.get("f") or {}, ())}
    elif t.endswith("[]"):
        schema = {"type": "array", "items": dict(_SCALAR.get(t[:-2], {}))}
    else:
        schema = dict(_SCALAR.get(t, {}))
    schema["description"] = _FLAG[spec["r"]]
    return schema


def _object_schema(fields: Dict[str, Any], hidden) -> Dict[str, Any]:
    if not fields:
        return {"type": "object"}
    props = {k: _prop_schema(v) for k, v in fields.items() if k not in hidden}
    required = [k for k, v in fields.items()
                if v["r"] == "R" and k not in hidden]
    return {"type": "object", "properties": props, "required": required,
            "additionalProperties": False}


def _hidden(action: SpecAction) -> frozenset:
    return frozenset({"advertiser_id"} | {k for k, _ in action.forced})


def as_action(action: SpecAction) -> Action:
    fields = SPEC[action.path]["f"]
    schema = _object_schema(fields, _hidden(action))
    if action.request_id:
        # Optional for the caller: generated unless reused for a retry.
        schema["required"] = [k for k in schema["required"] if k != "request_id"]
        schema["properties"]["request_id"]["description"] = (
            "Optional. TikTok's idempotency key; generated when not given. "
            "Pass the same value to retry a create safely.")
    return Action(action.id, action.name, action.description, schema=schema,
                  destructive=action.destructive)


# --- validation -------------------------------------------------------------


def _bad(message: str, **details) -> ApiError:
    return ApiError(ErrorCode.INVALID_ACTION_PARAMS, message, retriable=False,
                    details=details)


def _check_object(value: Any, fields: Dict[str, Any], where: str) -> None:
    if not isinstance(value, dict):
        raise _bad(f"{where or 'params'} must be an object.", field=where)
    if not fields:
        return   # sub-fields not documented; TikTok checks them
    unknown = sorted(set(value) - set(fields))
    if unknown:
        raise _bad(f"Not a TikTok field at {where or 'top level'}: "
                   f"{', '.join(unknown)}. Allowed: {', '.join(fields)}.",
                   unknown=[f"{where}{u}" for u in unknown])
    missing = [k for k, s in fields.items()
               if s["r"] == "R" and value.get(k) in (None, "", [], {})]
    if missing:
        raise _bad(f"Missing required TikTok field(s) at "
                   f"{where or 'top level'}: {', '.join(missing)}.",
                   missing=[f"{where}{m}" for m in missing])
    for key, item in value.items():
        _check_value(item, fields[key], f"{where}{key}")


def _check_value(value: Any, spec: Dict[str, Any], where: str) -> None:
    t = spec["t"]
    if t == "object":
        _check_object(value, spec.get("f") or {}, f"{where}.")
    elif t == "object[]":
        if not isinstance(value, list):
            raise _bad(f"{where} must be a list of objects.", field=where)
        for i, item in enumerate(value):
            _check_object(item, spec.get("f") or {}, f"{where}[{i}].")
    elif t.endswith("[]"):
        if not isinstance(value, list):
            raise _bad(f"{where} must be a list.", field=where)
        for i, item in enumerate(value):
            _check_scalar(item, t[:-2], f"{where}[{i}]")
    else:
        _check_scalar(value, t, where)


def _check_scalar(value: Any, t: str, where: str) -> None:
    ok = {
        "string": isinstance(value, str),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool),
        "float": isinstance(value, (int, float)) and not isinstance(value, bool),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "boolean": isinstance(value, bool),
    }.get(t, True)
    if not ok:
        raise _bad(f"{where} must be a TikTok {t}.", field=where, value=value)


def _ids_at(params: Dict[str, Any], path: str) -> List[str]:
    """Ids at a path like 'campaign_ids', 'campaign_id' or 'budget[].x'."""
    if "[]." in path:
        head, tail = path.split("[].", 1)
        return [str(item[tail]) for item in params.get(head) or []
                if isinstance(item, dict) and item.get(tail) is not None]
    value = params.get(path)
    if value is None:
        return []
    return [str(v) for v in value] if isinstance(value, list) else [str(value)]


def new_request_id() -> str:
    """New numeric request_id."""
    return str(uuid.uuid4().int)[:18]


# --- execution --------------------------------------------------------------


class SmartPlusMixin:
    """Smart+ methods of TikTokAdsConnector."""

    def _sp_entities(self, account: str, kind: str,
                     ids: List[str]) -> Dict[str, Dict[str, Any]]:
        path, filter_key, id_key, _name, page_size = _GETTERS[kind]
        data = self._call(path, {
            "advertiser_id": account,
            "filtering": json.dumps({filter_key: ids}),
            "page_size": page_size,
        })
        found = {str(o.get(id_key)): o
                 for o in (data.get("list") or []) if isinstance(o, dict)}
        missing = [i for i in ids if i not in found]
        if missing:
            raise _bad(f"{_NOUN[kind]} {', '.join(missing)} was not found in "
                       f"advertiser {account} (or is deleted).",
                       missing=missing)
        return found

    def _run_spec_action(self, action: SpecAction, account: str,
                         params: Dict[str, Any], dry_run: bool) -> ActionResult:
        owned = sorted(set(params) & _hidden(action))
        if owned:
            raise _bad(f"{', '.join(owned)} is set by the connector "
                       f"(the account, and paused-on-create); remove it.",
                       fields=owned)
        body: Dict[str, Any] = dict(params)
        if action.request_id and not body.get("request_id"):
            body["request_id"] = new_request_id()
        body["advertiser_id"] = account
        body.update(dict(action.forced))
        _check_object(body, SPEC[action.path]["f"], "")

        before: Optional[Dict[str, Any]] = None
        label = action.name.lower()
        if action.entity:
            ids = list(dict.fromkeys(
                i for p in action.ids_from for i in _ids_at(params, p)))
            if not ids:
                raise _bad(f"No {_NOUN[action.entity].lower()} id given.")
            if len(ids) > _MAX_BATCH:
                raise _bad(f"At most {_MAX_BATCH} ids per call; got "
                           f"{len(ids)}.")
            current = self._sp_entities(account, action.entity, ids)
            name_key = _GETTERS[action.entity][3]
            changed = [k for k in params if k not in action.ids_from]
            snap = {i: {_GETTERS[action.entity][2]: i,
                        name_key: current[i].get(name_key),
                        "operation_status": current[i].get("operation_status"),
                        **{k: current[i].get(k) for k in changed
                           if k in current[i]}}
                    for i in ids}
            before = snap[ids[0]] if len(ids) == 1 else snap
            names = [current[i].get(name_key) or i for i in ids]
            label = (f"{action.name.lower()} — {', '.join(map(str, names[:3]))}"
                     f"{'…' if len(names) > 3 else ''}")

        if dry_run:
            shown = dict(body)
            return ActionResult(
                action=action.id, account=account,
                summary=f"[dry run] Would {label} (validated against TikTok's "
                        f"official fields; nothing sent).",
                before=before,
                details={"dry_run": True, "applied": False, "request": shown})

        data = self._post(action.path, body)
        result: Dict[str, Any] = {}
        if isinstance(data, dict):
            for key in _RESULT_KEYS.get(action.path, ()):
                if key in data:
                    result[key] = data[key]
        forced = dict(action.forced)
        after: Dict[str, Any] = {**result}
        if "operation_status" in forced:
            after["operation_status"] = forced["operation_status"]
        summary = f"Done: {label}."
        if action.path.endswith("/create/") and "operation_status" in forced:
            new_id = next(iter(result.values()), None)
            summary = (f"Created ({', '.join(f'{k} {v}' for k, v in result.items()) or 'no id returned'}), "
                       f"paused. Enable it with the matching smart_plus_enable_* "
                       f"action once it is ready.") if new_id is not None else \
                f"Created, paused: {label}."
        if action.request_id:
            after["request_id"] = body["request_id"]
        return ActionResult(action=action.id, account=account, summary=summary,
                            before=before, after=after or None,
                            details=result)


__all__ = ["SPEC_ACTIONS", "SPEC_ACTIONS_BY_ID", "SmartPlusMixin", "SpecAction",
           "as_action", "new_request_id"]
