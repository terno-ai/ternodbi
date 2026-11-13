"""Write-action handlers.

Each handler reads the entities it changes first (which also proves they
belong to the advertiser), creates things paused, and stops before writing
when `dry_run` is set.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Optional, Tuple

from terno_dbi.connectors.api.model.types import Action, ActionResult
from terno_dbi.connectors.api.model.errors import ApiError, ErrorCode
from terno_dbi.connectors.api.sources.tiktok.ads.smart_plus import (
    SPEC_ACTIONS,
    SPEC_ACTIONS_BY_ID,
    as_action,
)
from terno_dbi.connectors.api.sources.tiktok.ads.actions import (
    _ACTIONS,
    _ACTIONS_BY_ID,
    _ADGROUP_CREATE_KEYS,
    _ADGROUP_PROPS,
    _ADGROUP_SETTERS,
    _AD_CARRY,
    _AD_PROPS,
    _CAMPAIGN_PROPS,
    _CAMPAIGN_UPDATABLE,
    _COMMENT_IDENTITY_TYPES,
    _HANDLERS,
    _LOOKALIKE_OS,
    _LOOKALIKE_PLACEMENTS,
    _NOUN,
    _NO_PROMOTION_TYPE_OBJECTIVES,
    _RF_OBJECTIVES,
    _STATUS_ACTIONS,
    _STATUS_VERB,
    _bad,
    _check_ad_texts,
    _check_known,
    _check_rule,
    _coerce,
    _extra,
    _first_id,
    _hash_members,
    _ids,
    _label,
    _norm_id,
    _one_or_many,
    _pick,
    _required,
    _snap,
    _utc_soon,
    _would,
)


class WritesMixin:
    """Write methods of TikTokAdsConnector."""

    def list_actions(self) -> List[Action]:
        return [*_ACTIONS, *(as_action(a) for a in SPEC_ACTIONS)]

    def execute_action(
        self, action_id: str, account: str,
        params: Optional[Dict[str, Any]] = None, dry_run: bool = False,
    ) -> ActionResult:
        """Run one write action on one advertiser.

        The caller checks that writes are allowed for the account. TikTok has
        no validate-only mode, so `dry_run` checks input, does the reads and
        stops.
        """
        action = _ACTIONS_BY_ID.get(action_id)
        spec_action = SPEC_ACTIONS_BY_ID.get(action_id)
        if action is None and spec_action is None:
            raise ApiError(
                ErrorCode.UNKNOWN_ACTION,
                f"Unknown action {action_id!r}. Call list_actions for the "
                f"available actions.",
                retriable=False,
                details={"action": action_id,
                         "available": sorted([*_ACTIONS_BY_ID,
                                              *SPEC_ACTIONS_BY_ID])},
            )
        if params is not None and not isinstance(params, dict):
            raise _bad("'params' must be an object.")
        params = dict(params or {})
        if spec_action is not None:
            return self._run_spec_action(
                spec_action, _norm_id(account, "account"), params,
                bool(dry_run))
        _check_known(action, params)
        account = _norm_id(account, "account")
        if action_id in _STATUS_ACTIONS:
            return self._set_status(action_id, account, params, bool(dry_run))
        handler = getattr(self, _HANDLERS[action_id])
        return handler(action_id, account, params, bool(dry_run))

    def _entities(self, account: str, kind: str,
                  ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Fetch entities by id; raise if any doesn't exist."""
        data = self._call(f"{kind}/get/", {
            "advertiser_id": account,
            "filtering": json.dumps({f"{kind}_ids": ids}),
            "page_size": 100,
        })
        found = {str(o.get(f"{kind}_id")): o
                 for o in (data.get("list") or []) if isinstance(o, dict)}
        missing = [i for i in ids if i not in found]
        if missing:
            raise _bad(f"{_NOUN[kind]} {', '.join(missing)} was not found in "
                       f"advertiser {account} (or is deleted).",
                       missing=missing)
        return found

    def _audiences(self, account: str,
                   ids: List[str]) -> Dict[str, Dict[str, Any]]:
        data = self._call("dmp/custom_audience/get/", {
            "advertiser_id": account,
            "custom_audience_ids": json.dumps(ids),
        })
        found: Dict[str, Dict[str, Any]] = {}
        for item in (data.get("list") or []) if isinstance(data, dict) else []:
            detail = item.get("audience_details") or item
            aid = detail.get("custom_audience_id") or detail.get("audience_id")
            if aid is not None:
                found[str(aid)] = detail
        missing = [i for i in ids if i not in found]
        if missing:
            raise _bad(f"Audience {', '.join(missing)} was not found in "
                       f"advertiser {account}.", missing=missing)
        return found

    def _set_status(self, action_id, account, params, dry_run) -> ActionResult:
        kind, status = _STATUS_ACTIONS[action_id]
        noun = _NOUN[kind]
        ids = _ids(params, f"{kind}_id", f"{kind}_ids")
        current = self._entities(account, kind, ids)
        snaps = {i: _snap(current[i], (f"{kind}_id", f"{kind}_name",
                                        "operation_status", "secondary_status"))
                 for i in ids}
        names = {i: current[i].get(f"{kind}_name") for i in ids}
        verb, done = _STATUS_VERB[status]
        label = _label(ids, noun, names)
        body = {"advertiser_id": account, f"{kind}_ids": ids,
                "operation_status": status}
        if dry_run:
            return _would(action_id, account, f"Would {verb} {label}.",
                          before=_one_or_many(ids, snaps), request=body)
        self._post(f"{kind}/status/update/", body)
        after = {i: {**snaps[i], "operation_status": status} for i in ids}
        return ActionResult(
            action=action_id, account=account,
            summary=f"{label[0].upper()}{label[1:]} {done}.",
            before=_one_or_many(ids, snaps), after=_one_or_many(ids, after))

    def _create_campaign(self, action_id, account, params, dry_run):
        _required(params, "campaign_name", "objective_type", "budget_mode")
        fields = _pick(params, _CAMPAIGN_PROPS, _CAMPAIGN_PROPS)
        objective = fields["objective_type"]
        if objective in _RF_OBJECTIVES:
            raise _bad("Reach & Frequency campaigns cannot be created paused "
                       "(TikTok forbids DISABLE at creation), so they are not "
                       "created here. Create them in TikTok Ads Manager.",
                       param="objective_type")
        if objective == "APP_PROMOTION" and "app_promotion_type" not in fields:
            raise _bad("'app_promotion_type' is required for APP_PROMOTION.",
                       param="app_promotion_type")
        mode = fields["budget_mode"]
        cbo = bool(fields.get("budget_optimize_on"))
        allowed = (("BUDGET_MODE_TOTAL", "BUDGET_MODE_DYNAMIC_DAILY_BUDGET",
                    "BUDGET_MODE_DAY") if cbo else
                   ("BUDGET_MODE_INFINITE", "BUDGET_MODE_TOTAL",
                    "BUDGET_MODE_DAY"))
        if mode not in allowed:
            raise _bad(f"With budget optimisation {'on' if cbo else 'off'}, "
                       f"budget_mode must be one of {', '.join(allowed)}.",
                       param="budget_mode")
        if mode != "BUDGET_MODE_INFINITE" and "budget" not in fields:
            raise _bad(f"'budget' is required with {mode}.", param="budget")
        body = {"advertiser_id": account, **fields, **_extra(params),
                "operation_status": "DISABLE"}
        name = fields["campaign_name"]
        if dry_run:
            return _would(action_id, account,
                          f"Would create paused campaign {name!r} "
                          f"({objective}).", request=body)
        data = self._post("campaign/create/", body)
        new_id = _first_id(data, "campaign_id")
        after = {"campaign_id": new_id, **fields, "operation_status": "DISABLE"}
        return ActionResult(
            action=action_id, account=account,
            summary=f"Created campaign {name!r} (id {new_id}), paused. Enable "
                    f"it with enable_campaign once its ad groups and ads are "
                    f"ready.",
            after=after, details={"campaign_id": new_id})

    def _update_campaign(self, action_id, account, params, dry_run):
        campaign_id = _norm_id(params.get("campaign_id"), "campaign_id")
        changes = {**_pick(params, _CAMPAIGN_PROPS, _CAMPAIGN_UPDATABLE),
                   **_extra(params)}
        if not changes:
            raise _bad("Nothing to change: give at least one field.")
        return self._apply_update(action_id, account, "campaign", campaign_id,
                                  "campaign/update/", changes, dry_run)

    def _set_campaign_budget(self, action_id, account, params, dry_run):
        _required(params, "campaign_id", "budget")
        campaign_id = _norm_id(params["campaign_id"], "campaign_id")
        budget = _coerce("budget", params["budget"], "amount")
        current = self._entities(account, "campaign", [campaign_id])[campaign_id]
        if current.get("budget_mode") == "BUDGET_MODE_INFINITE":
            raise _bad("This campaign has no budget cap (BUDGET_MODE_INFINITE); "
                       "its budget is set per ad group — use "
                       "set_ad_group_budget.", campaign_id=campaign_id)
        return self._apply_update(action_id, account, "campaign", campaign_id,
                                  "campaign/update/", {"budget": budget},
                                  dry_run, current=current)

    def _apply_update(self, action_id, account, kind, entity_id, path, changes,
                      dry_run, *, current=None):
        """Update one entity and report its state before and after."""
        if current is None:
            current = self._entities(account, kind, [entity_id])[entity_id]
        name = current.get(f"{kind}_name")
        noun = _NOUN[kind]
        label = f"{noun.lower()} {name!r} ({entity_id})" if name \
            else f"{noun.lower()} {entity_id}"
        before = {f"{kind}_id": entity_id,
                  **{k: current.get(k) for k in changes}}
        body = {"advertiser_id": account, f"{kind}_id": entity_id, **changes}
        changed = ", ".join(f"{k}={v!r}" for k, v in changes.items())
        if dry_run:
            return _would(action_id, account, f"Would update {label}: {changed}.",
                          before=before, request=body)
        self._post(path, body)
        after = {f"{kind}_id": entity_id, **changes}
        return ActionResult(action=action_id, account=account,
                            summary=f"Updated {label}: {changed}.",
                            before=before, after=after)

    def _create_ad_group(self, action_id, account, params, dry_run):
        _required(params, "campaign_id", "adgroup_name", "location_ids",
                  "optimization_goal", "billing_event", "budget_mode",
                  "budget")
        campaign_id = _norm_id(params["campaign_id"], "campaign_id")
        fields = _pick(params, _ADGROUP_PROPS, _ADGROUP_CREATE_KEYS)
        campaign = self._entities(account, "campaign", [campaign_id])[campaign_id]
        objective = campaign.get("objective_type")
        mode = fields["budget_mode"]
        if mode not in ("BUDGET_MODE_DAY", "BUDGET_MODE_DYNAMIC_DAILY_BUDGET",
                        "BUDGET_MODE_TOTAL"):
            raise _bad("An ad group's budget_mode must be BUDGET_MODE_DAY, "
                       "BUDGET_MODE_DYNAMIC_DAILY_BUDGET or BUDGET_MODE_TOTAL.",
                       param="budget_mode")
        if objective not in _NO_PROMOTION_TYPE_OBJECTIVES \
                and "promotion_type" not in fields:
            raise _bad(f"'promotion_type' is required for a {objective} "
                       f"campaign.", param="promotion_type")
        if any(k in fields for k in ("frequency", "frequency_schedule")) \
                and objective != "REACH":
            raise _bad("A frequency cap is only available for REACH ads.",
                       param="frequency")
        fields.setdefault("placement_type", "PLACEMENT_TYPE_AUTOMATIC")
        if fields["placement_type"] == "PLACEMENT_TYPE_NORMAL" \
                and "placements" not in fields:
            raise _bad("'placements' is required with PLACEMENT_TYPE_NORMAL.",
                       param="placements")
        fields.setdefault("schedule_type", "SCHEDULE_FROM_NOW")
        if mode == "BUDGET_MODE_TOTAL" \
                and fields["schedule_type"] != "SCHEDULE_START_END":
            raise _bad("BUDGET_MODE_TOTAL needs schedule_type "
                       "SCHEDULE_START_END with a schedule_end_time.",
                       param="schedule_type")
        fields.setdefault("schedule_start_time", _utc_soon())
        if fields["schedule_type"] == "SCHEDULE_START_END" \
                and "schedule_end_time" not in fields:
            raise _bad("'schedule_end_time' is required with "
                       "SCHEDULE_START_END.", param="schedule_end_time")
        fields.setdefault("bid_type", "BID_TYPE_NO_BID")
        if fields["bid_type"] == "BID_TYPE_CUSTOM" and not any(
                k in fields for k in ("bid_price", "conversion_bid_price")):
            raise _bad("BID_TYPE_CUSTOM needs bid_price or "
                       "conversion_bid_price.", param="bid_price")
        fields.setdefault("pacing", "PACING_MODE_SMOOTH")
        body = {"advertiser_id": account, "campaign_id": campaign_id,
                **fields, **_extra(params), "operation_status": "DISABLE"}
        name = fields["adgroup_name"]
        cname = campaign.get("campaign_name") or campaign_id
        if dry_run:
            return _would(action_id, account,
                          f"Would create paused ad group {name!r} in campaign "
                          f"{cname!r}.", request=body)
        data = self._post("adgroup/create/", body)
        new_id = _first_id(data, "adgroup_id")
        return ActionResult(
            action=action_id, account=account,
            summary=f"Created ad group {name!r} (id {new_id}) in campaign "
                    f"{cname!r}, paused. Enable it with enable_ad_group.",
            after={"adgroup_id": new_id, "campaign_id": campaign_id, **fields,
                   "operation_status": "DISABLE"},
            details={"adgroup_id": new_id})

    def _set_ad_group(self, action_id, account, params, dry_run):
        adgroup_id = _norm_id(params.get("adgroup_id"), "adgroup_id")
        changes = _pick(params, _ADGROUP_PROPS, _ADGROUP_SETTERS[action_id])
        if action_id == "update_ad_group":
            changes.update(_extra(params))
        if not changes:
            raise _bad("Nothing to change: give at least one field.",
                       accepted=list(_ADGROUP_SETTERS[action_id]))
        return self._apply_update(action_id, account, "adgroup", adgroup_id,
                                  "adgroup/update/", changes, dry_run)

    def _set_ad_group_budget(self, action_id, account, params, dry_run):
        _required(params, "adgroup_id", "budget")
        adgroup_id = _norm_id(params["adgroup_id"], "adgroup_id")
        budget = _coerce("budget", params["budget"], "amount")
        current = self._entities(account, "adgroup", [adgroup_id])[adgroup_id]
        name = current.get("adgroup_name") or adgroup_id
        before = _snap(current, ("adgroup_id", "budget_mode", "budget"))
        body = {"advertiser_id": account, "adgroup_id": adgroup_id,
                "budget": budget}
        if dry_run:
            return _would(action_id, account,
                          f"Would set ad group {name!r}'s budget to {budget:g}.",
                          before=before, request=body)
        self._post("adgroup/update/", body)
        return ActionResult(
            action=action_id, account=account,
            summary=f"Ad group {name!r}'s budget set to {budget:g} "
                    f"(was {before.get('budget')}).",
            before=before, after={**before, "budget": budget})

    def _create_ad(self, action_id, account, params, dry_run):
        _required(params, "adgroup_id", "ad_name", "identity_type",
                  "identity_id")
        adgroup_id = _norm_id(params["adgroup_id"], "adgroup_id")
        creative = {**_pick(params, _AD_PROPS, _AD_PROPS), **_extra(params)}
        creative.setdefault("ad_format", "SINGLE_VIDEO")
        fmt = creative["ad_format"]
        identity_type = creative["identity_type"]
        if fmt == "SINGLE_VIDEO":
            if identity_type == "CUSTOMIZED_USER" \
                    and not creative.get("video_id"):
                raise _bad("A SINGLE_VIDEO ad with a CUSTOMIZED_USER identity "
                           "needs video_id (upload_video).", param="video_id")
            if not (creative.get("video_id") or creative.get("tiktok_item_id")):
                raise _bad("A SINGLE_VIDEO ad needs video_id or, for Spark "
                           "Ads, tiktok_item_id.", param="video_id")
        if fmt in ("SINGLE_IMAGE", "CAROUSEL_ADS"):
            if creative.get("video_id"):
                raise _bad(f"video_id is not supported for {fmt}.",
                           param="video_id")
            if not creative.get("image_ids"):
                raise _bad(f"A {fmt} ad needs image_ids (upload_image).",
                           param="image_ids")
        if not creative.get("tiktok_item_id") and not creative.get("ad_text"):
            raise _bad("'ad_text' is required (except for Spark Ads).",
                       param="ad_text")
        _check_ad_texts(creative)
        group = self._entities(account, "adgroup", [adgroup_id])[adgroup_id]
        gname = group.get("adgroup_name") or adgroup_id
        # Create the ad paused in the same call, so it never runs unreviewed.
        creative["operation_status"] = "DISABLE"
        body = {"advertiser_id": account, "adgroup_id": adgroup_id,
                "creatives": [creative]}
        if dry_run:
            return _would(action_id, account,
                          f"Would create paused ad {creative['ad_name']!r} in "
                          f"ad group {gname!r}.", request=body)
        data = self._post("ad/create/", body)
        ad_ids = [str(i) for i in (data.get("ad_ids") or [])] \
            if isinstance(data, dict) else []
        return ActionResult(
            action=action_id, account=account,
            summary=f"Created ad {creative['ad_name']!r} (id "
                    f"{', '.join(ad_ids) or '?'}) in ad group {gname!r}, "
                    f"paused. Enable it with enable_ad; it serves once TikTok "
                    f"approves it.",
            after={"ad_ids": ad_ids, "adgroup_id": adgroup_id, **creative},
            details={"ad_ids": ad_ids})

    def _update_ad(self, action_id, account, params, dry_run):
        ad_id = _norm_id(params.get("ad_id"), "ad_id")
        changes = {**_pick(params, _AD_PROPS, _AD_PROPS), **_extra(params)}
        _check_ad_texts(changes)
        if not changes:
            raise _bad("Nothing to change: give at least one field.")
        current = self._entities(account, "ad", [ad_id])[ad_id]
        carried = {k: current[k] for k in _AD_CARRY
                   if current.get(k) not in (None, "", [])}
        creative = {**carried, **changes, "ad_id": ad_id}
        adgroup_id = str(current.get("adgroup_id") or "")
        name = current.get("ad_name") or ad_id
        before = {"ad_id": ad_id, **{k: current.get(k) for k in changes}}
        body = {"advertiser_id": account, "adgroup_id": adgroup_id,
                "creatives": [creative]}
        changed = ", ".join(f"{k}={v!r}" for k, v in changes.items())
        if dry_run:
            return _would(action_id, account,
                          f"Would update ad {name!r} ({ad_id}): {changed}.",
                          before=before, request=body)
        self._post("ad/update/", body)
        return ActionResult(
            action=action_id, account=account,
            summary=f"Updated ad {name!r} ({ad_id}): {changed}. It goes back "
                    f"through TikTok review.",
            before=before, after={"ad_id": ad_id, **changes})

    def _upload_image(self, action_id, account, params, dry_run):
        url = _coerce("image_url", params.get("image_url"), "url")
        body = {"advertiser_id": account, "upload_type": "UPLOAD_BY_URL",
                "image_url": url}
        if params.get("file_name") is not None:
            body["file_name"] = _coerce("file_name", params["file_name"], "text")
        if dry_run:
            return _would(action_id, account, f"Would upload image {url}.",
                          request=body)
        data = self._post("file/image/ad/upload/", body)
        image_id = _first_id(data, "image_id", "id")
        return ActionResult(action=action_id, account=account,
                            summary=f"Uploaded image (image_id {image_id}).",
                            after={"image_id": image_id, "image_url": url},
                            details={"image_id": image_id})

    def _upload_video(self, action_id, account, params, dry_run):
        url = _coerce("video_url", params.get("video_url"), "url")
        body = {"advertiser_id": account, "upload_type": "UPLOAD_BY_URL",
                "video_url": url}
        if params.get("file_name") is not None:
            body["file_name"] = _coerce("file_name", params["file_name"], "text")
        if dry_run:
            return _would(action_id, account, f"Would upload video {url}.",
                          request=body)
        data = self._post("file/video/ad/upload/", body)
        video_id = _first_id(data, "video_id", "id")
        return ActionResult(action=action_id, account=account,
                            summary=f"Uploaded video (video_id {video_id}).",
                            after={"video_id": video_id, "video_url": url},
                            details={"video_id": video_id})

    def _create_identity(self, action_id, account, params, dry_run):
        name = _coerce("display_name", params.get("display_name"), "text")
        if len(name) > 100:
            raise _bad("'display_name' is at most 100 characters.")
        body = {"advertiser_id": account, "display_name": name}
        if params.get("image_uri") is not None:
            body["image_uri"] = _coerce("image_uri", params["image_uri"], "text")
        if dry_run:
            return _would(action_id, account,
                          f"Would create identity {name!r}.", request=body)
        data = self._post("identity/create/", body)
        identity_id = _first_id(data, "identity_id")
        return ActionResult(
            action=action_id, account=account,
            summary=f"Created identity {name!r} (identity_id {identity_id}, "
                    f"type CUSTOMIZED_USER).",
            after={"identity_id": identity_id, "identity_type": "CUSTOMIZED_USER",
                   "display_name": name},
            details={"identity_id": identity_id})

    def _authorize_spark_post(self, action_id, account, params, dry_run):
        code = _coerce("auth_code", params.get("auth_code"), "text")
        body = {"advertiser_id": account, "auth_code": code}
        if dry_run:
            return _would(action_id, account,
                          "Would authorise the Spark post for this advertiser.",
                          request={"advertiser_id": account,
                                   "auth_code": "<redacted>"})
        self._post("tt_video/authorize/", body)
        return ActionResult(
            action=action_id, account=account,
            summary="Spark post authorised; it now appears in the SparkPosts "
                    "report (use its item_id as tiktok_item_id).")

    def _upload_members(self, account, params) -> Tuple[str, str, int]:
        """Hash, upload and return (file_path, calculate_type, count)."""
        calc, hashes = _hash_members(params)
        content = ("\n".join(hashes) + "\n").encode("utf-8")
        signature = hashlib.md5(content).hexdigest()   # noqa: S324 (required by TikTok)
        data = self._post(
            "dmp/custom_audience/file/upload/",
            {"advertiser_id": account, "calculate_type": calc,
             "file_signature": signature},
            files={"file": ("audience.txt", content, "text/plain")})
        path = (data or {}).get("file_path") if isinstance(data, dict) else None
        if not path:
            raise ApiError(ErrorCode.UPSTREAM_ERROR,
                           "TikTok accepted the audience file but returned no "
                           "file_path.", retriable=False)
        return path, calc, len(hashes)

    def _create_customer_file_audience(self, action_id, account, params, dry_run):
        _required(params, "name", "identifier_type", "members")
        name = _coerce("name", params["name"], "text")
        retention = params.get("retention_in_days")
        if retention is not None:
            retention = _coerce("retention_in_days", retention, "int")
            if retention > 365:
                raise _bad("'retention_in_days' is 1–365.",
                           param="retention_in_days")
        calc, hashes = _hash_members(params)
        if dry_run:
            return _would(action_id, account,
                          f"Would create audience {name!r} from {len(hashes)} "
                          f"hashed {params['identifier_type']} identifiers "
                          f"(nothing uploaded).")
        path, calc, count = self._upload_members(account, params)
        body: Dict[str, Any] = {"advertiser_id": account,
                                "custom_audience_name": name,
                                "file_paths": [path], "calculate_type": calc}
        if retention is not None:
            body["retention_in_days"] = retention
        data = self._post("dmp/custom_audience/create/", body)
        new_id = _first_id(data, "custom_audience_id", "audience_id")
        return ActionResult(
            action=action_id, account=account,
            summary=f"Created audience {name!r} (id {new_id}) from {count} "
                    f"hashed identifiers. TikTok matches it over the next "
                    f"hours.",
            after={"custom_audience_id": new_id, "name": name,
                   "uploaded": count},
            details={"custom_audience_id": new_id})

    def _change_audience_members(self, action_id, account, params, dry_run):
        _required(params, "custom_audience_id", "identifier_type", "members")
        aid = _norm_id(params["custom_audience_id"], "custom_audience_id")
        current = self._audiences(account, [aid])[aid]
        before = _snap(current, ("custom_audience_id", "name", "cover_num"))
        mode = "APPEND" if action_id == "add_audience_members" else "REMOVE"
        verb = "add" if mode == "APPEND" else "remove"
        calc, hashes = _hash_members(params)
        if dry_run:
            return _would(action_id, account,
                          f"Would {verb} {len(hashes)} hashed members "
                          f"{'to' if mode == 'APPEND' else 'from'} audience "
                          f"{current.get('name')!r} (nothing uploaded).",
                          before=before)
        path, _calc, count = self._upload_members(account, params)
        self._post("dmp/custom_audience/update/", {
            "advertiser_id": account, "custom_audience_id": aid,
            "action": mode, "file_paths": [path]})
        return ActionResult(
            action=action_id, account=account,
            summary=f"{'Added' if mode == 'APPEND' else 'Removed'} {count} "
                    f"hashed members {'to' if mode == 'APPEND' else 'from'} "
                    f"audience {current.get('name')!r} ({aid}).",
            before=before, after={**before, "uploaded": count, "action": mode})

    def _rename_audience(self, action_id, account, params, dry_run):
        _required(params, "custom_audience_id", "name")
        aid = _norm_id(params["custom_audience_id"], "custom_audience_id")
        name = _coerce("name", params["name"], "text")
        current = self._audiences(account, [aid])[aid]
        before = {"custom_audience_id": aid, "name": current.get("name")}
        body = {"advertiser_id": account, "custom_audience_id": aid,
                "custom_audience_name": name}
        if dry_run:
            return _would(action_id, account,
                          f"Would rename audience {before['name']!r} to "
                          f"{name!r}.", before=before, request=body)
        self._post("dmp/custom_audience/update/", body)
        return ActionResult(action=action_id, account=account,
                            summary=f"Renamed audience {before['name']!r} to "
                                    f"{name!r}.",
                            before=before, after={**before, "name": name})

    def _create_lookalike_audience(self, action_id, account, params, dry_run):
        _required(params, "name", "source_audience_id", "location_ids",
                  "audience_size", "mobile_os", "placements", "include_source")
        name = _coerce("name", params["name"], "text")
        source = _norm_id(params["source_audience_id"], "source_audience_id")
        size = params["audience_size"]
        if size not in ("NARROW", "BALANCED", "BROAD"):
            raise _bad("'audience_size' must be NARROW, BALANCED or BROAD.")
        mobile_os = params["mobile_os"]
        if mobile_os not in _LOOKALIKE_OS:
            raise _bad("'mobile_os' must be ALL, ANDROID or IOS.")
        placements = params["placements"]
        if (not isinstance(placements, list) or not placements
                or any(x not in _LOOKALIKE_PLACEMENTS for x in placements)):
            raise _bad("'placements' must be a list of: "
                       + ", ".join(_LOOKALIKE_PLACEMENTS) + ".")
        spec = {"source_audience_id": source,
                "location_ids": _coerce("location_ids", params["location_ids"],
                                        "ids"),
                "audience_size": size,
                "mobile_os": mobile_os,
                "placements": list(dict.fromkeys(placements)),
                "include_source": _coerce("include_source",
                                          params["include_source"], "bool"),
                **_extra(params)}
        src = self._audiences(account, [source])[source]
        body = {"advertiser_id": account, "custom_audience_name": name,
                "lookalike_spec": spec}
        if dry_run:
            return _would(action_id, account,
                          f"Would create {size.lower()} lookalike {name!r} of "
                          f"{src.get('name')!r}.", request=body)
        data = self._post("dmp/custom_audience/lookalike/create/", body)
        new_id = _first_id(data, "custom_audience_id", "audience_id")
        return ActionResult(
            action=action_id, account=account,
            summary=f"Created lookalike audience {name!r} (id {new_id}) of "
                    f"{src.get('name')!r}.",
            after={"custom_audience_id": new_id, "name": name, **spec},
            details={"custom_audience_id": new_id})

    def _create_rule_audience(self, action_id, account, params, dry_run):
        _required(params, "name", "audience_type", "rule_spec")
        name = _coerce("name", params["name"], "text")
        body = {"advertiser_id": account, "custom_audience_name": name,
                "audience_type": _coerce("audience_type",
                                         params["audience_type"], "token"),
                "rule_spec": _coerce("rule_spec", params["rule_spec"], "object"),
                **_extra(params)}
        if dry_run:
            return _would(action_id, account,
                          f"Would create {body['audience_type'].lower()} "
                          f"audience {name!r}.", request=body)
        data = self._post("dmp/custom_audience/rule/create/", body)
        new_id = _first_id(data, "custom_audience_id", "audience_id")
        return ActionResult(
            action=action_id, account=account,
            summary=f"Created {body['audience_type'].lower()} audience "
                    f"{name!r} (id {new_id}).",
            after={"custom_audience_id": new_id, "name": name,
                   "audience_type": body["audience_type"]},
            details={"custom_audience_id": new_id})

    def _delete_audience(self, action_id, account, params, dry_run):
        ids = _ids(params, "custom_audience_id", "custom_audience_ids")
        current = self._audiences(account, ids)
        snaps = {i: _snap(current[i], ("custom_audience_id", "name",
                                       "audience_type", "cover_num"))
                 for i in ids}
        label = _label(ids, "Audience", {i: current[i].get("name") for i in ids})
        body = {"advertiser_id": account, "custom_audience_ids": ids}
        if dry_run:
            return _would(action_id, account,
                          f"Would PERMANENTLY delete {label}.",
                          before=_one_or_many(ids, snaps), request=body)
        self._post("dmp/custom_audience/delete/", body)
        return ActionResult(action=action_id, account=account,
                            summary=f"Deleted {label} (permanent).",
                            before=_one_or_many(ids, snaps),
                            after={"deleted": ids})

    def _share_audience(self, action_id, account, params, dry_run):
        _required(params, "custom_audience_ids", "shared_advertiser_ids")
        ids = _coerce("custom_audience_ids", params["custom_audience_ids"], "ids")
        targets = _coerce("shared_advertiser_ids",
                          params["shared_advertiser_ids"], "ids")
        if len(targets) > 10:
            raise _bad("Share with at most 10 advertisers per call.",
                       param="shared_advertiser_ids")
        self._audiences(account, ids)
        body = {"advertiser_id": account, "custom_audience_ids": ids,
                "shared_advertiser_ids": targets}
        text = (f"{len(ids)} audience(s) with {len(targets)} advertiser(s) "
                f"({', '.join(targets)})")
        if dry_run:
            return _would(action_id, account, f"Would share {text}.",
                          request=body)
        self._post("dmp/custom_audience/share/", body)
        return ActionResult(action=action_id, account=account,
                            summary=f"Shared {text}.",
                            after={"custom_audience_ids": ids,
                                   "shared_advertiser_ids": targets})

    def _reply_to_comment(self, action_id, account, params, dry_run):
        _required(params, "comment_id", "ad_id", "tiktok_item_id", "text",
                  "identity_id", "identity_type")
        text = _coerce("text", params["text"], "text")
        if params["identity_type"] not in _COMMENT_IDENTITY_TYPES:
            raise _bad("'identity_type' must be CUSTOMIZED_USER or TT_USER.",
                       param="identity_type")
        # The API can only post replies, not top-level comments.
        comment_type = "REPLY"
        body = {
            "advertiser_id": account,
            "comment_id": _norm_id(params["comment_id"], "comment_id"),
            "ad_id": _norm_id(params["ad_id"], "ad_id"),
            "tiktok_item_id": _coerce("tiktok_item_id", params["tiktok_item_id"],
                                      "text"),
            "comment_type": comment_type, "text": text,
            "identity_id": _coerce("identity_id", params["identity_id"], "text"),
            "identity_type": _coerce("identity_type", params["identity_type"],
                                     "token"),
        }
        if dry_run:
            return _would(action_id, account,
                          f"Would reply to comment {body['comment_id']}: "
                          f"{text!r}.", request=body)
        data = self._post("comment/post/", body)
        reply_id = _first_id(data, "comment_id", "reply_id")
        return ActionResult(
            action=action_id, account=account,
            summary=f"Replied to comment {body['comment_id']}: {text!r}.",
            after={"reply_comment_id": reply_id, "text": text},
            details={"comment_id": reply_id})

    def _set_comment_visibility(self, action_id, account, params, dry_run):
        ids = _ids(params, "comment_id", "comment_ids")
        hide = action_id == "hide_comment"
        body = {"advertiser_id": account, "comment_ids": ids,
                "operation": "HIDDEN" if hide else "PUBLIC"}
        text = f"{'hide' if hide else 'unhide'} {len(ids)} comment(s)"
        if dry_run:
            return _would(action_id, account, f"Would {text}.", request=body)
        self._post("comment/status/update/", body)
        return ActionResult(
            action=action_id, account=account,
            summary=f"{'Hid' if hide else 'Unhid'} {len(ids)} comment(s).",
            after={"comment_ids": ids,
                   "comment_status": "HIDDEN" if hide else "PUBLIC"})

    def _delete_comment(self, action_id, account, params, dry_run):
        _required(params, "comment_id", "ad_id", "tiktok_item_id",
                  "identity_id", "identity_type")
        body = {"advertiser_id": account,
                "comment_id": _norm_id(params["comment_id"], "comment_id"),
                "ad_id": _norm_id(params["ad_id"], "ad_id"),
                "tiktok_item_id": _coerce("tiktok_item_id",
                                          params["tiktok_item_id"], "text"),
                "identity_id": _coerce("identity_id", params["identity_id"],
                                       "text"),
                "identity_type": _coerce("identity_type",
                                         params["identity_type"], "token")}
        if dry_run:
            return _would(action_id, account,
                          f"Would PERMANENTLY delete comment "
                          f"{body['comment_id']}.", request=body)
        self._post("comment/delete/", body)
        return ActionResult(action=action_id, account=account,
                            summary=f"Deleted comment {body['comment_id']} "
                                    f"(permanent).",
                            after={"deleted": body["comment_id"]})

    def _blocked_words(self, action_id, account, params, dry_run):
        words = _coerce("words", params.get("words"), "texts")
        add = action_id == "add_blocked_words"
        body = {"advertiser_id": account, "blocked_words": words}
        text = f"{len(words)} blocked word(s)"
        if dry_run:
            return _would(action_id, account,
                          f"Would {'add' if add else 'remove'} {text}.",
                          request=body)
        self._post("blockedword/create/" if add else "blockedword/delete/", body)
        return ActionResult(action=action_id, account=account,
                            summary=f"{'Added' if add else 'Removed'} {text}.",
                            after={"blocked_words": words,
                                   "blocked": add})

    def _create_rule(self, action_id, account, params, dry_run):
        rule = dict(_coerce("rule", params.get("rule"), "object"))
        _check_rule(rule)
        if "rule_id" in rule:
            raise _bad("A new rule has no rule_id; use update_automated_rule.")
        body = {"advertiser_id": account, "rules": [rule]}
        if dry_run:
            return _would(action_id, account,
                          f"Would create automated rule {rule['name']!r}.",
                          request=body)
        data = self._post("optimizer/rule/create/", body)
        rule_id = _first_id(data, "rule_ids", "rule_id")
        return ActionResult(
            action=action_id, account=account,
            summary=f"Created automated rule {rule['name']!r} (id {rule_id}).",
            after={"rule_id": rule_id, **rule}, details={"rule_id": rule_id})

    def _update_rule(self, action_id, account, params, dry_run):
        _required(params, "rule_id", "rule")
        rule_id = _norm_id(params["rule_id"], "rule_id")
        rule = dict(_coerce("rule", params["rule"], "object"))
        _check_rule(rule)
        rule["rule_id"] = rule_id
        body = {"advertiser_id": account, "rules": [rule]}
        if dry_run:
            return _would(action_id, account,
                          f"Would update automated rule {rule_id}.",
                          request=body)
        self._post("optimizer/rule/update/", body)
        return ActionResult(action=action_id, account=account,
                            summary=f"Updated automated rule {rule_id}.",
                            after=rule)

    def _set_rule_status(self, action_id, account, params, dry_run):
        ids = _ids(params, "rule_id", "rule_ids")
        op, verb = {"enable_automated_rule": ("TURN_ON", "turned on"),
                    "disable_automated_rule": ("TURN_OFF", "turned off"),
                    "delete_automated_rule": ("DELETE", "deleted")}[action_id]
        body = {"advertiser_id": account, "rule_ids": ids, "operate_type": op}
        if dry_run:
            return _would(action_id, account,
                          f"Would set {len(ids)} automated rule(s) to {op}.",
                          request=body)
        self._post("optimizer/rule/update/status/", body)
        return ActionResult(action=action_id, account=account,
                            summary=f"{len(ids)} automated rule(s) {verb}.",
                            after={"rule_ids": ids, "status": op})

    def _create_pixel(self, action_id, account, params, dry_run):
        name = _coerce("pixel_name", params.get("pixel_name"), "text")
        # pixel_category is deprecated, so it isn't sent.
        body: Dict[str, Any] = {"advertiser_id": account, "pixel_name": name}
        body.update(_extra(params))
        if dry_run:
            return _would(action_id, account, f"Would create pixel {name!r}.",
                          request=body)
        data = self._post("pixel/create/", body)
        pixel_id = _first_id(data, "pixel_id")
        code = data.get("pixel_code") if isinstance(data, dict) else None
        return ActionResult(
            action=action_id, account=account,
            summary=f"Created pixel {name!r} (id {pixel_id}"
                    f"{', code ' + code if code else ''}). Install its code on "
                    f"the website to start tracking.",
            after={"pixel_id": pixel_id, "pixel_code": code, "pixel_name": name},
            details={"pixel_id": pixel_id})

    def _update_pixel(self, action_id, account, params, dry_run):
        _required(params, "pixel_id", "pixel_name")
        pixel_id = _norm_id(params["pixel_id"], "pixel_id")
        changes: Dict[str, Any] = {
            "pixel_name": _coerce("pixel_name", params["pixel_name"], "text"),
            **_extra(params)}
        body = {"advertiser_id": account, "pixel_id": pixel_id, **changes}
        if dry_run:
            return _would(action_id, account,
                          f"Would update pixel {pixel_id}.", request=body)
        self._post("pixel/update/", body)
        return ActionResult(action=action_id, account=account,
                            summary=f"Updated pixel {pixel_id}.",
                            after={"pixel_id": pixel_id, **changes})
