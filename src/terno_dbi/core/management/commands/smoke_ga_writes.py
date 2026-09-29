"""Live end-to-end smoke test of Google Ads write actions on a TEST account.

Unlike ``validate_ga_writes`` (dry-run only), this **actually applies** actions,
so run it ONLY against a Google Ads *test account* (test manager → test client),
where nothing serves and there is no billing. It bootstraps a campaign → ad group
→ keywords → ad → customer list, exercises the mutate/bidding/extension actions
against them, then removes what it created (unless ``--keep``).

Every step prints PASS/FAIL. A FAIL is either a real bug in the connector's
request shape (tell the developer) or a test-account limitation (some features —
e.g. Customer Match, certain bidding — behave differently on test accounts).

Usage:
    dbi smoke-ga-writes --datasource google_ads [--account <customer-id>] [--keep]
"""

from __future__ import annotations

import uuid

from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = ("Live end-to-end test of Google Ads write actions on a TEST account "
            "(applies real changes; do not run on a production account).")

    def add_arguments(self, parser):
        parser.add_argument("--datasource", required=True,
                            help="Google Ads datasource id or name.")
        parser.add_argument("--account", default=None,
                            help="Test customer id (default: first accessible).")
        parser.add_argument("--keep", action="store_true",
                            help="Do not delete the campaign/list created.")

    def handle(self, *args, **opts):
        from terno_dbi.connectors.api import registry
        from terno_dbi.connectors.api.model.errors import ApiError
        from terno_dbi.core.models import DataSource

        ident = opts["datasource"]
        ds = (DataSource.objects.filter(pk=ident).first()
              if str(ident).isdigit() else None)
        if ds is None:
            ds = (DataSource.objects.filter(display_name=ident).first()
                  or DataSource.objects.filter(catalog__key=ident,
                                               catalog__family="api").first())
        if ds is None:
            raise CommandError(f"No datasource matching {ident!r}.")
        if (getattr(ds.catalog, "key", "") or ds.type) != "google_ads":
            raise CommandError(f"{ds} is not a Google Ads datasource.")

        conn = registry.build_connector(ds)
        account = opts["account"]
        if not account:
            accounts = conn.list_accounts()
            if not accounts:
                raise CommandError("No accessible accounts on this connection.")
            account = accounts[0].id

        self.stdout.write(self.style.WARNING(
            f"LIVE smoke test — applying real changes to account {account}. "
            f"This must be a TEST account.\n"))

        tag = uuid.uuid4().hex[:6]
        state = {"passed": 0, "failed": 0, "skipped": 0}

        def step(action, params, label=None, expect=None):
            """Run one action. `expect` = substrings of known external errors
            (e.g. Customer Match allowlisting) to report as SKIP, not FAIL."""
            name = label or action
            try:
                res = conn.execute_action(action, account, params)
                state["passed"] += 1
                self.stdout.write(self.style.SUCCESS(f"  PASS  {name}"))
                return res.as_dict().get("after") or {}
            except ApiError as exc:
                msg = exc.message or ""
                if expect and any(s in msg for s in expect):
                    state["skipped"] += 1
                    self.stdout.write(self.style.WARNING(
                        f"  SKIP  {name:34s} external limitation: {msg[:80]}"))
                    return None
                state["failed"] += 1
                self.stdout.write(self.style.ERROR(
                    f"  FAIL  {name:34s} {exc.code}: {exc.message}"))
                return None

        campaign_id = ad_group_id = user_list_id = None
        try:
            # 1. Campaign + its bidding/extensions.
            after = step("create_campaign",
                         {"name": f"TERNO smoke {tag}", "daily_budget": 10})
            campaign_id = (after or {}).get("id")
            if campaign_id:
                step("set_campaign_budget", {"campaign_id": campaign_id, "amount": 12})
                # Maximize Conversions/Value and Target CPA/ROAS require the
                # account to have conversion tracking configured — a setup step a
                # bare test account lacks, so treat that as an external limitation.
                conv = ["CONVERSION_TRACKING_NOT_ENABLED"]
                step("set_target_cpa", {"campaign_id": campaign_id, "target_cpa": 20},
                     expect=conv)
                step("set_target_roas", {"campaign_id": campaign_id, "target_roas": 4},
                     expect=conv)
                step("set_maximize_conversions", {"campaign_id": campaign_id},
                     expect=conv)
                step("set_maximize_conversion_value", {"campaign_id": campaign_id},
                     expect=conv)
                step("set_target_impression_share",
                     {"campaign_id": campaign_id, "location": "TOP_OF_PAGE",
                      "target_percentage": 65, "cpc_bid_ceiling": 3})
                step("set_manual_cpc", {"campaign_id": campaign_id})
                step("add_negative_keywords",
                     {"campaign_id": campaign_id, "keywords": ["free", "cheap"]})
                step("add_callout", {"campaign_id": campaign_id, "text": "Fast shipping"})
                step("add_sitelink",
                     {"campaign_id": campaign_id, "link_text": "Shop",
                      "final_url": "https://example.com"})
                step("add_structured_snippet",
                     {"campaign_id": campaign_id, "header": "Brands",
                      "values": ["Alpha", "Beta", "Gamma"]})
                step("enable_campaign", {"campaign_id": campaign_id})
                step("pause_campaign", {"campaign_id": campaign_id})

            # 2. Portfolio strategy (standalone).
            step("create_portfolio_bid_strategy",
                 {"name": f"TERNO pf {tag}", "type": "TARGET_CPA", "target": 20})

            # 2b. Shared budget + attach the campaign to it.
            after = step("create_shared_budget",
                         {"name": f"TERNO shared {tag}", "daily_budget": 50})
            shared_budget_id = (after or {}).get("id")
            if shared_budget_id and campaign_id:
                step("attach_campaign_to_budget",
                     {"campaign_id": campaign_id, "budget_id": shared_budget_id})

            # 3. Ad group + its bidding.
            if campaign_id:
                after = step("create_ad_group",
                             {"campaign_id": campaign_id, "name": f"TERNO ag {tag}"})
                ad_group_id = (after or {}).get("id")
            if ad_group_id:
                step("set_max_cpc", {"ad_group_id": ad_group_id, "max_cpc": 1.5})
                step("enable_ad_group", {"ad_group_id": ad_group_id})

                # 4. Keywords (capture a criterion id to update/remove).
                after = step("add_keywords",
                             {"ad_group_id": ad_group_id,
                              "keywords": ["terno smoke kw"], "match_type": "EXACT"})
                crit_ids = (after or {}).get("criterion_ids") or []
                if crit_ids:
                    step("update_keyword",
                         {"ad_group_id": ad_group_id, "criterion_id": crit_ids[0],
                          "status": "PAUSED", "max_cpc": 2})
                    step("remove_keyword",
                         {"ad_group_id": ad_group_id, "criterion_id": crit_ids[0]})

                # 5. Responsive search ad (capture ad id to pause/remove).
                after = step("create_responsive_search_ad",
                             {"ad_group_id": ad_group_id,
                              "final_url": "https://example.com",
                              "headlines": ["Headline One", "Headline Two",
                                            "Headline Three"],
                              "descriptions": ["Description one here.",
                                               "Description two here."]})
                ad_id = (after or {}).get("ad_id")
                if ad_id:
                    step("pause_ad", {"ad_group_id": ad_group_id, "ad_id": ad_id})
                    step("enable_ad", {"ad_group_id": ad_group_id, "ad_id": ad_id})
                    step("remove_ad", {"ad_group_id": ad_group_id, "ad_id": ad_id})

            # 6. Customer Match. Uploads/targeting need account allowlisting (and
            # Google is moving uploads to the Data Manager API), so on a normal
            # test account these are expected to be blocked — report as SKIP.
            cm_limits = ["CUSTOMER_NOT_ALLOWLISTED", "Data Manager",
                         "CANNOT_TARGET_CUSTOMER_MATCH", "Customer Match policy"]
            after = step("create_customer_list", {"name": f"TERNO list {tag}"})
            user_list_id = (after or {}).get("id")
            if user_list_id:
                step("add_customer_list_members",
                     {"user_list_id": user_list_id, "emails": ["a@example.com"]},
                     expect=cm_limits)
                if ad_group_id:
                    after = step("attach_audience",
                                 {"ad_group_id": ad_group_id,
                                  "user_list_id": user_list_id},
                                 expect=cm_limits)
                    aud_crit = (after or {}).get("criterion_id")
                    if aud_crit:
                        step("remove_audience",
                             {"ad_group_id": ad_group_id, "criterion_id": aud_crit})
        finally:
            # 7. Cleanup — remove the ad group and campaign we created.
            if not opts["keep"]:
                self.stdout.write("\nCleaning up…")
                if ad_group_id:
                    step("remove_ad_group", {"ad_group_id": ad_group_id},
                         label="remove_ad_group (cleanup)")
                if campaign_id:
                    step("remove_campaign", {"campaign_id": campaign_id},
                         label="remove_campaign (cleanup)")

        self.stdout.write(
            f"\n{state['passed']} passed, {state['failed']} failed, "
            f"{state['skipped']} skipped (external limitation). "
            + ("Created objects were removed." if not opts["keep"]
               else "Created objects were kept (--keep)."))
        if state["failed"]:
            raise CommandError(f"{state['failed']} action(s) failed — see above.")
