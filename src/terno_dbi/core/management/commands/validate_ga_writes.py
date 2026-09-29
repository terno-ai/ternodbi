"""Dry-run every Google Ads write action against the live API — zero side effects.

Each action is sent with Google's `validateOnly` flag, so the request is fully
validated server-side (wrong field names, bad update masks, policy issues surface
as real errors) but *nothing is applied*: no spend, no ads, no changes. This is
the safe way to prove the connector's request shapes are correct before any
write is done for real.

It calls the connector directly (bypassing the per-account write gate and MCP),
so it is an operator tool — run it against a connected Google Ads datasource.

Usage:
    dbi validate-ga-writes --datasource <id-or-name> [--account <customer-id>]
        [--campaign <id>] [--ad-group <id>] [--ad <id>]
        [--criterion <id>] [--user-list <id>]

Actions that read an entity first (pause/enable/budget/status/remove/update)
need a REAL existing id of that kind; pass the matching flag or they are marked
SKIPPED (needs id). Create actions need no ids and always run.
"""

from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Dry-run (validateOnly) all Google Ads write actions — no side effects."

    def add_arguments(self, parser):
        parser.add_argument("--datasource", required=True,
                            help="Google Ads datasource id or name.")
        parser.add_argument("--account", default=None,
                            help="Customer id to target (default: first accessible).")
        parser.add_argument("--campaign", default=None)
        parser.add_argument("--ad-group", dest="ad_group", default=None)
        parser.add_argument("--ad", default=None)
        parser.add_argument("--criterion", default=None)
        parser.add_argument("--user-list", dest="user_list", default=None)

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

        connector = registry.build_connector(ds)
        account = opts["account"]
        if not account:
            accounts = connector.list_accounts()
            if not accounts:
                raise CommandError("No accessible accounts on this connection.")
            account = accounts[0].id
        self.stdout.write(f"Dry-running against account {account}\n")

        camp = opts["campaign"]
        ag = opts["ad_group"]
        ad = opts["ad"]
        crit = opts["criterion"]
        ul = opts["user_list"]

        # (action_id, params, needs) — `needs` lists ids required to exercise it.
        plan = [
            # campaign lifecycle
            ("create_campaign", {"name": "TERNO dry-run", "daily_budget": 10}, []),
            ("pause_campaign", {"campaign_id": camp}, ["campaign"]),
            ("enable_campaign", {"campaign_id": camp}, ["campaign"]),
            ("set_campaign_budget", {"campaign_id": camp, "amount": 12}, ["campaign"]),
            ("remove_campaign", {"campaign_id": camp}, ["campaign"]),
            # bidding
            ("set_target_cpa", {"campaign_id": camp, "target_cpa": 20}, ["campaign"]),
            ("set_target_roas", {"campaign_id": camp, "target_roas": 4}, ["campaign"]),
            ("set_maximize_conversions", {"campaign_id": camp}, ["campaign"]),
            ("set_maximize_conversion_value", {"campaign_id": camp}, ["campaign"]),
            ("set_manual_cpc", {"campaign_id": camp}, ["campaign"]),
            ("set_target_impression_share",
             {"campaign_id": camp, "location": "TOP_OF_PAGE", "target_percentage": 65},
             ["campaign"]),
            ("create_portfolio_bid_strategy",
             {"name": "TERNO dry-run pf", "type": "TARGET_CPA", "target": 20}, []),
            ("attach_campaign_to_portfolio",
             {"campaign_id": camp, "bidding_strategy_id": "1"}, ["campaign"]),
            # ad group lifecycle
            ("create_ad_group", {"campaign_id": camp, "name": "TERNO ag"}, ["campaign"]),
            ("pause_ad_group", {"ad_group_id": ag}, ["ad_group"]),
            ("enable_ad_group", {"ad_group_id": ag}, ["ad_group"]),
            ("set_max_cpc", {"ad_group_id": ag, "max_cpc": 1.5}, ["ad_group"]),
            ("remove_ad_group", {"ad_group_id": ag}, ["ad_group"]),
            # ads
            ("create_responsive_search_ad",
             {"ad_group_id": ag, "final_url": "https://example.com",
              "headlines": ["H1", "H2", "H3"], "descriptions": ["D1", "D2"]},
             ["ad_group"]),
            ("pause_ad", {"ad_group_id": ag, "ad_id": ad}, ["ad_group", "ad"]),
            ("enable_ad", {"ad_group_id": ag, "ad_id": ad}, ["ad_group", "ad"]),
            ("remove_ad", {"ad_group_id": ag, "ad_id": ad}, ["ad_group", "ad"]),
            # keywords
            ("add_keywords", {"ad_group_id": ag, "keywords": ["terno test"]}, ["ad_group"]),
            ("add_negative_keywords", {"campaign_id": camp, "keywords": ["free"]},
             ["campaign"]),
            ("update_keyword",
             {"ad_group_id": ag, "criterion_id": crit, "status": "PAUSED"},
             ["ad_group", "criterion"]),
            ("remove_keyword", {"ad_group_id": ag, "criterion_id": crit},
             ["ad_group", "criterion"]),
            # extensions
            ("add_sitelink",
             {"campaign_id": camp, "link_text": "Shop", "final_url": "https://e.com"},
             ["campaign"]),
            ("add_callout", {"campaign_id": camp, "text": "Free shipping"}, ["campaign"]),
            ("add_structured_snippet",
             {"campaign_id": camp, "header": "Brands", "values": ["A", "B"]},
             ["campaign"]),
            # customer match
            ("create_customer_list", {"name": "TERNO dry-run list"}, []),
            ("add_customer_list_members",
             {"user_list_id": ul, "emails": ["a@b.com"]}, ["user_list"]),
            ("attach_audience", {"ad_group_id": ag, "user_list_id": ul},
             ["ad_group", "user_list"]),
            ("remove_audience", {"ad_group_id": ag, "criterion_id": crit},
             ["ad_group", "criterion"]),
        ]
        have = {"campaign": camp, "ad_group": ag, "ad": ad,
                "criterion": crit, "user_list": ul}

        n_pass = n_fail = n_skip = 0
        for action_id, params, needs in plan:
            missing = [k for k in needs if not have.get(k)]
            if missing:
                n_skip += 1
                self.stdout.write(
                    f"  SKIP  {action_id:32s} needs --{'/--'.join(missing)}")
                continue
            try:
                res = connector.execute_action(action_id, account, params,
                                               dry_run=True)
                n_pass += 1
                note = "(partial)" if (res.details or {}).get("partial") else ""
                self.stdout.write(self.style.SUCCESS(
                    f"  PASS  {action_id:32s} {note}"))
            except ApiError as exc:
                n_fail += 1
                self.stdout.write(self.style.ERROR(
                    f"  FAIL  {action_id:32s} {exc.code}: {exc.message}"))

        self.stdout.write(
            f"\n{n_pass} passed, {n_fail} failed, {n_skip} skipped "
            f"(of {len(plan)} actions). Nothing was applied.")
        if n_fail:
            raise CommandError(f"{n_fail} action(s) failed validation.")
