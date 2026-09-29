"""Create a Google Ads test *client* account under a (test) manager, via the API.

Test client accounts must live under a manager account, and the Google Ads UI
for test managers (which show as "closed") is often unusable — so this creates
the client with `CustomerService.CreateCustomerClient`, which just works.

Any client created under a test manager is itself a test account: it never
serves ads and has no billing. The new client's customer id is what you pass to
`smoke_ga_writes --account`.

Usage:
    dbi create-ga-test-client --datasource google_ads --manager 2064533644 \
        [--name "Terno test client"] [--currency INR] [--timezone Asia/Kolkata]
"""

from __future__ import annotations

import os

from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Create a test client account under a Google Ads manager via the API."

    def add_arguments(self, parser):
        parser.add_argument("--datasource", required=True,
                            help="Google Ads datasource id or name (for credentials).")
        parser.add_argument("--manager", required=True,
                            help="Manager (MCC) customer id to create the client under.")
        parser.add_argument("--name", default="Terno test client",
                            help="Descriptive name for the new client account.")
        parser.add_argument("--currency", default="INR",
                            help="Currency code (default INR).")
        parser.add_argument("--timezone", default="Asia/Kolkata",
                            help="Time zone (default Asia/Kolkata).")

    def handle(self, *args, **opts):
        from terno_dbi.connectors.api import registry
        from terno_dbi.connectors.api.model.errors import ApiError
        from terno_dbi.connectors.api.sources import google_ads as ga
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

        manager = str(opts["manager"]).replace("-", "")
        # Creating a client under a manager requires login-customer-id = manager.
        os.environ[ga._LOGIN_CUSTOMER_ID_ENV] = manager

        conn = registry.build_connector(ds)
        url = f"{ga._BASE}/customers/{manager}:createCustomerClient"
        body = {
            "customerClient": {
                "descriptiveName": opts["name"],
                "currencyCode": opts["currency"],
                "timeZone": opts["timezone"],
            }
        }
        try:
            resp = conn._call("POST", url, body)
        except ApiError as exc:
            raise CommandError(f"Create failed ({exc.code}): {exc.message}")

        # Response carries the new client's resource name, e.g.
        # "customers/1234567890" (and/or a customerClient resource).
        res = resp.get("resourceName") or ""
        new_id = None
        for key in ("resourceName", "customerClient"):
            val = resp.get(key)
            if isinstance(val, str) and "customers/" in val:
                new_id = val.split("customers/")[-1].split("/")[0]
                break
        if not new_id and res:
            new_id = res.split("/")[-1]

        self.stdout.write(self.style.SUCCESS(
            f"Created test client account: {new_id}"))
        self.stdout.write(f"Full response: {resp}")
        self.stdout.write(
            "\nNext:\n"
            f"  export {ga._LOGIN_CUSTOMER_ID_ENV}={manager}\n"
            f"  python manage.py smoke_ga_writes --datasource {ident} "
            f"--account {new_id}")
