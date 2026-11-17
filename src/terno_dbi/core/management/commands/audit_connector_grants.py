"""List connected API sources whose stored grant lacks a required permission.

Dry run by default. `--apply` marks each one as needing a reconnect, with a
message naming the permission to allow.
"""

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = ("Find connected API sources missing a required OAuth permission; "
            "--apply marks them as needing a reconnect.")

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true",
                            help="Mark the sources found as needing a reconnect.")
        parser.add_argument("--connector", default="",
                            help="Only check this connector key.")

    def handle(self, *args, **options):
        from terno_dbi.connectors.api.auth.grants import grant_problem
        from terno_dbi.connectors.api.auth.oauth import mark_expired
        from terno_dbi.core.models import DataSource
        from terno_dbi.services.secrets import decrypt_dict

        rows = DataSource.objects.filter(
            catalog__family="api", auth_status=DataSource.AuthStatus.CONNECTED,
        ).select_related("organisation")
        if options["connector"]:
            rows = rows.filter(type=options["connector"])

        found = 0
        for ds in rows.iterator():
            try:
                bundle = decrypt_dict(ds.connection_json) or {}
            except Exception:   # noqa: BLE001
                continue
            if not isinstance(bundle, dict):
                continue
            message = grant_problem(ds.type, bundle)
            if not message:
                continue
            found += 1
            org = getattr(ds.organisation, "name", "") or ""
            self.stdout.write(f"{ds.id}\t{ds.type}\t{org}\t{message}")
            if options["apply"]:
                mark_expired(ds, message)

        verb = "Marked" if options["apply"] else "Found"
        self.stdout.write(f"{verb} {found} source(s) missing a required permission.")
        if found and not options["apply"]:
            self.stdout.write("Run again with --apply to mark them as needing a reconnect.")
