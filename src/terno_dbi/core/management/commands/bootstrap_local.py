"""Create everything needed to exercise the connector locally, in one command.

    python -m django bootstrap_local --settings terno_dbi.server.settings

Idempotent: re-running reuses the existing user and organisation and issues a
fresh token, so it is safe to run whenever you need a new one.
"""

from django.contrib.auth.models import Group, User
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Create a local user, organisation, group and service token for testing."

    def add_arguments(self, parser):
        parser.add_argument("--username", default="local")
        parser.add_argument("--email", default="local@example.com")
        parser.add_argument("--password", default="local")
        parser.add_argument(
            "--read-only",
            action="store_true",
            help="Issue a read-only token (no Org Admin group, query scopes only).",
        )

    def handle(self, *args, **options):
        from terno_dbi.core.models import CoreOrganisation, OrganisationUser
        from terno_dbi.oauth.minting import (
            generate_oauth_access_token,
            mint_service_token_for_key,
        )
        from terno_dbi.oauth.provisioning import default_org_name, generate_subdomain

        username = options["username"]
        user, created = User.objects.get_or_create(
            username=username, defaults={"email": options["email"]}
        )
        if created:
            user.set_password(options["password"])
            user.is_staff = True
            user.is_superuser = True
            user.save()

        membership = OrganisationUser.objects.filter(user=user).first()
        if membership is None:
            org = CoreOrganisation.objects.create(
                name=default_org_name(user),
                subdomain=generate_subdomain(username),
                owner=user,
                is_active=True,
            )
            membership = OrganisationUser.objects.create(organisation=org, user=user)
        else:
            org = membership.organisation

        # Write access needs the group as well as the scope — the token minter
        # strips write scopes for a non-admin, which is the behaviour being
        # demonstrated by --read-only.
        if not options["read_only"]:
            admin_group, _ = Group.objects.get_or_create(name="Org Admin")
            membership.groups.add(admin_group)

        scopes = ["query:read", "query:execute"]
        if not options["read_only"]:
            scopes += ["admin:read", "admin:write", "admin:sync"]

        key = generate_oauth_access_token()
        token = mint_service_token_for_key(
            key, user, org, scopes, client_name="Local testing"
        )

        w = self.style.SUCCESS
        self.stdout.write(w("\nLocal connector ready\n"))
        self.stdout.write(f"  user          {user.username} / {options['password']}")
        self.stdout.write(f"  organisation  {org.name}  (subdomain: {org.subdomain})")
        self.stdout.write(f"  scopes        {' '.join(token.scopes)}")
        self.stdout.write(f"  groups        {', '.join(g.name for g in membership.groups.all()) or '(none)'}")
        self.stdout.write(w(f"\n  TOKEN  {key}\n"))
        self.stdout.write("Connect Claude Code with:\n")
        self.stdout.write(
            f'  claude mcp add --transport http terno-local '
            f'http://127.0.0.1:8376/mcp --header "Authorization: Bearer {key}"\n'
        )
