import logging
from datetime import timedelta
from django.utils import timezone
from django.core.management.base import BaseCommand, CommandError
from django.contrib.auth import get_user_model
from terno_dbi.services.auth import generate_service_token
from terno_dbi.core.models import ServiceToken, CoreOrganisation

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Issue a new Service Token (API Key) for TernoDBI.'

    def add_arguments(self, parser):
        parser.add_argument('--name', type=str, required=True, help='Friendly name for the token')
        parser.add_argument(
            '--type',
            type=str,
            choices=['admin', 'query'],
            default='query',
            help='Token type/scope (default: query)'
        )
        parser.add_argument(
            '--expires',
            type=int,
            default=None,
            help='Token expiry in days (default: never)'
        )
        parser.add_argument(
            '--datasource',
            type=int,
            action='append',
            default=[],
            help='Limit token to specific datasource IDs (can specify multiple)'
        )
        parser.add_argument(
            '--org',
            type=str,
            default=None,
            help='Organisation subdomain to bind this token to. Required for the '
                 'token to be usable with org-scoped features (e.g. memory) — '
                 'without it, the token has no organisation identity.'
        )
        parser.add_argument(
            '--user',
            type=str,
            default=None,
            help='Username this token acts as (created_for) — the memory author '
                 'for user-store writes, and what visibility is scoped to. '
                 'Without it the token has no user identity.'
        )

    def handle(self, *args, **options):
        name = options['name']
        token_type = options['type']
        expires_days = options['expires']
        datasource_ids = options['datasource']

        type_enum = ServiceToken.TokenType.ADMIN if token_type == 'admin' else ServiceToken.TokenType.QUERY

        expires_at = None
        if expires_days:
            expires_at = timezone.now() + timedelta(days=expires_days)

        organisation = None
        if options['org']:
            try:
                organisation = CoreOrganisation.objects.get(subdomain=options['org'])
            except CoreOrganisation.DoesNotExist:
                raise CommandError(f"No organisation with subdomain '{options['org']}'")

        created_for = None
        if options['user']:
            User = get_user_model()
            try:
                created_for = User.objects.get(username=options['user'])
            except User.DoesNotExist:
                raise CommandError(f"No user '{options['user']}'")

        if not organisation:
            self.stdout.write(self.style.WARNING(
                "WARNING: no --org given. This token will have no organisation "
                "identity and cannot use org-scoped features (e.g. memory)."
            ))

        try:
            token, full_key = generate_service_token(
                name=name,
                token_type=type_enum,
                expires_at=expires_at,
                datasource_ids=datasource_ids,
                organisation=organisation,
                created_for=created_for,
            )

            logger.info(
                "Token issued via CLI: name='%s', type='%s', expires=%s, datasources=%s",
                name, token_type, expires_at or 'never', datasource_ids or 'global'
            )

            self.stdout.write(self.style.SUCCESS(f"Successfully issued token for '{name}'"))
            self.stdout.write("---------------------------------------------------------------")
            self.stdout.write(f"TOKEN TYPE: {token_type.upper()}")
            self.stdout.write(f"KEY       : {full_key}")
            self.stdout.write(f"ORG       : {organisation.subdomain if organisation else 'NONE (org-scoped features unusable)'}")
            self.stdout.write(f"USER      : {created_for.username if created_for else 'NONE (user-store memory unusable)'}")

            if expires_at:
                self.stdout.write(f"EXPIRES   : {expires_at.isoformat()}")
            else:
                self.stdout.write("EXPIRES   : Never")

            if datasource_ids:
                self.stdout.write(f"SCOPE     : Limited to datasources {datasource_ids}")
            else:
                self.stdout.write("SCOPE     : Global")

            self.stdout.write("---------------------------------------------------------------")
            self.stdout.write(self.style.WARNING("SAVE THIS KEY NOW. It will never be shown again."))
        except Exception as e:
            logger.error("Failed to issue token via CLI: name='%s', error=%s", name, str(e))
            self.stdout.write(self.style.ERROR(f"Error creating token: {e}"))
