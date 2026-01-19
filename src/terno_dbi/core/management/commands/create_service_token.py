"""
Management command to create service tokens.

Usage:
    python manage.py create_service_token --type admin --name "My Admin Token"
    python manage.py create_service_token --type query --name "API Query Token" --expires 30
"""

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from datetime import timedelta

from terno_dbi.core.models import ServiceToken


class Command(BaseCommand):
    help = 'Create a new service token for API authentication'

    def add_arguments(self, parser):
        parser.add_argument(
            '--type',
            type=str,
            required=True,
            choices=['admin', 'query'],
            help='Token type: admin (full access) or query (read-only)'
        )
        parser.add_argument(
            '--name',
            type=str,
            required=True,
            help='Friendly name for the token'
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

    def handle(self, *args, **options):
        token_type = options['type']
        name = options['name']
        expires_days = options['expires']
        datasource_ids = options['datasource']

        raw_key = ServiceToken.generate_key()
        key_hash = ServiceToken.hash_key(raw_key)
        key_prefix = raw_key[:12]

        expires_at = None
        if expires_days:
            expires_at = timezone.now() + timedelta(days=expires_days)

        token = ServiceToken.objects.create(
            key_hash=key_hash,
            key_prefix=key_prefix,
            name=name,
            token_type=token_type,
            expires_at=expires_at,
        )

        if datasource_ids:
            from terno_dbi.core.models import DataSource
            datasources = DataSource.objects.filter(id__in=datasource_ids)
            token.datasources.set(datasources)
            scope_msg = f"Scoped to {datasources.count()} datasource(s)"
        else:
            scope_msg = "Global access (all datasources)"

        self.stdout.write(self.style.SUCCESS('\n' + '=' * 60))
        self.stdout.write(self.style.SUCCESS('Service Token Created'))
        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(f'\nToken:   {raw_key}')
        self.stdout.write(f'Type:    {token_type}')
        self.stdout.write(f'Name:    {name}')
        self.stdout.write(f'Scope:   {scope_msg}')
        if expires_at:
            self.stdout.write(f'Expires: {expires_at.isoformat()}')
        else:
            self.stdout.write('Expires: Never')

        self.stdout.write(self.style.WARNING(
            '\n[IMPORTANT] Save this token now! It cannot be retrieved later.\n'
        ))

        self.stdout.write(self.style.SUCCESS(
            f'\nUsage:\n  curl -H "Authorization: Bearer {raw_key}" ...\n'
        ))
