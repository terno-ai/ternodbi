
from datetime import timedelta
from django.utils import timezone
from django.core.management.base import BaseCommand
from terno_dbi.services.auth import generate_service_token
from terno_dbi.core.models import ServiceToken


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

    def handle(self, *args, **options):
        name = options['name']
        token_type = options['type']
        expires_days = options['expires']
        datasource_ids = options['datasource']
        
        type_enum = ServiceToken.TokenType.ADMIN if token_type == 'admin' else ServiceToken.TokenType.QUERY

        expires_at = None
        if expires_days:
            expires_at = timezone.now() + timedelta(days=expires_days)

        try:
            token, full_key = generate_service_token(
                name=name, 
                token_type=type_enum, 
                expires_at=expires_at,
                datasource_ids=datasource_ids
            )
            
            self.stdout.write(self.style.SUCCESS(f"Successfully issued token for '{name}'"))
            self.stdout.write("---------------------------------------------------------------")
            self.stdout.write(f"TOKEN TYPE: {token_type.upper()}")
            self.stdout.write(f"KEY       : {full_key}")
            
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
            self.stdout.write(self.style.ERROR(f"Error creating token: {e}"))
