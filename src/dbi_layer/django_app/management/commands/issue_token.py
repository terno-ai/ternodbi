
from django.core.management.base import BaseCommand
from dbi_layer.services.auth import generate_service_token
from dbi_layer.django_app.models import ServiceToken


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

    def handle(self, *args, **options):
        name = options['name']
        token_type = options['type']
        type_enum = ServiceToken.TokenType.ADMIN if token_type == 'admin' else ServiceToken.TokenType.QUERY

        try:
            token, full_key = generate_service_token(name, type_enum) 
            self.stdout.write(self.style.SUCCESS(f"Successfully issued token for '{name}'"))
            self.stdout.write("---------------------------------------------------------------")
            self.stdout.write(f"TOKEN TYPE: {token_type.upper()}")
            self.stdout.write(f"KEY       : {full_key}")
            self.stdout.write("---------------------------------------------------------------")
            self.stdout.write(self.style.WARNING("SAVE THIS KEY NOW. It will never be shown again."))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error creating token: {e}"))
