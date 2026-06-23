from django.core.management.base import BaseCommand

from terno_dbi.services.db_guide_service import (
    generate_db_guide
)


class Command(BaseCommand):

    help = "Generate DB Guide"

    def add_arguments(self, parser):
        parser.add_argument(
            "datasource_id",
            type=int
        )

    def handle(self, *args, **options):

        datasource_id = options["datasource_id"]

        guide = generate_db_guide(
            datasource_id
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"Generated guide {guide.id}"
            )
        )