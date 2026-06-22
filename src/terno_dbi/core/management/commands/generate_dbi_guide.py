from django.core.management.base import BaseCommand

from terno_dbi.services.dbi_guide_service import (
    generate_dbi_guide
)


class Command(BaseCommand):

    help = "Generate DBI Guide"

    def add_arguments(self, parser):
        parser.add_argument(
            "datasource_id",
            type=int
        )

    def handle(self, *args, **options):

        datasource_id = options["datasource_id"]

        guide = generate_dbi_guide(
            datasource_id
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"Generated guide {guide.id}"
            )
        )