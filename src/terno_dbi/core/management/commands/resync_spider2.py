#!/usr/bin/env python3
"""
Resync Failed Spider2 Datasources

A Django management command to resync datasources with 0 tables.
Run from the server directory: python manage.py resync_spider2

Usage:
    cd /Users/navin/terno/ternodbi/server
    python manage.py resync_spider2
    
    # Or limit to specific datasources
    python manage.py resync_spider2 --limit 5
"""

from django.core.management.base import BaseCommand
from terno_dbi.core import models
from terno_dbi.services.schema_utils import sync_metadata
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Resync Spider2 datasources that have 0 tables'

    def add_arguments(self, parser):
        parser.add_argument(
            '--limit',
            type=int,
            help='Limit number of datasources to resync'
        )
        parser.add_argument(
            '--all',
            action='store_true',
            help='Resync all Spider2 datasources (not just empty ones)'
        )

    def handle(self, *args, **options):
        # Find Spider2 datasources
        datasources = models.DataSource.objects.filter(
            display_name__startswith="Spider2-",
            enabled=True
        )
        
        if not options.get('all'):
            # Filter to only empty ones
            empty_datasources = []
            for ds in datasources:
                table_count = models.Table.objects.filter(data_source=ds).count()
                if table_count == 0:
                    empty_datasources.append(ds)
            datasources = empty_datasources
        
        self.stdout.write(f"Found {len(datasources)} datasources to resync")
        
        if options.get('limit'):
            datasources = datasources[:options['limit']]
        
        fixed = 0
        failed = 0
        
        for ds in datasources:
            db_id = ds.display_name.replace("Spider2-", "")
            self.stdout.write(f"\n[{ds.id}] {db_id}...", ending=" ")
            
            try:
                result = sync_metadata(ds.id, overwrite=True)
                
                if "error" in result:
                    self.stdout.write(self.style.ERROR(f"❌ {result['error'][:50]}"))
                    failed += 1
                else:
                    tables = result.get('tables_created', 0) + result.get('tables_updated', 0)
                    if tables > 0:
                        self.stdout.write(self.style.SUCCESS(f"✅ {tables} tables"))
                        fixed += 1
                    else:
                        self.stdout.write(self.style.WARNING(f"⚠️ 0 tables"))
                        failed += 1
                        
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ {str(e)[:50]}"))
                failed += 1
        
        self.stdout.write(f"\n{'='*50}")
        self.stdout.write(f"Fixed: {fixed}, Failed: {failed}")
