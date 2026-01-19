import logging
from django.http import JsonResponse
from django.shortcuts import render
from terno_dbi.connectors import ConnectorFactory
from terno_dbi.core import conf

logger = logging.getLogger(__name__)


def landing_page(request):
    return render(request, 'terno_dbi/landing.html')


def health(request):
    return JsonResponse({
        "status": "ok",
        "service": "terno_dbi",
        "version": "1.0.0",
    })


def info(request):
    supported_dbs = ConnectorFactory.get_supported_databases()

    return JsonResponse({
        "service": "terno_dbi",
        "version": "1.0.0",
        "supported_databases": supported_dbs,
        "config": {
            "default_page_size": conf.get("DEFAULT_PAGE_SIZE"),
            "max_page_size": conf.get("MAX_PAGE_SIZE"),
            "cache_timeout": conf.get("CACHE_TIMEOUT"),
        }
    })
