import logging
from django.http import JsonResponse
from django.shortcuts import render
from terno_dbi.connectors import ConnectorFactory
from terno_dbi.core import conf
from django.shortcuts import redirect


logger = logging.getLogger(__name__)


def landing_page(request):
    logger.debug("Landing page visited")
    return render(request, 'terno_dbi/landing.html')


def health(request):
    logger.debug("Health check requested")
    return JsonResponse({
        "status": "ok",
        "service": "terno_dbi",
        "version": "1.0.0",
    })


def info(request):
    logger.debug("Info endpoint requested")
    supported_dbs = ConnectorFactory.get_supported_databases()

    return JsonResponse({
        "service": "terno_dbi",
        "version": "1.0.0",
        "supported_databases": supported_dbs,
        "config": {
            "cache_timeout": conf.get("CACHE_TIMEOUT"),
        }
    })


def doc_view(request, page="setup"):
    valid_pages = ["architecture", "setup", "mcp-guide", "security", "api"]
    if page not in valid_pages:
        page = "setup"

    external_url = f"https://terno-ai.github.io/ternodbi/{page}.html"
    logger.info("Redirecting internal documentation request for '%s' to %s", page, external_url)
    return redirect(external_url)
