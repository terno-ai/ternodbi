import logging
from django.http import JsonResponse
from django.shortcuts import render
from terno_dbi.connectors import ConnectorFactory
from terno_dbi.core import conf

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
    import markdown
    import os
    from django.conf import settings
    from django.http import Http404

    # Sanitize page
    valid_pages = ["architecture", "setup", "mcp-guide", "security", "api"]
    if page not in valid_pages:
        page = "setup"

    # Resolve docs path
    # Base dir is server/, docs are in root so ../docs
    docs_dir = settings.BASE_DIR.parent / "docs"
    file_path = docs_dir / f"{page}.md"

    if not file_path.exists():
        logger.warning("Documentation page not found: %s", page)
        raise Http404("Documentation not found")

    logger.info("Serving documentation page: %s", page)

    with open(file_path, "r") as f:
        md_content = f.read()

    html_content = markdown.markdown(
        md_content, extensions=["fenced_code", "tables", "toc"]
    )

    return render(request, "terno_dbi/docs.html", {
        "content": html_content,
        "current_page": page
    })
