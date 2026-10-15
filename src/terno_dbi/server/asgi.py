"""ASGI entrypoint for the standalone TernoDBI server.

Mirrors what terno-ai's `mysite/asgi.py` does — Django everywhere, raw ASGI at
`/mcp` — so the whole connector can be exercised locally without MySQL, redis,
or the rest of the terno-ai stack. See `docs/LOCAL-TESTING.md`.

The path check here is hand-rolled rather than using `channels.routing.URLRouter`
as terno-ai does. channels is a terno-ai dependency, not a ternodbi one, and
pulling it in so a standalone dev server can route one path would be a poor
trade. The behaviour is the same: exact match on `/mcp` and `/mcp/preview`
(see `terno_dbi.mcp.profiles`), everything else to Django.
"""

import logging
import os

from django.core.asgi import get_asgi_application

logger = logging.getLogger(__name__)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'terno_dbi.server.settings')

django_application = get_asgi_application()

# Imported after get_asgi_application() so the app registry is populated before
# anything touches the models.
from terno_dbi.mcp.http_app import build_asgi_app  # noqa: E402
from terno_dbi.mcp.merged_server import server as mcp_server  # noqa: E402
from terno_dbi.mcp.profiles import PREVIEW, STABLE  # noqa: E402

_port = os.environ.get("TERNO_LOCAL_PORT", "8376")
_base_url = os.environ.get("TERNO_MCP_BASE_URL", f"http://127.0.0.1:{_port}")

# One app per endpoint, keyed by path. in_process dispatches tool calls straight
# to the Django views rather than looping back over HTTP to this same process.
mcp_applications = {
    profile.path: build_asgi_app(
        mcp_server, base_url=_base_url, in_process=True, profile=profile,
    )
    for profile in (STABLE, PREVIEW)
}


async def application(scope, receive, send):
    if scope["type"] == "http":
        mcp_application = mcp_applications.get(scope.get("path", "").rstrip("/"))
        if mcp_application is not None:
            await mcp_application(scope, receive, send)
            return
    await django_application(scope, receive, send)


logger.info("ASGI application ready (Django + MCP at /mcp and /mcp/preview)")
