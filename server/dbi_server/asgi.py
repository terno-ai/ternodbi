import logging
import os

from django.core.asgi import get_asgi_application

logger = logging.getLogger(__name__)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbi_server.settings')

logger.info("Initializing ASGI application")
application = get_asgi_application()
logger.debug("ASGI application ready")
