import logging
import os

from django.core.wsgi import get_wsgi_application

logger = logging.getLogger(__name__)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'terno_dbi.server.settings')

logger.info("Initializing WSGI application")
application = get_wsgi_application()
logger.debug("WSGI application ready")
