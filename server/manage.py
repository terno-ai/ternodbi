import logging
import os
import sys

logger = logging.getLogger(__name__)


def main():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dbi_server.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        logger.error("Django import failed: %s", exc)
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable?"
        ) from exc
    
    logger.debug("Executing management command: %s", ' '.join(sys.argv))
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
