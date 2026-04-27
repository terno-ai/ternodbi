import os
import sys
import json
import django
import logging
from django.contrib.auth import get_user_model
from django.core.management import execute_from_command_line

logger = logging.getLogger(__name__)

# ANSI Colors for CLI styling
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


def print_welcome_message(port):
    server_url = f"http://127.0.0.1:{port}"
    admin_url = f"{server_url}/admin"

    banner = f"""
{BOLD}TernoDBI server is live and ready.{RESET}

{BOLD}ACCESS POINTS{RESET}
{DIM}────────────────────────────────────────────────────────────────{RESET}
  {BOLD}Local API{RESET}    {CYAN}{server_url}{RESET}
  {BOLD}Admin UI{RESET}     {CYAN}{admin_url}{RESET}

{BOLD}GETTING STARTED{RESET}
{DIM}────────────────────────────────────────────────────────────────{RESET}
  {BOLD}1. Authenticate{RESET}
     Sign in to the Admin UI with {CYAN}admin / admin{RESET}.

  {BOLD}2. Connect Data{RESET}
     Register your databases through the Administration panel.

  {BOLD}3. AI Integration{RESET}
     Configure AI agents (Claude, Cursor) via MCP:
     {CYAN}> ternodbi mcp-config{RESET}

{DIM}────────────────────────────────────────────────────────────────{RESET}
{DIM}Process running. Press Ctrl+C to terminate.{RESET}
"""
    logger.info(banner)


def create_default_superuser():
    django.setup()
    User = get_user_model()

    if not User.objects.filter(is_superuser=True).exists():
        logger.info("\nFirst Boot Detected: Creating default admin user...")
        User.objects.create_superuser('admin', 'admin@example.com', 'admin')
        logger.info("Default Login created! Username: admin, Password: admin")
        logger.warning("WARNING: Please change this in production!\n")


def main():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'terno_dbi.server.settings')

    if len(sys.argv) < 2:
        logger.info("Usage: ternodbi <command>")
        logger.info("\nAvailable commands:")
        logger.info("  start [port]  Start the TernoDBI server (default port: 8376)")
        logger.info("  mcp-config    Print the MCP configuration snippet for Claude Desktop")
        logger.info("  manage        Run standard Django management commands")
        sys.exit(1)

    command = sys.argv[1]

    if command == "start":
        port = "8376"
        if len(sys.argv) > 2:
            try:
                provided_port = int(sys.argv[2])
                if not (1 <= provided_port <= 65535):
                    raise ValueError
                port = str(provided_port)
            except ValueError:
                logger.error(f"Error: '{sys.argv[2]}' is not a valid port number (1-65535).")
                sys.exit(1)

        # Run Migrations automatically
        logger.info(f"{DIM}Initializing database schema...{RESET}")
        execute_from_command_line(['manage.py', 'migrate', '--verbosity', '0'])

        # Check and create default superuser
        logger.info(f"{DIM}Verifying account security...{RESET}")
        create_default_superuser()

        # Start the server
        logger.info(f"{DIM}Finalizing server boot...{RESET}")
        print_welcome_message(port)

        sys.argv = ['manage.py', 'runserver', '--noreload', f"127.0.0.1:{port}"]
        execute_from_command_line(sys.argv)

    elif command == "mcp-config":
        port = "8376"
        server_url = f"http://127.0.0.1:{port}"
        logger.info("\nMCP Configuration Snippet (for claude_desktop_config.json):")
        mcp_config = {
            "mcpServers": {
                "ternodbi-query": {
                    "command": "uvx",
                    "args": ["--from", "terno-dbi", "dbi-mcp", "query"],
                    "env": {
                        "TERNODBI_API_URL": server_url,
                        "TERNODBI_API_KEY": "dbi_query_YOUR_TOKEN_HERE" 
                    }
                },
                "ternodbi-admin": {
                    "command": "uvx",
                    "args": ["--from", "terno-dbi", "dbi-mcp", "admin"],
                    "env": {
                        "TERNODBI_API_URL": server_url,
                        "TERNODBI_API_KEY": "dbi_admin_YOUR_TOKEN_HERE" 
                    }
                }
            }
        }
        logger.info(json.dumps(mcp_config, indent=2))
        logger.info("\n")

    elif command == "manage":
        django_args = ['manage.py'] + sys.argv[2:]
        execute_from_command_line(django_args)

    else:
        logger.error(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
