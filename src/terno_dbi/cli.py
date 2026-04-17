import os
import sys
import json
import django
from django.contrib.auth import get_user_model
from django.core.management import execute_from_command_line


def print_welcome_message(port):
    server_url = f"http://127.0.0.1:{port}"
    admin_url = f"{server_url}/admin"

    print("\n" + "="*60)
    print("TernoDBI Server Started Successfully!")
    print("="*60)
    print(f"\nAPI Server:  {server_url}")
    print(f"Admin Panel: {admin_url}")

    print("-" * 60)
    print("Next Steps")
    print("-" * 60)
    print("1. Open the Admin Panel and login.(default Username: admin, Password: admin)")
    print("2. Add your Datasource connection in the Admin UI.")
    print("3. To connect AI agents (Claude Desktop, Cursor) via MCP,")
    print("   open a new terminal tab and run:")
    print("   > ternodbi mcp-config")
    print("-" * 60 + "\n")


def create_default_superuser():
    django.setup()
    User = get_user_model()

    if not User.objects.filter(is_superuser=True).exists():
        print("\nFirst Boot Detected: Creating default admin user...")
        User.objects.create_superuser('admin', 'admin@example.com', 'admin')
        print("Default Login created! Username: admin, Password: admin")
        print("WARNING: Please change this in production!\n")


def main():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'terno_dbi.server.settings')

    if len(sys.argv) < 2:
        print("Usage: ternodbi <command>")
        print("\nAvailable commands:")
        print("  start      Start the TernoDBI server (auto-runs migrations)")
        print("  mcp-config Print the MCP configuration snippet for Claude Desktop")
        print("  manage     Run standard Django management commands (e.g., manage issue_token)")
        sys.exit(1)

    command = sys.argv[1]

    if command == "start":

        # Run Migrations automatically
        print("Initializing TernoDBI Database (this may take a moment)...")
        execute_from_command_line(['manage.py', 'migrate', '--verbosity', '0'])
        print("Database ready.")

        # Check and create default superuser
        create_default_superuser()

        # Start the server on port 8376
        port = "8376"
        print_welcome_message(port)

        sys.argv = ['manage.py', 'runserver', '--noreload', f"127.0.0.1:{port}"]
        execute_from_command_line(sys.argv)

    elif command == "mcp-config":
        port = "8376"
        server_url = f"http://127.0.0.1:{port}"
        print("\nMCP Configuration Snippet (for claude_desktop_config.json):")
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
        print(json.dumps(mcp_config, indent=2))
        print("\n")

    elif command == "manage":
        django_args = ['manage.py'] + sys.argv[2:]
        execute_from_command_line(django_args)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
