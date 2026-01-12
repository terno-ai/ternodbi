# ternodbi
Database Interaction Semantic Layer

git clone git@github.com:terno-ai/ternodbi.git
git checkout feature/DBILayer
uv sync
cd server
python manage.py migrate
python manage.py changepassword
python manage.py runserver

Download and open Claude Desktop https://code.claude.com/docs/en/desktop

Account -> Setting -> Edit Config -> Edit claude_desktop_config.json

See [Config ](assets/config.png)
```
{
    "mcpServers": {
        "ternodbi-query": {
            "command": "/Users/navin/terno/ternodbi/run_mcp.sh",
            "args": [
                "query"
            ]
        },
        "ternodbi-admin": {
            "command": "/Users/navin/terno/ternodbi/run_mcp.sh",
            "args": [
                "admin"
            ]
        }
    }
}

```
Fix the command in this and also fix run_mcp.sh. 

Restart the Claude Desktop and check.

