#!/bin/bash
# Wrapper script for TernoDBI MCP servers (STANDALONE mode)
# Uses TernoDBI settings but TernoAI's database

# Source TernoAI env.sh to get USER_SQLITE_PATH (for shared database)
source /Users/navin/terno/terno-ai/env.sh

# Activate virtual environment
source /Users/navin/terno/env/terno_env/bin/activate

# Run the MCP server (uses dbi_server.settings by default)
python -m dbi_layer.mcp "$@"
