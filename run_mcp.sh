#!/bin/bash
# Wrapper script for TernoDBI MCP servers (Client Mode)

# Default to Local Development Server if not set
export TERNODBI_API_URL="${TERNODBI_API_URL:-http://127.0.0.1:8000}"

# Activate virtual environment
source /Users/sandeepgiri/projects/ternodbi/.venv/bin/activate
# source /Users/navin/terno/env/terno_env/bin/activate

# Run the MCP server
# Usage: ./run_mcp.sh admin OR ./run_mcp.sh query
python -m dbi_layer.mcp $1
