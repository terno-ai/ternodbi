# LangChain Agent Example

This example demonstrates how to use **LangChain** with **TernoDBI** via the Model Context Protocol (MCP).

## Overview

The LangChain agent dynamically loads database tools from TernoDBI's MCP servers and uses them to answer questions about your databases.

## Files

- **`mcp_adapter.py`**: Bridges MCP tools to LangChain's `StructuredTool` format
- **`agent_demo.py`**: Runnable demo script showing a ReAct agent in action

## Requirements

```bash
pip install langchain langchain-openai langchain-core
```

## Environment Variables

Set the following environment variables:

```bash
export OPENAI_API_KEY="your-openai-api-key"
export TERNODBI_QUERY_KEY="your-query-key"  # Optional
export TERNODBI_ADMIN_KEY="your-admin-key"  # Optional
```

## Usage

Run the demo:

```bash
python src/terno_dbi/examples/agents/langchain/agent_demo.py
```

The agent will:
1. Connect to TernoDBI's MCP servers (query + admin)
2. Dynamically load all available database tools
3. Create a LangChain ReAct agent
4. Answer the question: "List all my datasources and tell me which ones are PostgreSQL."

## How It Works

1. **MCP Connection**: Uses `mcp_config.get_default_server_params()` to connect to TernoDBI servers
2. **Tool Loading**: `load_mcp_tools()` converts MCP tools to LangChain tools
3. **Agent Creation**: Uses `create_tool_calling_agent()` with ChatOpenAI
4. **Execution**: The agent uses tools as needed to answer questions

## Key Advantage

Unlike manually defining tools, this approach **automatically discovers** all database capabilities from TernoDBI. Add a new feature to your MCP server, and the agent can use it immediately—no code changes needed.
