"""
MCP to OpenAI Tool Adapter.

This module bridges the Model Context Protocol (MCP) with OpenAI's tool calling capability.
It converts MCP tools discovered from a ClientSession into OpenAI tool definitions.
"""

import logging
import json
from typing import List, Any, Dict, Callable, Awaitable
from mcp import ClientSession

logger = logging.getLogger(__name__)

async def load_mcp_tools(session: ClientSession) -> tuple[List[Dict[str, Any]], Dict[str, Callable]]:
    """
    Load tools from an MCP ClientSession and convert them to OpenAI tool definitions.
    Args:
        session: An initialized MCP ClientSession connected to a server.
    Returns:
        A tuple containing:
        1. List of OpenAI tool definitions (JSON schemas)
        2. Dictionary mapping tool names to their async callable functions
    """
    openai_tools = []
    tool_functions = {}

    try:
        # Discover all available tools from the MCP server
        result = await session.list_tools()
        for mcp_tool in result.tools:
            # Extract tool metadata
            tool_name = mcp_tool.name
            tool_description = mcp_tool.description or f"Tool: {tool_name}"
            input_schema = mcp_tool.inputSchema

            # OpenAI Tool Definition
            openai_tool = {
                "type": "function",
                "function": {
                    "name": tool_name,
                    "description": tool_description,
                    "parameters": input_schema
                }
            }
            openai_tools.append(openai_tool)

            # Create a wrapper function that calls the MCP tool
            async def create_tool_func(session, tool_name):
                async def tool_func(**kwargs) -> str:
                    """Dynamically created tool function that calls MCP."""
                    try:
                        logger.debug(f"Calling MCP tool '{tool_name}' with args: {kwargs}")
                        result = await session.call_tool(tool_name, arguments=kwargs)

                        # Extract text content from the result
                        output_parts = []
                        if result.content:
                            for content in result.content:
                                if content.type == "text":
                                    output_parts.append(content.text)
                                else:
                                    output_parts.append(f"[{content.type} content]")

                        return "\n".join(output_parts) if output_parts else "Tool executed successfully."

                    except Exception as e:
                        logger.exception(f"Error executing MCP tool '{tool_name}'")
                        return f"Error: {str(e)}"

                return tool_func

            # Register the function
            tool_functions[tool_name] = await create_tool_func(session, tool_name)
            logger.info(f"Loaded MCP tool '{tool_name}' for OpenAI")

    except Exception as e:
        logger.exception("Failed to load MCP tools")
        raise RuntimeError(f"Failed to load MCP tools: {e}")

    logger.info(f"Successfully loaded {len(openai_tools)} tools from MCP server")
    return openai_tools, tool_functions
