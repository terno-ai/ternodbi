"""
MCP to LangChain Tool Adapter.

This module bridges the Model Context Protocol (MCP) with LangChain's tool system.
It dynamically converts MCP tools discovered from a ClientSession into LangChain StructuredTools.
"""

import logging
import json
from typing import List, Any, Dict
from mcp import ClientSession
from langchain_core.tools import StructuredTool

logger = logging.getLogger(__name__)


async def load_mcp_tools(session: ClientSession) -> List[StructuredTool]:
    """
    Load tools from an MCP ClientSession and convert them to LangChain StructuredTools.
    
    Args:
        session: An initialized MCP ClientSession connected to a server.
        
    Returns:
        A list of LangChain StructuredTool objects ready to be used by an agent.
    """
    tools = []
    
    try:
        # Discover all available tools from the MCP server
        result = await session.list_tools()
        
        for mcp_tool in result.tools:
            # Extract tool metadata
            tool_name = mcp_tool.name
            tool_description = mcp_tool.description or f"Tool: {tool_name}"
            input_schema = mcp_tool.inputSchema
            
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
            
            # Create the async function for this specific tool
            tool_func = await create_tool_func(session, tool_name)
            
            # Convert MCP JSON schema to LangChain StructuredTool
            # LangChain expects the args_schema to be a Pydantic model or dict
            langchain_tool = StructuredTool.from_function(
                coroutine=tool_func,
                name=tool_name,
                description=tool_description,
                args_schema=input_schema.get("properties", {}),
            )
            
            tools.append(langchain_tool)
            logger.info(f"Loaded MCP tool '{tool_name}' as LangChain tool")
    
    except Exception as e:
        logger.exception("Failed to load MCP tools")
        raise RuntimeError(f"Failed to load MCP tools: {e}")
    
    logger.info(f"Successfully loaded {len(tools)} tools from MCP server")
    return tools
