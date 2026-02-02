"""
MCP to LangChain Tool Adapter.

This module bridges the Model Context Protocol (MCP) with LangChain's tool system.
It dynamically converts MCP tools discovered from a ClientSession into LangChain StructuredTools.
"""

import logging
import json
from typing import List, Any, Dict, Type
from pydantic import create_model, Field, BaseModel
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
            
            # Convert JSON schema to Pydantic model
            fields = {}
            properties = input_schema.get("properties", {})
            required_fields = input_schema.get("required", [])
            
            for prop_name, prop_def in properties.items():
                # Map JSON types to Python types
                json_type = prop_def.get("type", "string")
                python_type = str
                if json_type == "integer":
                    python_type = int
                elif json_type == "boolean":
                    python_type = bool
                elif json_type == "number":
                    python_type = float
                elif json_type == "array":
                    python_type = list
                elif json_type == "object":
                    python_type = dict
                
                # Create Field definition
                description = prop_def.get("description", "")
                
                if prop_name in required_fields:
                    fields[prop_name] = (python_type, Field(description=description))
                else:
                    fields[prop_name] = (python_type | None, Field(default=None, description=description))
            
            # Create the Pydantic model
            pydantic_model = create_model(f"{tool_name}Schema", **fields)
            
            langchain_tool = StructuredTool.from_function(
                coroutine=tool_func,
                name=tool_name,
                description=tool_description,
                args_schema=pydantic_model,
            )
            
            tools.append(langchain_tool)
            logger.info(f"Loaded MCP tool '{tool_name}' as LangChain tool")
    
    except Exception as e:
        logger.exception("Failed to load MCP tools")
        raise RuntimeError(f"Failed to load MCP tools: {e}")
    
    logger.info(f"Successfully loaded {len(tools)} tools from MCP server")
    return tools
