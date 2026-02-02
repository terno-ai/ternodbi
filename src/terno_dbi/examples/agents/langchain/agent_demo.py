"""
LangChain Agent Demo for TernoDBI.

This script demonstrates how to use LangChain with TernoDBI via MCP (Model Context Protocol).
It creates a ReAct agent that can interact with your databases through dynamically loaded tools.
"""

import os
import sys
import logging
import asyncio

# Add src to path for direct execution
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../")))

from contextlib import AsyncExitStack
from mcp import ClientSession
from mcp.client.stdio import stdio_client

from langchain_openai import ChatOpenAI
from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate

from terno_dbi.examples.agents.mcp_config import get_default_server_params
from terno_dbi.examples.agents.langchain.mcp_adapter import load_mcp_tools

logging.basicConfig(
    level=os.environ.get('TERNODBI_LOG_LEVEL', 'INFO'),
    format='%(levelname)s %(asctime)s %(name)s %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Main execution function."""
    logger.info("Starting LangChain Agent Demo for TernoDBI")
    print("=" * 60)
    print("TernoDBI LangChain Agent Demo (MCP Integration)")
    print("=" * 60)
    
    # Check for OpenAI API key
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY environment variable not set")
        print("\nError: OPENAI_API_KEY environment variable is not set.")
        print("Please set it to run this demo.")
        return
    
    # Warn about missing TernoDBI keys
    if not os.environ.get("TERNODBI_QUERY_KEY") and not os.environ.get("TERNODBI_ADMIN_KEY"):
        logger.warning("No TernoDBI API keys configured")
        print("\nWarning: TERNODBI_QUERY_KEY and TERNODBI_ADMIN_KEY are not set.")
        print("         The agent may fail with 401 errors if authentication is required.\n")
    
    # Initialize the language model
    logger.info("Initializing ChatOpenAI model")
    llm = ChatOpenAI(
        model="gpt-4o",
        temperature=0,
        api_key=api_key
    )
    
    # Get MCP server parameters
    server_params = get_default_server_params()
    
    # Connect to MCP servers and load tools
    logger.info("Connecting to MCP servers")
    print("\nConnecting to MCP servers...")
    
    async with AsyncExitStack() as stack:
        sessions = []
        all_tools = []
        
        # Connect to each MCP server
        for params in server_params:
            try:
                stdio_transport = await stack.enter_async_context(stdio_client(params))
                session = await stack.enter_async_context(
                    ClientSession(stdio_transport[0], stdio_transport[1])
                )
                await session.initialize()
                sessions.append(session)
                
                # Load tools from this session
                tools = await load_mcp_tools(session)
                all_tools.extend(tools)
                logger.info(f"Loaded {len(tools)} tools from {params.command}")
                
            except Exception as e:
                logger.error(f"Failed to connect to MCP server {params.command}: {e}")
                print(f"Warning: Could not connect to {params.command}")
        
        if not all_tools:
            logger.error("No tools loaded from MCP servers")
            print("\nError: No tools were loaded. Cannot create agent.")
            return
        
        print(f"Successfully loaded {len(all_tools)} tools from MCP servers.\n")
        
        # Create the agent prompt
        prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a helpful database assistant powered by TernoDBI.
You have access to tools that let you interact with databases.

When answering questions:
1. First check what datasources are available if you don't know
2. Use appropriate tools to gather information
3. Provide clear, concise answers

Available tools will be automatically provided to you."""),
            ("human", "{input}"),
            ("placeholder", "{agent_scratchpad}"),
        ])
        
        # Create the agent
        logger.info("Creating LangChain agent")
        agent = create_tool_calling_agent(llm, all_tools, prompt)
        agent_executor = AgentExecutor(
            agent=agent,
            tools=all_tools,
            verbose=True,
            handle_parsing_errors=True
        )
        
        # Run a demo query
        question = "List all my datasources and tell me which ones are PostgreSQL."
        logger.info("Executing demo query")
        print(f"User Question: {question}\n")
        print("-" * 60)
        
        try:
            response = await agent_executor.ainvoke({"input": question})
            
            print("-" * 60)
            print("\n=== Final Answer ===")
            print(response.get("output", "No output returned"))
            logger.info("Demo completed successfully")
            
        except Exception as e:
            logger.exception("Error during agent execution")
            print(f"\nError: {e}")


if __name__ == "__main__":
    asyncio.run(main())
