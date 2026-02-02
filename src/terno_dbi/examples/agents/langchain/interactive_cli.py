"""
Interactive CLI for TernoDBI LangChain Agent.

This script provides an interactive command-line interface to chat with the 
LangChain-based agent that uses TernoDBI tools via MCP.
"""

import os
import sys
import logging
import asyncio
import getpass
from contextlib import AsyncExitStack
import importlib
from langchain_core.caches import BaseCache
from langchain_core.callbacks import Callbacks

# Add src to path for direct execution
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../")))

from mcp import ClientSession
from mcp.client.stdio import stdio_client

from langchain_openai import ChatOpenAI
from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate

from terno_dbi.examples.agents.mcp_config import get_default_server_params
from terno_dbi.examples.agents.langchain.mcp_adapter import load_mcp_tools

# Configure logging
logging.basicConfig(
    level=os.environ.get('TERNODBI_LOG_LEVEL', 'ERROR'),
    format='%(levelname)s %(asctime)s %(name)s %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Main execution function for the interactive CLI."""
    # PATCH: Fix Pydantic error for ChatOpenAI due to missing forward references
    try:
        mod = importlib.import_module(ChatOpenAI.__module__)
        if not hasattr(mod, "BaseCache"):
            logger.debug("Patching BaseCache into langchain_openai module")
            mod.BaseCache = BaseCache
        if not hasattr(mod, "Callbacks"):
            logger.debug("Patching Callbacks into langchain_openai module")
            mod.Callbacks = Callbacks

        ChatOpenAI.model_rebuild()
        logger.debug("Successfully rebuilt ChatOpenAI model")
    except Exception as e:
        logger.warning(f"Failed to patch ChatOpenAI: {e}")

    logger.info("Starting TernoDBI LangChain Interactive CLI")
    print("=" * 60)
    print("TernoDBI LangChain Agent CLI (MCP Integration)")
    print("=" * 60)

    # Check for OpenAI API key
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        api_key = getpass.getpass("Enter OpenAI API Key: ").strip()
        if not api_key:
            logger.error("OpenAI API Key not provided")
            print("Error: OpenAI API Key is required.")
            return
        os.environ["OPENAI_API_KEY"] = api_key

    # Check for TernoDBI keys
    if not os.environ.get("TERNODBI_QUERY_KEY"):
        print("TERNODBI_QUERY_KEY not found in env.")
        query_key = getpass.getpass("Enter Query Agent Key (or press Enter for none): ").strip()
        if query_key:
            os.environ["TERNODBI_QUERY_KEY"] = query_key

    if not os.environ.get("TERNODBI_ADMIN_KEY"):
        print("TERNODBI_ADMIN_KEY not found in env.")
        admin_key = getpass.getpass("Enter Admin Agent Key (or press Enter for none): ").strip()
        if admin_key:
            os.environ["TERNODBI_ADMIN_KEY"] = admin_key
    # Initialize the language model
    logger.info("Initializing ChatOpenAI model")
    llm = ChatOpenAI(
        model="gpt-4o",
        temperature=0,
        api_key=api_key
    )
    server_params = get_default_server_params()

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

        print(f"Successfully loaded {len(all_tools)} tools from MCP servers.")

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

        print("\nConnected! You can now ask questions about your databases.")
        print("Type 'exit' or 'quit' to stop.")

        # Interactive loop
        while True:
            try:
                user_input = input("\nLet's Cook: ").strip()
                if user_input.lower() in ("exit", "quit"):
                    logger.info("User requested exit")
                    print("Goodbye! 👋")
                    break

                if not user_input:
                    continue

                logger.debug("Processing user query: %s", user_input[:50])
                print("Agent is thinking...")

                try:
                    response = await agent_executor.ainvoke({"input": user_input})
                    print("-" * 60)
                    print(f"Answer: {response.get('output', 'No output returned')}")
                    print("-" * 60)
                except Exception as e:
                    logger.exception("Error executing agent")
                    print(f"Error: {e}")

            except KeyboardInterrupt:
                logger.info("CLI interrupted by user")
                print("\nGoodbye! 👋")
                break
            except Exception as e:
                logger.exception("Unexpected error in main loop")
                print(f"Error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
