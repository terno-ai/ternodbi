"""
AutoGen Agent Demo for TernoDBI.

This script demonstrates how to use AutoGen with TernoDBI via MCP.
It creates a two-agent system: a Database Assistant and a User Proxy.
"""

import os
import sys
import logging
import asyncio
from contextlib import AsyncExitStack

# Add src to path for direct execution
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../")))

from mcp import ClientSession
from mcp.client.stdio import stdio_client
import autogen

from terno_dbi.examples.agents.mcp_config import get_default_server_params
from terno_dbi.examples.agents.autogen.mcp_adapter import load_mcp_tools

logging.basicConfig(
    level=os.environ.get('TERNODBI_LOG_LEVEL', 'INFO'),
    format='%(levelname)s %(asctime)s %(name)s %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Main execution function."""
    logger.info("Starting AutoGen Agent Demo for TernoDBI")
    print("=" * 60)
    print("TernoDBI AutoGen Agent Demo (MCP Integration)")
    print("=" * 60)

    # Check for OpenAI API key
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY environment variable not set")
        print("\nError: OPENAI_API_KEY environment variable is not set.")
        return

    # Get MCP server parameters
    server_params = get_default_server_params()

    print("\nConnecting to MCP servers...")

    async with AsyncExitStack() as stack:
        sessions = []
        all_tool_defs = []
        all_tool_funcs = {}

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
                tool_defs, tool_funcs = await load_mcp_tools(session)
                all_tool_defs.extend(tool_defs)
                all_tool_funcs.update(tool_funcs)

                logger.info(f"Loaded {len(tool_defs)} tools from {params.command}")

            except Exception as e:
                logger.error(f"Failed to connect to MCP server {params.command}: {e}")
                print(f"Warning: Could not connect to {params.command}")

        if not all_tool_defs:
            logger.error("No tools loaded from MCP servers")
            print("\nError: No tools were loaded. Cannot create agent.")
            return

        print(f"Successfully loaded {len(all_tool_defs)} tools from MCP servers.\n")

        # Configure AutoGen
        config_list = [{"model": "gpt-4", "api_key": api_key}]

        llm_config = {
            "config_list": config_list,
            "tools": all_tool_defs,
            "timeout": 120,
        }

        # Helper function to detect termination
        def is_complete(msg):
            content = msg.get("content") or ""
            if content.rstrip().endswith("TERMINATE"):
                return True
            tool_calls = msg.get("tool_calls")
            if content and not tool_calls and msg.get("role") == "assistant":
                return True
            return False

        # Define Agents
        assistant = autogen.AssistantAgent(
            name="Database_Assistant",
            system_message="""You are a helpful database assistant powered by TernoDBI.
You have access to tools that let you interact with databases.

When answering questions:
1. First check what datasources are available if you don't know
2. Use appropriate tools to gather information
3. Provide clear, concise answers

IMPORTANT: After you have fully answered the user's question, end your response with the word TERMINATE on its own line.
""",
            llm_config=llm_config,
        )

        user_proxy = autogen.UserProxyAgent(
            name="User_Proxy",
            human_input_mode="NEVER",  # Automated for demo
            max_consecutive_auto_reply=3,
            is_termination_msg=is_complete,
            code_execution_config=False,  # We are using tools, not code execution
            function_map=all_tool_funcs, 
        )

        # Run a demo query
        question = "List all my datasources and tell me which ones are PostgreSQL. Terminate the conversation when done."
        print(f"User Question: {question}\n")
        print("-" * 60)

        # Initiate Chat (Async)
        try:
            await user_proxy.a_initiate_chat(
                assistant,
                message=question
            )

            logger.info("Demo completed successfully")

        except Exception as e:
            logger.exception("Error during agent execution")
            print(f"\nError: {e}")


if __name__ == "__main__":
    asyncio.run(main())
