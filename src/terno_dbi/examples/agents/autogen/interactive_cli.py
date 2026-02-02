"""
Interactive CLI for TernoDBI AutoGen Agent.

This script provides an interactive command-line interface to chat with 
AutoGen agents that use TernoDBI tools via MCP.
"""

import os
import sys
import logging
import asyncio
import getpass
from contextlib import AsyncExitStack

# Add src to path for direct execution
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../")))

from mcp import ClientSession
from mcp.client.stdio import stdio_client
import autogen

from terno_dbi.examples.agents.mcp_config import get_default_server_params
from terno_dbi.examples.agents.autogen.mcp_adapter import load_mcp_tools

# Configure logging
logging.basicConfig(
    level=os.environ.get('TERNODBI_LOG_LEVEL', 'ERROR'),
    format='%(levelname)s %(asctime)s %(name)s %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Main execution function for the interactive CLI."""
    logger.info("Starting TernoDBI AutoGen Interactive CLI")
    print("=" * 60)
    print("TernoDBI AutoGen Agent CLI (MCP Integration)")
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

        print(f"Successfully loaded {len(all_tool_defs)} tools from MCP servers.")

        config_list = [{"model": "gpt-4o", "api_key": api_key}]

        llm_config = {
            "config_list": config_list,
            "tools": all_tool_defs,
            "timeout": 120,
        }

        # Helper function to detect termination - handles None content and checks for completion
        def is_complete(msg):
            content = msg.get("content") or ""
            # Check for explicit TERMINATE
            if content.rstrip().endswith("TERMINATE"):
                return True
            # Check if this is a final answer without tool calls (assistant completed the task)
            # When the assistant gives a substantive answer without requesting tools, we're done
            tool_calls = msg.get("tool_calls")
            if content and not tool_calls and msg.get("role") == "assistant":
                # This is a final text response from assistant
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
Do NOT continue the conversation after providing your answer.
""",
            llm_config=llm_config,
        )

        user_proxy = autogen.UserProxyAgent(
            name="User_Proxy",
            human_input_mode="NEVER",  # We drive the loop manually
            max_consecutive_auto_reply=3,  # Reduced to prevent runaway loops
            is_termination_msg=is_complete,
            code_execution_config=False,
            function_map=all_tool_funcs, 
        )

        print("\nConnected! You can now ask questions about your databases.")
        print("Type 'exit' or 'quit' to stop.")

        # Interactive loop
        is_first_turn = True

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
                print("Agents are working...\n")

                try:
                    # For AutoGen, we initiate chat once, then continue?
                    # Or just initiate every time with history? 
                    # Simpler to initiate fresh or use a_initiate_chat with clear_history=False.
                    await user_proxy.a_initiate_chat(
                        assistant,
                        message=user_input,
                        clear_history=False  # Keep context
                    )
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
