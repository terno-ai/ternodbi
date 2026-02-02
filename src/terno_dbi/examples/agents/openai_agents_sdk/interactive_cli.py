"""
Interactive CLI for TernoDBI OpenAI SDK Agent.

This script provides an interactive command-line interface to chat with the 
OpenAI SDK-based agent that uses TernoDBI tools via MCP.
"""

import os
import sys
import logging
import asyncio
import getpass
import json

# Add src to path for direct execution
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../")))

from contextlib import AsyncExitStack
from mcp import ClientSession
from mcp.client.stdio import stdio_client

from openai import AsyncOpenAI

from terno_dbi.examples.agents.mcp_config import get_default_server_params
from terno_dbi.examples.agents.openai_agents_sdk.mcp_adapter import load_mcp_tools

# Configure logging
logging.basicConfig(
    level=os.environ.get('TERNODBI_LOG_LEVEL', 'ERROR'),
    format='%(levelname)s %(asctime)s %(name)s %(message)s'
)
logger = logging.getLogger(__name__)


async def run_agent_loop(client, model, messages, tools, tool_functions):
    """
    Executes the agent loop: checks for tool calls, executes them, and continues until final answer.
    """
    print("Agent is thinking...")

    while True:
        try:
            # 1. Call the model
            response = await client.chat.completions.create(
                model=model,
                messages=messages,
                tools=tools,
                tool_choice="auto",
                temperature=0
            )

            response_message = response.choices[0].message
            messages.append(response_message)

            tool_calls = response_message.tool_calls

            # 2. If no tool calls, we are done
            if not tool_calls:
                return response_message.content

            # 3. Handle tool calls
            for tool_call in tool_calls:
                function_name = tool_call.function.name
                function_args = json.loads(tool_call.function.arguments)

                print(f"  > Executing: {function_name}...")

                function_to_call = tool_functions.get(function_name)
                if function_to_call:
                    function_response = await function_to_call(**function_args)
                else:
                    function_response = f"Error: Tool {function_name} not found"  
                messages.append(
                    {
                        "tool_call_id": tool_call.id,
                        "role": "tool",
                        "name": function_name,
                        "content": function_response,
                    }
                )
        except Exception as e:
            logger.exception("Error in agent loop")
            return f"Error occurred: {str(e)}"


async def main():
    """Main execution function for the interactive CLI."""
    logger.info("Starting TernoDBI OpenAI SDK Interactive CLI")
    print("=" * 60)
    print("TernoDBI OpenAI SDK Agent CLI (MCP Integration)")
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

    # Initialize OpenAI Client
    client = AsyncOpenAI(api_key=api_key)

    server_params = get_default_server_params()

    print("\nConnecting to MCP servers...")

    async with AsyncExitStack() as stack:
        sessions = []
        all_openai_tools = []
        all_tool_functions = {}

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
                tools, tool_funcs = await load_mcp_tools(session)
                all_openai_tools.extend(tools)
                all_tool_functions.update(tool_funcs)

                logger.info(f"Loaded {len(tools)} tools from {params.command}")

            except Exception as e:
                logger.error(f"Failed to connect to MCP server {params.command}: {e}")
                print(f"Warning: Could not connect to {params.command}")

        if not all_openai_tools:
            logger.error("No tools loaded from MCP servers")
            print("\nError: No tools were loaded. Cannot create agent.")
            return

        print(f"Successfully loaded {len(all_openai_tools)} tools from MCP servers.")

        # Define System Prompt
        system_prompt = """You are a helpful database assistant powered by TernoDBI.
You have access to tools that let you interact with databases.
When answering questions:
1. First check what datasources are available if you don't know
2. Use appropriate tools to gather information
3. Provide clear, concise answers
"""

        messages = [{"role": "system", "content": system_prompt}]

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

                messages.append({"role": "user", "content": user_input})

                response = await run_agent_loop(
                    client, 
                    "gpt-4o", 
                    messages, 
                    all_openai_tools, 
                    all_tool_functions
                )

                print("-" * 60)
                print(f"Answer: {response}")
                print("-" * 60)

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
