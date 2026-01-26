import os
import sys
import logging
import asyncio
import getpass

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

from terno_dbi.agents.agent import ChainOfThoughtAgent
from terno_dbi.agents.llm_interface import OpenAIProvider
from terno_dbi.agents.mcp_config import get_default_server_params

logging.basicConfig(level=logging.ERROR)


async def main():
    print("==================================================")
    print("TernoDBI Agent CLI (Async MCP)")
    print("==================================================")

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        api_key = getpass.getpass("Enter OpenAI API Key: ").strip()
        if not api_key:
            print("OpenAI API Key is required.")
            return
        os.environ["OPENAI_API_KEY"] = api_key

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

    try:
        llm = OpenAIProvider(api_key=api_key)
        server_params = get_default_server_params()
        agent = ChainOfThoughtAgent(server_params=server_params, llm=llm, verbose=True)

        print("Connecting to MCP Servers (Query + Admin)...")
        async with agent:
            print("Connected! You can now ask questions about your databases.")
            print("   Type 'exit' or 'quit' to stop.\n")

            while True:
                try:
                    user_input = input("\n👤 You: ").strip()
                    if user_input.lower() in ("exit", "quit"):
                        print("Goodbye! 👋")
                        break

                    if not user_input:
                        continue

                    print("Agent is thinking...")
                    response = await agent.run(user_input)
                    print(f"Answer: {response}")

                except KeyboardInterrupt:
                    print("\nGoodbye! 👋")
                    break

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
