import os
import sys
import logging
import asyncio

# Add src to path to allow direct running from examples folder if package not installed
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

from terno_dbi.agents.agent import ChainOfThoughtAgent
from terno_dbi.agents.llm_interface import OpenAIProvider
from terno_dbi.agents.mcp_config import get_default_server_params

logging.basicConfig(level=logging.INFO)


async def main():
    print("--- TernoDBI Chain of Thought Agent Demo (MCP Client) ---")

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("Error: OPENAI_API_KEY environment variable is not set.")
        print("Please set it to run this demo.")
        return

    if not os.environ.get("TERNODBI_QUERY_KEY") and not os.environ.get("TERNODBI_ADMIN_KEY"):
        print("Warning: TERNODBI_QUERY_KEY and TERNODBI_ADMIN_KEY are not set.")
        print("         The agent may fail with 401 errors if authentication is required.")

    llm = OpenAIProvider(api_key=api_key)

    # Get Default MCP Server Config (Query + Admin)
    server_params = get_default_server_params()

    agent = ChainOfThoughtAgent(server_params=server_params, llm=llm, verbose=True)

    async with agent:
        question = "List all my datasources and tell me which ones are PostgreSQL."
        print(f"\nUser Question: {question}")

        final_answer = await agent.run(question)

        print("\n--- Final Answer ---")
        print(final_answer)


if __name__ == "__main__":
    asyncio.run(main())
