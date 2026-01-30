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

# Configure logging - default to ERROR for CLI to keep output clean
logging.basicConfig(
    level=os.environ.get('TERNODBI_LOG_LEVEL', 'ERROR'),
    format='%(levelname)s %(asctime)s %(name)s %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    logger.info("Starting TernoDBI Interactive CLI")
    print("==================================================")
    print("TernoDBI Agent CLI (Async MCP)")
    print("==================================================")

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        api_key = getpass.getpass("Enter OpenAI API Key: ").strip()
        if not api_key:
            logger.error("OpenAI API Key not provided")
            print("OpenAI API Key is required.")
            return
        os.environ["OPENAI_API_KEY"] = api_key

    if not os.environ.get("TERNODBI_QUERY_KEY"):
        logger.warning("TERNODBI_QUERY_KEY not found in environment")
        print("TERNODBI_QUERY_KEY not found in env.")
        query_key = getpass.getpass("Enter Query Agent Key (or press Enter for none): ").strip()
        if query_key:
            os.environ["TERNODBI_QUERY_KEY"] = query_key

    if not os.environ.get("TERNODBI_ADMIN_KEY"):
        logger.warning("TERNODBI_ADMIN_KEY not found in environment")
        print("TERNODBI_ADMIN_KEY not found in env.")
        admin_key = getpass.getpass("Enter Admin Agent Key (or press Enter for none): ").strip()
        if admin_key:
            os.environ["TERNODBI_ADMIN_KEY"] = admin_key

    try:
        llm = OpenAIProvider(api_key=api_key)
        server_params = get_default_server_params()
        agent = ChainOfThoughtAgent(server_params=server_params, llm=llm, verbose=True)

        logger.info("Connecting to MCP servers")
        print("Connecting to MCP Servers (Query + Admin)...")
        async with agent:
            logger.info("MCP connection established")
            print("Connected! You can now ask questions about your databases.")
            print("Type 'exit' or 'quit' to stop.\n")

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
                    response = await agent.run(user_input)
                    print(f"Answer: {response}")

                except KeyboardInterrupt:
                    logger.info("CLI interrupted by user")
                    print("\nGoodbye! 👋")
                    break

    except Exception as e:
        logger.exception("CLI error occurred")
        print(f"Error: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
