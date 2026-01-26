import pytest
import json
import asyncio
from unittest.mock import MagicMock, AsyncMock
from mcp import Tool
from mcp.types import CallToolResult, TextContent

from terno_dbi.agents.agent import ChainOfThoughtAgent
from terno_dbi.agents.llm_interface import MockLLMProvider


class TestChainOfThoughtAgent:

    @pytest.fixture
    def mock_server_params(self):
        return []

    @pytest.fixture
    def mock_session(self):
        session = AsyncMock()

        session.list_tools.return_value.tools = [
            Tool(
                name="list_datasources", 
                description="List datasources", 
                inputSchema={"type": "object", "properties": {}}
            ),
            Tool(
                name="list_tables", 
                description="List tables", 
                inputSchema={"type": "object", "properties": {"datasource_id": {"type": "int"}}}
            )
        ]

        async def call_tool_side_effect(name, arguments):
            if name == "list_datasources":
                result_text = json.dumps([{"id": 1, "name": "test_db", "type": "postgres"}])
            elif name == "list_tables":
                result_text = json.dumps([{"id": 101, "name": "users", "public_name": "Users Table"}])
            else:
                result_text = "Error"

            return CallToolResult(content=[TextContent(type="text", text=result_text)])

        session.call_tool.side_effect = call_tool_side_effect
        return session

    def test_run_simple_flow(self, mock_server_params, mock_session):
        """Agent asks for datasources, gets them, then gives final answer."""
        responses = [
            'Thought: I need to check available datasources.\nAction: list_datasources\nAction Input: {}',
            'Thought: The datasource is test_db.\nFinal Answer: There is one datasource named test_db.'
        ]

        llm = MockLLMProvider(responses=responses)
        agent = ChainOfThoughtAgent(server_params=mock_server_params, llm=llm, verbose=False)
        agent.sessions = [mock_session]

        async def run_test():
            # Register tools
            result = await mock_session.list_tools()
            for tool in result.tools:
                agent.tools_registry[tool.name] = mock_session
                agent.tool_descriptions.append(f"- `{tool.name}`")

            return await agent.run("What datasources do I have?")

        final_answer = asyncio.run(run_test())

        assert final_answer == "There is one datasource named test_db."
        assert llm.call_count == 2
        mock_session.call_tool.assert_called()

    def test_run_invalid_json_action_input(self, mock_server_params, mock_session):
        """Agent provides invalid JSON, recovers, and eventually succeeds."""
        responses = [
            # Thought 1: Invalid JSON
            'Thought: I will try to list tables.\nAction: list_tables\nAction Input: {invalid_json}',
            # Thought 2: Agent corrects itself
            'Thought: I made a mistake.\nAction: list_tables\nAction Input: {"datasource_id": 1}',
            # Thought 3: Final answer
            'Final Answer: Done.'
        ]

        llm = MockLLMProvider(responses=responses)
        agent = ChainOfThoughtAgent(server_params=mock_server_params, llm=llm, verbose=False)
        agent.sessions = [mock_session]

        async def run_test():
            result = await mock_session.list_tools()
            for tool in result.tools:
                agent.tools_registry[tool.name] = mock_session
                agent.tool_descriptions.append(f"- `{tool.name}`")
            return await agent.run("List tables")

        final_answer = asyncio.run(run_test())

        assert final_answer == "Done."
        # Should have called LLM 3 times (invalid -> correct -> final)
        assert llm.call_count == 3
        # Verify list_tables was called with correct args after recovery
        mock_session.call_tool.assert_called_with("list_tables", arguments={"datasource_id": 1})

    def test_max_steps_reached(self, mock_server_params, mock_session):
        """Agent loops forever and hits max steps."""
        responses = [
            'Thought: Thinking...' for _ in range(5)
        ]

        llm = MockLLMProvider(responses=responses)
        agent = ChainOfThoughtAgent(server_params=mock_server_params, llm=llm, max_steps=3, verbose=False)
        agent.sessions = [mock_session]

        async def run_test():
            return await agent.run("Loop test")

        result = asyncio.run(run_test())

        assert "Error: Maximum steps reached" in result
        assert llm.call_count == 3
