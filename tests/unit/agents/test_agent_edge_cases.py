import pytest
import asyncio
import json
from unittest.mock import MagicMock, AsyncMock, patch
from mcp import Tool, ClientSession
from mcp.types import CallToolResult, TextContent, ImageContent

from terno_dbi.agents.agent import ChainOfThoughtAgent
from terno_dbi.agents.llm_interface import MockLLMProvider

class TestAgentEdgeCases:
    
    @pytest.fixture
    def mock_session(self):
        session = AsyncMock(spec=ClientSession)
        session.list_tools.return_value.tools = []
        return session
    
    def test_context_manager_lifecycle(self):
        """Test __aenter__ and __aexit__ logic."""
        
        # We need to mock stdio_client and ClientSession inside __aenter__
        with patch('terno_dbi.agents.agent.stdio_client') as mock_stdio, \
             patch('terno_dbi.agents.agent.ClientSession') as mock_session_cls:
            
            mock_stdio.return_value.__aenter__.return_value = (MagicMock(), MagicMock())
            mock_session_instance = AsyncMock()
            mock_session_instance.list_tools.return_value.tools = [
                Tool(name="test_tool", description="desc", inputSchema={})
            ]
            mock_session_cls.return_value.__aenter__.return_value = mock_session_instance
            
            # Setup params using MagicMock properly
            params = MagicMock()
            params.command = "python" # Required for logging error access
            
            agent = ChainOfThoughtAgent(
                server_params=[params],
                llm=MagicMock()
            )
            
            async def run_lifecycle():
                async with agent as a:
                    assert len(a.sessions) == 1
                    assert "test_tool" in a.tools_registry
                    assert len(a.tool_descriptions) == 1
                    
            asyncio.run(run_lifecycle())

    def test_context_manager_connection_failure(self):
        """Test __aenter__ handling connection failures."""
        
        with patch('terno_dbi.agents.agent.stdio_client', side_effect=Exception("Connection failed")):
            params = MagicMock()
            params.command = "bad_server"
            
            agent = ChainOfThoughtAgent(
                server_params=[params],
                llm=MagicMock()
            )
            
            async def run_lifecycle():
                async with agent as a:
                    # Should log error but not raise
                    assert len(a.sessions) == 0
                    assert len(a.tools_registry) == 0
                    
            asyncio.run(run_lifecycle())

    def test_get_system_prompt_no_tools(self):
        """Test system prompt when no tools are available."""
        agent = ChainOfThoughtAgent(server_params=[], llm=MagicMock())
        agent.tool_descriptions = []
        prompt = agent._get_system_prompt()
        assert "No tools are currently available" in prompt

    def test_get_system_prompt_with_tools(self):
        """Test system prompt with tools."""
        agent = ChainOfThoughtAgent(server_params=[], llm=MagicMock())
        agent.tool_descriptions = ["- `tool1`: does things"]
        prompt = agent._get_system_prompt()
        assert "You have access to the following tools" in prompt
        assert "- `tool1`: does things" in prompt

    def test_extract_json_edge_cases(self):
        """Test _extract_json_from_string edge cases."""
        agent = ChainOfThoughtAgent(server_params=[], llm=MagicMock())
        
        # No braces
        assert agent._extract_json_from_string("no json here") is None
        
        # Unbalanced braces
        assert agent._extract_json_from_string("{ unbalanced") is None
        
        # Nested braces
        json_str = '{"foo": {"bar": "baz"}}'
        text = f"Some text {json_str} end text"
        assert agent._extract_json_from_string(text) == json_str

    def test_execute_tool_unknown(self):
        """Test execute_tool with unknown tool."""
        agent = ChainOfThoughtAgent(server_params=[], llm=MagicMock())
        
        async def run_test():
            return await agent._execute_tool("unknown_tool", {})
            
        result = asyncio.run(run_test())
        assert "Unknown tool" in result

    def test_execute_tool_content_types(self, mock_session):
        """Test execution handling different content types."""
        agent = ChainOfThoughtAgent(server_params=[], llm=MagicMock())
        agent.tools_registry["mixed_tool"] = mock_session
        
        mock_session.call_tool.return_value = CallToolResult(
            content=[
                TextContent(type="text", text="Hello"),
                ImageContent(type="image", data="base64data", mimeType="image/png"),
            ]
        )
        
        async def run_test():
            return await agent._execute_tool("mixed_tool", {})
            
        result = asyncio.run(run_test())
        assert "Hello" in result
        assert "[image content]" in result

    def test_execute_tool_exception(self, mock_session):
        """Test execution handling exceptions."""
        agent = ChainOfThoughtAgent(server_params=[], llm=MagicMock())
        agent.tools_registry["crashy_tool"] = mock_session
        
        mock_session.call_tool.side_effect = Exception("Boom")
        
        async def run_test():
            return await agent._execute_tool("crashy_tool", {})
            
        result = asyncio.run(run_test())
        assert "Error executing tool crashy_tool: Boom" in result

    def test_run_no_sessions(self):
        """Test run with no sessions connected."""
        agent = ChainOfThoughtAgent(server_params=[], llm=MagicMock())
        agent.sessions = []
        
        async def run_test():
            return await agent.run("Hello")
            
        result = asyncio.run(run_test())
        assert "Error: Agent is not connected" in result

    def test_run_thought_no_action(self, mock_session):
        """Test run where LLM provides Thought but loops without Action."""
        # Provide Thought but no Action, agent should prompt for Action
        # Then eventually give up or hit max steps (we test 1 iteration here by providing Final Answer next)
        responses = [
            "Thought: I'm thinking...",
            "Final Answer: Done"
        ]
        
        llm = MockLLMProvider(responses=responses)
        agent = ChainOfThoughtAgent(server_params=[], llm=llm)
        agent.sessions = [mock_session] # Fake session to bypass check
        
        async def run_test():
            return await agent.run("Hi")
            
        result = asyncio.run(run_test())
        assert result == "Done"
        # Check that user prompt was added
        assert any("You provided a Thought but no Action" in msg['content'] for msg in agent.messages if msg['role'] == 'user')

    def test_run_missing_action_input(self, mock_session):
        """Test run where Action Input label is missing."""
        responses = [
            "Action: test_tool\n(No input label)",
            "Final Answer: Corrected"
        ]
        
        llm = MockLLMProvider(responses=responses)
        agent = ChainOfThoughtAgent(server_params=[], llm=llm)
        agent.sessions = [mock_session]
        
        async def run_test():
            return await agent.run("Hi")
            
        result = asyncio.run(run_test())
        assert result == "Corrected"
        assert any("No Action Input found" in msg['content'] for msg in agent.messages)

    def test_execute_tool_empty_content(self, mock_session):
        """Test execution returning empty content."""
        agent = ChainOfThoughtAgent(server_params=[], llm=MagicMock())
        agent.tools_registry["empty_tool"] = mock_session
        
        mock_session.call_tool.return_value = CallToolResult(content=[])
        
        async def run_test():
            return await agent._execute_tool("empty_tool", {})
            
        result = asyncio.run(run_test())
        assert result == "Tool executed successfully (no output)."

    def test_run_chat_only(self, mock_session):
        """Test run where LLM replies without Thought/Action tags (just chat)."""
        responses = ["Just a chat response."]
        
        llm = MockLLMProvider(responses=responses)
        agent = ChainOfThoughtAgent(server_params=[], llm=llm)
        agent.sessions = [mock_session]
        
        async def run_test():
            return await agent.run("Hi")
            
        result = asyncio.run(run_test())
        assert result == "Just a chat response."

    def test_run_bad_action_input_brace_mismatch(self, mock_session):
        """Test action input that cannot be parsed by helper (returns None)."""
        # _extract_json_from_string returns None if braces don't match
        responses = [
            'Action: tool\nAction Input: {unbalanced',
            'Final Answer: Done'
        ]
        
        llm = MockLLMProvider(responses=responses)
        agent = ChainOfThoughtAgent(server_params=[], llm=llm)
        agent.sessions = [mock_session]
        
        async def run_test():
            return await agent.run("Do it")
            
        result = asyncio.run(run_test())
        assert result == "Done"
        assert any("Could not parse Action Input" in msg['content'] for msg in agent.messages)
