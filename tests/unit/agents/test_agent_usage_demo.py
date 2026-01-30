import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import os
import io
import contextlib

@pytest.fixture
def mock_env():
    with patch.dict(os.environ, {}, clear=True):
        yield

class TestAgentUsageDemo:

    @patch('terno_dbi.examples.agents.agent_usage_demo.ChainOfThoughtAgent')
    @patch('terno_dbi.examples.agents.agent_usage_demo.OpenAIProvider')
    def test_main_success(self, mock_llm_cls, mock_agent_cls, mock_env):
        """Should run agent successfully given correct env."""
        from terno_dbi.examples.agents.agent_usage_demo import main
        
        # Setup env
        os.environ['OPENAI_API_KEY'] = 'sk-test'
        os.environ['TERNODBI_QUERY_KEY'] = 'query-key'
        
        # Mock agent
        mock_agent = AsyncMock()
        mock_agent.run.return_value = "Here are your Postgres DBs."
        # Async context manager
        mock_agent.__aenter__.return_value = mock_agent
        mock_agent.__aexit__.return_value = None
        mock_agent_cls.return_value = mock_agent

        # Capture output
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
             import asyncio
             asyncio.run(main())
        
        output = f.getvalue()
        assert "Chain of Thought Agent Demo" in output
        assert "Final Answer" in output
        assert "Here are your Postgres DBs" in output

    def test_main_no_api_key(self, mock_env):
        """Should exit if OPENAI_API_KEY missing."""
        from terno_dbi.examples.agents.agent_usage_demo import main
        
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
             import asyncio
             asyncio.run(main())
             
        output = f.getvalue()
        assert "Error: OPENAI_API_KEY environment variable is not set" in output

    @patch('terno_dbi.examples.agents.agent_usage_demo.ChainOfThoughtAgent')
    @patch('terno_dbi.examples.agents.agent_usage_demo.OpenAIProvider')
    def test_main_warnings_missing_keys(self, mock_llm, mock_agent_cls, mock_env):
        """Should warn if Terno keys missing."""
        from terno_dbi.examples.agents.agent_usage_demo import main
        
        os.environ['OPENAI_API_KEY'] = 'sk-test'
        # No TERNODBI keys
        
        mock_agent = AsyncMock()
        mock_agent.__aenter__.return_value = mock_agent
        mock_agent.__aexit__.return_value = None
        mock_agent_cls.return_value = mock_agent
        
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
             import asyncio
             asyncio.run(main())
             
        output = f.getvalue()
        assert "Warning: TERNODBI_QUERY_KEY and TERNODBI_ADMIN_KEY are not set" in output
