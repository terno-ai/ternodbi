import pytest
import asyncio
import os
import sys
import getpass # Import actual getpass module
from unittest.mock import MagicMock, patch, AsyncMock

# Import directly
from terno_dbi.examples.agents import interactive_cli as cli_mod
from terno_dbi.examples.agents import agent_usage_demo as demo_mod
from terno_dbi.examples.agents.interactive_cli import main as cli_main
from terno_dbi.examples.agents.agent_usage_demo import main as demo_main

class TestAgentUsageDemo:
    
    @patch.dict(os.environ, {}, clear=True)
    def test_no_api_key(self):
        with patch('builtins.print') as mock_print:
            asyncio.run(demo_main())
            mock_print.assert_any_call("Error: OPENAI_API_KEY environment variable is not set.")

    @patch.dict(os.environ, {"OPENAI_API_KEY": "test"}, clear=True)
    def test_run_success(self):
        with patch('terno_dbi.examples.agents.agent_usage_demo.ChainOfThoughtAgent') as mock_agent_cls, \
             patch('terno_dbi.examples.agents.agent_usage_demo.get_default_server_params'), \
             patch('terno_dbi.examples.agents.agent_usage_demo.OpenAIProvider'):
             
             mock_agent = AsyncMock()
             mock_agent.__aenter__.return_value = mock_agent
             mock_agent.__aexit__.return_value = None
             mock_agent.run.return_value = "Demo Answer"
             mock_agent_cls.return_value = mock_agent
             
             with patch('builtins.print') as mock_print:
                 asyncio.run(demo_main())
                 
                 mock_agent.run.assert_called_once()
                 assert "Demo Answer" in str(mock_print.call_args_list)

class TestInteractiveCLI:
    
    # Pre-populate ALL keys to avoid getpass prompts
    @patch.dict(os.environ, {
        "OPENAI_API_KEY": "exists",
        "TERNODBI_QUERY_KEY": "q",
        "TERNODBI_ADMIN_KEY": "a"
    }, clear=True)
    def test_cli_loop(self):
        """Test chat loop execution."""
        inputs = ["Hello", "", "exit"] # Loop: Hello -> empty -> exit
        
        with patch.object(cli_mod, 'ChainOfThoughtAgent') as mock_agent_cls, \
             patch.object(cli_mod, 'get_default_server_params'), \
             patch.object(cli_mod, 'OpenAIProvider'), \
             patch('builtins.input', side_effect=inputs):
             
             mock_agent = AsyncMock()
             mock_agent.__aenter__.return_value = mock_agent
             mock_agent.run.return_value = "Hi there"
             mock_agent_cls.return_value = mock_agent
             
             # Patch getpass just in case, to prevent hanging if logic slips, but side_effect=Exception to allow fail fast
             with patch('getpass.getpass', side_effect=Exception("Should not be called!")):
                 asyncio.run(cli_main())
             
             mock_agent.run.assert_awaited_with("Hello")
             assert mock_agent.run.call_count == 1

    @patch.dict(os.environ, {
        "OPENAI_API_KEY": "exists",
        "TERNODBI_QUERY_KEY": "q",
        "TERNODBI_ADMIN_KEY": "a"
    }, clear=True)
    def test_cli_exception_handling(self):
        """Test exception handling in loop or setup."""
        # Exception during agent creation
        with patch.object(cli_mod, 'OpenAIProvider', side_effect=Exception("Init Fail")), \
             patch('builtins.print') as mock_print:
             
             asyncio.run(cli_main())
             mock_print.assert_any_call("Error: Init Fail")

    @patch.dict(os.environ, {
        "OPENAI_API_KEY": "exists",
        "TERNODBI_QUERY_KEY": "q",
        "TERNODBI_ADMIN_KEY": "a"
    }, clear=True)
    def test_keyboard_interrupt_in_loop(self):
        """Test KeyboardInterrupt in loop."""
        with patch.object(cli_mod, 'ChainOfThoughtAgent') as mock_agent_cls, \
             patch.object(cli_mod, 'get_default_server_params'), \
             patch.object(cli_mod, 'OpenAIProvider'), \
             patch('builtins.input', side_effect=KeyboardInterrupt):
             
             mock_agent = AsyncMock()
             mock_agent.__aenter__.return_value = mock_agent
             mock_agent_cls.return_value = mock_agent
             
             with patch('builtins.print') as mock_print:
                 asyncio.run(cli_main())
                 assert any("Goodbye" in str(c) for c in mock_print.call_args_list)

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_api_key_exit(self):
        """Test exit if no API key provided - we can simulate getpass returning empty string logic without hanging 
           by using input mock IF getpass uses it, OR just rely on patch('getpass.getpass') now that we reverted import?
           Reverted import uses 'module.getpass'. 
           Robust approach: Mock the module function globally."""
           
        # Since we reverted, usage is `getpass.getpass`. 
        # So patch('getpass.getpass') works globally.
        with patch('getpass.getpass', return_value=""), \
             patch('builtins.print') as mock_print:
             
             asyncio.run(cli_main())
             mock_print.assert_any_call("OpenAI API Key is required.")

    @patch.dict(os.environ, {}, clear=True)
    def test_prompts_for_missing_keys(self):
        """Test that missing keys trigger getpass prompts (mocked)."""
        # Patch the actual getpass.getpass function on the module object
        with patch.object(getpass, 'getpass', side_effect=["sk-key", "q-key", "a-key"]) as mock_gp, \
             patch.object(cli_mod, 'ChainOfThoughtAgent'), \
             patch.object(cli_mod, 'OpenAIProvider'), \
             patch.object(cli_mod, 'get_default_server_params'), \
             patch('builtins.input', side_effect=["exit"]):
             
             asyncio.run(cli_main())
             
             assert mock_gp.call_count == 3
             # Verify keys were set in env (cli sets them)
             assert os.environ["OPENAI_API_KEY"] == "sk-key"
             assert os.environ["TERNODBI_QUERY_KEY"] == "q-key"
             assert os.environ["TERNODBI_ADMIN_KEY"] == "a-key"
