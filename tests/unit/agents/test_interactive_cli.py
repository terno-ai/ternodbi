import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import os
import io
import contextlib
import asyncio

@pytest.fixture
def mock_env():
    with patch.dict(os.environ, {
        'OPENAI_API_KEY': 'sk-test',
        'TERNODBI_QUERY_KEY': 'q-key',
        'TERNODBI_ADMIN_KEY': 'a-key'
    }, clear=True):
        yield

class TestInteractiveCLI:

    @patch('terno_dbi.agents.interactive_cli.ChainOfThoughtAgent')
    @patch('terno_dbi.agents.interactive_cli.OpenAIProvider')
    @patch('builtins.print')
    def test_main_input_flow(self, mock_print, mock_llm_cls, mock_agent_cls, mock_env):
        """Should handle input loop: empty -> query -> exit."""
        from terno_dbi.agents.interactive_cli import main
        
        os.environ['OPENAI_API_KEY'] = 'sk-test'
        os.environ['TERNODBI_QUERY_KEY'] = 'q-key'
        os.environ['TERNODBI_ADMIN_KEY'] = 'a-key'
        
        # Mock inputs: empty line, verify databases, exit
        with patch('builtins.input', side_effect=["", "verify databases", "exit"]):
             mock_agent = AsyncMock()
             mock_agent.run.return_value = "Verified."
             mock_agent.__aenter__.return_value = mock_agent
             mock_agent.__aexit__.return_value = None
             mock_agent_cls.return_value = mock_agent
             
             asyncio.run(main())
             
             # agent.run called once (for verify databases, empty skipped)
             mock_agent.run.assert_called_once()
             assert "Verified." in str(mock_print.call_args_list)

    @patch('terno_dbi.agents.interactive_cli.getpass.getpass')
    @patch('builtins.input', side_effect=["exit"])
    def test_main_getpass_flow(self, mock_input, mock_getpass, mock_env):
        """Should prompt for missing keys via getpass."""
        from terno_dbi.agents.interactive_cli import main
        
        # Explicitly remove keys to trigger getpass
        del os.environ['OPENAI_API_KEY']
        del os.environ['TERNODBI_QUERY_KEY']
        del os.environ['TERNODBI_ADMIN_KEY']
        
        # Missing all keys -> prompts
        mock_getpass.side_effect = ['sk-test', 'q-key', 'a-key']
        
        with patch('terno_dbi.agents.interactive_cli.ChainOfThoughtAgent') as mock_agent_cls:
             mock_agent = AsyncMock()
             mock_agent.__aenter__.return_value = mock_agent
             mock_agent.__aexit__.return_value = None
             mock_agent_cls.return_value = mock_agent
             
             asyncio.run(main())
             
             assert os.environ['OPENAI_API_KEY'] == 'sk-test'
             assert os.environ['TERNODBI_QUERY_KEY'] == 'q-key'
             assert os.environ['TERNODBI_ADMIN_KEY'] == 'a-key'

    @patch('terno_dbi.agents.interactive_cli.getpass.getpass', return_value="")
    def test_main_missing_openai_key_exit(self, mock_getpass, mock_env):
        """Should exit if OpenAI key not provided."""
        from terno_dbi.agents.interactive_cli import main
        
        # Force missing key
        if 'OPENAI_API_KEY' in os.environ:
            del os.environ['OPENAI_API_KEY']
        
        with patch('builtins.print') as mock_print:
             asyncio.run(main())
             mock_print.assert_any_call("OpenAI API Key is required.")

    @patch('terno_dbi.agents.interactive_cli.ChainOfThoughtAgent')
    @patch('terno_dbi.agents.interactive_cli.OpenAIProvider')
    def test_keyboard_interrupt(self, mock_llm, mock_agent, mock_env):
        """Should handle KeyboardInterrupt gracefully."""
        from terno_dbi.agents.interactive_cli import main
        
        os.environ['OPENAI_API_KEY'] = 'sk-test'
        os.environ['TERNODBI_QUERY_KEY'] = 'q'
        
        # Setup agent context
        mock_agent_inst = AsyncMock()
        mock_agent_inst.__aenter__.return_value = mock_agent_inst
        mock_agent_inst.__aexit__.return_value = None
        mock_agent.return_value = mock_agent_inst
        
        # Simulate Ctrl+C on input
        with patch('builtins.input', side_effect=KeyboardInterrupt):
             with patch('builtins.print') as mock_print:
                 asyncio.run(main())
                 # Check last print (end of loop)
                 args, _ = mock_print.call_args
                 assert "Goodbye!" in args[0]
                 
    def test_main_exception_safety(self, mock_env):
        """Should catch top-level exceptions."""
        from terno_dbi.agents.interactive_cli import main
        
        os.environ['OPENAI_API_KEY'] = 'sk-test'
        
        with patch('terno_dbi.agents.interactive_cli.ChainOfThoughtAgent', side_effect=Exception("Explosion")):
             with patch('builtins.print') as mock_print:
                  asyncio.run(main())
                  args, _ = mock_print.call_args
                  assert "Error: Explosion" in str(args[0])
