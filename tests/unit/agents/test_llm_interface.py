import pytest
import os
import requests
from unittest.mock import MagicMock, patch
from terno_dbi.agents.llm_interface import OpenAIProvider, LLMProvider

class TestOpenAIProvider:

    @pytest.fixture
    def mock_requests(self):
        with patch("terno_dbi.agents.llm_interface.requests.post") as mock_post:
            yield mock_post

    def test_init_no_api_key(self):
        """Test initialization warning when missing API key."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("terno_dbi.agents.llm_interface.logger.warning") as mock_warn:
                provider = OpenAIProvider(api_key=None)
                mock_warn.assert_called_with("OpenAI API key not provided. Agent will likely fail.")
                assert provider.api_key is None

    def test_generate_missing_key(self):
        """Test generate method raises ValueError if no API key."""
        with patch.dict(os.environ, {}, clear=True):
            provider = OpenAIProvider(api_key=None)
            with pytest.raises(ValueError, match="OpenAI API Key is required"):
                provider.generate([{"role": "user", "content": "hi"}])

    def test_generate_success(self, mock_requests):
        """Test successful generation."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{"message": {"content": "Hello world"}}]
        }
        mock_requests.return_value = mock_response

        provider = OpenAIProvider(api_key="sk-test")
        result = provider.generate([{"role": "user", "content": "hi"}])

        assert result == "Hello world"
        mock_requests.assert_called_once()
        args, kwargs = mock_requests.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer sk-test"
        assert kwargs["timeout"] == 60

    def test_generate_retry_429(self, mock_requests):
        """Test retry logic on 429 rate limit."""
        # 3 calls: 429 -> 429 -> 200
        resp_429 = MagicMock()
        resp_429.status_code = 429

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.json.return_value = {"choices": [{"message": {"content": "Success"}}]}

        mock_requests.side_effect = [resp_429, resp_429, resp_200]

        with patch("terno_dbi.agents.llm_interface.time.sleep") as mock_sleep:
            provider = OpenAIProvider(api_key="sk-test", max_retries=3)
            result = provider.generate([{"role": "user", "content": "hi"}])

            assert result == "Success"
            assert mock_requests.call_count == 3
            assert mock_sleep.call_count == 2 # Slept twice

    def test_generate_retry_request_exception(self, mock_requests):
        """Test retry logic on request exception."""
        # 2 calls: ConnectionError -> 200
        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.json.return_value = {"choices": [{"message": {"content": "Recovered"}}]}

        mock_requests.side_effect = [requests.exceptions.ConnectionError("Fail"), resp_200]

        with patch("terno_dbi.agents.llm_interface.time.sleep") as mock_sleep:
            provider = OpenAIProvider(api_key="sk-test", max_retries=3)
            result = provider.generate([{"role": "user", "content": "hi"}])

            assert result == "Recovered"
            assert mock_requests.call_count == 2

    def test_generate_failure_after_retries(self, mock_requests):
        """Test failure after max retries."""
        mock_requests.side_effect = requests.exceptions.Timeout("Timeout")

        with patch("terno_dbi.agents.llm_interface.time.sleep"):
            provider = OpenAIProvider(api_key="sk-test", max_retries=2)
            with pytest.raises(requests.exceptions.Timeout):
                provider.generate([])

        assert mock_requests.call_count == 2

    def test_generate_bad_status_no_retry_default(self, mock_requests):
        """Test that non-retriable errors (like 400) raise immediately if unhandled or handled by raise_for_status."""
        # The logic in generate catches RequestException. 
        # requests.raise_for_status raises HTTPError which inherits form RequestException.
        # But specifically 4xx (except 429) are usually client errors. 
        # The code catches RequestException, inspects it in 'except' block:
        # if status < 500 and != 429 -> raise immediately
        
        resp_400 = MagicMock()
        resp_400.status_code = 400
        resp_400.text = "Bad Request"
        resp_400.raise_for_status.side_effect = requests.exceptions.HTTPError("400 Client Error", response=resp_400)

        mock_requests.return_value = resp_400

        provider = OpenAIProvider(api_key="sk-test")
        
        # Should fail immediately on first try
        with pytest.raises(requests.exceptions.HTTPError):
            provider.generate([])
            
        assert mock_requests.call_count == 1
