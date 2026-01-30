from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import os
import requests
import json
import logging
import time

logger = logging.getLogger(__name__)


class LLMProvider(ABC):
    @abstractmethod
    def generate(self, messages: List[Dict[str, str]], temperature: float = 0.0) -> str:
        """
        Generates a response from the LLM based on the provided messages.
        Args:
            messages: A list of message dictionaries (e.g., {"role": "user", "content": "..."}).
            temperature: Sampling temperature.
        Returns:
            The generated text response.
        """
        pass


class OpenAIProvider(LLMProvider):

    def __init__(
        self,
        api_key: Optional[str] = None, 
        model: str = "gpt-4o",
        timeout: int = 60,
        max_retries: int = 3
    ):
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries
        if not self.api_key:
            logger.warning("OpenAI API key not provided. Agent will likely fail.")

    def generate(self, messages: List[Dict[str, str]], temperature: float = 0.0) -> str:
        if not self.api_key:
            raise ValueError("OpenAI API Key is required.")

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }

        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature
        }

        last_exception = None
        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers=headers,
                    data=json.dumps(payload),
                    timeout=self.timeout
                )

                if response.status_code == 429:
                    wait_time = 2 ** attempt
                    logger.warning(f"Rate limited (429). Retrying in {wait_time}s... (attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                data = response.json()
                return data["choices"][0]["message"]["content"]

            except requests.exceptions.RequestException as e:
                last_exception = e
                logger.error(f"Error calling OpenAI API: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"API Response: {e.response.text}")
                    if e.response.status_code < 500 and e.response.status_code != 429:
                        raise

                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Retrying in {wait_time}s... (attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(wait_time)

        raise last_exception or Exception("Failed after all retries")


class MockLLMProvider(LLMProvider):
    def __init__(self, responses: Optional[List[str]] = None):
        self.responses = responses if responses is not None else []
        self.call_count = 0
        self.received_messages: List[List[Dict[str, str]]] = []

    def generate(self, messages: List[Dict[str, str]], temperature: float = 0.0) -> str:
        self.received_messages.append(messages)
        if self.call_count < len(self.responses):
            response = self.responses[self.call_count]
            self.call_count += 1
            return response
        return "No more mock responses configured."
