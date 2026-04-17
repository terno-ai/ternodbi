import requests
from .base import BaseLLM
from django.conf import settings
import logging
from openai.types.chat.chat_completion_message import ChatCompletionMessage


logger = logging.getLogger(__name__)


class TernoLLM(BaseLLM):
    model_name: str = "o4-mini"

    def __init__(self, api_key, model_name: str = None, **kwargs):
        super().__init__(api_key, **kwargs)
        self.api_key = api_key
        self.provisioner_url = settings.PROVISIONER_URL

        if model_name:
            self.model_name = model_name

    def get_model_instance(self):
        raise NotImplementedError("This is not implemented")

    def get_role_specific_message(self, message, role, tool_calls=None):
        if role == 'system':
            return {"role": "system", "content": message}
        elif role == 'assistant':
            if tool_calls:
                return {"role": "assistant", "tool_calls": tool_calls}
            return {"role": "assistant", "content": message}
        elif role == 'user':
            return {"role": "user", "content": message}
        elif role == 'summary':
            return {"role": "assistant", "content": message}
        else:
            raise Exception(f"Invalid role: {role}")

    def create_message_for_llm(self, system_prompt, ai_prompt, human_prompt):
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "assistant", "content": ai_prompt},
            {"role": "user", "content": human_prompt},
        ]
        return messages

    def call_provisioner(self, payload):
        headers = {
            "X-Terno-App-Version": settings.APP_VERSION,
        }
        response = requests.post(
            f"{self.provisioner_url}/root/llm/",
            headers=headers,
            json={
                **payload,
                "api_key": self.api_key,
            },
        )
        logger.info(f"Response: {response}")
        return response.json()

    def num_tokens_from_messages(self, messages, model=None):
        payload = {
            "type": "num_tokens_from_messages",
            "messages": messages,
            # "model": model or self.model_name,
        }
        try:
            response = self.call_provisioner(payload)
            return response["num_tokens"]
        except Exception as e:
            logger.error(f"Error getting token count from provisioner: {str(e)}")
            return None

    def get_response(self, messages, tools=None, tool_choice=None, priority=False, summarize=False) -> dict:
        payload = {
            "type": "get_response",
            "messages": messages,
            "tools": tools,
            "tool_choice": tool_choice,
            "priority": priority,
            "summarize": summarize,
        }
        try:
            response_data = self.call_provisioner(payload)

            logger.info(f"Response from Provisioner: {response_data}")

            message_obj = ChatCompletionMessage.model_validate(response_data.get("message") or {})
            raw_response = response_data.get("raw_generated_sql") or ""

            if response_data["status"] == "error":
                return response_data
            else:
                return {
                    'status': response_data.get("status"),
                    'message': message_obj,
                    'generated_sql': raw_response
                    .strip()
                    .removeprefix("```json")
                    .removeprefix("```sql")
                    .removeprefix("```")
                    .removesuffix("```")
                    .strip(),
                    'input_tokens': response_data.get("input_tokens"),
                    'input_tokens_cached': response_data.get("input_tokens_cached", 0),
                    'output_tokens': response_data.get("output_tokens"),
                    'model': response_data.get("model"),
                    'llm_provider': response_data.get("llm_provider"),
                }
        except requests.exceptions.RequestException as e:
            return {"status": "error", "error": f"Request failed: {str(e)}"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def csv_llm_response(self, messages):

        payload = {
            "type": "csv_llm_response",
            "messages": messages
        }
        raw = self.call_provisioner(payload)

        generated_csv_schema = raw["choices"][0]["message"]["content"]
        generated_csv_schema = (
            generated_csv_schema
            .strip()
            .removeprefix("```json")
            .removeprefix("```")
            .removesuffix("```")
            .strip()
        )
        return generated_csv_schema

    def generate_vector(self, prompt):
        payload = {
            "type": "generate_vector",
            "prompt": prompt
        }
        response = self.call_provisioner(payload)
        return response["embedding"]
