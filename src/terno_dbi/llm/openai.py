from .base import BaseLLM
from openai import OpenAI
import json
import tiktoken
import logging

logger=logging.getLogger(__name__)

class OpenAILLM(BaseLLM):
    o_series_models = ['o1', 'o1-2024-12-17',
                       'o1-preview', 'o1-preview-2024-09-12',
                       'o1-mini', 'o1-mini-2024-09-12',
                       'o3-mini', 'o3-mini-2025-01-31',
                       'o4-mini', 'o4-mini-2025-04-16',
                       'o3',
                       'o1-pro', 'o1-pro-2025-03-19']
    """O series models configuration."""
    model_name: str = "o4-mini"
    # model_name: str = "gpt-4o"
    """Model name to use.

    You can use the
    [List models](https://platform.openai.com/docs/api-reference/models/list) API to
    see all of your available models, or see OpenAI's
    [Model overview](https://platform.openai.com/docs/models/overview) for
    descriptions of them.
    """
    temperature: float = 0
    """What sampling temperature to use, between 0 and 2."""
    max_tokens: int = 1024
    """The maximum number of tokens to generate in the completion."""
    top_p: float = 1
    """Controls the cumulative probability threshold for next-word selection.
    The model considers the smallest set of words whose combined probability
    is at least top_p. A lower value reduces randomness, focusing on more
    probable words."""

    def __init__(self, api_key: str,
                 model_name: str = None,
                 temperature: float = None,
                 max_tokens: int = None,
                 top_p: float = None,
                 **kwargs):
        super().__init__(api_key, **kwargs)
        self.model_name = model_name or self.model_name
        self.temperature = temperature if temperature is not None else self.temperature
        self.max_tokens = max_tokens if max_tokens is not None else self.max_tokens
        self.top_p = top_p if top_p is not None else self.top_p

    def get_model_instance(self):
        client = OpenAI(
            api_key=self.api_key,
        )
        return client

    def num_tokens_from_messages(self, messages, model=None):
        if not model:
            model = self.model_name
        try:
            encoding = tiktoken.encoding_for_model(model)
        except Exception:
            # o200k_base is the encoding used by the "o" family (o4-mini, gpt-4o, etc.).
            encoding = tiktoken.get_encoding("o200k_base")

        # Overhead tokens per message (structure / delimiters)
        tokens_per_message = 4

        num_tokens = 0
        for message in messages:
            num_tokens += tokens_per_message
            for key, value in message.items():
                # value should be a string; encode -> list of tokens -> len()
                if isinstance(value, str):
                    num_tokens += len(encoding.encode(value))
                else:
                    # If value is non-string (e.g., dict), convert to str defensively
                    num_tokens += len(encoding.encode(str(value)))

        # Final priming tokens (assistant start marker)
        num_tokens += 4
        print(f"Total tokens for messages: {num_tokens}")
        return num_tokens

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

    # def get_response(self, messages) -> dict:
    def get_response(self, messages, tools=None, tool_choice=None, priority=False, summarize=False) -> dict:
        model = self.get_model_instance()
        model_name = self.model_name

        # Prepare API parameters
        api_params = {
            "model": self.model_name,
            "messages": messages,
            **self.custom_parameters
        }

        api_params["service_tier"] = "priority" if priority else "default"

        # Add tools and tool_choice if provided
        if tools is not None:
            api_params["tools"] = tools
        if tool_choice is not None:
            api_params["tool_choice"] = tool_choice

        if model_name in self.o_series_models:
            messages[0]['role'] = 'developer'
            try:
                response = model.chat.completions.create(**api_params)
            except Exception as e:
                return {'status': "error", "error_code": e.body['code'], "message": e.body['message']}
        else:
            api_params.update({
                "temperature": self.temperature,
                "top_p": self.top_p,
            })
            try:
                response = model.chat.completions.create(**api_params)
            except Exception as e:
                return {'status': "error", "error_code": e.body['code'], "message": e.body['message']}

        return_dict = {'status': 'success'}
        message = response.choices[0].message
        generated_sql = message.content or ""
        return_dict['generated_sql'] = generated_sql.strip().removeprefix("```json").removeprefix("```sql").removeprefix("```").removesuffix("```").strip()

        # Store the full message object for tool handling
        return_dict['message'] = message

        try:
            return_dict['llm_provider'] = 'openai'
            return_dict['model'] = response.model
            return_dict['input_tokens'] = response.usage.prompt_tokens
            return_dict['input_tokens_cached'] = response.usage.prompt_tokens_details['cached_tokens']
            return_dict['output_tokens'] = response.usage.completion_tokens
        except Exception as e:
            pass
        return return_dict

    def get_streaming_response(self, messages, chunk_handler) -> dict:
        model = self.get_model_instance()
        model_name = self.model_name
        # designate role for o-series
        if model_name in self.o_series_models:
            messages[0]['role'] = 'developer'
            stream = model.chat.completions.create(
                model=self.model_name,
                messages=messages,
                stream=True,
                stream_options={"include_usage": True},
                **self.custom_parameters
            )
        else:
            stream = model.chat.completions.create(
                model=self.model_name,
                messages=messages,
                temperature=self.temperature,
                top_p=self.top_p,
                stream=True,
                stream_options={"include_usage": True},
                **self.custom_parameters
            )

        full_response = ""
        usage = None
        return_dict = {}
        for chunk in stream:
            if len(chunk.choices) > 0:
                delta = getattr(chunk.choices[0].delta, "content", None)
            if delta:
                # call your handler (prints + channels)
                chunk_handler(delta)
                full_response += delta
            # The final chunk may carry the `.usage` field
            if hasattr(chunk, "usage") and chunk.usage is not None:
                usage = chunk.usage
                print("Chunk", chunk)
                return_dict['input_tokens'] = usage.prompt_tokens
                return_dict['input_tokens_cached'] = usage.prompt_tokens_details['cached_tokens']
                return_dict['output_tokens'] = usage.completion_tokens
                return_dict['model'] = chunk.model
                return_dict['llm_provider'] = 'openai'

        return_dict['generated_sql'] = full_response
        return return_dict

    def csv_llm_response(self, messages):
        model = self.get_model_instance()
        model_name = self.model_name
        if model_name in self.o_series_models:
            messages[0]['role'] = 'developer'
            response = model.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                **self.custom_parameters
            )
        else:
            response = model.chat.completions.create(
                model="gpt-4o",
                messages=messages,
                temperature=self.temperature,
                top_p=self.top_p,
                **self.custom_parameters
            )

        generated_csv_schema = response.choices[0].message.content
        generated_csv_schema = generated_csv_schema.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        generated_csv_schema_json = json.loads(generated_csv_schema)
        return generated_csv_schema_json

    def generate_vector(self, prompt):
        """
        Generates an embedding vector for a given prompt using OpenAI's text-embedding-ada-002 model.
        """
        model = self.get_model_instance()

        try:
            response = model.embeddings.create(
                input=prompt,
                model="text-embedding-ada-002"
            )
            if len(response.data) == 1:
                return response.data[0].embedding
            else:
                return [data_ins.embedding for data_ins in response.data]

        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            return None     

    def get_simple_response(self, prompt: str) -> str:
        model = self.get_model_instance()
        messages = [{"role": "user", "content": prompt}]
        response = model.chat.completions.create(
            model=self.model_name,
            messages=messages
        )
        answer = response.choices[0].message.content
        return answer.strip().removeprefix("```python").removeprefix("```json").removeprefix("```").removesuffix("```").strip()
