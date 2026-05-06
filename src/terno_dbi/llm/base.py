from abc import ABC, abstractmethod
from django.core.exceptions import ObjectDoesNotExist
from terno_dbi.core.models import LLMConfiguration
import logging

logger = logging.getLogger(__name__)


class NoActiveLLMException(Exception):
    def __init__(self, message="No active LLM configured for this organisation."):
        super().__init__(message)


class BaseLLM(ABC):
    def __init__(self, api_key: str, **kwargs):
        self.api_key = api_key
        self.custom_parameters = kwargs

    @abstractmethod
    def get_model_instance(self):
        pass

    @abstractmethod
    def get_role_specific_message(self, message, role):
        pass

    @abstractmethod
    def create_message_for_llm(self, system_prompt, ai_prompt, human_prompt):
        pass

    @abstractmethod
    def get_response(self, messages) -> dict:
        pass

    @abstractmethod
    def csv_llm_response(self, messages):
        pass

    @abstractmethod
    def generate_vector(self, prompt):
        pass


class LLMFactory:
    """
    Pluggable LLM factory for terno_dbi.

    Default behaviour: reads LLMConfiguration from the database and creates
    a direct provider instance (OpenAI, Gemini, etc.).

    Host applications (e.g. terno-ai SaaS) can call
    ``LLMFactory.register(factory_fn)`` at startup to override the default
    behaviour.  The registered function must accept
    ``(organisation, model_name_override=None)`` and return a ``BaseLLM``.
    """

    _override = None

    @classmethod
    def register(cls, factory_fn):
        """
        Register an override factory function.

        When set, ``create_llm()`` delegates to this function instead of the
        default provider logic.

        Args:
            factory_fn: callable(organisation, model_name_override=None) -> BaseLLM
        """
        cls._override = factory_fn

    @staticmethod
    def create_llm(organisation, model_name_override=None) -> BaseLLM:
        """
        Returns an LLM instance for a given CoreOrganisation.
        """
        # If a host app registered an override, delegate to it
        if LLMFactory._override:
            return LLMFactory._override(
                organisation, model_name_override=model_name_override
            )

        # Default: read LLMConfiguration, create direct provider
        try:
            config = LLMConfiguration.objects.filter(
                organisation=organisation,
                enabled=True
            ).first()

            if not config:
                raise NoActiveLLMException()

            return LLMFactory._build_llm(config, model_name_override)

        except ObjectDoesNotExist:
            raise NoActiveLLMException("LLM configuration not found.")

    @staticmethod
    def _build_llm(config, model_name_override=None) -> BaseLLM:

        common_params = {
            "api_key": config.api_key,
            "model_name": model_name_override or config.model_name,
            "temperature": config.temperature,
            "max_tokens": config.max_tokens,
            "top_p": config.top_p,
        }

        custom_params = config.custom_parameters or {}

        llm_type = config.llm_type

        if llm_type == "openai":
            from .openai import OpenAILLM
            return OpenAILLM(**common_params, **custom_params)

        elif llm_type == "gemini":
            from .gemini import GeminiLLM
            return GeminiLLM(
                **common_params,
                top_k=config.top_k,
                **custom_params
            )

        elif llm_type == "anthropic":
            from .anthropic import AnthropicLLM
            return AnthropicLLM(
                **common_params,
                top_k=config.top_k,
                **custom_params
            )

        elif llm_type == "ollama":
            from .ollama import OllamaLLM
            return OllamaLLM(**custom_params)

        else:
            raise ValueError(f"Unsupported LLM type: {llm_type}")