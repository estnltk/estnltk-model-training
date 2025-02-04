import openai

from dataclasses import dataclass
from configparser import ConfigParser

from typing import Optional


@dataclass
class GptConfig:
    deployment_id: str
    model_name: str


def configure_azure_model(config: ConfigParser) -> GptConfig:
    """
    Uses config to set up OpenAI models through Azure interface
    Sets various properties and keys in openai library.
    Returns configuration for Azure GPT model that will be used
    """
    if not config.has_section('azure-configuration'):
        raise ValueError("Configuration does not have section 'azure-configuration'")
    if not config.has_option('azure-configuration', 'api_type'):
        raise ValueError("Section 'azure-configuration' does not have the key 'api_type'")
    if not config.has_option('azure-configuration', 'api_key'):
        raise ValueError("Section 'azure-configuration' does not have the key 'api_key'")
    if not config.has_option('azure-configuration', 'api_base'):
        raise ValueError("Section 'azure-configuration' does not have the key 'api_base'")
    if not config.has_option('azure-configuration', 'api_version'):
        raise ValueError("Section 'azure-configuration' does not have the key 'api_version'")
    if not config.has_option('azure-configuration', 'deployment_id'):
        raise ValueError("Section 'azure-configuration' does not have the key 'deployment_id'")
    if not config.has_option('azure-configuration', 'model'):
        raise ValueError("Section 'azure-configuration' does not have the key 'model'")

    openai.api_type = config['azure-configuration']['api_type']
    openai.api_key = config['azure-configuration']['api_key']
    openai.api_base = config['azure-configuration']['api_base']
    openai.api_version = config['azure-configuration']['api_version']

    return GptConfig(
        deployment_id=config['azure-configuration']['deployment_id'],
        model_name=config['azure-configuration']['model'])


def ask_openai(conf: GptConfig,
               prompt: str, system_prompt: Optional[str] = None,
               temperature: Optional[float] = None,
               max_tokens: Optional[int] = None,
               **kwargs
               ) -> str:
    """
    Wrapper that hides boilerplate code from queries to OpenAI models.
    The function exposes basic turning knobs in OpenAI models.
    More refined parameters (top_p, frequency_penalty, presence_penalty, stop) can be passed as kwargs.
    """
    if system_prompt is not None:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}]
    else:
        messages = [{"role": "user", "content": prompt}]

    response = None
    try:
        if temperature is None and max_tokens is None:
            response = openai.ChatCompletion.create(
                deployment_id=conf.deployment_id,
                model=conf.model_name,
                messages=messages,
                **kwargs
            )
        elif temperature is not None and max_tokens is None:
            response = openai.ChatCompletion.create(
                deployment_id=conf.deployment_id,
                model=conf.model_name,
                messages=messages,
                temperature=temperature,
                **kwargs
            )
        elif temperature is None and max_tokens is not None:
            response = openai.ChatCompletion.create(
                deployment_id=conf.deployment_id,
                model=conf.model_name,
                messages=messages,
                max_tokens=max_tokens,
                **kwargs
            )
        elif temperature is not None and max_tokens is not None:
            response = openai.ChatCompletion.create(
                deployment_id=conf.deployment_id,
                model=conf.model_name,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                **kwargs
            )
    except Exception as e:
        print(e)
        return 'FAIL'
    return response['choices'][0]['message']['content']