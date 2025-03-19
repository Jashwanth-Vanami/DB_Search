import os
import time
import logging
from abc import ABC, abstractmethod
from typing import Optional
import openai
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

class AIClient(ABC):
    @abstractmethod
    def generate_query(self, prompt: str, max_tokens: int, system_prompt: Optional[str] = None) -> dict:
        pass

class OpenAIClient(AIClient):
    def __init__(self, api_key: str = None):
        openai.api_key = os.getenv("OPENAI_API_KEY", api_key)
        if not openai.api_key:
            raise ValueError("OpenAI API key not found. Please set OPENAI_API_KEY in .env or pass as an argument.")
        logger.info(f"OpenAI API Key loaded: {openai.api_key[:5]}...")

    def generate_query(self, prompt: str, max_tokens: int, system_prompt: Optional[str] = None) -> dict:
        try:
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            else:
                messages.append({"role": "system", "content": "You are a MYSQL expert assistant."})
            messages.append({"role": "user", "content": prompt})
            logger.info(f"Generating query with prompt: {prompt}")
            start_time = time.time()
            response = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=messages,
                max_tokens=max_tokens
            )
            latency = time.time() - start_time
            generated = response.choices[0].message.content.strip()
            usage = response.get("usage", {})  # Contains prompt_tokens, completion_tokens, total_tokens
            logger.info(f"Generated query: {generated} (latency: {latency:.2f}s, usage: {usage})")
            return {"query": generated, "latency": latency, "usage": usage}
        except Exception as e:
            logger.error(f"OpenAI API error: {str(e)}")
            raise

class DeepSeaClient(AIClient):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def generate_query(self, prompt: str, max_tokens: int, system_prompt: Optional[str] = None) -> dict:
        try:
            logger.info("DeepSeaClient generate_query called")
            # Return a dummy response with zero latency and no usage info.
            return {"query": "SELECT * FROM mock_data", "latency": 0.0, "usage": {}}
        except Exception as e:
            logger.error(f"DeepSea API error: {str(e)}")
            raise

class QwenClient(AIClient):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def generate_query(self, prompt: str, max_tokens: int, system_prompt: Optional[str] = None) -> dict:
        try:
            logger.info("QwenClient generate_query called")
            return {"query": "SELECT * FROM sample_table", "latency": 0.0, "usage": {}}
        except Exception as e:
            logger.error(f"Qwen API error: {str(e)}")
            raise