"""
Azure OpenAI ChatCompletionClient implementation for AutoGen
"""

import json
import os
import requests
from dataclasses import dataclass
from typing import List, Optional, AsyncGenerator, Any
from autogen_core.models import (
    ChatCompletionClient,
    ModelInfo,
    ModelCapabilities,
    ModelFamily,
    CreateResult,
    RequestUsage,
)


@dataclass
class SimpleLLMMessage:
    role: str
    content: str


class AzureOpenAIChatCompletionClient(ChatCompletionClient):
    """ChatCompletionClient implementation for Azure OpenAI."""
    
    def __init__(
        self,
        deployment: str,
        api_key: str,
        base_url: str,
        api_version: str = "2024-12-01-preview",
        reasoning_effort: Optional[str] = None,
    ):
        """
        Initialize Azure OpenAI client.
        
        Args:
            deployment: Deployment name (e.g., "enmapper-gpt-5.1-codex")
            api_key: Azure OpenAI API key
            base_url: Base URL for Azure OpenAI endpoint
            api_version: API version
            reasoning_effort: Reasoning effort for GPT-5.1-codex ('low', 'medium', 'high')
        """
        self.deployment = deployment
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.api_version = api_version
        self.reasoning_effort = reasoning_effort
        
        self._total_usage = RequestUsage(prompt_tokens=0, completion_tokens=0)
    
    @property
    def model_info(self) -> ModelInfo:
        """Get model information."""
        return ModelInfo(
            id=self.deployment,
            family=ModelFamily.OPENAI,
            capabilities=ModelCapabilities(
                supports_function_calling=True,
                supports_structured_outputs=False,
            ),
        )
    
    @property
    def capabilities(self) -> ModelCapabilities:
        """Get model capabilities."""
        return self.model_info.capabilities
    
    def create(
        self,
        messages: List[SimpleLLMMessage],
        *,
        tools: Optional[List[Any]] = None,
        tool_choice: Optional[Any] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> CreateResult:
        """Create a chat completion."""
        url = f"{self.base_url}/openai/deployments/{self.deployment}/chat/completions"
        headers = {
            "api-key": self.api_key,
            "Content-Type": "application/json",
        }
        params = {
            "api-version": self.api_version,
        }
        
        # Prepare payload
        def normalize(msg):
            if hasattr(msg, "role") and hasattr(msg, "content"):
                return msg.role, msg.content
            if isinstance(msg, dict):
                return msg.get("role"), msg.get("content")
            return str(msg), str(msg)

        payload = {
            "messages": [
                {"role": normalize_msg[0], "content": normalize_msg[1]}
                for normalize_msg in (normalize(msg) for msg in messages)
            ],
        }
        
        # Add optional parameters
        if temperature is not None:
            payload["temperature"] = temperature
        if max_tokens is not None:
            payload["max_completion_tokens"] = max_tokens  # Azure uses max_completion_tokens
        
        # Add reasoning_effort for GPT-5.1-codex
        if self.reasoning_effort:
            payload["reasoning_effort"] = self.reasoning_effort
        
        # Make request (600s timeout for reasoning models which can be slow)
        response = requests.post(url, headers=headers, params=params, json=payload, timeout=600)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            print(f"[AzureOpenAIClient] Request payload: {json.dumps(payload)[:500]}...")
            print(f"[AzureOpenAIClient] Response body: {response.text}")
            raise exc
        data = response.json()
        
        # Extract response
        choice = data["choices"][0]
        message = choice["message"]
        if os.environ.get("AUTOGEN_DEBUG"):
            print("[AzureOpenAIClient] Received choice payload:")
            print(json.dumps(choice, ensure_ascii=False, indent=2)[:3000])
        usage = data.get("usage", {})
        
        # Update total usage
        self._total_usage = RequestUsage(
            prompt_tokens=self._total_usage.prompt_tokens + usage.get("prompt_tokens", 0),
            completion_tokens=self._total_usage.completion_tokens + usage.get("completion_tokens", 0),
        )
        
        # Create result
        result_message = SimpleLLMMessage(
            role=message["role"],
            content=message.get("content", ""),
        )
        
        return CreateResult(
            messages=[result_message],
            usage=RequestUsage(
                prompt_tokens=usage.get("prompt_tokens", 0),
                completion_tokens=usage.get("completion_tokens", 0),
            ),
            finish_reason="stop",
            content=result_message.content,
            cached=False,
        )
    
    def create_stream(
        self,
        messages: List[SimpleLLMMessage],
        *,
        tools: Optional[List[Any]] = None,
        tool_choice: Optional[Any] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> AsyncGenerator[CreateResult, None]:
        """Create a streaming chat completion (not implemented)."""
        # For now, just return non-streaming result
        result = self.create(
            messages,
            tools=tools,
            tool_choice=tool_choice,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )
        yield result
    
    def count_tokens(self, messages: List[SimpleLLMMessage]) -> int:
        """Count tokens (simplified implementation)."""
        # Rough estimation: 4 characters per token
        total_chars = sum(len(str(msg.content)) for msg in messages if hasattr(msg, 'content'))
        return total_chars // 4
    
    @property
    def total_usage(self) -> RequestUsage:
        """Get total usage."""
        return self._total_usage
    
    @property
    def remaining_tokens(self) -> Optional[int]:
        """Get remaining tokens (not applicable for Azure OpenAI)."""
        return None
    
    def actual_usage(self, result: CreateResult) -> RequestUsage:
        """Get actual usage from result."""
        return result.usage
    
    def close(self) -> None:
        """Close the client."""
        pass

