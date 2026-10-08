"""Shared OpenRouter settings for DeepSeek-based analysis and corpus generation."""
from __future__ import annotations

import os

CHAT_COMPLETIONS_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "deepseek/deepseek-v4-flash-0731"
DEFAULT_PROVIDER = "deepinfra/fp8"


def openrouter_model() -> str:
    return os.getenv("OPENROUTER_MODEL", "").strip() or DEFAULT_MODEL


def openrouter_options() -> dict:
    provider = {"require_parameters": True, "allow_fallbacks": False}
    route = os.getenv("OPENROUTER_PROVIDER", DEFAULT_PROVIDER).strip()
    if route:
        provider["only"] = [route]
    return {"reasoning": {"enabled": False}, "provider": provider}
