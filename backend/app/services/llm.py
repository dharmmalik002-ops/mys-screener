"""Claude adapter that keeps the call shape the AI services were written against.

The services call ``client.models.generate_content(model=..., contents=..., config=...)``
and read ``response.text``. ``LLMClient`` offers exactly that on top of the Anthropic
Messages API, so swapping providers did not mean rewriting a dozen call sites.

Provider order: Claude first; if the call fails for any reason (quota, outage, bad key,
refusal) and a Gemini key is configured, the same request is re-run on Gemini.

Notes on the model: Claude Sonnet 5.5 / Opus 5.5 reject non-default ``temperature``, so
``GenConfig.temperature`` is accepted for source compatibility and not sent; depth is
controlled by ``effort`` instead. ``max_output_tokens`` is raised to leave room for that
reasoning before the visible answer.
"""
from __future__ import annotations

import base64
import json
import logging
import os
from dataclasses import dataclass
from typing import Any

DEFAULT_MODEL = os.getenv("SCANNER_CLAUDE_MODEL", "claude-sonnet-5-5")
DEFAULT_EFFORT = os.getenv("SCANNER_CLAUDE_EFFORT", "high")
GEMINI_MODELS = ["gemini-2.5-flash", "gemini-2.5-flash-lite"]
# Callers loop over MODELS; provider fallback (Claude -> Gemini) happens inside the client.
MODELS = [DEFAULT_MODEL]
_THINKING_HEADROOM = 4096
logger = logging.getLogger(__name__)


@dataclass
class GenConfig:
    temperature: float | None = None  # accepted, not sent (see module docstring)
    max_output_tokens: int = 4096
    response_mime_type: str | None = None
    response_schema: Any = None  # a pydantic model class


@dataclass
class ImagePart:
    data: bytes
    mime_type: str = "image/png"


@dataclass
class LLMResponse:
    text: str


def _strip_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
    return text


def _claude_generate(client: Any, model: str, contents: Any, config: GenConfig) -> str:
    parts = contents if isinstance(contents, list) else [contents]
    blocks: list[dict[str, Any]] = []
    for part in parts:
        if isinstance(part, ImagePart):
            blocks.append({
                "type": "image",
                "source": {"type": "base64", "media_type": part.mime_type, "data": base64.standard_b64encode(part.data).decode()},
            })
        else:
            blocks.append({"type": "text", "text": str(part)})

    system = None
    wants_json = config.response_mime_type == "application/json" or config.response_schema is not None
    if wants_json:
        system = "Reply with a single valid JSON object only. No markdown, no code fences, no commentary."
        if config.response_schema is not None:
            system += " It must validate against this JSON Schema: " + json.dumps(config.response_schema.model_json_schema())

    kwargs: dict[str, Any] = {
        "model": model,
        "max_tokens": int(config.max_output_tokens) + _THINKING_HEADROOM,
        "messages": [{"role": "user", "content": blocks}],
        "output_config": {"effort": DEFAULT_EFFORT},
    }
    if system:
        kwargs["system"] = system
    message = client.messages.create(**kwargs)
    if message.stop_reason == "refusal":
        raise RuntimeError("Claude declined this request")
    text = "".join(b.text for b in message.content if b.type == "text")
    return _strip_fences(text) if wants_json else text


def _gemini_generate(client: Any, contents: Any, config: GenConfig) -> str:
    from google.genai import types

    parts = contents if isinstance(contents, list) else [contents]
    gparts = [
        types.Part.from_bytes(data=p.data, mime_type=p.mime_type) if isinstance(p, ImagePart) else str(p)
        for p in parts
    ]
    kw: dict[str, Any] = {"max_output_tokens": config.max_output_tokens}
    if config.temperature is not None:
        kw["temperature"] = config.temperature
    if config.response_mime_type:
        kw["response_mime_type"] = config.response_mime_type
    if config.response_schema is not None:
        kw["response_schema"] = config.response_schema
    last: Exception | None = None
    for name in GEMINI_MODELS:
        try:
            resp = client.models.generate_content(model=name, contents=gparts, config=types.GenerateContentConfig(**kw))
            if resp.text:
                return resp.text
        except Exception as exc:  # try the next Gemini model
            last = exc
    raise last or RuntimeError("Gemini returned no text")


class _Models:
    def __init__(self, owner: "LLMClient") -> None:
        self._owner = owner

    def generate_content(self, *, model: str, contents: Any, config: GenConfig | None = None) -> LLMResponse:
        config = config or GenConfig()
        owner = self._owner
        claude_error: Exception | None = None
        if owner._claude is not None:
            try:
                return LLMResponse(text=_claude_generate(owner._claude, model, contents, config))
            except Exception as exc:
                claude_error = exc
                if owner._gemini is None:
                    raise
                logger.warning("Claude call failed (%s); falling back to Gemini", exc)
        if owner._gemini is None:
            raise RuntimeError("No AI provider is configured")
        try:
            return LLMResponse(text=_gemini_generate(owner._gemini, contents, config))
        except Exception as exc:
            if claude_error is not None:
                raise RuntimeError(f"Claude failed ({claude_error}); Gemini failed ({exc})") from exc
            raise


class LLMClient:
    """Claude first, Gemini as the fallback. Either key may be absent, not both."""

    def __init__(self, api_key: str | None = None, gemini_api_key: str | None = None) -> None:
        self._claude = None
        self._gemini = None
        if api_key:
            import anthropic

            # A key that is not scoped to one workspace needs the workspace named on every call.
            from app.core.config import get_settings

            workspace = (os.getenv("ANTHROPIC_WORKSPACE_ID") or get_settings().anthropic_workspace_id or "").strip()
            self._claude = anthropic.Anthropic(
                api_key=api_key,
                max_retries=2,
                timeout=120.0,
                default_headers={"anthropic-workspace-id": workspace} if workspace else None,
            )
        if gemini_api_key:
            from google import genai

            self._gemini = genai.Client(api_key=gemini_api_key)
        if self._claude is None and self._gemini is None:
            raise ValueError("LLMClient needs an Anthropic or Gemini API key")
        self.models = _Models(self)
