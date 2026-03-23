from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from alphavault.ai._errors import _append_trace, _mask_secret, extract_llm_error_details
from alphavault.ai._extract import _collect_streamed_ai_text, _extract_ai_text
from alphavault.ai._litellm import _import_litellm, _resolve_litellm_model_name
from alphavault.ai._text import parse_json_text


DEFAULT_AI_RETRY_BACKOFF_SEC = 2.0
DEFAULT_AI_RETRY_MAX_BACKOFF_SEC = 32.0


class AiInvalidJsonError(RuntimeError):
    """
    Raised when the model returns non-JSON text (or JSON parsing fails).

    Keep raw_ai_text for downstream logging (but callers should print only a short tail).
    """

    def __init__(self, message: str, *, raw_ai_text: str) -> None:
        super().__init__(message)
        self.raw_ai_text = str(raw_ai_text or "")


class AiValidationError(RuntimeError):
    """
    Raised when parsed JSON does not pass a caller-provided validator.

    Keep raw_ai_text for downstream logging (but callers should print only a short tail).
    """

    def __init__(self, message: str, *, raw_ai_text: str) -> None:
        super().__init__(message)
        self.raw_ai_text = str(raw_ai_text or "")


def _to_one_line_tail(text: str, *, max_chars: int) -> str:
    s = str(text or "").replace("\r", " ").replace("\n", " ").strip()
    if not s:
        return ""
    if len(s) <= int(max_chars):
        return s
    return s[-int(max_chars) :]


def _call_ai_with_litellm(
    *,
    prompt: str,
    api_mode: str,
    ai_stream: bool,
    model_name: str,
    base_url: str,
    api_key: str,
    timeout_seconds: float,
    retry_count: int,
    temperature: float,
    reasoning_effort: str,
    trace_out: Optional[Path],
    trace_label: str,
    validator: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    litellm = _import_litellm()

    request_model_name = _resolve_litellm_model_name(model_name, base_url)
    last_error: Optional[Exception] = None
    retries = max(0, int(retry_count))
    backoff_sec = DEFAULT_AI_RETRY_BACKOFF_SEC

    for attempt in range(retries + 1):
        raw_text = ""
        try:
            response: Any
            if api_mode == "responses":
                responses_fn = getattr(litellm, "responses", None)
                if not callable(responses_fn):
                    raise RuntimeError("litellm_responses_not_supported")
                call_kwargs: Dict[str, Any] = {
                    "model": request_model_name,
                    "input": prompt,
                    "temperature": float(temperature),
                    "timeout": float(timeout_seconds),
                    "api_key": api_key,
                    "reasoning_effort": str(reasoning_effort),
                    "stream": bool(ai_stream),
                }
                if base_url:
                    call_kwargs["api_base"] = base_url
                response = responses_fn(**call_kwargs)
            else:
                completion_fn = getattr(litellm, "completion", None)
                if not callable(completion_fn):
                    raise RuntimeError("litellm_completion_not_supported")
                call_kwargs = {
                    "model": request_model_name,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": float(temperature),
                    "timeout": float(timeout_seconds),
                    "api_key": api_key,
                    "reasoning_effort": str(reasoning_effort),
                    "stream": bool(ai_stream),
                }
                if base_url:
                    call_kwargs["api_base"] = base_url
                    call_kwargs["base_url"] = base_url
                response = completion_fn(**call_kwargs)

            if ai_stream:
                raw_text = _collect_streamed_ai_text(response, api_mode=api_mode)
            else:
                raw_text = _extract_ai_text(response)

            try:
                parsed = parse_json_text(raw_text)
            except Exception as parse_exc:
                raise AiInvalidJsonError(
                    f"ai_invalid_json:{type(parse_exc).__name__}",
                    raw_ai_text=raw_text,
                ) from parse_exc

            if validator is not None:
                try:
                    validator(parsed)
                except Exception as val_exc:
                    raise AiValidationError(
                        f"ai_invalid_output:{type(val_exc).__name__}:{val_exc}",
                        raw_ai_text=raw_text,
                    ) from val_exc
            _append_trace(
                trace_out,
                {
                    "label": trace_label,
                    "attempt": attempt + 1,
                    "api_mode": api_mode,
                    "model": request_model_name,
                    "base_url": base_url,
                    "stream": bool(ai_stream),
                    "temperature": float(temperature),
                    "reasoning_effort": str(reasoning_effort),
                    "timeout_seconds": float(timeout_seconds),
                    "api_key": _mask_secret(api_key),
                    "prompt_chars": len(prompt),
                    "raw_ai_text": raw_text,
                    "error": "",
                },
            )
            return parsed
        except Exception as exc:
            last_error = exc
            if isinstance(exc, AiValidationError):
                tail = _to_one_line_tail(getattr(exc, "raw_ai_text", ""), max_chars=240)
                msg = str(exc)[:400]
                print(
                    " ".join(
                        [
                            "[llm] validate_failed",
                            f"label={trace_label}",
                            f"attempt={attempt + 1}",
                            f"error={msg}",
                            f"raw_ai_tail={tail}",
                        ]
                    ),
                    flush=True,
                )
            _append_trace(
                trace_out,
                {
                    "label": trace_label,
                    "attempt": attempt + 1,
                    "api_mode": api_mode,
                    "model": request_model_name,
                    "base_url": base_url,
                    "stream": bool(ai_stream),
                    "temperature": float(temperature),
                    "reasoning_effort": str(reasoning_effort),
                    "timeout_seconds": float(timeout_seconds),
                    "api_key": _mask_secret(api_key),
                    "prompt_chars": len(prompt),
                    "raw_ai_text": raw_text,
                    "error": f"{type(exc).__name__}: {exc}",
                    "error_details": extract_llm_error_details(exc),
                },
            )
            if attempt >= retries:
                break
            time.sleep(min(backoff_sec, DEFAULT_AI_RETRY_MAX_BACKOFF_SEC))
            backoff_sec = min(backoff_sec * 2, DEFAULT_AI_RETRY_MAX_BACKOFF_SEC)

    assert last_error is not None
    raise last_error
