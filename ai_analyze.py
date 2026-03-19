from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

# NOTE: This module is extracted from the old CSV/local-sqlite scripts.
# It is now the single place for AI call + output normalization.

AI_MODE_COMPLETION = "completion"
AI_MODE_RESPONSES = "responses"

DEFAULT_MODEL = os.getenv("AI_MODEL", "openai/gpt-5.2")
DEFAULT_PROMPT_VERSION = os.getenv("AI_PROMPT_VERSION", "weibo_assertions_v1")

DEFAULT_AI_MODE = os.getenv("AI_API_MODE", AI_MODE_RESPONSES)
DEFAULT_AI_TEMPERATURE = float(os.getenv("AI_TEMPERATURE", "0.1"))
DEFAULT_AI_RETRY_COUNT = int(os.getenv("AI_RETRIES", "11"))
DEFAULT_AI_RETRY_BACKOFF_SEC = 2.0
DEFAULT_AI_RETRY_MAX_BACKOFF_SEC = 32.0
DEFAULT_AI_REASONING_EFFORT = os.getenv("AI_REASONING_EFFORT", "xhigh")

ALLOWED_ACTIONS = {
    "trade.buy",
    "trade.add",
    "trade.reduce",
    "trade.sell",
    "trade.hold",
    "trade.watch",
    "view.bullish",
    "view.bearish",
    "valuation.cheap",
    "valuation.expensive",
    "risk.warning",
    "risk.event",
    "education.method",
    "education.mindset",
    "education.life",
}

LEGACY_ACTION_MAP = {
    "buy": "trade.buy",
    "sell": "trade.sell",
    "hold": "trade.hold",
    "risk_warning": "risk.warning",
    "valuation": "valuation.cheap",
    "macro_view": "view.bearish",
    "news_interpretation": "view.bullish",
    "method": "education.method",
}


@dataclass
class AnalyzeResult:
    status: str
    invest_score: float
    assertions: List[Dict[str, Any]]


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\r\n", "\n").strip()


def clamp_float(x: Any, low: float, high: float, default: float) -> float:
    try:
        v = float(x)
    except Exception:
        return default
    if v < low:
        return low
    if v > high:
        return high
    return v


def clamp_int(x: Any, low: int, high: int, default: int) -> int:
    try:
        v = int(x)
    except Exception:
        return default
    if v < low:
        return low
    if v > high:
        return high
    return v


def parse_json_text(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.startswith("json"):
            raw = raw[4:].strip()
    return json.loads(raw)


def _response_attr(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _extract_text_from_response_content(content: Any) -> str:
    parts: List[str] = []

    def walk(node: Any) -> None:
        if node is None:
            return
        if isinstance(node, str):
            text = node.strip()
            if text:
                parts.append(text)
            return
        if isinstance(node, list):
            for item in node:
                walk(item)
            return

        text_value = _response_attr(node, "text")
        if isinstance(text_value, str):
            text = text_value.strip()
            if text:
                parts.append(text)

        content_value = _response_attr(node, "content")
        if content_value is not None and content_value is not node:
            walk(content_value)

    walk(content)
    return "\n".join(parts).strip()


def _extract_ai_text(response: Any, *, _seen_ids: Optional[set[int]] = None) -> str:
    if _seen_ids is None:
        _seen_ids = set()
    current_id = id(response)
    if current_id in _seen_ids:
        return ""
    _seen_ids.add(current_id)

    output_text = _response_attr(response, "output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    nested_response = _response_attr(response, "response")
    if nested_response is not None and nested_response is not response:
        nested_text = _extract_ai_text(nested_response, _seen_ids=_seen_ids)
        if nested_text:
            return nested_text

    output = _response_attr(response, "output")
    extracted_output_text = _extract_text_from_response_content(output)
    if extracted_output_text:
        return extracted_output_text

    choices = _response_attr(response, "choices")
    if isinstance(choices, list) and choices:
        first_choice = choices[0]
        message = _response_attr(first_choice, "message")
        if message is not None:
            content = _response_attr(message, "content")
            extracted_choice_text = _extract_text_from_response_content(content)
            if extracted_choice_text:
                return extracted_choice_text

    if isinstance(response, dict):
        raw_choices = response.get("choices")
        if isinstance(raw_choices, list) and raw_choices:
            first_choice = raw_choices[0]
            if isinstance(first_choice, dict):
                message = first_choice.get("message")
                if isinstance(message, dict):
                    extracted_choice_text = _extract_text_from_response_content(message.get("content"))
                    if extracted_choice_text:
                        return extracted_choice_text
    return ""


def _extract_stream_text_delta(chunk: Any) -> str:
    choices = _response_attr(chunk, "choices")
    if isinstance(choices, list) and choices:
        first_choice = choices[0]
        delta = _response_attr(first_choice, "delta")
        content = _response_attr(delta, "content")
        if isinstance(content, str) and content:
            return content

    event_type = str(_response_attr(chunk, "type") or "").strip().lower()
    delta = _response_attr(chunk, "delta")
    if "delta" in event_type:
        if isinstance(delta, str) and delta:
            return delta
        delta_text = _response_attr(delta, "text")
        if isinstance(delta_text, str) and delta_text:
            return delta_text
        delta_content = _response_attr(delta, "content")
        if isinstance(delta_content, str) and delta_content:
            return delta_content
    return ""


def _collect_streamed_ai_text(stream_response: Any, *, api_mode: str) -> str:
    chunks: List[Any] = []
    text_parts: List[str] = []

    for chunk in stream_response:
        chunks.append(chunk)
        text_delta = _extract_stream_text_delta(chunk)
        if text_delta:
            text_parts.append(text_delta)

    streamed_text = "".join(text_parts).strip()
    if streamed_text:
        return streamed_text

    if api_mode == AI_MODE_COMPLETION and chunks:
        try:
            litellm = _import_litellm()
            rebuilt = litellm.stream_chunk_builder(chunks)
            rebuilt_text = _extract_ai_text(rebuilt)
            if rebuilt_text:
                return rebuilt_text
        except Exception:
            pass

    for chunk in reversed(chunks):
        chunk_text = _extract_ai_text(chunk)
        if chunk_text:
            return chunk_text
    return ""


def _import_litellm():
    try:
        import litellm  # type: ignore
    except Exception as exc:
        raise RuntimeError("litellm_not_installed") from exc

    if hasattr(litellm, "suppress_debug_info"):
        # Prevent LiteLLM from printing "Give Feedback / Get Help" debug info on exceptions.
        litellm.suppress_debug_info = True

    return litellm


def _resolve_litellm_model_name(model_name: str, base_url: str) -> str:
    resolved_model_name = str(model_name or "").strip()
    if not resolved_model_name:
        return ""
    if "/" in resolved_model_name:
        return resolved_model_name
    if str(base_url or "").strip():
        return f"openai/{resolved_model_name}"
    return resolved_model_name


def _mask_secret(value: Any) -> str:
    secret = str(value or "").strip()
    if not secret:
        return ""
    if len(secret) <= 8:
        return "*" * len(secret)
    return f"{secret[:4]}***{secret[-4:]}"


def _compact_text(value: Any) -> str:
    # Keep it one line for logs / db.
    text = clean_text(value)
    return " ".join(text.split())


def _truncate_text(text: str, limit: int) -> str:
    n = max(1, int(limit))
    if len(text) <= n:
        return text
    if n <= 3:
        return text[:n]
    return text[: n - 3] + "..."


def extract_llm_error_details(exc: BaseException) -> Dict[str, Any]:
    """
    Extract JSON-safe error details for tracing.
    Keep this conservative: never raise, never include secrets.
    """
    details: Dict[str, Any] = {}
    try:
        details["type"] = type(exc).__name__
        details["message"] = _compact_text(str(exc))

        status_code = getattr(exc, "status_code", None)
        if status_code is None:
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
        if status_code is not None:
            details["status_code"] = int(status_code)

        provider = getattr(exc, "llm_provider", None) or getattr(exc, "provider", None)
        if provider:
            details["provider"] = _compact_text(provider)

        model = getattr(exc, "model", None)
        if model:
            details["model"] = _compact_text(model)

        request_id = getattr(exc, "request_id", None)
        if not request_id:
            response = getattr(exc, "response", None)
            headers = getattr(response, "headers", None)
            if headers and hasattr(headers, "get"):
                request_id = headers.get("x-request-id") or headers.get("x-requestid")
        if request_id:
            details["request_id"] = _compact_text(request_id)

        code = getattr(exc, "code", None)
        if code:
            details["code"] = _compact_text(code)
        error_type = getattr(exc, "type", None)
        if error_type:
            details["error_type"] = _compact_text(error_type)
        param = getattr(exc, "param", None)
        if param:
            details["param"] = _compact_text(param)

        request = getattr(exc, "request", None)
        if request is not None:
            try:
                details["request_method"] = _compact_text(getattr(request, "method", ""))
                details["request_url"] = _compact_text(str(getattr(request, "url", "")))
            except Exception:
                pass

        litellm_debug_info = getattr(exc, "litellm_debug_info", None)
        if litellm_debug_info:
            # This may contain prompt snippets depending on LiteLLM settings.
            # We only store a short, compact preview.
            details["litellm_debug_info"] = _truncate_text(_compact_text(litellm_debug_info), 400)

        cause_chain: List[Dict[str, str]] = []
        cur: Optional[BaseException] = exc
        seen: set[int] = set()
        for _ in range(4):
            if cur is None:
                break
            next_exc = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
            if next_exc is None:
                break
            obj_id = id(next_exc)
            if obj_id in seen:
                break
            seen.add(obj_id)
            cause_chain.append(
                {
                    "type": type(next_exc).__name__,
                    "message": _truncate_text(_compact_text(str(next_exc)), 500),
                }
            )
            cur = next_exc
        if cause_chain:
            details["cause_chain"] = cause_chain
    except Exception:
        # Never block the caller due to debug extraction.
        return {"type": type(exc).__name__, "message": _compact_text(str(exc))}
    return details


def format_llm_error_one_line(exc: BaseException, *, limit: int = 900) -> str:
    """
    One-line error for log + DB. Example:
    APIError status_code=401 provider=openai model=openai/gpt-5.2 message=...
    """
    try:
        details = extract_llm_error_details(exc)
        parts: List[str] = []
        parts.append(str(details.get("type") or type(exc).__name__))
        for key in ("status_code", "provider", "model", "request_id", "code", "error_type"):
            value = details.get(key)
            if value is None or value == "":
                continue
            parts.append(f"{key}={value}")
        message = details.get("message") or _compact_text(str(exc))
        if message:
            parts.append(f"message={_truncate_text(_compact_text(message), 300)}")
        cause_chain = details.get("cause_chain") or []
        if isinstance(cause_chain, list) and cause_chain:
            cause_items = [item for item in cause_chain if isinstance(item, dict)]
            picked: List[Dict[str, str]] = []
            for item in cause_items:
                msg = _compact_text(item.get("message", ""))
                if msg:
                    picked.append({"type": _compact_text(item.get("type", "")), "message": msg})
            if not picked and cause_items:
                first = cause_items[0]
                picked.append(
                    {
                        "type": _compact_text(first.get("type", "")),
                        "message": _compact_text(first.get("message", "")),
                    }
                )
            if picked:
                parts.append(
                    f"cause={picked[0].get('type','')}: {_truncate_text(_compact_text(picked[0].get('message','')), 200)}"
                )
            if len(picked) >= 2:
                parts.append(
                    f"cause2={picked[1].get('type','')}: {_truncate_text(_compact_text(picked[1].get('message','')), 140)}"
                )
            for item in picked[:2]:
                msg = _compact_text(item.get("message", "")).lower()
                if "<!doctype html" in msg or "<html" in msg:
                    parts.append("hint=base_url_returns_html")
                    break
        return _truncate_text(" ".join(parts), limit)
    except Exception:
        return _truncate_text(f"{type(exc).__name__} message={_compact_text(str(exc))}", limit)


def _append_trace(trace_out: Optional[Path], payload: Dict[str, Any]) -> None:
    if trace_out is None:
        return
    trace_out.parent.mkdir(parents=True, exist_ok=True)
    with trace_out.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def normalize_action(action: str) -> str:
    a = clean_text(action).lower()
    if a in ALLOWED_ACTIONS:
        return a
    if a in LEGACY_ACTION_MAP:
        return LEGACY_ACTION_MAP[a]
    if a.startswith("trade."):
        return a if a in ALLOWED_ACTIONS else "trade.watch"
    if a.startswith("risk."):
        return a if a in ALLOWED_ACTIONS else "risk.warning"
    if a.startswith("valuation."):
        return a if a in ALLOWED_ACTIONS else "valuation.cheap"
    if a.startswith("education."):
        return a if a in ALLOWED_ACTIONS else "education.method"
    if a.startswith("view."):
        return a if a in ALLOWED_ACTIONS else "view.bullish"
    return "view.bullish"


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
            if api_mode == AI_MODE_RESPONSES:
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

            parsed = parse_json_text(raw_text)
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


def analyze_with_litellm(
    api_key: str,
    model: str,
    analysis_context: Dict[str, str],
    row: Dict[str, str],
    base_url: str,
    api_mode: str,
    ai_stream: bool,
    ai_retries: int,
    ai_temperature: float,
    ai_reasoning_effort: str,
    trace_out: Optional[Path],
    timeout_seconds: float = 1000.0,
) -> AnalyzeResult:
    base_url = (base_url or "").rstrip("/")
    prompt = f"""
你是金融内容分析助手。请分析一条微博，输出严格 JSON（不要 Markdown）。

任务:
1) 判断是否为投资相关: status 只能是 "relevant" 或 "irrelevant"
2) 给出 invest_score (0 到 1)
3) 如果 relevant，抽取观点 assertions（0~5 条）

JSON 结构:
{{
  "status": "relevant|irrelevant",
  "invest_score": 0.0,
  "assertions": [
    {{
      "topic_key": "industry:电力 或 stock:601225.SH 等",
      "action": "trade.buy|trade.add|trade.reduce|trade.sell|trade.hold|trade.watch|view.bullish|view.bearish|valuation.cheap|valuation.expensive|risk.warning|risk.event|education.method|education.mindset|education.life",
      "action_strength": 0,
      "summary": "一句话摘要",
      "evidence": "必须是原文片段",
      "source_type": "commentary|extension|forward_only",
      "confidence": 0.0,
      "stock_codes_json": ["600000.SH"],
      "stock_names_json": ["浦发银行"],
      "industries_json": ["银行"],
      "commodities_json": [],
      "indices_json": []
    }}
  ]
}}

要求:
- irrelevant 时 assertions 必须为空数组
- action_strength 为 0~3 的整数
- confidence 为 0~1
- evidence 必须优先来自 commentary_text，不要编造
- 无法确定时给更保守分数并减少 assertions 数量

commentary_text（博主自己的评论段，核心）:
{analysis_context.get("commentary_text","")}

quoted_text（转发/引用上下文）:
{analysis_context.get("quoted_text","")}

补充元信息:
{json.dumps(row, ensure_ascii=False)}
""".strip()

    resolved_api_mode = (api_mode or DEFAULT_AI_MODE).strip().lower()

    trace_label = clean_text(row.get("id", "")) or clean_text(row.get("bid", "")) or "weibo"
    parsed = _call_ai_with_litellm(
        prompt=prompt,
        api_mode=resolved_api_mode,
        ai_stream=ai_stream,
        model_name=model,
        base_url=base_url,
        api_key=api_key,
        timeout_seconds=timeout_seconds,
        retry_count=ai_retries,
        temperature=ai_temperature,
        reasoning_effort=ai_reasoning_effort,
        trace_out=trace_out,
        trace_label=trace_label,
    )

    status = str(parsed.get("status", "irrelevant")).strip().lower()
    if status not in ("relevant", "irrelevant"):
        status = "irrelevant"
    invest_score = clamp_float(parsed.get("invest_score", 0.0), 0.0, 1.0, 0.0)
    assertions = parsed.get("assertions") or []
    if not isinstance(assertions, list):
        assertions = []
    if status == "irrelevant":
        assertions = []

    normalized_assertions: List[Dict[str, Any]] = []
    for a in assertions[:5]:
        if not isinstance(a, dict):
            continue
        source_type = clean_text(a.get("source_type", "commentary")).lower()
        source_type = source_type if source_type in {"commentary", "extension", "forward_only"} else "commentary"
        normalized_assertions.append(
            {
                "topic_key": clean_text(a.get("topic_key", "other:misc")) or "other:misc",
                "action": normalize_action(clean_text(a.get("action", "view.bullish"))),
                "action_strength": clamp_int(a.get("action_strength", 1), 0, 3, 1),
                "summary": clean_text(a.get("summary", "")) or "未提供摘要",
                "evidence": clean_text(a.get("evidence", "")),
                "confidence": clamp_float(a.get("confidence", 0.5), 0.0, 1.0, 0.5),
                "stock_codes_json": json.dumps(a.get("stock_codes_json", []), ensure_ascii=False),
                "stock_names_json": json.dumps(a.get("stock_names_json", []), ensure_ascii=False),
                "industries_json": json.dumps(a.get("industries_json", []), ensure_ascii=False),
                "commodities_json": json.dumps(a.get("commodities_json", []), ensure_ascii=False),
                "indices_json": json.dumps(a.get("indices_json", []), ensure_ascii=False),
                "source_type": source_type,
            }
        )

    return AnalyzeResult(status=status, invest_score=invest_score, assertions=normalized_assertions)


def validate_and_adjust_assertions(
    assertions: List[Dict[str, Any]],
    commentary_text: str,
    quoted_text: str,
) -> List[Dict[str, Any]]:
    commentary = commentary_text or ""
    quoted = quoted_text or ""
    fallback_evidence = commentary[:120] if commentary else (quoted[:120] if quoted else "")

    fixed: List[Dict[str, Any]] = []
    for a in assertions:
        if not isinstance(a, dict):
            continue
        evidence = clean_text(a.get("evidence", ""))
        summary = clean_text(a.get("summary", "未提供摘要")) or "未提供摘要"
        confidence = clamp_float(a.get("confidence", 0.5), 0.0, 1.0, 0.5)
        strength = clamp_int(a.get("action_strength", 1), 0, 3, 1)
        source_type = clean_text(a.get("source_type", "commentary")).lower()
        source_type = source_type if source_type in {"commentary", "extension", "forward_only"} else "commentary"

        if evidence and commentary and evidence in commentary:
            pass
        elif evidence and quoted and evidence in quoted:
            source_type = "forward_only" if source_type == "commentary" else source_type
            confidence = min(confidence, 0.45)
            strength = min(strength, 1)
            summary = f"[转发线索] {summary}"
        else:
            evidence = fallback_evidence
            confidence = min(confidence, 0.4)
            strength = min(strength, 1)
            summary = f"[弱证据] {summary}"

        fixed.append(
            {
                **a,
                "summary": summary,
                "evidence": evidence,
                "confidence": confidence,
                "action_strength": strength,
                "source_type": source_type,
            }
        )
    return fixed
