import argparse
import csv
import json
import os
import re
import sqlite3
import time
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

warnings.filterwarnings(
    "ignore",
    message=r"^Pydantic serializer warnings:",
    category=UserWarning,
    module=r"pydantic\.main",
)


def raise_for_status_with_body(resp: requests.Response, label: str) -> None:
    """
    Improve error visibility: include status code, url, and a short response body snippet.
    """
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        body = resp.text
        snippet = body[:500].replace("\n", " ").replace("\r", " ")
        raise RuntimeError(f"{label}_http_{resp.status_code}: {resp.url} body_snippet={snippet}") from e


def json_or_error(resp: requests.Response, label: str) -> Any:
    """Parse response JSON; if body为空或不是合法 JSON，则携带片段报错，避免 JSONDecodeError 看不出内容。"""
    text = resp.text
    if not text or not text.strip():
        raise RuntimeError(f"{label}_empty_body: {resp.url} status={resp.status_code}")
    try:
        return resp.json()
    except Exception as e:
        snippet = text[:500].replace("\n", " ").replace("\r", " ")
        raise RuntimeError(f"{label}_invalid_json: {resp.url} body_snippet={snippet}") from e


def post_with_retry(
    url: str,
    *,
    headers: Optional[Dict[str, str]],
    json_payload: Dict[str, Any],
    timeout: float,
    label: str,
    retry_statuses: set[int] = {429, 500, 503},
    retry_body_substrings: tuple[str, ...] = ("No available accounts", "All accounts unavailable"),
    max_retries: int = 6,
) -> requests.Response:
    """POST with simple exponential backoff for transient status codes."""
    backoffs = [1, 2, 4, 8, 16, 32]
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, headers=headers, json=json_payload, timeout=timeout)
            body_snippet = resp.text[:300]
            if resp.status_code in retry_statuses or any(s in body_snippet for s in retry_body_substrings):
                last_exc = RuntimeError(
                    f"{label}_retryable_{resp.status_code}: {resp.url} body_snippet={body_snippet}"
                )
            else:
                raise_for_status_with_body(resp, label)
                return resp
        except requests.RequestException as e:
            last_exc = e
        if attempt < max_retries - 1:
            time.sleep(backoffs[min(attempt, len(backoffs) - 1)])
            continue
        if last_exc:
            raise last_exc
    # Should not reach here
    raise RuntimeError(f"{label}_retry_exhausted")

litellm_import_error = None
try:
    import litellm  # type: ignore
except ImportError as e:
    litellm = None
    litellm_import_error = e
except Exception as e:
    raise RuntimeError(f"litellm_import_failed: {type(e).__name__}: {e}")

from init_workdb import init_workdb


DEFAULT_CSV_PATH = Path("weibo/挖地瓜的超级鹿鼎公/3962719063_14-17.csv")
DEFAULT_DB_PATH = Path("workdb.sqlite")
DEFAULT_AUTHOR = "挖地瓜的超级鹿鼎公"
DEFAULT_USER_ID = "3962719063"
DEFAULT_MODEL = os.getenv("AI_MODEL") or os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")
DEFAULT_PROMPT_VERSION = "weibo_assertions_v1"
DEFAULT_API_STYLE = os.getenv("AI_API_STYLE") or os.getenv("GEMINI_API_STYLE", "litellm")
DEFAULT_BASE_URL = os.getenv("AI_BASE_URL") or os.getenv("GEMINI_BASE_URL", "https://elysiver.h-e.top")
AI_MODE_COMPLETION = "completion"
AI_MODE_RESPONSES = "responses"
DEFAULT_AI_MODE = os.getenv("AI_API_MODE", AI_MODE_RESPONSES)
DEFAULT_AI_TEMPERATURE = float(os.getenv("AI_TEMPERATURE", "0.1"))
DEFAULT_AI_RETRY_COUNT = int(os.getenv("AI_RETRIES", "3"))
DEFAULT_AI_RETRY_BACKOFF_SEC = 2.0
DEFAULT_AI_RETRY_MAX_BACKOFF_SEC = 32.0
DEFAULT_AI_REASONING_EFFORT = os.getenv("AI_REASONING_EFFORT", "xhigh")
DEFAULT_AI_MAX_INFLIGHT = int(os.getenv("AI_MAX_INFLIGHT", "32"))
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


def now_str() -> str:
    # Use China Standard Time (UTC+8) for stored timestamps to match user expectation.
    cst = timezone(timedelta(hours=8))
    return datetime.now(cst).strftime("%Y-%m-%d %H:%M:%S")


def normalize_datetime(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return now_str()
    return v.replace("T", " ")


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("\r\n", "\n").strip()


def split_commentary_and_quoted(body: str) -> tuple[str, str]:
    text = clean_text(body)
    marker = "//@"
    idx = text.find(marker)
    if idx < 0:
        return text, ""
    commentary = text[:idx].strip()
    quoted = text[idx:].strip()
    return commentary, quoted


def build_raw_text(row: Dict[str, str]) -> str:
    body = clean_text(row.get("正文", ""))
    source_body = clean_text(row.get("源微博正文", ""))
    topic = clean_text(row.get("话题", ""))
    at_users = clean_text(row.get("@用户", ""))

    parts: List[str] = []
    if body:
        parts.append(body)

    meta_lines: List[str] = []
    if topic:
        meta_lines.append(f"话题: {topic}")
    if at_users:
        meta_lines.append(f"@用户: {at_users}")
    if meta_lines:
        parts.append("[微博元信息]\n" + "\n".join(meta_lines))

    if source_body:
        parts.append("[转发原文]\n" + source_body)

    # posts 表没有为 CSV 的每一列单独建字段，这里保留整行 JSON 便于后续追溯。
    parts.append("[CSV原始字段]\n" + json.dumps(row, ensure_ascii=False))

    return "\n\n".join(parts).strip()


def build_analysis_context(row: Dict[str, str]) -> Dict[str, str]:
    body = clean_text(row.get("正文", ""))
    source_body = clean_text(row.get("源微博正文", ""))
    commentary_text, quoted_inline = split_commentary_and_quoted(body)

    quoted_parts = []
    if quoted_inline:
        quoted_parts.append(quoted_inline)
    if source_body:
        quoted_parts.append(source_body)
    quoted_text = "\n".join([x for x in quoted_parts if x]).strip()

    return {
        "commentary_text": commentary_text,
        "quoted_text": quoted_text,
        "full_text": body,
        "source_text": source_body,
    }


def build_url(row: Dict[str, str], user_id: str) -> str:
    article_url = clean_text(row.get("头条文章url", ""))
    if article_url:
        return article_url
    bid = clean_text(row.get("bid", ""))
    mid = clean_text(row.get("id", "")).strip()
    if bid:
        return f"https://weibo.com/{user_id}/{bid}"
    return f"https://weibo.com/{user_id}/{mid}"


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
    if not raw:
        raise RuntimeError("ai_empty_text")

    def try_load(candidate: str) -> Optional[Dict[str, Any]]:
        c = (candidate or "").strip()
        if not c:
            return None
        try:
            obj = json.loads(c)
        except Exception:
            return None
        return obj if isinstance(obj, dict) else None

    # 1) Fast path: strict JSON
    parsed = try_load(raw)
    if parsed is not None:
        return parsed

    # 2) Common: fenced code block anywhere
    match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", raw, flags=re.IGNORECASE)
    if match:
        parsed = try_load(match.group(1))
        if parsed is not None:
            return parsed

    # 3) Best-effort: find the first JSON object in a noisy text
    decoder = json.JSONDecoder()
    start = raw.find("{")
    while start != -1:
        try:
            obj, _end = decoder.raw_decode(raw[start:])
        except json.JSONDecodeError:
            start = raw.find("{", start + 1)
            continue
        if isinstance(obj, dict):
            return obj
        start = raw.find("{", start + 1)

    snippet = raw[:500].replace("\n", " ").replace("\r", " ")
    raise RuntimeError(f"ai_invalid_json: body_snippet={snippet}")


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
                    extracted_choice_text = _extract_text_from_response_content(
                        message.get("content")
                    )
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
            import litellm

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


def _resolve_litellm_model_name(model_name: str, api_type: str, base_url: str) -> str:
    resolved_model_name = str(model_name or "").strip()
    if not resolved_model_name:
        return ""
    if "/" in resolved_model_name:
        return resolved_model_name
    if str(api_type or "").strip() == "openai" and str(base_url or "").strip():
        return f"openai/{resolved_model_name}"
    return resolved_model_name


def _mask_secret(value: Any) -> str:
    secret = str(value or "").strip()
    if not secret:
        return ""
    if len(secret) <= 8:
        return "*" * len(secret)
    return f"{secret[:4]}***{secret[-4:]}"


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
    api_type: str,
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
    try:
        import litellm  # type: ignore
    except Exception as exc:
        raise RuntimeError("litellm_not_installed") from exc

    request_model_name = _resolve_litellm_model_name(model_name, api_type, base_url)
    last_error: Optional[Exception] = None
    retries = max(0, int(retry_count))
    backoff_sec = DEFAULT_AI_RETRY_BACKOFF_SEC

    for attempt in range(retries + 1):
        raw_text = ""
        try:
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
                if api_type:
                    call_kwargs["custom_llm_provider"] = api_type
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
                if api_type:
                    call_kwargs["custom_llm_provider"] = api_type
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
                    "api_type": api_type,
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
                    "api_type": api_type,
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
                },
            )
            if attempt >= retries:
                break
            time.sleep(min(backoff_sec, DEFAULT_AI_RETRY_MAX_BACKOFF_SEC))
            backoff_sec = min(backoff_sec * 2, DEFAULT_AI_RETRY_MAX_BACKOFF_SEC)

    assert last_error is not None
    raise last_error


def analyze_with_gemini(
    api_key: str,
    model: str,
    analysis_context: Dict[str, str],
    row: Dict[str, str],
    api_style: str,
    base_url: str,
    litellm_provider: str,
    gemini_auth: str,
    api_type: str,
    api_mode: str,
    ai_stream: bool,
    ai_retries: int,
    ai_temperature: float,
    ai_reasoning_effort: str,
    trace_out: Optional[Path],
    timeout_seconds: int = 60,
) -> AnalyzeResult:
    base_url = base_url.rstrip("/")
    style = (api_style or "gemini").strip().lower()
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
{analysis_context["commentary_text"]}

quoted_text（转发/引用上下文）:
{analysis_context["quoted_text"]}

补充元信息:
{json.dumps(row, ensure_ascii=False)}
""".strip()

    resolved_api_type = clean_text(api_type)
    if not resolved_api_type and style == "litellm":
        resolved_api_type = clean_text(litellm_provider) or "gemini"
    resolved_api_mode = (api_mode or DEFAULT_AI_MODE).strip().lower()

    if resolved_api_type:
        trace_label = clean_text(row.get("id", "")) or clean_text(row.get("bid", "")) or "weibo"
        parsed = _call_ai_with_litellm(
            prompt=prompt,
            api_type=resolved_api_type,
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
    elif style == "openai":
        endpoint = f"{base_url}/chat/completions"
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        resp = post_with_retry(
            endpoint,
            headers=headers,
            json_payload=payload,
            timeout=timeout_seconds,
            label="openai",
        )
        data = json_or_error(resp, "openai")
        choices = data.get("choices") or []
        if not choices:
            raise RuntimeError(f"openai_empty_choices: {data}")
        message = choices[0].get("message", {})
        text = message.get("content", "")
        if isinstance(text, list):
            text = "".join(
                p.get("text", "") for p in text if isinstance(p, dict)
            )
        if not str(text).strip():
            raise RuntimeError(f"openai_empty_content: {data}")
        parsed = parse_json_text(text)
    else:
        if gemini_auth == "bearer":
            endpoint = f"{base_url}/models/{model}:generateContent"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
        else:
            endpoint = f"{base_url}/models/{model}:generateContent?key={api_key}"
            headers = None
        payload = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "responseMimeType": "application/json",
            },
        }
        resp = post_with_retry(
            endpoint,
            headers=headers,
            json_payload=payload,
            timeout=timeout_seconds,
            label="gemini",
        )
        data = json_or_error(resp, "gemini")
        candidates = data.get("candidates") or []
        if not candidates:
            raise RuntimeError(f"gemini_empty_candidates: {data}")

        parts = candidates[0].get("content", {}).get("parts", [])
        text = ""
        for p in parts:
            if "text" in p:
                text += p["text"]
        if not text.strip():
            raise RuntimeError(f"gemini_empty_text: {data}")

        parsed = parse_json_text(text)
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
        ev = clean_text(a.get("evidence", ""))
        summary = clean_text(a.get("summary", "未提供摘要")) or "未提供摘要"
        confidence = clamp_float(a.get("confidence", 0.5), 0.0, 1.0, 0.5)
        strength = clamp_int(a.get("action_strength", 1), 0, 3, 1)
        source_type = clean_text(a.get("source_type", "commentary")).lower()
        source_type = source_type if source_type in {"commentary", "extension", "forward_only"} else "commentary"

        if ev and commentary and ev in commentary:
            pass
        elif ev and quoted and ev in quoted:
            source_type = "forward_only" if source_type == "commentary" else source_type
            confidence = min(confidence, 0.45)
            strength = min(strength, 1)
            summary = f"[转发线索] {summary}"
        else:
            ev = fallback_evidence
            confidence = min(confidence, 0.4)
            strength = min(strength, 1)
            summary = f"[弱证据] {summary}"

        fixed.append(
            {
                **a,
                "summary": summary,
                "evidence": ev,
                "confidence": confidence,
                "action_strength": strength,
                "source_type": source_type,
            }
        )
    return fixed


def upsert_post_base(
    conn: sqlite3.Connection,
    *,
    post_uid: str,
    platform_post_id: str,
    author: str,
    created_at: str,
    url: str,
    raw_text: str,
) -> None:
    conn.execute(
        """
        INSERT INTO posts (
            post_uid, platform, platform_post_id, author, created_at, url, raw_text, ingested_at
        ) VALUES (?, 'weibo', ?, ?, ?, ?, ?, ?)
        ON CONFLICT(post_uid) DO UPDATE SET
            platform_post_id=excluded.platform_post_id,
            author=excluded.author,
            created_at=excluded.created_at,
            url=excluded.url,
            raw_text=excluded.raw_text
        """,
        (post_uid, platform_post_id, author, created_at, url, raw_text, now_str()),
    )


def mark_processing(conn: sqlite3.Connection, post_uid: str, model: str, prompt_version: str) -> int:
    row = conn.execute(
        "SELECT attempts FROM posts WHERE post_uid = ?",
        (post_uid,),
    ).fetchone()
    attempts = int(row[0] or 0) + 1 if row else 1
    conn.execute(
        """
        UPDATE posts
        SET status='processing',
            attempts=?,
            processed_at=NULL,
            last_error=NULL,
            next_retry_at=NULL,
            model=?,
            prompt_version=?
        WHERE post_uid=?
        """,
        (attempts, model, prompt_version, post_uid),
    )
    return attempts


def mark_error(conn: sqlite3.Connection, post_uid: str, error: str, attempts: int) -> None:
    retry_minutes = min(60, 2 ** min(attempts, 6))
    cst = timezone(timedelta(hours=8))
    next_retry = (datetime.now(cst) + timedelta(minutes=retry_minutes)).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        """
        UPDATE posts
        SET status='error',
            last_error=?,
            next_retry_at=?,
            processed_at=?
        WHERE post_uid=?
        """,
        (error[:1000], next_retry, now_str(), post_uid),
    )


def write_assertions(conn: sqlite3.Connection, post_uid: str, assertions: List[Dict[str, Any]]) -> None:
    conn.execute("DELETE FROM assertions WHERE post_uid = ?", (post_uid,))
    for idx, a in enumerate(assertions, start=1):
        conn.execute(
            """
            INSERT INTO assertions (
                post_uid, idx, topic_key, action, action_strength, summary, evidence, confidence,
                stock_codes_json, stock_names_json, industries_json, commodities_json, indices_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                post_uid,
                idx,
                a["topic_key"],
                a["action"],
                a["action_strength"],
                a["summary"],
                a["evidence"],
                a["confidence"],
                a["stock_codes_json"],
                a["stock_names_json"],
                a["industries_json"],
                a["commodities_json"],
                a["indices_json"],
            ),
        )


def mark_done(
    conn: sqlite3.Connection,
    post_uid: str,
    result: AnalyzeResult,
    model: str,
    prompt_version: str,
) -> None:
    conn.execute(
        """
        UPDATE posts
        SET status=?,
            invest_score=?,
            processed_at=?,
            last_error=NULL,
            next_retry_at=NULL,
            model=?,
            prompt_version=?
        WHERE post_uid=?
        """,
        (
            result.status,
            result.invest_score,
            now_str(),
            model,
            prompt_version,
            post_uid,
        ),
    )


def should_skip(conn: sqlite3.Connection, post_uid: str, reprocess: bool) -> bool:
    if reprocess:
        return False
    row = conn.execute(
        "SELECT status FROM posts WHERE post_uid = ?",
        (post_uid,),
    ).fetchone()
    if not row:
        return False
    return row[0] in ("relevant", "irrelevant")


def process_csv(
    csv_path: Path,
    db_path: Path,
    api_key: str,
    model: str,
    prompt_version: str,
    author: str,
    user_id: str,
    limit: Optional[int],
    sleep_seconds: float,
    reprocess: bool,
    relevant_threshold: float,
    api_style: str,
    base_url: str,
    litellm_provider: str,
    gemini_auth: str,
    api_type: str,
    api_mode: str,
    ai_stream: bool,
    ai_retries: int,
    ai_temperature: float,
    ai_reasoning_effort: str,
    ai_rpm: float,
    ai_max_inflight: int,
    trace_out: Optional[Path],
    timeout_seconds: float,
    progress_every: int,
    verbose: bool,
) -> None:
    init_workdb(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        processed = 0
        skipped = 0
        failed = 0
        total = 0
        last_ai_call_ts: Optional[float] = None
        min_ai_interval = 60.0 / ai_rpm if ai_rpm and ai_rpm > 0 else 0.0

        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total += 1
                mid = clean_text(row.get("id", "")).strip()
                if not mid:
                    skipped += 1
                    continue
                post_uid = f"weibo:{mid}"
                if should_skip(conn, post_uid, reprocess):
                    skipped += 1
                    continue

                platform_post_id = mid
                created_at = normalize_datetime(
                    clean_text(row.get("完整日期", "")) or clean_text(row.get("日期", ""))
                )
                url = build_url(row, user_id=user_id)
                raw_text = build_raw_text(row)
                analysis_context = build_analysis_context(row)
                if not raw_text:
                    skipped += 1
                    continue

                upsert_post_base(
                    conn,
                    post_uid=post_uid,
                    platform_post_id=platform_post_id,
                    author=author,
                    created_at=created_at,
                    url=url,
                    raw_text=raw_text,
                )
                attempts = mark_processing(conn, post_uid, model=model, prompt_version=prompt_version)
                conn.commit()

                try:
                    if verbose:
                        print(f"[{total}] call_api {post_uid}", flush=True)
                    if min_ai_interval > 0 and last_ai_call_ts is not None:
                        since_last = time.time() - last_ai_call_ts
                        if since_last < min_ai_interval:
                            time.sleep(min_ai_interval - since_last)
                    last_ai_call_ts = time.time()
                    start_ts = time.time()
                    try:
                        result = analyze_with_gemini(
                            api_key=api_key,
                            model=model,
                            analysis_context=analysis_context,
                            row=row,
                            api_style=api_style,
                            base_url=base_url,
                            litellm_provider=litellm_provider,
                            gemini_auth=gemini_auth,
                            api_type=api_type,
                            api_mode=api_mode,
                            ai_stream=ai_stream,
                            ai_retries=ai_retries,
                            ai_temperature=ai_temperature,
                            ai_reasoning_effort=ai_reasoning_effort,
                            trace_out=trace_out,
                            timeout_seconds=timeout_seconds,
                        )
                    except Exception as e:
                        print(
                            f"[{total}] llm_error {post_uid} {type(e).__name__}: {e}",
                            flush=True,
                        )
                        raise
                    if verbose:
                        cost = time.time() - start_ts
                        print(
                            f"[{total}] done {post_uid} status={result.status} score={result.invest_score:.3f} cost={cost:.1f}s",
                            flush=True,
                        )
                    if result.invest_score < relevant_threshold:
                        result = AnalyzeResult(status="irrelevant", invest_score=result.invest_score, assertions=[])
                    else:
                        result.assertions = validate_and_adjust_assertions(
                            result.assertions,
                            commentary_text=analysis_context["commentary_text"],
                            quoted_text=analysis_context["quoted_text"],
                        )
                    write_assertions(conn, post_uid, result.assertions)
                    mark_done(conn, post_uid, result, model=model, prompt_version=prompt_version)
                    conn.commit()
                    processed += 1
                except Exception as e:
                    failed += 1
                    print(
                        f"[{total}] error {post_uid} {type(e).__name__}: {e}",
                        flush=True,
                    )
                    mark_error(conn, post_uid, f"ai:{type(e).__name__}: {e}", attempts)
                    conn.commit()

                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
                if limit is not None and processed >= limit:
                    break
                if progress_every > 0 and total % progress_every == 0:
                    print(
                        f"[{total}] progress processed={processed} skipped={skipped} failed={failed}",
                        flush=True,
                    )

        print(
            json.dumps(
                {
                    "csv_total_rows": total,
                    "processed": processed,
                    "skipped": skipped,
                    "failed": failed,
                    "db": str(db_path.resolve()),
                    "model": model,
                    "prompt_version": prompt_version,
                    "api_style": api_style,
                    "base_url": base_url,
                    "api_type": api_type,
                    "api_mode": api_mode,
                    "ai_stream": ai_stream,
                    "ai_retries": ai_retries,
                    "ai_reasoning_effort": ai_reasoning_effort,
                    "ai_rpm": ai_rpm,
                    "ai_max_inflight": ai_max_inflight,
                    "trace_out": str(trace_out) if trace_out else "",
                },
                ensure_ascii=False,
            )
        )
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse Weibo CSV, call Gemini Flash tagging, write posts/assertions into SQLite workdb."
    )
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV_PATH)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--author", default=DEFAULT_AUTHOR)
    parser.add_argument("--user-id", default=DEFAULT_USER_ID)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--api-style", default=DEFAULT_API_STYLE, choices=["gemini", "openai", "litellm"])
    parser.add_argument("--api-type", default=None)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--api-key-env", default=None)
    parser.add_argument(
        "--api-mode",
        default=DEFAULT_AI_MODE,
        choices=[AI_MODE_COMPLETION, AI_MODE_RESPONSES],
    )
    parser.add_argument("--ai-stream", action="store_true")
    parser.add_argument("--litellm-provider", default=os.getenv("AI_API_TYPE") or os.getenv("LITELLM_PROVIDER", "gemini"))
    parser.add_argument("--gemini-auth", default=os.getenv("GEMINI_AUTH", "query"), choices=["query", "bearer"])
    parser.add_argument("--prompt-version", default=DEFAULT_PROMPT_VERSION)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--relevant-threshold", type=float, default=0.35)
    parser.add_argument("--reprocess", action="store_true")
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--ai-timeout-sec", type=float, default=None)
    parser.add_argument("--ai-retries", type=int, default=DEFAULT_AI_RETRY_COUNT)
    parser.add_argument("--ai-temperature", type=float, default=DEFAULT_AI_TEMPERATURE)
    parser.add_argument(
        "--ai-reasoning-effort",
        default=DEFAULT_AI_REASONING_EFFORT,
        choices=["none", "minimal", "low", "medium", "high", "xhigh"],
    )
    parser.add_argument("--ai-rpm", type=float, default=0.0)
    parser.add_argument("--ai-max-inflight", type=int, default=DEFAULT_AI_MAX_INFLIGHT)
    parser.add_argument("--trace-out", type=Path, default=None)
    parser.add_argument("--progress-every", type=int, default=100)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    api_key = ""
    if args.api_key:
        api_key = str(args.api_key).strip()
    else:
        key_env = str(args.api_key_env or "GEMINI_API_KEY").strip()
        if key_env:
            api_key = os.getenv(key_env, "").strip()
    if not api_key:
        raise RuntimeError("Missing API key. Provide --api-key or set GEMINI_API_KEY.")
    if not args.csv.exists():
        raise FileNotFoundError(f"CSV not found: {args.csv}")

    api_type = clean_text(args.api_type)
    if not api_type and args.api_style == "litellm":
        api_type = clean_text(args.litellm_provider)

    timeout_seconds = args.ai_timeout_sec if args.ai_timeout_sec is not None else args.timeout_seconds

    process_csv(
        csv_path=args.csv,
        db_path=args.db,
        api_key=api_key,
        model=args.model,
        prompt_version=args.prompt_version,
        author=args.author,
        user_id=args.user_id,
        limit=args.limit,
        sleep_seconds=args.sleep_seconds,
        reprocess=args.reprocess,
        relevant_threshold=max(0.0, min(1.0, args.relevant_threshold)),
        api_style=args.api_style,
        base_url=args.base_url,
        litellm_provider=args.litellm_provider,
        gemini_auth=args.gemini_auth,
        api_type=api_type,
        api_mode=args.api_mode,
        ai_stream=args.ai_stream,
        ai_retries=max(0, args.ai_retries),
        ai_temperature=args.ai_temperature,
        ai_reasoning_effort=args.ai_reasoning_effort,
        ai_rpm=max(0.0, args.ai_rpm),
        ai_max_inflight=max(0, args.ai_max_inflight),
        trace_out=args.trace_out,
        timeout_seconds=max(1.0, timeout_seconds),
        progress_every=max(0, args.progress_every),
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
