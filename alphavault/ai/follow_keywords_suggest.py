from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from alphavault.constants import (
    ENV_AI_API_KEY,
    ENV_AI_API_MODE,
    ENV_AI_BASE_URL,
    ENV_AI_MODEL,
    ENV_AI_REASONING_EFFORT,
    ENV_AI_RETRIES,
    ENV_AI_TEMPERATURE,
    ENV_AI_TIMEOUT_SEC,
)
from alphavault.ai.analyze import (
    AI_MODE_COMPLETION,
    AI_MODE_RESPONSES,
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_AI_RETRY_COUNT,
    DEFAULT_AI_TEMPERATURE,
    DEFAULT_MODEL,
    _call_ai_with_litellm,
    clean_text,
)


@dataclass(frozen=True)
class AiConfig:
    api_key: str
    model: str
    base_url: str
    api_mode: str
    temperature: float
    reasoning_effort: str
    timeout_seconds: float
    retries: int


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(str(raw).strip())
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def _get_ai_config_from_env() -> tuple[Optional[AiConfig], str]:
    api_key = os.getenv(ENV_AI_API_KEY, "").strip()
    if not api_key:
        return None, f"Missing {ENV_AI_API_KEY}"

    model = os.getenv(ENV_AI_MODEL, DEFAULT_MODEL).strip() or DEFAULT_MODEL
    base_url = os.getenv(ENV_AI_BASE_URL, "").strip()
    api_mode = (
        os.getenv(ENV_AI_API_MODE, DEFAULT_AI_MODE).strip().lower() or DEFAULT_AI_MODE
    )
    if api_mode not in {AI_MODE_COMPLETION, AI_MODE_RESPONSES}:
        api_mode = DEFAULT_AI_MODE

    temperature = _env_float(ENV_AI_TEMPERATURE, DEFAULT_AI_TEMPERATURE)
    timeout_seconds = _env_float(ENV_AI_TIMEOUT_SEC, 1000.0)
    retries = _env_int(ENV_AI_RETRIES, DEFAULT_AI_RETRY_COUNT)
    reasoning_effort = (
        os.getenv(ENV_AI_REASONING_EFFORT, DEFAULT_AI_REASONING_EFFORT).strip()
        or DEFAULT_AI_REASONING_EFFORT
    )

    return (
        AiConfig(
            api_key=api_key,
            model=model,
            base_url=base_url,
            api_mode=api_mode,
            temperature=temperature,
            reasoning_effort=reasoning_effort,
            timeout_seconds=timeout_seconds,
            retries=retries,
        ),
        "",
    )


def suggest_keywords_for_follow(
    *,
    follow_key: str,
    follow_label: str,
    seed_keywords: list[str],
    example_texts: list[str],
    max_keywords: int = 12,
    timeout_seconds: float | None = None,
    retries: int | None = None,
) -> Dict[str, Any]:
    """
    Ask AI to suggest OR keywords for a follow page.

    Output JSON:
    {"keywords": ["..."], "note": "..."}
    """
    config, err = _get_ai_config_from_env()
    if config is None:
        raise RuntimeError(err or "ai_not_configured")

    follow_key = clean_text(follow_key)
    follow_label = clean_text(follow_label) or follow_key

    seed = [clean_text(x) for x in (seed_keywords or [])]
    seed = [x for x in seed if x]
    seed_text = ", ".join(seed[:30])

    examples = [clean_text(x) for x in (example_texts or [])]
    examples = [x for x in examples if x]
    examples = examples[:12]
    examples_text = "\n".join([f"- {x}" for x in examples])

    effective_timeout_seconds = (
        float(timeout_seconds)
        if timeout_seconds is not None
        else float(config.timeout_seconds)
    )
    effective_timeout_seconds = max(1.0, effective_timeout_seconds)

    effective_retries = int(retries) if retries is not None else int(config.retries)
    effective_retries = max(0, effective_retries)

    prompt = f"""
你是关键字助手。我要做一个“关注页”，用关键字 OR 来补漏（AI 可能漏打标签、也可能写简称/外号）。

关注对象：
- follow_key: {follow_key}
- follow_label: {follow_label}

已有种子关键字（可能不全）：
{seed_text or "（空）"}

下面是一些原文例子（可能包含简称/代码/外号）：
{examples_text or "（空）"}

请输出严格 JSON（不要 Markdown），结构：
{{
  "keywords": ["..."],
  "note": "一句话说明"
}}

规则：
- keywords 用“普通字符串”，不要正则，不要加括号/|/.* 这种符号。
- 词尽量短但别太短（不要 1 个字）。
- 目标是：少而准（一般 5~{int(max_keywords)} 个就够）。
- 去重；不要把 seed_keywords 原样重复一堆。
""".strip()

    parsed = _call_ai_with_litellm(
        prompt=prompt,
        api_mode=config.api_mode,
        ai_stream=False,
        model_name=config.model,
        base_url=config.base_url,
        api_key=config.api_key,
        timeout_seconds=float(effective_timeout_seconds),
        retry_count=int(effective_retries),
        temperature=float(config.temperature),
        reasoning_effort=str(config.reasoning_effort),
        trace_out=None,
        trace_label=f"follow_keywords:{follow_key}",
    )
    if not isinstance(parsed, dict):
        raise RuntimeError("ai_invalid_json")

    kws = parsed.get("keywords")
    if not isinstance(kws, list):
        kws = []
    keywords = [clean_text(x) for x in kws if clean_text(x)]
    # keep small + unique
    out_keywords: list[str] = []
    seen: set[str] = set()
    for kw in keywords:
        if kw in seen:
            continue
        seen.add(kw)
        out_keywords.append(kw)
        if len(out_keywords) >= int(max(1, max_keywords)):
            break

    note = clean_text(parsed.get("note", ""))
    return {"keywords": out_keywords, "note": note}


__all__ = ["suggest_keywords_for_follow"]
