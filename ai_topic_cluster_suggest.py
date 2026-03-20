from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from ai_analyze import (
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
    api_key = os.getenv("AI_API_KEY", "").strip()
    if not api_key:
        return None, "Missing AI_API_KEY"

    model = os.getenv("AI_MODEL", DEFAULT_MODEL).strip() or DEFAULT_MODEL
    base_url = os.getenv("AI_BASE_URL", "").strip()
    api_mode = os.getenv("AI_API_MODE", DEFAULT_AI_MODE).strip().lower() or DEFAULT_AI_MODE
    if api_mode not in {AI_MODE_COMPLETION, AI_MODE_RESPONSES}:
        api_mode = DEFAULT_AI_MODE

    temperature = _env_float("AI_TEMPERATURE", DEFAULT_AI_TEMPERATURE)
    timeout_seconds = _env_float("AI_TIMEOUT_SEC", 1000.0)
    retries = _env_int("AI_RETRIES", DEFAULT_AI_RETRY_COUNT)
    reasoning_effort = os.getenv("AI_REASONING_EFFORT", DEFAULT_AI_REASONING_EFFORT).strip() or DEFAULT_AI_REASONING_EFFORT

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


def ai_is_configured() -> tuple[bool, str]:
    config, err = _get_ai_config_from_env()
    if config is None:
        return False, err
    return True, ""


def get_ai_config_summary() -> tuple[Dict[str, Any], str]:
    config, err = _get_ai_config_from_env()
    if config is None:
        return {}, err
    return (
        {
            "model": config.model,
            "base_url": config.base_url,
            "api_mode": config.api_mode,
            "timeout_seconds": float(config.timeout_seconds),
            "retries": int(config.retries),
            "temperature": float(config.temperature),
            "reasoning_effort": str(config.reasoning_effort),
        },
        "",
    )


def _format_topic_candidates(topics: Iterable[dict]) -> str:
    lines: List[str] = []
    for item in topics:
        topic_key = clean_text(item.get("topic_key", ""))
        if not topic_key:
            continue
        count = item.get("count", None)
        hint = clean_text(item.get("hint", ""))
        hint_part = f" hint={hint}" if hint else ""
        if count is None:
            lines.append(f"- {topic_key}{hint_part}")
        else:
            lines.append(f"- {topic_key} (count={int(count)}){hint_part}")
    return "\n".join(lines).strip()


def suggest_topics_for_cluster(
    *,
    cluster_name: str,
    description: str,
    candidates: List[Dict[str, Any]],
    max_items_per_list: int = 200,
) -> Dict[str, Any]:
    """
    Ask AI to classify topic_key candidates into include/exclude/unsure.

    candidates: list of {topic_key, count}
    Returns a parsed JSON dict from the model.
    """
    config, err = _get_ai_config_from_env()
    if config is None:
        raise RuntimeError(err or "ai_not_configured")

    cluster_name = clean_text(cluster_name)
    description = clean_text(description)
    candidates_text = _format_topic_candidates(candidates)

    prompt = f"""
你是分类助手。我要做一个板块：{cluster_name}
说明：{description or "（空）"}

下面是系统已有的 topic_key 候选列表（后面可能带 count）。
请你从候选中筛选：哪些 topic_key 属于这个板块。

输出严格 JSON（不要 Markdown），结构如下：
{{
  "include_topics": [{{"topic_key": "...", "confidence": 0.0, "reason": "..."}}],
  "unsure_topics": [{{"topic_key": "...", "confidence": 0.0, "reason": "..."}}],
  "exclude_topics": [{{"topic_key": "...", "reason": "..."}}],
  "keywords": ["..."],
  "negative_keywords": ["..."]
}}

规则：
- topic_key 只能从候选列表里选，禁止编造。
- include_topics 要保守；不确定就放 unsure_topics。
- 如果 topic_key 是类似 stock:601225.SH 这种“代码”，请优先参考候选里的 hint（比如 stock_name/industry）。没有 hint 就更保守，放 unsure_topics。
- confidence 范围 0~1。
- include_topics / unsure_topics / exclude_topics 每个最多 {int(max_items_per_list)} 条。
- keywords：给 5~15 个“可以直接拿去搜索”的短词（建议 2~6 个字），用于在系统里搜 topic_key/hint。
  - 尽量具体，别太泛（不要只给“相关/行业/板块/概念”这种）。
  - 不要带空格/标点；不要重复；如果实在想不到，可以给 []。
- negative_keywords：同理，给 0~10 个“不想要的词”，用于隐藏/过滤明显不相关的 topic_key/hint（没有就 []）。

候选 topic_key：
{candidates_text}
""".strip()

    parsed = _call_ai_with_litellm(
        prompt=prompt,
        api_mode=config.api_mode,
        ai_stream=False,
        model_name=config.model,
        base_url=config.base_url,
        api_key=config.api_key,
        timeout_seconds=float(config.timeout_seconds),
        retry_count=int(config.retries),
        temperature=float(config.temperature),
        reasoning_effort=str(config.reasoning_effort),
        trace_out=None,
        trace_label=f"cluster_suggest:{cluster_name}",
    )
    if not isinstance(parsed, dict):
        raise RuntimeError("ai_invalid_json")
    return parsed
