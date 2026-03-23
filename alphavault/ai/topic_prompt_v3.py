from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

TOPIC_PROMPT_VERSION = "topic-prompt-v3"
PROMPT_FOCUS_USERNAME_PLACEHOLDER = "__FOCUS_USERNAME__"

_HEADER_TEMPLATE_PATH = Path(__file__).with_name("topic_prompt_v3_header.txt")


@lru_cache(maxsize=1)
def _load_prompt_header_template() -> str:
    # Keep the prompt in a standalone file so it can be copied verbatim.
    return _HEADER_TEMPLATE_PATH.read_text(encoding="utf-8")


def build_prompt_header(*, focus_username: str) -> str:
    return _load_prompt_header_template().replace(
        PROMPT_FOCUS_USERNAME_PLACEHOLDER, str(focus_username or "").strip()
    )


def build_topic_prompt(
    *, ai_topic_package: Dict[str, Any], compact_json: bool = False
) -> str:
    focus_username = str(ai_topic_package.get("focus_username") or "").strip()
    header = build_prompt_header(focus_username=focus_username)
    if compact_json:
        topic_json = json.dumps(
            ai_topic_package, ensure_ascii=False, separators=(",", ":")
        )
    else:
        topic_json = json.dumps(ai_topic_package, ensure_ascii=False, indent=2)
    return (
        "\n\n".join(
            [
                header,
                "\n".join(
                    [
                        "话题块 1",
                        "JSON：",
                        "```json",
                        topic_json,
                        "```",
                    ]
                ),
            ]
        )
        + "\n"
    )


def build_topic_prompt_compact(*, ai_topic_package: Dict[str, Any]) -> str:
    return build_topic_prompt(ai_topic_package=ai_topic_package, compact_json=True)


__all__ = [
    "TOPIC_PROMPT_VERSION",
    "PROMPT_FOCUS_USERNAME_PLACEHOLDER",
    "build_prompt_header",
    "build_topic_prompt",
    "build_topic_prompt_compact",
]
