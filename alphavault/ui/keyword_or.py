from __future__ import annotations

"""
Keyword OR helpers for Streamlit UI.

Users can input multiple keywords, separated by comma/space/newline,
and we treat them as OR in text matching.
"""

import re


KEYWORD_SPLIT_RE = re.compile(r"[,\s]+")


def split_keywords_or(text: str) -> list[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    raw = raw.replace("，", ",").replace("、", ",").replace("；", ",").replace(";", ",")
    parts = KEYWORD_SPLIT_RE.split(raw)
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        word = str(part or "").strip()
        if not word or word in seen:
            continue
        seen.add(word)
        out.append(word)
    return out


__all__ = ["KEYWORD_SPLIT_RE", "split_keywords_or"]

