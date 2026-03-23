from __future__ import annotations

import json
from typing import Any, Dict


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
