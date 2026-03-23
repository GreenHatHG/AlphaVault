from __future__ import annotations

"""
AI output tag validation.

This module intentionally does NOT try to "auto-fix" ambiguous values.
If something is invalid, we fail fast so the caller can retry/regenerate.
"""

import json
import re
from typing import Any, Iterable

from alphavault.ai.analyze import ALLOWED_ACTIONS
from alphavault.ai.topic_prompt_v3 import TOPIC_PROMPT_VERSION


ALLOWED_TOPIC_KEY_PREFIXES = {
    "stock",
    "industry",
    "index",
    "macro",
    "commodity",
    "method",
    "mindset",
    "life",
}

_BAD_SEPARATORS = {",", "，", "、"}

_STOCK_SUFFIXES = {"SH", "SZ", "BJ", "HK", "US"}

# Stock code formats (keep minimal + strict to reduce DB pollution):
# - A-share: 6 digits + .SH/.SZ/.BJ
# - HK: 4-5 digits + .HK (0700.HK / 09988.HK)
# - US: 1-10 alnum (starts with letter) + .US (AMZN.US)
_STOCK_CODE_CN_RE = re.compile(r"^\d{6}\.(SH|SZ|BJ)$")
_STOCK_CODE_HK_RE = re.compile(r"^\d{4,5}\.HK$")
_STOCK_CODE_US_RE = re.compile(r"^[A-Z][A-Z0-9]{0,9}\.US$")


def _is_stock_code(value: str) -> bool:
    v = _clean_str(value).upper()
    return bool(_STOCK_CODE_CN_RE.match(v) or _STOCK_CODE_HK_RE.match(v) or _STOCK_CODE_US_RE.match(v))


class AiTagValidationError(RuntimeError):
    pass


def _clean_str(value: object) -> str:
    return str(value or "").strip()


def _short(value: object, *, max_len: int = 80) -> str:
    s = _clean_str(value)
    if not s:
        return ""
    if len(s) <= int(max_len):
        return s
    return s[: max(0, int(max_len) - 3)] + "..."


def _has_bad_separator(value: str) -> bool:
    return any(sep in value for sep in _BAD_SEPARATORS)


def _must(condition: bool, msg: str) -> None:
    if condition:
        return
    raise AiTagValidationError(msg)


def _parse_json_list(value: object) -> list[str]:
    """
    Parse a JSON list field from DB (e.g. stock_codes_json).

    - None/"" -> []
    - invalid JSON -> raise (treat as invalid stored data)
    """
    if value is None:
        return []
    if isinstance(value, list):
        return [_clean_str(x) for x in value if _clean_str(x)]
    raw = _clean_str(value)
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception as exc:
        raise AiTagValidationError(f"json_list_invalid:{type(exc).__name__} raw={_short(raw)}") from exc
    if not isinstance(parsed, list):
        raise AiTagValidationError(f"json_list_not_list type={type(parsed).__name__}")
    return [_clean_str(x) for x in parsed if _clean_str(x)]


def _validate_optional_str_list(
    value: object,
    *,
    field: str,
    max_items: int = 50,
    max_item_len: int = 60,
) -> list[str]:
    """
    For AI parsed JSON:
    - missing/None => []
    - if present => must be a list[str] (non-empty strings)
    """
    if value is None:
        return []
    if not isinstance(value, list):
        raise AiTagValidationError(f"{field}_not_list type={type(value).__name__}")
    out: list[str] = []
    for i, item in enumerate(value):
        s = _clean_str(item)
        _must(bool(s), f"{field}[{i}]_empty")
        _must(len(s) <= int(max_item_len), f"{field}[{i}]_too_long value={_short(s)}")
        _must(not _has_bad_separator(s), f"{field}[{i}]_has_separator value={_short(s)}")
        out.append(s)
        if len(out) >= int(max_items):
            break
    return out


def _validate_stock_code(code: str, *, field: str) -> None:
    c = _clean_str(code)
    _must(bool(c), f"{field}_empty")
    _must(not _has_bad_separator(c), f"{field}_has_separator value={_short(c)}")
    _must(bool(_is_stock_code(c)), f"{field}_invalid_code value={_short(c)}")
    # Double-check suffix allowlist for safer future edits.
    suffix = c.rsplit(".", 1)[-1].strip().upper()
    _must(suffix in _STOCK_SUFFIXES, f"{field}_invalid_suffix value={_short(c)}")


def _stock_value_kind(value: str, *, allow_stock_name: bool) -> str:
    """
    Returns: "code" | "name"
    """
    v = _clean_str(value)
    _must(bool(v), "topic_key_value_empty")
    _must(not _has_bad_separator(v), f"topic_key_value_has_separator value={_short(v)}")
    if "." in v:
        _must(bool(_is_stock_code(v)), f"topic_key_stock_code_invalid value={_short(v)}")
        return "code"
    if _is_stock_code(v):
        return "code"
    _must(bool(allow_stock_name), f"topic_key_stock_name_not_allowed value={_short(v)}")
    _must(not v.isdigit(), f"topic_key_stock_name_is_digit value={_short(v)}")
    _must(":" not in v, f"topic_key_stock_name_has_colon value={_short(v)}")
    _must(len(v) <= 40, f"topic_key_stock_name_too_long value={_short(v)}")
    return "name"


def _validate_topic_key(topic_key: object, *, allow_stock_name: bool) -> tuple[str, str, str]:
    raw = _clean_str(topic_key)
    _must(bool(raw), "topic_key_empty")
    _must(":" in raw, f"topic_key_missing_colon value={_short(raw)}")
    left, right = raw.split(":", 1)
    prefix = _clean_str(left).lower()
    value = _clean_str(right)
    _must(prefix in ALLOWED_TOPIC_KEY_PREFIXES, f"topic_key_prefix_invalid prefix={_short(prefix)}")
    _must(bool(value), f"topic_key_value_empty prefix={_short(prefix)}")
    _must(not _has_bad_separator(value), f"topic_key_value_has_separator value={_short(value)}")

    if prefix == "stock":
        kind = _stock_value_kind(value, allow_stock_name=allow_stock_name)
        return prefix, value, kind

    _must(":" not in value, f"topic_key_value_has_colon value={_short(value)}")
    _must("." not in value, f"topic_key_value_has_dot value={_short(value)}")
    _must(len(value) <= 40, f"topic_key_value_too_long value={_short(value)}")
    return prefix, value, "plain"


def _validate_action(action: object) -> None:
    a = _clean_str(action)
    _must(bool(a), "action_empty")
    _must(a in ALLOWED_ACTIONS, f"action_invalid value={_short(a)}")


def _validate_int_range(value: object, *, field: str, low: int, high: int) -> None:
    try:
        v = int(value)  # type: ignore[arg-type]
    except Exception as exc:
        raise AiTagValidationError(f"{field}_not_int value={_short(value)}") from exc
    _must(int(low) <= v <= int(high), f"{field}_out_of_range value={v}")


def _validate_float_range(value: object, *, field: str, low: float, high: float) -> None:
    try:
        v = float(value)  # type: ignore[arg-type]
    except Exception as exc:
        raise AiTagValidationError(f"{field}_not_float value={_short(value)}") from exc
    _must(float(low) <= v <= float(high), f"{field}_out_of_range value={v}")


def validate_topic_prompt_v3_ai_result(parsed: dict[str, object]) -> None:
    items = parsed.get("items")
    if not isinstance(items, list):
        raise AiTagValidationError("ai_topic_items_missing")
    for idx, raw_item in enumerate(items):
        if not isinstance(raw_item, dict):
            raise AiTagValidationError(f"items[{idx}]_not_object")
        validate_topic_prompt_v3_item(raw_item, item_index=idx)


def validate_topic_prompt_v3_item(item: dict[str, object], *, item_index: int) -> None:
    prefix, _value, stock_kind = _validate_topic_key(
        item.get("topic_key"),
        allow_stock_name=True,
    )
    _validate_action(item.get("action"))
    _validate_int_range(item.get("action_strength"), field="action_strength", low=0, high=3)
    _validate_float_range(item.get("confidence"), field="confidence", low=0.0, high=1.0)

    stock_codes = _validate_optional_str_list(item.get("stock_codes"), field="stock_codes", max_item_len=20)
    stock_names = _validate_optional_str_list(item.get("stock_names"), field="stock_names")
    industries = _validate_optional_str_list(item.get("industries"), field="industries")
    commodities = _validate_optional_str_list(item.get("commodities"), field="commodities")
    indices = _validate_optional_str_list(item.get("indices"), field="indices")

    if prefix == "stock":
        for i, code in enumerate(stock_codes):
            _validate_stock_code(code, field=f"stock_codes[{i}]")
        if stock_kind == "name":
            _must(not stock_codes, f"items[{item_index}]_stock_name_requires_empty_stock_codes")

    # Keep these variables referenced, to make future edits safer (no unused warnings in reviews).
    _ = (stock_names, industries, commodities, indices)


def validate_assertion_row(row: dict[str, object], *, prompt_version: str) -> None:
    """
    Validate one stored assertion row (DB shape).

    prompt_version is required so we can keep rules aligned with the prompt.
    """
    allow_stock_name = str(prompt_version or "").strip() == TOPIC_PROMPT_VERSION
    prefix, _value, stock_kind = _validate_topic_key(
        row.get("topic_key"),
        allow_stock_name=allow_stock_name,
    )

    _validate_action(row.get("action"))
    _validate_int_range(row.get("action_strength"), field="action_strength", low=0, high=3)
    _validate_float_range(row.get("confidence"), field="confidence", low=0.0, high=1.0)

    def _parse_field(field: str) -> list[str]:
        try:
            return _parse_json_list(row.get(field))
        except AiTagValidationError as exc:
            raise AiTagValidationError(f"{field}:{exc}") from exc

    stock_codes = _parse_field("stock_codes_json")
    stock_names = _parse_field("stock_names_json")
    industries = _parse_field("industries_json")
    commodities = _parse_field("commodities_json")
    indices = _parse_field("indices_json")

    # Enforce list element hygiene (no commas, non-empty).
    for field, items, max_len in [
        ("stock_codes_json", stock_codes, 20),
        ("stock_names_json", stock_names, 60),
        ("industries_json", industries, 60),
        ("commodities_json", commodities, 60),
        ("indices_json", indices, 60),
    ]:
        for i, v in enumerate(items):
            _must(bool(v), f"{field}[{i}]_empty")
            _must(len(v) <= int(max_len), f"{field}[{i}]_too_long")
            _must(not _has_bad_separator(v), f"{field}[{i}]_has_separator")

    if prefix == "stock":
        for i, code in enumerate(stock_codes):
            _validate_stock_code(code, field=f"stock_codes_json[{i}]")
        if stock_kind == "name":
            _must(not stock_codes, "stock_name_requires_empty_stock_codes")


def validate_many_assertion_rows(rows: Iterable[dict[str, object]], *, prompt_version: str) -> None:
    for row in rows:
        validate_assertion_row(row, prompt_version=prompt_version)


__all__ = [
    "AiTagValidationError",
    "validate_topic_prompt_v3_ai_result",
    "validate_topic_prompt_v3_item",
    "validate_assertion_row",
    "validate_many_assertion_rows",
]
