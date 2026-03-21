from __future__ import annotations

"""
Thread tree parsing helpers.

Keep this module focused on:
- text normalization
- parsing repost/quote info from raw_text/display_md
- parsing the JSON object after '[CSV原始字段]'
"""

import hashlib
import json
import re
from typing import Any, Dict

from alphavault.text.html import html_to_text

CSV_RAW_FIELDS_MARKER = "[CSV原始字段]"
FORWARD_ORIGINAL_MARKER = "[转发原文]"
REPOST_TOKEN = "转发 @"
MATCH_KEY_LEN = 80
DISPLAY_MD_SPLIT_RE = re.compile(r"(?m)^\s*---\s*$")

SYNTHETIC_SOURCE_ID_PREFIX = "src:"


def strip_csv_raw_fields(raw_text: str) -> str:
    text = str(raw_text or "")
    idx = text.find(CSV_RAW_FIELDS_MARKER)
    if idx < 0:
        return text.strip()
    return text[:idx].strip()


def _normalize_for_match(text: str) -> str:
    s = strip_csv_raw_fields(html_to_text(str(text or "")))
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _first_non_empty_line(text: str) -> str:
    raw = strip_csv_raw_fields(html_to_text(str(text or "")))
    for line in str(raw or "").splitlines():
        s = str(line).strip()
        if s:
            return s
    return str(raw or "").strip()


def _match_key(text: str) -> str:
    first_line = _first_non_empty_line(text)
    first_line = first_line.replace("\u00a0", " ")
    first_line = re.sub(r"\s+", " ", first_line).strip()
    if not first_line:
        return ""
    return first_line[:MATCH_KEY_LEN]


def _to_one_line_text(text: str) -> str:
    s = strip_csv_raw_fields(html_to_text(str(text or "")))
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_display_md_segments(display_md: str) -> list[str]:
    """
    Parse posts.display_md into message segments.

    The DB format often uses '---' as a separator between quoted original and
    later replies (multi-turn). We split by that separator and make every
    segment a single line (so it can become a tree node).
    """
    text = html_to_text(str(display_md or ""))
    if not text.strip():
        return []
    parts = DISPLAY_MD_SPLIT_RE.split(text)
    segments: list[str] = []
    for part in parts:
        seg = _to_one_line_text(str(part or ""))
        if seg:
            segments.append(seg)
    return segments


def _strip_leading_speaker(text: str, *, author_hint: str = "") -> str:
    s = _to_one_line_text(text)
    author = str(author_hint or "").strip()
    if author:
        for sep in ["：", ":"]:
            prefix = author + sep
            if s.startswith(prefix):
                return s[len(prefix) :].lstrip()

    # Fallback: treat the first "xxx：" as speaker prefix.
    m = re.match(r"^[^：:]{1,20}[：:]\s*(.+)$", s)
    if m:
        return str(m.group(1) or "").strip()
    return s


def _content_key_for_compare(text: str, *, author_hint: str = "") -> str:
    content = _strip_leading_speaker(text, author_hint=author_hint)
    return _match_key(content)


def _extract_speaker_name(text: str) -> str:
    s = _to_one_line_text(text)
    m = re.match(r"^([^：:]{1,20})[：:]\s*", s)
    return str(m.group(1)).strip() if m else ""


def _make_synthetic_source_id(first_segment: str) -> str:
    speaker = _extract_speaker_name(first_segment)
    key = _content_key_for_compare(first_segment)
    base = f"{speaker}|{key}".encode("utf-8")
    digest = hashlib.md5(base).hexdigest()[:10]
    return f"{SYNTHETIC_SOURCE_ID_PREFIX}{digest}"


def _extract_forward_original_text(raw_text: str) -> str:
    text = str(raw_text or "")
    idx = text.find(FORWARD_ORIGINAL_MARKER)
    if idx < 0:
        return ""
    tail = text[idx + len(FORWARD_ORIGINAL_MARKER) :]
    stop = tail.find(CSV_RAW_FIELDS_MARKER)
    if stop >= 0:
        tail = tail[:stop]
    return tail.strip()


def _extract_repost_original_text(raw_text: str) -> tuple[str, str]:
    """
    Best-effort parse: "... 转发 @某人: 原文".
    Return (author_hint, original_text). If not found, return ("", "").
    """
    text = html_to_text(str(raw_text or ""))
    idx = text.rfind(REPOST_TOKEN)
    if idx < 0:
        return "", ""

    start = idx + len(REPOST_TOKEN)
    colon_idx = text.find(":", start)
    colon_cn_idx = text.find("：", start)
    candidates = [i for i in [colon_idx, colon_cn_idx] if i >= 0]
    if not candidates:
        return "", ""
    colon = min(candidates)

    author_hint = text[start:colon].strip().lstrip("@").strip()
    original_text = text[colon + 1 :].strip()
    return author_hint, original_text


def extract_platform_post_id(post_uid: object) -> str:
    value = str(post_uid or "").strip()
    if not value:
        return ""
    parts = value.split(":")
    if len(parts) >= 2:
        return parts[-1].strip()
    return value


def _is_escaped(text: str, idx: int) -> bool:
    if idx <= 0 or idx >= len(text):
        return False
    backslashes = 0
    i = idx - 1
    while i >= 0 and text[i] == "\\":
        backslashes += 1
        i -= 1
    return (backslashes % 2) == 1


def _extract_json_object_text(text: str, *, after_idx: int) -> str:
    start = text.find("{", max(0, int(after_idx)))
    if start < 0:
        return ""

    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
                continue
            if ch == "\\":
                escape = True
                continue
            if ch == '"':
                in_string = False
            continue

        if ch == '"' and not _is_escaped(text, i):
            in_string = True
            continue
        if ch == "{":
            depth += 1
            continue
        if ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return ""


def parse_weibo_csv_raw_fields(raw_text: str) -> Dict[str, Any]:
    """
    Parse the JSON object after '[CSV原始字段]'.

    Best-effort:
    - Try json.loads directly (already valid JSON)
    - If failed, treat it as a JSON-string-escaped object and decode twice
    """
    text = str(raw_text or "")
    marker_idx = text.find(CSV_RAW_FIELDS_MARKER)
    if marker_idx < 0:
        return {}

    json_text = _extract_json_object_text(text, after_idx=marker_idx)
    if not json_text.strip():
        return {}

    candidate = json_text.strip()
    try:
        obj = json.loads(candidate)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass

    # Weibo CSV raw fields often escape every quote like: {\"id\": \"...\"}
    # That is not a valid JSON object, but it is valid JSON string content.
    try:
        wrapped = '"' + candidate.replace("\r", "\\r").replace("\n", "\\n") + '"'
        unescaped = json.loads(wrapped)
        obj = json.loads(unescaped)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _clean_id(value: object) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    s = s.replace("\t", "")
    s = re.sub(r"(?:\\+t)+$", "", s)
    return s.strip()


def extract_parent_post_id(*, csv_fields: Dict[str, Any]) -> str:
    return _clean_id(csv_fields.get("源微博id"))


__all__ = [
    "CSV_RAW_FIELDS_MARKER",
    "DISPLAY_MD_SPLIT_RE",
    "FORWARD_ORIGINAL_MARKER",
    "MATCH_KEY_LEN",
    "REPOST_TOKEN",
    "SYNTHETIC_SOURCE_ID_PREFIX",
    "extract_parent_post_id",
    "extract_platform_post_id",
    "parse_display_md_segments",
    "parse_weibo_csv_raw_fields",
    "strip_csv_raw_fields",
]

