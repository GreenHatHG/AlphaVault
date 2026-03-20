from __future__ import annotations

"""
Thread tree helpers (for Streamlit timeline).

Weibo "repost/quote" relationship is embedded inside posts.raw_text:
- marker: [CSV原始字段]
- JSON keys: 源微博id / 源用户昵称 / 源微博正文 / 源微博完整日期 ...

This module parses that JSON (best-effort) and builds a parent-child tree:
- parent: 源微博id (if exists and can be found)
- root: parent is missing

Goal: render a readable ASCII tree like 1.txt (├── / └── / │).
"""

import html
import hashlib
import json
import re
from typing import Any, Dict

import pandas as pd


CSV_RAW_FIELDS_MARKER = "[CSV原始字段]"
FORWARD_ORIGINAL_MARKER = "[转发原文]"
REPOST_TOKEN = "转发 @"
MATCH_KEY_LEN = 80
DISPLAY_MD_SPLIT_RE = re.compile(r"(?m)^\s*---\s*$")

VIRTUAL_NODE_LABEL = "回复"
SYNTHETIC_SOURCE_ID_PREFIX = "src:"


def _html_to_text(text: str) -> str:
    s = str(text or "")
    if "<" in s and ">" in s:
        s = re.sub(r"(?i)<br\\s*/?>", "\n", s)
        s = re.sub(r"(?is)<[^>]+>", "", s)
    return html.unescape(s)


def _normalize_for_match(text: str) -> str:
    s = strip_csv_raw_fields(_html_to_text(text))
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _first_non_empty_line(text: str) -> str:
    raw = strip_csv_raw_fields(_html_to_text(text))
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
    s = strip_csv_raw_fields(_html_to_text(text))
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
    text = _html_to_text(str(display_md or ""))
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
    text = _html_to_text(str(raw_text or ""))
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


def strip_csv_raw_fields(raw_text: str) -> str:
    text = str(raw_text or "")
    idx = text.find(CSV_RAW_FIELDS_MARKER)
    if idx < 0:
        return text.strip()
    return text[:idx].strip()


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


def _weibo_url(user_id: object, bid: object) -> str:
    uid = _clean_id(user_id)
    b = str(bid or "").strip()
    if not uid or not b:
        return ""
    return f"https://weibo.com/{uid}/{b}"


def _source_stub_from_child_csv(csv_fields: Dict[str, Any]) -> dict[str, object]:
    src_author = str(csv_fields.get("源用户昵称") or "").strip()
    src_text = str(csv_fields.get("源微博正文") or "").strip()
    src_created = csv_fields.get("源微博完整日期") or csv_fields.get("源微博日期") or None
    src_url = _weibo_url(csv_fields.get("源用户id"), csv_fields.get("源微博bid"))
    return {
        "author": src_author,
        "raw_text": src_text,
        "created_at": src_created,
        "url": src_url,
    }


def extract_parent_post_id(*, csv_fields: Dict[str, Any]) -> str:
    return _clean_id(csv_fields.get("源微博id"))


def _to_ts(value: object) -> pd.Timestamp | None:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts


def _short_text(raw_text: object, *, n: int) -> str:
    text = strip_csv_raw_fields(str(raw_text or ""))
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= n:
        return text
    return text[:n].rstrip() + "…"


def _format_ts(value: object) -> str:
    ts = _to_ts(value)
    if ts is None:
        return ""
    try:
        return ts.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ts)


def _build_posts_text_index(posts_lookup: dict[str, dict]) -> dict[str, list[dict]]:
    """
    Build a tiny index for inferring repost parent when CSV fields are missing.
    key: normalized text prefix -> list of candidates (post_id, author, created_at_ts)
    """
    index: dict[str, list[dict]] = {}
    for post_id, row in posts_lookup.items():
        pid = _clean_id(post_id)
        if not pid:
            continue
        display_md = str(row.get("display_md") or "").strip()
        raw_text = str(row.get("raw_text") or "").strip()
        candidates = []
        if raw_text:
            candidates.append(raw_text)
        if display_md and display_md != raw_text:
            candidates.append(display_md)

        for text in candidates:
            key = _match_key(text)
            if not key:
                continue
            index.setdefault(key, []).append(
                {
                    "post_id": pid,
                    "author": str(row.get("author") or "").strip(),
                    "created_at": _to_ts(row.get("created_at")),
                }
            )
    return index


def _infer_parent_post_id(
    *,
    child_post_id: str,
    child_created_at: object,
    raw_text: str,
    text_index: dict[str, list[dict]],
) -> str:
    """
    Try to infer repost parent post_id by parsing raw_text (no CSV marker) and
    matching the original text to an existing post in posts_all.
    """
    child_id = _clean_id(child_post_id)
    if not child_id:
        return ""
    child_ts = _to_ts(child_created_at)

    original_text = _extract_forward_original_text(raw_text)
    author_hint = ""
    if not original_text:
        author_hint, original_text = _extract_repost_original_text(raw_text)
    if not original_text:
        return ""

    key = _match_key(original_text)
    if not key:
        return ""

    candidates = list(text_index.get(key, []))
    if author_hint:
        candidates = [c for c in candidates if str(c.get("author") or "").strip() == author_hint]

    if child_ts is not None:
        candidates = [
            c
            for c in candidates
            if c.get("created_at") is not None and c.get("created_at") < child_ts
        ]

    if not candidates:
        return ""

    best = max(
        candidates,
        key=lambda c: c.get("created_at") if c.get("created_at") is not None else pd.Timestamp.min,
    )
    parent_id = _clean_id(best.get("post_id"))
    if not parent_id or parent_id == child_id:
        return ""
    return parent_id


def _render_ascii_tree(
    root_id: str,
    *,
    nodes: dict[str, dict],
    children: dict[str, list[str]],
) -> tuple[str, list[str]]:
    """
    Return (tree_text, preorder_ids).

    Root line has no branch prefix.
    Children use ├── / └── and indentation like 1.txt.
    """

    def _node_line(node_id: str, *, text_override: str | None = None) -> str:
        node = nodes.get(node_id) or {}
        missing = bool(node.get("missing"))
        kind = str(node.get("kind") or "").strip() or ("source" if missing else "status")
        kind_label = {"status": "原帖", "repost": "转发", "source": "源帖"}.get(kind, "帖子")
        prefix = f"[{kind_label} ID: {node_id}]"

        raw_text = str(node.get("raw_text") or "").strip()
        display_md = str(node.get("display_md") or "").strip()
        author = str(node.get("author") or "").strip()

        if text_override is not None and str(text_override or "").strip():
            return f"{prefix} {str(text_override).strip()}"

        segments = parse_display_md_segments(display_md) if display_md else []
        if segments:
            return f"{prefix} {segments[-1]}"

        text = _to_one_line_text(raw_text)
        if not text and missing:
            return f"{prefix} （缺失）"

        if author and text:
            if not (text.startswith(author + "：") or text.startswith(author + ":")):
                text = f"{author}：{text}"
        if text:
            return f"{prefix} {text}"
        if author:
            return f"{prefix} {author}"
        return prefix

    lines: list[str] = []
    order: list[str] = []
    visited: set[str] = set()

    lines.append(_node_line(root_id))
    order.append(root_id)
    visited.add(root_id)

    def _node_main_content(node_id: str) -> str:
        node = nodes.get(node_id) or {}
        raw_text = str(node.get("raw_text") or "").strip()
        display_md = str(node.get("display_md") or "").strip()
        author = str(node.get("author") or "").strip()

        segments = parse_display_md_segments(display_md) if display_md else []
        if segments:
            return segments[-1]

        text = _to_one_line_text(raw_text)
        if author and text and not (text.startswith(author + "：") or text.startswith(author + ":")):
            return f"{author}：{text}"
        return text

    def _expand_edge(parent_id: str, child_id: str) -> tuple[list[str], str]:
        parent_node = nodes.get(parent_id) or {}
        child_node = nodes.get(child_id) or {}

        parent_main = _node_main_content(parent_id)
        parent_key = _content_key_for_compare(
            parent_main,
            author_hint=str(parent_node.get("author") or "").strip(),
        )

        child_display_md = str(child_node.get("display_md") or "").strip()
        child_segments = parse_display_md_segments(child_display_md) if child_display_md else []

        if child_segments and len(child_segments) >= 2 and parent_key:
            first_key = _content_key_for_compare(child_segments[0])
            if first_key and first_key == parent_key:
                child_segments = child_segments[1:]

        if not child_segments:
            return [], _node_line(child_id)

        if len(child_segments) == 1:
            return [], _node_line(child_id, text_override=child_segments[-1])

        virtual_lines = [f"[{VIRTUAL_NODE_LABEL}] {seg}" for seg in child_segments[:-1]]
        real_line = _node_line(child_id, text_override=child_segments[-1])
        return virtual_lines, real_line

    def render_children(parent_id: str, prefix: str) -> None:
        child_ids = children.get(parent_id, [])
        for idx, child_id in enumerate(child_ids):
            is_last = idx == len(child_ids) - 1
            branch = "└── " if is_last else "├── "
            extension = "    " if is_last else "│   "

            if child_id in visited:
                lines.append(prefix + branch + _node_line(child_id) + " （循环）")
                continue

            visited.add(child_id)
            order.append(child_id)

            virtual_lines, real_line = _expand_edge(parent_id, child_id)
            if not virtual_lines:
                lines.append(prefix + branch + real_line)
                render_children(child_id, prefix + extension)
                continue

            lines.append(prefix + branch + virtual_lines[0])
            nested_prefix = prefix + extension
            for v in virtual_lines[1:]:
                lines.append(nested_prefix + "└── " + v)
                nested_prefix += "    "
            lines.append(nested_prefix + "└── " + real_line)
            render_children(child_id, nested_prefix + "    ")

    render_children(root_id, prefix="")
    return "\n".join(lines), order


def build_weibo_thread_forest(
    view_df: pd.DataFrame,
    *,
    posts_all: pd.DataFrame,
) -> list[dict]:
    """
    Build parent-child thread forest for a selected topic/board.

    Important: one tree must be complete.
    - view_df only tells us which posts belong to the current filter (selected_ids)
    - real tree nodes come from posts_all (full posts table), so we can include
      siblings/children even if they have no assertions.

    Input expects columns (best-effort):
    - post_uid, created_at, author, url, raw_text, display_md
    - action/action_strength/summary/confidence (for per-post assertions list)
    """
    if view_df.empty or "post_uid" not in view_df.columns:
        return []

    selected_ids: set[str] = set()
    for item in view_df["post_uid"].dropna().unique().tolist():
        pid = _clean_id(extract_platform_post_id(item))
        if pid:
            selected_ids.add(pid)
    if not selected_ids:
        return []

    base_cols = [
        "post_uid",
        "platform_post_id",
        "created_at",
        "author",
        "url",
        "raw_text",
        "display_md",
    ]
    have_cols = [col for col in base_cols if col in posts_all.columns]
    if not have_cols:
        return []
    posts = posts_all[have_cols].copy()
    if "platform_post_id" not in posts.columns and "post_uid" in posts.columns:
        posts["platform_post_id"] = posts["post_uid"].apply(extract_platform_post_id)
    posts["platform_post_id"] = posts["platform_post_id"].apply(_clean_id)
    posts = posts[posts["platform_post_id"].astype(str).str.strip().ne("")]
    posts = posts.drop_duplicates(subset=["platform_post_id"], keep="first")
    if "created_at" in posts.columns:
        posts = posts.sort_values(by="created_at", ascending=True)

    posts["csv_fields"] = posts["raw_text"].apply(parse_weibo_csv_raw_fields)
    posts["parent_post_id"] = posts["csv_fields"].apply(
        lambda item: extract_parent_post_id(csv_fields=item) if isinstance(item, dict) else ""
    )
    posts["parent_post_id"] = posts["parent_post_id"].apply(_clean_id)

    # Build index for inferring parent from raw_text (when CSV fields are missing).
    posts_lookup: dict[str, dict] = posts.set_index("platform_post_id").to_dict(orient="index")
    text_index = _build_posts_text_index(posts_lookup) if posts_lookup else {}

    if text_index:
        for idx, row in posts.iterrows():
            current_parent = _clean_id(row.get("parent_post_id"))
            if current_parent:
                continue
            child_id = _clean_id(row.get("platform_post_id"))
            inferred = _infer_parent_post_id(
                child_post_id=child_id,
                child_created_at=row.get("created_at"),
                raw_text=str(row.get("raw_text") or ""),
                text_index=text_index,
            )
            if inferred:
                posts.at[idx, "parent_post_id"] = inferred

    # If a post quotes another speaker (display_md has multi segments), but we still
    # can't find parent_id, create a synthetic "source" parent so root is correct.
    synthetic_sources: dict[str, dict[str, str]] = {}
    if "display_md" in posts.columns:
        for idx, row in posts.iterrows():
            if _clean_id(row.get("parent_post_id")):
                continue
            display_md = str(row.get("display_md") or "").strip()
            segments = parse_display_md_segments(display_md) if display_md else []
            if len(segments) < 2:
                continue
            post_author = str(row.get("author") or "").strip()
            first_speaker = _extract_speaker_name(segments[0])
            if not first_speaker or not post_author or first_speaker == post_author:
                continue

            source_id = _make_synthetic_source_id(segments[0])
            posts.at[idx, "parent_post_id"] = source_id
            synthetic_sources.setdefault(
                source_id,
                {
                    "author": first_speaker,
                    "display_md": segments[0],
                    "raw_text": segments[0],
                },
            )

    # Assertions per post_uid
    assertion_cols = [col for col in ["idx", "action", "action_strength", "summary", "evidence", "confidence"] if col in view_df.columns]
    assertions_by_post: dict[str, list[dict]] = {}
    if assertion_cols:
        order_cols = [col for col in ["post_uid", "idx", "created_at"] if col in view_df.columns]
        ordered = view_df.sort_values(by=order_cols, ascending=True)
        for post_uid, group in ordered.groupby("post_uid"):
            records: list[dict] = []
            for _, row in group[assertion_cols].iterrows():
                record = {col: row.get(col) for col in assertion_cols}
                records.append(record)
            assertions_by_post[str(post_uid)] = records

    nodes: dict[str, dict] = {}
    for _, row in posts.iterrows():
        platform_post_id = _clean_id(row.get("platform_post_id"))
        if not platform_post_id:
            continue
        post_uid = str(row.get("post_uid") or "").strip()
        node = {
            "platform_post_id": platform_post_id,
            "post_uid": post_uid,
            "created_at": row.get("created_at"),
            "author": str(row.get("author") or "").strip(),
            "url": str(row.get("url") or "").strip(),
            "raw_text": str(row.get("raw_text") or ""),
            "display_md": str(row.get("display_md") or ""),
            "csv_fields": row.get("csv_fields") if isinstance(row.get("csv_fields"), dict) else {},
            "parent_post_id": _clean_id(row.get("parent_post_id")),
            "assertions": assertions_by_post.get(post_uid, []),
            "missing": False,
            "kind": "repost" if _clean_id(row.get("parent_post_id")) else "status",
        }
        nodes[platform_post_id] = node

    for source_id, src in synthetic_sources.items():
        if source_id in nodes:
            continue
        nodes[source_id] = {
            "platform_post_id": source_id,
            "post_uid": "",
            "created_at": None,
            "author": str(src.get("author") or "").strip(),
            "url": "",
            "raw_text": str(src.get("raw_text") or ""),
            "display_md": str(src.get("display_md") or ""),
            "csv_fields": {},
            "parent_post_id": "",
            "assertions": [],
            "missing": True,
            "kind": "source",
        }

    queue: list[str] = list(nodes.keys())
    while queue:
        node_id = queue.pop()
        node = nodes.get(node_id) or {}
        parent_id = _clean_id(node.get("parent_post_id"))
        if not parent_id or parent_id == node_id:
            continue
        if parent_id in nodes:
            continue

        # Parent not found: create a stub (best-effort) from child's CSV "source" fields.
        child_csv = node.get("csv_fields") if isinstance(node.get("csv_fields"), dict) else {}
        source_stub = _source_stub_from_child_csv(child_csv)
        child_display_md = str(node.get("display_md") or "").strip()
        child_segments = parse_display_md_segments(child_display_md) if child_display_md else []
        fallback_source = child_segments[0] if child_segments else ""
        if fallback_source and not str(source_stub.get("raw_text") or "").strip():
            source_stub["raw_text"] = fallback_source
        stub_display_md = fallback_source if fallback_source else ""
        nodes[parent_id] = {
            "platform_post_id": parent_id,
            "post_uid": "",
            "created_at": source_stub.get("created_at"),
            "author": str(source_stub.get("author") or "").strip(),
            "url": str(source_stub.get("url") or "").strip(),
            "raw_text": str(source_stub.get("raw_text") or ""),
            "display_md": stub_display_md,
            "csv_fields": {},
            "parent_post_id": "",
            "assertions": [],
            "missing": True,
            "kind": "source",
        }

    # Build children map + roots.
    children: dict[str, list[str]] = {}
    for node_id, node in nodes.items():
        parent_id = _clean_id(node.get("parent_post_id"))
        if parent_id and parent_id in nodes and parent_id != node_id:
            children.setdefault(parent_id, []).append(node_id)

    def node_sort_key(nid: str) -> tuple:
        ts = _to_ts((nodes.get(nid) or {}).get("created_at"))
        ts_val = ts if ts is not None else pd.Timestamp.min
        return (ts_val, nid)

    for parent_id, kids in children.items():
        kids.sort(key=node_sort_key)
        children[parent_id] = kids

    def find_root_id(start_id: str) -> str:
        nid = _clean_id(start_id)
        visited_local: set[str] = set()
        while nid and nid not in visited_local:
            visited_local.add(nid)
            node = nodes.get(nid) or {}
            pid = _clean_id(node.get("parent_post_id"))
            if not pid or pid == nid or pid not in nodes:
                return nid
            nid = pid
        return nid or _clean_id(start_id)

    relevant_roots: list[str] = []
    seen_roots: set[str] = set()
    for sid in selected_ids:
        if sid not in nodes:
            continue
        rid = find_root_id(sid)
        if not rid or rid in seen_roots:
            continue
        seen_roots.add(rid)
        relevant_roots.append(rid)

    # Build threads: only roots that contain selected posts.
    threads: list[dict] = []

    for root_id in relevant_roots:
        stack = [root_id]
        subtree: set[str] = set()
        while stack:
            nid = stack.pop()
            if nid in subtree:
                continue
            subtree.add(nid)
            stack.extend(children.get(nid, []))

        latest: pd.Timestamp | None = None
        for nid in subtree:
            ts = _to_ts((nodes.get(nid) or {}).get("created_at"))
            if ts is None:
                continue
            latest = ts if latest is None else max(latest, ts)

        tree_text, order = _render_ascii_tree(root_id, nodes=nodes, children=children)

        root_node = nodes.get(root_id) or {}
        label_node_id = root_id
        if bool(root_node.get("missing")):
            first_child = (children.get(root_id) or [root_id])[0]
            label_node_id = first_child
        label_node = nodes.get(label_node_id) or root_node
        label_author = str(label_node.get("author") or "").strip() or "未知"
        label_ts = _format_ts(label_node.get("created_at"))
        label_preview = _short_text(label_node.get("raw_text"), n=40)
        label = f"{label_ts} · {label_author} · {label_preview}".strip(" ·")

        threads.append(
            {
                "root_id": root_id,
                "label": label,
                "latest_activity": latest,
                "tree_text": tree_text,
                "order": order,
                "nodes": {nid: nodes[nid] for nid in subtree if nid in nodes},
            }
        )

    threads.sort(
        key=lambda item: item.get("latest_activity") if item.get("latest_activity") is not None else pd.Timestamp.min,
        reverse=True,
    )
    return threads
