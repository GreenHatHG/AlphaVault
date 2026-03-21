from __future__ import annotations

"""
Thread tree rendering helpers.

Output: an ASCII tree text (├── / └── / │), like 1.txt.
"""

import re

import pandas as pd

from alphavault.ui.thread_tree_parse import (
    _content_key_for_compare,
    _to_one_line_text,
    parse_display_md_segments,
    strip_csv_raw_fields,
)

VIRTUAL_NODE_LABEL = "回复"


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


def _ascii_node_line(
    node_id: str,
    *,
    nodes: dict[str, dict],
    text_override: str | None = None,
) -> str:
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

    if author and text and not (text.startswith(author + "：") or text.startswith(author + ":")):
        text = f"{author}：{text}"

    if text:
        return f"{prefix} {text}"
    if author:
        return f"{prefix} {author}"
    return prefix


def _ascii_node_main_content(node_id: str, *, nodes: dict[str, dict]) -> str:
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


def _ascii_expand_edge(
    parent_id: str,
    child_id: str,
    *,
    nodes: dict[str, dict],
) -> tuple[list[str], str]:
    parent_node = nodes.get(parent_id) or {}
    child_node = nodes.get(child_id) or {}

    parent_main = _ascii_node_main_content(parent_id, nodes=nodes)
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
        return [], _ascii_node_line(child_id, nodes=nodes)
    if len(child_segments) == 1:
        return [], _ascii_node_line(child_id, nodes=nodes, text_override=child_segments[-1])

    virtual_lines = [f"[{VIRTUAL_NODE_LABEL}] {seg}" for seg in child_segments[:-1]]
    real_line = _ascii_node_line(child_id, nodes=nodes, text_override=child_segments[-1])
    return virtual_lines, real_line


def _ascii_render_children(
    parent_id: str,
    prefix: str,
    *,
    nodes: dict[str, dict],
    children: dict[str, list[str]],
    lines: list[str],
    order: list[str],
    visited: set[str],
) -> None:
    child_ids = children.get(parent_id, [])
    for idx, child_id in enumerate(child_ids):
        is_last = idx == len(child_ids) - 1
        branch = "└── " if is_last else "├── "
        extension = "    " if is_last else "│   "
        if child_id in visited:
            lines.append(prefix + branch + _ascii_node_line(child_id, nodes=nodes) + " （循环）")
            continue
        visited.add(child_id)
        order.append(child_id)
        virtual_lines, real_line = _ascii_expand_edge(parent_id, child_id, nodes=nodes)
        if not virtual_lines:
            lines.append(prefix + branch + real_line)
            _ascii_render_children(child_id, prefix + extension, nodes=nodes, children=children, lines=lines, order=order, visited=visited)
            continue
        lines.append(prefix + branch + virtual_lines[0])
        nested_prefix = prefix + extension
        for v in virtual_lines[1:]:
            lines.append(nested_prefix + "└── " + v)
            nested_prefix += "    "
        lines.append(nested_prefix + "└── " + real_line)
        _ascii_render_children(child_id, nested_prefix + "    ", nodes=nodes, children=children, lines=lines, order=order, visited=visited)


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
    lines: list[str] = []
    order: list[str] = []
    visited: set[str] = set()

    lines.append(_ascii_node_line(root_id, nodes=nodes))
    order.append(root_id)
    visited.add(root_id)

    _ascii_render_children(
        root_id,
        "",
        nodes=nodes,
        children=children,
        lines=lines,
        order=order,
        visited=visited,
    )
    return "\n".join(lines), order


__all__ = [
    "VIRTUAL_NODE_LABEL",
    "_format_ts",
    "_render_ascii_tree",
    "_short_text",
    "_to_ts",
]
