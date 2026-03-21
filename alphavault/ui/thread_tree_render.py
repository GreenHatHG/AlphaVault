from __future__ import annotations

"""
Thread tree rendering helpers.

Output: an ASCII tree text (├── / └── / │), like 1.txt.
"""

import re

import pandas as pd

from alphavault.ui.thread_tree_parse import (
    _content_key_for_compare,
    _extract_speaker_name,
    _to_one_line_text,
    parse_display_md_segments,
    strip_csv_raw_fields,
)

VIRTUAL_NODE_LABEL = "回复"
FOCUS_PREFIX_SYMBOL = "📌"
REPLY_PLACEHOLDER_CONTENT = "回复"


def _maybe_prefix_focus_symbol(text: str, *, blogger_authors: set[str] | None) -> str:
    if not blogger_authors:
        return text

    raw = str(text or "")
    if not raw.strip():
        return text

    leading_ws = raw[: len(raw) - len(raw.lstrip())]
    rest = raw[len(leading_ws) :]
    if rest.startswith(FOCUS_PREFIX_SYMBOL):
        return text

    idx_cn = rest.find("：")
    idx_en = rest.find(":")
    candidates = [i for i in [idx_cn, idx_en] if i > 0]
    if not candidates:
        return text
    sep_idx = min(candidates)

    speaker = rest[:sep_idx].strip()
    if not speaker or speaker not in blogger_authors:
        return text

    content = rest[sep_idx + 1 :].strip()
    if not content or content == REPLY_PLACEHOLDER_CONTENT:
        return text

    return leading_ws + FOCUS_PREFIX_SYMBOL + rest


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
    blogger_authors: set[str] | None,
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
        text = _maybe_prefix_focus_symbol(str(text_override).strip(), blogger_authors=blogger_authors)
        return f"{prefix} {text}"

    segments = parse_display_md_segments(display_md) if display_md else []
    if segments:
        text = _maybe_prefix_focus_symbol(segments[-1], blogger_authors=blogger_authors)
        return f"{prefix} {text}"

    text = _to_one_line_text(raw_text)
    if not text and missing:
        return f"{prefix} （缺失）"

    if author and text and not (text.startswith(author + "：") or text.startswith(author + ":")):
        text = f"{author}：{text}"

    if text:
        text = _maybe_prefix_focus_symbol(text, blogger_authors=blogger_authors)
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


def _segment_dedup_key(text: str, *, author_hint: str = "") -> str:
    """
    Build a stable key for comparing content across:
    - real node lines (with ID)
    - virtual '[回复]' lines (from display_md segments)

    Key = "speaker|content_key" (speaker optional).
    """
    s = str(text or "").strip()
    if not s:
        return ""

    speaker = str(_extract_speaker_name(s) or "").strip()
    if not speaker:
        speaker = str(author_hint or "").strip()

    content_key = _content_key_for_compare(s, author_hint=speaker)
    if not content_key:
        return ""

    return f"{speaker}|{content_key}" if speaker else content_key


def _subtree_ids(root_id: str, *, children: dict[str, list[str]]) -> set[str]:
    stack = [str(root_id or "").strip()]
    out: set[str] = set()
    while stack:
        nid = str(stack.pop() or "").strip()
        if not nid or nid in out:
            continue
        out.add(nid)
        stack.extend(children.get(nid, []))
    return out


def _build_real_node_key_set(
    root_id: str,
    *,
    nodes: dict[str, dict],
    children: dict[str, list[str]],
) -> set[str]:
    keys: set[str] = set()
    subtree = _subtree_ids(root_id, children=children)
    for node_id in subtree:
        node = nodes.get(node_id) or {}
        author = str(node.get("author") or "").strip()
        main = _ascii_node_main_content(node_id, nodes=nodes)
        key = _segment_dedup_key(main, author_hint=author)
        if key:
            keys.add(key)
    return keys


class _VirtualTrieNode:
    def __init__(self) -> None:
        self.virtual_children: dict[str, _VirtualTrieNode] = {}
        self.order: list[tuple[str, str]] = []

    def get_or_create_virtual(self, label: str) -> _VirtualTrieNode:
        key = str(label or "").strip()
        if not key:
            return self
        child = self.virtual_children.get(key)
        if child is not None:
            return child
        child = _VirtualTrieNode()
        self.virtual_children[key] = child
        self.order.append(("virtual", key))
        return child

    def add_real_leaf(self, node_id: str) -> None:
        nid = str(node_id or "").strip()
        if not nid:
            return
        self.order.append(("real", nid))


def _ascii_render_virtual_trie(
    trie: _VirtualTrieNode,
    prefix: str,
    *,
    nodes: dict[str, dict],
    children: dict[str, list[str]],
    lines: list[str],
    order: list[str],
    visited: set[str],
    real_line_by_id: dict[str, str],
    dedup_real_keys: set[str] | None,
    blogger_authors: set[str] | None,
) -> None:
    entries = trie.order
    for idx, (kind, key) in enumerate(entries):
        is_last = idx == len(entries) - 1
        branch = "└── " if is_last else "├── "
        extension = "    " if is_last else "│   "

        if kind == "virtual":
            lines.append(prefix + branch + key)
            child_trie = trie.virtual_children.get(key)
            if child_trie is None:
                continue
            _ascii_render_virtual_trie(
                child_trie,
                prefix + extension,
                nodes=nodes,
                children=children,
                lines=lines,
                order=order,
                visited=visited,
                real_line_by_id=real_line_by_id,
                dedup_real_keys=dedup_real_keys,
                blogger_authors=blogger_authors,
            )
            continue

        child_id = key
        if child_id in visited:
            lines.append(
                prefix
                + branch
                + _ascii_node_line(child_id, nodes=nodes, blogger_authors=blogger_authors)
                + " （循环）"
            )
            continue

        visited.add(child_id)
        order.append(child_id)
        real_line = real_line_by_id.get(child_id) or _ascii_node_line(
            child_id,
            nodes=nodes,
            blogger_authors=blogger_authors,
        )
        lines.append(prefix + branch + real_line)
        _ascii_render_children(
            child_id,
            prefix + extension,
            nodes=nodes,
            children=children,
            lines=lines,
            order=order,
            visited=visited,
            dedup_real_keys=dedup_real_keys,
            blogger_authors=blogger_authors,
        )


def _ascii_expand_edge(
    parent_id: str,
    child_id: str,
    *,
    nodes: dict[str, dict],
    dedup_real_keys: set[str] | None = None,
    blogger_authors: set[str] | None,
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
        return [], _ascii_node_line(child_id, nodes=nodes, blogger_authors=blogger_authors)
    last_seg = child_segments[-1]
    if len(child_segments) == 1:
        return [], _ascii_node_line(
            child_id,
            nodes=nodes,
            text_override=last_seg,
            blogger_authors=blogger_authors,
        )

    virtual_segments = child_segments[:-1]
    if dedup_real_keys and virtual_segments:
        child_author = str(child_node.get("author") or "").strip()
        filtered: list[str] = []
        for seg in virtual_segments:
            key = _segment_dedup_key(seg, author_hint=child_author)
            if key and key in dedup_real_keys:
                continue
            filtered.append(seg)
        virtual_segments = filtered

    if not virtual_segments:
        return [], _ascii_node_line(
            child_id,
            nodes=nodes,
            text_override=last_seg,
            blogger_authors=blogger_authors,
        )

    virtual_prefix = f"[{VIRTUAL_NODE_LABEL}] "
    virtual_lines = [
        virtual_prefix + _maybe_prefix_focus_symbol(str(seg), blogger_authors=blogger_authors)
        for seg in virtual_segments
    ]
    real_line = _ascii_node_line(child_id, nodes=nodes, text_override=last_seg, blogger_authors=blogger_authors)
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
    dedup_real_keys: set[str] | None = None,
    blogger_authors: set[str] | None,
) -> None:
    child_ids = children.get(parent_id, [])
    if not child_ids:
        return

    trie = _VirtualTrieNode()
    real_line_by_id: dict[str, str] = {}
    for child_id in child_ids:
        virtual_lines, real_line = _ascii_expand_edge(
            parent_id,
            child_id,
            nodes=nodes,
            dedup_real_keys=dedup_real_keys,
            blogger_authors=blogger_authors,
        )
        real_line_by_id[child_id] = real_line
        node = trie
        for v in virtual_lines:
            node = node.get_or_create_virtual(v)
        node.add_real_leaf(child_id)

    _ascii_render_virtual_trie(
        trie,
        prefix,
        nodes=nodes,
        children=children,
        lines=lines,
        order=order,
        visited=visited,
        real_line_by_id=real_line_by_id,
        dedup_real_keys=dedup_real_keys,
        blogger_authors=blogger_authors,
    )


def _render_ascii_tree(
    root_id: str,
    *,
    nodes: dict[str, dict],
    children: dict[str, list[str]],
    blogger_authors: set[str] | None = None,
) -> tuple[str, list[str]]:
    """
    Return (tree_text, preorder_ids).

    Root line has no branch prefix.
    Children use ├── / └── and indentation like 1.txt.
    """
    lines: list[str] = []
    order: list[str] = []
    visited: set[str] = set()
    dedup_real_keys = _build_real_node_key_set(root_id, nodes=nodes, children=children)

    lines.append(_ascii_node_line(root_id, nodes=nodes, blogger_authors=blogger_authors))
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
        dedup_real_keys=dedup_real_keys,
        blogger_authors=blogger_authors,
    )
    return "\n".join(lines), order


__all__ = [
    "VIRTUAL_NODE_LABEL",
    "_format_ts",
    "_render_ascii_tree",
    "_short_text",
    "_to_ts",
]
