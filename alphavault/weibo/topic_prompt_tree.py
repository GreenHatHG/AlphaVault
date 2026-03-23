"""
Build an AI-ready message_tree for a Weibo author thread.

Design notes:
- We only ingest the author's posts (RSS). Other users' "comments" are reconstructed
  from display_md segments ("speaker：text").
- Prompt v3 expects a tree: nodes have source_kind/source_id/speaker/text/children.
- Keep it conservative: hard limits (no extra configs) to avoid huge prompts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from alphavault.ui.thread_tree_parse import (
    _content_key_for_compare,
    _extract_speaker_name,
    _make_synthetic_source_id,
    _strip_leading_speaker,
    _to_one_line_text,
    parse_display_md_segments,
)
from alphavault.weibo.display import format_weibo_display_md

# Hard limits to prevent huge prompts.
MAX_THREAD_POSTS = 60

# NOTE:
# - MAX_NODE_TEXT_CHARS is kept for backward-compat, but default truncation is disabled.
# - Prompt length is controlled by a total prompt chars budget (worker layer).
MAX_NODE_TEXT_CHARS = 0
MAX_TOPIC_PROMPT_CHARS = 50_000


def _node_key(source_kind: str, source_id: str) -> str:
    return f"{str(source_kind or '').strip()}:{str(source_id or '').strip()}"


def _truncate_text(text: str, *, max_chars: int) -> tuple[str, bool]:
    s = str(text or "")
    if max_chars <= 0 or len(s) <= max_chars:
        return s, False
    return s[: max(0, int(max_chars))], True


def _ensure_display_md(*, raw_text: str, display_md: str, author: str) -> str:
    if str(display_md or "").strip():
        return str(display_md or "")
    return format_weibo_display_md(
        str(raw_text or ""), author=str(author or "").strip()
    )


def thread_root_info_for_post(
    *,
    raw_text: str,
    display_md: str,
    author: str,
) -> tuple[str, str, str]:
    """
    Return (root_key, root_segment, root_content_key).

    root_key: stable synthetic id (src:xxxx) derived from the 1st segment.
    root_segment: the 1st display_md segment line ("speaker：text").
    root_content_key: normalized compare key for skipping repeated roots.
    """
    resolved_author = str(author or "").strip()
    md = _ensure_display_md(
        raw_text=raw_text, display_md=display_md, author=resolved_author
    )
    segments = parse_display_md_segments(md) if md.strip() else []
    if segments:
        root_segment = segments[0]
    else:
        root_segment = (
            f"{resolved_author}：{_to_one_line_text(str(raw_text or ''))}".strip("：")
        )
    root_key = _make_synthetic_source_id(root_segment) or "root"
    root_content_key = _content_key_for_compare(
        root_segment, author_hint=_extract_speaker_name(root_segment)
    )
    return root_key, root_segment, root_content_key


@dataclass
class _TreeNode:
    payload: dict[str, Any]
    children: dict[str, "_TreeNode"]

    @property
    def key(self) -> str:
        return _node_key(
            str(self.payload.get("source_kind") or ""),
            str(self.payload.get("source_id") or ""),
        )

    @property
    def sort_time(self) -> str:
        return str(self.payload.get("created_at") or "").strip()


def _sorted_children(node: _TreeNode) -> list[_TreeNode]:
    kids = list(node.children.values())
    kids.sort(
        key=lambda child: (
            child.sort_time,
            str(child.payload.get("source_kind") or "").strip(),
            str(child.payload.get("source_id") or "").strip(),
        )
    )
    return kids


def _serialize_tree(node: _TreeNode) -> dict[str, Any]:
    out: dict[str, Any] = {
        "source_kind": str(node.payload.get("source_kind") or "").strip(),
        "source_id": str(node.payload.get("source_id") or "").strip(),
        "speaker": str(node.payload.get("speaker") or "").strip(),
        "text": str(node.payload.get("text") or "").strip(),
    }
    quoted_text = str(node.payload.get("quoted_text") or "").strip()
    if quoted_text:
        out["quoted_text"] = quoted_text
    children = [_serialize_tree(child) for child in _sorted_children(node)]
    if children:
        out["children"] = children
    return out


def _insert_path(tree_root: _TreeNode, path_payloads: list[dict[str, Any]]) -> None:
    current = tree_root
    for payload in path_payloads:
        key = _node_key(
            str(payload.get("source_kind") or ""), str(payload.get("source_id") or "")
        )
        if not key.strip(":"):
            continue
        existing = current.children.get(key)
        if existing is None:
            existing = _TreeNode(payload=payload, children={})
            current.children[key] = existing
        current = existing


def build_topic_runtime_context(
    *,
    root_key: str,
    root_segment: str,
    root_content_key: str,
    focus_username: str,
    posts: list[dict[str, object]],
    include_virtual_comments: bool = True,
    max_node_text_chars: int = MAX_NODE_TEXT_CHARS,
) -> tuple[dict[str, Any], int]:
    """
    Build (runtime_context, truncated_nodes_count).

    posts: list of dicts (each needs platform_post_id/author/created_at/raw_text/display_md).
    include_virtual_comments: whether to reconstruct other speakers as virtual 'comment' nodes.
    """
    focus = str(focus_username or "").strip()
    root_speaker = _extract_speaker_name(root_segment) or focus or "未知"
    root_source_id = str(root_key or "root").strip() or "root"
    root_created_at = ""
    root_text = _strip_leading_speaker(root_segment, author_hint=root_speaker) or ""

    # If the real root post exists in this batch, use its platform_post_id as source_id.
    # That makes evidence_refs mappable back to a concrete post_uid.
    root_post_row: Optional[dict[str, object]] = None
    for row in posts:
        platform_post_id = str(row.get("platform_post_id") or "").strip()
        if not platform_post_id:
            continue
        author = str(row.get("author") or "").strip()
        raw_text = str(row.get("raw_text") or "")
        display_md = str(row.get("display_md") or "")
        md = _ensure_display_md(raw_text=raw_text, display_md=display_md, author=author)
        segments = parse_display_md_segments(md) if md.strip() else []
        if len(segments) != 1:
            continue
        seg_key = _content_key_for_compare(
            segments[0], author_hint=_extract_speaker_name(segments[0])
        )
        if seg_key and seg_key == root_content_key:
            if root_post_row is None:
                root_post_row = row
                continue
            current_ts = str(root_post_row.get("created_at") or "").strip()
            candidate_ts = str(row.get("created_at") or "").strip()
            if candidate_ts and (not current_ts or candidate_ts < current_ts):
                root_post_row = row

    if root_post_row is not None:
        root_source_id = (
            str(root_post_row.get("platform_post_id") or "").strip() or root_source_id
        )
        root_created_at = str(root_post_row.get("created_at") or "").strip()
        root_speaker = str(root_post_row.get("author") or "").strip() or root_speaker
        md = _ensure_display_md(
            raw_text=str(root_post_row.get("raw_text") or ""),
            display_md=str(root_post_row.get("display_md") or ""),
            author=root_speaker,
        )
        segments = parse_display_md_segments(md) if md.strip() else []
        if segments:
            root_text = (
                _strip_leading_speaker(segments[-1], author_hint=root_speaker)
                or root_text
            )

    root_text, root_truncated = _truncate_text(root_text, max_chars=max_node_text_chars)
    root_node = _TreeNode(
        payload={
            "source_kind": "topic_post",
            "source_id": root_source_id,
            "speaker": root_speaker,
            "created_at": root_created_at,
            "text": root_text,
        },
        children={},
    )

    truncated_nodes = 1 if root_truncated else 0

    for row in posts:
        platform_post_id = str(row.get("platform_post_id") or "").strip()
        if not platform_post_id:
            continue
        if platform_post_id == root_source_id:
            # Avoid duplicating the root post as both topic_post and status.
            continue

        author = str(row.get("author") or "").strip()
        created_at = str(row.get("created_at") or "").strip()
        raw_text = str(row.get("raw_text") or "")
        display_md = str(row.get("display_md") or "")

        md = _ensure_display_md(raw_text=raw_text, display_md=display_md, author=author)
        segments = parse_display_md_segments(md) if md.strip() else []
        if not segments:
            # Fallback: treat the whole raw_text as a single "author" segment.
            segments = [f"{author}：{_to_one_line_text(raw_text)}".strip("：")]

        last_seg = segments[-1]
        leaf_text = _strip_leading_speaker(last_seg, author_hint=author) or ""
        leaf_text, leaf_truncated = _truncate_text(
            leaf_text, max_chars=max_node_text_chars
        )
        if leaf_truncated:
            truncated_nodes += 1

        leaf_payload = {
            "source_kind": "status",
            "source_id": platform_post_id,
            "speaker": author or focus or "未知",
            "created_at": created_at,
            "text": leaf_text,
        }

        virtual_segments = segments[:-1] if include_virtual_comments else []
        if virtual_segments and root_content_key:
            first_key = _content_key_for_compare(
                virtual_segments[0],
                author_hint=_extract_speaker_name(virtual_segments[0]),
            )
            if first_key and first_key == root_content_key:
                virtual_segments = virtual_segments[1:]

        path_payloads: list[dict[str, Any]] = []
        for seg in virtual_segments:
            speaker = _extract_speaker_name(seg).strip()
            if not speaker:
                continue
            if focus and speaker == focus:
                # Avoid creating "virtual" focus-username nodes (hard to map back to a post).
                continue

            node_text = _strip_leading_speaker(seg, author_hint=speaker) or ""
            node_text, node_truncated = _truncate_text(
                node_text, max_chars=max_node_text_chars
            )
            if node_truncated:
                truncated_nodes += 1

            path_payloads.append(
                {
                    "source_kind": "comment",
                    "source_id": _make_synthetic_source_id(seg),
                    "speaker": speaker,
                    "created_at": created_at,
                    "text": node_text,
                }
            )

        path_payloads.append(leaf_payload)
        _insert_path(root_node, path_payloads)

    message_tree = _serialize_tree(root_node)
    runtime_context = {
        "root_key": root_key,
        "root_source_id": root_source_id,
        "focus_username": focus,
        "message_tree": message_tree,
        "message_lookup": build_message_lookup_from_tree(message_tree),
        "ai_topic_package": {
            "topic_status_id": root_source_id,
            "focus_username": focus,
            "message_tree": message_tree,
        },
    }
    return runtime_context, truncated_nodes


def build_message_lookup_from_tree(
    message_tree: dict[str, Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    """
    Build a lookup for validating/mapping evidence_refs.

    Key: (source_kind, source_id)
    Value: node payload without children + root_path.
    """
    lookup: dict[tuple[str, str], dict[str, Any]] = {}

    def walk(node: dict[str, Any], path: list[dict[str, Any]]) -> None:
        kind = str(node.get("source_kind") or "").strip()
        sid = str(node.get("source_id") or "").strip()
        node_payload = {k: v for k, v in node.items() if k != "children"}
        current_path = list(path)
        current_path.append(node_payload)
        if kind and sid and (kind, sid) not in lookup:
            lookup[(kind, sid)] = {**node_payload, "root_path": current_path}

        children = node.get("children")
        if isinstance(children, list):
            for child in children:
                if isinstance(child, dict):
                    walk(child, current_path)

    walk(message_tree, [])
    return lookup


__all__ = [
    "MAX_THREAD_POSTS",
    "MAX_NODE_TEXT_CHARS",
    "MAX_TOPIC_PROMPT_CHARS",
    "build_topic_runtime_context",
    "thread_root_info_for_post",
]
