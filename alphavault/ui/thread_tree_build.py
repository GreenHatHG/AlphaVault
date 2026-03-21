from __future__ import annotations

"""
Thread tree builder.

Input:
- view_df: current filtered view (tells which post_uid are "selected")
- posts_all: full posts table (used to build complete trees)
"""

from typing import Any, Dict

import pandas as pd

from alphavault.ui.thread_tree_parse import (
    _clean_id,
    _extract_forward_original_text,
    _extract_repost_original_text,
    _extract_speaker_name,
    _make_synthetic_source_id,
    _match_key,
    extract_parent_post_id,
    extract_platform_post_id,
    parse_display_md_segments,
    parse_weibo_csv_raw_fields,
)
from alphavault.ui.thread_tree_render import _format_ts, _render_ascii_tree, _short_text, _to_ts


def _collect_selected_ids(view_df: pd.DataFrame) -> set[str]:
    selected_ids: set[str] = set()
    for item in view_df["post_uid"].dropna().unique().tolist():
        pid = _clean_id(extract_platform_post_id(item))
        if pid:
            selected_ids.add(pid)
    return selected_ids


def _collect_blogger_authors(selected_ids: set[str], *, nodes: dict[str, dict]) -> set[str]:
    authors: set[str] = set()
    for post_id in selected_ids:
        node = nodes.get(str(post_id)) or {}
        author = str(node.get("author") or "").strip()
        if author:
            authors.add(author)
    return authors


def _prepare_posts(posts_all: pd.DataFrame) -> pd.DataFrame:
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
        return pd.DataFrame()

    posts = posts_all[have_cols].copy()
    if "platform_post_id" not in posts.columns and "post_uid" in posts.columns:
        posts["platform_post_id"] = posts["post_uid"].apply(extract_platform_post_id)
    posts["platform_post_id"] = posts["platform_post_id"].apply(_clean_id)
    posts = posts[posts["platform_post_id"].astype(str).str.strip().ne("")]
    posts = posts.drop_duplicates(subset=["platform_post_id"], keep="first")
    if "created_at" in posts.columns:
        posts = posts.sort_values(by="created_at", ascending=True)
    return posts


def _add_csv_parent_columns(posts: pd.DataFrame) -> pd.DataFrame:
    posts = posts.copy()
    posts["csv_fields"] = posts["raw_text"].apply(parse_weibo_csv_raw_fields)
    posts["parent_post_id"] = posts["csv_fields"].apply(
        lambda item: extract_parent_post_id(csv_fields=item) if isinstance(item, dict) else ""
    )
    posts["parent_post_id"] = posts["parent_post_id"].apply(_clean_id)
    return posts


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


def _infer_missing_parent_ids(posts: pd.DataFrame) -> pd.DataFrame:
    if posts.empty:
        return posts

    posts_lookup: dict[str, dict] = posts.set_index("platform_post_id").to_dict(orient="index")
    text_index = _build_posts_text_index(posts_lookup) if posts_lookup else {}
    if not text_index:
        return posts

    posts = posts.copy()
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
    return posts


def _add_synthetic_sources(posts: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, dict[str, str]]]:
    # If a post quotes another speaker (display_md has multi segments), but we still
    # can't find parent_id, create a synthetic "source" parent so root is correct.
    synthetic_sources: dict[str, dict[str, str]] = {}
    if posts.empty or "display_md" not in posts.columns:
        return posts, synthetic_sources

    posts = posts.copy()
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
    return posts, synthetic_sources


def _attach_parent_info(posts: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, dict[str, str]]]:
    posts = _add_csv_parent_columns(posts)
    posts = _infer_missing_parent_ids(posts)
    return _add_synthetic_sources(posts)


def _build_assertions_by_post(view_df: pd.DataFrame) -> dict[str, list[dict]]:
    cols = ["idx", "action", "action_strength", "summary", "evidence", "confidence"]
    assertion_cols = [col for col in cols if col in view_df.columns]
    if not assertion_cols:
        return {}

    order_cols = [col for col in ["post_uid", "idx", "created_at"] if col in view_df.columns]
    ordered = view_df.sort_values(by=order_cols, ascending=True)

    assertions_by_post: dict[str, list[dict]] = {}
    for post_uid, group in ordered.groupby("post_uid"):
        records: list[dict] = []
        for _, row in group[assertion_cols].iterrows():
            record = {col: row.get(col) for col in assertion_cols}
            records.append(record)
        assertions_by_post[str(post_uid)] = records
    return assertions_by_post


def _build_nodes(
    posts: pd.DataFrame,
    *,
    assertions_by_post: dict[str, list[dict]],
    synthetic_sources: dict[str, dict[str, str]],
) -> dict[str, dict]:
    nodes: dict[str, dict] = {}
    for _, row in posts.iterrows():
        platform_post_id = _clean_id(row.get("platform_post_id"))
        if not platform_post_id:
            continue
        post_uid = str(row.get("post_uid") or "").strip()
        parent_post_id = _clean_id(row.get("parent_post_id"))
        nodes[platform_post_id] = {
            "platform_post_id": platform_post_id,
            "post_uid": post_uid,
            "created_at": row.get("created_at"),
            "author": str(row.get("author") or "").strip(),
            "url": str(row.get("url") or "").strip(),
            "raw_text": str(row.get("raw_text") or ""),
            "display_md": str(row.get("display_md") or ""),
            "csv_fields": row.get("csv_fields") if isinstance(row.get("csv_fields"), dict) else {},
            "parent_post_id": parent_post_id,
            "assertions": assertions_by_post.get(post_uid, []),
            "missing": False,
            "kind": "repost" if parent_post_id else "status",
        }

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
    return nodes


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


def _ensure_parent_stubs(nodes: dict[str, dict]) -> None:
    queue: list[str] = list(nodes.keys())
    while queue:
        node_id = queue.pop()
        node = nodes.get(node_id) or {}
        parent_id = _clean_id(node.get("parent_post_id"))
        if not parent_id or parent_id == node_id:
            continue
        if parent_id in nodes:
            continue

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


def _build_children_map(nodes: dict[str, dict]) -> dict[str, list[str]]:
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
    return children


def _find_root_id(start_id: str, *, nodes: dict[str, dict]) -> str:
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


def _collect_relevant_roots(selected_ids: set[str], *, nodes: dict[str, dict]) -> list[str]:
    relevant_roots: list[str] = []
    seen_roots: set[str] = set()
    for sid in selected_ids:
        if sid not in nodes:
            continue
        rid = _find_root_id(sid, nodes=nodes)
        if not rid or rid in seen_roots:
            continue
        seen_roots.add(rid)
        relevant_roots.append(rid)
    return relevant_roots


def _subtree_ids(root_id: str, *, children: dict[str, list[str]]) -> set[str]:
    stack = [root_id]
    subtree: set[str] = set()
    while stack:
        nid = stack.pop()
        if nid in subtree:
            continue
        subtree.add(nid)
        stack.extend(children.get(nid, []))
    return subtree


def _latest_activity(subtree: set[str], *, nodes: dict[str, dict]) -> pd.Timestamp | None:
    latest: pd.Timestamp | None = None
    for nid in subtree:
        ts = _to_ts((nodes.get(nid) or {}).get("created_at"))
        if ts is None:
            continue
        latest = ts if latest is None else max(latest, ts)
    return latest


def _thread_label(root_id: str, *, nodes: dict[str, dict], children: dict[str, list[str]]) -> str:
    root_node = nodes.get(root_id) or {}
    label_node_id = root_id
    if bool(root_node.get("missing")):
        first_child = (children.get(root_id) or [root_id])[0]
        label_node_id = first_child
    label_node = nodes.get(label_node_id) or root_node
    label_author = str(label_node.get("author") or "").strip() or "未知"
    label_ts = _format_ts(label_node.get("created_at"))
    label_preview = _short_text(label_node.get("raw_text"), n=40)
    return f"{label_ts} · {label_author} · {label_preview}".strip(" ·")


def _build_thread(
    root_id: str,
    *,
    nodes: dict[str, dict],
    children: dict[str, list[str]],
    blogger_authors: set[str],
) -> dict:
    subtree = _subtree_ids(root_id, children=children)
    latest = _latest_activity(subtree, nodes=nodes)
    tree_text, order = _render_ascii_tree(
        root_id,
        nodes=nodes,
        children=children,
        blogger_authors=blogger_authors,
    )
    label = _thread_label(root_id, nodes=nodes, children=children)
    return {
        "root_id": root_id,
        "label": label,
        "latest_activity": latest,
        "tree_text": tree_text,
        "order": order,
        "nodes": {nid: nodes[nid] for nid in subtree if nid in nodes},
    }


def build_weibo_thread_forest(view_df: pd.DataFrame, *, posts_all: pd.DataFrame) -> list[dict]:
    """
    Build parent-child thread forest for a selected topic/board.

    Important: one tree must be complete.
    - view_df only tells us which posts belong to the current filter (selected_ids)
    - real tree nodes come from posts_all (full posts table), so we can include
      siblings/children even if they have no assertions.
    """
    if view_df.empty or "post_uid" not in view_df.columns:
        return []

    selected_ids = _collect_selected_ids(view_df)
    if not selected_ids:
        return []

    posts = _prepare_posts(posts_all)
    if posts.empty:
        return []

    posts, synthetic_sources = _attach_parent_info(posts)
    assertions_by_post = _build_assertions_by_post(view_df)
    nodes = _build_nodes(posts, assertions_by_post=assertions_by_post, synthetic_sources=synthetic_sources)
    _ensure_parent_stubs(nodes)
    children = _build_children_map(nodes)
    blogger_authors = _collect_blogger_authors(selected_ids, nodes=nodes)

    roots = _collect_relevant_roots(selected_ids, nodes=nodes)
    threads = [
        _build_thread(
            root_id,
            nodes=nodes,
            children=children,
            blogger_authors=blogger_authors,
        )
        for root_id in roots
    ]
    threads.sort(
        key=lambda item: item.get("latest_activity")
        if item.get("latest_activity") is not None
        else pd.Timestamp.min,
        reverse=True,
    )
    return threads


__all__ = ["build_weibo_thread_forest"]
