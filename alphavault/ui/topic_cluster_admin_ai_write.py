from __future__ import annotations

from typing import Dict

import pandas as pd
import streamlit as st
from sqlalchemy.engine import Engine

from alphavault.topic_cluster import ensure_cluster_schema, upsert_cluster_topics_detailed
from alphavault.ui.topic_cluster_admin_helpers import (
    _format_basic_topic,
    _format_move_topic,
    _parse_confidence,
    _sort_by_count,
    _split_new_and_move,
)


def _render_ai_write_section(
    *,
    engine: Engine,
    topic_map: pd.DataFrame,
    selected_cluster: str,
    include_items: list[dict],
    unsure_items: list[dict],
    count_by_topic: dict[str, int],
    name_by_key: Dict[str, str],
) -> None:
    st.markdown("**写入板块成员**")
    st.caption("先选，再写入：新增默认全选；已在别的板块默认不选；unsure 默认不选。")

    # Build a quick topic_key -> cluster_keys map for grouping (new vs existing).
    topic_to_clusters: dict[str, list[str]] = {}
    if not topic_map.empty and "topic_key" in topic_map.columns and "cluster_key" in topic_map.columns:
        for _, row in topic_map[["topic_key", "cluster_key"]].dropna().iterrows():
            topic_key = str(row.get("topic_key") or "").strip()
            cluster_key = str(row.get("cluster_key") or "").strip()
            if topic_key and cluster_key:
                topic_to_clusters.setdefault(topic_key, []).append(cluster_key)
    if topic_to_clusters:
        for topic_key, keys in list(topic_to_clusters.items()):
            topic_to_clusters[topic_key] = sorted(set(str(k).strip() for k in keys if str(k).strip()))

    include_conf_by_topic: dict[str, float] = {}
    for item in include_items:
        topic_key = str(item.get("topic_key") or "").strip()
        if topic_key:
            include_conf_by_topic[topic_key] = _parse_confidence(item.get("confidence", None), 0.8)

    unsure_conf_by_topic: dict[str, float] = {}
    for item in unsure_items:
        topic_key = str(item.get("topic_key") or "").strip()
        if topic_key and topic_key not in include_conf_by_topic:
            unsure_conf_by_topic[topic_key] = _parse_confidence(item.get("confidence", None), 0.55)

    include_keys = [
        str(item.get("topic_key") or "").strip()
        for item in include_items
        if str(item.get("topic_key") or "").strip()
    ]
    include_keys = sorted(set(include_keys))
    include_new, include_move, include_from = _split_new_and_move(
        include_keys,
        topic_to_clusters=topic_to_clusters,
        selected_cluster=selected_cluster,
    )

    unsure_keys = [
        str(item.get("topic_key") or "").strip()
        for item in unsure_items
        if str(item.get("topic_key") or "").strip()
    ]
    unsure_keys = sorted(set(unsure_keys))
    unsure_new, unsure_move, unsure_from = _split_new_and_move(
        unsure_keys,
        topic_to_clusters=topic_to_clusters,
        selected_cluster=selected_cluster,
    )

    include_new = _sort_by_count(include_new, count_by_topic=count_by_topic)
    include_move = _sort_by_count(include_move, count_by_topic=count_by_topic)
    unsure_new = _sort_by_count(unsure_new, count_by_topic=count_by_topic)
    unsure_move = _sort_by_count(unsure_move, count_by_topic=count_by_topic)

    from_clusters_by_topic = {**include_from, **unsure_from}

    col_w1, col_w2 = st.columns(2)
    with col_w1:
        st.markdown("**include：新增**")
        include_new_selected = st.multiselect(
            "选择要新增进本板块的 topic_key",
            options=include_new,
            default=include_new,
            format_func=lambda k: _format_basic_topic(str(k), count_by_topic=count_by_topic),
            key=f"cluster_ai_write_include_new:{selected_cluster}",
        )
    with col_w2:
        st.markdown("**include：已在别的板块**")
        st.caption("提示：只会“也加入”，不会移走。")
        include_move_selected = st.multiselect(
            "选择要加入本板块的 topic_key",
            options=include_move,
            default=[],
            format_func=lambda k: _format_move_topic(
                str(k),
                from_clusters_by_topic=from_clusters_by_topic,
                name_by_key=name_by_key,
                count_by_topic=count_by_topic,
            ),
            key=f"cluster_ai_write_include_move:{selected_cluster}",
        )

    want_unsure = st.checkbox(
        "我也要选 unsure",
        value=False,
        key=f"cluster_ai_write_want_unsure:{selected_cluster}",
    )
    unsure_new_selected: list[str] = []
    unsure_move_selected: list[str] = []
    if want_unsure and (unsure_new or unsure_move):
        col_u1, col_u2 = st.columns(2)
        with col_u1:
            st.markdown("**unsure：新增**")
            unsure_new_selected = st.multiselect(
                "选择要新增（unsure）的 topic_key",
                options=unsure_new,
                default=[],
                format_func=lambda k: _format_basic_topic(str(k), count_by_topic=count_by_topic),
                key=f"cluster_ai_write_unsure_new:{selected_cluster}",
            )
        with col_u2:
            st.markdown("**unsure：已在别的板块**")
            unsure_move_selected = st.multiselect(
                "选择要加入（unsure）的 topic_key",
                options=unsure_move,
                default=[],
                format_func=lambda k: _format_move_topic(
                    str(k),
                    from_clusters_by_topic=from_clusters_by_topic,
                    name_by_key=name_by_key,
                    count_by_topic=count_by_topic,
                ),
                key=f"cluster_ai_write_unsure_move:{selected_cluster}",
            )

    to_write = [
        *[str(x).strip() for x in include_new_selected],
        *[str(x).strip() for x in include_move_selected],
        *[str(x).strip() for x in unsure_new_selected],
        *[str(x).strip() for x in unsure_move_selected],
    ]
    to_write = [x for x in to_write if x]
    to_write = sorted(set(to_write))
    st.caption(f"本次将写入：{len(to_write)} 个 topic_key")

    if not st.button("写入这个板块（按你勾选的）", disabled=not bool(to_write)):
        return

    try:
        ensure_cluster_schema(engine)
        payloads: list[dict] = []
        for topic_key in to_write:
            conf = include_conf_by_topic.get(topic_key)
            if conf is None:
                conf = unsure_conf_by_topic.get(topic_key, 0.75)
            payloads.append({"topic_key": topic_key, "source": "ai", "confidence": float(conf)})
        n = upsert_cluster_topics_detailed(
            engine,
            cluster_key=selected_cluster,
            topic_items=payloads,
            default_source="ai",
            default_confidence=0.75,
        )
    except Exception as exc:
        st.error(f"写入失败：{type(exc).__name__}: {exc}")
        st.stop()

    st.success(f"已写入 {n} 个 topic_key。")
    st.cache_data.clear()
    st.rerun()
