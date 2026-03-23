from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy.engine import Engine

from alphavault.topic_cluster import (
    ensure_cluster_schema,
    upsert_cluster_topics_detailed,
)
from alphavault.ui.topic_cluster_admin_helpers import (
    _format_basic_topic,
    _parse_confidence,
    _sort_by_count,
)


def _render_ai_write_section(
    *,
    engine: Engine,
    topic_map: pd.DataFrame,
    selected_cluster: str,
    include_items: list[dict],
    unsure_items: list[dict],
    count_by_topic: dict[str, int],
) -> None:
    st.markdown("**写入板块成员**")
    st.caption("include 默认全选；unsure 默认不选。只会加入，不会移走。")

    selected_cluster = str(selected_cluster or "").strip()
    if not selected_cluster:
        st.info("请先选一个板块。")
        return

    existing_in_cluster: set[str] = set()
    if (
        not topic_map.empty
        and "topic_key" in topic_map.columns
        and "cluster_key" in topic_map.columns
    ):
        df = topic_map[["topic_key", "cluster_key"]].dropna().copy()
        df["topic_key"] = df["topic_key"].astype(str).str.strip()
        df["cluster_key"] = df["cluster_key"].astype(str).str.strip()
        existing_in_cluster = set(
            df.loc[df["cluster_key"] == selected_cluster, "topic_key"].tolist()
        )

    include_conf_by_topic: dict[str, float] = {}
    for item in include_items:
        key = str(item.get("key") or item.get("topic_key") or "").strip()
        if key:
            include_conf_by_topic[key] = _parse_confidence(
                item.get("confidence", None), 0.8
            )

    unsure_conf_by_topic: dict[str, float] = {}
    for item in unsure_items:
        key = str(item.get("key") or item.get("topic_key") or "").strip()
        if key and key not in include_conf_by_topic:
            unsure_conf_by_topic[key] = _parse_confidence(
                item.get("confidence", None), 0.55
            )

    include_keys_raw = [
        str(item.get("key") or item.get("topic_key") or "").strip()
        for item in include_items
        if str(item.get("key") or item.get("topic_key") or "").strip()
    ]
    include_keys = sorted(
        set([k for k in include_keys_raw if k and k not in existing_in_cluster])
    )
    include_keys = _sort_by_count(include_keys, count_by_topic=count_by_topic)

    unsure_keys_raw = [
        str(item.get("key") or item.get("topic_key") or "").strip()
        for item in unsure_items
        if str(item.get("key") or item.get("topic_key") or "").strip()
    ]
    unsure_keys = sorted(
        set(
            [
                k
                for k in unsure_keys_raw
                if k and k not in existing_in_cluster and k not in set(include_keys)
            ]
        )
    )
    unsure_keys = _sort_by_count(unsure_keys, count_by_topic=count_by_topic)

    st.markdown("**include（默认全选）**")
    include_selected = st.multiselect(
        "选择要加入本板块的 key",
        options=include_keys,
        default=include_keys,
        format_func=lambda k: _format_basic_topic(
            str(k), count_by_topic=count_by_topic
        ),
        key=f"cluster_ai_write_include:{selected_cluster}",
    )

    st.markdown("**unsure（默认不选）**")
    unsure_selected = st.multiselect(
        "选择要加入（unsure）的 key",
        options=unsure_keys,
        default=[],
        format_func=lambda k: _format_basic_topic(
            str(k), count_by_topic=count_by_topic
        ),
        key=f"cluster_ai_write_unsure:{selected_cluster}",
    )

    to_write = [
        *[str(x).strip() for x in include_selected],
        *[str(x).strip() for x in unsure_selected],
    ]
    to_write = [x for x in to_write if x]
    to_write = sorted(set(to_write))
    st.caption(f"本次将写入：{len(to_write)} 个 key")

    if not st.button("确认加入这个板块", type="primary", disabled=not bool(to_write)):
        return

    try:
        ensure_cluster_schema(engine)
        payloads: list[dict] = []
        for key in to_write:
            conf = include_conf_by_topic.get(key)
            if conf is None:
                conf = unsure_conf_by_topic.get(key, 0.75)
            # Note: DB column name stays "topic_key", but we store the generic member key.
            payloads.append(
                {"topic_key": key, "source": "ai", "confidence": float(conf)}
            )
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

    st.success(f"已写入 {n} 个 key。")
    st.cache_data.clear()
    st.rerun()
