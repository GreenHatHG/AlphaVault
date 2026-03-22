"""Streamlit page: topic cluster admin."""

from __future__ import annotations

from typing import Dict

import pandas as pd
import streamlit as st
from sqlalchemy.engine import Engine

from alphavault.topic_cluster import (
    delete_cluster,
    delete_cluster_topics,
    ensure_cluster_schema,
    upsert_cluster,
    upsert_cluster_topics,
)
from alphavault.ui.topic_cluster_admin_ai_core import _render_ai_section
from alphavault.ui.topic_cluster_admin_helpers import _build_cluster_display_maps, _format_cluster_label


def _maybe_init_cluster_tables(engine: Engine, load_error: str) -> None:
    if not load_error:
        return
    st.warning(f"聚合表可能还没初始化，或读取失败：{load_error}")
    if not st.button("初始化聚合表（创建缺失表）", type="primary"):
        return
    try:
        ensure_cluster_schema(engine)
    except Exception as exc:
        st.error(f"初始化失败：{type(exc).__name__}: {exc}")
        st.stop()
    st.cache_data.clear()
    st.rerun()


def _render_cluster_upsert_form(engine: Engine) -> None:
    st.markdown("**创建 / 更新板块**")
    with st.form("cluster_upsert_form", clear_on_submit=False):
        cluster_key = st.text_input("板块ID（cluster_key）", value="metal")
        cluster_name = st.text_input("板块名字（cluster_name）", value="金属")
        description = st.text_area("说明（可空）", value="")
        submitted = st.form_submit_button("保存")
        if not submitted:
            return
        if not str(cluster_key or "").strip():
            st.error("板块ID 不能为空。")
            st.stop()
        if not str(cluster_name or "").strip():
            st.error("板块名字 不能为空。")
            st.stop()
        try:
            ensure_cluster_schema(engine)
            upsert_cluster(
                engine,
                cluster_key=cluster_key,
                cluster_name=cluster_name,
                description=description,
            )
        except Exception as exc:
            st.error(f"保存失败：{type(exc).__name__}: {exc}")
            st.stop()
        st.success("已保存。")
        st.cache_data.clear()
        st.rerun()


def _select_cluster(clusters: pd.DataFrame, *, name_by_key: Dict[str, str]) -> str:
    cluster_keys = sorted(
        [str(x).strip() for x in clusters["cluster_key"].dropna().tolist() if str(x).strip()]
    )
    selected_cluster = st.selectbox(
        "选择板块",
        options=cluster_keys,
        format_func=lambda key: _format_cluster_label(str(key), name_by_key),
        key="topic_cluster_selected_cluster",
    )
    return str(selected_cluster or "").strip()


def _render_member_add(
    *,
    engine: Engine,
    assertions_all: pd.DataFrame,
    selected_cluster: str,
) -> None:
    st.markdown("**增加**")
    all_topic_keys = sorted(
        [
            str(x).strip()
            for x in assertions_all.get("topic_key", pd.Series(dtype=str)).dropna().unique().tolist()
            if str(x).strip()
        ]
    )
    search = (
        st.text_input("搜索 topic_key（建议先搜）", value="", key="topic_cluster_search_topic_key")
        .strip()
    )
    if search:
        candidates = [k for k in all_topic_keys if search.lower() in k.lower()]
        candidates = candidates[:300]
    else:
        candidates = []
        st.caption("提示：topic_key 可能很多，请先搜索再增加。")

    to_add = st.multiselect(
        "选择要增加的 topic_key",
        options=candidates,
        default=[],
        key="topic_cluster_to_add",
    )
    if not st.button("增加到这个板块", disabled=not bool(to_add)):
        return
    try:
        ensure_cluster_schema(engine)
        n = upsert_cluster_topics(engine, cluster_key=selected_cluster, topic_keys=to_add)
    except Exception as exc:
        st.error(f"增加失败：{type(exc).__name__}: {exc}")
        st.stop()
    st.success(f"已增加 {n} 个 topic_key。")
    st.cache_data.clear()
    st.rerun()


def _render_member_remove(
    *,
    engine: Engine,
    current_topics: list[str],
    selected_cluster: str,
) -> None:
    st.markdown("**移除**")
    to_remove = st.multiselect(
        "选择要移除的 topic_key",
        options=current_topics,
        default=[],
        key="topic_cluster_to_remove",
    )
    if not st.button("从板块移除", disabled=not bool(to_remove)):
        return
    try:
        ensure_cluster_schema(engine)
        n = delete_cluster_topics(engine, cluster_key=selected_cluster, topic_keys=to_remove)
    except Exception as exc:
        st.error(f"移除失败：{type(exc).__name__}: {exc}")
        st.stop()
    st.success(f"已移除 {n} 个 topic_key。")
    st.cache_data.clear()
    st.rerun()


def _render_current_member_list(current_topics: list[str]) -> None:
    st.markdown("**当前成员列表**")
    if current_topics:
        st.dataframe(
            pd.DataFrame({"topic_key": current_topics}).head(500),
            width="stretch",
            hide_index=True,
        )
    else:
        st.info("这个板块还没有成员。")


def _render_cluster_delete(
    *,
    engine: Engine,
    selected_cluster: str,
    cluster_name: str,
) -> None:
    st.markdown("**删除板块**")
    title = f"{selected_cluster} · {cluster_name}" if cluster_name and cluster_name != selected_cluster else selected_cluster
    st.caption(f"你要删：{title}")
    st.caption("注意：不会自动删“关注页”。如果你有关注这个板块，要去“关注页”手动删。")

    confirm_key = f"topic_cluster_delete_confirm:{selected_cluster}"
    btn_key = f"topic_cluster_delete_btn:{selected_cluster}"
    confirm = st.checkbox("我确认要删除这个板块", value=False, key=confirm_key)
    if not st.button("删除这个板块", type="secondary", disabled=not bool(confirm), key=btn_key):
        return

    try:
        ensure_cluster_schema(engine)
        deleted = delete_cluster(engine, cluster_key=selected_cluster)
    except Exception as exc:
        st.error(f"删除失败：{type(exc).__name__}: {exc}")
        st.stop()

    st.success(
        "已删除。"
        + f" clusters={deleted.get('clusters', 0)}"
        + f", topics={deleted.get('topics', 0)}"
        + f", overrides={deleted.get('overrides', 0)}"
    )
    st.session_state.pop("topic_cluster_selected_cluster", None)
    st.cache_data.clear()
    st.rerun()


def show_topic_cluster_admin(
    *,
    engine: Engine,
    assertions_all: pd.DataFrame,
    clusters: pd.DataFrame,
    topic_map: pd.DataFrame,
    load_error: str,
) -> None:
    st.markdown("**主题聚合（板块）**")
    st.caption("把很多 topic_key 合成一个板块看；不会改旧 topic_key。")

    _maybe_init_cluster_tables(engine, load_error)

    st.divider()
    _render_cluster_upsert_form(engine)

    st.divider()
    st.markdown("**成员管理（topic_key → 板块）**")
    if clusters.empty:
        st.info("还没有板块。先在上面创建一个。")
        return

    name_by_key, desc_by_key = _build_cluster_display_maps(clusters)
    selected_cluster = _select_cluster(clusters, name_by_key=name_by_key)
    if not selected_cluster:
        st.info("请选择一个板块。")
        return

    current = topic_map[topic_map["cluster_key"] == selected_cluster].copy() if not topic_map.empty else pd.DataFrame()
    current_topics = sorted(
        [
            str(x).strip()
            for x in current.get("topic_key", pd.Series(dtype=str)).dropna().tolist()
            if str(x).strip()
        ]
    )

    col_left, col_mid = st.columns([2, 1])
    with col_left:
        st.caption("v2 规则：一个 topic_key 可以属于多个板块；加入不会自动移走。")
    with col_mid:
        st.metric("当前成员数", f"{len(current_topics)}")

    action = st.radio("操作", options=["增加", "移除"], horizontal=True, key="topic_cluster_manage_action")
    if action == "增加":
        _render_member_add(
            engine=engine,
            assertions_all=assertions_all,
            selected_cluster=selected_cluster,
        )
    else:
        _render_member_remove(engine=engine, current_topics=current_topics, selected_cluster=selected_cluster)

    _render_current_member_list(current_topics)

    st.divider()
    cluster_name = (name_by_key.get(selected_cluster) or selected_cluster).strip()
    cluster_desc = (desc_by_key.get(selected_cluster) or "").strip()
    _render_cluster_delete(
        engine=engine,
        selected_cluster=selected_cluster,
        cluster_name=cluster_name,
    )

    st.divider()
    _render_ai_section(
        engine=engine,
        assertions_all=assertions_all,
        topic_map=topic_map,
        selected_cluster=selected_cluster,
        cluster_name=cluster_name,
        cluster_desc=cluster_desc,
    )


__all__ = ["show_topic_cluster_admin"]
