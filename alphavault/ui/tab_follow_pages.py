from __future__ import annotations

"""
Streamlit tab: follow pages (topic / cluster).

User flow:
1) Create a page: follow topic_key or cluster_key
2) Keywords OR (optional)
3) Render thread tree (reuse existing tree builder)
"""

import re

import pandas as pd
import streamlit as st
from sqlalchemy.engine import Engine

from alphavault.db.turso_db import ensure_turso_engine
from alphavault.follow_pages import (
    FOLLOW_TYPE_CLUSTER,
    FOLLOW_TYPE_TOPIC,
    delete_follow_page,
    ensure_follow_pages_schema,
    try_load_follow_pages,
    upsert_follow_page,
)
from alphavault.ui.keyword_or import split_keywords_or
from alphavault.ui.thread_tree import build_weibo_thread_forest
from alphavault.ui.thread_tree_view import render_thread_forest


DEFAULT_TOPIC_CANDIDATES_TOP_N = 30
MAX_TOPIC_CANDIDATES = 300


@st.cache_data(show_spinner=False)
def load_follow_pages_sources(db_url: str, auth_token: str) -> tuple[pd.DataFrame, str]:
    if not db_url:
        return pd.DataFrame(), "Missing TURSO_DATABASE_URL"
    engine = ensure_turso_engine(db_url, auth_token)
    return try_load_follow_pages(engine)


def _maybe_init_follow_pages_tables(engine: Engine, load_error: str) -> None:
    if not load_error:
        return
    st.warning(f"关注页表可能还没初始化，或读取失败：{load_error}")
    if not st.button("初始化关注页表（创建缺失表）", type="primary"):
        return
    try:
        ensure_follow_pages_schema(engine)
    except Exception as exc:
        st.error(f"初始化失败：{type(exc).__name__}: {exc}")
        st.stop()
    st.cache_data.clear()
    st.rerun()


def _ai_keywords_for_cluster(cluster_key: str) -> list[str]:
    key = str(cluster_key or "").strip()
    if not key:
        return []
    ai_result = st.session_state.get(f"cluster_ai_result:{key}", None)
    if not isinstance(ai_result, dict):
        return []
    raw = ai_result.get("keywords")
    if not isinstance(raw, list):
        return []
    words = [str(x).strip() for x in raw if str(x).strip()]
    # keep unique
    out: list[str] = []
    seen: set[str] = set()
    for w in words:
        if w in seen:
            continue
        seen.add(w)
        out.append(w)
    return out


def _format_cluster_label(clusters: pd.DataFrame, cluster_key: str) -> str:
    key = str(cluster_key or "").strip()
    if not key:
        return "板块"
    if clusters.empty or "cluster_key" not in clusters.columns:
        return key
    row = clusters[clusters["cluster_key"].astype(str).str.strip() == key].head(1)
    if row.empty:
        return key
    name = str(row.get("cluster_name", pd.Series([""])).iloc[0] or "").strip()
    return f"{key} · {name}" if name else key


def _format_page_label(clusters: pd.DataFrame, row: pd.Series) -> str:
    follow_type = str(row.get("follow_type") or "").strip()
    follow_key = str(row.get("follow_key") or "").strip()
    page_name = str(row.get("page_name") or "").strip()

    if follow_type == FOLLOW_TYPE_CLUSTER:
        base = _format_cluster_label(clusters, follow_key)
    else:
        base = follow_key

    if page_name:
        return f"{base} · {page_name}"
    return base or "关注页"


def _topic_candidates(assertions_filtered: pd.DataFrame) -> pd.Series:
    if assertions_filtered.empty or "topic_key" not in assertions_filtered.columns:
        return pd.Series(dtype=int)
    s = assertions_filtered["topic_key"].dropna().astype(str).str.strip()
    s = s[s.ne("")]
    if s.empty:
        return pd.Series(dtype=int)
    return s.value_counts()


def _select_topic_key(assertions_filtered: pd.DataFrame, *, key_prefix: str) -> str:
    st.caption("提示：topic_key 可能很多，建议先搜索。")
    search = st.text_input("搜索 topic_key", value="", key=f"{key_prefix}:topic_search").strip()

    counts = _topic_candidates(assertions_filtered)
    if counts.empty:
        st.info("没有 topic_key 数据。")
        return ""

    if search:
        matched = [k for k in counts.index.tolist() if search.lower() in str(k).lower()]
        matched = matched[:MAX_TOPIC_CANDIDATES]
        options = matched
        st.caption(f"搜索命中 {len(options)} 个（最多展示 {MAX_TOPIC_CANDIDATES} 个）")
    else:
        head = counts.head(DEFAULT_TOPIC_CANDIDATES_TOP_N)
        options = [str(k) for k in head.index.tolist()]
        st.caption(f"默认展示 Top {DEFAULT_TOPIC_CANDIDATES_TOP_N}")

    selected = st.selectbox("选择 topic_key", options=options, key=f"{key_prefix}:topic_select")
    return str(selected or "").strip()


def _select_cluster_key(clusters: pd.DataFrame, assertions_filtered: pd.DataFrame, *, key_prefix: str) -> str:
    if not clusters.empty and "cluster_key" in clusters.columns:
        options = sorted(
            [
                str(x).strip()
                for x in clusters["cluster_key"].dropna().tolist()
                if str(x).strip()
            ]
        )
    else:
        # Fallback: from assertions (only non-empty keys).
        options: list[str] = []
        if "cluster_key" in assertions_filtered.columns:
            options = sorted(
                [
                    str(x).strip()
                    for x in assertions_filtered["cluster_key"].dropna().tolist()
                    if str(x).strip()
                ]
            )
        elif "cluster_keys" in assertions_filtered.columns:
            raw = assertions_filtered["cluster_keys"].tolist()
            for item in raw:
                if not isinstance(item, list):
                    continue
                for k in item:
                    s = str(k).strip()
                    if s:
                        options.append(s)
            options = sorted(list(set(options)))
        else:
            return ""

    if not options:
        st.info("没有板块数据。先去“主题聚合”建一个板块。")
        return ""

    selected = st.selectbox(
        "选择板块（cluster_key）",
        options=options,
        format_func=lambda x: _format_cluster_label(clusters, str(x)),
        key=f"{key_prefix}:cluster_select",
    )
    return str(selected or "").strip()


def _build_keyword_post_uids(assertions_filtered: pd.DataFrame, *, keywords_text: str) -> set[str]:
    words = split_keywords_or(keywords_text)
    if not words:
        return set()
    escaped = [re.escape(w) for w in words]
    pattern = "|".join(escaped)

    if "post_uid" not in assertions_filtered.columns or "raw_text" not in assertions_filtered.columns:
        return set()

    post_text = assertions_filtered[["post_uid", "raw_text"]].copy()
    post_text["post_uid"] = post_text["post_uid"].astype(str).str.strip()
    post_text = post_text[post_text["post_uid"].ne("")]
    post_text = post_text.drop_duplicates(subset=["post_uid"], keep="first")

    mask = post_text["raw_text"].astype(str).str.contains(pattern, case=False, na=False, regex=True)
    return set(post_text.loc[mask, "post_uid"].tolist())


def _render_page_create(
    engine: Engine,
    *,
    clusters: pd.DataFrame,
    assertions_filtered: pd.DataFrame,
) -> None:
    st.markdown("**新建关注页**")
    with st.form("follow_pages_create_form", clear_on_submit=False):
        follow_type_label = st.radio(
            "关注类型",
            options=[FOLLOW_TYPE_TOPIC, FOLLOW_TYPE_CLUSTER],
            format_func=lambda x: "主题（topic_key）" if x == FOLLOW_TYPE_TOPIC else "板块（聚合）",
            horizontal=True,
        )

        if follow_type_label == FOLLOW_TYPE_CLUSTER:
            follow_key = _select_cluster_key(clusters, assertions_filtered, key_prefix="follow_pages_create")
        else:
            follow_key = _select_topic_key(assertions_filtered, key_prefix="follow_pages_create")

        page_name = st.text_input("页面名字（可空）", value="")

        default_keywords = ""
        if follow_type_label == FOLLOW_TYPE_CLUSTER and follow_key:
            words = _ai_keywords_for_cluster(follow_key)
            if words:
                default_keywords = "\n".join(words)
            else:
                st.caption("提示：没找到 AI keywords。你可以去“主题聚合”页点 AI，再回来。")

        keywords_widget_key = f"follow_pages_create_keywords:{follow_type_label}:{follow_key}"
        keywords_text = st.text_area(
            "关键字（多个，OR；可空）",
            value=default_keywords,
            height=80,
            key=keywords_widget_key,
        )

        submitted = st.form_submit_button("保存", type="primary")
        if not submitted:
            return
        if not str(follow_key or "").strip():
            st.error("你要先选一个 topic_key 或 cluster_key。")
            st.stop()
        try:
            ensure_follow_pages_schema(engine)
            page_key = upsert_follow_page(
                engine,
                follow_type=follow_type_label,
                follow_key=follow_key,
                page_name=page_name,
                keywords_text=keywords_text,
            )
        except Exception as exc:
            st.error(f"保存失败：{type(exc).__name__}: {exc}")
            st.stop()
        st.success("已保存。")
        st.session_state["follow_pages_selected_page_key"] = page_key
        st.cache_data.clear()
        st.rerun()


def _render_page_update(
    engine: Engine,
    *,
    clusters: pd.DataFrame,
    page_key: str,
    follow_type: str,
    follow_key: str,
    page_name: str,
    keywords_text: str,
) -> None:
    st.markdown("**页面设置**")

    widget_key = f"follow_pages_update_keywords:{page_key}"
    if follow_type == FOLLOW_TYPE_CLUSTER and not str(keywords_text or "").strip():
        words = _ai_keywords_for_cluster(follow_key)
        if words and not str(st.session_state.get(widget_key, "")).strip():
            st.session_state[widget_key] = "\n".join(words)

    with st.form(f"follow_pages_update_form:{page_key}", clear_on_submit=False):
        st.caption(f"page_key：{page_key}")
        st.caption(
            f"关注：{'板块' if follow_type == FOLLOW_TYPE_CLUSTER else '主题'} · "
            + (_format_cluster_label(clusters, follow_key) if follow_type == FOLLOW_TYPE_CLUSTER else follow_key)
        )

        page_name_new = st.text_input("页面名字（可空）", value=str(page_name or ""))
        keywords_new = st.text_area(
            "关键字（多个，OR；可空）",
            value=str(keywords_text or ""),
            height=80,
            key=widget_key,
        )
        submitted = st.form_submit_button("保存设置")
        if not submitted:
            return
        try:
            ensure_follow_pages_schema(engine)
            upsert_follow_page(
                engine,
                follow_type=follow_type,
                follow_key=follow_key,
                page_name=page_name_new,
                keywords_text=keywords_new,
            )
        except Exception as exc:
            st.error(f"保存失败：{type(exc).__name__}: {exc}")
            st.stop()
        st.success("已保存。")
        st.cache_data.clear()
        st.rerun()


def show_follow_pages(
    *,
    turso_url: str,
    turso_token: str,
    posts_all: pd.DataFrame,
    assertions_filtered: pd.DataFrame,
    clusters: pd.DataFrame,
) -> None:
    st.markdown("**关注页（无代码）**")
    st.caption("关注 topic_key / 板块（聚合） → 关键字 OR → tree。")

    engine = ensure_turso_engine(turso_url, turso_token)
    pages_df, load_error = load_follow_pages_sources(turso_url, turso_token)

    _maybe_init_follow_pages_tables(engine, load_error)

    st.divider()
    if pages_df.empty:
        st.info("还没有关注页。先新建一个。")
        _render_page_create(engine, clusters=clusters, assertions_filtered=assertions_filtered)
        return

    pages_df = pages_df.copy()
    pages_df["page_key"] = pages_df["page_key"].astype(str).str.strip()
    pages_df["follow_type"] = pages_df["follow_type"].astype(str).str.strip()
    pages_df["follow_key"] = pages_df["follow_key"].astype(str).str.strip()
    if "page_name" not in pages_df.columns:
        pages_df["page_name"] = ""
    else:
        pages_df["page_name"] = pages_df["page_name"].astype(str)
    pages_df = pages_df[pages_df["page_key"].ne("")]

    page_keys = pages_df["page_key"].dropna().tolist()
    labels = {
        str(row["page_key"]): _format_page_label(clusters, row)
        for _, row in pages_df.iterrows()
        if str(row.get("page_key") or "").strip()
    }

    default_key = st.session_state.get("follow_pages_selected_page_key", None)
    if default_key not in page_keys:
        default_key = page_keys[0] if page_keys else None

    selected_page_key = st.selectbox(
        "选择关注页",
        options=page_keys,
        index=page_keys.index(default_key) if default_key in page_keys else 0,
        format_func=lambda k: labels.get(str(k), str(k)),
        key="follow_pages_selected_page_key_selectbox",
    )
    selected_page_key = str(selected_page_key or "").strip()
    if not selected_page_key:
        st.info("请选择一个关注页。")
        return

    st.session_state["follow_pages_selected_page_key"] = selected_page_key

    row = pages_df[pages_df["page_key"] == selected_page_key].head(1)
    if row.empty:
        st.info("页面不存在。")
        return
    follow_type = str(row["follow_type"].iloc[0] or "").strip()
    follow_key = str(row["follow_key"].iloc[0] or "").strip()
    page_name = str(row.get("page_name", pd.Series([""])).iloc[0] or "").strip()
    keywords_text = str(row.get("keywords_text", pd.Series([""])).iloc[0] or "")

    _render_page_update(
        engine,
        clusters=clusters,
        page_key=selected_page_key,
        follow_type=follow_type,
        follow_key=follow_key,
        page_name=page_name,
        keywords_text=keywords_text,
    )

    col_left, col_right = st.columns([2, 1])
    with col_left:
        with st.expander("新建关注页"):
            _render_page_create(engine, clusters=clusters, assertions_filtered=assertions_filtered)
    with col_right:
        confirm = st.checkbox("我确认要删除", value=False, key=f"follow_pages_delete_confirm:{selected_page_key}")
        if st.button(
            "删除这个关注页",
            type="secondary",
            disabled=not bool(confirm),
            key=f"follow_pages_delete_btn:{selected_page_key}",
        ):
            try:
                ensure_follow_pages_schema(engine)
                n = delete_follow_page(engine, page_key=selected_page_key)
            except Exception as exc:
                st.error(f"删除失败：{type(exc).__name__}: {exc}")
                st.stop()
            st.success(f"已删除 {n} 条。")
            st.session_state.pop("follow_pages_selected_page_key", None)
            st.cache_data.clear()
            st.rerun()

    st.divider()
    st.markdown("**tree**")

    if assertions_filtered.empty:
        st.info("当前筛选下没有观点数据。")
        return

    if follow_type == FOLLOW_TYPE_CLUSTER:
        if "cluster_key" in assertions_filtered.columns:
            base_view = assertions_filtered[
                assertions_filtered["cluster_key"].astype(str).str.strip() == follow_key
            ]
        elif "cluster_keys" in assertions_filtered.columns:
            want = str(follow_key or "").strip()
            if want:
                cluster_lists = assertions_filtered["cluster_keys"].tolist()
                mask = []
                for item in cluster_lists:
                    if not isinstance(item, list):
                        mask.append(False)
                        continue
                    keys = [str(x).strip() for x in item if str(x).strip()]
                    mask.append(want in keys)
                base_view = assertions_filtered[pd.Series(mask, index=assertions_filtered.index)]
            else:
                base_view = assertions_filtered.head(0).copy()
        else:
            base_view = assertions_filtered.head(0).copy()
    else:
        if "topic_key" not in assertions_filtered.columns:
            base_view = assertions_filtered.head(0).copy()
        else:
            base_view = assertions_filtered[
                assertions_filtered["topic_key"].astype(str).str.strip() == follow_key
            ]

    base_post_uids = set(
        str(x).strip()
        for x in base_view.get("post_uid", pd.Series(dtype=str)).dropna().tolist()
        if str(x).strip()
    )
    keyword_post_uids = _build_keyword_post_uids(assertions_filtered, keywords_text=str(keywords_text or ""))
    merged_uids = base_post_uids | keyword_post_uids

    st.caption(
        f"关注命中 {len(base_post_uids)} 帖，关键字命中 {len(keyword_post_uids)} 帖，合并后 {len(merged_uids)} 帖"
    )

    if not merged_uids:
        st.info("暂无数据。")
        return

    view_df = assertions_filtered[assertions_filtered["post_uid"].astype(str).str.strip().isin(merged_uids)]
    threads_all = build_weibo_thread_forest(view_df, posts_all=posts_all)
    render_thread_forest(
        threads_all,
        title="tree（像 1.txt 那样）",
        key_prefix=f"follow_pages_tree:{selected_page_key}",
    )


__all__ = ["show_follow_pages"]
