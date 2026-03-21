from __future__ import annotations

"""
Thread tree rendering (Streamlit UI).

Input is the output of build_weibo_thread_forest(...).
This module only renders UI; it does not parse data.
"""

import math

import streamlit as st


DEFAULT_MAX_THREADS = 1000
DEFAULT_PAGE_SIZE = 10
MAX_PAGE_SIZE = 50


def render_thread_forest(
    threads_all: list[dict],
    *,
    title: str = "tree（像 1.txt 那样）",
    max_threads: int = DEFAULT_MAX_THREADS,
    default_page_size: int = DEFAULT_PAGE_SIZE,
    key_prefix: str = "thread_tree",
) -> None:
    st.markdown(f"**{title}**")
    if not threads_all:
        st.info("当前筛选下，没有可用的 tree。")
        return

    threads = threads_all[: max(0, int(max_threads))]
    total = len(threads)
    if total <= 0:
        st.info("当前筛选下，没有可用的 tree。")
        return

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        page_size = int(
            st.number_input(
                "一页多少棵",
                min_value=1,
                max_value=MAX_PAGE_SIZE,
                value=int(default_page_size),
                step=1,
                key=f"{key_prefix}:page_size",
            )
        )
    total_pages = max(1, int(math.ceil(total / max(1, page_size))))
    with col2:
        page = st.selectbox(
            "第几页",
            list(range(1, total_pages + 1)),
            key=f"{key_prefix}:page",
        )
    with col3:
        st.caption(f"共有 {total} 棵（最多展示前 {max_threads} 棵）")

    start_idx = (int(page) - 1) * page_size
    end_idx = min(start_idx + page_size, total)
    st.caption(f"本页：{start_idx + 1} - {end_idx}")

    for i in range(start_idx, end_idx):
        thread = threads[i]
        label = (
            str(thread.get("label") or "").strip()
            or str(thread.get("root_id") or "").strip()
            or "tree"
        )
        st.markdown(f"**{i + 1}. {label}**")

        tree_text = str(thread.get("tree_text") or "").rstrip()
        if tree_text.strip():
            st.code(tree_text, language="text")
        else:
            st.info("tree 为空。")

        if i + 1 < end_idx:
            st.divider()


__all__ = [
    "DEFAULT_MAX_THREADS",
    "DEFAULT_PAGE_SIZE",
    "MAX_PAGE_SIZE",
    "render_thread_forest",
]
