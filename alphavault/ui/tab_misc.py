from __future__ import annotations

"""
Streamlit tabs: misc.

This module groups the smaller tabs to keep files under control.
"""

from datetime import datetime, timedelta
import math

import pandas as pd
import streamlit as st

from alphavault.ui.thread_tree import build_weibo_thread_forest
from alphavault.weibo.display import format_weibo_display_md


def show_topic_timeline(
    posts_all: pd.DataFrame,
    assertions_filtered: pd.DataFrame,
    *,
    group_col: str,
    group_label: str,
) -> None:
    group_col = group_col if group_col in assertions_filtered.columns else "topic_key"
    st.markdown(f"**{group_label}时间线**")
    options = sorted(assertions_filtered[group_col].dropna().unique().tolist())
    if not options:
        st.info(f"暂无{group_label}数据。")
        return
    selected_key = st.selectbox(f"选择{group_label}", options)
    view_df = assertions_filtered[assertions_filtered[group_col] == selected_key]
    if view_df.empty:
        st.info("暂无数据。")
        return

    threads_all = build_weibo_thread_forest(view_df, posts_all=posts_all)

    st.markdown("**tree（像 1.txt 那样）**")
    if not threads_all:
        st.info("当前筛选下，没有可用的 tree。")
        return

    max_threads = 1000
    threads = threads_all[:max_threads]
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
                max_value=50,
                value=10,
                step=1,
            )
        )
    total_pages = max(1, int(math.ceil(total / max(1, page_size))))
    with col2:
        page = st.selectbox("第几页", list(range(1, total_pages + 1)))
    with col3:
        st.caption(f"共有 {total} 棵（最多展示前 {max_threads} 棵）")

    start_idx = (int(page) - 1) * page_size
    end_idx = min(start_idx + page_size, total)
    st.caption(f"本页：{start_idx + 1} - {end_idx}")

    for i in range(start_idx, end_idx):
        thread = threads[i]
        label = str(thread.get("label") or "").strip() or str(thread.get("root_id") or "").strip() or "tree"
        st.markdown(f"**{i + 1}. {label}**")

        tree_text = str(thread.get("tree_text") or "").rstrip()
        if tree_text.strip():
            st.code(tree_text, language="text")
        else:
            st.info("tree 为空。")

        if i + 1 < end_idx:
            st.divider()


def show_learning_library(assertions_filtered: pd.DataFrame) -> None:
    st.markdown("**学习库（方法论 / 心态 / 经验）**")
    education_mask = (
        assertions_filtered["action"].str.startswith("education.", na=False)
        | assertions_filtered["topic_key"].str.startswith("education", na=False)
        | assertions_filtered["topic_type"].isin(["method", "mindset", "life", "education"])
    )
    edu_df = assertions_filtered[education_mask].copy()
    if edu_df.empty:
        st.info("当前筛选下没有学习类观点。")
        return

    st.dataframe(
        edu_df.sort_values(by="created_at", ascending=False)[
            [
                "created_at",
                "author",
                "source",
                "topic_key",
                "action",
                "summary",
                "evidence",
                "url",
            ]
        ],
        width="stretch",
        hide_index=True,
    )


def show_conflicts_and_changes(assertions_filtered: pd.DataFrame, *, group_col: str, group_label: str) -> None:
    st.markdown("**观点变化 / 冲突**")
    group_col = group_col if group_col in assertions_filtered.columns else "topic_key"

    bullish_actions = {
        "view.bullish",
        "valuation.cheap",
        "trade.buy",
        "trade.add",
        "trade.hold",
    }
    bearish_actions = {
        "view.bearish",
        "valuation.expensive",
        "trade.sell",
        "trade.reduce",
        "risk.warning",
        "risk.event",
    }

    def polarity(action: str) -> str:
        if action in bullish_actions:
            return "bull"
        if action in bearish_actions:
            return "bear"
        return "neutral"

    data = assertions_filtered.copy()
    data["polarity"] = data["action"].apply(polarity)

    # Change detection per author + topic
    change_rows = []
    for (author, group_key), group in data.groupby(["author", group_col]):
        polarities = set(group["polarity"]) - {"neutral"}
        if "bull" in polarities and "bear" in polarities:
            group_sorted = group.sort_values(by="created_at")
            first_bull = group_sorted[group_sorted["polarity"] == "bull"].head(1)
            first_bear = group_sorted[group_sorted["polarity"] == "bear"].head(1)
            last_row = group_sorted.tail(1)
            change_rows.append(
                {
                    "author": author,
                    group_label: group_key,
                    "first_bull": first_bull["created_at"].iloc[0] if not first_bull.empty else None,
                    "first_bear": first_bear["created_at"].iloc[0] if not first_bear.empty else None,
                    "latest_action": last_row["action"].iloc[0],
                    "latest_time": last_row["created_at"].iloc[0],
                    "first_bull_summary": first_bull["summary"].iloc[0] if not first_bull.empty else "",
                    "first_bear_summary": first_bear["summary"].iloc[0] if not first_bear.empty else "",
                    "latest_summary": last_row["summary"].iloc[0],
                    "latest_evidence": last_row["evidence"].iloc[0],
                    "latest_url": last_row["url"].iloc[0],
                    "latest_raw": last_row["raw_text"].iloc[0],
                }
            )

    change_df = pd.DataFrame(change_rows)
    if change_df.empty:
        st.caption("未检测到同作者的观点方向变化。")
    else:
        st.markdown("**同作者观点变化**")
        st.dataframe(
            change_df.sort_values(by="latest_time", ascending=False),
            width="stretch",
            hide_index=True,
        )

    # Cross-author conflict within window
    window_days = st.slider("冲突检测窗口（天）", 7, 60, 14, step=1)
    if data["created_at"].notna().any():
        max_ts = data["created_at"].max()
    else:
        max_ts = datetime.now()
    cutoff = max_ts - timedelta(days=window_days)
    recent = data[data["created_at"] >= cutoff]

    conflict_rows = []
    for group_key, group in recent.groupby(group_col):
        bulls = group[group["polarity"] == "bull"]
        bears = group[group["polarity"] == "bear"]
        if not bulls.empty and not bears.empty:
            latest_bull = bulls.sort_values(by="created_at").tail(1)
            latest_bear = bears.sort_values(by="created_at").tail(1)
            conflict_rows.append(
                {
                    group_label: group_key,
                    "bull_authors": ", ".join(sorted(bulls["author"].unique().tolist())),
                    "bear_authors": ", ".join(sorted(bears["author"].unique().tolist())),
                    "bull_count": len(bulls),
                    "bear_count": len(bears),
                    "bull_summary": latest_bull["summary"].iloc[0],
                    "bull_evidence": latest_bull["evidence"].iloc[0],
                    "bull_time": latest_bull["created_at"].iloc[0],
                    "bull_url": latest_bull["url"].iloc[0],
                    "bull_raw": latest_bull["raw_text"].iloc[0],
                    "bear_summary": latest_bear["summary"].iloc[0],
                    "bear_evidence": latest_bear["evidence"].iloc[0],
                    "bear_time": latest_bear["created_at"].iloc[0],
                    "bear_url": latest_bear["url"].iloc[0],
                    "bear_raw": latest_bear["raw_text"].iloc[0],
                }
            )

    conflict_df = pd.DataFrame(conflict_rows)
    if conflict_df.empty:
        st.caption("窗口内未检测到观点冲突。")
    else:
        st.markdown("**不同作者观点冲突**")
        st.dataframe(
            conflict_df.sort_values(by="bull_count", ascending=False),
            width="stretch",
            hide_index=True,
        )


def show_tables(
    posts_filtered: pd.DataFrame,
    assertions_filtered: pd.DataFrame,
    *,
    group_col: str,
    group_label: str,
) -> None:
    st.markdown("**帖子列表**")
    if posts_filtered.empty:
        st.info("当前条件下没有符合的帖子。")
        return

    view = posts_filtered.copy()
    view["内容摘要"] = (
        view["raw_text"].fillna("").str.replace(r"\s+", " ", regex=True).str.slice(0, 120)
    )
    view = view.sort_values(by="created_at", ascending=False)
    st.dataframe(
        view[
            [
                "created_at",
                "author",
                "source",
                "status",
                "invest_score",
                "assertion_count",
                "内容摘要",
                "url",
            ]
        ],
        width="stretch",
        hide_index=True,
    )

    st.divider()
    st.markdown("**帖子详情（格式化展示）**")
    view_for_pick = view.dropna(subset=["post_uid"]).copy()
    view_for_pick["pick_label"] = (
        view_for_pick["created_at"].astype(str).fillna("")
        + " · "
        + view_for_pick["author"].astype(str).fillna("")
        + " · "
        + view_for_pick["post_uid"].astype(str).fillna("")
    )
    pick_options = view_for_pick["pick_label"].tolist()
    if pick_options:
        selected = st.selectbox("选择一条帖子", pick_options, index=0)
        picked_row = view_for_pick[view_for_pick["pick_label"] == selected].head(1)
        if not picked_row.empty:
            row = picked_row.iloc[0]
            raw_text = str(row.get("raw_text") or "")
            author = str(row.get("author") or "")
            display_md = str(row.get("display_md") or "")
            if not display_md.strip():
                display_md = format_weibo_display_md(raw_text, author=author)
            st.caption(f"{row.get('created_at', '')} · {row.get('url', '')}")
            st.markdown(display_md, unsafe_allow_html=True)

    st.markdown("**观点列表**")
    if assertions_filtered.empty:
        st.info("当前条件下没有观点。")
        return
    group_col = group_col if group_col in assertions_filtered.columns else "topic_key"
    show_cluster = group_col != "topic_key" and "cluster_display" in assertions_filtered.columns
    assertion_cols = [
        "created_at",
        "author",
        "source",
    ]
    if show_cluster:
        assertion_cols.append("cluster_display")
    assertion_cols += [
        "topic_key",
        "action",
        "action_strength",
        "confidence",
        "summary",
        "evidence",
        "stock_codes_str",
        "industries_str",
    ]
    st.dataframe(
        assertions_filtered.sort_values(by="created_at", ascending=False)[assertion_cols].rename(
            columns={"cluster_display": "板块", "topic_key": "主题"}
        ),
        width="stretch",
        hide_index=True,
    )
