"""
Streamlit tab: overview.
"""

from __future__ import annotations

import math

import pandas as pd
import streamlit as st


def show_kpis(posts_filtered: pd.DataFrame, assertions_filtered: pd.DataFrame) -> None:
    total_posts = len(posts_filtered)
    relevant_posts = int((posts_filtered["status"] == "relevant").sum())
    error_posts = int((posts_filtered["status"] == "error").sum())
    total_assertions = len(assertions_filtered)
    avg_conf = (
        float(assertions_filtered["confidence"].mean()) if total_assertions > 0 else 0.0
    )
    if posts_filtered.empty:
        repost_ratio = 0.0
    else:
        repost_ratio = float(posts_filtered["repost_flag"].mean())
        if math.isnan(repost_ratio):
            repost_ratio = 0.0

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("帖子数", f"{total_posts}")
    col2.metric("相关帖子", f"{relevant_posts}")
    col3.metric("观点数", f"{total_assertions}")
    col4.metric("错误帖子", f"{error_posts}")
    col5.metric("平均置信度", f"{avg_conf:.2f}")
    col6.metric("转发占比", f"{repost_ratio:.0%}")


def show_overview_charts(
    posts_filtered: pd.DataFrame,
    assertions_filtered: pd.DataFrame,
    *,
    group_col: str,
    group_label: str,
) -> None:
    group_col = group_col if group_col in assertions_filtered.columns else "topic_key"
    col_left, col_mid, col_right = st.columns([1, 1, 2])

    with col_left:
        st.markdown(f"**热门{group_label}**")
        group_counts = assertions_filtered[group_col].value_counts().head(12)
        if group_counts.empty:
            st.write(f"暂无{group_label}数据。")
        else:
            st.bar_chart(group_counts)

    with col_mid:
        st.markdown("**热门动作**")
        action_counts = assertions_filtered["action"].value_counts().head(12)
        if action_counts.empty:
            st.write("暂无动作数据。")
        else:
            st.bar_chart(action_counts)

    with col_right:
        st.markdown("**时间轴（帖子 & 观点）**")
        if posts_filtered["created_at"].notna().any():
            posts_daily = (
                posts_filtered.dropna(subset=["created_at"])
                .set_index("created_at")
                .resample("D")
                .size()
            )
        else:
            posts_daily = pd.Series(dtype=float)
        if assertions_filtered["created_at"].notna().any():
            assertions_daily = (
                assertions_filtered.dropna(subset=["created_at"])
                .set_index("created_at")
                .resample("D")
                .size()
            )
        else:
            assertions_daily = pd.Series(dtype=float)

        timeline = pd.DataFrame(
            {"posts": posts_daily, "assertions": assertions_daily}
        ).fillna(0)

        if timeline.empty:
            st.write("暂无时间序列数据。")
        else:
            st.line_chart(timeline)

    st.markdown("**来源分布**")
    if "source" in posts_filtered.columns:
        source_posts = posts_filtered["source"].value_counts()
        if not source_posts.empty:
            st.bar_chart(source_posts)
    if "source" in assertions_filtered.columns:
        source_assertions = assertions_filtered["source"].value_counts()
        if not source_assertions.empty:
            st.bar_chart(source_assertions)
