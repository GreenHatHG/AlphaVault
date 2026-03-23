"""
Streamlit tab: risk radar.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import math

import pandas as pd
import streamlit as st


def show_risk_radar(
    assertions_filtered: pd.DataFrame, *, group_col: str, group_label: str
) -> None:
    group_col = group_col if group_col in assertions_filtered.columns else "topic_key"
    st.markdown("**风险雷达（时间衰减评分）**")

    include_bearish = st.checkbox("把 bearish 观点也计入风险", value=True)
    risk_actions = {"risk.warning", "risk.event"}
    bearish_actions = {"view.bearish", "valuation.expensive"}

    if include_bearish:
        risk_mask = assertions_filtered["action"].isin(risk_actions | bearish_actions)
    else:
        risk_mask = assertions_filtered["action"].isin(risk_actions)

    risk_df = assertions_filtered[risk_mask].copy()
    if risk_df.empty:
        st.info("当前筛选下没有风险类观点。")
        return

    window_days = st.slider("时间窗口（天）", 7, 60, 14, step=1)
    if risk_df["created_at"].notna().any():
        max_ts = risk_df["created_at"].max()
    else:
        max_ts = datetime.now()
    cutoff = max_ts - timedelta(days=window_days)
    risk_df = risk_df[risk_df["created_at"] >= cutoff]

    if risk_df.empty:
        st.info("时间窗口内没有风险观点。")
        return

    def decay_score(row: pd.Series) -> float:
        days_ago = max((max_ts - row["created_at"]).days, 0)
        strength = float(row.get("action_strength", 0) or 0)
        return strength * math.exp(-days_ago / 7.0)

    risk_df["risk_score"] = risk_df.apply(decay_score, axis=1)

    agg = (
        risk_df.sort_values(by="created_at")
        .groupby(group_col, as_index=False)
        .agg(
            risk_score=("risk_score", "sum"),
            mentions=(group_col, "count"),
            last_time=("created_at", "max"),
        )
        .sort_values(by="risk_score", ascending=False)
    )

    last_rows = (
        risk_df.sort_values(by="created_at")
        .groupby(group_col)
        .tail(1)
        .set_index(group_col)
    )

    agg["last_author"] = agg[group_col].map(last_rows["author"])
    agg["last_summary"] = agg[group_col].map(last_rows["summary"])
    agg["last_evidence"] = agg[group_col].map(last_rows["evidence"])

    st.bar_chart(agg.set_index(group_col)["risk_score"].head(12))
    st.dataframe(
        agg[
            [
                group_col,
                "risk_score",
                "mentions",
                "last_time",
                "last_author",
                "last_summary",
                "last_evidence",
            ]
        ].rename(columns={group_col: group_label}),
        width="stretch",
        hide_index=True,
    )
