from __future__ import annotations

"""
Streamlit tab: trade flow.
"""

from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

TRADE_BUY_ACTIONS = frozenset({"trade.buy", "trade.add"})
TRADE_SELL_ACTIONS = frozenset({"trade.sell", "trade.reduce"})
TRADE_HOLD_ACTIONS = frozenset({"trade.hold"})


def format_age_label(max_ts: datetime, ts: datetime) -> str:
    """Return a short age label like '3m', '2h', '5d'."""
    if not isinstance(ts, datetime) or not isinstance(max_ts, datetime):
        return ""
    delta = max_ts - ts
    if delta.total_seconds() < 0:
        delta = timedelta(seconds=0)
    minutes = int(delta.total_seconds() // 60)
    if minutes < 60:
        return f"{minutes}m"
    hours = int(minutes // 60)
    if hours < 48:
        return f"{hours}h"
    days = int(hours // 24)
    return f"{days}d"


def trade_action_badge(action: str, strength: object) -> str:
    """Return a short badge like '↑买3' / '↓卖2' / '→看'."""
    action_str = str(action or "").strip()
    strength_val = pd.to_numeric(strength, errors="coerce")
    strength_num = 0 if pd.isna(strength_val) else int(strength_val)
    strength_num = max(0, min(3, strength_num))
    if action_str in TRADE_BUY_ACTIONS:
        return f"↑买{strength_num}"
    if action_str in TRADE_SELL_ACTIONS:
        return f"↓卖{strength_num}"
    if action_str in TRADE_HOLD_ACTIONS:
        return "→看"
    if action_str.startswith("trade."):
        return "·交易"
    return "·"


def _trade_group_col(assertions_filtered: pd.DataFrame, group_col: str) -> str:
    return group_col if group_col in assertions_filtered.columns else "topic_key"


def _filter_trade_df(assertions_filtered: pd.DataFrame) -> pd.DataFrame:
    if assertions_filtered.empty or "action" not in assertions_filtered.columns:
        return pd.DataFrame()
    trade_mask = assertions_filtered["action"].str.startswith("trade.", na=False)
    return assertions_filtered[trade_mask].copy()


def _render_recent_trade_flow(
    trade_df: pd.DataFrame,
    *,
    group_col: str,
    group_label: str,
) -> None:
    st.markdown("**最近交易流（按时间倒序）**")
    trade_view = trade_df.sort_values(by="created_at", ascending=False)
    show_raw_col = st.checkbox(
        "表格展示原文（完整格式）",
        value=False,
        key="trade_flow_show_raw_col",
    )
    group_cols = [group_col] if group_col == "topic_key" else [group_col, "topic_key"]
    base_cols = [
        "created_at",
        "author",
        "source",
        *group_cols,
        "action",
        "action_strength",
        "summary",
        "confidence",
        "url",
    ]
    raw_col = "display_md" if "display_md" in trade_view.columns else "raw_text"
    display_df = (
        trade_view.assign(原文=trade_view[raw_col].fillna(""))[base_cols + ["原文"]]
        if show_raw_col
        else trade_view[base_cols]
    )
    rename_map = {"topic_key": "主题"}
    if group_col != "topic_key":
        rename_map[group_col] = group_label
    display_df = display_df.rename(columns=rename_map)
    st.dataframe(display_df, width="stretch", hide_index=True)


def _trade_board_settings() -> tuple[int, str, int, int]:
    col_a, col_b, col_c, col_d = st.columns([1, 1, 1, 1])
    with col_a:
        window_days = st.slider(
            "时间窗（天）",
            1,
            60,
            7,
            step=1,
            key="trade_board_window_days",
        )
    with col_b:
        sort_mode = st.selectbox(
            "排序",
            ["最新", "大佬最多", "共识最强"],
            index=0,
            key="trade_board_sort_mode",
        )
    with col_c:
        max_assets = int(
            st.number_input(
                "标的数",
                min_value=5,
                max_value=200,
                value=30,
                step=5,
                key="trade_board_max_assets",
            )
        )
    with col_d:
        max_authors = int(
            st.number_input(
                "大佬数",
                min_value=1,
                max_value=30,
                value=8,
                step=1,
                key="trade_board_max_authors",
            )
        )
    return int(window_days), str(sort_mode), int(max_assets), int(max_authors)


def _prepare_trade_board_df(
    trade_df: pd.DataFrame,
    *,
    group_col: str,
    group_label: str,
    window_days: int,
) -> tuple[pd.DataFrame, datetime] | None:
    board_df = trade_df.dropna(subset=["created_at"]).copy()
    board_df = board_df[board_df[group_col].astype(str).str.strip().ne("")].copy()
    if board_df.empty:
        st.info(f"没有可用的数据（{group_label}/created_at 为空）。")
        return None

    max_ts = board_df["created_at"].max()
    cutoff = max_ts - timedelta(days=int(window_days))
    board_df = board_df[board_df["created_at"] >= cutoff].copy()
    if board_df.empty:
        st.info("时间窗内没有交易观点。")
        return None

    board_df["strength"] = (
        pd.to_numeric(board_df["action_strength"], errors="coerce")
        .fillna(0)
        .astype(int)
        .clip(lower=0, upper=3)
    )
    board_df["buy_strength"] = board_df["strength"].where(board_df["action"].isin(TRADE_BUY_ACTIONS), 0)
    board_df["sell_strength"] = board_df["strength"].where(board_df["action"].isin(TRADE_SELL_ACTIONS), 0)
    board_df["hold_mentions"] = board_df["action"].isin(TRADE_HOLD_ACTIONS).astype(int)
    return board_df, max_ts


def _build_trade_board_agg(board_df: pd.DataFrame, *, group_col: str, max_ts: datetime) -> pd.DataFrame:
    agg = board_df.groupby(group_col, as_index=False).agg(
        买强度=("buy_strength", "sum"),
        卖强度=("sell_strength", "sum"),
        只看次数=("hold_mentions", "sum"),
        提及次数=(group_col, "count"),
        大佬数=("author", "nunique"),
        最近时间=("created_at", "max"),
    )
    agg["净强度"] = agg["买强度"] - agg["卖强度"]

    last_rows = (
        board_df.sort_values(by="created_at")
        .groupby(group_col)
        .tail(1)
        .set_index(group_col)
    )
    last_badge = last_rows.apply(lambda row: trade_action_badge(row["action"], row["strength"]), axis=1)
    agg["最近大佬"] = agg[group_col].map(last_rows["author"])
    agg["最近动作"] = agg[group_col].map(last_badge)
    agg["最近摘要"] = (
        agg[group_col]
        .map(last_rows["summary"])
        .fillna("")
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.slice(0, 60)
    )
    agg["url"] = agg[group_col].map(last_rows["url"])
    agg["最近"] = agg["最近时间"].apply(lambda ts: format_age_label(max_ts, ts))

    consensus = pd.Series(["·不清楚"] * len(agg), index=agg.index)
    consensus = consensus.mask(agg["净强度"] > 0, "↑偏买")
    consensus = consensus.mask(agg["净强度"] < 0, "↓偏卖")
    consensus = consensus.mask(
        (agg["净强度"] == 0)
        & (agg["买强度"] == 0)
        & (agg["卖强度"] == 0)
        & (agg["只看次数"] > 0),
        "→只看",
    )
    agg["共识"] = consensus
    return agg


def _sort_trade_board_agg(agg: pd.DataFrame, *, sort_mode: str) -> pd.DataFrame:
    if sort_mode == "最新":
        return agg.sort_values(by="最近时间", ascending=False)
    if sort_mode == "大佬最多":
        return agg.sort_values(by=["大佬数", "提及次数", "最近时间"], ascending=False)
    return (
        agg.assign(_abs_net=agg["净强度"].abs())
        .sort_values(by=["_abs_net", "大佬数", "最近时间"], ascending=False)
        .drop(columns=["_abs_net"])
    )


def _render_trade_kpis(agg_sorted: pd.DataFrame, *, board_df: pd.DataFrame, group_label: str) -> None:
    kpi_left, kpi_mid, kpi_right = st.columns(3)
    kpi_left.metric(f"{group_label}数", f"{len(agg_sorted)}")
    kpi_mid.metric("大佬数", f"{int(board_df['author'].nunique())}")
    kpi_right.metric(
        "有共识",
        f"{int((agg_sorted['共识'].isin(['↑偏买', '↓偏卖'])).sum())}",
    )


def _render_top_assets(
    agg_sorted: pd.DataFrame,
    *,
    group_col: str,
    group_label: str,
    max_assets: int,
) -> pd.DataFrame:
    top_assets = agg_sorted.head(int(max_assets)).copy()
    st.dataframe(
        top_assets[
            [
                group_col,
                "共识",
                "净强度",
                "买强度",
                "卖强度",
                "提及次数",
                "大佬数",
                "最近",
                "最近动作",
                "最近大佬",
                "最近摘要",
                "url",
            ]
        ].rename(columns={group_col: group_label}),
        width="stretch",
        hide_index=True,
        column_config={
            "url": st.column_config.LinkColumn("链接", display_text="打开"),
        },
    )
    return top_assets


def _top_authors(board_df: pd.DataFrame, *, max_authors: int) -> list[str]:
    if "invest_score" in board_df.columns:
        score_series = pd.to_numeric(board_df["invest_score"], errors="coerce").fillna(0.0)
    else:
        score_series = pd.Series(0.0, index=board_df.index)

    author_stats = (
        board_df.assign(_score=score_series)
        .groupby("author", as_index=False)
        .agg(分数=("_score", "max"), 提及=("author", "count"))
    )
    author_stats = author_stats[author_stats["author"].astype(str).str.strip().ne("")]
    author_stats = author_stats.sort_values(by=["分数", "提及"], ascending=False)
    return author_stats.head(int(max_authors))["author"].tolist()


def _render_trade_matrix(
    board_df: pd.DataFrame,
    *,
    top_asset_keys: list,
    top_author_list: list[str],
    group_col: str,
    group_label: str,
    max_ts: datetime,
) -> None:
    if not top_asset_keys:
        st.info(f"没有{group_label}数据。")
        return

    if not top_author_list:
        st.info("没有大佬数据（author 为空）。")
        return

    pair_df = board_df[
        board_df[group_col].isin(top_asset_keys) & board_df["author"].isin(top_author_list)
    ].copy()
    if pair_df.empty:
        st.info("当前条件下，作业格没有数据。")
        return

    pair_last = pair_df.sort_values(by="created_at").groupby([group_col, "author"]).tail(1).copy()
    pair_last["badge"] = pair_last.apply(
        lambda row: trade_action_badge(row["action"], row["strength"]), axis=1
    )
    pair_last["age"] = pair_last["created_at"].apply(lambda ts: format_age_label(max_ts, ts))
    pair_last["cell"] = pair_last["badge"] + " " + pair_last["age"]

    matrix = (
        pair_last.pivot(index=group_col, columns="author", values="cell")
        .reindex(index=top_asset_keys, columns=top_author_list)
        .fillna("")
    )
    st.dataframe(
        matrix.reset_index().rename(columns={group_col: group_label}),
        width="stretch",
        hide_index=True,
    )


def _render_trade_detail(board_df: pd.DataFrame, *, top_asset_keys: list, group_col: str, group_label: str) -> None:
    st.markdown(f"**{group_label}细节（点开看最近几条）**")
    if not top_asset_keys:
        st.info(f"没有{group_label}数据。")
        return

    selected_key = st.selectbox(
        f"选择{group_label}",
        options=top_asset_keys,
        index=0,
        key="trade_board_selected_topic",
    )
    detail_n = st.slider(
        "显示条数",
        5,
        50,
        10,
        step=5,
        key="trade_board_detail_n",
    )
    detail_df = board_df[board_df[group_col] == selected_key].copy()
    detail_df = detail_df.sort_values(by="created_at", ascending=False).head(int(detail_n))
    detail_df["动作"] = detail_df.apply(lambda row: trade_action_badge(row["action"], row["strength"]), axis=1)
    detail_df["原文"] = (
        detail_df["raw_text"]
        .fillna("")
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.slice(0, 120)
    )
    detail_cols = [
        "created_at",
        "author",
        "source",
        "动作",
        "summary",
        "confidence",
        "原文",
        "url",
    ]
    if group_col != "topic_key" and "topic_key" in detail_df.columns:
        detail_cols.insert(3, "topic_key")
    st.dataframe(
        detail_df[detail_cols],
        width="stretch",
        hide_index=True,
        column_config={
            "url": st.column_config.LinkColumn("链接", display_text="打开"),
        },
    )


def show_trade_flow(assertions_filtered: pd.DataFrame, *, group_col: str, group_label: str) -> None:
    group_col = _trade_group_col(assertions_filtered, group_col)
    trade_df = _filter_trade_df(assertions_filtered)
    if trade_df.empty:
        st.info("当前筛选下没有交易类观点。")
        return
    _render_recent_trade_flow(trade_df, group_col=group_col, group_label=group_label)

    st.divider()
    st.markdown("**作业板（抄作业用，一眼看懂）**")

    window_days, sort_mode, max_assets, max_authors = _trade_board_settings()
    prepared = _prepare_trade_board_df(
        trade_df,
        group_col=group_col,
        group_label=group_label,
        window_days=window_days,
    )
    if prepared is None:
        return
    board_df, max_ts = prepared

    agg = _build_trade_board_agg(board_df, group_col=group_col, max_ts=max_ts)
    agg_sorted = _sort_trade_board_agg(agg, sort_mode=sort_mode)
    _render_trade_kpis(agg_sorted, board_df=board_df, group_label=group_label)
    top_assets = _render_top_assets(
        agg_sorted,
        group_col=group_col,
        group_label=group_label,
        max_assets=max_assets,
    )

    st.markdown(f"**作业格（大佬 × {group_label}）**")
    top_asset_keys = top_assets[group_col].tolist()
    top_author_list = _top_authors(board_df, max_authors=max_authors)
    _render_trade_matrix(
        board_df,
        top_asset_keys=top_asset_keys,
        top_author_list=top_author_list,
        group_col=group_col,
        group_label=group_label,
        max_ts=max_ts,
    )
    _render_trade_detail(
        board_df,
        top_asset_keys=top_asset_keys,
        group_col=group_col,
        group_label=group_label,
    )
