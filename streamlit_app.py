from __future__ import annotations

import json
import math
import os
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
import streamlit as st
from dotenv import load_dotenv

from turso_db import ensure_turso_engine

load_dotenv()

STREAMLIT_SOURCE_NAME = "archive"


def parse_json_list(value: object) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        return []
    if isinstance(data, list):
        return [str(item) for item in data if str(item).strip()]
    return []


def split_topic_key(value: str) -> Tuple[str, str]:
    if not isinstance(value, str) or not value.strip():
        return "unknown", ""
    if ":" in value:
        left, right = value.split(":", 1)
        return left.strip(), right.strip()
    if "." in value:
        left, right = value.split(".", 1)
        return left.strip(), right.strip()
    return "unknown", value.strip()


def action_group(action: str) -> str:
    if not isinstance(action, str) or not action.strip():
        return "unknown"
    return action.split(".", 1)[0].strip()


@st.cache_data(show_spinner=False)
def load_turso_tables(db_url: str, auth_token: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not db_url:
        raise RuntimeError("Missing TURSO_DATABASE_URL")
    engine = ensure_turso_engine(db_url, auth_token)
    posts_query = """
	        SELECT post_uid, platform, platform_post_id, author, created_at, url, raw_text,
	               final_status AS status, invest_score, processed_at, model, prompt_version
	        FROM posts
	        WHERE processed_at IS NOT NULL
	    """
    assertions_query = "SELECT * FROM assertions"
    posts = pd.read_sql_query(posts_query, engine)
    assertions = pd.read_sql_query(assertions_query, engine)
    return posts, assertions


def load_sources() -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    posts_frames: List[pd.DataFrame] = []
    assertions_frames: List[pd.DataFrame] = []
    missing: List[str] = []

    turso_url = os.getenv("TURSO_DATABASE_URL", "").strip()
    turso_token = os.getenv("TURSO_AUTH_TOKEN", "").strip()
    if not turso_url:
        missing.append("TURSO_DATABASE_URL")
        return pd.DataFrame(), pd.DataFrame(), missing

    try:
        posts, assertions = load_turso_tables(turso_url, turso_token)
    except Exception as e:
        missing.append(f"turso_connect_error:{type(e).__name__}")
        return pd.DataFrame(), pd.DataFrame(), missing

    posts = standardize_posts(posts, STREAMLIT_SOURCE_NAME)
    posts = normalize_datetime_columns(posts)
    assertions = standardize_assertions(assertions, posts, STREAMLIT_SOURCE_NAME)
    assertions = normalize_assertions_datetime(assertions)
    posts_frames.append(posts)
    assertions_frames.append(assertions)

    if posts_frames:
        posts_all = pd.concat(posts_frames, ignore_index=True)
    else:
        posts_all = pd.DataFrame()

    if assertions_frames:
        assertions_all = pd.concat(assertions_frames, ignore_index=True)
    else:
        assertions_all = pd.DataFrame()

    return posts_all, assertions_all, missing


def normalize_datetime_columns(posts: pd.DataFrame) -> pd.DataFrame:
    for col in [
        "created_at",
        "ingested_at",
        "processed_at",
        "next_retry_at",
        "synced_at",
    ]:
        if col in posts.columns:
            if not is_datetime64_any_dtype(posts[col]):
                posts[col] = pd.to_datetime(posts[col], errors="coerce", utc=True)
                posts[col] = posts[col].dt.tz_convert(None)
    return posts


def normalize_assertions_datetime(assertions: pd.DataFrame) -> pd.DataFrame:
    if "created_at" in assertions.columns:
        if not is_datetime64_any_dtype(assertions["created_at"]):
            assertions["created_at"] = pd.to_datetime(
                assertions["created_at"], errors="coerce", utc=True
            )
            assertions["created_at"] = assertions["created_at"].dt.tz_convert(None)
    return assertions


def enrich_posts(posts: pd.DataFrame) -> pd.DataFrame:
    posts = posts.copy()
    posts["has_quote"] = posts["raw_text"].str.contains("//@", na=False)
    posts["is_forward"] = posts["raw_text"].str.startswith("转发微博", na=False)
    posts["repost_flag"] = posts["has_quote"] | posts["is_forward"]
    return posts


def enrich_assertions(assertions: pd.DataFrame) -> pd.DataFrame:
    assertions = assertions.copy()
    assertions["stock_codes"] = assertions["stock_codes_json"].apply(parse_json_list)
    assertions["stock_names"] = assertions["stock_names_json"].apply(parse_json_list)
    assertions["industries"] = assertions["industries_json"].apply(parse_json_list)
    assertions["commodities"] = assertions["commodities_json"].apply(parse_json_list)
    assertions["indices"] = assertions["indices_json"].apply(parse_json_list)
    assertions["stock_codes_str"] = assertions["stock_codes"].apply(lambda items: ", ".join(items))
    assertions["stock_names_str"] = assertions["stock_names"].apply(lambda items: ", ".join(items))
    assertions["industries_str"] = assertions["industries"].apply(lambda items: ", ".join(items))
    assertions["commodities_str"] = assertions["commodities"].apply(lambda items: ", ".join(items))
    assertions["indices_str"] = assertions["indices"].apply(lambda items: ", ".join(items))

    topic_parts = assertions["topic_key"].apply(split_topic_key)
    assertions["topic_type"] = topic_parts.apply(lambda item: item[0])
    assertions["topic_value"] = topic_parts.apply(lambda item: item[1])
    assertions["action_group"] = assertions["action"].apply(action_group)
    return assertions


def standardize_posts(posts: pd.DataFrame, source_name: str) -> pd.DataFrame:
    posts = posts.copy()
    defaults: Dict[str, object] = {
        "post_uid": "",
        "platform": source_name,
        "platform_post_id": "",
        "author": "",
        "created_at": "",
        "url": "",
        "raw_text": "",
        "status": "",
        "invest_score": 0.0,
        "processed_at": "",
    }
    for col, default in defaults.items():
        if col not in posts.columns:
            posts[col] = default
    if "platform" in posts.columns:
        posts["platform"] = posts["platform"].replace("", pd.NA)
        posts["platform"] = posts["platform"].fillna(source_name)
    posts["source"] = source_name
    return posts


def standardize_assertions(
    assertions: pd.DataFrame,
    posts: pd.DataFrame,
    source_name: str,
) -> pd.DataFrame:
    assertions = assertions.copy()
    defaults: Dict[str, object] = {
        "post_uid": "",
        "idx": 0,
        "topic_key": "",
        "action": "",
        "action_strength": 0,
        "summary": "",
        "evidence": "",
        "confidence": 0.0,
        "stock_codes_json": "[]",
        "stock_names_json": "[]",
        "industries_json": "[]",
        "commodities_json": "[]",
        "indices_json": "[]",
        "author": "",
        "created_at": "",
    }
    for col, default in defaults.items():
        if col not in assertions.columns:
            assertions[col] = default

    assertions["source"] = source_name

    if "author" in assertions.columns:
        missing_author = assertions["author"].eq("") | assertions["author"].isna()
    else:
        missing_author = pd.Series([True] * len(assertions))
    if "created_at" in assertions.columns:
        missing_created = assertions["created_at"].eq("") | assertions["created_at"].isna()
    else:
        missing_created = pd.Series([True] * len(assertions))

    if not posts.empty:
        author_map = posts.set_index("post_uid")["author"]
        created_map = posts.set_index("post_uid")["created_at"]
        url_map = posts.set_index("post_uid")["url"]
        raw_map = posts.set_index("post_uid")["raw_text"]
        status_map = posts.set_index("post_uid")["status"]
        score_map = posts.set_index("post_uid")["invest_score"]

        assertions.loc[missing_author, "author"] = assertions.loc[missing_author, "post_uid"].map(author_map)
        assertions.loc[missing_created, "created_at"] = assertions.loc[missing_created, "post_uid"].map(created_map)
        assertions["url"] = assertions["post_uid"].map(url_map)
        assertions["raw_text"] = assertions["post_uid"].map(raw_map)
        assertions["status"] = assertions["post_uid"].map(status_map)
        assertions["invest_score"] = assertions["post_uid"].map(score_map)
    else:
        assertions["url"] = ""
        assertions["raw_text"] = ""
        assertions["status"] = ""
        assertions["invest_score"] = 0.0

    return assertions


def build_filters(
    posts: pd.DataFrame,
    assertions: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    st.sidebar.header("筛选条件")

    posts_filtered = posts.copy()
    required_post_cols = {
        "post_uid": "",
        "created_at": pd.NaT,
        "author": "",
        "status": "",
        "invest_score": 0.0,
        "url": "",
        "raw_text": "",
        "source": "unknown",
        "platform": "",
    }
    for col, default in required_post_cols.items():
        if col not in posts_filtered.columns:
            posts_filtered[col] = default

    date_col = "created_at"
    if not posts_filtered["created_at"].notna().any():
        date_col = "ingested_at"

    if posts_filtered[date_col].notna().any():
        min_date = posts_filtered[date_col].min().date()
        max_date = posts_filtered[date_col].max().date()
    else:
        max_date = date.today()
        min_date = max_date - timedelta(days=30)

    default_start = max(min_date, max_date - timedelta(days=30))
    selected_range = st.sidebar.date_input(
        "日期范围",
        value=(default_start, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(selected_range, tuple):
        start_date, end_date = selected_range
    else:
        start_date = selected_range
        end_date = selected_range

    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())
    posts_filtered = posts_filtered[
        (posts_filtered[date_col] >= start_dt)
        & (posts_filtered[date_col] <= end_dt)
    ]

    source_options = sorted(posts["source"].dropna().unique().tolist()) if "source" in posts.columns else []
    if source_options:
        selected_sources = st.sidebar.multiselect("来源", source_options, default=source_options)
        if selected_sources:
            posts_filtered = posts_filtered[posts_filtered["source"].isin(selected_sources)]

    platform_options = sorted(posts["platform"].dropna().unique().tolist()) if "platform" in posts.columns else []
    if platform_options:
        selected_platforms = st.sidebar.multiselect("平台", platform_options, default=None)
        if selected_platforms:
            posts_filtered = posts_filtered[posts_filtered["platform"].isin(selected_platforms)]

    status_options = sorted(posts["status"].dropna().unique().tolist())
    default_status = ["relevant"] if "relevant" in status_options else status_options
    selected_status = st.sidebar.multiselect("状态", status_options, default=default_status)
    if selected_status:
        posts_filtered = posts_filtered[posts_filtered["status"].isin(selected_status)]

    author_options = sorted(posts["author"].dropna().unique().tolist())
    selected_authors = st.sidebar.multiselect("作者", author_options, default=None)
    if selected_authors:
        posts_filtered = posts_filtered[posts_filtered["author"].isin(selected_authors)]

    keyword = st.sidebar.text_input("关键词 / 片段", value="").strip()
    if keyword:
        posts_filtered = posts_filtered[
            posts_filtered["raw_text"].str.contains(keyword, case=False, na=False)
        ]

    exclude_reposts = st.sidebar.checkbox("排除转发链", value=False)
    if exclude_reposts:
        posts_filtered = posts_filtered[~posts_filtered["repost_flag"]]

    required_assert_cols = {
        "post_uid": "",
        "topic_key": "",
        "action": "",
        "action_strength": 0,
        "summary": "",
        "evidence": "",
        "confidence": 0.0,
        "stock_codes_json": "[]",
        "stock_names_json": "[]",
        "industries_json": "[]",
        "commodities_json": "[]",
        "indices_json": "[]",
        "author": "",
        "created_at": pd.NaT,
    }
    for col, default in required_assert_cols.items():
        if col not in assertions.columns:
            assertions[col] = default

    assertions_joined = assertions.merge(
        posts_filtered[
            [
                "post_uid",
                "created_at",
                "author",
                "status",
                "invest_score",
                "url",
                "raw_text",
                "source",
                "platform",
            ]
        ],
        on="post_uid",
        how="inner",
    )
    for col in ["created_at", "author", "status", "invest_score", "url", "raw_text", "source"]:
        col_x = f"{col}_x"
        col_y = f"{col}_y"
        if col not in assertions_joined.columns:
            if col_x in assertions_joined.columns and col_y in assertions_joined.columns:
                assertions_joined[col] = assertions_joined[col_x].fillna(assertions_joined[col_y])
            elif col_x in assertions_joined.columns:
                assertions_joined[col] = assertions_joined[col_x]
            elif col_y in assertions_joined.columns:
                assertions_joined[col] = assertions_joined[col_y]
            else:
                assertions_joined[col] = ""

    topic_options = sorted(assertions["topic_key"].dropna().unique().tolist())
    selected_topics = st.sidebar.multiselect("主题", topic_options, default=None)
    if selected_topics:
        assertions_joined = assertions_joined[
            assertions_joined["topic_key"].isin(selected_topics)
        ]

    action_options = sorted(assertions["action"].dropna().unique().tolist())
    selected_actions = st.sidebar.multiselect("动作", action_options, default=None)
    if selected_actions:
        assertions_joined = assertions_joined[
            assertions_joined["action"].isin(selected_actions)
        ]

    strength_range = st.sidebar.slider("动作强度", 0, 3, (0, 3), step=1)
    assertions_joined = assertions_joined[
        (assertions_joined["action_strength"] >= strength_range[0])
        & (assertions_joined["action_strength"] <= strength_range[1])
    ]

    conf_range = st.sidebar.slider("置信度区间", 0.0, 1.0, (0.0, 1.0), step=0.05)
    assertions_joined = assertions_joined[
        (assertions_joined["confidence"] >= conf_range[0])
        & (assertions_joined["confidence"] <= conf_range[1])
    ]

    only_with_assertions = st.sidebar.checkbox("仅显示有观点的帖子", value=True)
    if only_with_assertions:
        posts_filtered = posts_filtered[
            posts_filtered["post_uid"].isin(assertions_joined["post_uid"].unique())
        ]

    meta = {
        "date_range": (start_date, end_date),
        "keyword": keyword,
        "only_with_assertions": only_with_assertions,
        "date_col": date_col,
    }
    return posts_filtered, assertions_joined, meta


def show_kpis(posts_filtered: pd.DataFrame, assertions_filtered: pd.DataFrame) -> None:
    total_posts = len(posts_filtered)
    relevant_posts = int((posts_filtered["status"] == "relevant").sum())
    error_posts = int((posts_filtered["status"] == "error").sum())
    total_assertions = len(assertions_filtered)
    avg_conf = (
        float(assertions_filtered["confidence"].mean())
        if total_assertions > 0
        else 0.0
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


def show_overview_charts(posts_filtered: pd.DataFrame, assertions_filtered: pd.DataFrame) -> None:
    col_left, col_mid, col_right = st.columns([1, 1, 2])

    with col_left:
        st.markdown("**热门主题**")
        topic_counts = assertions_filtered["topic_key"].value_counts().head(12)
        if topic_counts.empty:
            st.write("暂无主题数据。")
        else:
            st.bar_chart(topic_counts)

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


def show_trade_flow(assertions_filtered: pd.DataFrame) -> None:
    trade_mask = assertions_filtered["action"].str.startswith("trade.", na=False)
    trade_df = assertions_filtered[trade_mask].copy()

    if trade_df.empty:
        st.info("当前筛选下没有交易类观点。")
        return

    st.markdown("**最近交易流（按时间倒序）**")
    trade_view = trade_df.sort_values(by="created_at", ascending=False)
    show_raw_col = st.checkbox(
        "表格展示原文（完整格式）",
        value=False,
        key="trade_flow_show_raw_col",
    )
    st.dataframe(
        (
            trade_view.assign(原文=trade_view["raw_text"].fillna(""))[
                [
                    "created_at",
                    "author",
                    "source",
                    "topic_key",
                    "action",
                    "action_strength",
                    "summary",
                    "confidence",
                    "url",
                    "原文",
                ]
            ]
            if show_raw_col
            else trade_view[
                [
                    "created_at",
                    "author",
                    "source",
                    "topic_key",
                    "action",
                    "action_strength",
                    "summary",
                    "confidence",
                    "url",
                ]
            ]
        ),
        width="stretch",
        hide_index=True,
    )

    st.divider()
    st.markdown("**作业板（抄作业用，一眼看懂）**")

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

    board_df = trade_df.dropna(subset=["created_at"]).copy()
    board_df = board_df[board_df["topic_key"].astype(str).str.strip().ne("")].copy()
    if board_df.empty:
        st.info("没有可用的数据（topic_key/created_at 为空）。")
        return

    max_ts = board_df["created_at"].max()
    cutoff = max_ts - timedelta(days=int(window_days))
    board_df = board_df[board_df["created_at"] >= cutoff].copy()
    if board_df.empty:
        st.info("时间窗内没有交易观点。")
        return

    # Build numeric strength (0~3). (Keep it simple and visible in tables.)
    board_df["strength"] = (
        pd.to_numeric(board_df["action_strength"], errors="coerce")
        .fillna(0)
        .astype(int)
        .clip(lower=0, upper=3)
    )
    board_df["buy_strength"] = board_df["strength"].where(
        board_df["action"].isin(TRADE_BUY_ACTIONS), 0
    )
    board_df["sell_strength"] = board_df["strength"].where(
        board_df["action"].isin(TRADE_SELL_ACTIONS), 0
    )
    board_df["hold_mentions"] = board_df["action"].isin(TRADE_HOLD_ACTIONS).astype(int)

    agg = board_df.groupby("topic_key", as_index=False).agg(
        买强度=("buy_strength", "sum"),
        卖强度=("sell_strength", "sum"),
        只看次数=("hold_mentions", "sum"),
        提及次数=("topic_key", "count"),
        大佬数=("author", "nunique"),
        最近时间=("created_at", "max"),
    )
    agg["净强度"] = agg["买强度"] - agg["卖强度"]

    last_rows = (
        board_df.sort_values(by="created_at")
        .groupby("topic_key")
        .tail(1)
        .set_index("topic_key")
    )
    last_badge = last_rows.apply(
        lambda row: trade_action_badge(row["action"], row["strength"]), axis=1
    )
    agg["最近大佬"] = agg["topic_key"].map(last_rows["author"])
    agg["最近动作"] = agg["topic_key"].map(last_badge)
    agg["最近摘要"] = (
        agg["topic_key"]
        .map(last_rows["summary"])
        .fillna("")
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.slice(0, 60)
    )
    agg["url"] = agg["topic_key"].map(last_rows["url"])
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

    if sort_mode == "最新":
        agg_sorted = agg.sort_values(by="最近时间", ascending=False)
    elif sort_mode == "大佬最多":
        agg_sorted = agg.sort_values(
            by=["大佬数", "提及次数", "最近时间"], ascending=False
        )
    else:
        agg_sorted = (
            agg.assign(_abs_net=agg["净强度"].abs())
            .sort_values(by=["_abs_net", "大佬数", "最近时间"], ascending=False)
            .drop(columns=["_abs_net"])
        )

    kpi_left, kpi_mid, kpi_right = st.columns(3)
    kpi_left.metric("标的数", f"{len(agg_sorted)}")
    kpi_mid.metric("大佬数", f"{int(board_df['author'].nunique())}")
    kpi_right.metric(
        "有共识",
        f"{int((agg_sorted['共识'].isin(['↑偏买', '↓偏卖'])).sum())}",
    )

    top_assets = agg_sorted.head(max_assets).copy()
    st.dataframe(
        top_assets[
            [
                "topic_key",
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
        ].rename(columns={"topic_key": "标的"}),
        use_container_width=True,
        hide_index=True,
        column_config={
            "url": st.column_config.LinkColumn("链接", display_text="打开"),
        },
    )

    st.markdown("**作业格（大佬 × 标的）**")
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
    top_author_list = author_stats.head(max_authors)["author"].tolist()
    top_asset_keys = top_assets["topic_key"].tolist()

    if not top_asset_keys:
        st.info("没有标的数据。")
        return

    if not top_author_list:
        st.info("没有大佬数据（author 为空）。")
    else:
        pair_df = board_df[
            board_df["topic_key"].isin(top_asset_keys)
            & board_df["author"].isin(top_author_list)
        ].copy()
        if pair_df.empty:
            st.info("当前条件下，作业格没有数据。")
        else:
            pair_last = (
                pair_df.sort_values(by="created_at")
                .groupby(["topic_key", "author"])
                .tail(1)
                .copy()
            )
            pair_last["badge"] = pair_last.apply(
                lambda row: trade_action_badge(row["action"], row["strength"]), axis=1
            )
            pair_last["age"] = pair_last["created_at"].apply(
                lambda ts: format_age_label(max_ts, ts)
            )
            pair_last["cell"] = pair_last["badge"] + " " + pair_last["age"]

            matrix = (
                pair_last.pivot(index="topic_key", columns="author", values="cell")
                .reindex(index=top_asset_keys, columns=top_author_list)
                .fillna("")
            )
            st.dataframe(
                matrix.reset_index().rename(columns={"topic_key": "标的"}),
                use_container_width=True,
                hide_index=True,
            )

    st.markdown("**标的细节（点开看最近几条）**")
    selected_topic = st.selectbox(
        "选择标的",
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
    detail_df = board_df[board_df["topic_key"] == selected_topic].copy()
    detail_df = detail_df.sort_values(by="created_at", ascending=False).head(int(detail_n))
    detail_df["动作"] = detail_df.apply(
        lambda row: trade_action_badge(row["action"], row["strength"]), axis=1
    )
    detail_df["原文"] = (
        detail_df["raw_text"]
        .fillna("")
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.slice(0, 120)
    )
    st.dataframe(
        detail_df[
            [
                "created_at",
                "author",
                "source",
                "动作",
                "summary",
                "confidence",
                "原文",
                "url",
            ]
        ],
        width="stretch",
        hide_index=True,
        column_config={
            "url": st.column_config.LinkColumn("链接", display_text="打开"),
        },
    )


def show_risk_radar(assertions_filtered: pd.DataFrame) -> None:
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
        .groupby("topic_key", as_index=False)
        .agg(
            risk_score=("risk_score", "sum"),
            mentions=("topic_key", "count"),
            last_time=("created_at", "max"),
        )
        .sort_values(by="risk_score", ascending=False)
    )

    last_rows = (
        risk_df.sort_values(by="created_at")
        .groupby("topic_key")
        .tail(1)
        .set_index("topic_key")
    )

    agg["last_author"] = agg["topic_key"].map(last_rows["author"])
    agg["last_summary"] = agg["topic_key"].map(last_rows["summary"])
    agg["last_evidence"] = agg["topic_key"].map(last_rows["evidence"])

    st.bar_chart(agg.set_index("topic_key")["risk_score"].head(12))
    st.dataframe(
        agg[
            [
                "topic_key",
                "risk_score",
                "mentions",
                "last_time",
                "last_author",
                "last_summary",
                "last_evidence",
            ]
        ],
        width="stretch",
        hide_index=True,
    )


def show_topic_timeline(assertions_filtered: pd.DataFrame) -> None:
    st.markdown("**主题时间线**")
    topic_options = sorted(assertions_filtered["topic_key"].dropna().unique().tolist())
    if not topic_options:
        st.info("暂无主题数据。")
        return
    selected_topic = st.selectbox("选择主题", topic_options)
    topic_df = assertions_filtered[assertions_filtered["topic_key"] == selected_topic]
    if topic_df.empty:
        st.info("暂无数据。")
        return

    if topic_df["created_at"].notna().any():
        pivot = (
            topic_df.dropna(subset=["created_at"])
            .set_index("created_at")
            .groupby("action")
            .resample("D")
            .size()
            .unstack(level=0)
            .fillna(0)
        )
        if not pivot.empty:
            st.line_chart(pivot)

    st.dataframe(
        topic_df.sort_values(by="created_at", ascending=False)[
            [
                "created_at",
                "author",
                "source",
                "action",
                "action_strength",
                "summary",
                "confidence",
                "url",
            ]
        ],
        width="stretch",
        hide_index=True,
    )


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


def show_conflicts_and_changes(assertions_filtered: pd.DataFrame) -> None:
    st.markdown("**观点变化 / 冲突**")

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
    for (author, topic_key), group in data.groupby(["author", "topic_key"]):
        polarities = set(group["polarity"]) - {"neutral"}
        if "bull" in polarities and "bear" in polarities:
            group_sorted = group.sort_values(by="created_at")
            first_bull = group_sorted[group_sorted["polarity"] == "bull"].head(1)
            first_bear = group_sorted[group_sorted["polarity"] == "bear"].head(1)
            last_row = group_sorted.tail(1)
            change_rows.append(
                {
                    "author": author,
                    "topic_key": topic_key,
                    "first_bull": first_bull["created_at"].iloc[0]
                    if not first_bull.empty
                    else None,
                    "first_bear": first_bear["created_at"].iloc[0]
                    if not first_bear.empty
                    else None,
                    "latest_action": last_row["action"].iloc[0],
                    "latest_time": last_row["created_at"].iloc[0],
                    "first_bull_summary": first_bull["summary"].iloc[0]
                    if not first_bull.empty
                    else "",
                    "first_bear_summary": first_bear["summary"].iloc[0]
                    if not first_bear.empty
                    else "",
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
    for topic_key, group in recent.groupby("topic_key"):
        bulls = group[group["polarity"] == "bull"]
        bears = group[group["polarity"] == "bear"]
        if not bulls.empty and not bears.empty:
            latest_bull = bulls.sort_values(by="created_at").tail(1)
            latest_bear = bears.sort_values(by="created_at").tail(1)
            conflict_rows.append(
                {
                    "topic_key": topic_key,
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


def show_tables(posts_filtered: pd.DataFrame, assertions_filtered: pd.DataFrame) -> None:
    st.markdown("**帖子列表**")
    if posts_filtered.empty:
        st.info("当前条件下没有符合的帖子。")
        return

    view = posts_filtered.copy()
    view["内容摘要"] = (
        view["raw_text"]
        .fillna("")
        .str.replace(r"\s+", " ", regex=True)
        .str.slice(0, 120)
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

    st.markdown("**观点列表**")
    if assertions_filtered.empty:
        st.info("当前条件下没有观点。")
        return
    st.dataframe(
        assertions_filtered.sort_values(by="created_at", ascending=False)[
            [
                "created_at",
                "author",
                "source",
                "topic_key",
                "action",
                "action_strength",
                "confidence",
                "summary",
                "evidence",
                "stock_codes_str",
                "industries_str",
            ]
        ],
        width="stretch",
        hide_index=True,
    )


def main() -> None:
    st.set_page_config(page_title="AlphaVault 观点可视化", layout="wide")

    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700&family=Zilla+Slab:wght@600&display=swap');
        :root {
          --ink: #0f172a;
          --muted: #475569;
          --accent: #ff6a3d;
          --bg: #f5f1e9;
          --panel: #ffffff;
        }
        html, body, [class*="css"] {
          font-family: 'Manrope', 'Trebuchet MS', sans-serif;
          color: var(--ink);
        }
        .block-container {
          padding-top: 1.5rem;
          padding-bottom: 3rem;
        }
        .app-hero {
          background: linear-gradient(120deg, #fff5e8 0%, #f0f7ff 100%);
          border: 1px solid #f1e4d4;
          border-radius: 18px;
          padding: 18px 24px;
          margin-bottom: 18px;
        }
        .app-hero h1 {
          font-family: 'Zilla Slab', serif;
          margin: 0;
          font-size: 32px;
        }
        .app-hero p {
          margin: 6px 0 0 0;
          color: var(--muted);
        }
        .stSidebar {
          background: radial-gradient(circle at top, #fff4eb 0%, #f7f3ec 60%, #f2efe9 100%);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="app-hero">
          <h1>AlphaVault · 观点可视化</h1>
          <p>基于 posts / assertions 的交易流、风险雷达、主题时间线与学习库。</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    posts, assertions, missing = load_sources()
    if missing:
        st.error("Turso 没配好，或者连不上。")
        st.info(f"缺少/错误：{', '.join(missing)}")
        st.stop()
    if posts.empty:
        st.warning("Turso 里还没有“已处理”的数据（processed_at 为空会被隐藏）。")
        st.stop()
    posts = normalize_datetime_columns(posts)
    posts = enrich_posts(posts)
    assertions = normalize_assertions_datetime(assertions)
    assertions = enrich_assertions(assertions)

    assertion_counts = assertions.groupby("post_uid")["idx"].count()
    posts["assertion_count"] = posts["post_uid"].map(assertion_counts).fillna(0).astype(int)

    posts_filtered, assertions_filtered, _meta = build_filters(posts, assertions)

    tabs = st.tabs(
        [
            "总览",
            "交易流",
            "风险雷达",
            "主题时间线",
            "学习库",
            "冲突/变化",
            "数据表",
        ]
    )

    with tabs[0]:
        show_kpis(posts_filtered, assertions_filtered)
        st.divider()
        show_overview_charts(posts_filtered, assertions_filtered)

    with tabs[1]:
        show_trade_flow(assertions_filtered)

    with tabs[2]:
        show_risk_radar(assertions_filtered)

    with tabs[3]:
        show_topic_timeline(assertions_filtered)

    with tabs[4]:
        show_learning_library(assertions_filtered)

    with tabs[5]:
        show_conflicts_and_changes(assertions_filtered)

    with tabs[6]:
        show_tables(posts_filtered, assertions_filtered)


if __name__ == "__main__":
    main()
