from __future__ import annotations

import json
import math
import sqlite3
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DEFAULT_DOCKER_WORKDB = "/data/workdb.sqlite"


def default_local_db_path(env_key: str, fallback: str) -> str:
    value = os.getenv(env_key, "").strip()
    if value:
        return value
    if fallback == "workdb.sqlite":
        if Path(DEFAULT_DOCKER_WORKDB).exists():
            return DEFAULT_DOCKER_WORKDB
    return fallback


DB_SOURCES_LOCAL = [
    {"name": "weibo", "path": default_local_db_path("WORKDB_SQLITE_PATH", "workdb.sqlite")},
    {"name": "xueqiu", "path": default_local_db_path("XUEQIU_SQLITE_PATH", "xueqiu_batch.sqlite3")},
]

DB_MODE = os.getenv("STREAMLIT_DB_MODE", "local").strip().lower()
USE_TURSO = DB_MODE in {"cloud", "turso", "archive"}


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
def load_sqlite_tables(db_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    conn = sqlite3.connect(db_path, timeout=30.0)
    try:
        conn.execute("PRAGMA busy_timeout = 5000;")
        posts = pd.read_sql_query("SELECT * FROM posts", conn)
        assertions = pd.read_sql_query("SELECT * FROM assertions", conn)
    finally:
        conn.close()
    return posts, assertions


@st.cache_data(show_spinner=False)
def load_turso_tables(db_url: str, auth_token: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not db_url:
        raise RuntimeError("Missing TURSO_DATABASE_URL")
    if db_url.startswith("libsql://"):
        turso_url = db_url[9:]
    else:
        turso_url = db_url
    engine = create_engine(
        f"sqlite+libsql://{turso_url}?secure=true",
        connect_args={"auth_token": auth_token} if auth_token else {},
        future=True,
    )
    posts_query = """
        SELECT post_uid, platform, platform_post_id, author, created_at, url, raw_text,
               final_status AS status, invest_score, processed_at, model, prompt_version
        FROM posts
    """
    assertions_query = "SELECT * FROM assertions"
    posts = pd.read_sql_query(posts_query, engine)
    assertions = pd.read_sql_query(assertions_query, engine)
    return posts, assertions


def load_sources() -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    base = Path(__file__).resolve().parent
    posts_frames: List[pd.DataFrame] = []
    assertions_frames: List[pd.DataFrame] = []
    missing: List[str] = []

    if USE_TURSO:
        turso_url = os.getenv("TURSO_DATABASE_URL", "").strip()
        turso_token = os.getenv("TURSO_AUTH_TOKEN", "").strip()
        if not turso_url:
            missing.append("TURSO_DATABASE_URL")
        else:
            posts, assertions = load_turso_tables(turso_url, turso_token)
            posts = standardize_posts(posts, "archive")
            posts = normalize_datetime_columns(posts)
            assertions = standardize_assertions(assertions, posts, "archive")
            assertions = normalize_assertions_datetime(assertions)
            posts_frames.append(posts)
            assertions_frames.append(assertions)
    else:
        for source in DB_SOURCES_LOCAL:
            path = base / source["path"]
            if not path.exists():
                missing.append(source["path"])
                continue
            posts, assertions = load_sqlite_tables(str(path))
            posts = standardize_posts(posts, source["name"])
            posts = normalize_datetime_columns(posts)
            assertions = standardize_assertions(assertions, posts, source["name"])
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


def show_trade_flow(assertions_filtered: pd.DataFrame) -> None:
    trade_mask = assertions_filtered["action"].str.startswith("trade.", na=False)
    trade_df = assertions_filtered[trade_mask].copy()

    if trade_df.empty:
        st.info("当前筛选下没有交易类观点。")
        return

    st.markdown("**最近交易流（按时间倒序）**")
    trade_view = trade_df.sort_values(by="created_at", ascending=False)
    show_raw_col = st.checkbox("表格展示原文（完整格式）", value=False)
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
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("**按标的聚合（最近一次动作）**")
    last_trade = (
        trade_view.sort_values(by="created_at")
        .groupby("topic_key")
        .tail(1)
        .sort_values(by="created_at", ascending=False)
    )
    st.dataframe(
        last_trade[
            [
                "topic_key",
                "created_at",
                "author",
                "source",
                "action",
                "action_strength",
                "summary",
                "url",
            ]
        ],
        use_container_width=True,
        hide_index=True,
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
        use_container_width=True,
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
        use_container_width=True,
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
        use_container_width=True,
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
            use_container_width=True,
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
            use_container_width=True,
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
        use_container_width=True,
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
        use_container_width=True,
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
    if posts.empty:
        st.error("未找到可用数据库，请检查 workdb.sqlite 或 xueqiu_batch.sqlite3。")
        st.stop()
    if missing:
        st.info(f"未找到：{', '.join(missing)}")
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
