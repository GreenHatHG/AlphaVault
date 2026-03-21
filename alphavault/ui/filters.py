from __future__ import annotations

"""
Streamlit sidebar filters.

Keep: build_filters(posts, assertions) -> (posts_filtered, assertions_filtered, meta)
"""

from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from alphavault.topic_cluster import UNCATEGORIZED_LABEL


DEFAULT_DATE_RANGE_DAYS = 30
MAX_FILTER_OPTIONS = 2000
MAX_FILTER_MATCHES = 300

REQUIRED_POST_COLS: dict[str, object] = {
    "post_uid": "",
    "created_at": pd.NaT,
    "ingested_at": pd.NaT,
    "author": "",
    "status": "",
    "invest_score": 0.0,
    "url": "",
    "raw_text": "",
    "source": "unknown",
    "platform": "",
    "repost_flag": False,
}

REQUIRED_ASSERT_COLS: dict[str, object] = {
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

ASSERTIONS_JOIN_POST_COLS = [
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

ASSERTIONS_COALESCE_COLS = [
    "created_at",
    "author",
    "status",
    "invest_score",
    "url",
    "raw_text",
    "source",
]


def _ensure_required_columns(df: pd.DataFrame, required: dict[str, object]) -> pd.DataFrame:
    out = df.copy()
    for col, default in required.items():
        if col not in out.columns:
            out[col] = default
    return out


def _sidebar_grouping_config(assertions: pd.DataFrame) -> tuple[bool, str, str]:
    group_by_cluster = False
    if "cluster_display" in assertions.columns:
        group_by_cluster = st.sidebar.checkbox("按板块看（聚合）", value=False, key="filter_group_by_cluster")
    group_col = "cluster_display" if group_by_cluster else "topic_key"
    group_label = "板块" if group_by_cluster else "主题"
    return group_by_cluster, group_col, group_label


def _pick_date_col(posts: pd.DataFrame) -> str:
    if posts.get("created_at", pd.Series(dtype="datetime64[ns]")).notna().any():
        return "created_at"
    return "ingested_at"


def _safe_date_bounds(posts: pd.DataFrame, *, date_col: str) -> tuple[date, date]:
    if date_col in posts.columns and posts[date_col].notna().any():
        min_ts = posts[date_col].min()
        max_ts = posts[date_col].max()
        try:
            return min_ts.date(), max_ts.date()
        except Exception:
            pass
    max_date = date.today()
    min_date = max_date - timedelta(days=DEFAULT_DATE_RANGE_DAYS)
    return min_date, max_date


def _sidebar_date_range(posts: pd.DataFrame, *, date_col: str) -> tuple[date, date, datetime, datetime]:
    min_date, max_date = _safe_date_bounds(posts, date_col=date_col)
    default_start = max(min_date, max_date - timedelta(days=DEFAULT_DATE_RANGE_DAYS))
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
    return start_date, end_date, start_dt, end_dt


def _filter_posts_by_date(posts: pd.DataFrame, *, date_col: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    if date_col not in posts.columns:
        return posts
    return posts[(posts[date_col] >= start_dt) & (posts[date_col] <= end_dt)]


def _sidebar_filter_posts_sources(posts_filtered: pd.DataFrame, *, posts_all: pd.DataFrame) -> pd.DataFrame:
    source_options = (
        sorted(posts_all["source"].dropna().unique().tolist())
        if "source" in posts_all.columns
        else []
    )
    if not source_options:
        return posts_filtered
    selected_sources = st.sidebar.multiselect("来源", source_options, default=source_options)
    if not selected_sources:
        return posts_filtered
    return posts_filtered[posts_filtered["source"].isin(selected_sources)]


def _sidebar_filter_posts_platforms(posts_filtered: pd.DataFrame, *, posts_all: pd.DataFrame) -> pd.DataFrame:
    platform_options = (
        sorted(posts_all["platform"].dropna().unique().tolist())
        if "platform" in posts_all.columns
        else []
    )
    if not platform_options:
        return posts_filtered
    selected_platforms = st.sidebar.multiselect("平台", platform_options, default=None)
    if not selected_platforms:
        return posts_filtered
    return posts_filtered[posts_filtered["platform"].isin(selected_platforms)]


def _sidebar_filter_posts_status(posts_filtered: pd.DataFrame, *, posts_all: pd.DataFrame) -> pd.DataFrame:
    status_options: list = []
    if "status" in posts_all.columns:
        status_options = sorted(posts_all["status"].dropna().unique().tolist())
    elif "status" in posts_filtered.columns:
        status_options = sorted(posts_filtered["status"].dropna().unique().tolist())

    default_status = ["relevant"] if "relevant" in status_options else status_options
    selected_status = st.sidebar.multiselect("状态", status_options, default=default_status)
    if not selected_status:
        return posts_filtered
    return posts_filtered[posts_filtered["status"].isin(selected_status)]


def _sidebar_filter_posts_authors(posts_filtered: pd.DataFrame, *, posts_all: pd.DataFrame) -> pd.DataFrame:
    if "author" in posts_all.columns:
        series = posts_all["author"]
    elif "author" in posts_filtered.columns:
        series = posts_filtered["author"]
    else:
        return posts_filtered

    s = series.dropna().astype(str).str.strip()
    s = s[s.ne("")]
    if s.empty:
        return posts_filtered

    unique_count = int(s.nunique())
    if unique_count <= MAX_FILTER_OPTIONS:
        author_options = sorted(s.unique().tolist())
        selected_authors = st.sidebar.multiselect("作者", author_options, default=None)
    else:
        st.sidebar.caption("提示：作者很多，建议先搜索。")
        search = st.sidebar.text_input("搜索作者", value="", key="filter_author_search").strip()
        if not search:
            return posts_filtered
        if search:
            mask = s.str.contains(search, case=False, na=False, regex=False)
            candidates = s[mask].drop_duplicates().head(MAX_FILTER_MATCHES).tolist()
            st.sidebar.caption(f"搜索命中 {len(candidates)} 个（最多 {MAX_FILTER_MATCHES} 个）")

        selected_authors = st.sidebar.multiselect(
            "作者",
            options=[str(x) for x in candidates],
            default=None,
            key="filter_author_pick",
        )

    if not selected_authors:
        return posts_filtered
    return posts_filtered[posts_filtered["author"].isin(selected_authors)]


def _sidebar_keyword_filter(posts_filtered: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    keyword = st.sidebar.text_input("关键词 / 片段", value="").strip()
    if not keyword:
        return posts_filtered, ""
    out = posts_filtered[posts_filtered["raw_text"].str.contains(keyword, case=False, na=False)]
    return out, keyword


def _sidebar_exclude_reposts(posts_filtered: pd.DataFrame) -> pd.DataFrame:
    exclude_reposts = st.sidebar.checkbox("排除转发链", value=False)
    if not exclude_reposts:
        return posts_filtered
    if "repost_flag" not in posts_filtered.columns:
        return posts_filtered
    return posts_filtered[~posts_filtered["repost_flag"]]


def _join_assertions_with_posts(assertions: pd.DataFrame, *, posts_filtered: pd.DataFrame) -> pd.DataFrame:
    join_cols = [col for col in ASSERTIONS_JOIN_POST_COLS if col in posts_filtered.columns]
    if "post_uid" not in join_cols:
        join_cols = ["post_uid"] + join_cols
    return assertions.merge(posts_filtered[join_cols], on="post_uid", how="inner")


def _coalesce_joined_cols(df: pd.DataFrame, *, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            continue
        col_x = f"{col}_x"
        col_y = f"{col}_y"
        if col_x in out.columns and col_y in out.columns:
            out[col] = out[col_x].fillna(out[col_y])
        elif col_x in out.columns:
            out[col] = out[col_x]
        elif col_y in out.columns:
            out[col] = out[col_y]
        else:
            out[col] = ""
    return out


def _sidebar_filter_uncategorized(
    assertions_joined: pd.DataFrame,
    *,
    group_by_cluster: bool,
) -> tuple[pd.DataFrame, bool]:
    show_uncategorized = True
    if not group_by_cluster:
        return assertions_joined, show_uncategorized
    show_uncategorized = st.sidebar.checkbox("显示未归类", value=True, key="filter_show_uncategorized")
    if show_uncategorized or "cluster_display" not in assertions_joined.columns:
        return assertions_joined, show_uncategorized
    out = assertions_joined[assertions_joined["cluster_display"] != UNCATEGORIZED_LABEL]
    return out, show_uncategorized


def _sidebar_filter_groups(
    assertions_joined: pd.DataFrame,
    *,
    assertions_all: pd.DataFrame,
    group_col: str,
    group_label: str,
    group_by_cluster: bool,
    show_uncategorized: bool,
) -> pd.DataFrame:
    if group_col not in assertions_all.columns:
        return assertions_joined

    s = assertions_all[group_col].dropna().astype(str).str.strip()
    s = s[s.ne("")]
    if group_by_cluster and not show_uncategorized:
        s = s[s.ne(UNCATEGORIZED_LABEL)]
    if s.empty:
        return assertions_joined

    unique_count = int(s.nunique())
    if unique_count <= MAX_FILTER_OPTIONS:
        group_options = sorted(s.unique().tolist())
        selected_groups = st.sidebar.multiselect(group_label, group_options, default=None)
    else:
        st.sidebar.caption(f"提示：{group_label} 很多，建议先搜索。")
        search = st.sidebar.text_input(
            f"搜索{group_label}",
            value="",
            key=f"filter_group_search:{group_col}",
        ).strip()
        if not search:
            return assertions_joined
        if search:
            mask = s.str.contains(search, case=False, na=False, regex=False)
            candidates = s[mask].drop_duplicates().head(MAX_FILTER_MATCHES).tolist()
            st.sidebar.caption(f"搜索命中 {len(candidates)} 个（最多 {MAX_FILTER_MATCHES} 个）")

        selected_groups = st.sidebar.multiselect(
            group_label,
            options=[str(x) for x in candidates],
            default=None,
            key=f"filter_group_pick:{group_col}",
        )

    if not selected_groups or group_col not in assertions_joined.columns:
        return assertions_joined
    return assertions_joined[assertions_joined[group_col].isin(selected_groups)]


def _sidebar_filter_actions(assertions_joined: pd.DataFrame, *, assertions_all: pd.DataFrame) -> pd.DataFrame:
    action_options = (
        sorted(assertions_all["action"].dropna().unique().tolist())
        if "action" in assertions_all.columns
        else []
    )
    selected_actions = st.sidebar.multiselect("动作", action_options, default=None)
    if not selected_actions or "action" not in assertions_joined.columns:
        return assertions_joined
    return assertions_joined[assertions_joined["action"].isin(selected_actions)]


def _sidebar_filter_strength_and_confidence(assertions_joined: pd.DataFrame) -> pd.DataFrame:
    strength_range = st.sidebar.slider("动作强度", 0, 3, (0, 3), step=1)
    assertions_joined = assertions_joined[
        (assertions_joined["action_strength"] >= strength_range[0])
        & (assertions_joined["action_strength"] <= strength_range[1])
    ]
    conf_range = st.sidebar.slider("置信度区间", 0.0, 1.0, (0.0, 1.0), step=0.05)
    return assertions_joined[
        (assertions_joined["confidence"] >= conf_range[0])
        & (assertions_joined["confidence"] <= conf_range[1])
    ]


def _sidebar_only_with_assertions(
    *,
    posts_filtered: pd.DataFrame,
    assertions_joined: pd.DataFrame,
) -> tuple[pd.DataFrame, bool]:
    only_with_assertions = st.sidebar.checkbox("仅显示有观点的帖子", value=True)
    if not only_with_assertions:
        return posts_filtered, only_with_assertions
    if posts_filtered.empty or "post_uid" not in posts_filtered.columns:
        return posts_filtered, only_with_assertions
    if assertions_joined.empty or "post_uid" not in assertions_joined.columns:
        return posts_filtered.iloc[0:0], only_with_assertions
    post_uids = assertions_joined["post_uid"].dropna().unique()
    out = posts_filtered[posts_filtered["post_uid"].isin(post_uids)]
    return out, only_with_assertions


def _filter_posts_sidebar(
    posts: pd.DataFrame,
    *,
    posts_all: pd.DataFrame,
) -> tuple[pd.DataFrame, str, date, date, str]:
    posts_filtered = _ensure_required_columns(posts, REQUIRED_POST_COLS)
    date_col = _pick_date_col(posts_filtered)
    start_date, end_date, start_dt, end_dt = _sidebar_date_range(posts_filtered, date_col=date_col)
    posts_filtered = _filter_posts_by_date(posts_filtered, date_col=date_col, start_dt=start_dt, end_dt=end_dt)
    posts_filtered = _sidebar_filter_posts_sources(posts_filtered, posts_all=posts_all)
    posts_filtered = _sidebar_filter_posts_platforms(posts_filtered, posts_all=posts_all)
    posts_filtered = _sidebar_filter_posts_status(posts_filtered, posts_all=posts_all)
    posts_filtered = _sidebar_filter_posts_authors(posts_filtered, posts_all=posts_all)
    posts_filtered, keyword = _sidebar_keyword_filter(posts_filtered)
    posts_filtered = _sidebar_exclude_reposts(posts_filtered)
    return posts_filtered, date_col, start_date, end_date, keyword


def _filter_assertions_sidebar(
    assertions: pd.DataFrame,
    *,
    posts_filtered: pd.DataFrame,
    group_by_cluster: bool,
    group_col: str,
    group_label: str,
) -> tuple[pd.DataFrame, pd.DataFrame, bool]:
    assertions_all = _ensure_required_columns(assertions, REQUIRED_ASSERT_COLS)
    assertions_joined = _join_assertions_with_posts(assertions_all, posts_filtered=posts_filtered)
    assertions_joined = _coalesce_joined_cols(assertions_joined, cols=ASSERTIONS_COALESCE_COLS)
    assertions_joined, show_uncategorized = _sidebar_filter_uncategorized(
        assertions_joined,
        group_by_cluster=group_by_cluster,
    )
    assertions_joined = _sidebar_filter_groups(
        assertions_joined,
        assertions_all=assertions_all,
        group_col=group_col,
        group_label=group_label,
        group_by_cluster=group_by_cluster,
        show_uncategorized=show_uncategorized,
    )
    assertions_joined = _sidebar_filter_actions(assertions_joined, assertions_all=assertions_all)
    assertions_joined = _sidebar_filter_strength_and_confidence(assertions_joined)
    posts_filtered, only_with_assertions = _sidebar_only_with_assertions(
        posts_filtered=posts_filtered,
        assertions_joined=assertions_joined,
    )
    return assertions_joined, posts_filtered, only_with_assertions


def build_filters(
    posts: pd.DataFrame,
    assertions: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    st.sidebar.header("筛选条件")

    group_by_cluster, group_col, group_label = _sidebar_grouping_config(assertions)
    posts_filtered, date_col, start_date, end_date, keyword = _filter_posts_sidebar(posts, posts_all=posts)
    assertions_joined, posts_filtered, only_with_assertions = _filter_assertions_sidebar(
        assertions,
        posts_filtered=posts_filtered,
        group_by_cluster=group_by_cluster,
        group_col=group_col,
        group_label=group_label,
    )

    meta = {
        "date_range": (start_date, end_date),
        "keyword": keyword,
        "only_with_assertions": only_with_assertions,
        "date_col": date_col,
        "group_by_cluster": group_by_cluster,
        "group_col": group_col,
        "group_label": group_label,
    }
    return posts_filtered, assertions_joined, meta
