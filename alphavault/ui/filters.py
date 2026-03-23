"""
Streamlit sidebar filters.

Keep: build_filters(posts, assertions) -> (posts_filtered, assertions_filtered, meta)
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from alphavault.topic_cluster import UNCATEGORIZED_LABEL


DEFAULT_DATE_RANGE_DAYS = 30
MAX_FILTER_OPTIONS = 2000
MAX_FILTER_MATCHES = 300
UNTAGGED_LABEL = "未标注"

GROUP_MODE_TOPIC = "topic"
GROUP_MODE_CLUSTER = "cluster"
GROUP_MODE_STOCK = "stock"
GROUP_MODE_INDUSTRY = "industry"
GROUP_MODE_COMMODITY = "commodity"
GROUP_MODE_INDEX = "index"

GROUP_MODE_LABELS = {
    GROUP_MODE_TOPIC: "主题",
    GROUP_MODE_CLUSTER: "板块",
    GROUP_MODE_STOCK: "个股",
    GROUP_MODE_INDUSTRY: "行业",
    GROUP_MODE_COMMODITY: "商品",
    GROUP_MODE_INDEX: "指数",
}

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


def _ensure_required_columns(
    df: pd.DataFrame, required: dict[str, object]
) -> pd.DataFrame:
    out = df.copy()
    for col, default in required.items():
        if col not in out.columns:
            out[col] = default
    return out


def _sidebar_grouping_config(assertions: pd.DataFrame) -> tuple[str, str, str]:
    # Legacy toggle: keep for old sessions, but the new UI uses group_mode.
    legacy_group_by_cluster = bool(st.session_state.get("filter_group_by_cluster"))
    default_mode = GROUP_MODE_CLUSTER if legacy_group_by_cluster else GROUP_MODE_TOPIC

    options = [
        GROUP_MODE_TOPIC,
        GROUP_MODE_CLUSTER,
        GROUP_MODE_STOCK,
        GROUP_MODE_INDUSTRY,
        GROUP_MODE_COMMODITY,
        GROUP_MODE_INDEX,
    ]
    try:
        default_index = options.index(default_mode)
    except ValueError:
        default_index = 0

    group_mode = st.sidebar.radio(
        "按什么看",
        options=options,
        index=default_index,
        format_func=lambda x: GROUP_MODE_LABELS.get(str(x), str(x)),
        key="filter_group_mode",
        horizontal=True,
    )
    group_mode = str(group_mode or "").strip() or GROUP_MODE_TOPIC

    # Use a unified output column so other tabs can stay simple.
    group_col = "group_key"
    group_label = GROUP_MODE_LABELS.get(group_mode, "主题")
    return group_mode, group_col, group_label


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


def _sidebar_date_range(
    posts: pd.DataFrame, *, date_col: str
) -> tuple[date, date, datetime, datetime]:
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


def _filter_posts_by_date(
    posts: pd.DataFrame, *, date_col: str, start_dt: datetime, end_dt: datetime
) -> pd.DataFrame:
    if date_col not in posts.columns:
        return posts
    return posts[(posts[date_col] >= start_dt) & (posts[date_col] <= end_dt)]


def _sidebar_filter_posts_sources(
    posts_filtered: pd.DataFrame, *, posts_all: pd.DataFrame
) -> pd.DataFrame:
    source_options = (
        sorted(posts_all["source"].dropna().unique().tolist())
        if "source" in posts_all.columns
        else []
    )
    if not source_options:
        return posts_filtered
    selected_sources = st.sidebar.multiselect(
        "来源", source_options, default=source_options
    )
    if not selected_sources:
        return posts_filtered
    return posts_filtered[posts_filtered["source"].isin(selected_sources)]


def _sidebar_filter_posts_platforms(
    posts_filtered: pd.DataFrame, *, posts_all: pd.DataFrame
) -> pd.DataFrame:
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


def _sidebar_filter_posts_status(
    posts_filtered: pd.DataFrame, *, posts_all: pd.DataFrame
) -> pd.DataFrame:
    status_options: list = []
    if "status" in posts_all.columns:
        status_options = sorted(posts_all["status"].dropna().unique().tolist())
    elif "status" in posts_filtered.columns:
        status_options = sorted(posts_filtered["status"].dropna().unique().tolist())

    default_status = ["relevant"] if "relevant" in status_options else status_options
    selected_status = st.sidebar.multiselect(
        "状态", status_options, default=default_status
    )
    if not selected_status:
        return posts_filtered
    return posts_filtered[posts_filtered["status"].isin(selected_status)]


def _sidebar_filter_posts_authors(
    posts_filtered: pd.DataFrame, *, posts_all: pd.DataFrame
) -> pd.DataFrame:
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
        search = st.sidebar.text_input(
            "搜索作者", value="", key="filter_author_search"
        ).strip()
        if not search:
            return posts_filtered
        if search:
            mask = s.str.contains(search, case=False, na=False, regex=False)
            candidates = s[mask].drop_duplicates().head(MAX_FILTER_MATCHES).tolist()
            st.sidebar.caption(
                f"搜索命中 {len(candidates)} 个（最多 {MAX_FILTER_MATCHES} 个）"
            )

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
    out = posts_filtered[
        posts_filtered["raw_text"].str.contains(keyword, case=False, na=False)
    ]
    return out, keyword


def _sidebar_exclude_reposts(posts_filtered: pd.DataFrame) -> pd.DataFrame:
    exclude_reposts = st.sidebar.checkbox("排除转发链", value=False)
    if not exclude_reposts:
        return posts_filtered
    if "repost_flag" not in posts_filtered.columns:
        return posts_filtered
    return posts_filtered[~posts_filtered["repost_flag"]]


def _join_assertions_with_posts(
    assertions: pd.DataFrame, *, posts_filtered: pd.DataFrame
) -> pd.DataFrame:
    join_cols = [
        col for col in ASSERTIONS_JOIN_POST_COLS if col in posts_filtered.columns
    ]
    if "post_uid" not in join_cols:
        join_cols = ["post_uid"] + join_cols
    return assertions.merge(posts_filtered[join_cols], on="post_uid", how="inner")


def _explode_clusters_for_grouping(assertions_joined: pd.DataFrame) -> pd.DataFrame:
    """
    When "group by cluster" is enabled, one assertion can belong to multiple clusters.

    We explode list columns:
    - cluster_keys: list[str]
    - cluster_displays: list[str]
    into row-level:
    - cluster_key: str
    - cluster_display: str
    """
    if assertions_joined.empty:
        return assertions_joined
    if (
        "cluster_keys" not in assertions_joined.columns
        and "cluster_displays" not in assertions_joined.columns
    ):
        return assertions_joined

    def _pairs_for_row(row: pd.Series) -> list[tuple[str, str]]:
        keys = row.get("cluster_keys")
        displays = row.get("cluster_displays")
        if not isinstance(keys, list):
            keys = []
        if not isinstance(displays, list):
            displays = []

        keys = [str(x).strip() for x in keys if str(x).strip()]
        displays = [str(x).strip() for x in displays if str(x).strip()]

        if keys:
            if len(displays) != len(keys):
                displays = keys
            return list(zip(keys, displays, strict=False))
        # Uncategorized: keep exactly 1 row.
        display = displays[0] if displays else UNCATEGORIZED_LABEL
        return [("", display)]

    out = assertions_joined.copy()
    out["__cluster_pair"] = out.apply(_pairs_for_row, axis=1)
    out = out.explode("__cluster_pair", ignore_index=True)
    out["cluster_key"] = out["__cluster_pair"].apply(
        lambda item: str(item[0] or "").strip()
        if isinstance(item, tuple) and len(item) >= 2
        else ""
    )
    out["cluster_display"] = out["__cluster_pair"].apply(
        lambda item: str(item[1] or "").strip()
        if isinstance(item, tuple) and len(item) >= 2
        else ""
    )
    out["cluster_display"] = out["cluster_display"].where(
        out["cluster_display"].str.strip().ne(""), UNCATEGORIZED_LABEL
    )
    out = out.drop(columns=["__cluster_pair"])
    return out


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


def _uniq_str(items: list[object]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        s = str(item or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _match_values(match_keys_value: object, *, prefix: str) -> list[str]:
    """
    Extract values from match_keys by prefix, like:
    - prefix="stock:" -> ["601899.SH", ...]
    """
    if not isinstance(match_keys_value, list):
        return []
    pre = str(prefix or "")
    out: list[object] = []
    for item in match_keys_value:
        s = str(item or "").strip()
        if not s or not s.startswith(pre):
            continue
        out.append(s[len(pre) :].strip())
    return _uniq_str(out)


def _build_stock_name_by_code(assertions_joined: pd.DataFrame) -> dict[str, str]:
    """
    Best-effort code -> name mapping for UI labels.

    Keep it conservative:
    - only trust rows where exactly 1 code and 1 name
    """
    if assertions_joined.empty:
        return {}
    if (
        "stock_codes" not in assertions_joined.columns
        or "stock_names" not in assertions_joined.columns
    ):
        return {}

    counts: dict[str, dict[str, int]] = {}
    for codes, names in zip(
        assertions_joined["stock_codes"].tolist(),
        assertions_joined["stock_names"].tolist(),
        strict=False,
    ):
        if not isinstance(codes, list) or not isinstance(names, list):
            continue
        codes = [str(x).strip() for x in codes if str(x).strip()]
        names = [str(x).strip() for x in names if str(x).strip()]
        if len(codes) != 1 or len(names) != 1:
            continue
        code = codes[0]
        name = names[0]
        if not code or not name:
            continue
        counts.setdefault(code, {})
        counts[code][name] = int(counts[code].get(name, 0)) + 1

    out: dict[str, str] = {}
    for code, name_counts in counts.items():
        # pick the most frequent name; stable tiebreak by name string.
        best = sorted(name_counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[0][
            0
        ]
        out[str(code).strip()] = str(best).strip()
    return out


def _format_stock_label(code: str, *, stock_name_by_code: dict[str, str]) -> str:
    c = str(code or "").strip()
    if not c:
        return ""
    name = str(stock_name_by_code.get(c, "") or "").strip()
    if name:
        return f"{name} ({c})"
    return c


def _group_key_list_for_row(
    row: pd.Series,
    *,
    group_mode: str,
    stock_name_by_code: dict[str, str],
) -> list[str]:
    mode = str(group_mode or "").strip()
    match_keys = row.get("match_keys")

    if mode == GROUP_MODE_STOCK:
        codes = _match_values(match_keys, prefix="stock:")
        labels = [
            _format_stock_label(code, stock_name_by_code=stock_name_by_code)
            for code in codes
        ]
        labels = [x for x in labels if x]
        return labels if labels else [UNTAGGED_LABEL]

    if mode == GROUP_MODE_INDUSTRY:
        values = _match_values(match_keys, prefix="industry:")
        values = [str(x).strip() for x in values if str(x).strip()]
        return values if values else [UNTAGGED_LABEL]

    if mode == GROUP_MODE_COMMODITY:
        values = _match_values(match_keys, prefix="commodity:")
        values = [str(x).strip() for x in values if str(x).strip()]
        return values if values else [UNTAGGED_LABEL]

    if mode == GROUP_MODE_INDEX:
        values = _match_values(match_keys, prefix="index:")
        values = [str(x).strip() for x in values if str(x).strip()]
        return values if values else [UNTAGGED_LABEL]

    return [UNTAGGED_LABEL]


def _explode_group_key_list(
    assertions_joined: pd.DataFrame, *, list_col: str
) -> pd.DataFrame:
    """
    Explode a list column into row-level group_key.
    Keep exactly 1 row even if list is empty (use UNTAGGED_LABEL).
    """
    if assertions_joined.empty:
        return assertions_joined
    if list_col not in assertions_joined.columns:
        return assertions_joined

    out = assertions_joined.copy()
    out[list_col] = out[list_col].apply(lambda v: v if isinstance(v, list) else [])
    out[list_col] = out[list_col].apply(lambda v: v if v else [UNTAGGED_LABEL])
    out = out.explode(list_col, ignore_index=True)
    out["group_key"] = out[list_col].apply(lambda x: str(x or "").strip())
    out["group_key"] = out["group_key"].where(
        out["group_key"].str.strip().ne(""), UNTAGGED_LABEL
    )
    out = out.drop(columns=[list_col])
    return out


def _apply_group_mode(
    assertions_joined: pd.DataFrame, *, group_mode: str
) -> pd.DataFrame:
    mode = str(group_mode or "").strip() or GROUP_MODE_TOPIC
    out = assertions_joined.copy()

    if mode == GROUP_MODE_TOPIC:
        if "topic_key" in out.columns:
            out["group_key"] = out["topic_key"].astype(str).str.strip()
        else:
            out["group_key"] = ""
        out["group_key"] = out["group_key"].where(
            out["group_key"].str.strip().ne(""), UNTAGGED_LABEL
        )
        return out

    if mode == GROUP_MODE_CLUSTER:
        out = _explode_clusters_for_grouping(out)
        if "cluster_display" in out.columns:
            out["group_key"] = out["cluster_display"].astype(str).str.strip()
        else:
            out["group_key"] = UNCATEGORIZED_LABEL
        out["group_key"] = out["group_key"].where(
            out["group_key"].str.strip().ne(""), UNCATEGORIZED_LABEL
        )
        return out

    # Coverage modes: use match_keys (generated in ui.data.enrich_assertions).
    stock_name_by_code: dict[str, str] = {}
    if mode == GROUP_MODE_STOCK:
        stock_name_by_code = _build_stock_name_by_code(out)

    list_col = "__group_key_list"
    out[list_col] = out.apply(
        lambda row: _group_key_list_for_row(
            row,
            group_mode=mode,
            stock_name_by_code=stock_name_by_code,
        ),
        axis=1,
    )
    out = _explode_group_key_list(out, list_col=list_col)
    return out


def _sidebar_filter_uncategorized(
    assertions_joined: pd.DataFrame,
    *,
    group_mode: str,
) -> tuple[pd.DataFrame, bool]:
    mode = str(group_mode or "").strip() or GROUP_MODE_TOPIC
    if mode == GROUP_MODE_CLUSTER:
        show = st.sidebar.checkbox(
            "显示未归类", value=True, key="filter_show_uncategorized_cluster"
        )
        if show or "group_key" not in assertions_joined.columns:
            return assertions_joined, show
        out = assertions_joined[assertions_joined["group_key"] != UNCATEGORIZED_LABEL]
        return out, show

    if mode in {
        GROUP_MODE_STOCK,
        GROUP_MODE_INDUSTRY,
        GROUP_MODE_COMMODITY,
        GROUP_MODE_INDEX,
    }:
        show = st.sidebar.checkbox(
            "显示未标注", value=False, key=f"filter_show_untagged:{mode}"
        )
        if show or "group_key" not in assertions_joined.columns:
            return assertions_joined, show
        out = assertions_joined[assertions_joined["group_key"] != UNTAGGED_LABEL]
        return out, show

    return assertions_joined, True


def _sidebar_filter_groups(
    assertions_joined: pd.DataFrame,
    *,
    assertions_all: pd.DataFrame,
    group_col: str,
    group_label: str,
    group_mode: str,
    show_uncategorized: bool,
) -> pd.DataFrame:
    if group_col not in assertions_all.columns:
        return assertions_joined

    s = assertions_all[group_col].dropna().astype(str).str.strip()
    s = s[s.ne("")]
    # group_mode filtering is already applied in _sidebar_filter_uncategorized,
    # but keep a safe guard here for option building.
    mode = str(group_mode or "").strip() or GROUP_MODE_TOPIC
    if mode == GROUP_MODE_CLUSTER and not show_uncategorized:
        s = s[s.ne(UNCATEGORIZED_LABEL)]
    if (
        mode
        in {
            GROUP_MODE_STOCK,
            GROUP_MODE_INDUSTRY,
            GROUP_MODE_COMMODITY,
            GROUP_MODE_INDEX,
        }
        and not show_uncategorized
    ):
        s = s[s.ne(UNTAGGED_LABEL)]
    if s.empty:
        return assertions_joined

    unique_count = int(s.nunique())
    if unique_count <= MAX_FILTER_OPTIONS:
        group_options = sorted(s.unique().tolist())
        selected_groups = st.sidebar.multiselect(
            group_label, group_options, default=None
        )
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
            st.sidebar.caption(
                f"搜索命中 {len(candidates)} 个（最多 {MAX_FILTER_MATCHES} 个）"
            )

        selected_groups = st.sidebar.multiselect(
            group_label,
            options=[str(x) for x in candidates],
            default=None,
            key=f"filter_group_pick:{group_col}",
        )

    if not selected_groups or group_col not in assertions_joined.columns:
        return assertions_joined
    return assertions_joined[assertions_joined[group_col].isin(selected_groups)]


def _sidebar_filter_actions(
    assertions_joined: pd.DataFrame, *, assertions_all: pd.DataFrame
) -> pd.DataFrame:
    action_options = (
        sorted(assertions_all["action"].dropna().unique().tolist())
        if "action" in assertions_all.columns
        else []
    )
    selected_actions = st.sidebar.multiselect("动作", action_options, default=None)
    if not selected_actions or "action" not in assertions_joined.columns:
        return assertions_joined
    return assertions_joined[assertions_joined["action"].isin(selected_actions)]


def _sidebar_filter_strength_and_confidence(
    assertions_joined: pd.DataFrame,
) -> pd.DataFrame:
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
    start_date, end_date, start_dt, end_dt = _sidebar_date_range(
        posts_filtered, date_col=date_col
    )
    posts_filtered = _filter_posts_by_date(
        posts_filtered, date_col=date_col, start_dt=start_dt, end_dt=end_dt
    )
    posts_filtered = _sidebar_filter_posts_sources(posts_filtered, posts_all=posts_all)
    posts_filtered = _sidebar_filter_posts_platforms(
        posts_filtered, posts_all=posts_all
    )
    posts_filtered = _sidebar_filter_posts_status(posts_filtered, posts_all=posts_all)
    posts_filtered = _sidebar_filter_posts_authors(posts_filtered, posts_all=posts_all)
    posts_filtered, keyword = _sidebar_keyword_filter(posts_filtered)
    posts_filtered = _sidebar_exclude_reposts(posts_filtered)
    return posts_filtered, date_col, start_date, end_date, keyword


def _filter_assertions_sidebar(
    assertions: pd.DataFrame,
    *,
    posts_filtered: pd.DataFrame,
    group_mode: str,
    group_col: str,
    group_label: str,
) -> tuple[pd.DataFrame, pd.DataFrame, bool]:
    assertions_all = _ensure_required_columns(assertions, REQUIRED_ASSERT_COLS)
    assertions_joined = _join_assertions_with_posts(
        assertions_all, posts_filtered=posts_filtered
    )
    assertions_joined = _coalesce_joined_cols(
        assertions_joined, cols=ASSERTIONS_COALESCE_COLS
    )
    assertions_joined = _apply_group_mode(assertions_joined, group_mode=group_mode)
    assertions_joined, show_uncategorized = _sidebar_filter_uncategorized(
        assertions_joined,
        group_mode=group_mode,
    )
    assertions_joined = _sidebar_filter_groups(
        assertions_joined,
        assertions_all=assertions_joined,
        group_col=group_col,
        group_label=group_label,
        group_mode=group_mode,
        show_uncategorized=show_uncategorized,
    )
    assertions_joined = _sidebar_filter_actions(
        assertions_joined, assertions_all=assertions_all
    )
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

    group_mode, group_col, group_label = _sidebar_grouping_config(assertions)
    posts_filtered, date_col, start_date, end_date, keyword = _filter_posts_sidebar(
        posts, posts_all=posts
    )
    assertions_joined, posts_filtered, only_with_assertions = (
        _filter_assertions_sidebar(
            assertions,
            posts_filtered=posts_filtered,
            group_mode=group_mode,
            group_col=group_col,
            group_label=group_label,
        )
    )

    meta = {
        "date_range": (start_date, end_date),
        "keyword": keyword,
        "only_with_assertions": only_with_assertions,
        "date_col": date_col,
        "group_mode": group_mode,
        "group_col": group_col,
        "group_label": group_label,
    }
    return posts_filtered, assertions_joined, meta
