"""
Streamlit tab: follow pages (topic / cluster).

User flow:
1) Create a page: follow a key (topic_key/stock/industry/...) or a cluster
2) Keywords OR (optional)
3) Render thread tree (reuse existing tree builder)
"""

from __future__ import annotations

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
from alphavault.ai.topic_cluster_suggest import ai_is_configured
from alphavault.ai.follow_keywords_suggest import suggest_keywords_for_follow
from alphavault.ui.keyword_or import split_keywords_or
from alphavault.ui.thread_tree import build_weibo_thread_forest
from alphavault.ui.thread_tree_view import render_thread_forest


DEFAULT_KEY_CANDIDATES_TOP_N = 40
MAX_KEY_CANDIDATES = 500
MAX_EXAMPLE_TEXTS = 10


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


def _build_stock_name_by_code(assertions_filtered: pd.DataFrame) -> dict[str, str]:
    if assertions_filtered.empty:
        return {}
    if (
        "stock_codes" not in assertions_filtered.columns
        or "stock_names" not in assertions_filtered.columns
    ):
        return {}
    counts: dict[str, dict[str, int]] = {}
    for codes, names in zip(
        assertions_filtered["stock_codes"].tolist(),
        assertions_filtered["stock_names"].tolist(),
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
        best = sorted(name_counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[0][
            0
        ]
        out[str(code).strip()] = str(best).strip()
    return out


def _format_key_label(key: str, *, stock_name_by_code: dict[str, str]) -> str:
    k = str(key or "").strip()
    if not k:
        return ""
    if k.startswith("stock:"):
        code = k[len("stock:") :].strip()
        name = str(stock_name_by_code.get(code, "") or "").strip()
        return f"个股：{name} ({code})" if name else f"个股：{code}"
    if k.startswith("industry:"):
        return f"行业：{k[len('industry:') :].strip()}"
    if k.startswith("commodity:"):
        return f"商品：{k[len('commodity:') :].strip()}"
    if k.startswith("index:"):
        return f"指数：{k[len('index:') :].strip()}"
    # Keep raw key for other types (portfolio/macro/method/...).
    return k


def _key_candidates(assertions_filtered: pd.DataFrame) -> pd.Series:
    if assertions_filtered.empty:
        return pd.Series(dtype=int)
    if "match_keys" in assertions_filtered.columns:
        flat: list[str] = []
        for item in assertions_filtered["match_keys"].tolist():
            if not isinstance(item, list):
                continue
            for k in item:
                s = str(k).strip()
                if s:
                    flat.append(s)
        if not flat:
            return pd.Series(dtype=int)
        return pd.Series(flat, dtype=str).value_counts()

    # Fallback: old data path (topic_key only).
    if "topic_key" not in assertions_filtered.columns:
        return pd.Series(dtype=int)
    s = assertions_filtered["topic_key"].dropna().astype(str).str.strip()
    s = s[s.ne("")]
    if s.empty:
        return pd.Series(dtype=int)
    return s.value_counts()


def _select_follow_key(assertions_filtered: pd.DataFrame, *, key_prefix: str) -> str:
    st.caption("提示：key 可能很多，建议先搜索。")
    search = st.text_input(
        "搜索 key（个股/行业/商品/指数/主题...）",
        value="",
        key=f"{key_prefix}:key_search",
    ).strip()

    counts = _key_candidates(assertions_filtered)
    if counts.empty:
        st.info("没有 key 数据。")
        return ""

    stock_name_by_code = _build_stock_name_by_code(assertions_filtered)
    count_by_key = {str(k): int(v) for k, v in counts.items()}

    def _format_option(k: object) -> str:
        key = str(k or "").strip()
        label = _format_key_label(key, stock_name_by_code=stock_name_by_code) or key
        count = int(count_by_key.get(key, 0))
        return f"{label} · {count}次" if count else label

    if search:
        needle = search.lower()
        matched: list[str] = []
        for k in counts.index.tolist():
            key = str(k).strip()
            if not key:
                continue
            label = _format_key_label(key, stock_name_by_code=stock_name_by_code)
            hay = f"{key} {label}".lower()
            if needle in hay:
                matched.append(key)
            if len(matched) >= MAX_KEY_CANDIDATES:
                break
        options = matched
        st.caption(f"搜索命中 {len(options)} 个（最多展示 {MAX_KEY_CANDIDATES} 个）")
    else:
        head = counts.head(DEFAULT_KEY_CANDIDATES_TOP_N)
        options = [str(k) for k in head.index.tolist()]
        st.caption(f"默认展示 Top {DEFAULT_KEY_CANDIDATES_TOP_N}")

    selected = st.selectbox(
        "选择 key",
        options=options,
        format_func=_format_option,
        key=f"{key_prefix}:key_select",
    )
    return str(selected or "").strip()


def _select_cluster_key(
    clusters: pd.DataFrame, assertions_filtered: pd.DataFrame, *, key_prefix: str
) -> str:
    options: list[str] = []
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


def _build_keyword_post_uids(
    assertions_filtered: pd.DataFrame, *, keywords_text: str
) -> set[str]:
    words = split_keywords_or(keywords_text)
    if not words:
        return set()
    escaped = [re.escape(w) for w in words]
    pattern = "|".join(escaped)

    if (
        "post_uid" not in assertions_filtered.columns
        or "raw_text" not in assertions_filtered.columns
    ):
        return set()

    post_text = assertions_filtered[["post_uid", "raw_text"]].copy()
    post_text["post_uid"] = post_text["post_uid"].astype(str).str.strip()
    post_text = post_text[post_text["post_uid"].ne("")]
    post_text = post_text.drop_duplicates(subset=["post_uid"], keep="first")

    mask = (
        post_text["raw_text"]
        .astype(str)
        .str.contains(pattern, case=False, na=False, regex=True)
    )
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
            format_func=lambda x: "key（个股/行业/商品/指数/主题）"
            if x == FOLLOW_TYPE_TOPIC
            else "板块（聚合）",
            horizontal=True,
        )

        if follow_type_label == FOLLOW_TYPE_CLUSTER:
            follow_key = _select_cluster_key(
                clusters, assertions_filtered, key_prefix="follow_pages_create"
            )
        else:
            follow_key = _select_follow_key(
                assertions_filtered, key_prefix="follow_pages_create"
            )

        page_name = st.text_input("页面名字（可空）", value="")

        keywords_widget_key = (
            f"follow_pages_create_keywords:{follow_type_label}:{follow_key}"
        )
        keywords_text = st.text_area(
            "关键字（多个，OR；可空）",
            value="",
            height=80,
            key=keywords_widget_key,
        )

        submitted = st.form_submit_button("保存", type="primary")
        if not submitted:
            return
        if not str(follow_key or "").strip():
            st.error("你要先选一个 key 或板块。")
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
    assertions_filtered: pd.DataFrame,
    page_key: str,
    follow_type: str,
    follow_key: str,
    page_name: str,
    keywords_text: str,
) -> None:
    st.markdown("**页面设置**")

    widget_key = f"follow_pages_update_keywords:{page_key}"

    stock_name_by_code = _build_stock_name_by_code(assertions_filtered)
    follow_label = (
        _format_cluster_label(clusters, follow_key)
        if follow_type == FOLLOW_TYPE_CLUSTER
        else _format_key_label(follow_key, stock_name_by_code=stock_name_by_code)
    )

    def _seed_keywords() -> list[str]:
        k = str(follow_key or "").strip()
        if not k:
            return []
        out: list[str] = []
        if k.startswith("stock:"):
            code = k[len("stock:") :].strip()
            if code:
                out.append(code)
                out.append(
                    code.replace(".SH", "").replace(".SZ", "").replace(".HK", "")
                )
            name = str(stock_name_by_code.get(code, "") or "").strip()
            if name:
                out.append(name)
            return [x for x in out if x]

        if ":" in k:
            _t, v = k.split(":", 1)
            v = v.strip()
            if v:
                out.append(v)
        return [x for x in out if x]

    def _example_texts() -> list[str]:
        if assertions_filtered.empty or "raw_text" not in assertions_filtered.columns:
            return []
        if follow_type == FOLLOW_TYPE_CLUSTER:
            # Best-effort: use cluster_keys list membership if present.
            want = str(follow_key or "").strip()
            if not want or "cluster_keys" not in assertions_filtered.columns:
                return []
            mask = []
            for item in assertions_filtered["cluster_keys"].tolist():
                if not isinstance(item, list):
                    mask.append(False)
                    continue
                keys = [str(x).strip() for x in item if str(x).strip()]
                mask.append(want in keys)
            view = assertions_filtered[pd.Series(mask, index=assertions_filtered.index)]
        else:
            want = str(follow_key or "").strip()
            if not want:
                return []
            if "match_keys" in assertions_filtered.columns:
                mask = []
                for item in assertions_filtered["match_keys"].tolist():
                    if not isinstance(item, list):
                        mask.append(False)
                        continue
                    keys = [str(x).strip() for x in item if str(x).strip()]
                    mask.append(want in keys)
                view = assertions_filtered[
                    pd.Series(mask, index=assertions_filtered.index)
                ]
            elif "topic_key" in assertions_filtered.columns:
                view = assertions_filtered[
                    assertions_filtered["topic_key"].astype(str).str.strip() == want
                ]
            else:
                view = assertions_filtered.head(0).copy()

        post_text = (
            view.get("raw_text", pd.Series(dtype=str)).dropna().astype(str).tolist()
        )
        out: list[str] = []
        seen: set[str] = set()
        for t in post_text:
            s = str(t or "").strip()
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(s[:160])
            if len(out) >= MAX_EXAMPLE_TEXTS:
                break
        return out

    col_a, col_b = st.columns([1, 1])
    with col_a:
        if st.button(
            "填默认关键字",
            type="secondary",
            key=f"follow_pages_seed_keywords:{page_key}",
        ):
            seed = _seed_keywords()
            if seed:
                st.session_state[widget_key] = "\n".join(seed)
                st.rerun()
            st.info("没有默认关键字。")
    with col_b:
        if st.button(
            "AI 推荐关键字",
            type="secondary",
            key=f"follow_pages_ai_keywords:{page_key}",
        ):
            ok, err = ai_is_configured()
            if not ok:
                st.info(f"AI 没配好：{err}")
            else:
                seed = _seed_keywords()
                examples = _example_texts()
                try:
                    result = suggest_keywords_for_follow(
                        follow_key=follow_key,
                        follow_label=follow_label,
                        seed_keywords=seed,
                        example_texts=examples,
                    )
                except Exception as exc:
                    st.error(f"AI 失败：{type(exc).__name__}: {exc}")
                    st.stop()

                keywords = result.get("keywords") or []
                if not isinstance(keywords, list):
                    keywords = []
                words = [str(x).strip() for x in keywords if str(x).strip()]
                st.session_state[widget_key] = "\n".join(words)
                note = str(result.get("note") or "").strip()
                if note:
                    st.session_state[f"{widget_key}:ai_note"] = note
                st.rerun()

    with st.form(f"follow_pages_update_form:{page_key}", clear_on_submit=False):
        st.caption(f"page_key：{page_key}")
        st.caption(
            f"关注：{'板块' if follow_type == FOLLOW_TYPE_CLUSTER else 'key'} · {follow_label or follow_key}"
        )
        ai_note = str(st.session_state.get(f"{widget_key}:ai_note") or "").strip()
        if ai_note:
            st.caption(f"AI 提示：{ai_note}")

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
    st.caption("关注 key（个股/行业/商品/指数/主题）/板块 → 关键字 OR → tree。")

    engine = ensure_turso_engine(turso_url, turso_token)
    pages_df, load_error = load_follow_pages_sources(turso_url, turso_token)

    _maybe_init_follow_pages_tables(engine, load_error)

    st.divider()
    if pages_df.empty:
        st.info("还没有关注页。先新建一个。")
        _render_page_create(
            engine, clusters=clusters, assertions_filtered=assertions_filtered
        )
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
        assertions_filtered=assertions_filtered,
        page_key=selected_page_key,
        follow_type=follow_type,
        follow_key=follow_key,
        page_name=page_name,
        keywords_text=keywords_text,
    )

    col_left, col_right = st.columns([2, 1])
    with col_left:
        with st.expander("新建关注页"):
            _render_page_create(
                engine, clusters=clusters, assertions_filtered=assertions_filtered
            )
    with col_right:
        confirm = st.checkbox(
            "我确认要删除",
            value=False,
            key=f"follow_pages_delete_confirm:{selected_page_key}",
        )
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
                base_view = assertions_filtered[
                    pd.Series(mask, index=assertions_filtered.index)
                ]
            else:
                base_view = assertions_filtered.head(0).copy()
        else:
            base_view = assertions_filtered.head(0).copy()
    else:
        want = str(follow_key or "").strip()
        if not want:
            base_view = assertions_filtered.head(0).copy()
        elif "match_keys" in assertions_filtered.columns:
            mask = []
            for item in assertions_filtered["match_keys"].tolist():
                if not isinstance(item, list):
                    mask.append(False)
                    continue
                keys = [str(x).strip() for x in item if str(x).strip()]
                mask.append(want in keys)
            base_view = assertions_filtered[
                pd.Series(mask, index=assertions_filtered.index)
            ]
        elif "topic_key" in assertions_filtered.columns:
            base_view = assertions_filtered[
                assertions_filtered["topic_key"].astype(str).str.strip() == want
            ]
        else:
            base_view = assertions_filtered.head(0).copy()

    base_post_uids = set(
        str(x).strip()
        for x in base_view.get("post_uid", pd.Series(dtype=str)).dropna().tolist()
        if str(x).strip()
    )
    keyword_post_uids = _build_keyword_post_uids(
        assertions_filtered, keywords_text=str(keywords_text or "")
    )
    merged_uids = base_post_uids | keyword_post_uids

    st.caption(
        f"关注命中 {len(base_post_uids)} 帖，关键字命中 {len(keyword_post_uids)} 帖，合并后 {len(merged_uids)} 帖"
    )

    if not merged_uids:
        st.info("暂无数据。")
        return

    view_df = assertions_filtered[
        assertions_filtered["post_uid"].astype(str).str.strip().isin(merged_uids)
    ]
    threads_all = build_weibo_thread_forest(view_df, posts_all=posts_all)
    render_thread_forest(
        threads_all,
        title="tree（像 1.txt 那样）",
        key_prefix=f"follow_pages_tree:{selected_page_key}",
    )


__all__ = ["show_follow_pages"]
