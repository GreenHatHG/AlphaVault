from __future__ import annotations

import time
from typing import Dict

import pandas as pd
import streamlit as st
from sqlalchemy.engine import Engine

from ai_topic_cluster_suggest import ai_is_configured, get_ai_config_summary, suggest_topics_for_cluster
from topic_cluster import (
    delete_cluster_topics,
    ensure_cluster_schema,
    upsert_cluster,
    upsert_cluster_topics,
    upsert_cluster_topics_detailed,
)


def _build_cluster_display_maps(clusters: pd.DataFrame) -> tuple[Dict[str, str], Dict[str, str]]:
    name_by_key: Dict[str, str] = {}
    desc_by_key: Dict[str, str] = {}
    if clusters.empty:
        return name_by_key, desc_by_key
    for _, row in clusters.iterrows():
        key = str(row.get("cluster_key") or "").strip()
        if not key:
            continue
        name_by_key[key] = str(row.get("cluster_name") or "").strip()
        desc_by_key[key] = str(row.get("description") or "").strip()
    return name_by_key, desc_by_key


def _format_cluster_label(cluster_key: str, name_by_key: Dict[str, str]) -> str:
    name = (name_by_key.get(cluster_key) or "").strip()
    if name and name != cluster_key:
        return f"{name} ({cluster_key})"
    return cluster_key


def _normalize_topic_items(raw: object) -> list[dict]:
    if raw is None:
        return []
    if isinstance(raw, list):
        out: list[dict] = []
        for item in raw:
            if isinstance(item, str):
                topic_key = item.strip()
                if topic_key:
                    out.append({"topic_key": topic_key, "confidence": None, "reason": ""})
                continue
            if isinstance(item, dict):
                topic_key = str(item.get("topic_key") or "").strip()
                if not topic_key:
                    continue
                out.append(
                    {
                        "topic_key": topic_key,
                        "confidence": item.get("confidence", None),
                        "reason": str(item.get("reason") or "").strip(),
                    }
                )
        return out
    return []


def _pick_first_nonempty_from_list_col(values: pd.Series) -> str:
    for row in values:
        if not isinstance(row, list):
            continue
        for item in row:
            s = str(item or "").strip()
            if s:
                return s
    return ""


def _build_candidate_records(
    assertions_all: pd.DataFrame,
    topic_keys: list[str],
    count_by_topic: dict,
) -> list[dict]:
    if not topic_keys:
        return []

    cols = ["topic_key"]
    for col in ["topic_type", "topic_value", "stock_names", "industries", "commodities", "indices"]:
        if col in assertions_all.columns:
            cols.append(col)
    df = assertions_all[cols].copy()
    df["topic_key"] = df["topic_key"].astype(str).str.strip()
    df = df[df["topic_key"].isin(set(topic_keys))]
    if df.empty:
        return [{"topic_key": k, "count": int(count_by_topic.get(k, 0)), "hint": ""} for k in topic_keys]

    first_type = (
        df.groupby("topic_key")["topic_type"].first()
        if "topic_type" in df.columns
        else pd.Series(dtype=str)
    )
    first_value = (
        df.groupby("topic_key")["topic_value"].first()
        if "topic_value" in df.columns
        else pd.Series(dtype=str)
    )
    first_stock_name = (
        df.groupby("topic_key")["stock_names"].apply(_pick_first_nonempty_from_list_col)
        if "stock_names" in df.columns
        else pd.Series(dtype=str)
    )
    first_industry = (
        df.groupby("topic_key")["industries"].apply(_pick_first_nonempty_from_list_col)
        if "industries" in df.columns
        else pd.Series(dtype=str)
    )

    records: list[dict] = []
    for topic_key in topic_keys:
        count = int(count_by_topic.get(topic_key, 0))
        t = str(first_type.get(topic_key, "") or "").strip()
        v = str(first_value.get(topic_key, "") or "").strip()
        stock_name = str(first_stock_name.get(topic_key, "") or "").strip() if isinstance(first_stock_name, pd.Series) else ""
        industry = str(first_industry.get(topic_key, "") or "").strip() if isinstance(first_industry, pd.Series) else ""
        hint_parts: list[str] = []
        if t == "stock":
            if stock_name:
                hint_parts.append(f"stock_name={stock_name}")
            if industry:
                hint_parts.append(f"industry={industry}")
            if not hint_parts and v:
                hint_parts.append(f"stock_value={v}")
        hint = ", ".join(hint_parts)[:120]
        records.append({"topic_key": topic_key, "count": count, "hint": hint})
    return records


def show_topic_cluster_admin(
    *,
    engine: Engine,
    assertions_all: pd.DataFrame,
    clusters: pd.DataFrame,
    topic_map: pd.DataFrame,
    load_error: str,
) -> None:
    st.markdown("**主题聚合（板块）**")
    st.caption("把很多 topic_key 合成一个板块看；不会改旧 topic_key。")

    if load_error:
        st.warning(f"聚合表可能还没初始化，或读取失败：{load_error}")
        if st.button("初始化聚合表（创建缺失表）", type="primary"):
            try:
                ensure_cluster_schema(engine)
            except Exception as exc:
                st.error(f"初始化失败：{type(exc).__name__}: {exc}")
                st.stop()
            st.cache_data.clear()
            st.rerun()

    st.divider()
    st.markdown("**创建 / 更新板块**")
    with st.form("cluster_upsert_form", clear_on_submit=False):
        cluster_key = st.text_input("板块ID（cluster_key）", value="metal")
        cluster_name = st.text_input("板块名字（cluster_name）", value="金属")
        description = st.text_area("说明（可空）", value="")
        submitted = st.form_submit_button("保存")
        if submitted:
            if not str(cluster_key or "").strip():
                st.error("板块ID 不能为空。")
                st.stop()
            if not str(cluster_name or "").strip():
                st.error("板块名字 不能为空。")
                st.stop()
            try:
                ensure_cluster_schema(engine)
                upsert_cluster(
                    engine,
                    cluster_key=cluster_key,
                    cluster_name=cluster_name,
                    description=description,
                )
            except Exception as exc:
                st.error(f"保存失败：{type(exc).__name__}: {exc}")
                st.stop()
            st.success("已保存。")
            st.cache_data.clear()
            st.rerun()

    st.divider()
    st.markdown("**成员管理（topic_key → 板块）**")
    if clusters.empty:
        st.info("还没有板块。先在上面创建一个。")
        return

    name_by_key, _desc_by_key = _build_cluster_display_maps(clusters)
    cluster_keys = sorted([str(x).strip() for x in clusters["cluster_key"].dropna().tolist() if str(x).strip()])
    selected_cluster = st.selectbox(
        "选择板块",
        options=cluster_keys,
        format_func=lambda key: _format_cluster_label(str(key), name_by_key),
        key="topic_cluster_selected_cluster",
    )
    selected_cluster = str(selected_cluster or "").strip()
    if not selected_cluster:
        st.info("请选择一个板块。")
        return

    current = topic_map[topic_map["cluster_key"] == selected_cluster].copy() if not topic_map.empty else pd.DataFrame()
    current_topics = sorted([str(x).strip() for x in current.get("topic_key", pd.Series(dtype=str)).dropna().tolist() if str(x).strip()])

    def apply_keyword_search(word: str) -> None:
        st.session_state["topic_cluster_manage_action"] = "增加"
        st.session_state["topic_cluster_search_topic_key"] = str(word or "")
        st.session_state["topic_cluster_to_add"] = []

    col_left, col_mid = st.columns([2, 1])
    with col_left:
        st.caption("v1 规则：一个 topic_key 只能属于一个板块；加入会自动“移动”。")
    with col_mid:
        st.metric("当前成员数", f"{len(current_topics)}")

    # AI keywords shortcut: click to fill the search box below.
    ai_shortcut = st.session_state.get(f"cluster_ai_result:{selected_cluster}", None)
    if isinstance(ai_shortcut, dict):
        shortcut_keywords = ai_shortcut.get("keywords")
        if isinstance(shortcut_keywords, list):
            shortcut_words = [str(x).strip() for x in shortcut_keywords if str(x).strip()]
        else:
            shortcut_words = []
    else:
        shortcut_words = []

    if shortcut_words:
        st.markdown("**AI keywords 快捷搜**")
        st.caption("点一下：自动切到“增加”，并把搜索框填好。")
        words_for_buttons = shortcut_words[:15]
        cols_kw = st.columns(min(5, len(words_for_buttons)))
        for idx, word in enumerate(words_for_buttons):
            col = cols_kw[idx % len(cols_kw)]
            col.button(
                str(word),
                key=f"cluster_ai_keyword_quick_btn:{selected_cluster}:{idx}",
                on_click=apply_keyword_search,
                args=(str(word),),
            )

    action = st.radio("操作", options=["增加", "移除"], horizontal=True, key="topic_cluster_manage_action")
    if action == "增加":
        st.markdown("**增加**")
        all_topic_keys = sorted(
            [
                str(x).strip()
                for x in assertions_all.get("topic_key", pd.Series(dtype=str)).dropna().unique().tolist()
                if str(x).strip()
            ]
        )
        search = st.text_input("搜索 topic_key（建议先搜）", value="", key="topic_cluster_search_topic_key").strip()
        if search:
            candidates = [k for k in all_topic_keys if search.lower() in k.lower()]
            candidates = candidates[:300]
        else:
            candidates = []
            st.caption("提示：topic_key 可能很多，请先搜索再增加。")

        to_add = st.multiselect(
            "选择要增加的 topic_key",
            options=candidates,
            default=[],
            key="topic_cluster_to_add",
        )
        if st.button("增加到这个板块", disabled=not bool(to_add)):
            try:
                ensure_cluster_schema(engine)
                n = upsert_cluster_topics(engine, cluster_key=selected_cluster, topic_keys=to_add)
            except Exception as exc:
                st.error(f"增加失败：{type(exc).__name__}: {exc}")
                st.stop()
            st.success(f"已增加 {n} 个 topic_key。")
            st.cache_data.clear()
            st.rerun()
    else:
        st.markdown("**移除**")
        to_remove = st.multiselect(
            "选择要移除的 topic_key",
            options=current_topics,
            default=[],
            key="topic_cluster_to_remove",
        )
        if st.button("从板块移除", disabled=not bool(to_remove)):
            try:
                ensure_cluster_schema(engine)
                n = delete_cluster_topics(engine, topic_keys=to_remove)
            except Exception as exc:
                st.error(f"移除失败：{type(exc).__name__}: {exc}")
                st.stop()
            st.success(f"已移除 {n} 个 topic_key。")
            st.cache_data.clear()
            st.rerun()

    st.markdown("**当前成员列表**")
    if current_topics:
        st.dataframe(
            pd.DataFrame({"topic_key": current_topics}).head(500),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("这个板块还没有成员。")

    st.divider()
    st.markdown("**AI 筛 topic_key（只输入板块）**")
    ok, ai_err = ai_is_configured()
    if not ok:
        st.info(f"AI 没配好：{ai_err}。需要设置环境变量 AI_API_KEY（以及可选 AI_MODEL/AI_BASE_URL）。")
        return
    config_summary, _cfg_err = get_ai_config_summary()
    if config_summary:
        st.caption(
            "AI 配置："
            + f"model={config_summary.get('model','')}, "
            + f"api_mode={config_summary.get('api_mode','')}, "
            + f"base_url={config_summary.get('base_url','') or '（空）'}"
        )

    cluster_name = (name_by_key.get(selected_cluster) or selected_cluster).strip()
    cluster_desc = (_desc_by_key.get(selected_cluster) or "").strip()
    st.caption(f"当前板块：{cluster_name}")

    if assertions_all.empty or "topic_key" not in assertions_all.columns:
        st.info("没有 topic_key 数据，无法给 AI 作为候选。")
        return

    topic_counts = assertions_all["topic_key"].dropna().astype(str).str.strip()
    topic_counts = topic_counts[topic_counts.ne("")]
    if topic_counts.empty:
        st.info("topic_key 全空，无法筛选。")
        return

    only_unmapped = st.checkbox(
        "只建议未归类 topic_key",
        value=True,
        key="cluster_ai_only_unmapped",
        help="打开：只从“还没进任何板块”的 topic_key 里选。关闭：可能会把别的板块成员也选进来（写入会移动）。",
    )

    counts_series = topic_counts.value_counts()
    if only_unmapped and not topic_map.empty:
        mapped = set(topic_map["topic_key"].dropna().astype(str).str.strip().tolist())
        if mapped:
            counts_series = counts_series[~counts_series.index.isin(mapped)]

    total_topics = int(len(counts_series))
    st.caption(f"候选 topic_key 数量：{total_topics}（将分批让 AI 处理）")

    with st.expander("高级（可不看）", expanded=False):
        debug_terminal_logs = st.checkbox(
            "输出 log 到终端",
            value=False,
            help="打开：你运行 streamlit 的终端会看到每一批 AI 调用的耗时与数量。",
        )
        default_max_total_topics = min(5000, max(200, total_topics))
        max_total_topics = int(
            st.number_input(
                "最多处理 topic_key 数量",
                min_value=200,
                max_value=12000,
                value=int(default_max_total_topics),
                step=100,
                help="越大：越全，但更慢、也更费。一般 2000~5000 就够看效果。",
                key="cluster_ai_max_total_topics_input",
            )
        )
        chunk_size = int(
            st.number_input(
                "每批 topic_key 数量",
                min_value=100,
                max_value=800,
                value=400,
                step=50,
                help="每批越大：调用次数更少，但更容易超长；每批越小：更稳，但调用次数更多。",
                key="cluster_ai_chunk_size_input",
            )
        )

    topic_keys = [str(x).strip() for x in counts_series.index.tolist() if str(x).strip()]
    topic_keys = topic_keys[: int(max_total_topics)]
    count_by_topic = {str(k): int(v) for k, v in counts_series.head(int(max_total_topics)).items()}
    candidate_records = _build_candidate_records(assertions_all, topic_keys, count_by_topic)
    candidate_set = set(topic_keys)
    hint_by_topic = {
        str(item.get("topic_key") or "").strip(): str(item.get("hint") or "").strip()
        for item in candidate_records
        if str(item.get("topic_key") or "").strip()
    }

    call_count = (len(topic_keys) + int(chunk_size) - 1) // int(chunk_size) if topic_keys else 0
    if st.button(f"让 AI 分批筛 topic_key（共 {call_count} 次调用）", type="primary", disabled=not bool(topic_keys)):
        chunks: list[list[dict]] = []
        for i in range(0, len(candidate_records), int(chunk_size)):
            chunks.append(candidate_records[i : i + int(chunk_size)])

        merged: dict = {
            "include_topics": [],
            "unsure_topics": [],
            "exclude_topics": [],
            "keywords": [],
            "negative_keywords": [],
        }
        call_logs: list[dict] = []
        progress = st.progress(0.0, text="AI 处理中...")
        log_placeholder = st.empty()
        if debug_terminal_logs:
            print(
                f"[cluster-ai] start cluster={cluster_name} total_topics={len(topic_keys)} "
                f"chunk_size={int(chunk_size)} calls={len(chunks)}",
                flush=True,
            )
        for idx, chunk in enumerate(chunks, start=1):
            progress.progress(
                float(idx - 1) / float(len(chunks)),
                text=f"准备调用 AI... {idx}/{len(chunks)}",
            )
            if debug_terminal_logs:
                print(
                    f"[cluster-ai] call {idx}/{len(chunks)} topics={len(chunk)}",
                    flush=True,
                )
            start_ts = time.time()
            try:
                part = suggest_topics_for_cluster(
                    cluster_name=cluster_name,
                    description=cluster_desc,
                    candidates=chunk,
                )
            except Exception as exc:
                cost_sec = max(0.0, time.time() - start_ts)
                call_logs.append(
                    {
                        "batch": idx,
                        "topics": len(chunk),
                        "sec": round(cost_sec, 2),
                        "include": 0,
                        "unsure": 0,
                        "exclude": 0,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                st.session_state[f"cluster_ai_call_logs:{selected_cluster}"] = call_logs
                log_placeholder.dataframe(pd.DataFrame(call_logs), use_container_width=True, hide_index=True)
                if debug_terminal_logs:
                    print(
                        f"[cluster-ai] error {idx}/{len(chunks)} sec={cost_sec:.2f} {type(exc).__name__}: {exc}",
                        flush=True,
                    )
                st.error(f"AI 失败（第 {idx}/{len(chunks)} 批）：{type(exc).__name__}: {exc}")
                st.stop()
            cost_sec = max(0.0, time.time() - start_ts)
            include_n = len(part.get("include_topics") or []) if isinstance(part.get("include_topics"), list) else 0
            unsure_n = len(part.get("unsure_topics") or []) if isinstance(part.get("unsure_topics"), list) else 0
            exclude_n = len(part.get("exclude_topics") or []) if isinstance(part.get("exclude_topics"), list) else 0
            call_logs.append(
                {
                    "batch": idx,
                    "topics": len(chunk),
                    "sec": round(cost_sec, 2),
                    "include": include_n,
                    "unsure": unsure_n,
                    "exclude": exclude_n,
                    "error": "",
                }
            )
            st.session_state[f"cluster_ai_call_logs:{selected_cluster}"] = call_logs
            log_placeholder.dataframe(pd.DataFrame(call_logs), use_container_width=True, hide_index=True)
            if debug_terminal_logs:
                print(
                    f"[cluster-ai] batch {idx}/{len(chunks)} topics={len(chunk)} sec={cost_sec:.2f} "
                    f"include={include_n} unsure={unsure_n} exclude={exclude_n}",
                    flush=True,
                )

            for key in ["include_topics", "unsure_topics", "exclude_topics"]:
                items = part.get(key)
                if isinstance(items, list):
                    merged[key].extend(items)
            for key in ["keywords", "negative_keywords"]:
                items = part.get(key)
                if isinstance(items, list):
                    merged[key].extend(items)

            progress.progress(float(idx) / float(len(chunks)), text=f"AI 处理中... {idx}/{len(chunks)}")

        def uniq_str(items: list) -> list[str]:
            out: list[str] = []
            seen: set[str] = set()
            for item in items:
                s = str(item or "").strip()
                if not s or s in seen:
                    continue
                seen.add(s)
                out.append(s)
            return out

        merged["keywords"] = uniq_str(merged.get("keywords", []))
        merged["negative_keywords"] = uniq_str(merged.get("negative_keywords", []))

        st.session_state[f"cluster_ai_result:{selected_cluster}"] = merged
        st.session_state[f"cluster_ai_call_logs:{selected_cluster}"] = call_logs

    result = st.session_state.get(f"cluster_ai_result:{selected_cluster}", None)
    if not isinstance(result, dict):
        st.caption("提示：点上面的按钮，AI 才会给结果。")
        return

    call_logs = st.session_state.get(f"cluster_ai_call_logs:{selected_cluster}", None)
    if isinstance(call_logs, list) and call_logs:
        st.markdown("**本次 AI 调用记录**")
        st.dataframe(pd.DataFrame(call_logs), use_container_width=True, hide_index=True)

    include_items = _normalize_topic_items(result.get("include_topics"))
    unsure_items = _normalize_topic_items(result.get("unsure_topics"))
    exclude_items = _normalize_topic_items(result.get("exclude_topics"))

    def filter_to_candidates(items: list[dict]) -> list[dict]:
        out: list[dict] = []
        for item in items:
            topic_key = str(item.get("topic_key") or "").strip()
            if not topic_key or topic_key not in candidate_set:
                continue
            out.append(
                {
                    **item,
                    "count": int(count_by_topic.get(topic_key, 0)),
                    "hint": str(hint_by_topic.get(topic_key, "") or "").strip(),
                }
            )
        return out

    include_items = filter_to_candidates(include_items)
    unsure_items = filter_to_candidates(unsure_items)
    exclude_items = filter_to_candidates(exclude_items)

    keywords = result.get("keywords")
    words: list[str] = []
    if isinstance(keywords, list) and keywords:
        words = [str(x).strip() for x in keywords if str(x).strip()]
        if words:
            st.caption("AI keywords: " + ", ".join(words[:30]))
            st.caption("点一个 keyword：去上面“成员管理→增加”里搜。")
            words_for_buttons = words[:15]
            cols_kw2 = st.columns(min(5, len(words_for_buttons)))
            for idx, word in enumerate(words_for_buttons):
                col = cols_kw2[idx % len(cols_kw2)]
                col.button(
                    str(word),
                    key=f"cluster_ai_keyword_btn_ai:{selected_cluster}:{idx}",
                    on_click=apply_keyword_search,
                    args=(str(word),),
                )

    negative_keywords = result.get("negative_keywords")
    negative_words: list[str] = []
    if isinstance(negative_keywords, list) and negative_keywords:
        negative_words = [str(x).strip() for x in negative_keywords if str(x).strip()]
        if negative_words:
            st.caption("AI negative: " + ", ".join(negative_words[:30]))

    def contains_negative(text_value: object) -> bool:
        text = str(text_value or "").lower()
        for word in negative_words:
            if word and word.lower() in text:
                return True
        return False

    hide_negative = False
    if negative_words:
        hide_negative = st.checkbox("隐藏 negative", value=False, key="cluster_ai_hide_negative")
        if hide_negative:
            include_items = [
                item
                for item in include_items
                if not (contains_negative(item.get("topic_key")) or contains_negative(item.get("hint")))
            ]
            unsure_items = [
                item
                for item in unsure_items
                if not (contains_negative(item.get("topic_key")) or contains_negative(item.get("hint")))
            ]
            exclude_items = [
                item
                for item in exclude_items
                if not (contains_negative(item.get("topic_key")) or contains_negative(item.get("hint")))
            ]

    col_a, col_b, col_c = st.columns(3)
    col_a.metric("include", f"{len(include_items)}")
    col_b.metric("unsure", f"{len(unsure_items)}")
    col_c.metric("exclude", f"{len(exclude_items)}")

    show_unsure = st.checkbox("显示 unsure", value=True, key="cluster_ai_show_unsure")
    show_exclude = st.checkbox("显示 exclude", value=False, key="cluster_ai_show_exclude")

    if include_items:
        st.markdown("**include（建议加入）**")
        st.dataframe(
            pd.DataFrame(include_items).sort_values(by=["count"], ascending=False).head(300),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("include 为空。你可以在“高级”里调大“最多处理 topic_key 数量”，或者换个板块名字更具体。")

    if show_unsure and unsure_items:
        st.markdown("**unsure（不确定）**")
        st.dataframe(
            pd.DataFrame(unsure_items).sort_values(by=["count"], ascending=False).head(300),
            use_container_width=True,
            hide_index=True,
        )

    if show_exclude and exclude_items:
        st.markdown("**exclude（不加入）**")
        st.dataframe(
            pd.DataFrame(exclude_items).sort_values(by=["count"], ascending=False).head(300),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("**写入板块成员**")
    st.caption("先选，再写入：新增默认全选；移入默认不选；unsure 默认不选。")

    # Build a quick topic_key -> cluster_key map for grouping (new vs move-in).
    topic_to_cluster: dict[str, str] = {}
    if not topic_map.empty and "topic_key" in topic_map.columns and "cluster_key" in topic_map.columns:
        for _, row in topic_map[["topic_key", "cluster_key"]].dropna().iterrows():
            topic_key = str(row.get("topic_key") or "").strip()
            cluster_key = str(row.get("cluster_key") or "").strip()
            if topic_key and cluster_key:
                topic_to_cluster[topic_key] = cluster_key

    def parse_confidence(raw: object, default_value: float) -> float:
        try:
            val = float(raw)
        except Exception:
            val = float(default_value)
        return max(0.0, min(1.0, float(val)))

    include_conf_by_topic: dict[str, float] = {}
    for item in include_items:
        topic_key = str(item.get("topic_key") or "").strip()
        if topic_key:
            include_conf_by_topic[topic_key] = parse_confidence(item.get("confidence", None), 0.8)

    unsure_conf_by_topic: dict[str, float] = {}
    for item in unsure_items:
        topic_key = str(item.get("topic_key") or "").strip()
        if topic_key and topic_key not in include_conf_by_topic:
            unsure_conf_by_topic[topic_key] = parse_confidence(item.get("confidence", None), 0.55)

    def split_new_and_move(topic_keys: list[str]) -> tuple[list[str], list[str], dict[str, str]]:
        new_keys: list[str] = []
        move_keys: list[str] = []
        from_cluster_by_topic: dict[str, str] = {}
        for topic_key in topic_keys:
            existing_cluster = str(topic_to_cluster.get(topic_key, "") or "").strip()
            if not existing_cluster:
                new_keys.append(topic_key)
                continue
            if existing_cluster == selected_cluster:
                continue
            move_keys.append(topic_key)
            from_cluster_by_topic[topic_key] = existing_cluster
        return new_keys, move_keys, from_cluster_by_topic

    include_keys = [str(item.get("topic_key") or "").strip() for item in include_items if str(item.get("topic_key") or "").strip()]
    include_keys = sorted(set(include_keys))
    include_new, include_move, include_from = split_new_and_move(include_keys)

    unsure_keys = [str(item.get("topic_key") or "").strip() for item in unsure_items if str(item.get("topic_key") or "").strip()]
    unsure_keys = sorted(set(unsure_keys))
    unsure_new, unsure_move, unsure_from = split_new_and_move(unsure_keys)

    def sort_by_count(items: list[str]) -> list[str]:
        return sorted(items, key=lambda k: (-int(count_by_topic.get(k, 0)), str(k)))

    include_new = sort_by_count(include_new)
    include_move = sort_by_count(include_move)
    unsure_new = sort_by_count(unsure_new)
    unsure_move = sort_by_count(unsure_move)

    from_cluster_by_topic = {**include_from, **unsure_from}

    def format_basic(topic_key: str) -> str:
        count = int(count_by_topic.get(topic_key, 0))
        return f"{topic_key}（{count}次）" if count else topic_key

    def format_move(topic_key: str) -> str:
        from_key = str(from_cluster_by_topic.get(topic_key, "") or "").strip()
        from_label = _format_cluster_label(from_key, name_by_key) if from_key else "未知"
        count = int(count_by_topic.get(topic_key, 0))
        count_part = f"，{count}次" if count else ""
        return f"{topic_key}（从 {from_label} 移入{count_part}）"

    col_w1, col_w2 = st.columns(2)
    with col_w1:
        st.markdown("**include：新增**")
        include_new_selected = st.multiselect(
            "选择要新增进本板块的 topic_key",
            options=include_new,
            default=include_new,
            format_func=format_basic,
            key=f"cluster_ai_write_include_new:{selected_cluster}",
        )
    with col_w2:
        st.markdown("**include：移入**")
        st.caption("提示：移入会把 topic_key 从原板块挪到本板块。")
        include_move_selected = st.multiselect(
            "选择要移入本板块的 topic_key",
            options=include_move,
            default=[],
            format_func=format_move,
            key=f"cluster_ai_write_include_move:{selected_cluster}",
        )

    want_unsure = st.checkbox("我也要选 unsure", value=False, key=f"cluster_ai_write_want_unsure:{selected_cluster}")
    unsure_new_selected: list[str] = []
    unsure_move_selected: list[str] = []
    if want_unsure and (unsure_new or unsure_move):
        col_u1, col_u2 = st.columns(2)
        with col_u1:
            st.markdown("**unsure：新增**")
            unsure_new_selected = st.multiselect(
                "选择要新增（unsure）的 topic_key",
                options=unsure_new,
                default=[],
                format_func=format_basic,
                key=f"cluster_ai_write_unsure_new:{selected_cluster}",
            )
        with col_u2:
            st.markdown("**unsure：移入**")
            unsure_move_selected = st.multiselect(
                "选择要移入（unsure）的 topic_key",
                options=unsure_move,
                default=[],
                format_func=format_move,
                key=f"cluster_ai_write_unsure_move:{selected_cluster}",
            )

    to_write = [
        *[str(x).strip() for x in include_new_selected],
        *[str(x).strip() for x in include_move_selected],
        *[str(x).strip() for x in unsure_new_selected],
        *[str(x).strip() for x in unsure_move_selected],
    ]
    to_write = [x for x in to_write if x]
    to_write = sorted(set(to_write))
    st.caption(f"本次将写入：{len(to_write)} 个 topic_key")

    if st.button("写入这个板块（按你勾选的）", disabled=not bool(to_write)):
        try:
            ensure_cluster_schema(engine)
            payloads: list[dict] = []
            for topic_key in to_write:
                conf = include_conf_by_topic.get(topic_key)
                if conf is None:
                    conf = unsure_conf_by_topic.get(topic_key, 0.75)
                payloads.append({"topic_key": topic_key, "source": "ai", "confidence": float(conf)})
            n = upsert_cluster_topics_detailed(
                engine,
                cluster_key=selected_cluster,
                topic_items=payloads,
                default_source="ai",
                default_confidence=0.75,
            )
        except Exception as exc:
            st.error(f"写入失败：{type(exc).__name__}: {exc}")
            st.stop()
        st.success(f"已写入 {n} 个 topic_key。")
        st.cache_data.clear()
        st.rerun()
