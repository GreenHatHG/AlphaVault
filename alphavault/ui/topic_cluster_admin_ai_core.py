from __future__ import annotations

import time
from typing import Dict

import pandas as pd
import streamlit as st
from sqlalchemy.engine import Engine

from alphavault.ai.topic_cluster_suggest import ai_is_configured, get_ai_config_summary, suggest_topics_for_cluster
from alphavault.ui.topic_cluster_admin_ai_write import _render_ai_write_section
from alphavault.ui.topic_cluster_admin_helpers import (
    _build_candidate_records,
    _contains_any_word,
    _filter_items_to_candidates,
    _normalize_topic_items,
    _uniq_str,
)

MIN_CHUNK_SIZE = 1
RECOMMENDED_MIN_CHUNK_SIZE = 100
DEFAULT_CHUNK_SIZE = RECOMMENDED_MIN_CHUNK_SIZE
MAX_CHUNK_SIZE = 800
CHUNK_SIZE_STEP = 50


def _run_ai_batches(
    *,
    cluster_name: str,
    cluster_desc: str,
    candidate_records: list[dict],
    chunk_size: int,
    selected_cluster: str,
    debug_terminal_logs: bool,
) -> None:
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
            f"[cluster-ai] start cluster={cluster_name} total_topics={len(candidate_records)} "
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
            log_placeholder.dataframe(pd.DataFrame(call_logs), width="stretch", hide_index=True)
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
        log_placeholder.dataframe(pd.DataFrame(call_logs), width="stretch", hide_index=True)

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

    merged["keywords"] = _uniq_str(merged.get("keywords", []))
    merged["negative_keywords"] = _uniq_str(merged.get("negative_keywords", []))

    st.session_state[f"cluster_ai_result:{selected_cluster}"] = merged
    st.session_state[f"cluster_ai_call_logs:{selected_cluster}"] = call_logs


def _render_ai_section(
    *,
    engine: Engine,
    assertions_all: pd.DataFrame,
    topic_map: pd.DataFrame,
    selected_cluster: str,
    cluster_name: str,
    cluster_desc: str,
    name_by_key: Dict[str, str],
    apply_keyword_search,
) -> None:
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

    st.caption(f"当前板块：{cluster_name}")

    if assertions_all.empty or "topic_key" not in assertions_all.columns:
        st.info("没有 topic_key 数据，无法给 AI 作为候选。")
        return

    topic_counts = assertions_all["topic_key"].dropna().astype(str).str.strip()
    topic_counts = topic_counts[topic_counts.ne("")]
    if topic_counts.empty:
        st.info("topic_key 全空，无法筛选。")
        return

    counts_series = topic_counts.value_counts()

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
                min_value=MIN_CHUNK_SIZE,
                max_value=MAX_CHUNK_SIZE,
                value=DEFAULT_CHUNK_SIZE,
                step=CHUNK_SIZE_STEP,
                help=(
                    f"建议每批至少 {RECOMMENDED_MIN_CHUNK_SIZE}。"
                    "每批越大：调用次数更少，但更容易超长；每批越小：更稳，但调用次数更多。"
                ),
                key="cluster_ai_chunk_size_input",
            )
        )
        if chunk_size < RECOMMENDED_MIN_CHUNK_SIZE:
            # Soft guidance: allow small chunk_size, but warn about time/cost.
            st.warning(
                f"你现在每批是 {chunk_size} 个 topic_key。这样 AI 要调用很多次，会更慢、也更费。"
                f"建议每批至少 {RECOMMENDED_MIN_CHUNK_SIZE}（default={DEFAULT_CHUNK_SIZE}）。"
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
    if st.button(
        f"让 AI 分批筛 topic_key（共 {call_count} 次调用）",
        type="primary",
        disabled=not bool(topic_keys),
    ):
        _run_ai_batches(
            cluster_name=cluster_name,
            cluster_desc=cluster_desc,
            candidate_records=candidate_records,
            chunk_size=int(chunk_size),
            selected_cluster=selected_cluster,
            debug_terminal_logs=bool(debug_terminal_logs),
        )

    result = st.session_state.get(f"cluster_ai_result:{selected_cluster}", None)
    if not isinstance(result, dict):
        st.caption("提示：点上面的按钮，AI 才会给结果。")
        return

    call_logs = st.session_state.get(f"cluster_ai_call_logs:{selected_cluster}", None)
    if isinstance(call_logs, list) and call_logs:
        st.markdown("**本次 AI 调用记录**")
        st.dataframe(pd.DataFrame(call_logs), width="stretch", hide_index=True)

    include_items = _normalize_topic_items(result.get("include_topics"))
    unsure_items = _normalize_topic_items(result.get("unsure_topics"))
    exclude_items = _normalize_topic_items(result.get("exclude_topics"))

    include_items = _filter_items_to_candidates(
        include_items,
        candidate_set=candidate_set,
        count_by_topic=count_by_topic,
        hint_by_topic=hint_by_topic,
    )
    unsure_items = _filter_items_to_candidates(
        unsure_items,
        candidate_set=candidate_set,
        count_by_topic=count_by_topic,
        hint_by_topic=hint_by_topic,
    )
    exclude_items = _filter_items_to_candidates(
        exclude_items,
        candidate_set=candidate_set,
        count_by_topic=count_by_topic,
        hint_by_topic=hint_by_topic,
    )

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

    hide_negative = False
    if negative_words:
        hide_negative = st.checkbox("隐藏 negative", value=False, key="cluster_ai_hide_negative")
        if hide_negative:
            include_items = [
                item
                for item in include_items
                if not (
                    _contains_any_word(negative_words, item.get("topic_key"))
                    or _contains_any_word(negative_words, item.get("hint"))
                )
            ]
            unsure_items = [
                item
                for item in unsure_items
                if not (
                    _contains_any_word(negative_words, item.get("topic_key"))
                    or _contains_any_word(negative_words, item.get("hint"))
                )
            ]
            exclude_items = [
                item
                for item in exclude_items
                if not (
                    _contains_any_word(negative_words, item.get("topic_key"))
                    or _contains_any_word(negative_words, item.get("hint"))
                )
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
            width="stretch",
            hide_index=True,
        )
    else:
        st.info("include 为空。你可以在“高级”里调大“最多处理 topic_key 数量”，或者换个板块名字更具体。")

    if show_unsure and unsure_items:
        st.markdown("**unsure（不确定）**")
        st.dataframe(
            pd.DataFrame(unsure_items).sort_values(by=["count"], ascending=False).head(300),
            width="stretch",
            hide_index=True,
        )

    if show_exclude and exclude_items:
        st.markdown("**exclude（不加入）**")
        st.dataframe(
            pd.DataFrame(exclude_items).sort_values(by=["count"], ascending=False).head(300),
            width="stretch",
            hide_index=True,
        )

    _render_ai_write_section(
        engine=engine,
        topic_map=topic_map,
        selected_cluster=selected_cluster,
        include_items=include_items,
        unsure_items=unsure_items,
        count_by_topic=count_by_topic,
        name_by_key=name_by_key,
    )
