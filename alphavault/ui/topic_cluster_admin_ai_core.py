from __future__ import annotations

import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Dict

import pandas as pd
import streamlit as st
from sqlalchemy.engine import Engine

from alphavault.ai.topic_cluster_suggest import ai_is_configured, get_ai_config_summary, suggest_topics_for_cluster
from alphavault.constants import DEFAULT_SPOOL_DIR, ENV_SPOOL_DIR
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


def _result_state_key(cluster_key: str) -> str:
    return f"cluster_ai_result:{str(cluster_key or '').strip()}"


def _call_logs_state_key(cluster_key: str) -> str:
    return f"cluster_ai_call_logs:{str(cluster_key or '').strip()}"


def _resume_state_key(cluster_key: str) -> str:
    return f"cluster_ai_resume_state:{str(cluster_key or '').strip()}"


def _resume_enabled_widget_key(cluster_key: str) -> str:
    return f"cluster_ai_resume_enabled:{str(cluster_key or '').strip()}"


def _build_resume_signature(
    *,
    cluster_name: str,
    cluster_desc: str,
    topic_keys: list[str],
    max_total_topics: int,
    chunk_size: int,
) -> str:
    # Keep it stable and small, so we can safely decide whether to resume.
    text = "\n".join([str(x).strip() for x in topic_keys if str(x).strip()])
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()
    payload = {
        "cluster_name": str(cluster_name or "").strip(),
        "cluster_desc": str(cluster_desc or "").strip(),
        "max_total_topics": int(max_total_topics),
        "chunk_size": int(chunk_size),
        "topic_keys_sha1": digest,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


AI_CACHE_FILE_VERSION = 1
AI_CACHE_SUBDIR = "cluster_ai_cache"


def _ai_cache_root_dir() -> Path:
    base = str(os.getenv(ENV_SPOOL_DIR, DEFAULT_SPOOL_DIR) or "").strip() or DEFAULT_SPOOL_DIR
    return Path(base) / AI_CACHE_SUBDIR


def _safe_cache_file_name(cluster_key: str) -> str:
    raw = str(cluster_key or "").strip()
    safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", raw) or "unknown"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10] if raw else "noid"
    return f"{safe}-{digest}.json"


def _cache_file_path(cluster_key: str) -> Path:
    return _ai_cache_root_dir() / _safe_cache_file_name(cluster_key)


def _build_cache_file_payload(
    *,
    signature: str,
    next_batch: int,
    total_batches: int,
    merged: dict,
    call_logs: list[dict],
) -> dict:
    return {
        "version": AI_CACHE_FILE_VERSION,
        "signature": str(signature or ""),
        "next_batch": int(next_batch),
        "total_batches": int(total_batches),
        "updated_at": time.time(),
        "merged": merged,
        "call_logs": call_logs,
    }


def _try_write_cache_file(path: Path, *, payload: dict, debug_terminal_logs: bool) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = Path(str(path) + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(path)
    except Exception as exc:
        if debug_terminal_logs:
            print(f"[cluster-ai] cache write failed: {type(exc).__name__}: {exc}", flush=True)


def _try_load_cache_file(path: Path) -> dict | None:
    try:
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    if int(data.get("version") or 0) != int(AI_CACHE_FILE_VERSION):
        return None
    return data


def _run_ai_batches(
    *,
    cluster_name: str,
    cluster_desc: str,
    candidate_records: list[dict],
    chunk_size: int,
    selected_cluster: str,
    debug_terminal_logs: bool,
    resume_enabled: bool,
    resume_signature: str,
    start_batch_idx: int,
    merged_seed: dict | None,
    call_logs_seed: list[dict] | None,
) -> None:
    chunks: list[list[dict]] = []
    for i in range(0, len(candidate_records), int(chunk_size)):
        chunks.append(candidate_records[i : i + int(chunk_size)])

    result_key = _result_state_key(selected_cluster)
    call_logs_key = _call_logs_state_key(selected_cluster)
    resume_key = _resume_state_key(selected_cluster)
    cache_path = _cache_file_path(selected_cluster) if resume_enabled else None

    if isinstance(merged_seed, dict):
        merged: dict = dict(merged_seed)
    else:
        merged = {}
    for k in ["include_topics", "unsure_topics", "exclude_topics", "keywords", "negative_keywords"]:
        if not isinstance(merged.get(k), list):
            merged[k] = []

    call_logs: list[dict] = call_logs_seed if isinstance(call_logs_seed, list) else []

    if not chunks:
        st.session_state[result_key] = merged
        st.session_state[call_logs_key] = call_logs
        if resume_enabled:
            st.session_state[resume_key] = {
                "signature": resume_signature,
                "next_batch": 1,
                "total_batches": 0,
                "updated_at": time.time(),
            }
            if cache_path is not None:
                payload = _build_cache_file_payload(
                    signature=resume_signature,
                    next_batch=1,
                    total_batches=0,
                    merged=merged,
                    call_logs=call_logs,
                )
                _try_write_cache_file(cache_path, payload=payload, debug_terminal_logs=debug_terminal_logs)
        return

    start_batch_idx = int(start_batch_idx)
    start_batch_idx = max(1, min(start_batch_idx, len(chunks) + 1))

    progress = st.progress(0.0, text="AI 处理中...")
    log_placeholder = st.empty()
    if call_logs:
        log_placeholder.dataframe(pd.DataFrame(call_logs), width="stretch", hide_index=True)

    if resume_enabled:
        st.session_state[resume_key] = {
            "signature": resume_signature,
            "next_batch": start_batch_idx,
            "total_batches": len(chunks),
            "updated_at": time.time(),
        }
        if cache_path is not None:
            payload = _build_cache_file_payload(
                signature=resume_signature,
                next_batch=start_batch_idx,
                total_batches=len(chunks),
                merged=merged,
                call_logs=call_logs,
            )
            _try_write_cache_file(cache_path, payload=payload, debug_terminal_logs=debug_terminal_logs)

    if debug_terminal_logs:
        print(
            f"[cluster-ai] start cluster={cluster_name} total_topics={len(candidate_records)} "
            f"chunk_size={int(chunk_size)} calls={len(chunks)} start_batch={start_batch_idx}",
            flush=True,
        )

    progress.progress(
        float(max(0, start_batch_idx - 1)) / float(len(chunks)),
        text=f"准备调用 AI... {start_batch_idx}/{len(chunks)}",
    )

    for idx in range(start_batch_idx, len(chunks) + 1):
        chunk = chunks[idx - 1]
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
            st.session_state[call_logs_key] = call_logs
            st.session_state[result_key] = merged
            if resume_enabled:
                st.session_state[resume_key] = {
                    "signature": resume_signature,
                    "next_batch": idx,
                    "total_batches": len(chunks),
                    "updated_at": time.time(),
                }
                if cache_path is not None:
                    payload = _build_cache_file_payload(
                        signature=resume_signature,
                        next_batch=idx,
                        total_batches=len(chunks),
                        merged=merged,
                        call_logs=call_logs,
                    )
                    _try_write_cache_file(
                        cache_path,
                        payload=payload,
                        debug_terminal_logs=debug_terminal_logs,
                    )
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
        st.session_state[call_logs_key] = call_logs
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

        st.session_state[result_key] = merged
        if resume_enabled:
            st.session_state[resume_key] = {
                "signature": resume_signature,
                "next_batch": idx + 1,
                "total_batches": len(chunks),
                "updated_at": time.time(),
            }
            if cache_path is not None:
                payload = _build_cache_file_payload(
                    signature=resume_signature,
                    next_batch=idx + 1,
                    total_batches=len(chunks),
                    merged=merged,
                    call_logs=call_logs,
                )
                _try_write_cache_file(cache_path, payload=payload, debug_terminal_logs=debug_terminal_logs)

        progress.progress(float(idx) / float(len(chunks)), text=f"AI 处理中... {idx}/{len(chunks)}")

    merged["keywords"] = _uniq_str(merged.get("keywords", []))
    merged["negative_keywords"] = _uniq_str(merged.get("negative_keywords", []))

    st.session_state[result_key] = merged
    st.session_state[call_logs_key] = call_logs
    if resume_enabled:
        st.session_state[resume_key] = {
            "signature": resume_signature,
            "next_batch": len(chunks) + 1,
            "total_batches": len(chunks),
            "updated_at": time.time(),
        }
        if cache_path is not None:
            payload = _build_cache_file_payload(
                signature=resume_signature,
                next_batch=len(chunks) + 1,
                total_batches=len(chunks),
                merged=merged,
                call_logs=call_logs,
            )
            _try_write_cache_file(cache_path, payload=payload, debug_terminal_logs=debug_terminal_logs)


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
    resume_enabled = st.checkbox(
        "断点续跑（失败后继续）",
        value=True,
        key=_resume_enabled_widget_key(selected_cluster),
    )

    resume_key = _resume_state_key(selected_cluster)
    result_key = _result_state_key(selected_cluster)
    call_logs_key = _call_logs_state_key(selected_cluster)
    resume_signature = _build_resume_signature(
        cluster_name=cluster_name,
        cluster_desc=cluster_desc,
        topic_keys=topic_keys,
        max_total_topics=int(max_total_topics),
        chunk_size=int(chunk_size),
    )

    cache_path = _cache_file_path(selected_cluster)
    if resume_enabled:
        st.caption(f"缓存文件：{cache_path}")
        # Load cache from file once per session (best effort).
        existing_state = st.session_state.get(resume_key)
        if not isinstance(existing_state, dict):
            file_cache = _try_load_cache_file(cache_path)
            if isinstance(file_cache, dict):
                if str(file_cache.get("signature") or "") == resume_signature:
                    merged_from_file = file_cache.get("merged")
                    call_logs_from_file = file_cache.get("call_logs")
                    if isinstance(merged_from_file, dict):
                        st.session_state[result_key] = merged_from_file
                    if isinstance(call_logs_from_file, list):
                        st.session_state[call_logs_key] = call_logs_from_file
                    st.session_state[resume_key] = {
                        "signature": resume_signature,
                        "next_batch": int(file_cache.get("next_batch") or 1),
                        "total_batches": int(file_cache.get("total_batches") or 0),
                        "updated_at": file_cache.get("updated_at", None),
                    }
                else:
                    st.caption("发现旧缓存文件（参数变了）。要么重跑，要么先点“清空缓存”。")

    cached_state = st.session_state.get(resume_key)
    cached_ok = isinstance(cached_state, dict) and str(cached_state.get("signature") or "") == resume_signature
    cached_next_batch = 1
    if cached_ok:
        try:
            cached_next_batch = int(cached_state.get("next_batch") or 1)
        except Exception:
            cached_next_batch = 1
        done_batches = max(0, min(call_count, cached_next_batch - 1))
        st.caption(f"缓存进度：{done_batches}/{call_count} 批")
    elif isinstance(cached_state, dict) and cached_state:
        st.caption("发现旧缓存（参数变了）。继续跑会从头跑；也可以先点“清空缓存”。")

    if st.button(
        "清空缓存",
        type="secondary",
        key=f"cluster_ai_clear_cache:{selected_cluster}",
    ):
        for k in [resume_key, result_key, call_logs_key]:
            st.session_state.pop(k, None)
        try:
            if cache_path.exists():
                cache_path.unlink()
        except Exception:
            pass
        st.rerun()

    if not resume_enabled and cached_ok and 1 <= cached_next_batch <= call_count:
        st.caption("你关了断点续跑：会从头跑。")

    if resume_enabled and cached_ok and 1 <= cached_next_batch <= call_count:
        run_label = f"继续跑 AI（从 {cached_next_batch}/{call_count} 批）"
    elif cached_ok and cached_next_batch > call_count and call_count > 0:
        run_label = f"重新跑 AI（会覆盖上次结果，共 {call_count} 批）"
    else:
        run_label = f"让 AI 分批筛 topic_key（共 {call_count} 次调用）"

    if st.button(
        run_label,
        type="primary",
        disabled=not bool(topic_keys),
    ):
        start_batch_idx = 1
        merged_seed: dict | None = None
        call_logs_seed: list[dict] | None = None
        if resume_enabled and cached_ok and 1 <= cached_next_batch <= call_count:
            start_batch_idx = cached_next_batch
            merged_seed_raw = st.session_state.get(result_key)
            merged_seed = merged_seed_raw if isinstance(merged_seed_raw, dict) else None

            call_logs_seed_raw = st.session_state.get(call_logs_key)
            if isinstance(call_logs_seed_raw, list):
                call_logs_seed = call_logs_seed_raw
                if call_logs_seed:
                    last = call_logs_seed[-1]
                    if (
                        str(last.get("error") or "").strip()
                        and int(last.get("batch") or 0) == int(start_batch_idx)
                    ):
                        call_logs_seed = call_logs_seed[:-1]
        else:
            for k in [resume_key, result_key, call_logs_key]:
                st.session_state.pop(k, None)

        _run_ai_batches(
            cluster_name=cluster_name,
            cluster_desc=cluster_desc,
            candidate_records=candidate_records,
            chunk_size=int(chunk_size),
            selected_cluster=selected_cluster,
            debug_terminal_logs=bool(debug_terminal_logs),
            resume_enabled=bool(resume_enabled),
            resume_signature=resume_signature,
            start_batch_idx=int(start_batch_idx),
            merged_seed=merged_seed,
            call_logs_seed=call_logs_seed,
        )

    result = st.session_state.get(result_key, None)
    if not isinstance(result, dict):
        st.caption("提示：点上面的按钮，AI 才会给结果。")
        return

    call_logs = st.session_state.get(call_logs_key, None)
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
