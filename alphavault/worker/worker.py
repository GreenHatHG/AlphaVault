from __future__ import annotations

import argparse
import json
import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

from sqlalchemy.engine import Engine

from alphavault.constants import (
    ENV_AI_API_KEY,
    ENV_AI_STREAM,
    ENV_AI_TRACE_OUT,
    ENV_RSS_ACTIVE_HOURS,
    ENV_RSS_INTERVAL_SECONDS,
)
from alphavault.ai.analyze import (
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_MODEL,
    DEFAULT_PROMPT_VERSION,
    AnalyzeResult,
    _call_ai_with_litellm,
    analyze_with_litellm,
    format_llm_error_one_line,
    normalize_action,
    validate_and_adjust_assertions,
)
from alphavault.ai._client import AiInvalidJsonError
from alphavault.ai.tag_validate import validate_topic_prompt_v3_ai_result
from alphavault.ai.topic_prompt_v3 import TOPIC_PROMPT_VERSION, build_topic_prompt
from alphavault.db.turso_db import (
    get_turso_engine_from_env,
    is_turso_libsql_panic_error,
    is_turso_stream_not_found_error,
)
from alphavault.db.turso_queue import (
    ensure_cloud_queue_schema,
    load_cloud_post,
    load_recent_posts_by_author,
    mark_ai_error,
    recover_done_without_processed_at,
    recover_stuck_ai_tasks,
    select_due_post_uids,
    try_mark_ai_running,
    write_assertions_and_mark_done,
)
from alphavault.rss.utils import (
    CST,
    RateLimiter,
    build_analysis_context,
    build_row_meta,
    env_bool,
    env_float,
    in_active_hours,
    now_str,
    parse_active_hours,
    sleep_until_active,
)
from alphavault.worker.cli import (
    _parse_worker_active_hours_from_args,
    _resolve_rss_urls,
    _resolve_worker_interval_seconds,
    _resolve_worker_threads,
    parse_args,
)
from alphavault.worker.ingest import ingest_rss_many_once
from alphavault.worker.redis_queue import flush_redis_to_turso, try_get_redis
from alphavault.worker.spool import ensure_spool_dir, flush_spool_to_turso

from alphavault.weibo.topic_prompt_tree import (
    MAX_THREAD_POSTS,
    MAX_TOPIC_PROMPT_CHARS,
    build_topic_runtime_context,
    thread_root_info_for_post,
)

TURSO_READY_RETRY_SECONDS = 5.0
_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)


@dataclass
class LLMConfig:
    api_key: str
    model: str
    prompt_version: str
    relevant_threshold: float
    base_url: str
    api_mode: str
    ai_stream: bool
    ai_retries: int
    ai_temperature: float
    ai_reasoning_effort: str
    ai_rpm: float
    ai_timeout_seconds: float
    trace_out: Optional[Path]
    verbose: bool


def _build_config(args: argparse.Namespace) -> LLMConfig:
    ai_stream_env = env_bool(ENV_AI_STREAM)
    ai_stream = True
    if ai_stream_env is not None:
        ai_stream = bool(ai_stream_env)
    elif args.ai_stream:
        ai_stream = True

    trace_out = args.trace_out
    trace_out_env = os.getenv(ENV_AI_TRACE_OUT, "").strip()
    if trace_out_env and trace_out is None:
        trace_out = Path(trace_out_env)

    base_url = str(args.base_url or "").strip()
    if bool(args.verbose) and base_url:
        if not base_url.rstrip("/").endswith("/v1"):
            print(
                f"[ai] warn base_url_maybe_missing_v1 base_url={base_url}", flush=True
            )

    api_key = ""
    if args.api_key:
        api_key = str(args.api_key).strip()
    else:
        api_key = os.getenv(ENV_AI_API_KEY, "").strip()
    if not api_key:
        raise RuntimeError(f"Missing {ENV_AI_API_KEY}. Set {ENV_AI_API_KEY}.")

    return LLMConfig(
        api_key=api_key,
        model=str(args.model or DEFAULT_MODEL),
        prompt_version=str(args.prompt_version or DEFAULT_PROMPT_VERSION),
        relevant_threshold=max(0.0, min(1.0, float(args.relevant_threshold))),
        base_url=str(base_url or ""),
        api_mode=str(args.api_mode or DEFAULT_AI_MODE),
        ai_stream=ai_stream,
        ai_retries=max(0, int(args.ai_retries)),
        ai_temperature=float(args.ai_temperature),
        ai_reasoning_effort=str(
            args.ai_reasoning_effort or DEFAULT_AI_REASONING_EFFORT
        ),
        ai_rpm=max(0.0, float(args.ai_rpm or 0.0)),
        ai_timeout_seconds=max(1.0, float(args.ai_timeout_sec)),
        trace_out=trace_out,
        verbose=bool(args.verbose),
    )


def _backoff_seconds(retry_count: int) -> int:
    n = max(1, int(retry_count))
    delay = 30 * (2 ** max(0, n - 1))
    return int(min(3600, delay))


def _to_one_line_tail(value: str, *, max_chars: int) -> str:
    s = str(value or "")
    s = " ".join(s.split())
    if max_chars <= 0 or len(s) <= max_chars:
        return s
    return s[-max_chars:]


def _max_message_tree_text_len(node: object) -> int:
    if not isinstance(node, dict):
        return 0
    max_len = len(str(node.get("text") or ""))
    children = node.get("children")
    if isinstance(children, list):
        for child in children:
            max_len = max(max_len, _max_message_tree_text_len(child))
    return max_len


def _build_topic_prompt_v3_with_prompt_chars_limit(
    *,
    root_key: str,
    root_segment: str,
    root_content_key: str,
    focus_username: str,
    posts: list[dict[str, object]],
    max_prompt_chars: int,
) -> tuple[dict[str, object], int, str, int, int, bool, bool]:
    """
    Build a topic-prompt-v3 prompt with a hard prompt chars budget.

    Returns:
      (runtime_context, truncated_nodes, prompt, prompt_chars, node_chars_limit, compact_json, include_comments)
    """

    def build_ctx(
        *, node_chars: int, include_comments: bool
    ) -> tuple[dict[str, object], int]:
        return build_topic_runtime_context(
            root_key=root_key,
            root_segment=root_segment,
            root_content_key=root_content_key,
            focus_username=focus_username,
            posts=posts,
            include_virtual_comments=bool(include_comments),
            max_node_text_chars=int(node_chars),
        )

    def build_prompt(ctx: dict[str, object], *, compact_json: bool) -> tuple[str, int]:
        pkg = ctx.get("ai_topic_package")
        if not isinstance(pkg, dict):
            raise RuntimeError("ai_topic_package_invalid")
        p = build_topic_prompt(ai_topic_package=pkg, compact_json=bool(compact_json))
        return p, len(p)

    def search_best_cap(
        *, include_comments: bool
    ) -> Optional[tuple[dict[str, object], int, str, int, int]]:
        base_ctx, _base_truncated = build_ctx(
            node_chars=0, include_comments=include_comments
        )
        max_len = max(1, _max_message_tree_text_len(base_ctx.get("message_tree")))
        lo = 1
        hi = int(max_len)
        best: Optional[tuple[dict[str, object], int, str, int, int]] = None
        while lo <= hi:
            mid = (lo + hi) // 2
            mid_ctx, mid_truncated = build_ctx(
                node_chars=mid, include_comments=include_comments
            )
            mid_prompt, mid_chars = build_prompt(mid_ctx, compact_json=True)
            if mid_chars <= max_prompt_chars:
                best = (
                    mid_ctx,
                    int(mid_truncated),
                    mid_prompt,
                    int(mid_chars),
                    int(mid),
                )
                lo = mid + 1
                continue
            hi = mid - 1
        return best

    # 1) Full context + pretty JSON (readable)
    ctx_full, truncated_full = build_ctx(node_chars=0, include_comments=True)
    pretty_prompt, pretty_chars = build_prompt(ctx_full, compact_json=False)
    if max_prompt_chars <= 0 or pretty_chars <= max_prompt_chars:
        return (
            ctx_full,
            int(truncated_full),
            pretty_prompt,
            int(pretty_chars),
            0,
            False,
            True,
        )

    # 2) Full context + compact JSON (save chars on whitespace)
    compact_prompt, compact_chars = build_prompt(ctx_full, compact_json=True)
    if compact_chars <= max_prompt_chars:
        return (
            ctx_full,
            int(truncated_full),
            compact_prompt,
            int(compact_chars),
            0,
            True,
            True,
        )

    # 3) Full context + compact JSON + per-node cap
    best = search_best_cap(include_comments=True)
    if best is not None:
        best_ctx, best_truncated, best_prompt, best_prompt_chars, best_cap = best
        return (
            best_ctx,
            best_truncated,
            best_prompt,
            best_prompt_chars,
            best_cap,
            True,
            True,
        )

    # 4) Fallback: remove virtual comment nodes (keep only status nodes)
    ctx_no_comments, truncated_nc = build_ctx(node_chars=0, include_comments=False)
    nc_pretty_prompt, nc_pretty_chars = build_prompt(
        ctx_no_comments, compact_json=False
    )
    if nc_pretty_chars <= max_prompt_chars:
        return (
            ctx_no_comments,
            int(truncated_nc),
            nc_pretty_prompt,
            int(nc_pretty_chars),
            0,
            False,
            False,
        )

    nc_compact_prompt, nc_compact_chars = build_prompt(
        ctx_no_comments, compact_json=True
    )
    if nc_compact_chars <= max_prompt_chars:
        return (
            ctx_no_comments,
            int(truncated_nc),
            nc_compact_prompt,
            int(nc_compact_chars),
            0,
            True,
            False,
        )

    best_nc = search_best_cap(include_comments=False)
    if best_nc is not None:
        best_ctx, best_truncated, best_prompt, best_prompt_chars, best_cap = best_nc
        return (
            best_ctx,
            best_truncated,
            best_prompt,
            best_prompt_chars,
            best_cap,
            True,
            False,
        )

    raise RuntimeError(f"topic_prompt_too_long max_prompt_chars={max_prompt_chars}")


def _clamp_int(value: object, low: int, high: int, default: int) -> int:
    try:
        v = int(str(value).strip())
    except Exception:
        return int(default)
    return int(max(low, min(high, v)))


def _clamp_float(value: object, low: float, high: float, default: float) -> float:
    try:
        v = float(str(value).strip())
    except Exception:
        return float(default)
    return float(max(low, min(high, v)))


def _as_str_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    return []


def _map_topic_prompt_items_to_assertions(
    *,
    ai_result: dict[str, object],
    focus_username: str,
    message_lookup: dict[tuple[str, str], dict[str, object]],
    post_uid_by_platform_post_id: dict[str, str],
    max_assertions_per_post: int = 5,
) -> dict[str, list[dict[str, object]]]:
    """
    Convert topic-prompt-v3 items -> per-post assertions rows (AlphaVault schema).

    We only accept items that:
    - speaker == focus_username
    - have evidence_refs pointing to a known post platform_post_id (leaf "status" nodes)
    """
    focus = str(focus_username or "").strip()
    items = ai_result.get("items")
    if not isinstance(items, list):
        raise RuntimeError("ai_topic_items_missing")

    out: dict[str, list[dict[str, object]]] = {}
    for raw_item in items:
        if not isinstance(raw_item, dict):
            continue

        speaker = str(raw_item.get("speaker") or "").strip()
        if focus and speaker != focus:
            continue

        topic_key = str(raw_item.get("topic_key") or "").strip()
        if not topic_key:
            continue

        evidence_refs = raw_item.get("evidence_refs")
        refs = evidence_refs if isinstance(evidence_refs, list) else []
        first_ref = refs[0] if refs and isinstance(refs[0], dict) else {}
        source_kind = str(first_ref.get("source_kind") or "").strip()
        source_id = str(first_ref.get("source_id") or "").strip()
        quote = str(first_ref.get("quote") or "").strip()
        if not source_id:
            continue

        post_uid = post_uid_by_platform_post_id.get(source_id)
        if not post_uid:
            continue

        lookup_key = (source_kind, source_id)
        node = message_lookup.get(lookup_key)
        if node is None and source_id:
            node = message_lookup.get(("status", source_id))
        node_text = str((node or {}).get("text") or "")

        evidence = (
            quote
            if quote and node_text and quote in node_text
            else (node_text[:120] if node_text else quote)
        )
        if not evidence:
            continue

        summary = str(raw_item.get("summary") or "").strip() or "未提供摘要"
        confidence = _clamp_float(raw_item.get("confidence"), 0.0, 1.0, 0.5)
        action_strength = _clamp_int(raw_item.get("action_strength"), 0, 3, 1)
        action = normalize_action(
            str(raw_item.get("action") or "").strip() or "trade.watch"
        )

        row = {
            "topic_key": topic_key,
            "action": action,
            "action_strength": action_strength,
            "summary": summary,
            "evidence": evidence,
            "confidence": confidence,
            "stock_codes_json": json.dumps(
                _as_str_list(raw_item.get("stock_codes")), ensure_ascii=False
            ),
            "stock_names_json": json.dumps(
                _as_str_list(raw_item.get("stock_names")), ensure_ascii=False
            ),
            "industries_json": json.dumps(
                _as_str_list(raw_item.get("industries")), ensure_ascii=False
            ),
            "commodities_json": json.dumps(
                _as_str_list(raw_item.get("commodities")), ensure_ascii=False
            ),
            "indices_json": json.dumps(
                _as_str_list(raw_item.get("indices")), ensure_ascii=False
            ),
        }
        bucket = out.setdefault(post_uid, [])
        if len(bucket) < max(0, int(max_assertions_per_post)):
            bucket.append(row)

    return out


def _process_one_post_uid_topic_prompt_v3(
    *,
    engine: Engine,
    post_uid: str,
    config: LLMConfig,
    limiter: RateLimiter,
) -> None:
    post = load_cloud_post(engine, post_uid)
    focus = str(post.author or "").strip()
    root_key, root_segment, root_content_key = thread_root_info_for_post(
        raw_text=post.raw_text or "",
        display_md=post.display_md or "",
        author=focus,
    )

    # Scan recent posts from the same author, then keep only the same "root_key" thread.
    recent = load_recent_posts_by_author(engine, author=focus, limit=200)
    current_row = {
        "post_uid": post.post_uid,
        "platform_post_id": post.platform_post_id,
        "author": post.author,
        "created_at": post.created_at,
        "url": post.url,
        "raw_text": post.raw_text,
        "display_md": post.display_md,
        "processed_at": "",
        "ai_status": "running",
        "ai_retry_count": int(post.ai_retry_count or 0),
    }

    thread_rows: list[dict[str, object]] = []
    seen_uids: set[str] = set()
    for row in [current_row, *recent]:
        uid = str(row.get("post_uid") or "").strip()
        if not uid or uid in seen_uids:
            continue
        seen_uids.add(uid)

        is_current = uid == str(post.post_uid or "").strip()
        ai_status = str(row.get("ai_status") or "").strip().lower()
        if not is_current and ai_status not in {"pending", "error"}:
            continue

        rk, _seg, _ck = thread_root_info_for_post(
            raw_text=str(row.get("raw_text") or ""),
            display_md=str(row.get("display_md") or ""),
            author=str(row.get("author") or "").strip(),
        )
        if rk != root_key:
            continue
        thread_rows.append(row)

    post_count = len(thread_rows)
    thread_rows.sort(key=lambda r: str(r.get("created_at") or ""), reverse=True)
    kept = (
        thread_rows[:MAX_THREAD_POSTS] if post_count > MAX_THREAD_POSTS else thread_rows
    )
    if str(post.post_uid or "").strip() not in {
        str(r.get("post_uid") or "").strip() for r in kept
    }:
        kept = [current_row, *kept[: max(0, MAX_THREAD_POSTS - 1)]]
    trimmed_count = max(0, post_count - len(kept))

    # Lock additional posts in this thread (best-effort), so we can write results back once.
    locked_post_uids: list[str] = [post.post_uid]
    locked_set: set[str] = {post.post_uid}
    now_epoch = int(time.time())
    for row in kept:
        uid = str(row.get("post_uid") or "").strip()
        if not uid or uid in locked_set:
            continue
        ai_status = str(row.get("ai_status") or "").strip().lower()
        if ai_status not in {"pending", "error"}:
            continue
        try:
            if try_mark_ai_running(engine, post_uid=uid, now_epoch=now_epoch):
                locked_post_uids.append(uid)
                locked_set.add(uid)
        except Exception:
            continue

    (
        runtime_context,
        truncated_nodes,
        prompt,
        prompt_chars,
        node_chars,
        compact_json,
        include_comments,
    ) = _build_topic_prompt_v3_with_prompt_chars_limit(
        root_key=root_key,
        root_segment=root_segment,
        root_content_key=root_content_key,
        focus_username=focus,
        posts=kept,
        max_prompt_chars=MAX_TOPIC_PROMPT_CHARS,
    )
    if (
        trimmed_count > 0
        or truncated_nodes > 0
        or compact_json
        or (not include_comments)
    ):
        print(
            " ".join(
                [
                    "[ai_topic] tree_trim",
                    f"author={focus or '(empty)'}",
                    f"root_key={root_key}",
                    f"post_count={post_count}",
                    f"trimmed_count={trimmed_count}",
                    f"max_nodes={MAX_THREAD_POSTS}",
                    f"prompt_chars={prompt_chars}",
                    f"max_prompt_chars={MAX_TOPIC_PROMPT_CHARS}",
                    f"compact_json={1 if compact_json else 0}",
                    f"comments={1 if include_comments else 0}",
                    f"node_chars={node_chars}",
                    f"truncated_nodes={truncated_nodes}",
                ]
            ),
            flush=True,
        )

    trace_label = f"topic:{root_key}"

    if config.verbose:
        print(
            f"[llm] call_api topic_prompt_v3 root_key={root_key} locked={len(locked_post_uids)}",
            flush=True,
        )

    retry_count_by_uid = {
        str(row.get("post_uid") or "").strip(): _clamp_int(
            row.get("ai_retry_count"),
            1,
            1000,
            1,
        )
        for row in kept
        if str(row.get("post_uid") or "").strip()
    }

    try:
        limiter.wait()
        start_ts = time.time()
        parsed = _call_ai_with_litellm(
            prompt=prompt,
            api_mode=str(config.api_mode or DEFAULT_AI_MODE),
            ai_stream=bool(config.ai_stream),
            model_name=str(config.model or DEFAULT_MODEL),
            base_url=str(config.base_url or ""),
            api_key=str(config.api_key or ""),
            timeout_seconds=float(config.ai_timeout_seconds),
            retry_count=int(config.ai_retries),
            temperature=float(config.ai_temperature),
            reasoning_effort=str(
                config.ai_reasoning_effort or DEFAULT_AI_REASONING_EFFORT
            ),
            trace_out=config.trace_out,
            trace_label=trace_label,
            validator=validate_topic_prompt_v3_ai_result,
        )

        if config.verbose:
            cost = time.time() - start_ts
            print(
                f"[llm] done topic_prompt_v3 root_key={root_key} cost={cost:.1f}s",
                flush=True,
            )

        if not isinstance(parsed, dict):
            raise RuntimeError("ai_topic_invalid_json_root")

        message_lookup = runtime_context.get("message_lookup")
        if not isinstance(message_lookup, dict):
            raise RuntimeError("ai_topic_message_lookup_invalid")

        post_uid_by_pid = {
            str(row.get("platform_post_id") or "").strip(): str(
                row.get("post_uid") or ""
            ).strip()
            for row in kept
            if str(row.get("post_uid") or "").strip() in locked_set
        }
        assertions_by_post_uid = _map_topic_prompt_items_to_assertions(
            ai_result=parsed,
            focus_username=focus,
            message_lookup=message_lookup,  # type: ignore[arg-type]
            post_uid_by_platform_post_id=post_uid_by_pid,
            max_assertions_per_post=5,
        )

        for uid in locked_post_uids:
            rows = assertions_by_post_uid.get(uid, [])
            is_relevant = bool(rows)
            final_status = "relevant" if is_relevant else "irrelevant"
            invest_score = 1.0 if is_relevant else 0.0
            write_assertions_and_mark_done(
                engine,
                post_uid=uid,
                final_status=final_status,
                invest_score=invest_score,
                processed_at=now_str(),
                model=config.model,
                prompt_version=config.prompt_version,
                archived_at=now_str(),
                ai_result_json=None,
                assertions=rows,
            )
    except Exception as e:
        if isinstance(e, AiInvalidJsonError):
            raw_tail = _to_one_line_tail(getattr(e, "raw_ai_text", ""), max_chars=240)
            print(
                " ".join(
                    [
                        "[ai_topic] invalid_json",
                        f"post_uid={post.post_uid}",
                        f"author={focus or '(empty)'}",
                        f"root_key={root_key}",
                        f"prompt_version={config.prompt_version}",
                        f"raw_ai_len={len(getattr(e, 'raw_ai_text', '') or '')}",
                        f"raw_ai_tail={raw_tail}",
                    ]
                ),
                flush=True,
            )

        base_url_for_log = (config.base_url or "").strip()
        if base_url_for_log:
            base_url_for_log = base_url_for_log.split("?", 1)[0].split("#", 1)[0]
            base_url_for_log = base_url_for_log[:220]
        ctx = (
            f" cfg_model={config.model}"
            f" api_mode={config.api_mode}"
            f" stream={1 if config.ai_stream else 0}"
            f" base_url={base_url_for_log or '(empty)'}"
            f" prompt_version={config.prompt_version}"
        )
        msg = f"ai:{format_llm_error_one_line(e, limit=700)}{ctx}"
        now_epoch = int(time.time())
        for uid in locked_post_uids:
            retry_count = retry_count_by_uid.get(uid, 1)
            next_retry = now_epoch + _backoff_seconds(retry_count)
            try:
                mark_ai_error(
                    engine,
                    post_uid=uid,
                    error=msg,
                    next_retry_at=next_retry,
                    archived_at=now_str(),
                )
            except Exception:
                continue
        print(
            f"[llm] error topic_prompt_v3 root_key={root_key} locked={len(locked_post_uids)} {msg}",
            flush=True,
        )
        return


def _process_one_post_uid(
    *,
    engine: Engine,
    post_uid: str,
    config: LLMConfig,
    limiter: RateLimiter,
) -> None:
    try:
        if str(config.prompt_version or "").strip() == TOPIC_PROMPT_VERSION:
            _process_one_post_uid_topic_prompt_v3(
                engine=engine,
                post_uid=post_uid,
                config=config,
                limiter=limiter,
            )
            return

        post = load_cloud_post(engine, post_uid)
        analysis_context = build_analysis_context(post.raw_text or "")
        row_meta = build_row_meta(
            mid_or_bid=str(post.platform_post_id or ""),
            bid="",
            link=str(post.url or ""),
            title="",
            author=str(post.author or ""),
            created_at=str(post.created_at or ""),
            raw_text=str(post.raw_text or ""),
        )
        if config.verbose:
            print(f"[llm] call_api {post_uid}", flush=True)
        limiter.wait()
        start_ts = time.time()
        result: AnalyzeResult = analyze_with_litellm(
            api_key=config.api_key,
            model=config.model,
            analysis_context=analysis_context,
            row=row_meta,
            base_url=config.base_url,
            api_mode=config.api_mode,
            ai_stream=config.ai_stream,
            ai_retries=config.ai_retries,
            ai_temperature=config.ai_temperature,
            ai_reasoning_effort=config.ai_reasoning_effort,
            trace_out=config.trace_out,
            timeout_seconds=config.ai_timeout_seconds,
        )
        if config.verbose:
            cost = time.time() - start_ts
            print(
                f"[llm] done {post_uid} status={result.status} score={result.invest_score:.3f} cost={cost:.1f}s",
                flush=True,
            )

        final_result = result
        if final_result.invest_score < config.relevant_threshold:
            final_result = AnalyzeResult(
                status="irrelevant",
                invest_score=final_result.invest_score,
                assertions=[],
            )
        else:
            final_result.assertions = validate_and_adjust_assertions(
                final_result.assertions,
                commentary_text=analysis_context["commentary_text"],
                quoted_text=analysis_context["quoted_text"],
            )

        assertions = (
            final_result.assertions if final_result.status == "relevant" else []
        )

        write_assertions_and_mark_done(
            engine,
            post_uid=post_uid,
            final_status=final_result.status,
            invest_score=float(final_result.invest_score),
            processed_at=now_str(),
            model=config.model,
            prompt_version=config.prompt_version,
            archived_at=now_str(),
            ai_result_json=None,
            assertions=assertions,
        )
    except Exception as e:
        base_url_for_log = (config.base_url or "").strip()
        if base_url_for_log:
            base_url_for_log = base_url_for_log.split("?", 1)[0].split("#", 1)[0]
            base_url_for_log = base_url_for_log[:220]
        ctx = (
            f" cfg_model={config.model}"
            f" api_mode={config.api_mode}"
            f" stream={1 if config.ai_stream else 0}"
            f" base_url={base_url_for_log or '(empty)'}"
        )
        msg = f"ai:{format_llm_error_one_line(e, limit=700)}{ctx}"
        now_epoch = int(time.time())
        retry_count = 1
        try:
            loaded = load_cloud_post(engine, post_uid)
            retry_count = int(getattr(loaded, "ai_retry_count", 1) or 1)
        except Exception:
            retry_count = 1
        next_retry = now_epoch + _backoff_seconds(retry_count)
        try:
            mark_ai_error(
                engine,
                post_uid=post_uid,
                error=msg,
                next_retry_at=next_retry,
                archived_at=now_str(),
            )
        except Exception as mark_e:
            if config.verbose:
                print(
                    f"[llm] mark_error_failed {post_uid} {type(mark_e).__name__}: {mark_e}",
                    flush=True,
                )
        print(f"[llm] error {post_uid} {msg}", flush=True)


def _log_spool_and_redis(
    *, verbose: bool, spool_dir: Path, redis_client, redis_queue_key: str
) -> None:
    if not verbose:
        return
    print(f"[spool] dir={spool_dir}", flush=True)
    if redis_client:
        print(f"[redis] enabled key={redis_queue_key}", flush=True)


def _maybe_dispose_turso_engine_on_transient_error(
    *, engine: Engine, err: BaseException, verbose: bool
) -> None:
    reason = ""
    if is_turso_stream_not_found_error(err):
        reason = "stream_not_found"
    elif is_turso_libsql_panic_error(err):
        reason = "libsql_panic"
    else:
        return
    try:
        engine.dispose()
        if verbose:
            print(f"[turso] disposed_engine reason={reason}", flush=True)
    except Exception as dispose_e:
        if verbose:
            print(
                f"[turso] dispose_engine_failed {type(dispose_e).__name__}: {dispose_e}",
                flush=True,
            )


def _ensure_turso_ready(*, engine: Engine, verbose: bool, turso_ready: bool) -> bool:
    if turso_ready:
        return True
    try:
        ensure_cloud_queue_schema(engine, verbose=bool(verbose))
        print("[turso] ready", flush=True)
        return True
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(
            engine=engine, err=e, verbose=bool(verbose)
        )
        if verbose:
            print(f"[turso] not_ready {type(e).__name__}: {e}", flush=True)
        return False


def _seconds_until_next_active_start(
    now_dt: datetime, active_hours: tuple[int, int]
) -> float:
    start_hour, end_hour = active_hours
    today_start = now_dt.replace(hour=start_hour, minute=0, second=0, microsecond=0)

    if start_hour <= end_hour:
        if now_dt.hour < start_hour:
            next_dt = today_start
        else:
            next_dt = today_start + timedelta(days=1)
    else:
        next_dt = today_start

    return max(1.0, (next_dt - now_dt).total_seconds())


def _schedule_ai(
    executor: ThreadPoolExecutor,
    *,
    engine: Optional[Engine],
    worker_threads: int,
    inflight_futures: set[Future],
    wakeup_event: threading.Event,
    config: LLMConfig,
    limiter: RateLimiter,
    verbose: bool,
) -> Tuple[int, bool]:
    if engine is None:
        return 0, False
    # Keep inflight bounded: do not queue more than worker_threads tasks.
    inflight_futures.difference_update({f for f in inflight_futures if f.done()})
    available = max(0, int(worker_threads) - len(inflight_futures))
    if available <= 0:
        return 0, False
    now_epoch = int(time.time())
    try:
        due = select_due_post_uids(
            engine,
            now_epoch=now_epoch,
            limit=max(1, int(available) * 2),
        )
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(
            engine=engine, err=e, verbose=bool(verbose)
        )
        if verbose:
            print(f"[ai] select_due_error {type(e).__name__}: {e}", flush=True)
        return 0, True

    scheduled = 0
    for post_uid in due:
        if scheduled >= available:
            break
        try:
            ok = try_mark_ai_running(engine, post_uid=post_uid, now_epoch=now_epoch)
        except BaseException as e:
            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                raise
            _maybe_dispose_turso_engine_on_transient_error(
                engine=engine, err=e, verbose=bool(verbose)
            )
            if verbose:
                print(f"[ai] mark_running_error {type(e).__name__}: {e}", flush=True)
            return scheduled, True
        if not ok:
            continue
        fut = executor.submit(
            _process_one_post_uid,
            engine=engine,
            post_uid=post_uid,
            config=config,
            limiter=limiter,
        )
        fut.add_done_callback(lambda _f: wakeup_event.set())
        inflight_futures.add(fut)
        scheduled += 1
    return scheduled, False


def _run_turso_maintenance(
    *,
    engine: Optional[Engine],
    spool_dir: Path,
    redis_client,
    redis_queue_key: str,
    stuck_seconds: int,
    verbose: bool,
) -> Tuple[int, int, int, bool]:
    if engine is None:
        return 0, 0, 0, False

    turso_error = False
    recovered = 0
    try:
        recovered = recover_stuck_ai_tasks(
            engine,
            now_epoch=int(time.time()),
            stuck_seconds=max(60, int(stuck_seconds)),
            verbose=bool(verbose),
        )
        recovered += recover_done_without_processed_at(engine, verbose=bool(verbose))
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(
            engine=engine, err=e, verbose=bool(verbose)
        )
        turso_error = True
        if verbose:
            print(f"[ai] recover_error {type(e).__name__}: {e}", flush=True)

    flushed_redis = 0
    flush_redis_error = False
    try:
        flushed_redis, flush_redis_error = flush_redis_to_turso(
            client=redis_client,
            queue_key=redis_queue_key,
            spool_dir=spool_dir,
            engine=engine,
            max_items=200,
            verbose=bool(verbose),
        )
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(
            engine=engine, err=e, verbose=bool(verbose)
        )
        flush_redis_error = True
        if verbose:
            print(f"[redis] flush_error {type(e).__name__}: {e}", flush=True)

    flushed_spool = 0
    flush_spool_error = False
    try:
        flushed_spool, flush_spool_error = flush_spool_to_turso(
            spool_dir=spool_dir,
            engine=engine,
            max_items=200,
            verbose=bool(verbose),
        )
    except BaseException as e:
        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
            raise
        _maybe_dispose_turso_engine_on_transient_error(
            engine=engine, err=e, verbose=bool(verbose)
        )
        flush_spool_error = True
        if verbose:
            print(f"[spool] flush_error {type(e).__name__}: {e}", flush=True)

    turso_error = bool(turso_error or flush_redis_error or flush_spool_error)
    return recovered, flushed_redis, flushed_spool, turso_error


def main() -> None:
    args = parse_args()
    rss_urls = _resolve_rss_urls(args)
    worker_active_hours = _parse_worker_active_hours_from_args(args)
    worker_interval = _resolve_worker_interval_seconds(args)
    config = _build_config(args)
    limiter = RateLimiter(config.ai_rpm)
    worker_threads = _resolve_worker_threads(args)
    engine = get_turso_engine_from_env()
    spool_dir = ensure_spool_dir()
    redis_client, redis_queue_key = try_get_redis()
    _log_spool_and_redis(
        verbose=bool(args.verbose),
        spool_dir=spool_dir,
        redis_client=redis_client,
        redis_queue_key=redis_queue_key,
    )
    limit = args.limit if args.limit and args.limit > 0 else None

    rss_active_hours: Optional[tuple[int, int]] = None
    rss_active_hours_value = os.getenv(ENV_RSS_ACTIVE_HOURS, "").strip()
    if rss_active_hours_value:
        rss_active_hours = parse_active_hours(rss_active_hours_value)

    rss_interval_seconds = env_float(ENV_RSS_INTERVAL_SECONDS)
    if rss_interval_seconds is None or rss_interval_seconds <= 0:
        rss_interval_seconds = 600.0
    rss_interval_seconds = max(1.0, float(rss_interval_seconds))
    rss_next_ingest_at = 0.0 if rss_urls else float("inf")
    maintenance_next_at = 0.0

    with ThreadPoolExecutor(max_workers=worker_threads) as executor:
        wakeup_event = threading.Event()
        inflight_futures: set[Future] = set()
        turso_ready = False
        turso_next_ready_check_at = 0.0
        while True:
            verbose = bool(args.verbose)
            if worker_active_hours is not None:
                sleep_until_active(worker_active_hours, verbose=verbose)

            wakeup_event.clear()
            inflight_futures.difference_update(
                {f for f in inflight_futures if f.done()}
            )

            now = time.time()
            do_maintenance = bool(now >= maintenance_next_at)
            if do_maintenance:
                maintenance_next_at = now + float(worker_interval)

            do_ingest_rss = False
            rss_skip_reason = ""
            if not rss_urls:
                rss_skip_reason = "no_sources"
            else:
                now_dt = datetime.now(CST)
                if rss_active_hours is not None and not in_active_hours(
                    now_dt, rss_active_hours
                ):
                    rss_skip_reason = "inactive"
                    rss_next_ingest_at = now + _seconds_until_next_active_start(
                        now_dt, rss_active_hours
                    )
                elif now < rss_next_ingest_at:
                    rss_skip_reason = "interval"
                else:
                    do_ingest_rss = True
                    rss_next_ingest_at = now + float(rss_interval_seconds)

            force_maintenance = False
            if not turso_ready and now >= float(turso_next_ready_check_at):
                turso_ready = _ensure_turso_ready(
                    engine=engine, verbose=verbose, turso_ready=turso_ready
                )
                if turso_ready:
                    turso_next_ready_check_at = 0.0
                    # Turso just recovered: run maintenance ASAP to flush spool/redis quickly.
                    force_maintenance = True
                else:
                    turso_next_ready_check_at = now + float(TURSO_READY_RETRY_SECONDS)
            active_engine: Optional[Engine] = engine if turso_ready else None

            inserted = 0
            ingest_turso_error = False
            if do_ingest_rss and rss_urls:
                try:
                    inserted, ingest_turso_error = ingest_rss_many_once(
                        rss_urls=rss_urls,
                        engine=active_engine,
                        spool_dir=spool_dir,
                        redis_client=redis_client,
                        redis_queue_key=redis_queue_key,
                        author=args.author.strip(),
                        user_id=(args.user_id.strip() or None),
                        limit=limit,
                        rss_timeout=float(args.rss_timeout),
                        verbose=verbose,
                    )
                except BaseException as e:
                    if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                        raise
                    _maybe_dispose_turso_engine_on_transient_error(
                        engine=engine, err=e, verbose=bool(verbose)
                    )
                    ingest_turso_error = True
                    inserted = 0
                    if verbose:
                        print(f"[rss] ingest_error {type(e).__name__}: {e}", flush=True)

            recovered = 0
            flushed_redis = 0
            flushed_spool = 0
            maintenance_error = False
            if (do_maintenance or force_maintenance) and active_engine is not None:
                if force_maintenance:
                    maintenance_next_at = now + float(worker_interval)
                recovered, flushed_redis, flushed_spool, maintenance_error = (
                    _run_turso_maintenance(
                        engine=active_engine,
                        spool_dir=spool_dir,
                        redis_client=redis_client,
                        redis_queue_key=redis_queue_key,
                        stuck_seconds=int(args.ai_stuck_seconds),
                        verbose=verbose,
                    )
                )

            scheduled = 0
            schedule_error = False
            if active_engine is not None:
                scheduled, schedule_error = _schedule_ai(
                    executor,
                    engine=active_engine,
                    worker_threads=worker_threads,
                    inflight_futures=inflight_futures,
                    wakeup_event=wakeup_event,
                    config=config,
                    limiter=limiter,
                    verbose=verbose,
                )

            turso_error = bool(
                ingest_turso_error or maintenance_error or schedule_error
            )
            if turso_error:
                turso_ready = False
                turso_next_ready_check_at = min(
                    float(turso_next_ready_check_at) or float("inf"),
                    time.time() + float(TURSO_READY_RETRY_SECONDS),
                )

            if verbose and (
                do_maintenance
                or inserted > 0
                or recovered > 0
                or flushed_redis > 0
                or flushed_spool > 0
            ):
                next_maintenance_in = max(0.0, maintenance_next_at - time.time())
                next_rss_in = (
                    -1.0
                    if rss_next_ingest_at == float("inf")
                    else max(0.0, rss_next_ingest_at - time.time())
                )
                next_turso_in = -1.0
                if not turso_ready:
                    next_turso_in = max(
                        0.0, float(turso_next_ready_check_at) - time.time()
                    )
                print(
                    f"[tick] turso_ready={1 if active_engine is not None else 0} "
                    f"inflight={len(inflight_futures)} ai_scheduled={scheduled} "
                    f"ai_recovered={recovered} redis_flush={flushed_redis} spool_flush={flushed_spool} "
                    f"rss_inserted={inserted} rss_skip={rss_skip_reason or '-'} "
                    f"next_maint={int(next_maintenance_in)}s "
                    f"next_rss={int(next_rss_in) if next_rss_in >= 0 else -1}s "
                    f"next_turso={int(next_turso_in) if next_turso_in >= 0 else -1}s",
                    flush=True,
                )

            next_deadline = maintenance_next_at
            if rss_next_ingest_at != float("inf"):
                next_deadline = min(next_deadline, rss_next_ingest_at)
            if not turso_ready:
                next_deadline = min(next_deadline, float(turso_next_ready_check_at))
            timeout = max(0.0, next_deadline - time.time())
            if inflight_futures:
                wakeup_event.wait(timeout)
            else:
                time.sleep(timeout)


__all__ = ["main", "parse_args", "LLMConfig"]
