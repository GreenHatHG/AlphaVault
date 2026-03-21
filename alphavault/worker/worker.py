from __future__ import annotations

import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from sqlalchemy.engine import Engine

from alphavault.constants import (
    ENV_AI_API_KEY,
    ENV_AI_API_MODE,
    ENV_AI_BASE_URL,
    ENV_AI_MAX_INFLIGHT,
    ENV_AI_MODEL,
    ENV_AI_PROMPT_VERSION,
    ENV_AI_REASONING_EFFORT,
    ENV_AI_STREAM,
    ENV_AI_TEMPERATURE,
    ENV_AI_TRACE_OUT,
)
from alphavault.ai.analyze import (
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_MODEL,
    DEFAULT_PROMPT_VERSION,
    AnalyzeResult,
    analyze_with_litellm,
    format_llm_error_one_line,
    validate_and_adjust_assertions,
)
from alphavault.db.turso_db import get_turso_engine_from_env
from alphavault.db.turso_queue import (
    ensure_cloud_queue_schema,
    load_cloud_post,
    mark_ai_error,
    recover_done_without_processed_at,
    recover_stuck_ai_tasks,
    select_due_post_uids,
    try_mark_ai_running,
    write_assertions_and_mark_done,
)
from alphavault.rss.utils import (
    RateLimiter,
    build_analysis_context,
    build_row_meta,
    env_bool,
    now_str,
    sleep_until_active,
)
from alphavault.worker.cli import (
    _parse_active_hours_from_args,
    _require_rss_urls,
    _resolve_interval_seconds,
    _resolve_worker_threads,
    parse_args,
)
from alphavault.worker.ingest import ingest_rss_many_once
from alphavault.worker.redis_queue import flush_redis_to_turso, try_get_redis
from alphavault.worker.spool import ensure_spool_dir, flush_spool_to_turso


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
            print(f"[ai] warn base_url_maybe_missing_v1 base_url={base_url}", flush=True)

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
        ai_reasoning_effort=str(args.ai_reasoning_effort or DEFAULT_AI_REASONING_EFFORT),
        ai_rpm=max(0.0, float(args.ai_rpm or 0.0)),
        ai_timeout_seconds=max(1.0, float(args.ai_timeout_sec)),
        trace_out=trace_out,
        verbose=bool(args.verbose),
    )


def _backoff_seconds(retry_count: int) -> int:
    n = max(1, int(retry_count))
    delay = 30 * (2 ** max(0, n - 1))
    return int(min(3600, delay))


def _process_one_post_uid(
    *,
    engine: Engine,
    post_uid: str,
    config: LLMConfig,
    limiter: RateLimiter,
) -> None:
    try:
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
            final_result = AnalyzeResult(status="irrelevant", invest_score=final_result.invest_score, assertions=[])
        else:
            final_result.assertions = validate_and_adjust_assertions(
                final_result.assertions,
                commentary_text=analysis_context["commentary_text"],
                quoted_text=analysis_context["quoted_text"],
            )

        assertions = final_result.assertions if final_result.status == "relevant" else []

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
            retry_count = int(post.ai_retry_count or 1)  # type: ignore[name-defined]
        except Exception:
            retry_count = 1
        next_retry = now_epoch + _backoff_seconds(retry_count)
        try:
            mark_ai_error(engine, post_uid=post_uid, error=msg, next_retry_at=next_retry, archived_at=now_str())
        except Exception as mark_e:
            if config.verbose:
                print(f"[llm] mark_error_failed {post_uid} {type(mark_e).__name__}: {mark_e}", flush=True)
        print(f"[llm] error {post_uid} {msg}", flush=True)


def _log_spool_and_redis(*, verbose: bool, spool_dir: Path, redis_client, redis_queue_key: str) -> None:
    if not verbose:
        return
    print(f"[spool] dir={spool_dir}", flush=True)
    if redis_client:
        print(f"[redis] enabled key={redis_queue_key}", flush=True)


def _ensure_turso_ready(*, engine: Engine, verbose: bool, turso_ready: bool) -> bool:
    if turso_ready:
        return True
    try:
        ensure_cloud_queue_schema(engine, verbose=bool(verbose))
        print("[turso] ready", flush=True)
        return True
    except Exception as e:
        if verbose:
            print(f"[turso] not_ready {type(e).__name__}: {e}", flush=True)
        return False


def _schedule_ai(
    executor: ThreadPoolExecutor,
    *,
    engine: Optional[Engine],
    worker_threads: int,
    config: LLMConfig,
    limiter: RateLimiter,
    verbose: bool,
) -> Tuple[int, bool]:
    if engine is None:
        return 0, False
    now_epoch = int(time.time())
    try:
        due = select_due_post_uids(
            engine,
            now_epoch=now_epoch,
            limit=max(1, int(worker_threads) * 2),
        )
    except Exception as e:
        if verbose:
            print(f"[ai] select_due_error {type(e).__name__}: {e}", flush=True)
        return 0, True

    scheduled = 0
    for post_uid in due:
        try:
            ok = try_mark_ai_running(engine, post_uid=post_uid, now_epoch=now_epoch)
        except Exception as e:
            if verbose:
                print(f"[ai] mark_running_error {type(e).__name__}: {e}", flush=True)
            return scheduled, True
        if not ok:
            continue
        executor.submit(
            _process_one_post_uid,
            engine=engine,
            post_uid=post_uid,
            config=config,
            limiter=limiter,
        )
        scheduled += 1
    return scheduled, False


def _run_turso_tick(
    executor: ThreadPoolExecutor,
    *,
    engine: Optional[Engine],
    spool_dir: Path,
    redis_client,
    redis_queue_key: str,
    worker_threads: int,
    config: LLMConfig,
    limiter: RateLimiter,
    stuck_seconds: int,
    verbose: bool,
) -> Tuple[int, int, int, int, bool]:
    if engine is None:
        return 0, 0, 0, 0, False

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
    except Exception as e:
        turso_error = True
        if verbose:
            print(f"[ai] recover_error {type(e).__name__}: {e}", flush=True)

    flushed_redis, flush_redis_error = flush_redis_to_turso(
        client=redis_client,
        queue_key=redis_queue_key,
        spool_dir=spool_dir,
        engine=engine,
        max_items=200,
        verbose=bool(verbose),
    )
    flushed_spool, flush_spool_error = flush_spool_to_turso(
        spool_dir=spool_dir,
        engine=engine,
        max_items=200,
        verbose=bool(verbose),
    )
    scheduled, schedule_error = _schedule_ai(
        executor,
        engine=engine,
        worker_threads=worker_threads,
        config=config,
        limiter=limiter,
        verbose=bool(verbose),
    )
    turso_error = bool(turso_error or flush_redis_error or flush_spool_error or schedule_error)
    return recovered, flushed_redis, flushed_spool, scheduled, turso_error


def _run_loop_once(
    executor: ThreadPoolExecutor,
    *,
    args: argparse.Namespace,
    engine: Engine,
    rss_urls: list[str],
    spool_dir: Path,
    redis_client,
    redis_queue_key: str,
    limit: Optional[int],
    worker_threads: int,
    config: LLMConfig,
    limiter: RateLimiter,
    active_hours: Optional[tuple[int, int]],
    turso_ready: bool,
) -> Tuple[bool, bool, int, int, int, int, int]:
    verbose = bool(args.verbose)
    if active_hours is not None:
        sleep_until_active(active_hours, verbose=verbose)

    active_engine_used = False
    if not turso_ready:
        turso_ready = _ensure_turso_ready(engine=engine, verbose=verbose, turso_ready=turso_ready)
    active_engine: Optional[Engine] = engine if turso_ready else None
    active_engine_used = active_engine is not None

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

    recovered = 0
    flushed_redis = 0
    flushed_spool = 0
    scheduled = 0
    turso_error = bool(ingest_turso_error)

    if active_engine is not None:
        recovered, flushed_redis, flushed_spool, scheduled, tick_error = _run_turso_tick(
            executor,
            engine=active_engine,
            spool_dir=spool_dir,
            redis_client=redis_client,
            redis_queue_key=redis_queue_key,
            worker_threads=worker_threads,
            config=config,
            limiter=limiter,
            stuck_seconds=int(args.ai_stuck_seconds),
            verbose=verbose,
        )
        turso_error = bool(turso_error or tick_error)

    if turso_error:
        turso_ready = False

    return turso_ready, active_engine_used, inserted, flushed_redis, flushed_spool, recovered, scheduled


def main() -> None:
    args = parse_args()
    rss_urls = _require_rss_urls(args)
    active_hours = _parse_active_hours_from_args(args)
    interval = _resolve_interval_seconds(args)
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

    with ThreadPoolExecutor(max_workers=worker_threads) as executor:
        turso_ready = False
        loop_idx = 0
        while True:
            loop_idx += 1
            turso_ready, active_engine_used, inserted, flushed_redis, flushed_spool, recovered, scheduled = _run_loop_once(
                executor,
                args=args,
                engine=engine,
                rss_urls=rss_urls,
                spool_dir=spool_dir,
                redis_client=redis_client,
                redis_queue_key=redis_queue_key,
                limit=limit,
                worker_threads=worker_threads,
                config=config,
                limiter=limiter,
                active_hours=active_hours,
                turso_ready=turso_ready,
            )
            if args.verbose:
                print(
                    f"[loop] idx={loop_idx} turso_ready={int(active_engine_used)} "
                    f"rss_inserted={inserted} redis_flush={flushed_redis} spool_flush={flushed_spool} "
                    f"ai_recovered={recovered} ai_scheduled={scheduled}",
                    flush=True,
                )
            time.sleep(interval)


__all__ = ["main", "parse_args", "LLMConfig"]
