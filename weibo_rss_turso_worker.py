"""
Weibo RSS -> Turso -> AI -> Turso (single instance).

Design goals:
- No local sqlite queue required (no paid docker volume needed).
- RSS items are inserted to Turso first (as pending), then AI runs and updates rows.
- If Turso is temporarily down, items are spooled to local files and (optionally) Redis.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ai_analyze import (
    AI_MODE_COMPLETION,
    AI_MODE_RESPONSES,
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_AI_RETRY_COUNT,
    DEFAULT_AI_TEMPERATURE,
    DEFAULT_MODEL,
    DEFAULT_PROMPT_VERSION,
    AnalyzeResult,
    analyze_with_litellm,
    clean_text,
    validate_and_adjust_assertions,
)
from rss_utils import (
    RateLimiter,
    build_analysis_context,
    build_ids,
    build_row_meta,
    choose_author,
    env_bool,
    env_float,
    env_int,
    fetch_feed,
    get_entry_content,
    html_to_text,
    infer_user_id_from_rss_url,
    now_str,
    parse_active_hours,
    parse_datetime,
    parse_rss_urls,
    sleep_until_active,
)
from turso_db import get_turso_engine_from_env
from turso_queue import (
    ensure_cloud_queue_schema,
    load_cloud_post,
    mark_ai_error,
    recover_done_without_processed_at,
    recover_stuck_ai_tasks,
    select_due_post_uids,
    try_mark_ai_running,
    upsert_pending_post,
    write_assertions_and_mark_done,
)


DEFAULT_SPOOL_DIR = "/tmp/alphavault-spool"
DEFAULT_REDIS_QUEUE_KEY = "alphavault:rss_spool"
DEFAULT_REDIS_DEDUP_TTL_SECONDS = 24 * 3600


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


def _sha1_short(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:20]


def _ensure_spool_dir() -> Path:
    value = os.getenv("SPOOL_DIR", "").strip() or DEFAULT_SPOOL_DIR
    path = Path(value)
    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"[spool] dir_error {path} {type(e).__name__}: {e}", flush=True)
    return path


def _spool_path(spool_dir: Path, post_uid: str) -> Path:
    return spool_dir / f"{_sha1_short(post_uid)}.json"


def _spool_write(spool_dir: Path, post_uid: str, payload: Dict[str, Any]) -> Path:
    path = _spool_path(spool_dir, post_uid)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)
    return path


def _spool_delete(spool_dir: Path, post_uid: str) -> None:
    path = _spool_path(spool_dir, post_uid)
    try:
        path.unlink(missing_ok=True)
    except Exception:
        return


def _try_get_redis():
    redis_url = os.getenv("REDIS_URL", "").strip()
    if not redis_url:
        return None, ""
    try:
        import redis  # type: ignore
    except Exception as e:
        print(f"[redis] disabled import_error {type(e).__name__}: {e}", flush=True)
        return None, ""
    try:
        client = redis.Redis.from_url(redis_url, decode_responses=True)
        client.ping()
    except Exception as e:
        print(f"[redis] disabled connect_error {type(e).__name__}: {e}", flush=True)
        return None, ""
    key = os.getenv("REDIS_QUEUE_KEY", "").strip() or DEFAULT_REDIS_QUEUE_KEY
    return client, key


def _redis_processing_key(queue_key: str) -> str:
    return f"{queue_key}:processing"


def _redis_dedup_key(queue_key: str, post_uid: str) -> str:
    return f"{queue_key}:dedup:{_sha1_short(post_uid)}"


def _redis_try_push_dedup(
    client,
    queue_key: str,
    *,
    post_uid: str,
    payload: Dict[str, Any],
    ttl_seconds: int,
    verbose: bool,
) -> bool:
    if not client or not queue_key or not post_uid:
        return False

    dedup_key = _redis_dedup_key(queue_key, post_uid)
    try:
        ok = client.set(dedup_key, "1", nx=True, ex=max(1, int(ttl_seconds)))
    except Exception as e:
        if verbose:
            print(f"[redis] dedup_set_error {type(e).__name__}: {e}", flush=True)
        return False
    if not ok:
        return False

    try:
        _redis_push(client, queue_key, payload)
        return True
    except Exception as e:
        try:
            client.delete(dedup_key)
        except Exception:
            pass
        if verbose:
            print(f"[redis] push_error {type(e).__name__}: {e}", flush=True)
        return False


def _cloud_post_is_processed_or_newer(engine: Engine, post_uid: str, payload_ingested_at: int) -> bool:
    with engine.connect() as conn:
        row = (
            conn.execute(
                text("SELECT processed_at, ingested_at FROM posts WHERE post_uid = :post_uid LIMIT 1"),
                {"post_uid": post_uid},
            )
            .fetchone()
        )
        if not row:
            return False
        processed_at = row[0]
        if processed_at is not None and str(processed_at).strip() != "":
            return True
        try:
            existing_ingested_at = int(row[1] or 0)
        except Exception:
            existing_ingested_at = 0
        return int(existing_ingested_at) >= int(payload_ingested_at)


def _redis_requeue_processing(client, queue_key: str, *, max_items: int, verbose: bool) -> int:
    if max_items <= 0:
        return 0
    src = _redis_processing_key(queue_key)
    moved = 0
    for _ in range(max_items):
        msg = client.rpoplpush(src, queue_key)
        if not msg:
            break
        moved += 1
    if moved and verbose:
        print(f"[redis] requeue moved={moved}", flush=True)
    return moved


def _redis_pop_to_processing(client, queue_key: str) -> Optional[str]:
    dst = _redis_processing_key(queue_key)
    msg = client.rpoplpush(queue_key, dst)
    if not msg:
        return None
    return str(msg)


def _redis_ack_processing(client, queue_key: str, msg: str) -> None:
    key = _redis_processing_key(queue_key)
    client.lrem(key, 1, msg)


def _redis_push(client, queue_key: str, payload: Dict[str, Any]) -> None:
    client.lpush(queue_key, json.dumps(payload, ensure_ascii=False))


def _flush_redis_to_turso(
    *,
    client,
    queue_key: str,
    spool_dir: Path,
    engine: Optional[Engine],
    max_items: int,
    verbose: bool,
) -> Tuple[int, bool]:
    if not client or not queue_key:
        return 0, False
    if engine is None:
        return 0, False

    # Avoid "stuck in processing": move a few items back each tick.
    try:
        _redis_requeue_processing(client, queue_key, max_items=min(200, max_items), verbose=verbose)
    except Exception as e:
        if verbose:
            print(f"[redis] requeue_error {type(e).__name__}: {e}", flush=True)
        return 0, False

    processed = 0
    for _ in range(max_items):
        try:
            msg = _redis_pop_to_processing(client, queue_key)
        except Exception as e:
            if verbose:
                print(f"[redis] pop_error {type(e).__name__}: {e}", flush=True)
            break
        if not msg:
            break
        try:
            payload = json.loads(msg)
        except Exception as e:
            if verbose:
                print(f"[redis] bad_message {type(e).__name__}: {e}", flush=True)
            try:
                _redis_ack_processing(client, queue_key, msg)
            except Exception:
                pass
            continue

        post_uid = str(payload.get("post_uid") or "")
        if not post_uid:
            try:
                _redis_ack_processing(client, queue_key, msg)
            except Exception:
                pass
            continue

        payload_ingested_at = 0
        try:
            payload_ingested_at = int(payload.get("ingested_at") or 0)
        except Exception:
            payload_ingested_at = 0

        try:
            skip_upsert = _cloud_post_is_processed_or_newer(engine, post_uid, payload_ingested_at)
        except Exception as e:
            if verbose:
                print(f"[redis] turso_check_error {type(e).__name__}: {e}", flush=True)
            return processed, True
        if skip_upsert:
            if verbose:
                print(f"[redis] skip_upsert {post_uid}", flush=True)
            try:
                _redis_ack_processing(client, queue_key, msg)
                try:
                    client.delete(_redis_dedup_key(queue_key, post_uid))
                except Exception:
                    pass
            except Exception as e:
                if verbose:
                    print(f"[redis] ack_error {type(e).__name__}: {e}", flush=True)
                return processed, False
            _spool_delete(spool_dir, post_uid)
            processed += 1
            continue

        try:
            upsert_pending_post(
                engine,
                post_uid=post_uid,
                platform=str(payload.get("platform") or "weibo"),
                platform_post_id=str(payload.get("platform_post_id") or ""),
                author=str(payload.get("author") or ""),
                created_at=str(payload.get("created_at") or now_str()),
                url=str(payload.get("url") or ""),
                raw_text=str(payload.get("raw_text") or ""),
                archived_at=now_str(),
                ingested_at=int(payload.get("ingested_at") or int(time.time())),
            )
        except Exception as e:
            if verbose:
                print(f"[redis] turso_write_error {type(e).__name__}: {e}", flush=True)
            # Leave message in processing; next tick will requeue.
            return processed, True
        try:
            _redis_ack_processing(client, queue_key, msg)
            try:
                client.delete(_redis_dedup_key(queue_key, post_uid))
            except Exception:
                pass
        except Exception as e:
            if verbose:
                print(f"[redis] ack_error {type(e).__name__}: {e}", flush=True)
            return processed, False
        _spool_delete(spool_dir, post_uid)
        processed += 1
    return processed, False


def _flush_spool_to_turso(
    *,
    spool_dir: Path,
    engine: Optional[Engine],
    max_items: int,
    verbose: bool,
) -> Tuple[int, bool]:
    if engine is None:
        return 0, False
    paths = sorted(spool_dir.glob("*.json"))
    if not paths:
        return 0, False
    processed = 0
    for path in paths[: max(0, int(max_items))]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            if verbose:
                print(f"[spool] bad_file {path.name} {type(e).__name__}: {e}", flush=True)
            path.unlink(missing_ok=True)
            continue

        post_uid = str(payload.get("post_uid") or "")
        if not post_uid:
            path.unlink(missing_ok=True)
            continue

        try:
            upsert_pending_post(
                engine,
                post_uid=post_uid,
                platform=str(payload.get("platform") or "weibo"),
                platform_post_id=str(payload.get("platform_post_id") or ""),
                author=str(payload.get("author") or ""),
                created_at=str(payload.get("created_at") or now_str()),
                url=str(payload.get("url") or ""),
                raw_text=str(payload.get("raw_text") or ""),
                archived_at=now_str(),
                ingested_at=int(payload.get("ingested_at") or int(time.time())),
            )
        except Exception as e:
            if verbose:
                print(f"[spool] turso_write_error {path.name} {type(e).__name__}: {e}", flush=True)
            return processed, True

        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
        processed += 1
    return processed, False


def _build_config(args: argparse.Namespace) -> LLMConfig:
    ai_stream_env = env_bool("AI_STREAM")
    ai_stream = True
    if ai_stream_env is not None:
        ai_stream = bool(ai_stream_env)
    elif args.ai_stream:
        ai_stream = True

    trace_out = args.trace_out
    trace_out_env = os.getenv("AI_TRACE_OUT", "").strip()
    if trace_out_env and trace_out is None:
        trace_out = Path(trace_out_env)

    base_url = str(args.base_url or "").strip()

    api_key = ""
    if args.api_key:
        api_key = str(args.api_key).strip()
    else:
        api_key = os.getenv("AI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing AI_API_KEY. Set AI_API_KEY.")

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

        if final_result.status == "relevant":
            assertions = final_result.assertions
        else:
            assertions = []

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
        msg = f"ai:{type(e).__name__}: {e}"
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
        print(f"[llm] error {post_uid} {type(e).__name__}: {e}", flush=True)


def _ingest_rss_many_once(
    *,
    rss_urls: list[str],
    engine: Optional[Engine],
    spool_dir: Path,
    redis_client,
    redis_queue_key: str,
    author: str,
    user_id: Optional[str],
    limit: Optional[int],
    rss_timeout: float,
    verbose: bool,
) -> Tuple[int, bool]:
    inserted = 0
    turso_error = False
    seen_post_uids: set[str] = set()
    seen_urls: set[str] = set()

    for rss_url in rss_urls:
        try:
            feed_user_id = user_id or infer_user_id_from_rss_url(rss_url)
            feed = fetch_feed(rss_url, timeout=rss_timeout)
            entries = feed.entries or []
            if limit:
                entries = entries[:limit]
        except Exception as e:
            if verbose:
                print(f"[rss] source_error url={rss_url} {type(e).__name__}: {e}", flush=True)
            continue

        for entry in entries:
            link = (entry.get("link") or entry.get("id") or "").strip()
            if not link:
                continue
            if link in seen_urls:
                continue

            platform_post_id, post_uid, _bid = build_ids(entry, link, feed_user_id)
            if not post_uid or not platform_post_id:
                continue
            if post_uid in seen_post_uids:
                continue

            title = clean_text(entry.get("title") or "")
            content_html = get_entry_content(entry)
            content_text = html_to_text(content_html)

            if title and content_text and title not in content_text:
                raw_text = f"{title}\n\n{content_text}"
            else:
                raw_text = content_text or title
            raw_text = (raw_text or "").strip()
            if not raw_text:
                continue

            created_at = parse_datetime(entry)
            resolved_author = choose_author(entry, feed, author)

            payload: Dict[str, Any] = {
                "post_uid": post_uid,
                "platform": "weibo",
                "platform_post_id": platform_post_id,
                "author": resolved_author,
                "created_at": created_at,
                "url": link,
                "raw_text": raw_text,
                "ingested_at": int(time.time()),
            }

            try:
                _spool_write(spool_dir, post_uid, payload)
            except Exception as e:
                print(f"[spool] write_error {post_uid} {type(e).__name__}: {e}", flush=True)

            if engine is None:
                if redis_client and redis_queue_key:
                    _redis_try_push_dedup(
                        redis_client,
                        redis_queue_key,
                        post_uid=post_uid,
                        payload=payload,
                        ttl_seconds=DEFAULT_REDIS_DEDUP_TTL_SECONDS,
                        verbose=bool(verbose),
                    )
                continue

            try:
                upsert_pending_post(
                    engine,
                    post_uid=post_uid,
                    platform="weibo",
                    platform_post_id=platform_post_id,
                    author=resolved_author,
                    created_at=created_at,
                    url=link,
                    raw_text=raw_text,
                    archived_at=now_str(),
                    ingested_at=int(payload["ingested_at"]),
                )
                _spool_delete(spool_dir, post_uid)
                inserted += 1
                if verbose:
                    print(f"[rss] inserted {post_uid}", flush=True)
            except Exception as e:
                turso_error = True
                if redis_client and redis_queue_key:
                    _redis_try_push_dedup(
                        redis_client,
                        redis_queue_key,
                        post_uid=post_uid,
                        payload=payload,
                        ttl_seconds=DEFAULT_REDIS_DEDUP_TTL_SECONDS,
                        verbose=bool(verbose),
                    )
                if verbose:
                    print(f"[rss] turso_write_error {post_uid} {type(e).__name__}: {e}", flush=True)

            seen_post_uids.add(post_uid)
            seen_urls.add(link)

    return inserted, turso_error


def parse_args() -> argparse.Namespace:
    ai_retries_env = env_int("AI_RETRIES")
    ai_rpm_env = env_float("AI_RPM")
    ai_timeout_env = env_float("AI_TIMEOUT_SEC")
    ai_max_inflight_env = env_int("AI_MAX_INFLIGHT")

    parser = argparse.ArgumentParser(description="Weibo RSS -> Turso queue -> AI -> Turso")
    parser.add_argument("--rss-url", action="append", default=[], help="RSS 地址（可重复传多次）")
    parser.add_argument("--rss-urls", default="", help="多个 RSS 地址（逗号或换行分隔）")
    parser.add_argument("--author", default="", help="作者名（为空则从 RSS 里取）")
    parser.add_argument("--user-id", default="", help="微博用户ID（可为空，自动推断）")
    parser.add_argument("--active-hours", default="", help="只在这些小时运行（CST），格式: 6-22；为空=全天")
    parser.add_argument("--limit", type=int, default=0, help="最多处理多少条（0 表示不限）")
    parser.add_argument("--rss-timeout", type=float, default=15.0, help="RSS HTTP 超时秒数")
    parser.add_argument("--interval-seconds", type=float, default=0.0, help="轮询间隔（0=读 RSS_INTERVAL_SECONDS 或默认 600）")
    parser.add_argument("--worker-threads", type=int, default=0, help="后台 AI 线程数（0=自动）")
    parser.add_argument("--ai-stuck-seconds", type=int, default=600, help="running 超过多久算卡死（秒）")

    # AI config (litellm only; mostly via env)
    parser.add_argument("--model", default=os.getenv("AI_MODEL", DEFAULT_MODEL))
    parser.add_argument("--base-url", default=os.getenv("AI_BASE_URL", "").strip(), help="可选：OpenAI 兼容接口 base_url（也可用 AI_BASE_URL）")
    parser.add_argument("--api-key", default=None, help="可选：API Key（默认读 AI_API_KEY）")
    parser.add_argument("--api-mode", default=os.getenv("AI_API_MODE", DEFAULT_AI_MODE), choices=[AI_MODE_COMPLETION, AI_MODE_RESPONSES])
    parser.add_argument("--ai-stream", action="store_true")
    parser.add_argument("--prompt-version", default=os.getenv("AI_PROMPT_VERSION", DEFAULT_PROMPT_VERSION))
    parser.add_argument(
        "--ai-retries",
        type=int,
        default=ai_retries_env if ai_retries_env is not None else DEFAULT_AI_RETRY_COUNT,
    )
    parser.add_argument("--ai-temperature", type=float, default=float(os.getenv("AI_TEMPERATURE", str(DEFAULT_AI_TEMPERATURE))))
    parser.add_argument("--ai-reasoning-effort", default=os.getenv("AI_REASONING_EFFORT", DEFAULT_AI_REASONING_EFFORT), choices=["none", "minimal", "low", "medium", "high", "xhigh"])
    parser.add_argument(
        "--ai-rpm",
        type=float,
        default=ai_rpm_env if ai_rpm_env is not None else 12.0,
    )
    parser.add_argument(
        "--ai-timeout-sec",
        type=float,
        default=ai_timeout_env if ai_timeout_env is not None else 1000.0,
    )
    parser.add_argument(
        "--ai-max-inflight",
        type=int,
        default=ai_max_inflight_env if ai_max_inflight_env is not None else 12,
    )
    parser.add_argument("--trace-out", type=Path, default=None)
    parser.add_argument("--relevant-threshold", type=float, default=0.35, help="相关度阈值")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rss_urls = parse_rss_urls(args)
    if not rss_urls:
        raise RuntimeError("Missing RSS url(s). Set RSS_URLS/RSS_URL or pass --rss-url/--rss-urls.")
    if args.verbose:
        print(f"[rss] sources={len(rss_urls)}", flush=True)

    active_hours_value = args.active_hours.strip() if args.active_hours else os.getenv("RSS_ACTIVE_HOURS", "").strip()
    active_hours = None
    if active_hours_value:
        active_hours = parse_active_hours(active_hours_value)

    interval = float(args.interval_seconds or 0.0)
    if interval <= 0:
        interval = float(os.getenv("RSS_INTERVAL_SECONDS", "600") or "600")
    interval = max(1.0, interval)

    config = _build_config(args)
    limiter = RateLimiter(config.ai_rpm)

    if args.worker_threads and args.worker_threads > 0:
        worker_threads = int(args.worker_threads)
    elif args.ai_max_inflight and args.ai_max_inflight > 0:
        worker_threads = int(args.ai_max_inflight)
    else:
        worker_threads = 4
    if args.ai_max_inflight and args.ai_max_inflight > 0:
        worker_threads = min(worker_threads, int(args.ai_max_inflight))

    engine = get_turso_engine_from_env()
    turso_ready = False

    spool_dir = _ensure_spool_dir()
    redis_client, redis_queue_key = _try_get_redis()
    if args.verbose:
        print(f"[spool] dir={spool_dir}", flush=True)
        if redis_client:
            print(f"[redis] enabled key={redis_queue_key}", flush=True)

    def _ensure_turso() -> bool:
        nonlocal turso_ready
        if turso_ready:
            return True
        try:
            ensure_cloud_queue_schema(engine, verbose=bool(args.verbose))
            turso_ready = True
            print("[turso] ready", flush=True)
            return True
        except Exception as e:
            turso_ready = False
            if args.verbose:
                print(f"[turso] not_ready {type(e).__name__}: {e}", flush=True)
            return False

    def _schedule_ai(executor: ThreadPoolExecutor, *, engine: Optional[Engine]) -> Tuple[int, bool]:
        if engine is None:
            return 0, False
        now_epoch = int(time.time())
        try:
            due = select_due_post_uids(engine, now_epoch=now_epoch, limit=max(1, worker_threads * 2))
        except Exception as e:
            if args.verbose:
                print(f"[ai] select_due_error {type(e).__name__}: {e}", flush=True)
            return 0, True

        scheduled = 0
        for post_uid in due:
            try:
                ok = try_mark_ai_running(engine, post_uid=post_uid, now_epoch=now_epoch)
            except Exception as e:
                if args.verbose:
                    print(f"[ai] mark_running_error {type(e).__name__}: {e}", flush=True)
                return scheduled, True
            if not ok:
                continue
            executor.submit(_process_one_post_uid, engine=engine, post_uid=post_uid, config=config, limiter=limiter)
            scheduled += 1
        return scheduled, False

    limit = args.limit if args.limit and args.limit > 0 else None

    with ThreadPoolExecutor(max_workers=worker_threads) as executor:
        loop_idx = 0
        while True:
            loop_idx += 1
            if active_hours is not None:
                sleep_until_active(active_hours, verbose=bool(args.verbose))

            if not turso_ready:
                _ensure_turso()
            active_engine: Optional[Engine] = engine if turso_ready else None

            inserted, ingest_turso_error = _ingest_rss_many_once(
                rss_urls=rss_urls,
                engine=active_engine,
                spool_dir=spool_dir,
                redis_client=redis_client,
                redis_queue_key=redis_queue_key,
                author=args.author.strip(),
                user_id=(args.user_id.strip() or None),
                limit=limit,
                rss_timeout=float(args.rss_timeout),
                verbose=bool(args.verbose),
            )

            turso_error = bool(ingest_turso_error)
            recovered = 0
            flushed_redis = 0
            flushed_spool = 0
            scheduled = 0

            if active_engine is not None:
                try:
                    recovered = recover_stuck_ai_tasks(
                        active_engine,
                        now_epoch=int(time.time()),
                        stuck_seconds=max(60, int(args.ai_stuck_seconds)),
                        verbose=bool(args.verbose),
                    )
                    recovered += recover_done_without_processed_at(active_engine, verbose=bool(args.verbose))
                except Exception as e:
                    turso_error = True
                    if args.verbose:
                        print(f"[ai] recover_error {type(e).__name__}: {e}", flush=True)

                flushed_redis, flush_redis_turso_error = _flush_redis_to_turso(
                    client=redis_client,
                    queue_key=redis_queue_key,
                    spool_dir=spool_dir,
                    engine=active_engine,
                    max_items=200,
                    verbose=bool(args.verbose),
                )
                flushed_spool, flush_spool_turso_error = _flush_spool_to_turso(
                    spool_dir=spool_dir,
                    engine=active_engine,
                    max_items=200,
                    verbose=bool(args.verbose),
                )
                scheduled, schedule_turso_error = _schedule_ai(executor, engine=active_engine)

                turso_error = bool(turso_error or flush_redis_turso_error or flush_spool_turso_error or schedule_turso_error)

            if turso_error:
                turso_ready = False

            if args.verbose:
                print(
                    f"[loop] idx={loop_idx} turso_ready={int(bool(active_engine))} "
                    f"rss_inserted={inserted} redis_flush={flushed_redis} spool_flush={flushed_spool} "
                    f"ai_recovered={recovered} ai_scheduled={scheduled}",
                    flush=True,
                )

            time.sleep(interval)


if __name__ == "__main__":
    main()
