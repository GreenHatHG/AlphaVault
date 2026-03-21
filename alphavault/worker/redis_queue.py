from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

from alphavault.constants import ENV_REDIS_QUEUE_KEY, ENV_REDIS_URL
from alphavault.db.turso_queue import upsert_pending_post
from alphavault.rss.utils import now_str
from alphavault.weibo.display import format_weibo_display_md
from alphavault.worker.spool import sha1_short, spool_delete


DEFAULT_REDIS_QUEUE_KEY = "alphavault:rss_spool"
DEFAULT_REDIS_DEDUP_TTL_SECONDS = 24 * 3600


def try_get_redis():
    redis_url = os.getenv(ENV_REDIS_URL, "").strip()
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
    key = os.getenv(ENV_REDIS_QUEUE_KEY, "").strip() or DEFAULT_REDIS_QUEUE_KEY
    return client, key


def _redis_processing_key(queue_key: str) -> str:
    return f"{queue_key}:processing"


def _redis_dedup_key(queue_key: str, post_uid: str) -> str:
    return f"{queue_key}:dedup:{sha1_short(post_uid)}"


def redis_try_push_dedup(
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


def _ack_and_cleanup(
    client,
    queue_key: str,
    *,
    msg: str,
    post_uid: str,
    spool_dir: Path,
    verbose: bool,
) -> bool:
    """
    Ack a message and cleanup local state.

    Keep behavior the same as the old inline code:
    - Ack failure => return False (caller decides return value).
    - dedup key deletion is best-effort (ignore errors).
    - spool_delete errors are NOT swallowed (raise).
    """
    try:
        _redis_ack_processing(client, queue_key, msg)
        try:
            client.delete(_redis_dedup_key(queue_key, post_uid))
        except Exception:
            pass
    except Exception as e:
        if verbose:
            print(f"[redis] ack_error {type(e).__name__}: {e}", flush=True)
        return False

    spool_delete(spool_dir, post_uid)
    return True


def flush_redis_to_turso(
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
            if not _ack_and_cleanup(
                client,
                queue_key,
                msg=msg,
                post_uid=post_uid,
                spool_dir=spool_dir,
                verbose=verbose,
            ):
                return processed, False
            processed += 1
            continue

        try:
            raw_text = str(payload.get("raw_text") or "")
            author = str(payload.get("author") or "")
            display_md = str(payload.get("display_md") or "")
            if not display_md.strip():
                display_md = format_weibo_display_md(raw_text, author=author)
            upsert_pending_post(
                engine,
                post_uid=post_uid,
                platform=str(payload.get("platform") or "weibo"),
                platform_post_id=str(payload.get("platform_post_id") or ""),
                author=str(payload.get("author") or ""),
                created_at=str(payload.get("created_at") or now_str()),
                url=str(payload.get("url") or ""),
                raw_text=raw_text,
                display_md=display_md,
                archived_at=now_str(),
                ingested_at=int(payload.get("ingested_at") or int(time.time())),
            )
        except Exception as e:
            if verbose:
                print(f"[redis] turso_write_error {type(e).__name__}: {e}", flush=True)
            # Leave message in processing; next tick will requeue.
            return processed, True
        if not _ack_and_cleanup(
            client,
            queue_key,
            msg=msg,
            post_uid=post_uid,
            spool_dir=spool_dir,
            verbose=verbose,
        ):
            return processed, False
        processed += 1
    return processed, False
