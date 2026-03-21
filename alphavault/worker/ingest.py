from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.engine import Engine

from alphavault.ai.analyze import clean_text
from alphavault.db.turso_queue import upsert_pending_post
from alphavault.rss.utils import (
    build_ids,
    choose_author,
    fetch_feed,
    get_entry_content,
    html_to_text,
    infer_user_id_from_rss_url,
    now_str,
    parse_datetime,
)
from alphavault.weibo.display import extract_image_urls_from_html, format_weibo_display_md
from alphavault.worker.redis_queue import DEFAULT_REDIS_DEDUP_TTL_SECONDS, redis_try_push_dedup
from alphavault.worker.spool import spool_delete, spool_write


def _try_push_to_redis(redis_client, redis_queue_key: str, *, post_uid: str, payload: Dict[str, Any], verbose: bool) -> bool:
    if not redis_client or not redis_queue_key or not post_uid:
        return False
    return redis_try_push_dedup(
        redis_client,
        redis_queue_key,
        post_uid=post_uid,
        payload=payload,
        ttl_seconds=DEFAULT_REDIS_DEDUP_TTL_SECONDS,
        verbose=bool(verbose),
    )


def ingest_rss_many_once(
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

            raw_title = clean_text(entry.get("title") or "")
            title = (html_to_text(raw_title) or raw_title).strip()
            content_html = get_entry_content(entry)
            content_text = html_to_text(content_html)
            image_urls = extract_image_urls_from_html(content_html)

            if title and content_text and title not in content_text:
                raw_text = f"{title}\n\n{content_text}"
            else:
                raw_text = content_text or title
            raw_text = (raw_text or "").strip()
            if not raw_text:
                continue

            created_at = parse_datetime(entry)
            resolved_author = choose_author(entry, feed, author)
            display_md = format_weibo_display_md(raw_text, author=resolved_author, image_urls=image_urls)

            payload: Dict[str, Any] = {
                "post_uid": post_uid,
                "platform": "weibo",
                "platform_post_id": platform_post_id,
                "author": resolved_author,
                "created_at": created_at,
                "url": link,
                "raw_text": raw_text,
                "display_md": display_md,
                "ingested_at": int(time.time()),
            }

            try:
                spool_write(spool_dir, post_uid, payload)
            except Exception as e:
                print(f"[spool] write_error {post_uid} {type(e).__name__}: {e}", flush=True)

            if engine is None:
                _try_push_to_redis(
                    redis_client,
                    redis_queue_key,
                    post_uid=post_uid,
                    payload=payload,
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
                    display_md=display_md,
                    archived_at=now_str(),
                    ingested_at=int(payload["ingested_at"]),
                )
                spool_delete(spool_dir, post_uid)
                inserted += 1
                if verbose:
                    print(f"[rss] inserted {post_uid}", flush=True)
            except Exception as e:
                turso_error = True
                _try_push_to_redis(
                    redis_client,
                    redis_queue_key,
                    post_uid=post_uid,
                    payload=payload,
                    verbose=bool(verbose),
                )
                if verbose:
                    print(f"[rss] turso_write_error {post_uid} {type(e).__name__}: {e}", flush=True)

            seen_post_uids.add(post_uid)
            seen_urls.add(link)

    return inserted, turso_error
