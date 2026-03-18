import argparse
import hashlib
import html
import os
import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import feedparser
import requests

from init_workdb import init_workdb
from ingest_weibo_to_workdb import (
    AI_MODE_COMPLETION,
    AI_MODE_RESPONSES,
    DEFAULT_AI_MODE,
    DEFAULT_AI_REASONING_EFFORT,
    DEFAULT_AI_RETRY_COUNT,
    DEFAULT_AI_TEMPERATURE,
    DEFAULT_API_STYLE,
    DEFAULT_BASE_URL,
    DEFAULT_MODEL,
    DEFAULT_PROMPT_VERSION,
    AnalyzeResult,
    analyze_with_gemini,
    clean_text,
    mark_done,
    mark_error,
    mark_processing,
    split_commentary_and_quoted,
    upsert_post_base,
    validate_and_adjust_assertions,
    write_assertions,
)


CST = timezone(timedelta(hours=8))
BASE62_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
BASE62_INDEX = {ch: idx for idx, ch in enumerate(BASE62_ALPHABET)}

SQLITE_CONNECT_TIMEOUT_SEC = 30.0
SQLITE_BUSY_TIMEOUT_MS = 5000


def connect_workdb(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=SQLITE_CONNECT_TIMEOUT_SEC)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS};")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def is_workdb_empty(db_path: Path) -> bool:
    conn = connect_workdb(db_path)
    try:
        row = conn.execute("SELECT 1 FROM posts LIMIT 1").fetchone()
        return row is None
    finally:
        conn.close()


def env_bool(name: str) -> Optional[bool]:
    value = os.getenv(name)
    if value is None:
        return None
    v = str(value).strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return None


def env_int(name: str) -> Optional[int]:
    value = os.getenv(name)
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    try:
        return int(v)
    except Exception:
        return None


def env_float(name: str) -> Optional[float]:
    value = os.getenv(name)
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    try:
        return float(v)
    except Exception:
        return None


def cleanup_old_posts(db_path: Path, *, keep_days: int, verbose: bool) -> int:
    if keep_days <= 0:
        return 0
    threshold = (datetime.now(CST) - timedelta(days=int(keep_days))).strftime("%Y-%m-%d %H:%M:%S")
    conn = connect_workdb(db_path)
    try:
        conn.execute(
            """
            DELETE FROM posts
            WHERE ingested_at < ?
              AND status != 'processing'
            """,
            (threshold,),
        )
        deleted = int(conn.execute("SELECT changes()").fetchone()[0])
        conn.commit()
        if deleted and verbose:
            print(f"[cleanup] deleted_posts={deleted} before_ingested_at={threshold}", flush=True)
        return deleted
    finally:
        conn.close()


@dataclass
class Counters:
    total: int = 0
    inserted: int = 0
    processed: int = 0
    skipped_existing: int = 0
    skipped_done: int = 0
    skipped_no_id: int = 0
    skipped_no_url: int = 0
    skipped_empty: int = 0
    failed: int = 0


@dataclass
class LLMConfig:
    api_key: str
    model: str
    prompt_version: str
    relevant_threshold: float
    api_style: str
    base_url: str
    litellm_provider: str
    gemini_auth: str
    api_type: str
    api_mode: str
    ai_stream: bool
    ai_retries: int
    ai_temperature: float
    ai_reasoning_effort: str
    ai_rpm: float
    trace_out: Optional[Path]
    ai_timeout_seconds: float
    reprocess: bool
    verbose: bool


class RateLimiter:
    def __init__(self, rpm: float) -> None:
        self.min_interval = 60.0 / rpm if rpm and rpm > 0 else 0.0
        self._lock = threading.Lock()
        self._next_ts = 0.0

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        with self._lock:
            now = time.time()
            if now < self._next_ts:
                time.sleep(self._next_ts - now)
                now = time.time()
            self._next_ts = max(self._next_ts, now) + self.min_interval


def now_str() -> str:
    return datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")


def fetch_feed(url: str, timeout: float) -> feedparser.FeedParserDict:
    headers = {
        "User-Agent": "AlphaVault-RSS-Ingest/1.0",
        "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml",
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return feedparser.parse(resp.content)


def html_to_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = str(value)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n", text)
    text = re.sub(r"(?i)<p\s*>", "", text)
    text = re.sub(r"(?is)<script.*?>.*?</script>", "", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", "", text)
    text = re.sub(r"(?s)<[^>]+>", "", text)
    text = html.unescape(text)
    text = text.replace("\r\n", "\n").strip()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def get_entry_content(entry: feedparser.FeedParserDict) -> str:
    if "content" in entry and entry.content:
        for item in entry.content:
            if isinstance(item, dict) and item.get("value"):
                return item["value"]
            if hasattr(item, "value") and item.value:
                return item.value
    for key in ("summary", "description", "title"):
        val = entry.get(key)
        if val:
            return val
    return ""


def parse_datetime(entry: feedparser.FeedParserDict) -> str:
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        parsed = getattr(entry, key, None)
        if parsed:
            dt = datetime(*parsed[:6], tzinfo=timezone.utc).astimezone(CST)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    return now_str()


def extract_numeric_id(value: str) -> str:
    if not value:
        return ""
    value = str(value)
    if value.isdigit():
        return value
    match = re.search(r"(?:mid|id)=([0-9]{6,})", value)
    if match:
        return match.group(1)
    match = re.search(r"/detail/([0-9]{6,})", value)
    if match:
        return match.group(1)
    return ""


def extract_bid(link: str, user_id: Optional[str]) -> str:
    if not link:
        return ""
    if user_id:
        match = re.search(rf"/{re.escape(user_id)}/([A-Za-z0-9]+)", link)
        if match:
            return match.group(1)
    path = urlparse(link).path.strip("/")
    if not path:
        return ""
    seg = path.split("/")[-1]
    if user_id and seg == user_id:
        return ""
    return seg


def base62_to_int(value: str) -> int:
    num = 0
    for ch in value:
        if ch not in BASE62_INDEX:
            raise ValueError(f"invalid_base62_char:{ch}")
        num = num * 62 + BASE62_INDEX[ch]
    return num


def bid_to_mid(bid: str) -> str:
    if not bid:
        return ""
    result = ""
    i = len(bid)
    while i > 0:
        start = max(0, i - 4)
        part = bid[start:i]
        num = base62_to_int(part)
        part_str = str(num)
        if start > 0:
            part_str = part_str.zfill(7)
        result = part_str + result
        i = start
    return result


def build_ids(entry: feedparser.FeedParserDict, link: str, user_id: Optional[str]) -> tuple[str, str, str]:
    candidates = []
    for key in ("id", "guid"):
        val = entry.get(key)
        if val:
            candidates.append(str(val))
    if link:
        candidates.append(link)
    for val in candidates:
        mid = extract_numeric_id(val)
        if mid:
            return mid, f"weibo:{mid}", ""
    bid = extract_bid(link, user_id)
    if bid:
        try:
            mid = bid_to_mid(bid)
        except ValueError:
            mid = ""
        if mid:
            return mid, f"weibo:{mid}", bid
        return bid, f"weibo:bid:{bid}", bid
    if link:
        digest = hashlib.sha1(link.encode("utf-8")).hexdigest()[:20]
        return f"linkhash:{digest}", f"weibo:linkhash:{digest}", ""
    return "", "", ""


def choose_author(entry: feedparser.FeedParserDict, feed: feedparser.FeedParserDict, fallback: str) -> str:
    author = entry.get("author") or feed.feed.get("author") or feed.feed.get("title")
    author = (author or "").strip()
    if author:
        return author
    return fallback


def exists_by_post_uid(conn: sqlite3.Connection, post_uid: str) -> bool:
    row = conn.execute("SELECT 1 FROM posts WHERE post_uid = ? LIMIT 1", (post_uid,)).fetchone()
    return row is not None


def exists_by_url(conn: sqlite3.Connection, url: str) -> bool:
    row = conn.execute("SELECT 1 FROM posts WHERE url = ? LIMIT 1", (url,)).fetchone()
    return row is not None


# First-boot helper: if cloud already has a post_uid, copy it back to local to skip LLM/sync cost.
def backfill_posts_from_cloud(
    *,
    local_db: Path,
    engine: Any,
    post_uids: list[str],
    verbose: bool,
) -> set[str]:
    if not post_uids:
        return set()
    from sqlalchemy import text

    backfilled: set[str] = set()
    local_conn = connect_workdb(local_db)
    try:
        with engine.connect() as cloud_conn:
            for post_uid in post_uids:
                row = (
                    cloud_conn.execute(
                        text(
                            """
                            SELECT post_uid, platform, platform_post_id, author, created_at, url, raw_text,
                                   final_status, invest_score, processed_at, model, prompt_version, archived_at
                            FROM posts
                            WHERE post_uid = :post_uid
                            LIMIT 1
                            """
                        ),
                        {"post_uid": post_uid},
                    )
                    .mappings()
                    .fetchone()
                )
                if not row:
                    continue

                assertions_rows = (
                    cloud_conn.execute(
                        text(
                            """
                            SELECT idx, topic_key, action, action_strength, summary, evidence, confidence,
                                   stock_codes_json, stock_names_json, industries_json, commodities_json, indices_json
                            FROM assertions
                            WHERE post_uid = :post_uid
                            ORDER BY idx ASC
                            """
                        ),
                        {"post_uid": post_uid},
                    )
                    .mappings()
                    .fetchall()
                )

                status = clean_text(row.get("final_status", "")).lower()
                if status not in {"relevant", "irrelevant"}:
                    status = "irrelevant"

                processed_at = clean_text(row.get("processed_at", "")).strip() or None
                model = clean_text(row.get("model", "")).strip() or None
                prompt_version = clean_text(row.get("prompt_version", "")).strip() or None
                synced_at = clean_text(row.get("archived_at", "")).strip() or now_str()

                local_conn.execute(
                    """
                    UPDATE posts
                    SET platform=?,
                        platform_post_id=?,
                        author=?,
                        created_at=?,
                        url=?,
                        raw_text=?,
                        status=?,
                        invest_score=?,
                        processed_at=?,
                        last_error=NULL,
                        next_retry_at=NULL,
                        model=?,
                        prompt_version=?,
                        sync_status='synced',
                        sync_attempts=0,
                        next_sync_at=NULL,
                        synced_at=?,
                        sync_error=NULL
                    WHERE post_uid=?
                    """,
                    (
                        clean_text(row.get("platform", "")).strip() or "weibo",
                        clean_text(row.get("platform_post_id", "")),
                        clean_text(row.get("author", "")),
                        clean_text(row.get("created_at", "")),
                        clean_text(row.get("url", "")),
                        clean_text(row.get("raw_text", "")),
                        status,
                        row.get("invest_score"),
                        processed_at,
                        model,
                        prompt_version,
                        synced_at,
                        post_uid,
                    ),
                )

                cloud_assertions: list[Dict[str, Any]] = []
                for a in assertions_rows:
                    cloud_assertions.append(
                        {
                            "topic_key": clean_text(a.get("topic_key", "")) or "other:misc",
                            "action": clean_text(a.get("action", "")) or "view.bullish",
                            "action_strength": int(a.get("action_strength") or 1),
                            "summary": clean_text(a.get("summary", "")) or "未提供摘要",
                            "evidence": clean_text(a.get("evidence", "")),
                            "confidence": float(a.get("confidence") or 0.5),
                            "stock_codes_json": clean_text(a.get("stock_codes_json", "[]")) or "[]",
                            "stock_names_json": clean_text(a.get("stock_names_json", "[]")) or "[]",
                            "industries_json": clean_text(a.get("industries_json", "[]")) or "[]",
                            "commodities_json": clean_text(a.get("commodities_json", "[]")) or "[]",
                            "indices_json": clean_text(a.get("indices_json", "[]")) or "[]",
                        }
                    )
                write_assertions(local_conn, post_uid, cloud_assertions)
                backfilled.add(post_uid)
                if verbose:
                    print(
                        f"[backfill] hit {post_uid} status={status} assertions={len(cloud_assertions)}",
                        flush=True,
                    )
        local_conn.commit()
    finally:
        local_conn.close()

    return backfilled


def build_analysis_context(raw_text: str) -> Dict[str, str]:
    commentary_text, quoted_text = split_commentary_and_quoted(raw_text)
    return {
        "commentary_text": commentary_text,
        "quoted_text": quoted_text,
    }


def _split_rss_urls(value: str) -> list[str]:
    if not value:
        return []
    parts: list[str] = []
    for item in re.split(r"[,\n]+", str(value)):
        url = item.strip()
        if url:
            parts.append(url)
    return parts


def parse_rss_urls(args: argparse.Namespace) -> list[str]:
    urls: list[str] = []
    for item in getattr(args, "rss_url", []) or []:
        urls.extend(_split_rss_urls(item))
    urls.extend(_split_rss_urls(getattr(args, "rss_urls", "") or ""))

    if not urls:
        urls.extend(_split_rss_urls(os.getenv("RSS_URLS", "")))
        urls.extend(_split_rss_urls(os.getenv("RSS_URL", "")))

    seen: set[str] = set()
    out: list[str] = []
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def infer_user_id_from_rss_url(rss_url: str) -> Optional[str]:
    if not rss_url:
        return None
    match = re.search(r"/weibo/user/([0-9]{4,})", rss_url)
    if match:
        return match.group(1)
    match = re.search(r"/user/([0-9]{4,})", rss_url)
    if match:
        return match.group(1)
    return None


def parse_active_hours(value: str) -> Optional[tuple[int, int]]:
    v = str(value or "").strip()
    if not v:
        return None
    match = re.fullmatch(r"(\d{1,2})\s*-\s*(\d{1,2})", v)
    if not match:
        raise ValueError("invalid_active_hours_format")
    start = int(match.group(1))
    end = int(match.group(2))
    if start < 0 or start > 23 or end < 0 or end > 23:
        raise ValueError("invalid_active_hours_range")
    return start, end


def in_active_hours(now_dt: datetime, active_hours: tuple[int, int]) -> bool:
    start, end = active_hours
    hour = now_dt.hour
    if start <= end:
        return start <= hour <= end
    return hour >= start or hour <= end


def sleep_until_active(active_hours: tuple[int, int], *, verbose: bool) -> None:
    now_dt = datetime.now(CST)
    if in_active_hours(now_dt, active_hours):
        return
    start_hour, end_hour = active_hours
    today_start = now_dt.replace(hour=start_hour, minute=0, second=0, microsecond=0)

    if start_hour <= end_hour:
        if now_dt.hour < start_hour:
            next_dt = today_start
        else:
            next_dt = today_start + timedelta(days=1)
    else:
        # Cross midnight, e.g. 22-6. If we are inactive, next start is today at start_hour.
        if now_dt.hour < start_hour and now_dt.hour > end_hour:
            next_dt = today_start
        else:
            next_dt = today_start

    sleep_sec = max(1.0, (next_dt - now_dt).total_seconds())
    if verbose:
        print(
            f"[schedule] inactive now={now_dt.strftime('%Y-%m-%d %H:%M:%S')} "
            f"active={start_hour}-{end_hour} sleep={int(sleep_sec)}s",
            flush=True,
        )
    time.sleep(sleep_sec)


def build_row_meta(
    *,
    mid_or_bid: str,
    bid: str,
    link: str,
    title: str,
    author: str,
    created_at: str,
    raw_text: str,
) -> Dict[str, str]:
    row: Dict[str, str] = {
        "link": link,
        "title": title,
        "author": author,
        "published_at": created_at,
        "正文": raw_text,
    }
    if mid_or_bid.isdigit():
        row["id"] = mid_or_bid
    if bid:
        row["bid"] = bid
    return row


def ingest_rss_once(
    *,
    rss_url: str,
    db_path: Path,
    author: str,
    user_id: Optional[str],
    limit: Optional[int],
    rss_timeout: float,
    verbose: bool,
) -> Tuple[Counters, list[Tuple[str, str]]]:
    init_workdb(db_path)
    feed = fetch_feed(rss_url, timeout=rss_timeout)
    conn = connect_workdb(db_path)

    counts = Counters()
    seen_post_uids: set[str] = set()
    seen_urls: set[str] = set()
    new_posts: list[Tuple[str, str]] = []

    try:
        entries = feed.entries or []
        if limit:
            entries = entries[:limit]
        for entry in entries:
            counts.total += 1
            link = (entry.get("link") or entry.get("id") or "").strip()
            if not link:
                counts.skipped_no_url += 1
                continue
            if link in seen_urls:
                counts.skipped_existing += 1
                continue

            platform_post_id, post_uid, bid = build_ids(entry, link, user_id)
            if not post_uid or not platform_post_id:
                counts.skipped_no_id += 1
                continue
            if post_uid in seen_post_uids:
                counts.skipped_existing += 1
                continue

            if exists_by_url(conn, link) and not exists_by_post_uid(conn, post_uid):
                counts.skipped_existing += 1
                continue
            if exists_by_post_uid(conn, post_uid):
                counts.skipped_done += 1
                continue

            title = clean_text(entry.get("title") or "")
            content_html = get_entry_content(entry)
            content_text = html_to_text(content_html)

            raw_text = ""
            if title and content_text and title not in content_text:
                raw_text = f"{title}\n\n{content_text}"
            else:
                raw_text = content_text or title
            raw_text = raw_text.strip()
            if not raw_text:
                counts.skipped_empty += 1
                continue

            created_at = parse_datetime(entry)
            resolved_author = choose_author(entry, feed, author)

            counts.inserted += 1

            upsert_post_base(
                conn,
                post_uid=post_uid,
                platform_post_id=platform_post_id,
                author=resolved_author,
                created_at=created_at,
                url=link,
                raw_text=raw_text,
            )
            conn.commit()

            new_posts.append((post_uid, bid))

            seen_post_uids.add(post_uid)
            seen_urls.add(link)

            if verbose:
                print(f"[rss] inserted {post_uid}", flush=True)
    finally:
        conn.close()

    return counts, new_posts


def ingest_rss_many_once(
    *,
    rss_urls: list[str],
    db_path: Path,
    author: str,
    user_id: Optional[str],
    limit: Optional[int],
    rss_timeout: float,
    verbose: bool,
) -> Tuple[Counters, list[Tuple[str, str]]]:
    merged = Counters()
    all_new_posts: list[Tuple[str, str]] = []
    seen_post_uids: set[str] = set()

    for rss_url in rss_urls:
        try:
            feed_user_id = user_id or infer_user_id_from_rss_url(rss_url)
            counts, new_posts = ingest_rss_once(
                rss_url=rss_url,
                db_path=db_path,
                author=author,
                user_id=feed_user_id,
                limit=limit,
                rss_timeout=rss_timeout,
                verbose=verbose,
            )
        except Exception as e:
            if verbose:
                print(f"[rss] source_error url={rss_url} {type(e).__name__}: {e}", flush=True)
            continue
        for key, value in counts.__dict__.items():
            setattr(merged, key, getattr(merged, key) + int(value or 0))
        for post_uid, bid in new_posts:
            if post_uid in seen_post_uids:
                continue
            seen_post_uids.add(post_uid)
            all_new_posts.append((post_uid, bid))

    return merged, all_new_posts


def _load_post_row(conn: sqlite3.Connection, post_uid: str) -> Optional[tuple]:
    return conn.execute(
        """
        SELECT status, raw_text, author, created_at, url, platform_post_id
        FROM posts WHERE post_uid = ?
        """,
        (post_uid,),
    ).fetchone()


def process_post_uid(
    post_uid: str,
    bid: str,
    *,
    db_path: Path,
    config: LLMConfig,
    limiter: RateLimiter,
) -> bool:
    conn = connect_workdb(db_path)
    try:
        row = _load_post_row(conn, post_uid)
        if not row:
            return False
        status, raw_text, author, created_at, url, platform_post_id = row
        if status in ("processing", "relevant", "irrelevant") and not config.reprocess:
            return False

        attempts = mark_processing(conn, post_uid, model=config.model, prompt_version=config.prompt_version)
        conn.commit()

        analysis_context = build_analysis_context(raw_text or "")
        row_meta = build_row_meta(
            mid_or_bid=str(platform_post_id or ""),
            bid=bid or "",
            link=str(url or ""),
            title="",
            author=str(author or ""),
            created_at=str(created_at or ""),
            raw_text=str(raw_text or ""),
        )

        try:
            if config.verbose:
                print(f"[llm] call_api {post_uid}", flush=True)
            limiter.wait()
            start_ts = time.time()

            result = analyze_with_gemini(
                api_key=config.api_key,
                model=config.model,
                analysis_context=analysis_context,
                row=row_meta,
                api_style=config.api_style,
                base_url=config.base_url,
                litellm_provider=config.litellm_provider,
                gemini_auth=config.gemini_auth,
                api_type=config.api_type,
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
                    f"[llm] done {post_uid} status={result.status} "
                    f"score={result.invest_score:.3f} cost={cost:.1f}s",
                    flush=True,
                )
            if result.invest_score < config.relevant_threshold:
                result = AnalyzeResult(status="irrelevant", invest_score=result.invest_score, assertions=[])
            else:
                result.assertions = validate_and_adjust_assertions(
                    result.assertions,
                    commentary_text=analysis_context["commentary_text"],
                    quoted_text=analysis_context["quoted_text"],
                )
            write_assertions(conn, post_uid, result.assertions)
            mark_done(conn, post_uid, result, model=config.model, prompt_version=config.prompt_version)
            conn.commit()
            return True
        except Exception as e:
            print(f"[llm] error {post_uid} {type(e).__name__}: {e}", flush=True)
            mark_error(conn, post_uid, f"ai:{type(e).__name__}: {e}", attempts)
            conn.commit()
            return False
    finally:
        conn.close()



def recover_processing_posts(db_path: Path, *, verbose: bool) -> int:
    conn = connect_workdb(db_path)
    try:
        now = now_str()
        cur = conn.execute(
            """
            UPDATE posts
            SET status='error',
                last_error=?,
                next_retry_at=?,
                processed_at=?
            WHERE status='processing'
            """,
            ("ai:recovered_after_restart", now, now),
        )
        conn.commit()
        recovered = int(cur.rowcount or 0)
        if recovered and verbose:
            print(f"[llm] recovered_processing={recovered}", flush=True)
        return recovered
    finally:
        conn.close()



def select_due_post_uids(db_path: Path, *, limit: int) -> list[str]:
    conn = connect_workdb(db_path)
    try:
        conn.row_factory = sqlite3.Row
        now = now_str()
        rows = conn.execute(
            """
            SELECT post_uid
            FROM posts
            WHERE status = 'new'
               OR (status = 'error' AND (next_retry_at IS NULL OR next_retry_at <= ?))
            ORDER BY ingested_at ASC
            LIMIT ?
            """,
            (now, max(0, int(limit))),
        ).fetchall()
        return [str(r["post_uid"]) for r in rows if r and r["post_uid"]]
    finally:
        conn.close()



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally ingest Weibo RSS into workdb.sqlite with LLM")
    parser.add_argument(
        "--rss-url",
        action="append",
        default=[],
        help="RSS 地址（可重复传多次）。也支持把多个 URL 用逗号分隔写在一个参数里。",
    )
    parser.add_argument(
        "--rss-urls",
        default="",
        help="多个 RSS 地址（逗号或换行分隔）。如果不传参数，也会尝试读环境变量 RSS_URLS / RSS_URL。",
    )
    parser.add_argument("--db-path", default="workdb.sqlite", help="SQLite 路径")
    parser.add_argument("--author", default="", help="作者名（为空则从 RSS 里取）")
    parser.add_argument("--user-id", default="", help="微博用户ID，用于从链接提取 bid")
    parser.add_argument(
        "--active-hours",
        default="",
        help="只在这些小时运行（CST），格式: 6-22；为空表示全天。",
    )
    parser.add_argument("--limit", type=int, default=0, help="最多处理多少条（0 表示不限）")
    parser.add_argument("--rss-timeout", type=float, default=15.0, help="RSS HTTP 超时秒数")
    parser.add_argument("--reprocess", action="store_true", help="重新处理已有记录")
    parser.add_argument("--relevant-threshold", type=float, default=0.35, help="相关度阈值")
    parser.add_argument("--interval-seconds", type=float, default=0.0, help="RSS 轮询间隔（0 表示只跑一次）")
    parser.add_argument("--worker-threads", type=int, default=0, help="后台 LLM 处理线程数（0=自动）")

    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--api-style", default=DEFAULT_API_STYLE, choices=["gemini", "openai", "litellm"])
    parser.add_argument("--api-type", default=None)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--api-key-env", default=None)
    parser.add_argument(
        "--api-mode",
        default=DEFAULT_AI_MODE,
        choices=[AI_MODE_COMPLETION, AI_MODE_RESPONSES],
    )
    parser.add_argument("--ai-stream", action="store_true")
    parser.add_argument("--litellm-provider", default=os.getenv("AI_API_TYPE") or os.getenv("LITELLM_PROVIDER", "gemini"))
    parser.add_argument("--gemini-auth", default=os.getenv("GEMINI_AUTH", "query"), choices=["query", "bearer"])
    parser.add_argument("--prompt-version", default=DEFAULT_PROMPT_VERSION)
    parser.add_argument("--ai-retries", type=int, default=DEFAULT_AI_RETRY_COUNT)
    parser.add_argument("--ai-temperature", type=float, default=DEFAULT_AI_TEMPERATURE)
    parser.add_argument(
        "--ai-reasoning-effort",
        default=DEFAULT_AI_REASONING_EFFORT,
        choices=["none", "minimal", "low", "medium", "high", "xhigh"],
    )
    parser.add_argument("--ai-rpm", type=float, default=env_float("AI_RPM") or 0.0)
    parser.add_argument("--ai-timeout-sec", type=float, default=env_float("AI_TIMEOUT_SEC") or 60.0)
    parser.add_argument("--ai-max-inflight", type=int, default=env_int("AI_MAX_INFLIGHT") or 0)
    parser.add_argument("--trace-out", type=Path, default=None)
    parser.add_argument("--progress-every", type=int, default=10)
    parser.add_argument("--enable-sync", action="store_true", help="同一个进程里跑 Turso sync（后台线程）")
    parser.add_argument("--sync-batch-size", type=int, default=200, help="sync 每次条数（enable-sync 时生效）")
    parser.add_argument("--sync-interval-seconds", type=float, default=0.0, help="sync 循环间隔（0=读 SYNC_INTERVAL_SECONDS 或默认 600）")
    parser.add_argument("--sync-active-hours", default="", help="sync 只在这些小时运行（CST），格式: 6-22；为空=全天（默认读 SYNC_ACTIVE_HOURS）")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    init_workdb(Path(args.db_path))

    rss_urls = parse_rss_urls(args)
    if not rss_urls:
        raise RuntimeError("Missing RSS url(s). Provide --rss-url/--rss-urls or set RSS_URLS/RSS_URL.")
    if args.verbose:
        print(f"[rss] sources={len(rss_urls)}", flush=True)

    active_hours_value = args.active_hours.strip() if args.active_hours else os.getenv("RSS_ACTIVE_HOURS", "").strip()
    active_hours: Optional[tuple[int, int]] = None
    if active_hours_value:
        try:
            active_hours = parse_active_hours(active_hours_value)
        except ValueError as e:
            raise RuntimeError(f"Invalid active hours: {active_hours_value} ({e})") from e

    # AI config via env (recommended for Docker/supervisord)
    ai_stream_env = env_bool("AI_STREAM")
    if ai_stream_env is not None:
        args.ai_stream = bool(ai_stream_env)
    trace_out_env = os.getenv("AI_TRACE_OUT", "").strip()
    if trace_out_env and args.trace_out is None:
        args.trace_out = Path(trace_out_env)

    if args.base_url == DEFAULT_BASE_URL and not os.getenv("AI_BASE_URL", "").strip():
        env_base_url = os.getenv("OPENAI_BASE_URL", "").strip()
        if env_base_url:
            args.base_url = env_base_url

    api_key = ""
    if args.api_key:
        api_key = str(args.api_key).strip()
    else:
        api_key = os.getenv("AI_API_KEY", "").strip()
        if not api_key:
            if args.api_key_env:
                key_env = str(args.api_key_env).strip()
            else:
                key_env = "OPENAI_API_KEY" if os.getenv("OPENAI_API_KEY") else "GEMINI_API_KEY"
            if key_env:
                api_key = os.getenv(key_env, "").strip()
    if not api_key:
        raise RuntimeError("Missing API key. Provide --api-key or set AI_API_KEY/OPENAI_API_KEY/GEMINI_API_KEY.")

    api_type = clean_text(args.api_type)
    if not api_type and args.api_style == "litellm":
        api_type = clean_text(args.litellm_provider)

    limit = args.limit if args.limit and args.limit > 0 else None

    config = LLMConfig(
        api_key=api_key,
        model=args.model,
        prompt_version=args.prompt_version,
        relevant_threshold=max(0.0, min(1.0, args.relevant_threshold)),
        api_style=args.api_style,
        base_url=args.base_url,
        litellm_provider=args.litellm_provider,
        gemini_auth=args.gemini_auth,
        api_type=api_type,
        api_mode=args.api_mode,
        ai_stream=bool(args.ai_stream),
        ai_retries=max(0, args.ai_retries),
        ai_temperature=args.ai_temperature,
        ai_reasoning_effort=args.ai_reasoning_effort,
        ai_rpm=max(0.0, args.ai_rpm),
        trace_out=args.trace_out,
        ai_timeout_seconds=max(1.0, args.ai_timeout_sec),
        reprocess=bool(args.reprocess),
        verbose=bool(args.verbose),
    )

    limiter = RateLimiter(config.ai_rpm)
    if args.worker_threads and args.worker_threads > 0:
        worker_threads = int(args.worker_threads)
    elif args.ai_max_inflight and args.ai_max_inflight > 0:
        worker_threads = int(args.ai_max_inflight)
    else:
        worker_threads = 4
    if args.ai_max_inflight and args.ai_max_inflight > 0:
        worker_threads = min(worker_threads, int(args.ai_max_inflight))

    # Avoid stuck tasks after restart (e.g. crash while processing)
    recover_processing_posts(Path(args.db_path), verbose=bool(args.verbose))

    # First-boot optimization: only run once when local is empty (and user didn't ask to reprocess).
    cloud_backfill_enabled = (not bool(args.reprocess)) and is_workdb_empty(Path(args.db_path))

    retention_days = env_int("WORKDB_RETENTION_DAYS")
    if retention_days is None:
        retention_days = 10
    cleanup_interval_sec = env_float("WORKDB_CLEANUP_INTERVAL_SECONDS")
    if cleanup_interval_sec is None:
        cleanup_interval_sec = 3600.0
    last_cleanup_ts = 0.0

    def _maybe_cleanup(*, force: bool = False) -> None:
        nonlocal last_cleanup_ts
        if retention_days <= 0:
            return
        if not force and cleanup_interval_sec > 0:
            if time.time() - last_cleanup_ts < float(cleanup_interval_sec):
                return
        last_cleanup_ts = time.time()
        try:
            cleanup_old_posts(Path(args.db_path), keep_days=int(retention_days), verbose=bool(args.verbose))
        except Exception as e:
            print(f"[cleanup] error {type(e).__name__}: {e}", flush=True)

    _maybe_cleanup(force=True)

    scheduled: set[str] = set()
    scheduled_lock = threading.Lock()

    def _on_done(post_uid: str, fut) -> None:
        with scheduled_lock:
            scheduled.discard(post_uid)
        try:
            if fut.exception():
                print(f"[llm] task_error {post_uid}: {fut.exception()}", flush=True)
        except Exception:
            pass

    def _submit(executor: ThreadPoolExecutor, post_uid: str, bid: str) -> None:
        with scheduled_lock:
            if post_uid in scheduled:
                return
            scheduled.add(post_uid)
        fut = executor.submit(
            process_post_uid,
            post_uid,
            bid,
            db_path=Path(args.db_path),
            config=config,
            limiter=limiter,
        )
        fut.add_done_callback(lambda f, pid=post_uid: _on_done(pid, f))

    def _schedule_due(executor: ThreadPoolExecutor) -> int:
        # Keep queue bounded; RSS always has priority.
        with scheduled_lock:
            inflight = len(scheduled)
        max_queue = max(20, int(worker_threads) * 4)
        if inflight >= max_queue:
            return 0
        due = select_due_post_uids(Path(args.db_path), limit=max(1, max_queue - inflight))
        for post_uid in due:
            _submit(executor, post_uid, "")
        return len(due)

    def _run_once(executor: ThreadPoolExecutor) -> Counters:
        nonlocal cloud_backfill_enabled
        counts, new_posts = ingest_rss_many_once(
            rss_urls=rss_urls,
            db_path=Path(args.db_path),
            author=args.author.strip(),
            user_id=(args.user_id.strip() or None),
            limit=limit,
            rss_timeout=args.rss_timeout,
            verbose=bool(args.verbose),
        )
        if cloud_backfill_enabled:
            cloud_backfill_enabled = False
            turso_url = os.getenv("TURSO_DATABASE_URL", "").strip()
            turso_token = os.getenv("TURSO_AUTH_TOKEN", "").strip()
            if turso_url and new_posts:
                try:
                    from sync_workdb_to_turso import ensure_turso_engine

                    engine = ensure_turso_engine(turso_url, turso_token)
                    backfilled = backfill_posts_from_cloud(
                        local_db=Path(args.db_path),
                        engine=engine,
                        post_uids=[pid for pid, _ in new_posts],
                        verbose=bool(args.verbose),
                    )
                    if backfilled:
                        new_posts = [(pid, bid) for pid, bid in new_posts if pid not in backfilled]
                        if args.verbose:
                            print(
                                f"[backfill] done hit={len(backfilled)} remaining={len(new_posts)}",
                                flush=True,
                            )
                except Exception as e:
                    if args.verbose:
                        print(f"[backfill] error {type(e).__name__}: {e}", flush=True)

        for post_uid, bid in new_posts:
            _submit(executor, post_uid, bid)
        _schedule_due(executor)
        _maybe_cleanup(force=False)
        return counts

    sync_stop = threading.Event()

    def _sync_loop() -> None:
        interval = float(args.sync_interval_seconds or 0.0)
        if interval <= 0:
            interval = float(os.getenv("SYNC_INTERVAL_SECONDS", "600") or "600")
        interval = max(1.0, interval)

        active_hours_value = (
            (args.sync_active_hours or "").strip()
            or os.getenv("SYNC_ACTIVE_HOURS", "").strip()
        )
        sync_active_hours: Optional[tuple[int, int]] = None
        if active_hours_value:
            try:
                sync_active_hours = parse_active_hours(active_hours_value)
            except ValueError as e:
                print(f"[sync] invalid_active_hours {active_hours_value} ({e})", flush=True)
                return

        turso_url = os.getenv("TURSO_DATABASE_URL", "").strip()
        turso_token = os.getenv("TURSO_AUTH_TOKEN", "").strip()
        if not turso_url:
            print("[sync] disabled: missing TURSO_DATABASE_URL", flush=True)
            return

        engine = None
        while not sync_stop.is_set():
            try:
                if sync_active_hours is not None:
                    sleep_until_active(sync_active_hours, verbose=bool(args.verbose))

                if engine is None:
                    from sync_workdb_to_turso import (
                        ensure_turso_engine,
                        init_cloud_schema,
                        mark_connect_error_for_pending_posts,
                        sync_once,
                    )

                    try:
                        engine = ensure_turso_engine(turso_url, turso_token)
                        init_cloud_schema(engine)
                    except Exception as e:
                        engine = None
                        error = f"turso:connect:{type(e).__name__}: {e}"
                        try:
                            mark_connect_error_for_pending_posts(
                                local_db=Path(args.db_path),
                                batch_size=max(1, int(args.sync_batch_size)),
                                error=error,
                                verbose=bool(args.verbose),
                            )
                        except Exception as mark_e:
                            print(
                                f"[sync] mark_connect_error_failed {type(mark_e).__name__}: {mark_e}",
                                flush=True,
                            )
                        print(f"[sync] connect_error {type(e).__name__}: {e}", flush=True)
                        sync_stop.wait(interval)
                        continue

                stats = sync_once(
                    local_db=Path(args.db_path),
                    engine=engine,
                    batch_size=max(1, int(args.sync_batch_size)),
                    verbose=bool(args.verbose),
                )
                if args.verbose:
                    print(
                        "[sync] total={total} synced={synced} failed={failed}".format(**stats.__dict__),
                        flush=True,
                    )
            except Exception as e:
                engine = None
                print(f"[sync] loop_error {type(e).__name__}: {e}", flush=True)
            sync_stop.wait(interval)

    with ThreadPoolExecutor(max_workers=worker_threads) as executor:
        sync_thread = None
        if bool(args.enable_sync):
            sync_thread = threading.Thread(target=_sync_loop, name="sync_loop", daemon=True)
            sync_thread.start()

        if args.interval_seconds and args.interval_seconds > 0:
            loop_idx = 0
            try:
                while True:
                    loop_idx += 1
                    if active_hours is not None:
                        sleep_until_active(active_hours, verbose=bool(args.verbose))
                    counts = _run_once(executor)
                    if args.progress_every and args.progress_every > 0:
                        if loop_idx % int(args.progress_every) == 0:
                            print(
                                "[rss] loop={loop} total={total} inserted={inserted} skipped_existing={skipped_existing} "
                                "skipped_done={skipped_done} skipped_no_id={skipped_no_id} skipped_no_url={skipped_no_url} "
                                "skipped_empty={skipped_empty}".format(loop=loop_idx, **counts.__dict__),
                                flush=True,
                            )
                    elif args.verbose:
                        print(
                            "[rss] loop={loop} total={total} inserted={inserted} skipped_existing={skipped_existing} "
                            "skipped_done={skipped_done} skipped_no_id={skipped_no_id} skipped_no_url={skipped_no_url} "
                            "skipped_empty={skipped_empty}".format(loop=loop_idx, **counts.__dict__),
                            flush=True,
                        )
                    time.sleep(max(1.0, float(args.interval_seconds)))
            except KeyboardInterrupt:
                print("[rss] stopped by user", flush=True)
        else:
            counts = _run_once(executor)
            print(
                "[done] total={total} inserted={inserted} skipped_existing={skipped_existing} "
                "skipped_done={skipped_done} skipped_no_id={skipped_no_id} skipped_no_url={skipped_no_url} "
                "skipped_empty={skipped_empty}".format(**counts.__dict__),
                flush=True,
            )
            executor.shutdown(wait=True)
        sync_stop.set()
        if sync_thread is not None:
            sync_thread.join(timeout=5.0)


if __name__ == "__main__":
    main()
