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


def build_analysis_context(raw_text: str) -> Dict[str, str]:
    commentary_text, quoted_text = split_commentary_and_quoted(raw_text)
    return {
        "commentary_text": commentary_text,
        "quoted_text": quoted_text,
    }


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
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

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
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
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
            mark_error(conn, post_uid, f"{type(e).__name__}: {e}", attempts)
            conn.commit()
            return False
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally ingest Weibo RSS into workdb.sqlite with LLM")
    parser.add_argument(
        "--rss-url",
        required=True,
        help="RSS 地址，例如: https://rsshub-flyctl.fly.dev/weibo/user/3962719063?key=...",
    )
    parser.add_argument("--db-path", default="workdb.sqlite", help="SQLite 路径")
    parser.add_argument("--author", default="", help="作者名（为空则从 RSS 里取）")
    parser.add_argument("--user-id", default="", help="微博用户ID，用于从链接提取 bid")
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
    parser.add_argument("--litellm-provider", default=os.getenv("LITELLM_PROVIDER", "gemini"))
    parser.add_argument("--gemini-auth", default=os.getenv("GEMINI_AUTH", "query"), choices=["query", "bearer"])
    parser.add_argument("--prompt-version", default=DEFAULT_PROMPT_VERSION)
    parser.add_argument("--ai-retries", type=int, default=DEFAULT_AI_RETRY_COUNT)
    parser.add_argument("--ai-temperature", type=float, default=DEFAULT_AI_TEMPERATURE)
    parser.add_argument(
        "--ai-reasoning-effort",
        default=DEFAULT_AI_REASONING_EFFORT,
        choices=["none", "minimal", "low", "medium", "high", "xhigh"],
    )
    parser.add_argument("--ai-rpm", type=float, default=0.0)
    parser.add_argument("--ai-timeout-sec", type=float, default=60.0)
    parser.add_argument("--ai-max-inflight", type=int, default=0)
    parser.add_argument("--trace-out", type=Path, default=None)
    parser.add_argument("--progress-every", type=int, default=10)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    api_key = ""
    if args.api_key:
        api_key = str(args.api_key).strip()
    else:
        key_env = str(args.api_key_env or "GEMINI_API_KEY").strip()
        if key_env:
            api_key = os.getenv(key_env, "").strip()
    if not api_key:
        raise RuntimeError("Missing API key. Provide --api-key or set GEMINI_API_KEY.")

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

    def _run_once(executor: ThreadPoolExecutor) -> Counters:
        counts, new_posts = ingest_rss_once(
            rss_url=args.rss_url,
            db_path=Path(args.db_path),
            author=args.author.strip(),
            user_id=(args.user_id.strip() or None),
            limit=limit,
            rss_timeout=args.rss_timeout,
            verbose=bool(args.verbose),
        )
        for post_uid, bid in new_posts:
            _submit(executor, post_uid, bid)
        return counts

    with ThreadPoolExecutor(max_workers=worker_threads) as executor:
        if args.interval_seconds and args.interval_seconds > 0:
            loop_idx = 0
            try:
                while True:
                    loop_idx += 1
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


if __name__ == "__main__":
    main()
