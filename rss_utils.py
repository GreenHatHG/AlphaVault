from __future__ import annotations

import argparse
import hashlib
import html as _html
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from urllib.parse import urlparse

import feedparser
import requests

# NOTE: This module is extracted from the old local-sqlite ingest scripts.
# It keeps only RSS parsing + small helpers, so the worker can delete the old route.

CST = timezone(timedelta(hours=8))

BASE62_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
BASE62_INDEX = {ch: idx for idx, ch in enumerate(BASE62_ALPHABET)}


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
    text = re.sub(r"(?i)<br\\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\\s*>", "\n", text)
    text = re.sub(r"(?i)<p\\s*>", "", text)
    text = re.sub(r"(?is)<script.*?>.*?</script>", "", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", "", text)
    text = re.sub(r"(?s)<[^>]+>", "", text)
    text = _html.unescape(text)
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


def build_ids(
    entry: feedparser.FeedParserDict, link: str, user_id: Optional[str]
) -> tuple[str, str, str]:
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


def split_commentary_and_quoted(raw_text: str) -> tuple[str, str]:
    text = (raw_text or "").strip()
    marker = "//@"
    idx = text.find(marker)
    if idx < 0:
        return text, ""
    commentary = text[:idx].strip()
    quoted = text[idx:].strip()
    return commentary, quoted


def build_analysis_context(raw_text: str) -> Dict[str, str]:
    commentary_text, quoted_text = split_commentary_and_quoted(raw_text)
    return {"commentary_text": commentary_text, "quoted_text": quoted_text}


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
