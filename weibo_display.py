from __future__ import annotations

import html
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Iterable, List, Optional

# NOTE: This module is intentionally small and Weibo-specific.
# It converts noisy "回复@xxx:...//@yyy:..." strings into readable Markdown.


QUOTE_MARKER = "//@"
SEGMENT_SEPARATOR = "\n\n---\n\n"
DEFAULT_UNKNOWN_AUTHOR = "未知"
IMG_LINE_TEMPLATE = '<img class="ke_img" src="{url}" />'

_RE_REPLY_PREFIX = re.compile(r"^\s*回复@[^:：\s]+[:：]\s*")
_RE_TRAILING_DASH = re.compile(r"[\s\-–—]+$")
_RE_REPOST_MARKER = re.compile(
    r"(?:^|[\s\n])(?:-\s*)?转发\s*@(?P<nick>[^:：\s]+)\s*[:：]\s*",
    re.MULTILINE,
)


def normalize_weibo_text(text: str) -> str:
    value = html.unescape(str(text or ""))
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    for ch in ("\u00a0", "\u2002", "\u2003", "\u2009", "\u202f", "\ufeff"):
        value = value.replace(ch, " ")
    value = re.sub(r"[ \t]{2,}", " ", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


@dataclass(frozen=True)
class WeiboDisplaySegment:
    speaker: str
    text: str
    is_current: bool


def _strip_reply_prefix(text: str) -> str:
    return _RE_REPLY_PREFIX.sub("", (text or "").strip()).strip()


def _split_nick_and_text(value: str) -> tuple[str, str]:
    """
    Parse "昵称:内容" or "昵称：内容".
    Returns ("", value) when separator not found.
    """
    raw = (value or "").strip()
    if not raw:
        return "", ""

    for sep in (":", "："):
        idx = raw.find(sep)
        if idx > 0:
            nick = raw[:idx].strip()
            content = raw[idx + 1 :].strip()
            return nick, content
    return "", raw


def parse_weibo_reply_chain(raw_text: str, *, default_author: str) -> List[WeiboDisplaySegment]:
    """
    Convert a Weibo-style reply/repost chain to ordered segments (oldest -> newest).

    Example input:
      "回复@A:有点怕//@A:公公真不怕？//@B:原文"
    Output order:
      B -> A -> default_author
    """
    text = normalize_weibo_text(raw_text)
    if not text:
        return []

    parts = text.split(QUOTE_MARKER)
    if not parts:
        return []

    # Current post text (outside the //@ chain)
    current_text = normalize_weibo_text(_strip_reply_prefix(parts[0]))

    quoted_segments: List[WeiboDisplaySegment] = []
    for part in parts[1:]:
        item = (part or "").strip()
        if not item:
            continue
        nick, content = _split_nick_and_text(item)
        content = normalize_weibo_text(_strip_reply_prefix(content))
        speaker = (nick or "").strip() or (default_author or "").strip() or DEFAULT_UNKNOWN_AUTHOR
        if not content:
            continue
        quoted_segments.append(WeiboDisplaySegment(speaker=speaker, text=content, is_current=False))

    ordered: List[WeiboDisplaySegment] = list(reversed(quoted_segments))

    if current_text:
        speaker = (default_author or "").strip() or DEFAULT_UNKNOWN_AUTHOR
        ordered.append(WeiboDisplaySegment(speaker=speaker, text=current_text, is_current=True))

    # Edge: if nothing parsed (e.g. text was only "回复@xxx:"), keep a minimal segment.
    if not ordered and text:
        speaker = (default_author or "").strip() or DEFAULT_UNKNOWN_AUTHOR
        ordered.append(WeiboDisplaySegment(speaker=speaker, text=text, is_current=True))

    return ordered


class _ImgSrcExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.urls: List[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if str(tag or "").lower() != "img":
            return
        attr_map = {str(k or "").lower(): (v or "") for k, v in attrs}
        url = (attr_map.get("src") or "").strip()
        if not url:
            url = (attr_map.get("data-src") or attr_map.get("data-original") or "").strip()
        if not url:
            return
        if url.startswith("//"):
            url = "https:" + url
        self.urls.append(url)


def extract_image_urls_from_html(content_html: str) -> List[str]:
    """
    Extract image urls from RSS HTML content.
    This is a best-effort helper (no heavy dependencies).
    """
    html = (content_html or "").strip()
    if not html:
        return []
    parser = _ImgSrcExtractor()
    try:
        parser.feed(html)
        parser.close()
    except Exception:
        return []
    return dedup_keep_order(parser.urls)


def dedup_keep_order(items: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for item in items:
        value = str(item or "").strip()
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def format_weibo_display_md(
    raw_text: str,
    *,
    author: str,
    image_urls: Optional[List[str]] = None,
) -> str:
    """
    Build the final Markdown (with optional HTML <img>) for UI display.
    """
    segments = parse_weibo_reply_chain(raw_text, default_author=author)
    if not segments:
        return ""

    expanded_segments: List[WeiboDisplaySegment] = []
    for seg in segments:
        expanded_segments.extend(_expand_repost_segments(seg, max_depth=3))

    images = dedup_keep_order(image_urls or [])
    img_lines = [IMG_LINE_TEMPLATE.format(url=url) for url in images]

    blocks: List[str] = []
    for seg in expanded_segments:
        speaker = (seg.speaker or "").strip() or DEFAULT_UNKNOWN_AUTHOR
        text = normalize_weibo_text(seg.text or "")
        if not text:
            continue

        block = f"{speaker}：{text}"

        if seg.is_current and img_lines:
            block = block.rstrip() + "\n" + "\n".join(img_lines)

        blocks.append(block)

    # If there is no "current" segment, attach images to the newest block.
    if blocks and img_lines and not any(seg.is_current for seg in expanded_segments):
        blocks[-1] = blocks[-1].rstrip() + "\n" + "\n".join(img_lines)

    return SEGMENT_SEPARATOR.join(blocks).strip()


def _extract_repost(text: str) -> Optional[tuple[str, str, str]]:
    """
    Best-effort parse:
      "评论 ... - 转发 @A: 原文"
      "转发@A：原文"
    Return: (comment_text, nick, original_text)
    """
    value = normalize_weibo_text(text)
    if not value:
        return None

    match = _RE_REPOST_MARKER.search(value)
    if not match:
        return None

    marker_idx = match.start() + match.group(0).find("转发")
    comment_text = value[:marker_idx].rstrip()
    comment_text = _RE_TRAILING_DASH.sub("", comment_text).strip()
    nick = str(match.group("nick") or "").strip()
    original_text = value[match.end() :].strip()
    if not nick or not original_text:
        return None
    return comment_text, nick, original_text


def _expand_repost_segments(seg: WeiboDisplaySegment, *, max_depth: int) -> List[WeiboDisplaySegment]:
    if max_depth <= 0:
        return [seg]

    extracted = _extract_repost(seg.text)
    if not extracted:
        return [seg]

    comment_text, nick, original_text = extracted

    original_seg = WeiboDisplaySegment(speaker=nick, text=original_text, is_current=False)

    expanded_original = _expand_repost_segments(original_seg, max_depth=max_depth - 1)

    speaker = (seg.speaker or "").strip() or DEFAULT_UNKNOWN_AUTHOR
    retweeter_text = comment_text.rstrip()
    retweeter_seg = WeiboDisplaySegment(speaker=speaker, text=retweeter_text, is_current=seg.is_current)

    return expanded_original + [retweeter_seg]
