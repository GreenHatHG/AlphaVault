from __future__ import annotations

import json
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
CSV_RAW_FIELD_MARKER = "[CSV原始字段]"
WEIBO_META_SECTION_HEADER = "[微博元信息]"
WEIBO_REPOST_ORIGINAL_SECTION_HEADER = "[转发原文]"

_RE_REPLY_PREFIX = re.compile(r"^\s*回复@[^:：\s]+[:：]\s*")
_RE_TRAILING_DASH = re.compile(r"[\s\-–—]+$")
_RE_REPOST_MARKER = re.compile(
    r"(?:^|[\s\n])(?:-\s*)?转发\s*@(?P<nick>[^:：\s]+)\s*[:：]\s*",
    re.MULTILINE,
)
_RE_BLANK_LINE_SPLIT = re.compile(r"\n\s*\n+")
_RE_KNOWN_SECTION_HEADER_LINE = re.compile(r"(?m)^\s*\[(?:CSV原始字段|微博元信息|转发原文)\]\s*$")


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


def _collapse_whitespace_key(text: str) -> str:
    normalized = normalize_weibo_text(text)
    return re.sub(r"\s+", " ", normalized).strip()


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


def _find_json_object_end(text: str, *, start_idx: int) -> int:
    """
    Return the position right after the matched JSON object, or -1 when not found.
    This is a small brace matcher that ignores braces inside JSON strings.
    """
    if start_idx < 0 or start_idx >= len(text) or text[start_idx] != "{":
        return -1

    depth = 0
    in_string = False
    escaped = False
    for idx in range(start_idx, len(text)):
        ch = text[idx]
        if in_string:
            if escaped:
                escaped = False
                continue
            if ch == "\\":
                escaped = True
                continue
            if ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            depth += 1
            continue
        if ch == "}":
            depth -= 1
            if depth == 0:
                return idx + 1

    return -1


def _extract_section_blocks(text: str, *, header: str) -> tuple[List[str], str]:
    """
    Extract all blocks after a section header line.

    Example:
      "[转发原文]\\nAAA\\nBBB\\n\\n[CSV原始字段]\\n{...}"
    Returns:
      (["AAA\\nBBB"], remaining_text_without_that_section)
    """
    value = str(text or "")
    if not value:
        return [], value

    header_re = re.compile(rf"(?m)^\s*{re.escape(header)}\s*$")
    blocks: List[str] = []

    while True:
        match = header_re.search(value)
        if not match:
            break

        header_line_end = value.find("\n", match.end())
        if header_line_end < 0:
            value = value[: match.start()]
            break
        content_start = header_line_end + 1

        next_header = _RE_KNOWN_SECTION_HEADER_LINE.search(value, pos=content_start)
        block_end = next_header.start() if next_header else len(value)

        block = value[content_start:block_end].strip()
        if block:
            blocks.append(block)

        value = value[: match.start()] + value[block_end:]

    return blocks, value


def _strip_section_blocks(text: str, *, header: str) -> str:
    _blocks, remaining = _extract_section_blocks(text, header=header)
    return remaining


def _extract_csv_source_nick(raw_text: str) -> str:
    """
    Extract "源用户昵称" from the "[CSV原始字段]" JSON block (if present).
    Used only for display attribution; the CSV block itself is still removed from display_md.
    """
    text = str(raw_text or "")
    if not text:
        return ""

    search_from = 0
    while True:
        marker_idx = text.find(CSV_RAW_FIELD_MARKER, search_from)
        if marker_idx < 0:
            return ""

        marker_line_end = text.find("\n", marker_idx)
        marker_line_end = len(text) if marker_line_end < 0 else marker_line_end + 1

        scan = marker_line_end
        while scan < len(text) and text[scan] in {" ", "\t", "\r", "\n"}:
            scan += 1
        if scan >= len(text) or text[scan] != "{":
            search_from = marker_line_end
            continue

        json_start = scan
        json_end = _find_json_object_end(text, start_idx=json_start)
        if json_end < 0:
            search_from = marker_line_end
            continue

        try:
            payload = json.loads(text[json_start:json_end])
        except Exception:
            search_from = json_end
            continue

        if isinstance(payload, dict):
            nick = payload.get("源用户昵称")
            if nick:
                return str(nick).strip()

        search_from = json_end


def _strip_csv_raw_field_blocks(raw_text: str) -> str:
    """
    Remove the "[CSV原始字段]" blocks and the following JSON object (maybe multi-line),
    so they won't show up in display_md.

    Handles:
      "[CSV原始字段]\\n{...}"
      "昵称：[CSV原始字段]\\n{...}"
    """
    text = str(raw_text or "")
    if not text:
        return ""

    while True:
        marker_idx = text.find(CSV_RAW_FIELD_MARKER)
        if marker_idx < 0:
            return text

        line_start = text.rfind("\n", 0, marker_idx)
        line_start = 0 if line_start < 0 else line_start + 1

        marker_line_end = text.find("\n", marker_idx)
        marker_line_end = len(text) if marker_line_end < 0 else marker_line_end + 1

        # Prefer the JSON object right after the marker line (usually next line).
        brace_start = -1
        scan = marker_line_end
        while scan < len(text) and text[scan] in {" ", "\t", "\r", "\n"}:
            scan += 1
        if scan < len(text) and text[scan] == "{":
            brace_start = scan

        # Also handle: "...[CSV原始字段]{...}" on the same line.
        if brace_start < 0:
            scan = marker_idx + len(CSV_RAW_FIELD_MARKER)
            while scan < len(text) and scan < marker_line_end and text[scan] in {" ", "\t"}:
                scan += 1
            if scan < len(text) and scan < marker_line_end and text[scan] == "{":
                brace_start = scan

        removal_end = marker_line_end
        if brace_start >= 0:
            brace_end = _find_json_object_end(text, start_idx=brace_start)
            if brace_end >= 0:
                json_line_end = text.find("\n", brace_end)
                removal_end = len(text) if json_line_end < 0 else json_line_end + 1

        text = text[:line_start] + text[removal_end:]


def _collapse_duplicate_rss_title_and_content(raw_text: str) -> str:
    """
    Some RSS feeds put the same text in both entry.title and entry.content/summary.
    Our worker may concatenate them with a blank line, which can break the //@ parsing.

    If there are exactly 2 blocks separated by blank lines, and after normalization one
    block contains the other (or equals), keep the longer block only.
    """
    raw = str(raw_text or "").strip()
    if not raw:
        return ""

    blocks = [b.strip() for b in _RE_BLANK_LINE_SPLIT.split(raw) if str(b or "").strip()]
    if len(blocks) != 2:
        return raw

    first, second = blocks
    norm_first = _collapse_whitespace_key(first)
    norm_second = _collapse_whitespace_key(second)
    if not norm_first or not norm_second:
        return raw

    if norm_first == norm_second:
        return second if len(second) >= len(first) else first
    if norm_first in norm_second:
        return second
    if norm_second in norm_first:
        return first
    return raw


def parse_weibo_reply_chain(raw_text: str, *, default_author: str) -> List[WeiboDisplaySegment]:
    """
    Convert a Weibo-style reply/repost chain to ordered segments (oldest -> newest).

    Example input:
      "回复@A:有点怕//@A:公公真不怕？//@B:原文"
    Output order:
      B -> A -> default_author
    """
    raw = str(raw_text or "")
    repost_blocks, remaining = _extract_section_blocks(raw, header=WEIBO_REPOST_ORIGINAL_SECTION_HEADER)
    repost_original_text = normalize_weibo_text("\n\n".join(repost_blocks))
    repost_original_author = _extract_csv_source_nick(raw) if repost_original_text else ""

    remaining = _strip_section_blocks(remaining, header=WEIBO_META_SECTION_HEADER)
    remaining = _strip_csv_raw_field_blocks(remaining)
    deduped = _collapse_duplicate_rss_title_and_content(remaining)
    text = normalize_weibo_text(deduped)
    if not text:
        if repost_original_text:
            speaker = repost_original_author or (default_author or "").strip() or DEFAULT_UNKNOWN_AUTHOR
            return [WeiboDisplaySegment(speaker=speaker, text=repost_original_text, is_current=True)]
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

    if repost_original_text:
        speaker = repost_original_author or (default_author or "").strip() or DEFAULT_UNKNOWN_AUTHOR
        if not ordered:
            ordered.insert(0, WeiboDisplaySegment(speaker=speaker, text=repost_original_text, is_current=True))
        else:
            first_key = _collapse_whitespace_key(ordered[0].text)
            repost_key = _collapse_whitespace_key(repost_original_text)
            if repost_key and first_key and repost_key == first_key:
                return ordered
            ordered.insert(0, WeiboDisplaySegment(speaker=speaker, text=repost_original_text, is_current=False))

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
        text = _format_markdown_text(normalize_weibo_text(seg.text or ""))
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


def _format_markdown_text(text: str) -> str:
    """
    Preserve author-intended line breaks inside Markdown by converting single
    newlines to hard breaks, while keeping paragraph breaks.
    """
    value = str(text or "")
    if "\n" not in value:
        return value

    paragraphs = value.split("\n\n")
    formatted: List[str] = []
    for para in paragraphs:
        lines = para.split("\n")
        formatted.append("  \n".join(lines))
    return "\n\n".join(formatted)


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
