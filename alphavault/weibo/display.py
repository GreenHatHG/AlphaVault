from __future__ import annotations

"""
Weibo display helpers.

This project stores raw RSS text in posts.raw_text. Streamlit uses display_md to show
readable content. Keep it simple: escape text, preserve line breaks, and optionally
render images.
"""

import html
import re
from typing import Iterable, List, Optional


CSV_RAW_FIELDS_MARKER = "[CSV原始字段]"

_IMG_SRC_RE = re.compile(r'(?is)<img[^>]+src=["\\\']([^"\\\']+)["\\\']')
_IMG_DATA_ORIG_RE = re.compile(r'(?is)<img[^>]+data-original=["\\\']([^"\\\']+)["\\\']')


def extract_image_urls_from_html(content_html: str) -> List[str]:
    if not content_html:
        return []
    text = str(content_html)
    urls: List[str] = []
    for pattern in (_IMG_DATA_ORIG_RE, _IMG_SRC_RE):
        for match in pattern.finditer(text):
            url = str(match.group(1) or "").strip()
            if not url:
                continue
            if url not in urls:
                urls.append(url)
    return urls


def _escape_with_breaks(text: str) -> str:
    safe = html.escape(str(text or ""))
    # Keep paragraphs readable in Streamlit HTML rendering.
    return "<br>".join(safe.splitlines())


def _strip_csv_raw_fields(raw_text: str) -> str:
    text = str(raw_text or "")
    idx = text.find(CSV_RAW_FIELDS_MARKER)
    if idx < 0:
        return text.strip()
    return text[:idx].strip()


def format_weibo_display_md(
    raw_text: str,
    *,
    author: str = "",
    image_urls: Optional[Iterable[str]] = None,
) -> str:
    author_text = str(author or "").strip()
    raw = _strip_csv_raw_fields(raw_text)

    header = f"<div><b>{html.escape(author_text)}</b></div>" if author_text else ""
    body = f"<div style='white-space:pre-wrap; line-height:1.45'>{_escape_with_breaks(raw)}</div>"

    imgs = ""
    if image_urls:
        parts: List[str] = []
        for url in image_urls:
            u = str(url or "").strip()
            if not u:
                continue
            parts.append(
                "<div style='margin-top:8px'>"
                f"<img src='{html.escape(u, quote=True)}' style='max-width:100%; border-radius:8px' />"
                "</div>"
            )
        imgs = "".join(parts)

    return f"{header}{body}{imgs}"
