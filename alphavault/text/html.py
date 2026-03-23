"""HTML -> text helpers (keep it light: stdlib only)."""

from __future__ import annotations

import html as _html
import re
from typing import Optional


def html_to_text(value: Optional[str]) -> str:
    """
    Convert HTML-ish text to plain text.

    Note: this is best-effort. It is used by both RSS ingest and Streamlit UI.
    """
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
