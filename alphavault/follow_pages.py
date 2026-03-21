from __future__ import annotations

"""
Follow pages (no-code configs) stored in Turso.

One page = follow one thing:
- follow_type: "topic" or "cluster"
- follow_key: topic_key or cluster_key
- keywords_text: optional OR keywords
"""

from datetime import datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from alphavault.constants import DATETIME_FMT


FOLLOW_PAGES_TABLE = "follow_pages"

FOLLOW_TYPE_TOPIC = "topic"
FOLLOW_TYPE_CLUSTER = "cluster"
ALLOWED_FOLLOW_TYPES = {FOLLOW_TYPE_TOPIC, FOLLOW_TYPE_CLUSTER}


def _now_str() -> str:
    # Keep the same datetime shape used in other modules (YYYY-MM-DD HH:MM:SS).
    return datetime.now().strftime(DATETIME_FMT)


def normalize_follow_type(value: object) -> str:
    s = str(value or "").strip().lower()
    return s if s in ALLOWED_FOLLOW_TYPES else ""


def make_page_key(*, follow_type: str, follow_key: str) -> str:
    t = normalize_follow_type(follow_type)
    k = str(follow_key or "").strip()
    if not t or not k:
        return ""
    return f"{t}:{k}"


def init_follow_pages_schema(engine: Engine) -> None:
    """
    Create optional follow_pages table.

    Intentionally additive (CREATE TABLE IF NOT EXISTS).
    """
    ddl_pages = f"""
    CREATE TABLE IF NOT EXISTS {FOLLOW_PAGES_TABLE} (
        page_key TEXT PRIMARY KEY,
        follow_type TEXT NOT NULL CHECK (follow_type IN ('topic', 'cluster')),
        follow_key TEXT NOT NULL,
        page_name TEXT NOT NULL DEFAULT '',
        keywords_text TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """
    idx_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_{FOLLOW_PAGES_TABLE}_follow_type_key
        ON {FOLLOW_PAGES_TABLE}(follow_type, follow_key);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl_pages))
        for stmt in idx_sql.strip().split(";\n"):
            if stmt.strip():
                conn.execute(text(stmt))


def ensure_follow_pages_schema(engine: Engine) -> None:
    init_follow_pages_schema(engine)


def try_load_follow_pages(engine: Engine) -> tuple[pd.DataFrame, str]:
    """
    Best-effort load follow_pages.

    Returns: (pages_df, error_message)
    """
    try:
        pages = pd.read_sql_query(
            f"""
            SELECT page_key, follow_type, follow_key, page_name, keywords_text, created_at, updated_at
            FROM {FOLLOW_PAGES_TABLE}
            """,
            engine,
        )
        return pages, ""
    except Exception as exc:
        return pd.DataFrame(), f"{type(exc).__name__}: {exc}"


def upsert_follow_page(
    engine: Engine,
    *,
    follow_type: str,
    follow_key: str,
    page_name: str,
    keywords_text: str,
) -> str:
    follow_type_norm = normalize_follow_type(follow_type)
    follow_key_norm = str(follow_key or "").strip()
    page_key = make_page_key(follow_type=follow_type_norm, follow_key=follow_key_norm)
    if not page_key:
        raise ValueError("Invalid follow_type/follow_key")

    now = _now_str()
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {FOLLOW_PAGES_TABLE}(
                    page_key, follow_type, follow_key, page_name, keywords_text, created_at, updated_at
                )
                VALUES (:page_key, :follow_type, :follow_key, :page_name, :keywords_text, :now, :now)
                ON CONFLICT(page_key) DO UPDATE SET
                    page_name = excluded.page_name,
                    keywords_text = excluded.keywords_text,
                    updated_at = excluded.updated_at
                """
            ),
            {
                "page_key": page_key,
                "follow_type": follow_type_norm,
                "follow_key": follow_key_norm,
                "page_name": str(page_name or "").strip(),
                "keywords_text": str(keywords_text or ""),
                "now": now,
            },
        )
    return page_key


def delete_follow_page(engine: Engine, *, page_key: str) -> int:
    key = str(page_key or "").strip()
    if not key:
        return 0
    with engine.begin() as conn:
        res = conn.execute(
            text(
                f"""
                DELETE FROM {FOLLOW_PAGES_TABLE}
                WHERE page_key = :page_key
                """
            ),
            {"page_key": key},
        )
        return int(res.rowcount or 0)


__all__ = [
    "ALLOWED_FOLLOW_TYPES",
    "FOLLOW_PAGES_TABLE",
    "FOLLOW_TYPE_CLUSTER",
    "FOLLOW_TYPE_TOPIC",
    "delete_follow_page",
    "ensure_follow_pages_schema",
    "init_follow_pages_schema",
    "make_page_key",
    "normalize_follow_type",
    "try_load_follow_pages",
    "upsert_follow_page",
]
