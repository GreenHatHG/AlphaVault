"""
Turso queue helpers.

This module treats Turso (libsql) as the single source of truth for:
- RSS items (raw_text)
- AI processing state (ai_status / retry fields)
- AI outputs (assertions)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from turso_db import init_cloud_schema


AI_STATUS_PENDING = "pending"
AI_STATUS_RUNNING = "running"
AI_STATUS_DONE = "done"
AI_STATUS_ERROR = "error"

AI_STATUSES_DUE = (AI_STATUS_PENDING, AI_STATUS_ERROR)


@dataclass(frozen=True)
class CloudPost:
    post_uid: str
    platform: str
    platform_post_id: str
    author: str
    created_at: str
    url: str
    raw_text: str
    ai_retry_count: int


def _table_columns(conn, table: str) -> set[str]:
    rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    out: set[str] = set()
    for row in rows:
        # row is a tuple: (cid, name, type, notnull, dflt_value, pk)
        if row and len(row) >= 2:
            out.add(str(row[1]))
    return out


def ensure_cloud_queue_schema(engine: Engine, *, verbose: bool) -> None:
    """
    Ensure base schema exists (posts/assertions), then add queue columns to posts.
    """
    init_cloud_schema(engine)

    extra_columns: list[tuple[str, str]] = [
        ("ai_status", "ai_status TEXT NOT NULL DEFAULT 'done'"),
        ("ai_retry_count", "ai_retry_count INTEGER NOT NULL DEFAULT 0"),
        ("ai_next_retry_at", "ai_next_retry_at INTEGER"),
        ("ai_running_at", "ai_running_at INTEGER"),
        ("ai_last_error", "ai_last_error TEXT"),
        ("ai_result_json", "ai_result_json TEXT"),
        ("ingested_at", "ingested_at INTEGER NOT NULL DEFAULT 0"),
    ]

    with engine.begin() as conn:
        cols = _table_columns(conn, "posts")
        for col_name, col_def in extra_columns:
            if col_name in cols:
                continue
            conn.execute(text(f"ALTER TABLE posts ADD COLUMN {col_def}"))
            if verbose:
                print(f"[turso] schema add_column posts.{col_name}", flush=True)

        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_posts_ai_status_next_retry_at
                    ON posts(ai_status, ai_next_retry_at);
                """
            )
        )


def cloud_post_is_processed(engine: Engine, post_uid: str) -> bool:
    with engine.connect() as conn:
        row = (
            conn.execute(
                text("SELECT processed_at FROM posts WHERE post_uid = :post_uid LIMIT 1"),
                {"post_uid": post_uid},
            )
            .fetchone()
        )
        if not row:
            return False
        processed_at = row[0]
        return processed_at is not None and str(processed_at).strip() != ""


def upsert_pending_post(
    engine: Engine,
    *,
    post_uid: str,
    platform: str,
    platform_post_id: str,
    author: str,
    created_at: str,
    url: str,
    raw_text: str,
    archived_at: str,
    ingested_at: int,
) -> None:
    """
    Insert a new RSS item as a pending AI task.

    NOTE: Cloud posts.final_status is required by existing schema, so we set a placeholder
    final_status='irrelevant' and keep processed_at=NULL until AI is done.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO posts (
                    post_uid, platform, platform_post_id, author, created_at, url, raw_text,
                    final_status, invest_score, processed_at, model, prompt_version, archived_at,
                    ai_status, ai_retry_count, ai_next_retry_at, ai_running_at, ai_last_error, ai_result_json,
                    ingested_at
                ) VALUES (
                    :post_uid, :platform, :platform_post_id, :author, :created_at, :url, :raw_text,
                    :final_status, NULL, NULL, NULL, NULL, :archived_at,
                    :ai_status, 0, NULL, NULL, NULL, NULL,
                    :ingested_at
                )
                ON CONFLICT(post_uid) DO UPDATE SET
                    platform=excluded.platform,
                    platform_post_id=excluded.platform_post_id,
                    author=excluded.author,
                    created_at=excluded.created_at,
                    url=excluded.url,
                    raw_text=excluded.raw_text,
                    archived_at=excluded.archived_at,
                    ingested_at=excluded.ingested_at
                WHERE posts.processed_at IS NULL
                """
            ),
            {
                "post_uid": post_uid,
                "platform": platform,
                "platform_post_id": platform_post_id,
                "author": author,
                "created_at": created_at,
                "url": url,
                "raw_text": raw_text,
                "final_status": "irrelevant",
                "archived_at": archived_at,
                "ai_status": AI_STATUS_PENDING,
                "ingested_at": int(ingested_at),
            },
        )


def select_due_post_uids(engine: Engine, *, now_epoch: int, limit: int) -> list[str]:
    with engine.connect() as conn:
        rows = (
            conn.execute(
                text(
                    """
                    SELECT post_uid
                    FROM posts
                    WHERE ai_status IN ('pending', 'error')
                      AND (ai_next_retry_at IS NULL OR ai_next_retry_at <= :now)
                      AND processed_at IS NULL
                    ORDER BY COALESCE(ai_next_retry_at, 0) ASC, ingested_at ASC
                    LIMIT :limit
                    """
                ),
                {"now": int(now_epoch), "limit": max(0, int(limit))},
            )
            .fetchall()
        )
        return [str(r[0]) for r in rows if r and r[0]]


def try_mark_ai_running(
    engine: Engine,
    *,
    post_uid: str,
    now_epoch: int,
) -> bool:
    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                UPDATE posts
                SET ai_status='running',
                    ai_running_at=:now,
                    ai_retry_count=COALESCE(ai_retry_count, 0) + 1,
                    ai_last_error=NULL,
                    ai_next_retry_at=NULL
                WHERE post_uid=:post_uid
                  AND ai_status IN ('pending', 'error')
                  AND (ai_next_retry_at IS NULL OR ai_next_retry_at <= :now)
                  AND processed_at IS NULL
                """
            ),
            {"post_uid": post_uid, "now": int(now_epoch)},
        )
        return int(res.rowcount or 0) > 0


def load_cloud_post(engine: Engine, post_uid: str) -> CloudPost:
    with engine.connect() as conn:
        row = (
            conn.execute(
                text(
                    """
                    SELECT post_uid, platform, platform_post_id, author, created_at, url, raw_text, ai_retry_count
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
            raise RuntimeError("cloud_post_not_found")
        return CloudPost(
            post_uid=str(row["post_uid"]),
            platform=str(row.get("platform") or "weibo"),
            platform_post_id=str(row.get("platform_post_id") or ""),
            author=str(row.get("author") or ""),
            created_at=str(row.get("created_at") or ""),
            url=str(row.get("url") or ""),
            raw_text=str(row.get("raw_text") or ""),
            ai_retry_count=int(row.get("ai_retry_count") or 0),
        )


def mark_ai_done(
    engine: Engine,
    *,
    post_uid: str,
    final_status: str,
    invest_score: Optional[float],
    processed_at: str,
    model: str,
    prompt_version: str,
    archived_at: str,
    ai_result_json: Optional[str],
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE posts
                SET final_status=:final_status,
                    invest_score=:invest_score,
                    processed_at=:processed_at,
                    model=:model,
                    prompt_version=:prompt_version,
                    archived_at=:archived_at,
                    ai_status='done',
                    ai_running_at=NULL,
                    ai_next_retry_at=NULL,
                    ai_last_error=NULL,
                    ai_result_json=:ai_result_json
                WHERE post_uid=:post_uid
                """
            ),
            {
                "post_uid": post_uid,
                "final_status": final_status,
                "invest_score": invest_score,
                "processed_at": processed_at,
                "model": model,
                "prompt_version": prompt_version,
                "archived_at": archived_at,
                "ai_result_json": ai_result_json,
            },
        )


def write_assertions_and_mark_done(
    engine: Engine,
    *,
    post_uid: str,
    final_status: str,
    invest_score: Optional[float],
    processed_at: str,
    model: str,
    prompt_version: str,
    archived_at: str,
    ai_result_json: Optional[str],
    assertions: Iterable[Dict[str, Any]],
) -> None:
    """
    Commit AI outputs in a single transaction:
    - overwrite assertions for post_uid
    - mark posts row as done
    """
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM assertions WHERE post_uid = :post_uid"), {"post_uid": post_uid})
        for idx, a in enumerate(assertions, start=1):
            conn.execute(
                text(
                    """
                    INSERT INTO assertions (
                        post_uid, idx, topic_key, action, action_strength, summary, evidence, confidence,
                        stock_codes_json, stock_names_json, industries_json, commodities_json, indices_json
                    ) VALUES (
                        :post_uid, :idx, :topic_key, :action, :action_strength, :summary, :evidence, :confidence,
                        :stock_codes_json, :stock_names_json, :industries_json, :commodities_json, :indices_json
                    )
                    """
                ),
                {
                    "post_uid": post_uid,
                    "idx": int(idx),
                    "topic_key": a["topic_key"],
                    "action": a["action"],
                    "action_strength": int(a["action_strength"]),
                    "summary": a["summary"],
                    "evidence": a["evidence"],
                    "confidence": float(a["confidence"]),
                    "stock_codes_json": a.get("stock_codes_json", "[]"),
                    "stock_names_json": a.get("stock_names_json", "[]"),
                    "industries_json": a.get("industries_json", "[]"),
                    "commodities_json": a.get("commodities_json", "[]"),
                    "indices_json": a.get("indices_json", "[]"),
                },
            )

        conn.execute(
            text(
                """
                UPDATE posts
                SET final_status=:final_status,
                    invest_score=:invest_score,
                    processed_at=:processed_at,
                    model=:model,
                    prompt_version=:prompt_version,
                    archived_at=:archived_at,
                    ai_status='done',
                    ai_running_at=NULL,
                    ai_next_retry_at=NULL,
                    ai_last_error=NULL,
                    ai_result_json=:ai_result_json
                WHERE post_uid=:post_uid
                """
            ),
            {
                "post_uid": post_uid,
                "final_status": final_status,
                "invest_score": invest_score,
                "processed_at": processed_at,
                "model": model,
                "prompt_version": prompt_version,
                "archived_at": archived_at,
                "ai_result_json": ai_result_json,
            },
        )


def mark_ai_error(
    engine: Engine,
    *,
    post_uid: str,
    error: str,
    next_retry_at: int,
    archived_at: str,
) -> None:
    msg = (error or "")[:1000]
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE posts
                SET ai_status='error',
                    ai_running_at=NULL,
                    ai_last_error=:error,
                    ai_next_retry_at=:next_retry_at,
                    archived_at=:archived_at
                WHERE post_uid=:post_uid
                """
            ),
            {
                "post_uid": post_uid,
                "error": msg,
                "next_retry_at": int(next_retry_at),
                "archived_at": archived_at,
            },
        )


def recover_stuck_ai_tasks(
    engine: Engine,
    *,
    now_epoch: int,
    stuck_seconds: int,
    verbose: bool,
) -> int:
    threshold = int(now_epoch) - max(0, int(stuck_seconds))
    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                UPDATE posts
                SET ai_status='error',
                    ai_running_at=NULL,
                    ai_last_error='ai:recovered_after_restart',
                    ai_next_retry_at=:next_retry_at
                WHERE ai_status='running'
                  AND ai_running_at IS NOT NULL
                  AND ai_running_at <= :threshold
                  AND processed_at IS NULL
                """
            ),
            {"threshold": threshold, "next_retry_at": int(now_epoch) + 60},
        )
        recovered = int(res.rowcount or 0)
        if recovered and verbose:
            print(f"[ai] recovered_running={recovered}", flush=True)
        return recovered


def recover_done_without_processed_at(engine: Engine, *, verbose: bool) -> int:
    """
    Fix inconsistent rows:
    - ai_status='done' but processed_at is NULL/blank

    Such rows will never be picked by the AI scheduler, so we reset them to pending.
    """
    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                UPDATE posts
                SET ai_status='pending',
                    ai_running_at=NULL,
                    ai_next_retry_at=NULL,
                    ai_last_error='ai:recovered_done_without_processed_at'
                WHERE ai_status='done'
                  AND (processed_at IS NULL OR TRIM(processed_at) = '')
                """
            )
        )
        fixed = int(res.rowcount or 0)
        if fixed and verbose:
            print(f"[ai] recovered_done_without_processed_at={fixed}", flush=True)
        return fixed


def write_cloud_assertions(
    engine: Engine,
    *,
    post_uid: str,
    assertions: Iterable[Dict[str, Any]],
) -> None:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM assertions WHERE post_uid = :post_uid"), {"post_uid": post_uid})
        for idx, a in enumerate(assertions, start=1):
            conn.execute(
                text(
                    """
                    INSERT INTO assertions (
                        post_uid, idx, topic_key, action, action_strength, summary, evidence, confidence,
                        stock_codes_json, stock_names_json, industries_json, commodities_json, indices_json
                    ) VALUES (
                        :post_uid, :idx, :topic_key, :action, :action_strength, :summary, :evidence, :confidence,
                        :stock_codes_json, :stock_names_json, :industries_json, :commodities_json, :indices_json
                    )
                    """
                ),
                {
                    "post_uid": post_uid,
                    "idx": int(idx),
                    "topic_key": a["topic_key"],
                    "action": a["action"],
                    "action_strength": int(a["action_strength"]),
                    "summary": a["summary"],
                    "evidence": a["evidence"],
                    "confidence": float(a["confidence"]),
                    "stock_codes_json": a.get("stock_codes_json", "[]"),
                    "stock_names_json": a.get("stock_names_json", "[]"),
                    "industries_json": a.get("industries_json", "[]"),
                    "commodities_json": a.get("commodities_json", "[]"),
                    "indices_json": a.get("indices_json", "[]"),
                },
            )
