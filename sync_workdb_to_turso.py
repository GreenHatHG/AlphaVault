import argparse
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


CST = timezone(timedelta(hours=8))


@dataclass
class SyncStats:
    total: int = 0
    synced: int = 0
    failed: int = 0
    skipped: int = 0


def now_str() -> str:
    return datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")


def ensure_turso_engine(url: str, token: str) -> Engine:
    if not url:
        raise RuntimeError("Missing TURSO_DATABASE_URL")
    if url.startswith("libsql://"):
        turso_url = url[9:]
    else:
        turso_url = url
    return create_engine(
        f"sqlite+libsql://{turso_url}?secure=true",
        connect_args={"auth_token": token} if token else {},
        future=True,
    )


def init_cloud_schema(engine: Engine) -> None:
    ddl_posts = """
    CREATE TABLE IF NOT EXISTS posts (
        post_uid TEXT PRIMARY KEY,
        platform TEXT NOT NULL,
        platform_post_id TEXT NOT NULL,
        author TEXT NOT NULL,
        created_at TEXT NOT NULL,
        url TEXT NOT NULL,
        raw_text TEXT NOT NULL,
        final_status TEXT NOT NULL CHECK (final_status IN ('relevant', 'irrelevant')),
        invest_score REAL,
        processed_at TEXT,
        model TEXT,
        prompt_version TEXT,
        archived_at TEXT NOT NULL
    );
    """
    ddl_assertions = """
    CREATE TABLE IF NOT EXISTS assertions (
        post_uid TEXT NOT NULL,
        idx INTEGER NOT NULL CHECK (idx >= 1),
        topic_key TEXT NOT NULL,
        action TEXT NOT NULL,
        action_strength INTEGER NOT NULL CHECK (action_strength BETWEEN 0 AND 3),
        summary TEXT NOT NULL,
        evidence TEXT NOT NULL,
        confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
        stock_codes_json TEXT NOT NULL DEFAULT '[]',
        stock_names_json TEXT NOT NULL DEFAULT '[]',
        industries_json TEXT NOT NULL DEFAULT '[]',
        commodities_json TEXT NOT NULL DEFAULT '[]',
        indices_json TEXT NOT NULL DEFAULT '[]',
        UNIQUE(post_uid, idx)
    );
    """
    idx_sql = """
    CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at);
    CREATE INDEX IF NOT EXISTS idx_posts_author_created_at ON posts(author, created_at);
    CREATE INDEX IF NOT EXISTS idx_posts_platform_post_id ON posts(platform_post_id);
    CREATE INDEX IF NOT EXISTS idx_assertions_topic_key ON assertions(topic_key);
    CREATE INDEX IF NOT EXISTS idx_assertions_action ON assertions(action);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl_posts))
        conn.execute(text(ddl_assertions))
        for stmt in idx_sql.strip().split(";\n"):
            if stmt.strip():
                conn.execute(text(stmt))


def select_pending_posts(conn: sqlite3.Connection, limit: int) -> Iterable[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT post_uid, platform, platform_post_id, author, created_at, url, raw_text,
               status, invest_score, processed_at, model, prompt_version, sync_attempts
        FROM posts
        WHERE status IN ('relevant', 'irrelevant')
          AND (sync_status IS NULL OR sync_status != 'synced')
          AND (next_sync_at IS NULL OR next_sync_at <= ?)
        ORDER BY processed_at ASC
        LIMIT ?
        """,
        (now_str(), limit),
    )


def load_assertions(conn: sqlite3.Connection, post_uid: str) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT idx, topic_key, action, action_strength, summary, evidence, confidence,
               stock_codes_json, stock_names_json, industries_json, commodities_json, indices_json
        FROM assertions
        WHERE post_uid = ?
        ORDER BY idx ASC
        """,
        (post_uid,),
    ).fetchall()


def upsert_cloud_post(engine: Engine, row: sqlite3.Row) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO posts (
                    post_uid, platform, platform_post_id, author, created_at, url, raw_text,
                    final_status, invest_score, processed_at, model, prompt_version, archived_at
                ) VALUES (
                    :post_uid, :platform, :platform_post_id, :author, :created_at, :url, :raw_text,
                    :final_status, :invest_score, :processed_at, :model, :prompt_version, :archived_at
                )
                ON CONFLICT(post_uid) DO UPDATE SET
                    platform=excluded.platform,
                    platform_post_id=excluded.platform_post_id,
                    author=excluded.author,
                    created_at=excluded.created_at,
                    url=excluded.url,
                    raw_text=excluded.raw_text,
                    final_status=excluded.final_status,
                    invest_score=excluded.invest_score,
                    processed_at=excluded.processed_at,
                    model=excluded.model,
                    prompt_version=excluded.prompt_version,
                    archived_at=excluded.archived_at
                """
            ),
            {
                "post_uid": row["post_uid"],
                "platform": row["platform"] or "weibo",
                "platform_post_id": row["platform_post_id"],
                "author": row["author"],
                "created_at": row["created_at"],
                "url": row["url"],
                "raw_text": row["raw_text"],
                "final_status": row["status"],
                "invest_score": row["invest_score"],
                "processed_at": row["processed_at"],
                "model": row["model"],
                "prompt_version": row["prompt_version"],
                "archived_at": now_str(),
            },
        )


def sync_cloud_assertions(engine: Engine, post_uid: str, assertions: list[sqlite3.Row]) -> None:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM assertions WHERE post_uid = :post_uid"), {"post_uid": post_uid})
        for a in assertions:
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
                    ON CONFLICT(post_uid, idx) DO UPDATE SET
                        topic_key=excluded.topic_key,
                        action=excluded.action,
                        action_strength=excluded.action_strength,
                        summary=excluded.summary,
                        evidence=excluded.evidence,
                        confidence=excluded.confidence,
                        stock_codes_json=excluded.stock_codes_json,
                        stock_names_json=excluded.stock_names_json,
                        industries_json=excluded.industries_json,
                        commodities_json=excluded.commodities_json,
                        indices_json=excluded.indices_json
                    """
                ),
                {
                    "post_uid": post_uid,
                    "idx": a["idx"],
                    "topic_key": a["topic_key"],
                    "action": a["action"],
                    "action_strength": a["action_strength"],
                    "summary": a["summary"],
                    "evidence": a["evidence"],
                    "confidence": a["confidence"],
                    "stock_codes_json": a["stock_codes_json"],
                    "stock_names_json": a["stock_names_json"],
                    "industries_json": a["industries_json"],
                    "commodities_json": a["commodities_json"],
                    "indices_json": a["indices_json"],
                },
            )


def mark_sync_success(conn: sqlite3.Connection, post_uid: str, attempts: int) -> None:
    conn.execute(
        """
        UPDATE posts
        SET sync_status='synced',
            sync_attempts=?,
            next_sync_at=NULL,
            sync_error=NULL,
            synced_at=?
        WHERE post_uid=?
        """,
        (attempts, now_str(), post_uid),
    )


def mark_sync_error(conn: sqlite3.Connection, post_uid: str, attempts: int, error: str) -> None:
    retry_minutes = min(60, 2 ** min(attempts, 6))
    next_retry = (datetime.now(timezone.utc) + timedelta(minutes=retry_minutes)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    conn.execute(
        """
        UPDATE posts
        SET sync_status='error',
            sync_attempts=?,
            next_sync_at=?,
            sync_error=?
        WHERE post_uid=?
        """,
        (attempts, next_retry, error[:1000], post_uid),
    )


def sync_once(
    *,
    local_db: Path,
    engine: Engine,
    batch_size: int,
    verbose: bool,
) -> SyncStats:
    stats = SyncStats()
    conn = sqlite3.connect(local_db)
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        rows = list(select_pending_posts(conn, batch_size))
        if not rows:
            return stats
        for row in rows:
            stats.total += 1
            attempts = int(row["sync_attempts"] or 0) + 1
            try:
                upsert_cloud_post(engine, row)
                if row["status"] == "relevant":
                    assertions = load_assertions(conn, row["post_uid"])
                    sync_cloud_assertions(engine, row["post_uid"], assertions)
                else:
                    sync_cloud_assertions(engine, row["post_uid"], [])
                mark_sync_success(conn, row["post_uid"], attempts)
                conn.commit()
                stats.synced += 1
                if verbose:
                    print(f"[sync] ok {row['post_uid']}", flush=True)
            except Exception as e:
                mark_sync_error(conn, row["post_uid"], attempts, f"{type(e).__name__}: {e}")
                conn.commit()
                stats.failed += 1
                if verbose:
                    print(f"[sync] error {row['post_uid']} {type(e).__name__}: {e}", flush=True)
    finally:
        conn.close()
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync local workdb.sqlite to Turso (incremental upsert).")
    parser.add_argument("--local-db", default="workdb.sqlite", help="本地 workdb.sqlite 路径")
    parser.add_argument("--turso-url", default=os.getenv("TURSO_DATABASE_URL", ""), help="Turso URL")
    parser.add_argument("--turso-token", default=os.getenv("TURSO_AUTH_TOKEN", ""), help="Turso Auth Token")
    parser.add_argument("--batch-size", type=int, default=200, help="每次同步条数")
    parser.add_argument("--interval-seconds", type=float, default=0.0, help="循环间隔（0=只跑一次）")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    engine = ensure_turso_engine(args.turso_url, args.turso_token)
    init_cloud_schema(engine)

    if args.interval_seconds and args.interval_seconds > 0:
        loop_idx = 0
        try:
            while True:
                loop_idx += 1
                stats = sync_once(
                    local_db=Path(args.local_db),
                    engine=engine,
                    batch_size=max(1, int(args.batch_size)),
                    verbose=bool(args.verbose),
                )
                if args.verbose:
                    print(
                        "[sync] loop={loop} total={total} synced={synced} failed={failed}".format(
                            loop=loop_idx, **stats.__dict__
                        ),
                        flush=True,
                    )
                time.sleep(max(1.0, float(args.interval_seconds)))
        except KeyboardInterrupt:
            print("[sync] stopped by user", flush=True)
    else:
        stats = sync_once(
            local_db=Path(args.local_db),
            engine=engine,
            batch_size=max(1, int(args.batch_size)),
            verbose=bool(args.verbose),
        )
        print(
            "[sync] total={total} synced={synced} failed={failed}".format(**stats.__dict__),
            flush=True,
        )


if __name__ == "__main__":
    main()
