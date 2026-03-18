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

from init_workdb import init_workdb


CST = timezone(timedelta(hours=8))

SQLITE_CONNECT_TIMEOUT_SEC = 30.0
SQLITE_BUSY_TIMEOUT_MS = 5000


def connect_workdb(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=SQLITE_CONNECT_TIMEOUT_SEC)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS};")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


@dataclass
class SyncStats:
    total: int = 0
    synced: int = 0
    failed: int = 0
    skipped: int = 0


def now_str() -> str:
    return datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")


def parse_active_hours(value: str) -> Optional[tuple[int, int]]:
    v = str(value or "").strip()
    if not v:
        return None
    parts = v.split("-", 1)
    if len(parts) != 2:
        raise ValueError("invalid_active_hours_format")
    start_str, end_str = parts[0].strip(), parts[1].strip()
    if not start_str.isdigit() or not end_str.isdigit():
        raise ValueError("invalid_active_hours_format")
    start = int(start_str)
    end = int(end_str)
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
    next_retry = (datetime.now(CST) + timedelta(minutes=retry_minutes)).strftime("%Y-%m-%d %H:%M:%S")
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


def mark_connect_error_for_pending_posts(
    *,
    local_db: Path,
    batch_size: int,
    error: str,
    verbose: bool,
) -> int:
    updated = 0
    conn = connect_workdb(local_db)
    try:
        rows = list(select_pending_posts(conn, max(1, int(batch_size))))
        if not rows:
            return 0
        for row in rows:
            attempts = int(row["sync_attempts"] or 0) + 1
            mark_sync_error(conn, row["post_uid"], attempts, error)
            updated += 1
        conn.commit()
        if updated and verbose:
            print(f"[sync] marked_connect_error={updated}", flush=True)
        return updated
    finally:
        conn.close()


def sync_once(
    *,
    local_db: Path,
    engine: Engine,
    batch_size: int,
    verbose: bool,
) -> SyncStats:
    stats = SyncStats()
    conn = connect_workdb(local_db)
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
                mark_sync_error(conn, row["post_uid"], attempts, f"turso:{type(e).__name__}: {e}")
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
    parser.add_argument(
        "--active-hours",
        default="",
        help="只在这些小时运行（CST），格式: 6-22；为空表示全天。",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    init_workdb(Path(args.local_db))

    active_hours_value = args.active_hours.strip() if args.active_hours else os.getenv("SYNC_ACTIVE_HOURS", "").strip()
    active_hours: Optional[tuple[int, int]] = None
    if active_hours_value:
        try:
            active_hours = parse_active_hours(active_hours_value)
        except ValueError as e:
            raise RuntimeError(f"Invalid active hours: {active_hours_value} ({e})") from e

    local_db = Path(args.local_db)
    interval = float(args.interval_seconds or 0.0)
    if interval > 0:
        interval = max(1.0, interval)

    engine: Optional[Engine] = None

    def _ensure_engine() -> Optional[Engine]:
        nonlocal engine
        if engine is not None:
            return engine
        try:
            engine = ensure_turso_engine(args.turso_url, args.turso_token)
            init_cloud_schema(engine)
            return engine
        except Exception as e:
            engine = None
            error = f"turso:connect:{type(e).__name__}: {e}"
            try:
                mark_connect_error_for_pending_posts(
                    local_db=local_db,
                    batch_size=max(1, int(args.batch_size)),
                    error=error,
                    verbose=bool(args.verbose),
                )
            except Exception as mark_e:
                print(f"[sync] mark_connect_error_failed {type(mark_e).__name__}: {mark_e}", flush=True)
            if args.verbose:
                print(f"[sync] connect_error {type(e).__name__}: {e}", flush=True)
            return None

    if args.interval_seconds and args.interval_seconds > 0:
        loop_idx = 0
        try:
            while True:
                loop_idx += 1
                if active_hours is not None:
                    sleep_until_active(active_hours, verbose=bool(args.verbose))
                if _ensure_engine() is None:
                    time.sleep(interval)
                    continue
                stats = sync_once(
                    local_db=local_db,
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
                time.sleep(interval)
        except KeyboardInterrupt:
            print("[sync] stopped by user", flush=True)
    else:
        if active_hours is not None:
            sleep_until_active(active_hours, verbose=bool(args.verbose))
        if _ensure_engine() is None:
            raise RuntimeError("turso_connect_failed")
        stats = sync_once(
            local_db=local_db,
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
