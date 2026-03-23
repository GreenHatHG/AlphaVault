from __future__ import annotations

import argparse
import re
import time

from sqlalchemy import text

from alphavault.env import load_dotenv_if_present

load_dotenv_if_present()

from alphavault.db.turso_db import get_turso_engine_from_env, turso_connect_autocommit, turso_savepoint
from alphavault.db.turso_queue import ensure_cloud_queue_schema
from alphavault.weibo.display import format_weibo_display_md


DEFAULT_BATCH_SIZE = 200


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill posts.display_md from posts.raw_text")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--limit", type=int, default=0, help="最多处理多少条（0=不限）")
    parser.add_argument("--sleep-sec", type=float, default=0.0, help="每批之间 sleep 秒数（默认 0）")
    parser.add_argument("--overwrite", action="store_true", help="覆盖已有 display_md（默认只补空的）")
    parser.add_argument(
        "--post-uids",
        type=str,
        default="",
        help="只处理指定 post_uid（逗号/换行分隔）。提供后将强制覆盖重算 display_md。",
    )
    parser.add_argument("--dry-run", action="store_true", help="只打印进度，不写 DB")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _select_batch(
    conn,
    *,
    batch_size: int,
    overwrite: bool,
    last_post_uid: str,
    stop_post_uid: str,
) -> list[dict]:
    limit = max(1, int(batch_size))
    if not overwrite:
        query = """
            SELECT post_uid, author, raw_text
            FROM posts
            WHERE (display_md IS NULL OR TRIM(display_md) = '')
              AND TRIM(raw_text) <> ''
              AND post_uid > :last_post_uid
              AND post_uid <= :stop_post_uid
            ORDER BY post_uid ASC
            LIMIT :limit
            """
        params = {
            "limit": limit,
            "last_post_uid": str(last_post_uid or ""),
            "stop_post_uid": str(stop_post_uid or ""),
        }
    else:
        query = """
            SELECT post_uid, author, raw_text
            FROM posts
            WHERE TRIM(raw_text) <> ''
              AND post_uid > :last_post_uid
              AND post_uid <= :stop_post_uid
            ORDER BY post_uid ASC
            LIMIT :limit
            """
        params = {
            "limit": limit,
            "last_post_uid": str(last_post_uid or ""),
            "stop_post_uid": str(stop_post_uid or ""),
        }

    rows = conn.execute(text(query), params).mappings().fetchall()
    return [dict(row) for row in rows]


def _count_targets(conn, *, overwrite: bool) -> int:
    if overwrite:
        query = "SELECT COUNT(*) FROM posts WHERE TRIM(raw_text) <> ''"
    else:
        query = """
            SELECT COUNT(*)
            FROM posts
            WHERE (display_md IS NULL OR TRIM(display_md) = '')
              AND TRIM(raw_text) <> ''
            """
    return int(conn.execute(text(query)).scalar() or 0)


def _max_target_post_uid(conn, *, overwrite: bool) -> str:
    if overwrite:
        query = "SELECT MAX(post_uid) FROM posts WHERE TRIM(raw_text) <> ''"
    else:
        query = """
            SELECT MAX(post_uid)
            FROM posts
            WHERE (display_md IS NULL OR TRIM(display_md) = '')
              AND TRIM(raw_text) <> ''
            """
    return str(conn.execute(text(query)).scalar() or "").strip()

def _parse_post_uids(value: str) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    items: list[str] = []
    seen: set[str] = set()
    for part in re.split(r"[,\n]+", raw):
        uid = str(part or "").strip()
        if not uid:
            continue
        if uid.isdigit():
            uid = f"weibo:{uid}"
        if uid in seen:
            continue
        seen.add(uid)
        items.append(uid)
    return items


def _select_rows_by_post_uids(conn, post_uids: list[str]) -> list[dict]:
    if not post_uids:
        return []
    placeholders = ", ".join([f":uid{i}" for i in range(len(post_uids))])
    query = f"""
        SELECT post_uid, author, raw_text
        FROM posts
        WHERE post_uid IN ({placeholders})
          AND TRIM(raw_text) <> ''
        ORDER BY post_uid ASC
        """
    params = {f"uid{i}": uid for i, uid in enumerate(post_uids)}
    rows = conn.execute(text(query), params).mappings().fetchall()
    return [dict(row) for row in rows]


def _chunks(items: list[str], size: int) -> list[list[str]]:
    n = max(1, int(size))
    return [items[i : i + n] for i in range(0, len(items), n)]


def _build_updates(rows: list[dict]) -> tuple[list[dict], set[str]]:
    updates: list[dict] = []
    found_uids: set[str] = set()
    for row in rows:
        post_uid = str(row.get("post_uid") or "").strip()
        author = str(row.get("author") or "").strip()
        raw_text = str(row.get("raw_text") or "")
        if not post_uid:
            continue
        display_md = format_weibo_display_md(raw_text, author=author)
        updates.append({"post_uid": post_uid, "display_md": display_md})
        found_uids.add(post_uid)
    return updates, found_uids


def _update_batch(conn, *, updates: list[dict], overwrite: bool) -> int:
    query = """
        UPDATE posts
        SET display_md = :display_md
        WHERE post_uid = :post_uid
        """
    if not overwrite:
        query = (
            query
            + """
              AND (display_md IS NULL OR TRIM(display_md) = '')
            """
        )

    updated = 0
    for item in updates:
        res = conn.execute(text(query), item)
        updated += int(res.rowcount or 0)
    return updated


def _backfill_by_post_uids(engine, *, post_uids: list[str], batch_size: int, dry_run: bool, sleep_sec: float, verbose: bool) -> None:
    processed = 0
    updated = 0
    found_uids: set[str] = set()

    with turso_connect_autocommit(engine) as conn:
        total_posts = int(conn.execute(text("SELECT COUNT(*) FROM posts")).scalar() or 0)

    print(f"[backfill] start total_posts={total_posts} target={len(post_uids)} overwrite=True by_post_uids=True")

    for chunk in _chunks(post_uids, batch_size):
        with turso_connect_autocommit(engine) as conn:
            rows = _select_rows_by_post_uids(conn, chunk)

        updates, found_this_batch = _build_updates(rows)
        found_uids.update(found_this_batch)
        processed += len(rows)

        if dry_run:
            print(f"[dry-run] batch rows={len(rows)} total_processed={processed}")
            continue

        with turso_connect_autocommit(engine) as conn:
            with turso_savepoint(conn):
                updated_this_batch = _update_batch(conn, updates=updates, overwrite=True)
        updated += updated_this_batch
        print(f"[backfill] batch updated={updated_this_batch} total_updated={updated} total_processed={processed}")

        if sleep_sec > 0:
            time.sleep(float(sleep_sec))

    missing = [uid for uid in post_uids if uid not in found_uids]
    if missing and verbose:
        head = ", ".join(missing[:10])
        tail = "" if len(missing) <= 10 else f" ... (+{len(missing) - 10})"
        print(f"[backfill] missing_post_uids={len(missing)} {head}{tail}")

    print(f"[backfill] done updated={updated} processed={processed} target={len(post_uids)}")


def _backfill_scan(engine, *, batch_size: int, limit: int, sleep_sec: float, overwrite: bool, dry_run: bool, verbose: bool) -> None:
    processed = 0
    updated = 0
    last_post_uid = ""

    with turso_connect_autocommit(engine) as conn:
        total_posts = int(conn.execute(text("SELECT COUNT(*) FROM posts")).scalar() or 0)
        target_total = _count_targets(conn, overwrite=bool(overwrite))
        stop_post_uid = _max_target_post_uid(conn, overwrite=bool(overwrite))

    if not stop_post_uid or target_total <= 0:
        print(f"[backfill] nothing_to_do total_posts={total_posts} target={target_total}")
        return

    print(f"[backfill] start total_posts={total_posts} target={target_total} overwrite={bool(overwrite)}")

    while True:
        remaining = None
        if limit > 0:
            remaining = max(0, int(limit) - processed)
            if remaining <= 0:
                break

        with turso_connect_autocommit(engine) as conn:
            effective_batch_size = int(batch_size)
            if remaining is not None:
                effective_batch_size = min(effective_batch_size, remaining)
            rows = _select_batch(
                conn,
                batch_size=effective_batch_size,
                overwrite=bool(overwrite),
                last_post_uid=last_post_uid,
                stop_post_uid=stop_post_uid,
            )

        if not rows:
            break

        updates, _found = _build_updates(rows)

        last_row = rows[-1]
        last_post_uid = str(last_row.get("post_uid") or last_post_uid)
        processed += len(rows)

        if dry_run:
            print(f"[dry-run] batch rows={len(rows)} total_processed={processed}")
        else:
            with turso_connect_autocommit(engine) as conn:
                with turso_savepoint(conn):
                    updated_this_batch = _update_batch(conn, updates=updates, overwrite=bool(overwrite))
            updated += updated_this_batch
            print(f"[backfill] batch updated={updated_this_batch} total_updated={updated} total_processed={processed}")

        if sleep_sec > 0:
            time.sleep(float(sleep_sec))

    print(f"[backfill] done updated={updated} processed={processed}")


def main() -> None:
    args = parse_args()
    engine = get_turso_engine_from_env()

    ensure_cloud_queue_schema(engine, verbose=bool(args.verbose))

    post_uids = _parse_post_uids(getattr(args, "post_uids", ""))
    if post_uids:
        _backfill_by_post_uids(
            engine,
            post_uids=post_uids,
            batch_size=max(1, int(args.batch_size)),
            dry_run=bool(args.dry_run),
            sleep_sec=float(args.sleep_sec or 0.0),
            verbose=bool(args.verbose),
        )
        return

    _backfill_scan(
        engine,
        batch_size=max(1, int(args.batch_size)),
        limit=int(args.limit or 0),
        sleep_sec=float(args.sleep_sec or 0.0),
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
        verbose=bool(args.verbose),
    )


if __name__ == "__main__":
    main()
