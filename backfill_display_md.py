from __future__ import annotations

import argparse
import time

from sqlalchemy import text

from turso_db import get_turso_engine_from_env
from turso_queue import ensure_cloud_queue_schema
from weibo_display import format_weibo_display_md


DEFAULT_BATCH_SIZE = 200


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill posts.display_md from posts.raw_text")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--limit", type=int, default=0, help="最多处理多少条（0=不限）")
    parser.add_argument("--sleep-sec", type=float, default=0.0, help="每批之间 sleep 秒数（默认 0）")
    parser.add_argument("--overwrite", action="store_true", help="覆盖已有 display_md（默认只补空的）")
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


def main() -> None:
    args = parse_args()
    engine = get_turso_engine_from_env()

    ensure_cloud_queue_schema(engine, verbose=bool(args.verbose))

    processed = 0
    updated = 0

    last_post_uid = ""
    stop_post_uid = ""
    with engine.connect() as conn:
        total_posts = int(conn.execute(text("SELECT COUNT(*) FROM posts")).scalar() or 0)
        target_total = _count_targets(conn, overwrite=bool(args.overwrite))
        stop_post_uid = _max_target_post_uid(conn, overwrite=bool(args.overwrite))

    if not stop_post_uid or target_total <= 0:
        print(f"[backfill] nothing_to_do total_posts={total_posts} target={target_total}")
        return

    print(f"[backfill] start total_posts={total_posts} target={target_total} overwrite={bool(args.overwrite)}")

    while True:
        remaining = None
        if int(args.limit or 0) > 0:
            remaining = max(0, int(args.limit) - processed)
            if remaining <= 0:
                break

        with engine.connect() as conn:
            batch_size = int(args.batch_size)
            if remaining is not None:
                batch_size = min(batch_size, remaining)
            rows = _select_batch(
                conn,
                batch_size=batch_size,
                overwrite=bool(args.overwrite),
                last_post_uid=last_post_uid,
                stop_post_uid=stop_post_uid,
            )

        if not rows:
            break

        updates: list[dict] = []
        for row in rows:
            post_uid = str(row.get("post_uid") or "").strip()
            author = str(row.get("author") or "").strip()
            raw_text = str(row.get("raw_text") or "")
            if not post_uid:
                continue
            display_md = format_weibo_display_md(raw_text, author=author)
            updates.append({"post_uid": post_uid, "display_md": display_md})

        # advance cursor (works for both overwrite and non-overwrite, including dry-run)
        last_row = rows[-1]
        last_post_uid = str(last_row.get("post_uid") or last_post_uid)

        processed += len(rows)

        if args.dry_run:
            print(f"[dry-run] batch rows={len(rows)} total_processed={processed}")
        else:
            with engine.begin() as conn:
                updated_this_batch = 0
                for item in updates:
                    if bool(args.overwrite):
                        res = conn.execute(
                            text(
                                """
                                UPDATE posts
                                SET display_md = :display_md
                                WHERE post_uid = :post_uid
                                """
                            ),
                            item,
                        )
                    else:
                        res = conn.execute(
                            text(
                                """
                                UPDATE posts
                                SET display_md = :display_md
                                WHERE post_uid = :post_uid
                                  AND (display_md IS NULL OR TRIM(display_md) = '')
                                """
                            ),
                            item,
                        )
                    updated_this_batch += int(res.rowcount or 0)
            updated += updated_this_batch
            print(
                f"[backfill] batch updated={updated_this_batch} "
                f"total_updated={updated} total_processed={processed}"
            )

        if float(args.sleep_sec or 0.0) > 0:
            time.sleep(float(args.sleep_sec))

    print(f"[backfill] done updated={updated} processed={processed}")


if __name__ == "__main__":
    main()
