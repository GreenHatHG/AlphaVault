from __future__ import annotations

import argparse
import re

from sqlalchemy import text

from alphavault.env import load_dotenv_if_present

from alphavault.db.turso_db import get_turso_engine_from_env, turso_connect_autocommit
from alphavault.db.turso_queue import (
    ensure_cloud_queue_schema,
    reset_ai_results_all,
    reset_ai_results_for_post_uids,
)
from alphavault.rss.utils import now_str


DEFAULT_CHUNK_SIZE = 200


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reset AI queue state: set posts back to pending (assertions kept until worker overwrites)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="全量重置（不删 assertions，只把 posts 标记为待处理）",
    )
    parser.add_argument(
        "--post-uids",
        type=str,
        default="",
        help="只重置这些 post_uid（逗号/换行分隔；纯数字会自动加 weibo:）",
    )
    parser.add_argument(
        "--yes", action="store_true", help="配合 --all 使用，确认你真的要全量重置"
    )
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--dry-run", action="store_true", help="只打印，不写 DB")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


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


def _select_existing_post_uids(engine, post_uids: list[str]) -> set[str]:
    if not post_uids:
        return set()
    placeholders = ", ".join([f":uid{i}" for i in range(len(post_uids))])
    params = {f"uid{i}": uid for i, uid in enumerate(post_uids)}
    query = f"SELECT post_uid FROM posts WHERE post_uid IN ({placeholders})"
    with turso_connect_autocommit(engine) as conn:
        rows = conn.execute(text(query), params).fetchall()
    return {str(r[0]) for r in rows if r and r[0]}


def main() -> None:
    load_dotenv_if_present()
    args = parse_args()
    if args.all and str(args.post_uids or "").strip():
        raise SystemExit("参数冲突：--all 和 --post-uids 只能选一个")
    if not args.all and not str(args.post_uids or "").strip():
        raise SystemExit("缺参数：要么用 --all，要么用 --post-uids")
    if args.all and not args.yes:
        raise SystemExit("全量重置太危险：请加 --yes 确认")

    engine = get_turso_engine_from_env()
    ensure_cloud_queue_schema(engine, verbose=bool(args.verbose))
    archived_at = now_str()

    if args.all:
        with turso_connect_autocommit(engine) as conn:
            total_posts = int(
                conn.execute(text("SELECT COUNT(*) FROM posts")).scalar() or 0
            )
            total_assertions = int(
                conn.execute(text("SELECT COUNT(*) FROM assertions")).scalar() or 0
            )

        print(
            f"[reset] plan all=1 posts={total_posts} assertions={total_assertions} keep_assertions=1 dry_run={int(bool(args.dry_run))}",
            flush=True,
        )
        if args.dry_run:
            return

        deleted, updated = reset_ai_results_all(engine, archived_at=archived_at)
        print(
            f"[reset] done all=1 deleted_assertions={deleted} updated_posts={updated}",
            flush=True,
        )
        return

    post_uids = _parse_post_uids(args.post_uids)
    existing = _select_existing_post_uids(engine, post_uids)
    missing = [uid for uid in post_uids if uid not in existing]
    targets = [uid for uid in post_uids if uid in existing]

    print(
        " ".join(
            [
                "[reset] plan",
                "all=0",
                f"target={len(targets)}",
                f"missing={len(missing)}",
                f"dry_run={int(bool(args.dry_run))}",
            ]
        ),
        flush=True,
    )
    if missing and args.verbose:
        head = ", ".join(missing[:10])
        tail = "" if len(missing) <= 10 else f" ... (+{len(missing) - 10})"
        print(f"[reset] missing_post_uids={len(missing)} {head}{tail}", flush=True)

    if args.dry_run or not targets:
        return

    deleted, updated = reset_ai_results_for_post_uids(
        engine,
        post_uids=targets,
        archived_at=archived_at,
        chunk_size=max(1, int(args.chunk_size)),
    )
    print(
        f"[reset] done all=0 deleted_assertions={deleted} updated_posts={updated}",
        flush=True,
    )


if __name__ == "__main__":
    main()
