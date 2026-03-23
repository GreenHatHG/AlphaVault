from __future__ import annotations

import argparse

from sqlalchemy import text

from alphavault.env import load_dotenv_if_present

load_dotenv_if_present()

from alphavault.ai.tag_validate import validate_assertion_row
from alphavault.ai.topic_prompt_v3 import TOPIC_PROMPT_VERSION
from alphavault.db.turso_db import get_turso_engine_from_env
from alphavault.db.turso_queue import ensure_cloud_queue_schema, reset_ai_results_for_post_uids
from alphavault.rss.utils import now_str


DEFAULT_CHUNK_SIZE = 200


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan invalid AI tags in DB, then reset posts to rerun AI.")
    parser.add_argument(
        "--prompt-version",
        type=str,
        default="",
        help="只扫描这个 prompt_version（默认扫全部；可选：topic-prompt-v3 / weibo_assertions_v1）",
    )
    parser.add_argument("--limit", type=int, default=0, help="最多 reset 多少个 post（0=不限）")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--dry-run", action="store_true", help="只打印，不写 DB")
    parser.add_argument("--yes", action="store_true", help="非 dry-run 时必须加 --yes 确认")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _scan_invalid_post_uids(
    engine,
    *,
    prompt_version: str,
    limit: int,
    verbose: bool,
) -> tuple[list[str], dict[str, str], int]:
    query = """
        SELECT
            p.post_uid AS post_uid,
            COALESCE(p.prompt_version, '') AS prompt_version,
            a.idx AS idx,
            a.topic_key AS topic_key,
            a.action AS action,
            a.action_strength AS action_strength,
            a.confidence AS confidence,
            a.stock_codes_json AS stock_codes_json,
            a.stock_names_json AS stock_names_json,
            a.industries_json AS industries_json,
            a.commodities_json AS commodities_json,
            a.indices_json AS indices_json
        FROM posts p
        JOIN assertions a
          ON a.post_uid = p.post_uid
        WHERE p.processed_at IS NOT NULL
        ORDER BY p.post_uid ASC, a.idx ASC
        """
    params = {"prompt_version": str(prompt_version or "").strip()}
    if params["prompt_version"]:
        query = query.replace(
            "WHERE p.processed_at IS NOT NULL",
            "WHERE p.processed_at IS NOT NULL AND COALESCE(p.prompt_version, '') = :prompt_version",
        )

    invalid_post_uids: list[str] = []
    first_error_by_uid: dict[str, str] = {}
    scanned_rows = 0

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings()
        for row in rows:
            scanned_rows += 1
            uid = str(row.get("post_uid") or "").strip()
            if not uid:
                continue
            if uid in first_error_by_uid:
                continue
            try:
                row_prompt_version = str(row.get("prompt_version") or "").strip()
                validate_assertion_row(dict(row), prompt_version=row_prompt_version)
            except Exception as exc:
                idx = str(row.get("idx") or "").strip()
                first_error_by_uid[uid] = f"idx={idx} {type(exc).__name__}:{exc}"
                invalid_post_uids.append(uid)
                if limit > 0 and len(invalid_post_uids) >= int(limit):
                    break

    if verbose and invalid_post_uids:
        for uid in invalid_post_uids[:10]:
            print(f"[scan_reset] bad post_uid={uid} {first_error_by_uid.get(uid,'')}", flush=True)
        if len(invalid_post_uids) > 10:
            print(f"[scan_reset] bad_post_uids_more count={len(invalid_post_uids) - 10}", flush=True)

    return invalid_post_uids, first_error_by_uid, scanned_rows


def main() -> None:
    args = parse_args()
    prompt_version = str(args.prompt_version or "").strip()
    if not args.dry_run and not args.yes:
        raise SystemExit("危险操作：非 dry-run 请加 --yes 确认")

    engine = get_turso_engine_from_env()
    ensure_cloud_queue_schema(engine, verbose=bool(args.verbose))

    limit = max(0, int(args.limit))
    invalid_uids, _errors, scanned_rows = _scan_invalid_post_uids(
        engine,
        prompt_version=prompt_version,
        limit=limit,
        verbose=bool(args.verbose),
    )

    print(
        " ".join(
            [
                "[scan_reset] plan",
                f"prompt_version={prompt_version}",
                f"scanned_rows={scanned_rows}",
                f"bad_posts={len(invalid_uids)}",
                f"dry_run={1 if args.dry_run else 0}",
            ]
        ),
        flush=True,
    )

    if args.dry_run or not invalid_uids:
        return

    archived_at = now_str()
    _deleted, updated = reset_ai_results_for_post_uids(
        engine,
        post_uids=invalid_uids,
        archived_at=archived_at,
        chunk_size=max(1, int(args.chunk_size)),
    )
    print(f"[scan_reset] done bad_posts={len(invalid_uids)} updated_posts={updated}", flush=True)


if __name__ == "__main__":
    main()
