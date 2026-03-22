from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from alphavault.constants import (
    DEFAULT_SPOOL_DIR,
    ENV_REDIS_URL,
    ENV_SPOOL_DIR,
    ENV_TURSO_AUTH_TOKEN,
    ENV_TURSO_DATABASE_URL,
)
from alphavault.db.turso_db import ensure_turso_engine
from alphavault.env import load_dotenv_if_present

load_dotenv_if_present()


HEALTHCHECK_TABLE = "__alphavault_healthcheck"


def _print(msg: str) -> None:
    print(msg, flush=True)


def _eprint(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _spool_dir_value() -> str:
    return os.getenv(ENV_SPOOL_DIR, "").strip() or DEFAULT_SPOOL_DIR


def _check_spool_dir() -> None:
    path = Path(_spool_dir_value())
    _print(f"[healthcheck] spool start dir={path}")

    try:
        path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise RuntimeError(f"spool mkdir failed: {type(e).__name__}: {e}") from e

    if not path.exists() or not path.is_dir():
        raise RuntimeError("spool not a directory")

    # Simple write test.
    test_path = path / f".healthcheck.{os.getpid()}.{int(time.time())}.tmp"
    try:
        test_path.write_text("ok\n", encoding="utf-8")
        test_path.unlink(missing_ok=True)
    except Exception as e:
        try:
            test_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise RuntimeError(f"spool write failed: {type(e).__name__}: {e}") from e

    _print("[healthcheck] spool ok")


def _get_turso_engine_from_env() -> Engine:
    url = os.getenv(ENV_TURSO_DATABASE_URL, "").strip()
    token = os.getenv(ENV_TURSO_AUTH_TOKEN, "").strip()
    if not url:
        raise RuntimeError(f"missing {ENV_TURSO_DATABASE_URL}")
    return ensure_turso_engine(url, token)


def _check_turso() -> None:
    _print("[healthcheck] turso start")
    engine = _get_turso_engine_from_env()

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).fetchone()
    except Exception as e:
        raise RuntimeError(f"turso connect failed: {type(e).__name__}: {e}") from e

    try:
        note = f"pid={os.getpid()} ts={int(time.time())}"
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    CREATE TABLE IF NOT EXISTS {HEALTHCHECK_TABLE} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at INTEGER NOT NULL,
                        note TEXT NOT NULL
                    )
                    """
                )
            )
            conn.execute(
                text(f"INSERT INTO {HEALTHCHECK_TABLE}(created_at, note) VALUES (:ts, :note)"),
                {"ts": int(time.time()), "note": note},
            )
            inserted_id = conn.execute(text("SELECT last_insert_rowid()")).scalar_one()
            conn.execute(
                text(f"DELETE FROM {HEALTHCHECK_TABLE} WHERE id = :id"),
                {"id": int(inserted_id)},
            )
    except Exception as e:
        raise RuntimeError(f"turso write failed: {type(e).__name__}: {e}") from e

    _print("[healthcheck] turso ok")


def _try_check_redis(redis_url: str) -> None:
    try:
        import redis  # type: ignore
    except Exception as e:
        raise RuntimeError(f"redis import failed: {type(e).__name__}: {e}") from e

    try:
        client = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        client.ping()
    except Exception as e:
        raise RuntimeError(f"redis connect failed: {type(e).__name__}: {e}") from e

    key = f"alphavault:healthcheck:{os.getpid()}:{int(time.time())}"
    try:
        ok = client.set(key, "1", ex=30)
        if ok is not True:
            raise RuntimeError("redis set returned false")
        client.delete(key)
    except Exception as e:
        raise RuntimeError(f"redis write failed: {type(e).__name__}: {e}") from e


def _check_redis() -> None:
    redis_url = os.getenv(ENV_REDIS_URL, "").strip()
    if not redis_url:
        _print(f"[healthcheck] redis skip ({ENV_REDIS_URL} empty)")
        return

    _print("[healthcheck] redis start")
    _try_check_redis(redis_url)
    _print("[healthcheck] redis ok")


def run_startup_healthcheck() -> None:
    _check_spool_dir()
    _check_turso()
    _check_redis()


def main(argv: list[str]) -> int:
    if len(argv) > 1:
        _eprint("[healthcheck] error: no args supported")
        return 2
    try:
        run_startup_healthcheck()
    except Exception as e:
        _eprint(f"[healthcheck] failed: {e}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
