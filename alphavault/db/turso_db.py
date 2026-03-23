from __future__ import annotations

import os
import re
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from alphavault.constants import ENV_TURSO_AUTH_TOKEN, ENV_TURSO_DATABASE_URL
# NOTE: This module is extracted from the old local-sqlite sync scripts.
# It keeps only Turso engine creation + base schema (posts/assertions).

TOPIC_CLUSTERS_TABLE = "topic_clusters"
TOPIC_CLUSTER_TOPICS_TABLE = "topic_cluster_topics"
TOPIC_CLUSTER_POST_OVERRIDES_TABLE = "topic_cluster_post_overrides"

TURSO_AUTOCOMMIT_ISOLATION_LEVEL = "AUTOCOMMIT"
TURSO_SAVEPOINT_NAME = "alphavault_sp"
_SAVEPOINT_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SQLALCHEMY_DIALECT_PATCH_FLAG = "_alphavault_turso_dialect_patched"
_FATAL_BASE_EXCEPTIONS = (KeyboardInterrupt, SystemExit, GeneratorExit)

TOPIC_CLUSTER_TOPICS_V2_TABLE = f"{TOPIC_CLUSTER_TOPICS_TABLE}_v2"


def is_turso_stream_not_found_error(err: BaseException) -> bool:
    """
    Detect the transient Hrana error: "stream not found" (HTTP 404).

    This usually means the client is trying to reuse an old stream id.
    Disposing the SQLAlchemy engine pool typically fixes it.
    """
    needle = "stream not found"
    current: BaseException | None = err
    for _ in range(8):
        if current is None:
            break
        try:
            msg = str(current)
        except Exception:
            msg = ""
        if needle in msg.lower():
            return True
        # SQLAlchemy often wraps the original exception in `.orig`.
        orig = getattr(current, "orig", None)
        if isinstance(orig, BaseException) and orig is not current:
            current = orig
            continue
        current = (
            current.__cause__
            if isinstance(current.__cause__, BaseException)
            else current.__context__
        )
        if not isinstance(current, BaseException):
            current = None
    return False


def is_turso_libsql_panic_error(err: BaseException) -> bool:
    """
    Detect libsql/pyo3 panic surfaced as a Python BaseException.

    This is NOT a normal Exception and may bypass `except Exception`.
    Typical message: "called `Option::unwrap()` on a `None` value".
    """
    current: BaseException | None = err
    for _ in range(10):
        if current is None:
            break
        t = type(current)
        if getattr(t, "__name__", "") == "PanicException":
            mod = str(getattr(t, "__module__", "") or "")
            if "pyo3" in mod or "pyo3_runtime" in mod:
                return True
        try:
            msg = str(current)
        except BaseException as e:
            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                raise
            msg = ""
        msg_lower = msg.lower()
        if "option::unwrap" in msg_lower:
            return True
        if "panicexception" in msg_lower and "pyo3" in msg_lower:
            return True
        # SQLAlchemy often wraps the original exception in `.orig`.
        orig = getattr(current, "orig", None)
        if isinstance(orig, BaseException) and orig is not current:
            current = orig
            continue
        current = (
            current.__cause__
            if isinstance(current.__cause__, BaseException)
            else current.__context__
        )
        if not isinstance(current, BaseException):
            current = None
    return False


def turso_connect_autocommit(engine: Engine) -> Connection:
    """
    Return a Connection that behaves like AUTOCOMMIT for Turso (libsql).

    Why:
    - Avoid DBAPI commit()/rollback(), which may panic in some libsql builds.
    - SQLAlchemy may call rollback() on close for cleanup; avoid that path too.
    - Some libsql DBAPI connections expose a read-only `isolation_level`, which breaks
      SQLAlchemy's default SQLite isolation_level setter.
    """
    dialect = getattr(engine, "dialect", None)
    if dialect is not None and not getattr(
        dialect, _SQLALCHEMY_DIALECT_PATCH_FLAG, False
    ):
        original_do_rollback = getattr(dialect, "do_rollback", None)
        original_set_isolation_level = getattr(dialect, "set_isolation_level", None)

        if callable(original_do_rollback):

            def _patched_do_rollback(dbapi_connection):  # type: ignore[no-untyped-def]
                # NOTE: SQLAlchemy may issue a rollback() on connection close even if the
                # application never started a transaction (cleanup behavior).
                #
                # Some libsql builds may panic on DBAPI rollback(), so we avoid calling it.
                try:
                    cursor = dbapi_connection.cursor()
                except BaseException as e:
                    if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                        raise
                    cursor = None

                if cursor is not None:
                    try:
                        cursor.execute("ROLLBACK")
                    except BaseException as e:
                        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                            raise
                        # Cleanup rollback must never crash the worker.
                        pass
                    finally:
                        try:
                            cursor.close()
                        except BaseException as e:
                            if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                                raise
                            pass
                    return None

                execute = getattr(dbapi_connection, "execute", None)
                if callable(execute):
                    try:
                        execute("ROLLBACK")
                    except BaseException as e:
                        if isinstance(e, _FATAL_BASE_EXCEPTIONS):
                            raise
                        pass
                return None

        if callable(original_set_isolation_level):

            def _patched_set_isolation_level(dbapi_connection, level):  # type: ignore[no-untyped-def]
                try:
                    return original_set_isolation_level(dbapi_connection, level)
                except AttributeError as exc:
                    # libsql may expose a read-only Connection.isolation_level
                    # This can happen both when setting AUTOCOMMIT and when SQLAlchemy resets the connection
                    # characteristics on close / return-to-pool.
                    if "isolation_level" in str(exc):
                        return None
                    raise

            dialect.set_isolation_level = _patched_set_isolation_level  # type: ignore[assignment]
        if callable(original_do_rollback):
            dialect.do_rollback = _patched_do_rollback  # type: ignore[assignment]
        setattr(dialect, _SQLALCHEMY_DIALECT_PATCH_FLAG, True)

    return engine.connect().execution_options(
        isolation_level=TURSO_AUTOCOMMIT_ISOLATION_LEVEL
    )


@contextmanager
def turso_savepoint(conn: Connection, name: str = TURSO_SAVEPOINT_NAME) -> Connection:
    """
    Keep the old helper API, but use a SQL transaction instead of SAVEPOINT.

    Why:
    - Some libsql/Hrana builds lose SAVEPOINT state across AUTOCOMMIT executes and then fail
      with "no such savepoint" on RELEASE/ROLLBACK TO.
    - Sending SQL BEGIN/COMMIT/ROLLBACK keeps us away from DBAPI commit()/rollback(), which is
      the original reason this helper exists.
    """
    savepoint = str(name or "").strip()
    if not savepoint or _SAVEPOINT_NAME_RE.fullmatch(savepoint) is None:
        raise ValueError("invalid_savepoint_name")

    conn.execute(text("BEGIN"))
    try:
        yield conn
    except Exception:
        try:
            conn.execute(text("ROLLBACK"))
        except Exception:
            # Preserve the original application error if rollback itself also fails.
            pass
        raise
    else:
        conn.execute(text("COMMIT"))


def _topic_cluster_topics_pk_cols(conn) -> list[str]:
    try:
        rows = (
            conn.execute(text(f"PRAGMA table_info({TOPIC_CLUSTER_TOPICS_TABLE})"))
            .mappings()
            .all()
        )
    except Exception:
        return []
    if not rows:
        return []
    ordered = sorted(rows, key=lambda r: int(r.get("pk") or 0))
    cols = [
        str(r.get("name") or "").strip() for r in ordered if int(r.get("pk") or 0) > 0
    ]
    return [c for c in cols if c]


def _migrate_topic_cluster_topics_to_v2(conn) -> None:
    """
    Migrate v1 schema -> v2 schema (topic_key PK -> (topic_key, cluster_key) PK).

    Keep it simple and safe:
    - create a new table
    - copy old data
    - verify row count
    - swap tables
    """
    # Clean any leftover temp table from a previous failed attempt.
    conn.execute(text(f"DROP TABLE IF EXISTS {TOPIC_CLUSTER_TOPICS_V2_TABLE}"))

    conn.execute(
        text(
            f"""
            CREATE TABLE {TOPIC_CLUSTER_TOPICS_V2_TABLE} (
                topic_key TEXT NOT NULL,
                cluster_key TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'manual',
                confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
                created_at TEXT NOT NULL,
                PRIMARY KEY (topic_key, cluster_key)
            );
            """
        )
    )

    conn.execute(
        text(
            f"""
            INSERT OR IGNORE INTO {TOPIC_CLUSTER_TOPICS_V2_TABLE}(
                topic_key, cluster_key, source, confidence, created_at
            )
            SELECT topic_key, cluster_key, source, confidence, created_at
            FROM {TOPIC_CLUSTER_TOPICS_TABLE}
            """
        )
    )

    old_n = int(
        (
            conn.execute(
                text(f"SELECT COUNT(1) AS n FROM {TOPIC_CLUSTER_TOPICS_TABLE}")
            )
            .mappings()
            .first()
            or {}
        ).get("n")
        or 0
    )
    new_n = int(
        (
            conn.execute(
                text(f"SELECT COUNT(1) AS n FROM {TOPIC_CLUSTER_TOPICS_V2_TABLE}")
            )
            .mappings()
            .first()
            or {}
        ).get("n")
        or 0
    )
    if new_n < old_n:
        raise RuntimeError(
            f"topic_cluster_topics migrate failed: copied_rows={new_n} < old_rows={old_n}"
        )

    conn.execute(text(f"DROP TABLE {TOPIC_CLUSTER_TOPICS_TABLE}"))
    conn.execute(
        text(
            f"ALTER TABLE {TOPIC_CLUSTER_TOPICS_V2_TABLE} RENAME TO {TOPIC_CLUSTER_TOPICS_TABLE}"
        )
    )


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
        pool_pre_ping=True,
        # Avoid calling DBAPI rollback() on connection return.
        # Some libsql builds may panic on rollback after transient failures.
        pool_reset_on_return=None,
        future=True,
    )


def get_turso_engine_from_env() -> Engine:
    url = os.getenv(ENV_TURSO_DATABASE_URL, "").strip()
    token = os.getenv(ENV_TURSO_AUTH_TOKEN, "").strip()
    if not url:
        raise RuntimeError("Missing TURSO_DATABASE_URL")
    return ensure_turso_engine(url, token)


def init_topic_cluster_schema(engine: Engine) -> None:
    """
    Create optional topic cluster tables.

    This is intentionally additive (CREATE TABLE IF NOT EXISTS) so it won't break
    existing deployments.
    """
    ddl_clusters = f"""
    CREATE TABLE IF NOT EXISTS {TOPIC_CLUSTERS_TABLE} (
        cluster_key TEXT PRIMARY KEY,
        cluster_name TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """
    ddl_topics = f"""
    CREATE TABLE IF NOT EXISTS {TOPIC_CLUSTER_TOPICS_TABLE} (
        topic_key TEXT NOT NULL,
        cluster_key TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'manual',
        confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
        created_at TEXT NOT NULL,
        PRIMARY KEY (topic_key, cluster_key)
    );
    """
    ddl_overrides = f"""
    CREATE TABLE IF NOT EXISTS {TOPIC_CLUSTER_POST_OVERRIDES_TABLE} (
        post_uid TEXT PRIMARY KEY,
        cluster_key TEXT NOT NULL,
        reason TEXT NOT NULL DEFAULT '',
        confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
        created_at TEXT NOT NULL
    );
    """
    idx_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_{TOPIC_CLUSTER_TOPICS_TABLE}_cluster_key
        ON {TOPIC_CLUSTER_TOPICS_TABLE}(cluster_key);
    CREATE INDEX IF NOT EXISTS idx_{TOPIC_CLUSTER_POST_OVERRIDES_TABLE}_cluster_key
        ON {TOPIC_CLUSTER_POST_OVERRIDES_TABLE}(cluster_key);
    """
    with turso_connect_autocommit(engine) as conn:
        conn.execute(text(ddl_clusters))
        conn.execute(text(ddl_topics))
        conn.execute(text(ddl_overrides))

        # v1 -> v2 schema migration (topic_key -> (topic_key, cluster_key)).
        pk_cols = _topic_cluster_topics_pk_cols(conn)
        if pk_cols == ["topic_key"]:
            _migrate_topic_cluster_topics_to_v2(conn)

        for stmt in idx_sql.strip().split(";\n"):
            if stmt.strip():
                conn.execute(text(stmt))


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
    with turso_connect_autocommit(engine) as conn:
        conn.execute(text(ddl_posts))
        conn.execute(text(ddl_assertions))
        for stmt in idx_sql.strip().split(";\n"):
            if stmt.strip():
                conn.execute(text(stmt))

    init_topic_cluster_schema(engine)
