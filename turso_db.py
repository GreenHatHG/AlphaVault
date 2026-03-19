from __future__ import annotations

import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# NOTE: This module is extracted from the old local-sqlite sync scripts.
# It keeps only Turso engine creation + base schema (posts/assertions).

TOPIC_CLUSTERS_TABLE = "topic_clusters"
TOPIC_CLUSTER_TOPICS_TABLE = "topic_cluster_topics"
TOPIC_CLUSTER_POST_OVERRIDES_TABLE = "topic_cluster_post_overrides"


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
        # Avoid calling DBAPI rollback() on connection return.
        # Some libsql builds may panic on rollback after transient failures.
        pool_reset_on_return=None,
        future=True,
    )


def get_turso_engine_from_env() -> Engine:
    url = os.getenv("TURSO_DATABASE_URL", "").strip()
    token = os.getenv("TURSO_AUTH_TOKEN", "").strip()
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
        topic_key TEXT PRIMARY KEY,
        cluster_key TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT 'manual',
        confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0 AND confidence <= 1),
        created_at TEXT NOT NULL
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
    with engine.begin() as conn:
        conn.execute(text(ddl_clusters))
        conn.execute(text(ddl_topics))
        conn.execute(text(ddl_overrides))
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
    with engine.begin() as conn:
        conn.execute(text(ddl_posts))
        conn.execute(text(ddl_assertions))
        for stmt in idx_sql.strip().split(";\n"):
            if stmt.strip():
                conn.execute(text(stmt))

    init_topic_cluster_schema(engine)
