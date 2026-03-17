import sqlite3
from pathlib import Path


DB_PATH = Path("workdb.sqlite")


def init_workdb(db_path: Path = DB_PATH) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS posts (
                post_uid TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                platform_post_id TEXT NOT NULL,
                author TEXT NOT NULL,
                created_at TEXT NOT NULL,
                url TEXT NOT NULL,
                raw_text TEXT NOT NULL,
                ingested_at TEXT NOT NULL,

                status TEXT NOT NULL DEFAULT 'new'
                    CHECK (status IN ('new', 'processing', 'relevant', 'irrelevant', 'error')),
                invest_score REAL,
                processed_at TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_retry_at TEXT,
                last_error TEXT,
                model TEXT,
                prompt_version TEXT,

                sync_status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (sync_status IN ('pending', 'synced', 'error')),
                sync_attempts INTEGER NOT NULL DEFAULT 0,
                next_sync_at TEXT,
                synced_at TEXT,
                sync_error TEXT
            );

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

                UNIQUE(post_uid, idx),
                FOREIGN KEY (post_uid) REFERENCES posts(post_uid) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_posts_status_next_retry
                ON posts(status, next_retry_at);
            CREATE INDEX IF NOT EXISTS idx_posts_sync_status_next_sync
                ON posts(sync_status, next_sync_at);
            CREATE INDEX IF NOT EXISTS idx_posts_created_at
                ON posts(created_at);
            CREATE INDEX IF NOT EXISTS idx_posts_author_created_at
                ON posts(author, created_at);
            CREATE INDEX IF NOT EXISTS idx_posts_platform_post_id
                ON posts(platform, platform_post_id);
            CREATE INDEX IF NOT EXISTS idx_posts_ingested_at
                ON posts(ingested_at);

            CREATE INDEX IF NOT EXISTS idx_assertions_topic_key
                ON assertions(topic_key);
            CREATE INDEX IF NOT EXISTS idx_assertions_action
                ON assertions(action);
            """
        )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    init_workdb()
    print(f"Initialized database: {DB_PATH.resolve()}")
