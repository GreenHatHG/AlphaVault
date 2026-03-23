from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from alphavault.constants import DATETIME_FMT
from alphavault.db.turso_db import (
    TOPIC_CLUSTER_POST_OVERRIDES_TABLE,
    TOPIC_CLUSTER_TOPICS_TABLE,
    TOPIC_CLUSTERS_TABLE,
    init_topic_cluster_schema,
    turso_connect_autocommit,
    turso_savepoint,
)


UNCATEGORIZED_LABEL = "未归类"


def _now_str() -> str:
    # Keep the same shape as rss_utils.now_str() (YYYY-MM-DD HH:MM:SS).
    return datetime.now().strftime(DATETIME_FMT)


def ensure_cluster_schema(engine: Engine) -> None:
    init_topic_cluster_schema(engine)


def try_load_cluster_tables(engine: Engine) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    """
    Best-effort load cluster tables.

    Returns: (clusters, topic_map, post_overrides, error_message)
    """
    try:
        clusters = pd.read_sql_query(
            f"SELECT cluster_key, cluster_name, description, created_at, updated_at FROM {TOPIC_CLUSTERS_TABLE}",
            engine,
        )
        topic_map = pd.read_sql_query(
            f"SELECT topic_key, cluster_key, source, confidence, created_at FROM {TOPIC_CLUSTER_TOPICS_TABLE}",
            engine,
        )
        post_overrides = pd.read_sql_query(
            f"SELECT post_uid, cluster_key, reason, confidence, created_at FROM {TOPIC_CLUSTER_POST_OVERRIDES_TABLE}",
            engine,
        )
        return clusters, topic_map, post_overrides, ""
    except Exception as exc:
        empty = pd.DataFrame()
        return empty, empty, empty, f"{type(exc).__name__}: {exc}"


@dataclass(frozen=True)
class ClusterMaps:
    cluster_name_by_key: Dict[str, str]
    cluster_keys_by_member_key: Dict[str, List[str]]
    cluster_by_post_uid: Dict[str, str]


def build_cluster_maps(
    clusters: pd.DataFrame,
    topic_map: pd.DataFrame,
    post_overrides: pd.DataFrame,
) -> ClusterMaps:
    cluster_name_by_key: Dict[str, str] = {}
    if not clusters.empty:
        for _, row in clusters.iterrows():
            key = str(row.get("cluster_key") or "").strip()
            name = str(row.get("cluster_name") or "").strip()
            if key:
                cluster_name_by_key[key] = name

    # Note: historical column name is still "topic_key" in DB/table,
    # but we treat it as a generic "member_key" now.
    cluster_keys_by_member_key: Dict[str, List[str]] = {}
    if not topic_map.empty:
        for _, row in topic_map.iterrows():
            member_key = str(row.get("topic_key") or "").strip()
            cluster_key = str(row.get("cluster_key") or "").strip()
            if member_key and cluster_key:
                cluster_keys_by_member_key.setdefault(member_key, []).append(cluster_key)
    if cluster_keys_by_member_key:
        for member_key, keys in list(cluster_keys_by_member_key.items()):
            # keep unique + stable (sorted) for deterministic UI.
            uniq = sorted(set(str(k).strip() for k in keys if str(k).strip()))
            cluster_keys_by_member_key[member_key] = uniq

    cluster_by_post_uid: Dict[str, str] = {}
    if not post_overrides.empty:
        for _, row in post_overrides.iterrows():
            post_uid = str(row.get("post_uid") or "").strip()
            cluster_key = str(row.get("cluster_key") or "").strip()
            if post_uid and cluster_key:
                cluster_by_post_uid[post_uid] = cluster_key

    return ClusterMaps(
        cluster_name_by_key=cluster_name_by_key,
        cluster_keys_by_member_key=cluster_keys_by_member_key,
        cluster_by_post_uid=cluster_by_post_uid,
    )


def enrich_assertions_with_clusters(
    assertions: pd.DataFrame,
    *,
    clusters: pd.DataFrame,
    topic_map: pd.DataFrame,
    post_overrides: pd.DataFrame,
) -> pd.DataFrame:
    """
    Add cluster columns on top of existing assertions.

    Columns added:
    - cluster_keys: list of resolved keys (topic_map first; if empty then post override)
    - cluster_displays: list of display names (cluster_name or cluster_key); if empty then ['未归类']
    """
    if assertions.empty:
        out = assertions.copy()
        out["cluster_keys"] = [[] for _ in range(len(out))]
        out["cluster_displays"] = [[UNCATEGORIZED_LABEL] for _ in range(len(out))]
        return out

    maps = build_cluster_maps(clusters, topic_map, post_overrides)
    out = assertions.copy()

    if "match_keys" in out.columns:
        match_keys = out["match_keys"]
    else:
        match_keys = out.get("topic_key", pd.Series([""] * len(out), index=out.index))
    post_uids = out.get("post_uid", pd.Series([""] * len(out), index=out.index))

    def _cluster_keys_for_keys_and_post(keys_value: object, post_uid: object) -> list[str]:
        keys_out: list[str] = []
        if isinstance(keys_value, list):
            for item in keys_value:
                member_key = str(item or "").strip()
                if not member_key:
                    continue
                resolved = maps.cluster_keys_by_member_key.get(member_key)
                if resolved:
                    keys_out.extend(resolved)
        else:
            member_key = str(keys_value or "").strip()
            if member_key:
                resolved = maps.cluster_keys_by_member_key.get(member_key)
                if resolved:
                    keys_out.extend(resolved)

        if keys_out:
            # Keep unique + stable for deterministic UI.
            return sorted(set(str(x).strip() for x in keys_out if str(x).strip()))
        p = str(post_uid or "").strip()
        if p:
            override_key = str(maps.cluster_by_post_uid.get(p, "") or "").strip()
            if override_key:
                return [override_key]
        return []

    out["cluster_keys"] = [
        _cluster_keys_for_keys_and_post(keys_value, post_uid)
        for keys_value, post_uid in zip(match_keys.tolist(), post_uids.tolist(), strict=False)
    ]

    def _displays_for_keys(keys: object) -> list[str]:
        if not isinstance(keys, list):
            keys = []
        out_labels: list[str] = []
        for key in keys:
            k = str(key or "").strip()
            if not k:
                continue
            name = str(maps.cluster_name_by_key.get(k, "") or "").strip()
            out_labels.append(name if name else k)
        if out_labels:
            return out_labels
        return [UNCATEGORIZED_LABEL]

    out["cluster_displays"] = out["cluster_keys"].apply(_displays_for_keys)
    return out


def upsert_cluster(
    engine: Engine,
    *,
    cluster_key: str,
    cluster_name: str,
    description: str,
) -> None:
    now = _now_str()
    with turso_connect_autocommit(engine) as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {TOPIC_CLUSTERS_TABLE}(
                    cluster_key, cluster_name, description, created_at, updated_at
                )
                VALUES (:cluster_key, :cluster_name, :description, :now, :now)
                ON CONFLICT(cluster_key) DO UPDATE SET
                    cluster_name = excluded.cluster_name,
                    description = excluded.description,
                    updated_at = excluded.updated_at
                """
            ),
            {
                "cluster_key": str(cluster_key or "").strip(),
                "cluster_name": str(cluster_name or "").strip(),
                "description": str(description or "").strip(),
                "now": now,
            },
        )


def upsert_cluster_topics(
    engine: Engine,
    *,
    cluster_key: str,
    topic_keys: Iterable[str],
    source: str = "manual",
    confidence: float = 1.0,
) -> int:
    now = _now_str()
    items = [str(item or "").strip() for item in topic_keys]
    items = [item for item in items if item]
    if not items:
        return 0

    cluster_key = str(cluster_key or "").strip()
    if not cluster_key:
        return 0

    payloads = [
        {
            "topic_key": topic_key,
            "cluster_key": cluster_key,
            "source": str(source or "manual").strip(),
            "confidence": float(confidence),
            "now": now,
        }
        for topic_key in items
    ]

    with turso_connect_autocommit(engine) as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {TOPIC_CLUSTER_TOPICS_TABLE}(
                    topic_key, cluster_key, source, confidence, created_at
                )
                VALUES (:topic_key, :cluster_key, :source, :confidence, :now)
                ON CONFLICT(topic_key, cluster_key) DO UPDATE SET
                    source = excluded.source,
                    confidence = excluded.confidence,
                    created_at = excluded.created_at
                """
            ),
            payloads,
        )
    return len(items)


def upsert_cluster_topics_detailed(
    engine: Engine,
    *,
    cluster_key: str,
    topic_items: Iterable[dict],
    default_source: str = "manual",
    default_confidence: float = 1.0,
) -> int:
    now = _now_str()
    cluster_key = str(cluster_key or "").strip()
    if not cluster_key:
        return 0

    payloads: list[dict] = []
    for item in topic_items:
        if not isinstance(item, dict):
            continue
        topic_key = str(item.get("topic_key") or "").strip()
        if not topic_key:
            continue
        source = str(item.get("source") or default_source).strip() or default_source
        confidence_raw = item.get("confidence", default_confidence)
        try:
            confidence_val = float(confidence_raw)
        except Exception:
            confidence_val = float(default_confidence)
        confidence_val = max(0.0, min(1.0, confidence_val))
        payloads.append(
            {
                "topic_key": topic_key,
                "cluster_key": cluster_key,
                "source": source,
                "confidence": confidence_val,
                "now": now,
            }
        )

    if not payloads:
        return 0

    with turso_connect_autocommit(engine) as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO {TOPIC_CLUSTER_TOPICS_TABLE}(
                    topic_key, cluster_key, source, confidence, created_at
                )
                VALUES (:topic_key, :cluster_key, :source, :confidence, :now)
                ON CONFLICT(topic_key, cluster_key) DO UPDATE SET
                    source = excluded.source,
                    confidence = excluded.confidence,
                    created_at = excluded.created_at
                """
            ),
            payloads,
        )
    return len(payloads)


def delete_cluster_topics(engine: Engine, *, cluster_key: str, topic_keys: Iterable[str]) -> int:
    items = [str(item or "").strip() for item in topic_keys]
    items = [item for item in items if item]
    if not items:
        return 0
    key = str(cluster_key or "").strip()
    if not key:
        return 0
    with turso_connect_autocommit(engine) as conn:
        with turso_savepoint(conn):
            for topic_key in items:
                conn.execute(
                    text(
                        f"""
                        DELETE FROM {TOPIC_CLUSTER_TOPICS_TABLE}
                        WHERE topic_key = :topic_key AND cluster_key = :cluster_key
                        """
                    ),
                    {"topic_key": topic_key, "cluster_key": key},
                )
    return len(items)


def delete_cluster(engine: Engine, *, cluster_key: str) -> dict[str, int]:
    """
    Delete one cluster and its mappings.

    Note: this does NOT delete follow_pages. Users delete those manually.
    """
    key = str(cluster_key or "").strip()
    if not key:
        return {"clusters": 0, "topics": 0, "overrides": 0}

    with turso_connect_autocommit(engine) as conn:
        with turso_savepoint(conn):
            res_topics = conn.execute(
                text(f"DELETE FROM {TOPIC_CLUSTER_TOPICS_TABLE} WHERE cluster_key = :cluster_key"),
                {"cluster_key": key},
            )
            res_overrides = conn.execute(
                text(f"DELETE FROM {TOPIC_CLUSTER_POST_OVERRIDES_TABLE} WHERE cluster_key = :cluster_key"),
                {"cluster_key": key},
            )
            res_clusters = conn.execute(
                text(f"DELETE FROM {TOPIC_CLUSTERS_TABLE} WHERE cluster_key = :cluster_key"),
                {"cluster_key": key},
            )

    return {
        "clusters": int(res_clusters.rowcount or 0),
        "topics": int(res_topics.rowcount or 0),
        "overrides": int(res_overrides.rowcount or 0),
    }
