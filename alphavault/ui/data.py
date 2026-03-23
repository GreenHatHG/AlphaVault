"""
Streamlit data helpers.

Keep UI code out of here.
This module does:
- load from Turso
- normalize/standardize DataFrame columns
"""

from __future__ import annotations

import json
import os
from typing import Dict, List, Tuple

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
import streamlit as st

from alphavault.constants import ENV_TURSO_AUTH_TOKEN, ENV_TURSO_DATABASE_URL
from alphavault.db.introspect import table_columns
from alphavault.db.turso_db import ensure_turso_engine, turso_connect_autocommit
from alphavault.topic_cluster import try_load_cluster_tables

STREAMLIT_SOURCE_NAME = "archive"

MATCH_KEY_PREFIX_STOCK = "stock"
MATCH_KEY_PREFIX_INDUSTRY = "industry"
MATCH_KEY_PREFIX_COMMODITY = "commodity"
MATCH_KEY_PREFIX_INDEX = "index"


@st.cache_data(show_spinner=False)
def load_topic_cluster_sources(
    db_url: str,
    auth_token: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    if not db_url:
        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            f"Missing {ENV_TURSO_DATABASE_URL}",
        )
    engine = ensure_turso_engine(db_url, auth_token)
    return try_load_cluster_tables(engine)


def parse_json_list(value: object) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        return []
    if isinstance(data, list):
        return [str(item) for item in data if str(item).strip()]
    return []


def _uniq_str(items: list[object]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        s = str(item or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _make_match_key(prefix: str, value: object) -> str:
    p = str(prefix or "").strip()
    v = str(value or "").strip()
    if not p or not v:
        return ""
    return f"{p}:{v}"


def build_match_keys(
    *,
    topic_key: object,
    stock_codes: object,
    industries: object,
    commodities: object,
    indices: object,
) -> list[str]:
    """
    Build a "match keys" list for one assertion.

    This is the core refactor:
    - topic_key is only the *main title* (1 per assertion)
    - match_keys are the *coverage keys* (can be many)
    """
    keys: list[object] = []
    topic = str(topic_key or "").strip()
    if topic:
        keys.append(topic)

    codes = stock_codes if isinstance(stock_codes, list) else []
    inds = industries if isinstance(industries, list) else []
    comms = commodities if isinstance(commodities, list) else []
    idxs = indices if isinstance(indices, list) else []

    for code in codes:
        k = _make_match_key(MATCH_KEY_PREFIX_STOCK, code)
        if k:
            keys.append(k)
    for name in inds:
        k = _make_match_key(MATCH_KEY_PREFIX_INDUSTRY, name)
        if k:
            keys.append(k)
    for name in comms:
        k = _make_match_key(MATCH_KEY_PREFIX_COMMODITY, name)
        if k:
            keys.append(k)
    for name in idxs:
        k = _make_match_key(MATCH_KEY_PREFIX_INDEX, name)
        if k:
            keys.append(k)

    return _uniq_str(keys)


def split_topic_key(value: str) -> Tuple[str, str]:
    if not isinstance(value, str) or not value.strip():
        return "unknown", ""
    if ":" in value:
        left, right = value.split(":", 1)
        return left.strip(), right.strip()
    if "." in value:
        left, right = value.split(".", 1)
        return left.strip(), right.strip()
    return "unknown", value.strip()


def action_group(action: str) -> str:
    if not isinstance(action, str) or not action.strip():
        return "unknown"
    return action.split(".", 1)[0].strip()


@st.cache_data(show_spinner=False)
def load_turso_tables(
    db_url: str, auth_token: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not db_url:
        raise RuntimeError(f"Missing {ENV_TURSO_DATABASE_URL}")
    engine = ensure_turso_engine(db_url, auth_token)
    with turso_connect_autocommit(engine) as conn:
        post_cols = table_columns(conn, "posts")
        display_expr = "display_md" if "display_md" in post_cols else "'' AS display_md"
        assertion_cols = table_columns(conn, "assertions")
        posts_query = """
		        SELECT post_uid, platform, platform_post_id, author, created_at, url, raw_text,
		               {display_expr},
		               final_status AS status, invest_score, processed_at, model, prompt_version
		        FROM posts
		        WHERE processed_at IS NOT NULL
		    """
        posts_query = posts_query.format(display_expr=display_expr)
        wanted_assertion_cols = [
            "post_uid",
            "idx",
            "topic_key",
            "action",
            "action_strength",
            "summary",
            "evidence",
            "confidence",
            "stock_codes_json",
            "stock_names_json",
            "industries_json",
            "commodities_json",
            "indices_json",
            "author",
            "created_at",
        ]
        selected_assertion_cols = [
            col for col in wanted_assertion_cols if col in assertion_cols
        ]
        if selected_assertion_cols:
            assertions_query = (
                f"SELECT {', '.join(selected_assertion_cols)} FROM assertions"
            )
        else:
            assertions_query = "SELECT * FROM assertions"
        posts = pd.read_sql_query(posts_query, conn)
        assertions = pd.read_sql_query(assertions_query, conn)
        return posts, assertions


def load_sources() -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
    posts_frames: List[pd.DataFrame] = []
    assertions_frames: List[pd.DataFrame] = []
    missing: List[str] = []

    turso_url = os.getenv(ENV_TURSO_DATABASE_URL, "").strip()
    turso_token = os.getenv(ENV_TURSO_AUTH_TOKEN, "").strip()
    if not turso_url:
        missing.append(ENV_TURSO_DATABASE_URL)
        return pd.DataFrame(), pd.DataFrame(), missing

    try:
        posts, assertions = load_turso_tables(turso_url, turso_token)
    except Exception as e:
        missing.append(f"turso_connect_error:{type(e).__name__}")
        return pd.DataFrame(), pd.DataFrame(), missing

    posts = standardize_posts(posts, STREAMLIT_SOURCE_NAME)
    posts = normalize_datetime_columns(posts)
    assertions = standardize_assertions(assertions, posts, STREAMLIT_SOURCE_NAME)
    assertions = normalize_assertions_datetime(assertions)
    posts_frames.append(posts)
    assertions_frames.append(assertions)

    if posts_frames:
        posts_all = pd.concat(posts_frames, ignore_index=True)
    else:
        posts_all = pd.DataFrame()

    if assertions_frames:
        assertions_all = pd.concat(assertions_frames, ignore_index=True)
    else:
        assertions_all = pd.DataFrame()

    return posts_all, assertions_all, missing


def normalize_datetime_columns(posts: pd.DataFrame) -> pd.DataFrame:
    for col in [
        "created_at",
        "ingested_at",
        "processed_at",
        "next_retry_at",
        "synced_at",
    ]:
        if col in posts.columns:
            if not is_datetime64_any_dtype(posts[col]):
                posts[col] = pd.to_datetime(posts[col], errors="coerce", utc=True)
                posts[col] = posts[col].dt.tz_convert(None)
    return posts


def normalize_assertions_datetime(assertions: pd.DataFrame) -> pd.DataFrame:
    if "created_at" in assertions.columns:
        if not is_datetime64_any_dtype(assertions["created_at"]):
            assertions["created_at"] = pd.to_datetime(
                assertions["created_at"], errors="coerce", utc=True
            )
            assertions["created_at"] = assertions["created_at"].dt.tz_convert(None)
    return assertions


def enrich_posts(posts: pd.DataFrame) -> pd.DataFrame:
    posts = posts.copy()
    posts["has_quote"] = posts["raw_text"].str.contains("//@", na=False)
    posts["is_forward"] = posts["raw_text"].str.startswith("转发微博", na=False)
    posts["repost_flag"] = posts["has_quote"] | posts["is_forward"]
    return posts


def enrich_assertions(assertions: pd.DataFrame) -> pd.DataFrame:
    assertions = assertions.copy()
    assertions["stock_codes"] = assertions["stock_codes_json"].apply(parse_json_list)
    assertions["stock_names"] = assertions["stock_names_json"].apply(parse_json_list)
    assertions["industries"] = assertions["industries_json"].apply(parse_json_list)
    assertions["commodities"] = assertions["commodities_json"].apply(parse_json_list)
    assertions["indices"] = assertions["indices_json"].apply(parse_json_list)
    assertions["stock_codes_str"] = assertions["stock_codes"].apply(
        lambda items: ", ".join(items)
    )
    assertions["stock_names_str"] = assertions["stock_names"].apply(
        lambda items: ", ".join(items)
    )
    assertions["industries_str"] = assertions["industries"].apply(
        lambda items: ", ".join(items)
    )
    assertions["commodities_str"] = assertions["commodities"].apply(
        lambda items: ", ".join(items)
    )
    assertions["indices_str"] = assertions["indices"].apply(
        lambda items: ", ".join(items)
    )

    topic_parts = assertions["topic_key"].apply(split_topic_key)
    assertions["topic_type"] = topic_parts.apply(lambda item: item[0])
    assertions["topic_value"] = topic_parts.apply(lambda item: item[1])
    assertions["action_group"] = assertions["action"].apply(action_group)

    assertions["match_keys"] = assertions.apply(
        lambda row: build_match_keys(
            topic_key=row.get("topic_key"),
            stock_codes=row.get("stock_codes"),
            industries=row.get("industries"),
            commodities=row.get("commodities"),
            indices=row.get("indices"),
        ),
        axis=1,
    )
    return assertions


def standardize_posts(posts: pd.DataFrame, source_name: str) -> pd.DataFrame:
    posts = posts.copy()
    defaults: Dict[str, object] = {
        "post_uid": "",
        "platform": source_name,
        "platform_post_id": "",
        "author": "",
        "created_at": "",
        "url": "",
        "raw_text": "",
        "display_md": "",
        "status": "",
        "invest_score": 0.0,
        "processed_at": "",
    }
    for col, default in defaults.items():
        if col not in posts.columns:
            posts[col] = default
    if "platform" in posts.columns:
        posts["platform"] = posts["platform"].replace("", pd.NA)
        posts["platform"] = posts["platform"].fillna(source_name)
    posts["source"] = source_name
    return posts


def standardize_assertions(
    assertions: pd.DataFrame,
    posts: pd.DataFrame,
    source_name: str,
) -> pd.DataFrame:
    assertions = assertions.copy()
    defaults: Dict[str, object] = {
        "post_uid": "",
        "idx": 0,
        "topic_key": "",
        "action": "",
        "action_strength": 0,
        "summary": "",
        "evidence": "",
        "confidence": 0.0,
        "stock_codes_json": "[]",
        "stock_names_json": "[]",
        "industries_json": "[]",
        "commodities_json": "[]",
        "indices_json": "[]",
        "author": "",
        "created_at": "",
    }
    for col, default in defaults.items():
        if col not in assertions.columns:
            assertions[col] = default

    assertions["source"] = source_name

    if "author" in assertions.columns:
        missing_author = assertions["author"].eq("") | assertions["author"].isna()
    else:
        missing_author = pd.Series([True] * len(assertions))
    if "created_at" in assertions.columns:
        missing_created = (
            assertions["created_at"].eq("") | assertions["created_at"].isna()
        )
    else:
        missing_created = pd.Series([True] * len(assertions))

    if not posts.empty:
        author_map = posts.set_index("post_uid")["author"]
        created_map = posts.set_index("post_uid")["created_at"]
        url_map = posts.set_index("post_uid")["url"]
        raw_map = posts.set_index("post_uid")["raw_text"]
        display_map = posts.set_index("post_uid")["display_md"]
        status_map = posts.set_index("post_uid")["status"]
        score_map = posts.set_index("post_uid")["invest_score"]

        assertions.loc[missing_author, "author"] = assertions.loc[
            missing_author, "post_uid"
        ].map(author_map)
        assertions.loc[missing_created, "created_at"] = assertions.loc[
            missing_created, "post_uid"
        ].map(created_map)
        assertions["url"] = assertions["post_uid"].map(url_map)
        assertions["raw_text"] = assertions["post_uid"].map(raw_map)
        assertions["display_md"] = assertions["post_uid"].map(display_map)
        assertions["status"] = assertions["post_uid"].map(status_map)
        assertions["invest_score"] = assertions["post_uid"].map(score_map)
    else:
        assertions["url"] = ""
        assertions["raw_text"] = ""
        assertions["display_md"] = ""
        assertions["status"] = ""
        assertions["invest_score"] = 0.0

    return assertions
