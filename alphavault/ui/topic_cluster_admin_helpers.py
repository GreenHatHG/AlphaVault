from __future__ import annotations

from typing import Dict

import pandas as pd


def _build_cluster_display_maps(clusters: pd.DataFrame) -> tuple[Dict[str, str], Dict[str, str]]:
    name_by_key: Dict[str, str] = {}
    desc_by_key: Dict[str, str] = {}
    if clusters.empty:
        return name_by_key, desc_by_key
    for _, row in clusters.iterrows():
        key = str(row.get("cluster_key") or "").strip()
        if not key:
            continue
        name_by_key[key] = str(row.get("cluster_name") or "").strip()
        desc_by_key[key] = str(row.get("description") or "").strip()
    return name_by_key, desc_by_key


def _format_cluster_label(cluster_key: str, name_by_key: Dict[str, str]) -> str:
    name = (name_by_key.get(cluster_key) or "").strip()
    if name and name != cluster_key:
        return f"{name} ({cluster_key})"
    return cluster_key


def _normalize_topic_items(raw: object) -> list[dict]:
    if raw is None:
        return []
    if isinstance(raw, list):
        out: list[dict] = []
        for item in raw:
            if isinstance(item, str):
                topic_key = item.strip()
                if topic_key:
                    out.append({"topic_key": topic_key, "confidence": None, "reason": ""})
                continue
            if isinstance(item, dict):
                topic_key = str(item.get("topic_key") or "").strip()
                if not topic_key:
                    continue
                out.append(
                    {
                        "topic_key": topic_key,
                        "confidence": item.get("confidence", None),
                        "reason": str(item.get("reason") or "").strip(),
                    }
                )
        return out
    return []


def _pick_first_nonempty_from_list_col(values: pd.Series) -> str:
    for row in values:
        if not isinstance(row, list):
            continue
        for item in row:
            s = str(item or "").strip()
            if s:
                return s
    return ""


def _build_candidate_records(
    assertions_all: pd.DataFrame,
    topic_keys: list[str],
    count_by_topic: dict,
) -> list[dict]:
    if not topic_keys:
        return []

    cols = ["topic_key"]
    for col in ["topic_type", "topic_value", "stock_names", "industries", "commodities", "indices"]:
        if col in assertions_all.columns:
            cols.append(col)
    df = assertions_all[cols].copy()
    df["topic_key"] = df["topic_key"].astype(str).str.strip()
    df = df[df["topic_key"].isin(set(topic_keys))]
    if df.empty:
        return [{"topic_key": k, "count": int(count_by_topic.get(k, 0)), "hint": ""} for k in topic_keys]

    first_type = (
        df.groupby("topic_key")["topic_type"].first()
        if "topic_type" in df.columns
        else pd.Series(dtype=str)
    )
    first_value = (
        df.groupby("topic_key")["topic_value"].first()
        if "topic_value" in df.columns
        else pd.Series(dtype=str)
    )
    first_stock_name = (
        df.groupby("topic_key")["stock_names"].apply(_pick_first_nonempty_from_list_col)
        if "stock_names" in df.columns
        else pd.Series(dtype=str)
    )
    first_industry = (
        df.groupby("topic_key")["industries"].apply(_pick_first_nonempty_from_list_col)
        if "industries" in df.columns
        else pd.Series(dtype=str)
    )

    records: list[dict] = []
    for topic_key in topic_keys:
        count = int(count_by_topic.get(topic_key, 0))
        t = str(first_type.get(topic_key, "") or "").strip()
        v = str(first_value.get(topic_key, "") or "").strip()
        stock_name = (
            str(first_stock_name.get(topic_key, "") or "").strip()
            if isinstance(first_stock_name, pd.Series)
            else ""
        )
        industry = (
            str(first_industry.get(topic_key, "") or "").strip()
            if isinstance(first_industry, pd.Series)
            else ""
        )
        hint_parts: list[str] = []
        if t == "stock":
            if stock_name:
                hint_parts.append(f"stock_name={stock_name}")
            if industry:
                hint_parts.append(f"industry={industry}")
            if not hint_parts and v:
                hint_parts.append(f"stock_value={v}")
        hint = ", ".join(hint_parts)[:120]
        records.append({"topic_key": topic_key, "count": count, "hint": hint})
    return records


def _uniq_str(items: list) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        s = str(item or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _filter_items_to_candidates(
    items: list[dict],
    *,
    candidate_set: set[str],
    count_by_topic: dict[str, int],
    hint_by_topic: dict[str, str],
) -> list[dict]:
    out: list[dict] = []
    for item in items:
        topic_key = str(item.get("topic_key") or "").strip()
        if not topic_key or topic_key not in candidate_set:
            continue
        out.append(
            {
                **item,
                "count": int(count_by_topic.get(topic_key, 0)),
                "hint": str(hint_by_topic.get(topic_key, "") or "").strip(),
            }
        )
    return out


def _contains_any_word(words: list[str], text_value: object) -> bool:
    text = str(text_value or "").lower()
    for word in words:
        if word and word.lower() in text:
            return True
    return False


def _parse_confidence(raw: object, default_value: float) -> float:
    try:
        val = float(raw)
    except Exception:
        val = float(default_value)
    return max(0.0, min(1.0, float(val)))


def _split_new_and_move(
    topic_keys: list[str],
    *,
    topic_to_clusters: dict[str, list[str]],
    selected_cluster: str,
) -> tuple[list[str], list[str], dict[str, list[str]]]:
    new_keys: list[str] = []
    move_keys: list[str] = []
    from_clusters_by_topic: dict[str, list[str]] = {}
    for topic_key in topic_keys:
        existing_clusters = topic_to_clusters.get(topic_key, [])
        existing_clusters = [str(x).strip() for x in existing_clusters if str(x).strip()]
        if not existing_clusters:
            new_keys.append(topic_key)
            continue
        if selected_cluster in existing_clusters:
            continue
        move_keys.append(topic_key)
        from_clusters_by_topic[topic_key] = existing_clusters
    return new_keys, move_keys, from_clusters_by_topic


def _sort_by_count(items: list[str], *, count_by_topic: dict[str, int]) -> list[str]:
    return sorted(items, key=lambda k: (-int(count_by_topic.get(k, 0)), str(k)))


def _format_basic_topic(topic_key: str, *, count_by_topic: dict[str, int]) -> str:
    count = int(count_by_topic.get(topic_key, 0))
    return f"{topic_key}（{count}次）" if count else topic_key


def _format_move_topic(
    topic_key: str,
    *,
    from_clusters_by_topic: dict[str, list[str]],
    name_by_key: Dict[str, str],
    count_by_topic: dict[str, int],
) -> str:
    from_keys = from_clusters_by_topic.get(topic_key, [])
    from_keys = [str(x).strip() for x in from_keys if str(x).strip()]
    if from_keys:
        labels = [_format_cluster_label(k, name_by_key) for k in from_keys[:3]]
        from_label = " / ".join([x for x in labels if x])
        if len(from_keys) > 3:
            from_label = f"{from_label} 等{len(from_keys)}个"
    else:
        from_label = "未知"
    count = int(count_by_topic.get(topic_key, 0))
    count_part = f"，{count}次" if count else ""
    return f"{topic_key}（已在 {from_label}，也加入{count_part}）"
