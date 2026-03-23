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
                key = item.strip()
                if key:
                    out.append({"key": key, "confidence": None, "reason": ""})
                continue
            if isinstance(item, dict):
                key = str(item.get("key") or item.get("topic_key") or "").strip()
                if not key:
                    continue
                out.append(
                    {
                        "key": key,
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
    keys = [str(x).strip() for x in (topic_keys or []) if str(x).strip()]
    if not keys:
        return []

    # Best-effort stock hints (conservative mapping).
    stock_name_by_code: dict[str, str] = {}
    stock_industry_by_code: dict[str, str] = {}
    if (
        "stock_codes" in assertions_all.columns
        and "stock_names" in assertions_all.columns
        and "industries" in assertions_all.columns
    ):
        for codes, names, industries in zip(
            assertions_all["stock_codes"].tolist(),
            assertions_all["stock_names"].tolist(),
            assertions_all["industries"].tolist(),
            strict=False,
        ):
            if not isinstance(codes, list) or not isinstance(names, list):
                continue
            codes = [str(x).strip() for x in codes if str(x).strip()]
            names = [str(x).strip() for x in names if str(x).strip()]
            inds = [str(x).strip() for x in industries if str(x).strip()] if isinstance(industries, list) else []
            if len(codes) != 1 or len(names) != 1:
                continue
            code = codes[0]
            name = names[0]
            if code and name and code not in stock_name_by_code:
                stock_name_by_code[code] = name
            if code and inds and code not in stock_industry_by_code:
                stock_industry_by_code[code] = inds[0]

    records: list[dict] = []
    for key in keys:
        count = int(count_by_topic.get(key, 0))
        hint_parts: list[str] = []
        if key.startswith("stock:"):
            code = key[len("stock:") :].strip()
            if code:
                name = str(stock_name_by_code.get(code, "") or "").strip()
                industry = str(stock_industry_by_code.get(code, "") or "").strip()
                if name:
                    hint_parts.append(f"stock_name={name}")
                if industry:
                    hint_parts.append(f"industry={industry}")
        hint = ", ".join(hint_parts)[:120]
        records.append({"key": key, "count": count, "hint": hint})
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
        key = str(item.get("key") or item.get("topic_key") or "").strip()
        if not key or key not in candidate_set:
            continue
        out.append(
            {
                **item,
                "count": int(count_by_topic.get(key, 0)),
                "hint": str(hint_by_topic.get(key, "") or "").strip(),
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
