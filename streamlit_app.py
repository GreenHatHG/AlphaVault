from __future__ import annotations

import os

import streamlit as st
from dotenv import load_dotenv

from alphavault.constants import ENV_TURSO_AUTH_TOKEN, ENV_TURSO_DATABASE_URL
from alphavault.db.turso_db import ensure_turso_engine
from alphavault.topic_cluster import enrich_assertions_with_clusters
from alphavault.ui.data import (
    enrich_assertions,
    enrich_posts,
    load_sources,
    load_topic_cluster_sources,
    normalize_assertions_datetime,
    normalize_datetime_columns,
)
from alphavault.ui.filters import build_filters
from alphavault.ui.tab_misc import (
    show_conflicts_and_changes,
    show_learning_library,
    show_tables,
    show_topic_timeline,
)
from alphavault.ui.tab_follow_pages import show_follow_pages
from alphavault.ui.tab_overview import show_kpis, show_overview_charts
from alphavault.ui.tab_risk import show_risk_radar
from alphavault.ui.tab_trade import show_trade_flow
from alphavault.ui.topic_cluster_admin import show_topic_cluster_admin

load_dotenv()


def main() -> None:
    st.set_page_config(page_title="AlphaVault 观点可视化", layout="wide")

    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700&family=Zilla+Slab:wght@600&display=swap');
        :root {
          --ink: #0f172a;
          --muted: #475569;
          --accent: #ff6a3d;
          --bg: #f5f1e9;
          --panel: #ffffff;
        }
        html, body, [class*="css"] {
          font-family: 'Manrope', 'Trebuchet MS', sans-serif;
          color: var(--ink);
        }
        .block-container {
          padding-top: 1.5rem;
          padding-bottom: 3rem;
        }
        .app-hero {
          background: linear-gradient(120deg, #fff5e8 0%, #f0f7ff 100%);
          border: 1px solid #f1e4d4;
          border-radius: 18px;
          padding: 18px 24px;
          margin-bottom: 18px;
        }
        .app-hero h1 {
          font-family: 'Zilla Slab', serif;
          margin: 0;
          font-size: 32px;
        }
        .app-hero p {
          margin: 6px 0 0 0;
          color: var(--muted);
        }
        .stSidebar {
          background: radial-gradient(circle at top, #fff4eb 0%, #f7f3ec 60%, #f2efe9 100%);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="app-hero">
          <h1>AlphaVault · 观点可视化</h1>
          <p>基于 posts / assertions 的交易流、风险雷达、主题时间线与学习库。</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    posts, assertions, missing = load_sources()
    if missing:
        st.error("Turso 没配好，或者连不上。")
        st.info(f"缺少/错误：{', '.join(missing)}")
        st.stop()
    if posts.empty:
        st.warning("Turso 里还没有“已处理”的数据（processed_at 为空会被隐藏）。")
        st.stop()
    posts = normalize_datetime_columns(posts)
    posts = enrich_posts(posts)
    assertions = normalize_assertions_datetime(assertions)
    assertions = enrich_assertions(assertions)

    turso_url = os.getenv(ENV_TURSO_DATABASE_URL, "").strip()
    turso_token = os.getenv(ENV_TURSO_AUTH_TOKEN, "").strip()
    clusters_df, cluster_topic_map_df, cluster_post_overrides_df, cluster_load_error = load_topic_cluster_sources(
        turso_url, turso_token
    )
    assertions = enrich_assertions_with_clusters(
        assertions,
        clusters=clusters_df,
        topic_map=cluster_topic_map_df,
        post_overrides=cluster_post_overrides_df,
    )

    assertion_counts = assertions.groupby("post_uid")["idx"].count()
    posts["assertion_count"] = posts["post_uid"].map(assertion_counts).fillna(0).astype(int)

    posts_filtered, assertions_filtered, meta = build_filters(posts, assertions)

    tabs = st.tabs(
        [
            "总览",
            "交易流",
            "风险雷达",
            "主题时间线",
            "关注页",
            "主题聚合",
            "学习库",
            "冲突/变化",
            "数据表",
        ]
    )

    with tabs[0]:
        show_kpis(posts_filtered, assertions_filtered)
        st.divider()
        show_overview_charts(
            posts_filtered,
            assertions_filtered,
            group_col=meta["group_col"],
            group_label=meta["group_label"],
        )

    with tabs[1]:
        show_trade_flow(
            assertions_filtered,
            group_col=meta["group_col"],
            group_label=meta["group_label"],
        )

    with tabs[2]:
        show_risk_radar(
            assertions_filtered,
            group_col=meta["group_col"],
            group_label=meta["group_label"],
        )

    with tabs[3]:
        show_topic_timeline(
            posts,
            assertions_filtered,
            group_col=meta["group_col"],
            group_label=meta["group_label"],
        )

    with tabs[4]:
        show_follow_pages(
            turso_url=turso_url,
            turso_token=turso_token,
            posts_all=posts,
            assertions_filtered=assertions_filtered,
            clusters=clusters_df,
        )

    with tabs[5]:
        engine = ensure_turso_engine(turso_url, turso_token)
        show_topic_cluster_admin(
            engine=engine,
            assertions_all=assertions,
            clusters=clusters_df,
            topic_map=cluster_topic_map_df,
            load_error=cluster_load_error,
        )

    with tabs[6]:
        show_learning_library(assertions_filtered)

    with tabs[7]:
        show_conflicts_and_changes(
            assertions_filtered,
            group_col=meta["group_col"],
            group_label=meta["group_label"],
        )

    with tabs[8]:
        show_tables(
            posts_filtered,
            assertions_filtered,
            group_col=meta["group_col"],
            group_label=meta["group_label"],
        )


if __name__ == "__main__":
    main()
