"""
app.py – Streamlit Web App (Phase 6)
======================================
Web frontend for the Betting Financial Reports ETL pipeline.

Run with:  streamlit run app.py
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from src.ingestion import load_all_data, load_campaign_data, DATA_DIR, RAW_DIR, CAMPAIGNS_DIR
from src.analytics import generate_monthly_summaries, generate_campaign_summaries, generate_cohort_matrix, generate_segmentation_summary, generate_both_business_summary, generate_time_series, generate_program_summary, generate_rfm_summary, generate_smart_narrative, generate_player_master_list, generate_retention_heatmap, generate_overlap_stats, generate_ltv_curves
from src.exporter import export_to_excel

# ── Cached wrappers to prevent recomputation on Streamlit rerun ───────────
@st.cache_data(show_spinner=False)
def _cached_time_series(data):
    return generate_time_series(data)

@st.cache_data(show_spinner=False)
def _cached_rfm_summary(raw_df, target_month):
    return generate_rfm_summary(raw_df, target_month)

@st.cache_data(show_spinner=False)
def _cached_player_master_list(raw_df):
    return generate_player_master_list(raw_df)

@st.cache_data(show_spinner=False)
def _cached_retention_heatmap(raw_df):
    return generate_retention_heatmap(raw_df)

@st.cache_data(show_spinner=False)
def _cached_ltv_curves(raw_df):
    return generate_ltv_curves(raw_df)

# ── Config ───────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
OUTPUT_DIR = DATA_DIR / "output"
OUTPUT_FILE = OUTPUT_DIR / "Summary_Data_Auto.xlsx"

BRANDS = ["latribet", "rojabet"]

# ── Page config ──────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Betting Financial Reports",
    page_icon="📊",
    layout="wide",
)

# ── Matrix theme: neon glow CSS ──────────────────────────────────────────
st.markdown(
    """
    <style>
    /* Neon green glow on metric numbers */
    [data-testid="stMetricValue"] {
        text-shadow: 0 0 7px #00FF41, 0 0 14px #00FF4180;
    }
    /* Glow on metric delta text */
    [data-testid="stMetricDelta"] {
        text-shadow: 0 0 5px #00FF4160;
    }
    /* Subtle glow on headers */
    h1, h2, h3, h4 {
        text-shadow: 0 0 10px #00FF4140;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("D-ROCK FINANCIAL TERMINAL v1.0")
st.caption("Upload CSVs → Run Pipeline → Download Intel.")

# ── Session State Initialization ───────────────────────────────────────
if "data_loaded" not in st.session_state:
    st.session_state["data_loaded"] = False


# ═══════════════════════════════════════════════════════════════════════════
#  Sidebar: File Uploaders
# ═══════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("📁 Upload Data Files")
    st.markdown("---")

    # ── Financial Data ───────────────────────────────────────────────────
    st.subheader("Financial Data")
    for brand in BRANDS:
        brand_dir = RAW_DIR / brand
        brand_dir.mkdir(parents=True, exist_ok=True)
        existing = sorted(brand_dir.glob("*.csv"))

        with st.expander(f"**{brand.title()}** ({len(existing)} files)", expanded=False):
            uploaded = st.file_uploader(
                f"Upload {brand.title()} financial CSVs",
                type="csv",
                accept_multiple_files=True,
                key=f"fin_{brand}",
            )
            if uploaded:
                for f in uploaded:
                    dest = brand_dir / f.name
                    dest.write_bytes(f.getvalue())
                st.success(f"Saved {len(uploaded)} file(s) to `data/raw/{brand}/`")

            if existing:
                st.caption("Files on disk:")
                for fp in existing:
                    st.text(f"  • {fp.name}")

    st.markdown("---")

    # ── Campaign Data ────────────────────────────────────────────────────
    st.subheader("Campaign Data")
    for brand in BRANDS:
        brand_dir = CAMPAIGNS_DIR / brand
        brand_dir.mkdir(parents=True, exist_ok=True)
        existing = sorted(brand_dir.glob("*.csv"))

        with st.expander(f"**{brand.title()}** ({len(existing)} files)", expanded=False):
            uploaded = st.file_uploader(
                f"Upload {brand.title()} campaign CSVs",
                type="csv",
                accept_multiple_files=True,
                key=f"camp_{brand}",
            )
            if uploaded:
                for f in uploaded:
                    dest = brand_dir / f.name
                    dest.write_bytes(f.getvalue())
                st.success(f"Saved {len(uploaded)} file(s) to `data/campaigns/{brand}/`")

            if existing:
                st.caption("Files on disk:")
                for fp in existing:
                    st.text(f"  • {fp.name}")

    # ── Excel Report Download (persistent) ────────────────────────
    st.markdown("---")
    _excel_path = OUTPUT_DIR / "Summary_Data_Auto.xlsx"
    if _excel_path.exists():
        with open(_excel_path, "rb") as _fh:
            st.download_button(
                label="Download Excel Report",
                data=_fh.read(),
                file_name="Summary_Data_Auto.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True,
            )


# ═══════════════════════════════════════════════════════════════════════════
#  Main Area: Pipeline Execution
# ═══════════════════════════════════════════════════════════════════════════
st.markdown("---")

run_clicked = st.button("🚀 Run Analytics Pipeline", type="primary", use_container_width=True)

if run_clicked:
    with st.status("⏳ Executing ETL Pipeline...", expanded=True) as status:

        # ── Phase 2: Ingestion ───────────────────────────────────────────
        st.write("> Ingesting financial data...")
        try:
            df, registry = load_all_data()
        except Exception as exc:
            st.error(f"Ingestion failed: {exc}")
            st.stop()

        if df.empty:
            st.warning("No financial data found. Upload CSVs in the sidebar first.")
            st.stop()

        st.write(f"> Loaded {len(df):,} player records across {df['report_month'].nunique()} months.")

        # ── Phase 3: Analytics ───────────────────────────────────────────
        st.write("> Computing financial summaries...")
        financial_summary = generate_monthly_summaries(df)

        # ── Phase 5: Campaigns ───────────────────────────────────────────
        st.write("> Processing campaign data...")
        campaign_raw = load_campaign_data()
        campaign_summary: pd.DataFrame | None = None

        if campaign_raw.empty:
            st.write("No campaign data found — skipping.")
        else:
            campaign_summary = generate_campaign_summaries(campaign_raw)
            st.write(f"Campaign summary: {len(campaign_summary)} rows.")

        # ── Phase 7: Cohort Matrix ────────────────────────────────────────
        st.write("> Building cohort retention matrices...")
        cohort_matrices = generate_cohort_matrix(df)

        # ── Phase 8: Segmentation ────────────────────────────────────────
        st.write("> Building segmentation matrix...")
        segmentation = generate_segmentation_summary(df)

        # ── Phase 9: Both Business ────────────────────────────────────────
        st.write("> Building Both Business summary...")
        both_business = generate_both_business_summary(financial_summary)

        # ── Phase 12: Program Summary ───────────────────────────────────────
        st.write("> Building program summary...")
        program_summary = generate_program_summary(df)

        # ── Phase 4: Export ──────────────────────────────────────────────
        st.write("> Generating Excel report...")
        output_path = export_to_excel(
            financial_summary,
            OUTPUT_DIR,
            campaign_df=campaign_summary,
            cohort_matrices=cohort_matrices,
            segmentation_df=segmentation,
            both_business_df=both_business,
        )

        status.update(label="✅ Pipeline complete! Report written.", state="complete", expanded=False)

    # Save to session state so dashboard survives reruns
    st.session_state["df"] = df
    st.session_state["registry"] = registry
    st.session_state["financial_summary"] = financial_summary
    st.session_state["campaign_summary"] = campaign_summary
    st.session_state["cohort_matrices"] = cohort_matrices
    st.session_state["segmentation"] = segmentation
    st.session_state["both_business"] = both_business
    st.session_state["program_summary"] = program_summary
    st.session_state["output_path"] = output_path
    st.session_state["data_loaded"] = True

# ══════════════════════════════════════════════════════════════════════════════
#  BI Dashboard — reads from session state
# ══════════════════════════════════════════════════════════════════════════════
if st.session_state["data_loaded"]:
    df = st.session_state["df"]
    registry = st.session_state["registry"]
    financial_summary = st.session_state["financial_summary"]
    campaign_summary = st.session_state["campaign_summary"]
    cohort_matrices = st.session_state["cohort_matrices"]
    segmentation = st.session_state["segmentation"]
    both_business = st.session_state["both_business"]
    program_summary = st.session_state["program_summary"]
    output_path = st.session_state["output_path"]

    # ══════════════════════════════════════════════════════════════════════
    #  BI Dashboard (Phase 9)
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("---")

    # ── Helper: render a financial brand tab ──────────────────────────────
    def _render_financial_tab(brand_key: str, emoji: str) -> None:
        """Render KPIs, GGR chart, full table, and cohort matrix for a brand."""
        bdf = (
            financial_summary[financial_summary["brand"] == brand_key]
            .sort_values("month")
            .reset_index(drop=True)
        )
        if bdf.empty:
            st.warning(f"No data for {brand_key}")
            return

        latest = bdf.iloc[-1]
        prev = bdf.iloc[-2] if len(bdf) > 1 else None

        def _delta(col: str) -> float | None:
            if prev is not None and col in bdf.columns:
                return float(latest[col] - prev[col])
            return None

        # ── Top-line KPI cards ────────────────────────────────────────────
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        with k1:
            st.metric("GGR", f"${latest['ggr']:,.0f}", delta=f"${_delta('ggr'):,.0f}" if _delta('ggr') is not None else None)
        with k2:
            st.metric("Total Handle", f"${latest['total_handle']:,.0f}", delta=f"${_delta('total_handle'):,.0f}" if _delta('total_handle') is not None else None)
        with k3:
            st.metric("Hold %", f"{latest['hold_pct']:.2f}%")
        with k4:
            st.metric("Total Players", f"{int(latest['total_players']):,}", delta=f"{int(_delta('total_players')):,}" if _delta('total_players') is not None else None)
        with k5:
            st.metric("Retention %", f"{latest['retention_pct']:.1f}%")
        with k6:
            st.metric("Profitable %", f"{latest['profitable_pct']:.1f}%")

        # ── GGR Trend Chart ───────────────────────────────────────────────
        st.markdown("#### 📈 GGR Month-over-Month")
        chart_data = bdf[["month", "ggr"]].set_index("month")
        st.bar_chart(chart_data, use_container_width=True)

        # ── Player Demographics Chart ─────────────────────────────────────
        st.markdown(f"#### > {brand_key.upper()} PLAYER DEMOGRAPHICS (MONTH OVER MONTH)_")
        demo_df = bdf[["month", "total_players", "profitable_players", "negative_yield_players"]].copy()
        demo_df = demo_df.rename(columns={
            "total_players": "Total Players",
            "profitable_players": "Profitable (Winners)",
            "negative_yield_players": "Neg. Yield (Losers)",
        })
        st.line_chart(
            demo_df.set_index("month"),
            use_container_width=True,
            color=["#AAAAAA", "#00FF41", "#FF4444"],
        )

        # ── Comparative Intelligence (brand-level) ────────────────────────
        # Map total_handle→turnover for time-series compatibility
        bdf_ts = bdf.rename(columns={"total_handle": "turnover"})
        brand_ts = _cached_time_series(bdf_ts)
        brand_ts_m = brand_ts["monthly"]
        brand_ts_q = brand_ts["quarterly"]

        if not brand_ts_m.empty:
            # ── Smart Narrative (brand-level) ───────────────────────
            b_whale = float(latest.get("top_10_pct_ggr_share", 0))
            b_margin = float(latest.get("hold_pct", 0))  # hold_pct = margin for brands
            b_narrative = generate_smart_narrative(brand_ts_m.iloc[-1], b_margin, b_whale)
            if b_margin < 2.5 or b_whale >= 70:
                st.warning(b_narrative)
            else:
                st.info(b_narrative)

            st.markdown(f"#### > {brand_key.upper()} COMPARATIVE INTELLIGENCE_")
            b_latest_m = brand_ts_m.iloc[-1]
            b_latest_q = brand_ts_q.iloc[-1] if not brand_ts_q.empty else None

            def _b_arrow(val):
                if pd.isna(val): return "—"
                return f"↑ {val:+,.0f}" if val >= 0 else f"↓ {val:,.0f}"

            def _b_arrow_pct(val):
                if pd.isna(val): return "—"
                return f"↑ {val:+.1f}%" if val >= 0 else f"↓ {val:.1f}%"

            # [ FINANCIALS ]
            st.markdown("##### 💰 Financials")
            b_fin_cols = ["turnover", "ggr", "revenue_share_deduction"]
            b_fin_labels = ["Turnover", "GGR", "Revenue (15%)"]
            b_fin_rows = []
            for col, label in zip(b_fin_cols, b_fin_labels):
                row = {"Metric": label}
                row["MoM Δ"] = _b_arrow(b_latest_m.get(f"{col}_mom_delta"))
                row["MoM %"] = _b_arrow_pct(b_latest_m.get(f"{col}_mom_pct"))
                row["YoY Δ"] = _b_arrow(b_latest_m.get(f"{col}_yoy_delta"))
                row["YoY %"] = _b_arrow_pct(b_latest_m.get(f"{col}_yoy_pct"))
                row["YTD"] = f"${b_latest_m.get(f'{col}_ytd', 0):,.0f}"
                if b_latest_q is not None:
                    row["QoQ Δ"] = _b_arrow(b_latest_q.get(f"{col}_qoq_delta"))
                    row["QoQ %"] = _b_arrow_pct(b_latest_q.get(f"{col}_qoq_pct"))
                b_fin_rows.append(row)
            st.dataframe(pd.DataFrame(b_fin_rows), use_container_width=True, hide_index=True)

            # EOY Projected metrics — Dual Engine (brand-level)
            b_eoy_rows = []
            for proj_col, proj_label in [("ggr", "GGR"), ("turnover", "Turnover"), ("revenue_share_deduction", "Revenue 15%")]:
                for engine, prefix in [("Seasonal", "eoy_seasonal"), ("Momentum", "eoy_momentum")]:
                    eoy_key = f"{prefix}_{proj_col}"
                    eoy_val = b_latest_m.get(eoy_key, 0) or 0
                    b_eoy_rows.append({"Metric": f"EOY {proj_label} ({engine})", "MoM Δ": "—", "MoM %": "—",
                                       "YoY Δ": "—", "YoY %": "—",
                                       "YTD": f"${eoy_val:,.0f}"})
            if b_eoy_rows:
                st.dataframe(pd.DataFrame(b_eoy_rows), use_container_width=True, hide_index=True)
            st.caption("🔮 **EOY PROJECTIONS:** Seasonal uses prior-year proportional scaling. Momentum uses 3-month rolling average × remaining months.")

            # [ PLAYER DEMOGRAPHICS ]
            st.markdown("##### 👥 Player Demographics")
            b_plr_cols = ["total_players", "profitable_players", "negative_yield_players", "conversions", "new_players", "reactivated_players", "returning_players"]
            b_plr_labels = ["Total Active", "Profitable (Winners)", "Neg. Yield (Losers)", "Conversions", "New Players", "Reactivated", "Returning (Retained)"]
            b_plr_rows = []
            for col, label in zip(b_plr_cols, b_plr_labels):
                row = {"Metric": label}
                row["MoM Δ"] = _b_arrow(b_latest_m.get(f"{col}_mom_delta"))
                row["MoM %"] = _b_arrow_pct(b_latest_m.get(f"{col}_mom_pct"))
                row["YoY Δ"] = _b_arrow(b_latest_m.get(f"{col}_yoy_delta"))
                row["YoY %"] = _b_arrow_pct(b_latest_m.get(f"{col}_yoy_pct"))
                row["YTD"] = f"{int(b_latest_m.get(f'{col}_ytd', 0)):,}"
                if b_latest_q is not None:
                    row["QoQ Δ"] = _b_arrow(b_latest_q.get(f"{col}_qoq_delta"))
                    row["QoQ %"] = _b_arrow_pct(b_latest_q.get(f"{col}_qoq_pct"))
                b_plr_rows.append(row)
            st.dataframe(pd.DataFrame(b_plr_rows), use_container_width=True, hide_index=True)

        # ── Risk & Value Metrics (brand-level) ─────────────────────
        st.markdown(f"#### > {brand_key.upper()} RISK & VALUE METRICS_")
        brv1, brv2 = st.columns(2)
        with brv1:
            st.metric("Turnover Per Player",
                      f"${float(latest.get('turnover_per_player', 0)):,.2f}")
        with brv2:
            st.metric("Whale Dependency (Top 10% GGR)",
                      f"{float(latest.get('top_10_pct_ggr_share', 0)):.2f}%")

        # Revenue Composition (brand)
        if "new_player_ggr" in bdf.columns and "returning_player_ggr" in bdf.columns:
            st.markdown("##### 📊 Revenue Composition: New vs Returning Player GGR")
            b_rev = bdf[["month", "new_player_ggr", "returning_player_ggr"]].copy()
            b_rev = b_rev.rename(columns={"month": "Month", "new_player_ggr": "New_Player_GGR", "returning_player_ggr": "Returning_Player_GGR"})
            b_rev["New (Profit)"] = b_rev["New_Player_GGR"].clip(lower=0)
            b_rev["New (Loss)"] = b_rev["New_Player_GGR"].clip(upper=0)
            b_rev["Returning (Profit)"] = b_rev["Returning_Player_GGR"].clip(lower=0)
            b_rev["Returning (Loss)"] = b_rev["Returning_Player_GGR"].clip(upper=0)
            st.bar_chart(b_rev, x="Month",
                         y=["New (Profit)", "New (Loss)", "Returning (Profit)", "Returning (Loss)"],
                         color=["#00FF41", "#FF0000", "#CCCCCC", "#804040"])

        # RFM Tiering (brand-filtered)
        b_latest_month = bdf["month"].max()
        brand_raw = df[df["brand"] == brand_key]
        b_rfm = _cached_rfm_summary(brand_raw, b_latest_month)
        if not b_rfm.empty:
            st.markdown(f"##### 🏆 VIP Tiering — RFM Segmentation ({b_latest_month})")
            bt1, bt2, bt3 = st.columns(3)
            for col_w, tier_name in [(bt1, "True VIP"), (bt2, "Churn Risk"), (bt3, "Casual")]:
                tier_row = b_rfm[b_rfm["Tier"] == tier_name]
                players = int(tier_row["Players"].iloc[0]) if not tier_row.empty else 0
                ggr_v = float(tier_row["GGR"].iloc[0]) if not tier_row.empty else 0.0
                with col_w:
                    st.metric(tier_name, f"{players:,} players")
                    st.caption(f"GGR: ${ggr_v:,.2f}")
            st.dataframe(b_rfm, use_container_width=True, hide_index=True,
                         column_config={
                             "Tier": st.column_config.TextColumn("Tier"),
                             "Players": st.column_config.NumberColumn("Players", format="%d"),
                             "GGR": st.column_config.NumberColumn("GGR", format="$%.2f"),
                         })

        # ── Full Data Table ───────────────────────────────────────────────
        with st.expander(f"📋 {brand_key} — Full Financial Data ({len(bdf)} months)", expanded=False):
            st.dataframe(
                bdf,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "month": st.column_config.TextColumn("Month"),
                    "brand": st.column_config.TextColumn("Brand"),
                    "negative_yield_players": st.column_config.NumberColumn("Losers", format="%d"),
                    "profitable_players": st.column_config.NumberColumn("Winners", format="%d"),
                    "flat": st.column_config.NumberColumn("Flat", format="%d"),
                    "total_players": st.column_config.NumberColumn("Total Players", format="%d"),
                    "profitable_pct": st.column_config.NumberColumn("Winners %", format="%.2f%%"),
                    "ggr": st.column_config.NumberColumn("GGR", format="$%.2f"),
                    "total_handle": st.column_config.NumberColumn("Turnover", format="$%.2f"),
                    "hold_pct": st.column_config.NumberColumn("Hold %", format="%.2f%%"),
                    "ggr_per_player": st.column_config.NumberColumn("GGR/Player", format="$%.2f"),
                    "top_10_pct_ggr_share": st.column_config.NumberColumn("Top 10% GGR", format="%.1f%%"),
                    "new_players": st.column_config.NumberColumn("New Players", format="%d"),
                    "returning_players": st.column_config.NumberColumn("Returning", format="%d"),
                    "retention_pct": st.column_config.NumberColumn("Retention %", format="%.2f%%"),
                },
            )

        # ── Cohort Matrix ─────────────────────────────────────────────────
        if cohort_matrices and brand_key in cohort_matrices:
            matrix = cohort_matrices[brand_key]
            if not matrix.empty:
                with st.expander(f"🔄 {brand_key} — Cohort Retention Matrix", expanded=False):
                    st.dataframe(
                        matrix.style.format("{:.1f}%", na_rep="—"),
                        use_container_width=True,
                    )

        # ── Cohort Retention Heatmap (Phase 18) ──────────────────────────
        st.markdown("---")
        st.markdown("#### > COHORT RETENTION HEATMAP_")
        brand_raw = df[df["brand"] == brand_key]
        heatmap_fig = _cached_retention_heatmap(brand_raw)
        if heatmap_fig is not None:
            st.plotly_chart(heatmap_fig, use_container_width=True, config={"scrollZoom": False})
        else:
            st.info("Not enough data to generate a retention heatmap.")

        # ── Cumulative LTV Curves ────────────────────────────────────
        st.markdown("---")
        st.markdown("#### > CUMULATIVE LTV TRAJECTORY_")
        st.markdown("*Insight: Tracks the cumulative revenue generation of player cohorts over time to determine break-even points and long-term value.*")
        ltv_fig = _cached_ltv_curves(brand_raw)
        if ltv_fig is not None:
            st.plotly_chart(ltv_fig, use_container_width=True, config={"scrollZoom": False})
        else:
            st.info("Not enough data to generate LTV curves.")

        # ── Segmentation by Program ─────────────────────────────────
        if program_summary is not None and not program_summary.empty:
            brand_progs = program_summary[program_summary["brand"] == brand_key]
            if not brand_progs.empty:
                st.markdown("---")
                st.markdown("#### > SEGMENTATION BY PROGRAM_")
                st.markdown("*Insight: Evaluates the financial efficiency and house edge (Margin) across different marketing programs (ACQ, RET, WB).*")
                st.dataframe(
                    brand_progs,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "brand": st.column_config.TextColumn("Brand"),
                        "month": st.column_config.TextColumn("Month"),
                        "Program": st.column_config.TextColumn("Program"),
                        "ggr": st.column_config.NumberColumn("GGR", format="$%d"),
                        "Turnover": st.column_config.NumberColumn("Turnover", format="$%d"),
                        "Margin": st.column_config.NumberColumn("Margin", format="%.2f%%"),
                        "total_players": st.column_config.NumberColumn("Players", format="%d"),
                    },
                )

    # ── Dashboard tabs ────────────────────────────────────────────────────
    # ── Dashboard tabs ────────────────────────────────────────────────────
    tab_exec, tab_both, tab_roja, tab_latri, tab_campaigns, tab_crm = st.tabs([
        "📊 Executive Summary",
        "🏦 Combined Deep-Dive",
        "🔴 Rojabet",
        "🟢 Latribet",
        "📈 Campaigns",
        "🕵️ CRM Intelligence",
    ])

    # ═════════════════════════════════════════════════════════════════════
    #  TAB: Executive Summary (Phase 16)
    # ═════════════════════════════════════════════════════════════════════
    with tab_exec:
        # ── System Diagnostic (Combined) ──────────────────────────────────
        if not both_business.empty:
            exec_bb = both_business.iloc[-1]
            exec_ts = _cached_time_series(both_business)
            exec_ts_m = exec_ts["monthly"]

            if not exec_ts_m.empty:
                exec_latest = exec_ts_m.iloc[-1]
                combined_fin = financial_summary[
                    financial_summary["brand"] == "Combined"
                ].sort_values("month").iloc[-1]
                e_whale = float(combined_fin.get("top_10_pct_ggr_share", 0))
                e_margin = float(exec_bb.get("margin", 0))
                e_narrative = generate_smart_narrative(exec_latest, e_margin, e_whale)
                if e_margin < 2.5 or e_whale >= 70:
                    st.warning(e_narrative)
                else:
                    st.info(e_narrative)

            # ── Cross-Brand Comparison Matrix ─────────────────────────────
            st.markdown("#### > CROSS-BRAND EXECUTIVE MATRIX_")
            st.markdown("*Insight: Tracks core revenue generation, operating margin safety, and top-line agency commissions across all entities.*")

            latest_month = both_business["month"].max()

            # MoM mapping: metric label → financial_summary column name
            _mom_map = {
                "Turnover": "total_handle",
                "GGR": "ggr",
                "Margin %": "hold_pct",
                "Revenue (15%)": "revenue_share_deduction",
                "Conversions": "conversions",
                "Turnover / Player": "turnover_per_player",
                "Whale Risk %": None,  # no MoM
            }

            def _brand_snapshot(brand_name: str) -> dict:
                """Extract key metrics for a brand in the latest month."""
                bdata = financial_summary[
                    (financial_summary["brand"] == brand_name)
                    & (financial_summary["month"] == latest_month)
                ]
                if bdata.empty:
                    return {}
                row = bdata.iloc[0]
                return {
                    "Turnover": float(row.get("total_handle", 0)),
                    "GGR": float(row.get("ggr", 0)),
                    "Margin %": float(row.get("hold_pct", 0)),
                    "Revenue (15%)": float(row.get("revenue_share_deduction", 0)),
                    "Conversions": int(row.get("conversions", 0)),
                    "Turnover / Player": float(row.get("turnover_per_player", 0)),
                    "Whale Risk %": float(row.get("top_10_pct_ggr_share", 0)),
                }

            def _brand_mom(brand_name: str) -> list:
                """Calculate MoM % change for each metric, formatted as string."""
                bdata = financial_summary[
                    financial_summary["brand"] == brand_name
                ].sort_values("month")
                if len(bdata) < 2:
                    return ["N/A"] * len(metrics_list)
                curr = bdata.iloc[-1]
                prev = bdata.iloc[-2]
                results = []
                for metric in metrics_list:
                    col = _mom_map.get(metric)
                    if col is None:
                        results.append("N/A")
                        continue
                    c_val = float(curr.get(col, 0))
                    p_val = float(prev.get(col, 0))
                    if p_val == 0:
                        results.append("N/A")
                    else:
                        pct = ((c_val - p_val) / abs(p_val)) * 100
                        results.append(f"{pct:+.2f}%")
                return results

            def _bb_mom() -> list:
                """MoM for Both Business (combined)."""
                if len(both_business) < 2:
                    return ["N/A"] * len(metrics_list)
                curr = both_business.iloc[-1]
                prev = both_business.iloc[-2]
                bb_mom_map = {
                    "Turnover": "turnover",
                    "GGR": "ggr",
                    "Margin %": "margin",
                    "Revenue (15%)": "revenue_share_deduction",
                    "Conversions": "conversions",
                    "Turnover / Player": "turnover_per_player",
                    "Whale Risk %": None,
                }
                results = []
                for metric in metrics_list:
                    col = bb_mom_map.get(metric)
                    if col is None:
                        results.append("N/A")
                        continue
                    c_val = float(curr.get(col, 0))
                    p_val = float(prev.get(col, 0))
                    if p_val == 0:
                        results.append("N/A")
                    else:
                        pct = ((c_val - p_val) / abs(p_val)) * 100
                        results.append(f"{pct:+.2f}%")
                return results

            bb_snap = {
                "Turnover": float(exec_bb.get("turnover", 0)),
                "GGR": float(exec_bb.get("ggr", 0)),
                "Margin %": float(exec_bb.get("margin", 0)),
                "Revenue (15%)": float(exec_bb.get("revenue_share_deduction", 0)),
                "Conversions": int(exec_bb.get("conversions", 0)),
                "Turnover / Player": float(exec_bb.get("turnover_per_player", 0)),
                "Whale Risk %": e_whale,
            }
            roja_snap = _brand_snapshot("Rojabet")
            latri_snap = _brand_snapshot("Latribet")

            def _brand_yoy(brand_name: str) -> list:
                """Extract YoY % from pre-computed time series (_yoy_pct via shift(12))."""
                bdata = financial_summary[
                    financial_summary["brand"] == brand_name
                ].sort_values("month")
                if bdata.empty:
                    return ["-"] * len(metrics_list)
                brand_ts = _cached_time_series(bdata)
                brand_ts_m = brand_ts.get("monthly", pd.DataFrame())
                if brand_ts_m.empty:
                    return ["-"] * len(metrics_list)
                latest = brand_ts_m.iloc[-1]
                results = []
                for metric in metrics_list:
                    col = _mom_map.get(metric)
                    if col is None:
                        results.append("N/A")
                        continue
                    yoy_key = f"{col}_yoy_pct"
                    val = latest.get(yoy_key)
                    if val is None or pd.isna(val):
                        results.append("-")
                    else:
                        results.append(f"{val:+.1f}%")
                return results

            def _bb_yoy() -> list:
                """Extract YoY % from combined time series."""
                if exec_ts_m.empty:
                    return ["-"] * len(metrics_list)
                bb_ts_map = {
                    "Turnover": "turnover",
                    "GGR": "ggr",
                    "Margin %": "margin",
                    "Revenue (15%)": "revenue_share_deduction",
                    "Conversions": "conversions",
                    "Turnover / Player": "turnover_per_player",
                    "Whale Risk %": None,
                }
                results = []
                for metric in metrics_list:
                    col = bb_ts_map.get(metric)
                    if col is None:
                        results.append("N/A")
                        continue
                    yoy_key = f"{col}_yoy_pct"
                    val = exec_latest.get(yoy_key)
                    if val is None or pd.isna(val):
                        results.append("-")
                    else:
                        results.append(f"{val:+.1f}%")
                return results

            metrics_list = ["Turnover", "GGR", "Margin %", "Revenue (15%)",
                            "Conversions", "Turnover / Player", "Whale Risk %"]
            matrix_data = {
                "Metric": metrics_list,
                "Combined": [bb_snap.get(m, 0) for m in metrics_list],
                "Combined MoM": _bb_mom(),
                "Combined YoY": _bb_yoy(),
                "Rojabet": [roja_snap.get(m, 0) for m in metrics_list],
                "Rojabet MoM": _brand_mom("Rojabet"),
                "Rojabet YoY": _brand_yoy("Rojabet"),
                "Latribet": [latri_snap.get(m, 0) for m in metrics_list],
                "Latribet MoM": _brand_mom("Latribet"),
                "Latribet YoY": _brand_yoy("Latribet"),
            }
            matrix_df = pd.DataFrame(matrix_data)

            st.dataframe(
                matrix_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Metric": st.column_config.TextColumn("Metric"),
                    "Combined": st.column_config.NumberColumn("Combined", format="%.2f"),
                    "Combined MoM": st.column_config.TextColumn("Combined MoM"),
                    "Combined YoY": st.column_config.TextColumn("Combined YoY"),
                    "Rojabet": st.column_config.NumberColumn("Rojabet", format="%.2f"),
                    "Rojabet MoM": st.column_config.TextColumn("Rojabet MoM"),
                    "Rojabet YoY": st.column_config.TextColumn("Rojabet YoY"),
                    "Latribet": st.column_config.NumberColumn("Latribet", format="%.2f"),
                    "Latribet MoM": st.column_config.TextColumn("Latribet MoM"),
                    "Latribet YoY": st.column_config.TextColumn("Latribet YoY"),
                },
            )
            st.caption(f"Snapshot: {latest_month}")
            st.caption("🐳 **WHALE RISK %:** The percentage of total monthly GGR generated by the top 10% of players. Values > 70% indicate extreme revenue concentration risk.")

            # ── Brand vs Brand Trajectory ─────────────────────────────────
            st.markdown("#### > BRAND vs BRAND TRAJECTORY_")

            roja_ts = financial_summary[
                financial_summary["brand"] == "Rojabet"
            ][["month", "ggr"]].sort_values("month")
            latri_ts = financial_summary[
                financial_summary["brand"] == "Latribet"
            ][["month", "ggr"]].sort_values("month")

            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=roja_ts["month"], y=roja_ts["ggr"],
                name="Rojabet", marker_color="#FF4444",
            ))
            fig.add_trace(go.Bar(
                x=latri_ts["month"], y=latri_ts["ggr"],
                name="Latribet", marker_color="#00FF41",
            ))
            fig.update_layout(
                barmode="group",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#00FF41",
                legend=dict(font=dict(color="#00FF41")),
                xaxis=dict(gridcolor="#1a1a1a"),
                yaxis=dict(gridcolor="#1a1a1a", tickprefix="$", tickformat=",.0f"),
                margin=dict(l=0, r=0, t=30, b=0),
                height=400,
            )
            st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": False})

            # ── Cross-Brand Demographics ─────────────────────────────────
            st.markdown("---")
            st.markdown("#### > CROSS-BRAND DEMOGRAPHICS_")
            st.markdown("*Insight: Evaluates acquisition velocity, retention strength, and the balance of players winning vs. losing against the house.*")

            demo_metrics = [
                ("Total Active", "total_players"),
                ("Conversions", "conversions"),
                ("New Players", "new_players"),
                ("Reactivated", "reactivated_players"),
                ("Retained", "returning_players"),
                ("Profitable", "profitable_players"),
                ("Neg. Yield", "negative_yield_players"),
            ]

            def _demo_snap(brand_name: str) -> list:
                bdata = financial_summary[
                    (financial_summary["brand"] == brand_name)
                    & (financial_summary["month"] == latest_month)
                ]
                if bdata.empty:
                    return [0] * len(demo_metrics)
                row = bdata.iloc[0]
                return [int(row.get(col, 0)) for _, col in demo_metrics]

            # BB uses both_business which has different column names
            bb_demo = [int(exec_bb.get(col, 0)) for _, col in demo_metrics]

            demo_matrix = pd.DataFrame({
                "Metric": [label for label, _ in demo_metrics],
                "Both Business": bb_demo,
                "Rojabet": _demo_snap("Rojabet"),
                "Latribet": _demo_snap("Latribet"),
            })
            st.dataframe(
                demo_matrix,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Metric": st.column_config.TextColumn("Metric"),
                    "Both Business": st.column_config.NumberColumn("Both Business", format="%d"),
                    "Rojabet": st.column_config.NumberColumn("Rojabet", format="%d"),
                    "Latribet": st.column_config.NumberColumn("Latribet", format="%d"),
                },
            )

            # ── Cross-Brand VIP Health ────────────────────────────────────
            st.markdown("---")
            st.markdown("#### > CROSS-BRAND VIP HEALTH_")
            st.markdown("*Insight: Assesses long-term revenue sustainability by tracking active VIPs against high-value players at risk of churning.*")

            tier_labels = ["True VIPs", "Churn Risk VIPs", "Casuals"]
            tier_search = ["True VIP", "Churn Risk", "Casual"]

            def _vip_snap(raw_subset):
                rfm = _cached_rfm_summary(raw_subset, latest_month)
                if rfm.empty:
                    return [0] * len(tier_labels)
                results = []
                for search in tier_search:
                    mask = rfm.iloc[:, 0].str.contains(search, na=False, case=False)
                    results.append(int(rfm.loc[mask, rfm.columns[1]].sum()) if mask.any() else 0)
                return results

            vip_matrix = pd.DataFrame({
                "Tier": tier_labels,
                "Both Business": _vip_snap(df),
                "Rojabet": _vip_snap(df[df["brand"] == "Rojabet"]),
                "Latribet": _vip_snap(df[df["brand"] == "Latribet"]),
            })
            st.dataframe(
                vip_matrix,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Tier": st.column_config.TextColumn("Tier"),
                    "Both Business": st.column_config.NumberColumn("Both Business", format="%d"),
                    "Rojabet": st.column_config.NumberColumn("Rojabet", format="%d"),
                    "Latribet": st.column_config.NumberColumn("Latribet", format="%d"),
                },
            )

            # ── Cross-Brand Cannibalization ──────────────────────────────
            st.markdown("---")
            st.markdown("#### > CROSS-BRAND CANNIBALIZATION (ALL-TIME)_")
            st.markdown("*Insight: Identifies players active on both platforms to expose duplicate customer acquisition costs and shared revenue dependency.*")

            overlap = generate_overlap_stats(df)
            ov1, ov2 = st.columns(2)
            with ov1:
                st.metric("Shared Players (Overlap)", f"{overlap['overlap_count']:,}")
            with ov2:
                st.metric("Shared Lifetime GGR", f"${overlap['overlap_ggr']:,.2f}")
        else:
            st.warning("No financial data available for Executive Summary.")

    with tab_both:
        if not both_business.empty:
            bb_latest = both_business.iloc[-1]
            bb_prev = both_business.iloc[-2] if len(both_business) > 1 else None

            def _bb_delta(col: str):
                if bb_prev is not None and col in both_business.columns:
                    return float(bb_latest[col] - bb_prev[col])
                return None

            # KPI cards
            k1, k2, k3, k4, k5 = st.columns(5)
            with k1:
                st.metric("Turnover", f"${bb_latest['turnover']:,.2f}",
                          delta=f"${_bb_delta('turnover'):,.2f}" if _bb_delta('turnover') is not None else None)
            with k2:
                st.metric("GGR", f"${bb_latest['ggr']:,.2f}",
                          delta=f"${_bb_delta('ggr'):,.2f}" if _bb_delta('ggr') is not None else None)
            with k3:
                st.metric("Revenue (15%)", f"${bb_latest['revenue_share_deduction']:,.2f}",
                          delta=f"${_bb_delta('revenue_share_deduction'):,.2f}" if _bb_delta('revenue_share_deduction') is not None else None)
            with k4:
                st.metric("Margin", f"{bb_latest['margin']:.2f}%")
            with k5:
                st.metric("Total Players", f"{int(bb_latest['total_players']):,}",
                          delta=f"{int(_bb_delta('total_players')):,}" if _bb_delta('total_players') is not None else None)

            # GGR trend chart
            st.markdown("#### 📈 Combined GGR Month-over-Month")
            chart_data = both_business[["month", "ggr"]].set_index("month")
            st.bar_chart(chart_data, use_container_width=True)

            # ── Player Demographics Chart ───────────────────────────────
            st.markdown("#### > COMBINED PLAYER DEMOGRAPHICS (MONTH OVER MONTH)_")
            demo_bb = both_business[["month", "total_players", "profitable_players", "negative_yield_players"]].copy()
            demo_bb = demo_bb.rename(columns={
                "total_players": "Total Players",
                "profitable_players": "Profitable (Winners)",
                "negative_yield_players": "Neg. Yield (Losers)",
            })
            st.line_chart(
                demo_bb.set_index("month"),
                use_container_width=True,
                color=["#AAAAAA", "#00FF41", "#FF4444"],
            )

            # ── Comparative Intelligence (Phase 11) ───────────────────
            ts = _cached_time_series(both_business)
            ts_m = ts["monthly"]
            ts_q = ts["quarterly"]

            if not ts_m.empty:
                st.markdown("#### > COMPARATIVE INTELLIGENCE_")
                latest_m = ts_m.iloc[-1]
                latest_q = ts_q.iloc[-1] if not ts_q.empty else None

                # ── Smart Narrative (Phase 15) ────────────────────────
                combined_fin_latest = financial_summary[
                    financial_summary["brand"] == "Combined"
                ].sort_values("month").iloc[-1]
                whale_dep = float(combined_fin_latest.get("top_10_pct_ggr_share", 0))
                margin_val = float(bb_latest.get("margin", 0))
                narrative = generate_smart_narrative(latest_m, margin_val, whale_dep)
                if margin_val < 2.5 or whale_dep >= 70:
                    st.warning(narrative)
                else:
                    st.info(narrative)

                def _arrow(val):
                    if pd.isna(val): return "—"
                    return f"↑ {val:+,.0f}" if val >= 0 else f"↓ {val:,.0f}"

                def _arrow_pct(val):
                    if pd.isna(val): return "—"
                    return f"↑ {val:+.1f}%" if val >= 0 else f"↓ {val:.1f}%"

                # Financials group
                st.markdown("##### 💰 Financials")
                fin_cols = ["turnover", "ggr", "revenue_share_deduction"]
                fin_labels = ["Turnover", "GGR", "Revenue (15%)"]
                fin_rows = []
                for col, label in zip(fin_cols, fin_labels):
                    row = {"Metric": label}
                    row["MoM Δ"] = _arrow(latest_m.get(f"{col}_mom_delta"))
                    row["MoM %"] = _arrow_pct(latest_m.get(f"{col}_mom_pct"))
                    row["YoY Δ"] = _arrow(latest_m.get(f"{col}_yoy_delta"))
                    row["YoY %"] = _arrow_pct(latest_m.get(f"{col}_yoy_pct"))
                    row["YTD"] = f"${latest_m.get(f'{col}_ytd', 0):,.0f}"
                    if latest_q is not None:
                        row["QoQ Δ"] = _arrow(latest_q.get(f"{col}_qoq_delta"))
                        row["QoQ %"] = _arrow_pct(latest_q.get(f"{col}_qoq_pct"))
                    fin_rows.append(row)

                # EOY Projected metrics — Dual Engine (Phase 15 upgrade)
                eoy_rows = []
                for proj_col, proj_label in [("ggr", "GGR"), ("turnover", "Turnover"), ("revenue_share_deduction", "Revenue 15%")]:
                    for engine, prefix in [("Seasonal", "eoy_seasonal"), ("Momentum", "eoy_momentum")]:
                        eoy_key = f"{prefix}_{proj_col}"
                        eoy_val = latest_m.get(eoy_key, 0) or 0
                        fin_rows.append({"Metric": f"EOY {proj_label} ({engine})", "MoM Δ": "—", "MoM %": "—",
                                         "YoY Δ": "—", "YoY %": "—",
                                         "YTD": f"${eoy_val:,.0f}"})
                st.dataframe(pd.DataFrame(fin_rows), use_container_width=True, hide_index=True)
                st.caption("🔮 **EOY PROJECTIONS:** Seasonal uses prior-year proportional scaling. Momentum uses 3-month rolling average × remaining months.")

                # Player Demographics group
                st.markdown("##### 👥 Player Demographics")
                plr_cols = ["total_players", "profitable_players", "negative_yield_players", "conversions", "new_players", "reactivated_players", "returning_players"]
                plr_labels = ["Total Active", "Profitable (Winners)", "Neg. Yield (Losers)", "Conversions", "New Players", "Reactivated Players", "Returning Players"]
                plr_rows = []
                for col, label in zip(plr_cols, plr_labels):
                    row = {"Metric": label}
                    row["MoM Δ"] = _arrow(latest_m.get(f"{col}_mom_delta"))
                    row["MoM %"] = _arrow_pct(latest_m.get(f"{col}_mom_pct"))
                    row["YoY Δ"] = _arrow(latest_m.get(f"{col}_yoy_delta"))
                    row["YoY %"] = _arrow_pct(latest_m.get(f"{col}_yoy_pct"))
                    row["YTD"] = f"{int(latest_m.get(f'{col}_ytd', 0)):,}"
                    if latest_q is not None:
                        row["QoQ Δ"] = _arrow(latest_q.get(f"{col}_qoq_delta"))
                        row["QoQ %"] = _arrow_pct(latest_q.get(f"{col}_qoq_pct"))
                    plr_rows.append(row)
                st.dataframe(pd.DataFrame(plr_rows), use_container_width=True, hide_index=True)

            # ── Risk & Value Metrics (Phase 12) ─────────────────────────
            st.markdown("#### > RISK & VALUE METRICS_")
            rv1, rv2 = st.columns(2)
            with rv1:
                st.metric("Turnover Per Player",
                          f"${bb_latest['turnover_per_player']:,.2f}")
            with rv2:
                top10 = financial_summary[
                    financial_summary["brand"] == "Combined"
                ].sort_values("month").iloc[-1].get("top_10_pct_ggr_share", 0)
                st.metric("Whale Dependency (Top 10% GGR)",
                          f"{top10:.2f}%")

            # Revenue Composition chart
            st.markdown("##### 📊 Revenue Composition: New vs Returning Player GGR")
            rev_comp = both_business[["month", "new_player_ggr", "returning_player_ggr"]].copy()
            rev_comp = rev_comp.rename(columns={"month": "Month", "new_player_ggr": "New_Player_GGR", "returning_player_ggr": "Returning_Player_GGR"})
            rev_comp["New (Profit)"] = rev_comp["New_Player_GGR"].clip(lower=0)
            rev_comp["New (Loss)"] = rev_comp["New_Player_GGR"].clip(upper=0)
            rev_comp["Returning (Profit)"] = rev_comp["Returning_Player_GGR"].clip(lower=0)
            rev_comp["Returning (Loss)"] = rev_comp["Returning_Player_GGR"].clip(upper=0)
            st.bar_chart(rev_comp, x="Month",
                         y=["New (Profit)", "New (Loss)", "Returning (Profit)", "Returning (Loss)"],
                         color=["#00FF41", "#FF0000", "#CCCCCC", "#804040"])

            # ── VIP Tiering (Phase 15 - RFM) ─────────────────────────
            latest_month_str = both_business["month"].max()
            rfm = _cached_rfm_summary(df, latest_month_str)
            if not rfm.empty:
                st.markdown(f"##### 🏆 VIP Tiering — RFM Segmentation ({latest_month_str})")
                t1, t2, t3 = st.columns(3)
                for i, (col_widget, tier_name, color) in enumerate([
                    (t1, "True VIP", "#00FF41"),
                    (t2, "Churn Risk", "#FF4444"),
                    (t3, "Casual", "#AAAAAA"),
                ]):
                    tier_row = rfm[rfm["Tier"] == tier_name]
                    players = int(tier_row["Players"].iloc[0]) if not tier_row.empty else 0
                    ggr = float(tier_row["GGR"].iloc[0]) if not tier_row.empty else 0.0
                    with col_widget:
                        st.metric(tier_name, f"{players:,} players")
                        st.caption(f"GGR: ${ggr:,.2f}")
                st.dataframe(
                    rfm,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Tier": st.column_config.TextColumn("Tier"),
                        "Players": st.column_config.NumberColumn("Players", format="%d"),
                        "GGR": st.column_config.NumberColumn("GGR", format="$%.2f"),
                    },
                )

            # Full Both Business table
            with st.expander(f"📋 Both Business Summary ({len(both_business)} months)", expanded=True):
                st.dataframe(
                    both_business,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "month": st.column_config.TextColumn("Month"),
                        "turnover": st.column_config.NumberColumn("Turnover", format="$%.2f"),
                        "ggr": st.column_config.NumberColumn("GGR", format="$%.2f"),
                        "margin": st.column_config.NumberColumn("Margin %", format="%.2f%%"),
                        "revenue_share_deduction": st.column_config.NumberColumn("Rev Share (15%)", format="$%.2f"),
                        "net_income": st.column_config.NumberColumn("Net Income", format="$%.2f"),
                        "new_players": st.column_config.NumberColumn("New Players", format="%d"),
                        "returning_players": st.column_config.NumberColumn("Returning", format="%d"),
                        "reactivated_players": st.column_config.NumberColumn("Reactivated", format="%d"),
                        "conversions": st.column_config.NumberColumn("Conversions", format="%d"),
                        "total_players": st.column_config.NumberColumn("Total Players", format="%d"),
                        "profitable_players": st.column_config.NumberColumn("Winners", format="%d"),
                        "negative_yield_players": st.column_config.NumberColumn("Losers", format="%d"),
                        "new_players_pct": st.column_config.NumberColumn("New %", format="%.2f%%"),
                        "returning_players_pct": st.column_config.NumberColumn("Returning %", format="%.2f%%"),
                        "ggr_per_player": st.column_config.NumberColumn("GGR/Player", format="$%.2f"),
                        "turnover_per_player": st.column_config.NumberColumn("Turnover/Player", format="$%.2f"),
                        "income_per_player": st.column_config.NumberColumn("Income/Player", format="$%.2f"),
                        "new_player_ggr": st.column_config.NumberColumn("New Player GGR", format="$%.2f"),
                        "returning_player_ggr": st.column_config.NumberColumn("Ret. Player GGR", format="$%.2f"),
                    },
                )

            # Combined cohort matrix
            if cohort_matrices and "Combined" in cohort_matrices:
                matrix = cohort_matrices["Combined"]
                if not matrix.empty:
                    with st.expander("🔄 Combined Cohort Retention Matrix", expanded=False):
                        st.dataframe(
                            matrix.style.format("{:.1f}%", na_rep="—"),
                            use_container_width=True,
                        )

            # ── Cohort Retention Heatmap (Phase 18) ──────────────────────
            st.markdown("---")
            st.markdown("#### > COHORT RETENTION HEATMAP_")
            heatmap_fig = _cached_retention_heatmap(df)
            if heatmap_fig is not None:
                st.plotly_chart(heatmap_fig, use_container_width=True, config={"scrollZoom": False})
            else:
                st.info("Not enough data to generate a retention heatmap.")

            # ── Cumulative LTV Curves ────────────────────────────────
            st.markdown("---")
            st.markdown("#### > CUMULATIVE LTV TRAJECTORY_")
            st.markdown("*Insight: Tracks the cumulative revenue generation of player cohorts over time to determine break-even points and long-term value.*")
            ltv_fig = _cached_ltv_curves(df)
            if ltv_fig is not None:
                st.plotly_chart(ltv_fig, use_container_width=True, config={"scrollZoom": False})
            else:
                st.info("Not enough data to generate LTV curves.")

            # ── Segmentation by Program ─────────────────────────────
            if program_summary is not None and not program_summary.empty:
                st.markdown("---")
                st.markdown("#### > SEGMENTATION BY PROGRAM_")
                st.markdown("*Insight: Evaluates the financial efficiency and house edge (Margin) across different marketing programs (ACQ, RET, WB).*")
                st.dataframe(
                    program_summary,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "brand": st.column_config.TextColumn("Brand"),
                        "month": st.column_config.TextColumn("Month"),
                        "Program": st.column_config.TextColumn("Program"),
                        "ggr": st.column_config.NumberColumn("GGR", format="$%d"),
                        "Turnover": st.column_config.NumberColumn("Turnover", format="$%d"),
                        "Margin": st.column_config.NumberColumn("Margin", format="%.2f%%"),
                        "total_players": st.column_config.NumberColumn("Players", format="%d"),
                    },
                )
        else:
            st.warning("No Both Business data available.")

    with tab_roja:
        _render_financial_tab("Rojabet", "🔴")

    with tab_latri:
        _render_financial_tab("Latribet", "🟢")

    with tab_campaigns:
        if campaign_summary is not None and not campaign_summary.empty:
            # Show Combined campaign KPIs for latest month
            camp_combined = (
                campaign_summary[campaign_summary["brand"] == "Combined"]
                .sort_values("month")
            )
            if not camp_combined.empty:
                camp_latest = camp_combined.iloc[-1]
                c1, c2, c3, c4, c5 = st.columns(5)
                with c1:
                    st.metric("Records", f"{int(camp_latest['total_records']):,}")
                with c2:
                    st.metric("Conversions", f"{int(camp_latest['total_kpi1']):,}")
                with c3:
                    st.metric("Logins", f"{int(camp_latest['total_kpi2']):,}")
                with c4:
                    st.metric("Conversion Rate", f"{camp_latest['kpi1_conversion_rate']:.1f}%")
                with c5:
                    st.metric("Login Rate", f"{camp_latest['kpi2_login_rate']:.1f}%")

            st.dataframe(
                campaign_summary.sort_values(["month", "brand"]).reset_index(drop=True),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "month": st.column_config.TextColumn("Month"),
                    "brand": st.column_config.TextColumn("Brand"),
                    "total_records": st.column_config.NumberColumn("Records", format="%d"),
                    "total_kpi1": st.column_config.NumberColumn("Conversions", format="%d"),
                    "total_kpi2": st.column_config.NumberColumn("Logins", format="%d"),
                    "total_calls": st.column_config.NumberColumn("Calls", format="%d"),
                    "total_emails": st.column_config.NumberColumn("Emails", format="%d"),
                    "total_sms": st.column_config.NumberColumn("SMS", format="%d"),
                    "kpi1_conversion_rate": st.column_config.NumberColumn("Conv Rate %", format="%.2f%%"),
                    "kpi2_login_rate": st.column_config.NumberColumn("Login Rate %", format="%.2f%%"),
                },
            )
        else:
            st.info("No campaign data available. Upload campaign CSVs in the sidebar.")


    # ═════════════════════════════════════════════════════════════════════
    #  TAB: CRM Intelligence (Phase 17.1)
    # ═════════════════════════════════════════════════════════════════════
    with tab_crm:
        st.markdown("#### > VIP & RISK LEADERBOARDS_")

        # Generate master list
        master_df = _cached_player_master_list(df)

        if master_df.empty:
            st.warning("No player data available.")
        else:
            # Brand filter
            crm_brand = st.selectbox(
                "Filter by Brand",
                ["Both Business", "Rojabet", "Latribet"],
                key="crm_brand_filter",
            )
            if crm_brand != "Both Business":
                filtered_master = master_df[master_df["brand"] == crm_brand].copy()
            else:
                filtered_master = master_df.copy()

            st.caption(f"{len(filtered_master):,} players loaded")

            _lb_col_config = {
                "id": st.column_config.TextColumn("Player ID"),
                "brand": st.column_config.TextColumn("Brand"),
                "Lifetime_GGR": st.column_config.NumberColumn("Lifetime GGR", format="$%.2f"),
                "Lifetime_Turnover": st.column_config.NumberColumn("Lifetime Turnover", format="$%.2f"),
                "First_Month": st.column_config.TextColumn("First Month"),
                "Last_Month": st.column_config.TextColumn("Last Month"),
                "Months_Active": st.column_config.NumberColumn("Months Active", format="%d"),
                "Months_Inactive": st.column_config.NumberColumn("Months Inactive", format="%d"),
            }

            lb1, lb2 = st.columns(2)
            with lb1:
                st.markdown("##### 👑 The Crown Jewels (Top 50 GGR)")
                top50 = filtered_master.nlargest(50, "Lifetime_GGR")
                st.dataframe(
                    top50,
                    use_container_width=True,
                    hide_index=True,
                    column_config=_lb_col_config,
                )

            with lb2:
                st.markdown("##### ⚠️ Bonus Abusers (High Volume, Negative GGR)")
                abusers = (
                    filtered_master[filtered_master["Lifetime_GGR"] < 0]
                    .nlargest(50, "Lifetime_Turnover")
                )
                if abusers.empty:
                    st.info("No negative-GGR players found — clean book.")
                else:
                    st.dataframe(
                        abusers,
                        use_container_width=True,
                        hide_index=True,
                        column_config=_lb_col_config,
                    )

            # ── Churn Targeting Generator (Phase 17.2) ────────────────────
            st.markdown("---")
            st.markdown("#### > CHURN TARGETING GENERATOR_")

            max_inactive = int(filtered_master["Months_Inactive"].max()) if not filtered_master.empty else 12
            if max_inactive < 1:
                max_inactive = 1

            ct1, ct2 = st.columns(2)
            with ct1:
                min_inactive = st.slider(
                    "Minimum Months Inactive",
                    min_value=1, max_value=max(max_inactive, 1), value=min(3, max_inactive),
                    key="churn_min_inactive",
                )
            with ct2:
                min_ggr = st.number_input(
                    "Minimum Lifetime GGR ($)",
                    min_value=0.0, value=500.0, step=100.0,
                    key="churn_min_ggr",
                )

            target_df = filtered_master[
                (filtered_master["Months_Inactive"] >= min_inactive)
                & (filtered_master["Lifetime_GGR"] >= min_ggr)
            ].sort_values("Lifetime_GGR", ascending=False)

            st.metric(label="🎯 TARGET ACQUIRED (Players Found)", value=f"{len(target_df):,}")

            if not target_df.empty:
                display_cols = ["id", "brand", "Last_Month", "Months_Inactive", "Lifetime_GGR", "Lifetime_Turnover", "Recommended_Campaign"]
                st.dataframe(
                    target_df[display_cols],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "id": st.column_config.TextColumn("Player ID"),
                        "brand": st.column_config.TextColumn("Brand"),
                        "Last_Month": st.column_config.TextColumn("Last Month"),
                        "Months_Inactive": st.column_config.NumberColumn("Months Inactive", format="%d"),
                        "Lifetime_GGR": st.column_config.NumberColumn("Lifetime GGR", format="$%.2f"),
                        "Lifetime_Turnover": st.column_config.NumberColumn("Lifetime Turnover", format="$%.2f"),
                        "Recommended_Campaign": st.column_config.TextColumn("Campaign"),
                    },
                )
                st.download_button(
                    label="⬇️ DOWNLOAD TARGET LIST (CSV)",
                    data=target_df[display_cols].to_csv(index=False).encode("utf-8"),
                    file_name="winback_targets.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            else:
                st.info("No players match the current filters. Adjust the sliders above.")

            # ── Smart Campaign Profiling (Phase 17.4) ─────────────────────
            st.markdown("---")
            st.markdown("#### > SMART CAMPAIGN PROFILING_")

            special_campaigns = filtered_master[
                filtered_master["Recommended_Campaign"] != "✉️ Standard Lifecycle"
            ]
            campaign_counts = (
                special_campaigns["Recommended_Campaign"]
                .value_counts()
                .reindex(["🏆 Ironman Legend", "🛑 Promo Exclusion", "🚨 Early Churn VIP", "🌟 Rising Star", "🎯 Cold Crown Jewel", "👑 Active Crown Jewel", "📉 Cooling Down"], fill_value=0)
            )

            row1 = st.columns(4)
            row1[0].metric("🏆 Ironman Legend", f"{campaign_counts.get('🏆 Ironman Legend', 0):,}")
            row1[1].metric("🛑 Promo Exclusion", f"{campaign_counts.get('🛑 Promo Exclusion', 0):,}")
            row1[2].metric("🚨 Early Churn VIP", f"{campaign_counts.get('🚨 Early Churn VIP', 0):,}")
            row1[3].metric("🌟 Rising Star", f"{campaign_counts.get('🌟 Rising Star', 0):,}")
            row2 = st.columns(4)
            row2[0].metric("🎯 Cold Crown Jewel", f"{campaign_counts.get('🎯 Cold Crown Jewel', 0):,}")
            row2[1].metric("👑 Active Crown Jewel", f"{campaign_counts.get('👑 Active Crown Jewel', 0):,}")
            row2[2].metric("📉 Cooling Down", f"{campaign_counts.get('📉 Cooling Down', 0):,}")

            st.caption(f"{len(special_campaigns):,} players flagged for specialized campaigns out of {len(filtered_master):,} total.")

            # ── Campaign Extraction (Phase 17.5) ──────────────────────────
            st.markdown("### 📥 Extract Campaign List")
            all_campaigns = sorted(filtered_master["Recommended_Campaign"].unique().tolist())
            selected_campaign = st.selectbox(
                "Select Campaign",
                all_campaigns,
                key="crm_campaign_dropdown",
            )
            campaign_extract_df = filtered_master[
                filtered_master["Recommended_Campaign"] == selected_campaign
            ].sort_values("Lifetime_GGR", ascending=False)

            st.caption(f"{len(campaign_extract_df):,} players in **{selected_campaign}**")

            if not campaign_extract_df.empty:
                extract_cols = ["id", "brand", "First_Month", "Last_Month", "Months_Active", "Months_Inactive", "Lifetime_GGR", "Lifetime_Turnover", "Recommended_Campaign"]
                st.dataframe(
                    campaign_extract_df[extract_cols],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "id": st.column_config.TextColumn("Player ID"),
                        "brand": st.column_config.TextColumn("Brand"),
                        "First_Month": st.column_config.TextColumn("First Month"),
                        "Last_Month": st.column_config.TextColumn("Last Month"),
                        "Months_Active": st.column_config.NumberColumn("Active", format="%d"),
                        "Months_Inactive": st.column_config.NumberColumn("Inactive", format="%d"),
                        "Lifetime_GGR": st.column_config.NumberColumn("Lifetime GGR", format="$%.2f"),
                        "Lifetime_Turnover": st.column_config.NumberColumn("Lifetime Turnover", format="$%.2f"),
                        "Recommended_Campaign": st.column_config.TextColumn("Campaign"),
                    },
                )
                safe_name = selected_campaign.replace(" ", "_").replace(".", "").lower()
                st.download_button(
                    label=f"⬇️ Download {selected_campaign} List",
                    data=campaign_extract_df[extract_cols].to_csv(index=False).encode("utf-8"),
                    file_name=f"campaign_{safe_name}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            else:
                st.info(f"No players in {selected_campaign}.")

