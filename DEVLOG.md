# DEVELOPMENT LOG
**Status:** V2.0 Enterprise Ready (Multi-Client Architecture Deployed)
**Started:** 2026-03-01

## LOG ENTRIES

### [Phase 0 - Setup] - 2026-03-01 - COMPLETED
- Initialized project structure and methodology.
- Created `SPEC.md` to define raw data schemas, monthly summary schemas, and core business logic (winners, losers, new vs. returning).
- Created `.cursorrules` to enforce Spec-Driven Development.
- Prepared for Phase 1 (Setup & Scaffolding).

### [Phase 0.5 - Spec Refinement] - 2026-03-01 - COMPLETED
- Clarified that data sources will be individual CSVs per brand/month, not Excel tabs.
- Added `IngestionRegistry` to the Spec to track missing files and alert the user if a month is incomplete across brands.

### [Phase 1 - Setup & Scaffolding] - 2026-03-02 - COMPLETED
- Created folder structure per `DIRECTORY.md`: `data/raw/latribet/`, `data/raw/rojabet/`, `data/output/`, `src/`.
- Extracted Excel workbook sheets into 36 individual CSVs (18 per brand) with naming convention `{brand}_{YYYY}_{MM}.csv`.
- Created `requirements.txt` (pandas, openpyxl).
- Created `src/__init__.py` package init.

### [Phase 2 - Data Ingestion & Transformation] - 2026-03-02 - COMPLETED
- Built `src/ingestion.py` with full `IngestionRegistry` class:
  - Reads all CSVs from brand sub-directories under `data/raw/`.
  - Normalises raw column headers to spec `PlayerRecord` fields (`id`, `brand`, `wb_tag`, `bet`, `win`, `revenue`, `report_month`).
  - Coerces numerics, drops null-ID rows, normalises brand casing.
  - Cross-references expected months across ALL brands (§4 Ingestion Validation) — flags and warns on any missing brand×month slots.
  - Supports `--strict` flag to halt on missing data.
  - Persists registry state to `data/registry.json`.
- Created `src/analytics.py` (Phase 3 stub) and `src/exporter.py` (Phase 4 stub).
- Created `main.py` pipeline entrypoint with diagnostics output.
- **Verification Results:**
  - 51,307 total rows loaded across 18 months, 2 brands.
  - All 36 brand×month slots marked COMPLETE in `registry.json`.
  - Cross-checked player totals against reference `Summary data (4).xlsx` — exact matches on most months; minor differences (1–8 rows) on a few months due to null-ID row cleanup.
- **Ready for Phase 3 (Core Analytics Logic).**

### [Phase 3 - Core Analytics Logic] - 2026-03-02 - COMPLETED
- Implemented `src/analytics.py` with `generate_monthly_summaries(df)`:
  - **Financial Calculator** (vectorised): Winners/Losers/Flat counts, GGR, GGR-per-player, winners_pct.
  - **Cohort Analyzer** (stateful): Sorts data chronologically, maintains `seen_ids` set per brand to classify New vs Returning players per month. Calculates `retention_pct`.
  - **Edge cases**: Division-by-zero handled via `_safe_pct()` helper returning 0.0.
- Updated `main.py` to run analytics and print full summary table.
- **Verification Results (36 summary rows = 2 brands × 18 months):**
  - GGR totals match reference `Summary data (4).xlsx` within rounding tolerance.
  - Cohort logic verified: Aug 2024 = all New; Sept 2024 = correct Returning counts matching reference retention rates.
  - **Note:** Reference Excel's "Losers" column actually maps to our `winners` (revenue > 0). The SPEC §4 defines Loser = Revenue < 0 (player lost money), Winner = Revenue > 0 (player won). Our code follows the SPEC definitions exactly.
- **Ready for Phase 4 (Output Generation).**

### [Phase 4 - Output Generation] - 2026-03-02 - COMPLETED
- Implemented `src/exporter.py` with `export_to_excel(summary_df, output_dir)`:
  - Creates `Summary_Data_Auto.xlsx` in `data/output/` with one tab per brand (`Latribet Financial`, `Rojabet Financial`).
  - openpyxl formatting: bold blue headers, percentage columns (0.00%), GGR as #,##0.00, integer counts as #,##0, auto-width columns.
  - Converts "YYYY-MM" to human-readable month names (e.g., "August 2024").
- Updated `main.py` to run full Phase 2→3→4 pipeline end-to-end.
- **Verification Results:**
  - `Summary_Data_Auto.xlsx` generated with 2 sheets × 18 rows each.
  - All columns match `MonthlyBrandSummary` spec entity.
  - Formatting renders correctly in Excel.
- **Ready for Phase 5 (Campaign Extension).**

### [Phase 5 - Campaign Extension] - 2026-03-02 - COMPLETED
- Updated `src/ingestion.py` — added `load_campaign_data()`:
  - Reads campaign CSVs from `data/campaigns/{brand}/` directories.
  - Maps raw columns to `CampaignRecord` spec entity fields.
  - Case-insensitive column matching fallback for flexibility.
  - Returns empty DataFrame gracefully when no campaign data exists.
- Updated `src/analytics.py` — added `generate_campaign_summaries(df)`:
  - **Campaign Duplication Scrub (§4):** Zeroes `records` and `kpi2_logins` for any row where `campaign_type == "LI"` BEFORE the groupby aggregation.
  - Aggregates by brand × month into `CampaignSummary` entity.
- Updated `src/exporter.py`:
  - `export_to_excel()` now accepts optional `campaign_df` parameter.
  - Writes "Summary Campaigns" tab with formatted headers and `#,##0` integer formatting when campaign data exists.
  - Refactored internal helpers to be parameterised (reusable across financial and campaign tabs).
- Updated `main.py` — full pipeline: Ingestion → Analytics → Campaigns → Export.
  - Gracefully skips campaign processing if `data/campaigns/` is empty.
- **Verification Results:**
  - Pipeline runs end-to-end successfully.
  - Campaign skip path tested: logs "No campaign data found — skipping campaign analytics."
  - All previous financial output remains unchanged.
  - Campaign directories ready at `data/campaigns/latribet/` and `data/campaigns/rojabet/` for future CSV drops.
- **All 5 phases complete. Pipeline is production-ready.**

### [Phase 6 - Streamlit Web App] - 2026-03-02 - COMPLETED
- Created `app.py` — Streamlit frontend wrapping the existing ETL pipeline:
  - **Sidebar:** File uploaders grouped by brand (Latribet/Rojabet) × data type (Financial/Campaign). Uploaded files saved directly to `data/raw/` and `data/campaigns/` directories.
  - **Main area:** "Run Analytics Pipeline" button, financial summary preview (tabbed by brand), campaign summary preview, download button for `Summary_Data_Auto.xlsx`, registry status display.
  - Calls existing `src/` functions — zero logic duplication.
  - Graceful handling: skips campaigns if no campaign data exists.
- Added `streamlit>=1.30` to `requirements.txt`.
- **Run:** `streamlit run app.py --server.port=8502`
- **Version control:** `git init` + initial commit completed (11 files, `.gitignore` protects `data/`).
- **All 6 phases complete.**

### [Phase 7 - Cohort Matrix Generation] - 2026-03-02 - COMPLETED
- Added `generate_cohort_matrix(df)` to `src/analytics.py`:
  - Identifies each player's acquisition month (earliest `report_month` per brand).
  - Computes relative month offsets for all subsequent activity months.
  - Pivots into retention % matrix: rows = Acquisition Month, columns = Month 1…N.
  - Returns `dict[str, pd.DataFrame]` keyed by brand.
- Updated `src/exporter.py`:
  - `export_to_excel()` accepts `cohort_matrices` parameter.
  - `_write_cohort_section()` writes matrix below financial summary with green header styling and percentage formatting.
- Updated `app.py` — displays cohort matrices in expandable sections per brand.
- Updated `main.py` — cohort generation runs before export.
- **Verification Results:**
  - Latribet: 17 cohorts × 17 retention months.
  - Rojabet: 17 cohorts × 17 retention months.
  - Cohort matrices rendered in Excel with `0.00%` format and green headers.
  - All existing financial/campaign output unchanged.
- **All 7 phases complete.**

### [Phase 8 - Top-Tier Business Intelligence] - 2026-03-02 - COMPLETED
- Updated `src/analytics.py`:
  - Renamed `winners`→`profitable_players`, `losers`→`negative_yield_players`.
  - Added `total_handle` (sum of bet), `hold_pct` (GGR/handle), `top_10_pct_ggr_share` (whale analysis).
  - Added `generate_segmentation_summary()` — groups by brand × month × wb_tag.
  - Added `kpi1_conversion_rate` and `kpi2_login_rate` to campaign summaries.
- Updated `src/exporter.py`:
  - Updated all column headers and format maps for renamed/new financial and campaign fields.
  - Added "Segmentation" tab with player count and GGR per segment.
  - Campaign rates formatted as percentages.
- Updated `app.py` and `main.py` — segmentation generation, display, and export integration.
- **Verification Results:**
  - 36 monthly summaries with all new metrics.
  - Segmentation tab: 36 rows (2 brands, 1 segment).
  - Cohort matrices and campaign summaries unchanged.
  - Excel output verified with correct formatting.
- **All 8 phases complete.**

### [Phase 9 - Interactive BI Dashboard & Client Aggregation] - 2026-03-02 - COMPLETED
- Updated `src/analytics.py`:
  - `generate_monthly_summaries`: adds "Combined" rows by summing additive metrics across brands per month, then recalculating all ratio metrics (profitable_pct, hold_pct, ggr_per_player, retention_pct, top_10_pct_ggr_share).
  - `generate_campaign_summaries`: adds "Combined" campaign rows with recalculated conversion/login rates.
  - `generate_cohort_matrix`: adds "Combined" key treating all players as a single pool.
- Exporter automatically generates "Combined Financial" tab (loop already iterates brands).
- Transformed `app.py` into a BI Dashboard:
  - 4 tabs: 🏦 Combined Client | 🔴 Rojabet | 🟢 Latribet | 📈 Campaigns.
  - KPI metric cards with month-over-month deltas (GGR, Handle, Hold%, Players, Retention%, Profitable%).
  - GGR bar chart per brand tab.
  - Full financial tables with `st.column_config` formatting (currency, percentages).
  - Cohort retention matrices in collapsible expanders.
  - Campaign tab with Combined KPIs and formatted table.
- **Verification Results:**
  - 54 monthly summaries (3 brands × 18 months).
  - 3 cohort matrices (Latribet, Rojabet, Combined) — 17 cohorts × 17 months each.
  - Combined Financial tab exported with cohort matrix below.
  - All ratio metrics correctly recalculated from sums (not averaged).
- **All 9 phases complete.**

### [Phase 9 - Both Business Aggregation & BI Dashboard] - 2026-03-02 - COMPLETED
- Added `generate_both_business_summary()` to `src/analytics.py`:
  - 13-column `BothBusinessSummary` entity (SPEC §2).
  - Sums Turnover (bet) and GGR (revenue) across brands per month.
  - Calculates Margin (GGR/Turnover), 15% Revenue Share deduction, Net Income.
  - Player counts + recalculated ratios (New/Returning %, GGR/player, Income/player).
- Updated `src/exporter.py`:
  - "Both Business Summary" written as **first** tab in Excel with gold header styling.
  - Currency formatting for Turnover/GGR/Rev Share/Net Income/Per-Player metrics.
  - Percentage formatting for Margin and Player %.
- Updated `main.py` — integrated Both Business generation and export.
- Transformed `app.py` into 4-tab BI Dashboard:
  - 🏦 Both Business (Combined) | 🔴 Rojabet | 🟢 Latribet | 📈 Campaigns.
  - Both Business tab: 4 KPI metric cards (Turnover, GGR, Margin, Total Players) with MoM deltas.
  - Combined GGR bar chart.
  - Full BothBusinessSummary dataframe with `st.column_config` formatting.
  - Combined cohort retention matrix in expander.
- **Verification Results:**
  - 18 months of Both Business data generated correctly.
  - Exported as first tab in Summary_Data_Auto.xlsx.
  - All ratio metrics recalculated from combined sums (not averaged — per SPEC §4).
  - Pipeline completes end-to-end with no errors.
- **All 9 phases complete (with Both Business).**

### [Phase 10 - Matrix Theme] - 2026-03-02 - COMPLETED
- Created `.streamlit/config.toml` with Matrix theme (neon green `#00FF41` on black `#000000`, monospace font).
- Injected custom CSS in `app.py` with neon green `text-shadow` glow on `st.metric` values, deltas, and headings.
- **All 10 phases complete.**

### [Phase 11 - Time-Series Intelligence & Terminal Rebrand] - 2026-03-02 - COMPLETED
- Added `generate_time_series()` to `src/analytics.py`:
  - MoM (shift 1), YoY (shift 12), YTD (cumsum by year), QoQ (quarterly aggregation + shift 1).
  - Targets 5 metrics: Turnover, GGR, Total Players, Profitable Players, Negative Yield Players.
  - Returns `{"monthly": df, "quarterly": df}` with absolute deltas and % changes.
- Rebranded `app.py` header → **"D-ROCK FINANCIAL TERMINAL v1.0"**.
- Added "> COMPARATIVE INTELLIGENCE_" section in Both Business tab:
  - **💰 Financials** table: Turnover & GGR with MoM/YoY/YTD/QoQ deltas and ↑/↓ arrows.
  - **👥 Player Demographics** table: Total/Winners/Losers with same time-series columns.
- **Verification:** Pipeline runs end-to-end, 18 monthly + quarterly time-series rows generated.
- **All 11 phases complete.**

### [Phase 11.5 - UI Formatting Polish] - 2026-03-02 - COMPLETED
- Applied `st.column_config` to all `st.dataframe()` calls in `app.py`:
  - **Brand Financial tab**: 16 column configs (currency, percentage, count).
  - **Both Business tab**: 14 column configs with count formatting for player columns.
  - **Campaign tab**: 10 column configs (counts for records/KPIs/comms, pct for rates).
  - **Segmentation tab**: 5 column configs (text, count, currency).
- Format types: `$%,.2f` (currency), `%.2f%%` (percentage), `%,d` (integer count).
- **All 11.5 phases complete.**

### [Hotfix - Floating-Point Precision Artifacts] - 2026-03-02 - COMPLETED
- Applied `.round(2)` to all float columns in:
  - `generate_monthly_summaries` — all float cols via `select_dtypes`.
  - `generate_both_business_summary` — 9 currency/pct columns.
  - `generate_time_series` — all float cols in monthly + quarterly DataFrames.
- All `st.metric` calls verified to use explicit f-string formatting (`:,.0f`, `:.2f%`, `int():,`).
- Updated `Dockerfile` to include `.streamlit/` config. Docker rebuilt and redeployed on port 8502.

### [UI - Player Demographics Charts] - 2026-03-02 - COMPLETED
- Added multi-line `st.line_chart()` to all 3 financial tabs in `app.py`:
  - **Both Business**: "> COMBINED PLAYER DEMOGRAPHICS (MONTH OVER MONTH)_" — pulls Combined brand from `financial_summary`.
  - **Rojabet / Latribet**: "> {BRAND} PLAYER DEMOGRAPHICS (MONTH OVER MONTH)_" — uses brand summary directly.
- Lines: Total Players (grey `#AAAAAA`), Profitable/Winners (green `#00FF41`), Neg. Yield/Losers (red `#FF4444`).
- Docker rebuilt and redeployed on port 8502.

### [Fix - Both Business Missing Player Data] - 2026-03-02 - COMPLETED
- Added `profitable_players` and `negative_yield_players` to Both Business pipeline:
  - `BOTH_BUSINESS_COLS` in `src/analytics.py` — added to column list.
  - `generate_both_business_summary` — added to groupby `.agg()` sums.
  - `src/exporter.py` — added to `BOTH_BUSINESS_HEADERS`, `BOTH_BUSINESS_DF_COLS`, and `_BOTH_BUSINESS_FORMAT_MAP`.
  - `app.py` — simplified Player Demographics chart (now pulls directly from `both_business` instead of workaround via `financial_summary["Combined"]`). Added `column_config` for new columns.
- Docker rebuilt and redeployed on port 8502.

### [Phase 11.6 - Client Revenue Integration] - 2026-03-02 - COMPLETED
- Added `revenue_share_deduction` to `_TS_METRICS` in `src/analytics.py` — now receives MoM/QoQ/YoY/YTD calculations with `.round(2)`.
- Expanded Both Business KPI cards from 4 → 5 columns: Turnover, GGR, **Revenue (15%)**, Margin, Total Players.
- Revenue (15%) KPI card formatted as `$%,.2f` with MoM delta.
- Added "Revenue (15%)" to Comparative Intelligence financials table alongside Turnover and GGR.
- Docker rebuilt and redeployed on port 8502.

### [Phase 12 - C-Suite Insights & Lifecycle ROI] - 2026-03-02 - COMPLETED
- **`src/analytics.py`:**
  - Cohort analyzer now calculates `new_player_ggr` and `returning_player_ggr` (sum of revenue for new/returning ID sets).
  - Added `turnover_per_player` to `generate_monthly_summaries` + `_build_combined_financial` + `generate_both_business_summary`.
  - Added `new_players`/`returning_players` to `_TS_METRICS` for time-series analysis.
  - Created `generate_program_summary(df)` — groups raw data by brand/month/wb_tag for lifecycle ROI.
  - Updated `SUMMARY_COLS`, `BOTH_BUSINESS_COLS`, `_ADDITIVE_FINANCIAL_COLS`.
- **`src/exporter.py`:** Added 3 new columns to BB tab configs (headers, DF_COLS, format map).
- **`app.py`:**
  - Player Demographics expanded: 5 rows (Total Active, New, Returning, Profitable, Neg. Yield).
  - **"> RISK & VALUE METRICS_"**: Turnover/Player card + Whale Dependency (Top 10% GGR Share) card.
  - **Revenue Composition** bar chart: New vs Returning Player GGR over time.
  - **Lifecycle Program Performance** horizontal bar chart: GGR by wb_tag for latest month.
  - Added `column_config` for `turnover_per_player`, `new_player_ggr`, `returning_player_ggr`.
- Docker rebuilt and redeployed on port 8502.

### [Phase 14 - Conversion Tracking] - 2026-03-02 - COMPLETED
- **`src/analytics.py`:**
  - Cohort analyzer: added `last_month_ids` tracking. `reactivated_players` = seen historically but NOT last month. `conversions` = new + reactivated.
  - Added to `SUMMARY_COLS`, `_ADDITIVE_FINANCIAL_COLS`, `BOTH_BUSINESS_COLS`, BB `.agg()`, `_TS_METRICS`.
  - Both columns now receive MoM/YoY/YTD/QoQ time-series calculations.
- **`src/exporter.py`:** Added both columns to BB tab configs (headers, DF_COLS, format map — integer format).
- **`app.py`:**
  - Player Demographics table: 7 rows now (Total Active, New, Returning, Reactivated, Conversions, Profitable, Neg. Yield).
  - Added `column_config` for both new columns (`%,d` integer format).
- Docker rebuilt and redeployed on port 8502.

### [Phase 14.5 - Brand-Level Comparative Intelligence] - 2026-03-02 - COMPLETED
- Injected full `> COMPARATIVE INTELLIGENCE_` section into `_render_financial_tab()` in `app.py`.
- Maps `total_handle→turnover` before calling `generate_time_series(bdf)` for brand-filtered data.
- **Financials**: Turnover, GGR (no Revenue Share — brand-level doesn't have it).
- **Player Demographics**: 7 rows — Total Active, Conversions, New, Reactivated, Returning (Retained), Profitable (Winners), Neg. Yield (Losers).
- Both Rojabet and Latribet tabs now have full MoM/YoY/YTD/QoQ intelligence tables.
- Docker rebuilt and redeployed on port 8502.

### [Phase 15 - Predictive & Diagnostic Analytics] - 2026-03-02 - COMPLETED
- **`src/analytics.py`:**
  - `generate_time_series()`: Added `eoy_projected_ggr` and `eoy_projected_turnover` using `(YTD / month_int) * 12`.
  - Created `generate_rfm_summary(raw_df, target_month)` — 3 tiers: True VIP (freq≥3, monetary>0, recent), Churn Risk (freq≥3, monetary>0, not recent), Casual (else).
  - Created `generate_smart_narrative(ts_row, margin, whale_dep)` — 3-sentence diagnostic (GGR trend, Margin health, Whale risk).
- **`app.py`:**
  - Smart Narrative displayed via `st.info()` (or `st.warning()` if margin < 2.5% or whale ≥ 70%).
  - Financials table: 2 new rows — EOY Projected GGR, EOY Projected Turnover.
  - Risk & Value section: 🏆 VIP Tiering cards (True VIP / Churn Risk / Casual) + RFM dataframe with column_config.
- Docker rebuilt and redeployed on port 8502.

### [Hotfix - Player Demographics Order + Brand Revenue Share] - 2026-03-02 - COMPLETED
- **`src/analytics.py`:** Added `revenue_share_deduction = ggr * 0.15` to `generate_monthly_summaries` for individual brands. Added to `SUMMARY_COLS`, `_ADDITIVE_FINANCIAL_COLS`.
- **`app.py`:**
  - Brand tabs now include "Revenue (15%)" in Financials table alongside Turnover and GGR.
  - Player Demographics reordered in ALL 3 tabs: Total Active → Profitable (Winners) → Neg. Yield (Losers) → Conversions → New Players → Reactivated → Returning (Retained).
- Docker rebuilt and redeployed on port 8502.

### [Phase 15.2 - Insights Universality & Formatting Patch] - 2026-03-02 - COMPLETED
- **`src/analytics.py`:**
  - `generate_smart_narrative()`: sentences now joined with `\n\n` for Markdown line breaks.
  - `generate_program_summary()`: `wb_tag` renamed to `Program` in output DataFrame.
- **`app.py`:**
  - Brand tabs now have: Smart Narrative (`st.info`/`st.warning`), Risk & Value Metrics (Turnover/Player + Whale Dependency), Revenue Composition bar chart, RFM VIP Tiering (brand-filtered raw data), Program Segmentation horizontal bar chart.
  - Both Business tab: "Lifecycle Program Performance" → `> SEGMENTATION BY PROGRAM_` header, uses `Program` column.
  - All sections use consistent `st.column_config` formatting.
- Docker rebuilt and redeployed on port 8502.

### [Phase 16 - Executive Command Center] - 2026-03-02 - COMPLETED
- **`requirements.txt`:** Added `plotly>=5.18`.
- **`app.py`:**
  - Tabs restructured to 5: 📊 Executive Summary → 🏦 Combined Deep-Dive → 🔴 Rojabet → 🟢 Latribet → 📈 Campaigns.
  - Executive Summary tab includes:
    - `> SYSTEM DIAGNOSTIC_` smart narrative for Combined entity.
    - `> CROSS-BRAND EXECUTIVE MATRIX_` — 7 metrics (Turnover, GGR, Margin %, Revenue 15%, Conversions, Turnover/Player, Whale Risk %) × 3 columns (Both Business, Rojabet, Latribet).
    - `> BRAND vs BRAND TRAJECTORY_` — Plotly grouped bar chart comparing Rojabet (red) vs Latribet (green) GGR month-over-month, styled with Matrix dark theme.
  - All existing deep-dive metrics (RFM, Demographics, etc.) remain in their respective tabs.
- Docker rebuilt and redeployed on port 8502.

### [Phase 17.1 - Master Player List & Leaderboards] - 2026-03-02 - COMPLETED
- **`src/analytics.py`:** Created `generate_player_master_list(raw_df)` — groups by `id`+`brand`, aggregates Lifetime_GGR (sum revenue), Lifetime_Turnover (sum bet), First_Month (min), Last_Month (max), Months_Active (nunique).
- **`app.py`:**
  - Added 🕵️ CRM Intelligence tab (6th tab).
  - Brand filter selectbox: Both Business / Rojabet / Latribet.
  - Two side-by-side leaderboards:
    - 👑 Crown Jewels — Top 50 by Lifetime GGR.
    - ⚠️ Bonus Abusers — Negative GGR players, Top 50 by Lifetime Turnover.
  - Full `column_config` formatting ($%,.2f for currency, %,d for integers).
- **Patch:** Added `Months_Inactive` (pd.Period subtraction from global max month) to `generate_player_master_list`. CRM tab heading updated to `> VIP & RISK LEADERBOARDS_`. Column config updated.
- Docker rebuilt and redeployed on port 8502.

### [Phase 17.2 - Interactive Win-Back Generator] - 2026-03-02 - COMPLETED
- **`app.py`:** Added `> CHURN TARGETING GENERATOR_` section to CRM Intelligence tab:
  - 2-column filter layout: Minimum Months Inactive slider + Minimum Lifetime GGR input.
  - 🎯 TARGET ACQUIRED metric card showing match count.
  - Filtered dataframe (id, brand, Last_Month, Months_Inactive, Lifetime_GGR, Lifetime_Turnover) with `column_config`.
  - ⬇️ DOWNLOAD TARGET LIST (CSV) button for win-back campaign export.
- Docker rebuilt and redeployed on port 8502.

### [Phase 15.4 - EOY Projected Revenue & UI Universality Audit] - 2026-03-02 - COMPLETED
- **`src/analytics.py`:** Added `eoy_projected_revenue_share_deduction` to `generate_time_series()` — calculated as `eoy_projected_ggr * 0.15`.
- **`app.py`:**
  - BB tab Financials: 3 EOY rows (GGR, Turnover, **Revenue 15%**).
  - Brand tabs Financials: Now include matching EOY projection rows + 🔮 caption.
  - UI Universality audit confirmed: all brand tabs have parity with BB (Diagnostic, Financials+EOY, Demographics, Risk & Value, RFM, Revenue Composition, Program Segmentation).
- Docker rebuilt and redeployed on port 8502.

### [Phase 17.3 - UI Cleanup & Export Fix] - 2026-03-02 - COMPLETED
- **`app.py`:**
  - Removed `> SEGMENTATION BY PROGRAM_` from all tabs (Brand + Both Business).
  - Moved Excel download button to sidebar — persistent `st.download_button` checks `OUTPUT_DIR / Summary_Data_Auto.xlsx`.
  - Removed old main-layout download button (was trapped inside `if run_clicked:` scope).
- Docker rebuilt and redeployed on port 8502.

### [Phase 17.4 - Smart Campaign Profiling] - 2026-03-02 - COMPLETED
- **`src/analytics.py`:**
  - Added `import numpy as np`.
  - `generate_player_master_list()` now calculates `Recommended_Campaign` via `np.select()`:
    1. 🛑 Promo Exclusion (GGR < 0 & Turnover > 5k)
    2. 🚨 Early Churn VIP (Inactive == 1 & GGR > 500)
    3. 🌟 Rising Star (Active ≤ 2mo & Turnover > 1k & active now)
    4. 🎯 Cold Crown Jewel (Inactive ≥ 3 & GGR > 1k)
    5. ✉️ Standard Lifecycle (default)
- **`app.py`:**
  - Win-back target table/CSV now includes `Recommended_Campaign` column.
  - Added `> SMART CAMPAIGN PROFILING_` section: 4 metric cards (Promo Exclusion, Early Churn VIP, Rising Star, Cold Crown Jewel) with counts of players in each specialized bucket.
- Docker rebuilt and redeployed on port 8502.

### [Session State Refactor] - 2026-03-02 - COMPLETED
- **`app.py`:**
  - Added `st.session_state["data_loaded"]` initialization after page config.
  - Pipeline saves all dataframes (df, registry, financial_summary, campaign_summary, cohort_matrices, segmentation, both_business, program_summary, output_path) to session state on completion.
  - Dashboard rendering un-indented from `if run_clicked:` block, now wrapped in `if st.session_state["data_loaded"]:` — reads from state.
  - **Result:** Widget interactions (selectbox, slider, expanders) no longer trigger full state loss.
- Docker rebuilt and redeployed on port 8502.

### [Phase 17.5 - CRM Campaign Extraction & VIP Fix] - 2026-03-02 - COMPLETED
- **`src/analytics.py`:**
  - Added `👑 Active Crown Jewel` (GGR ≥ 1000 & Inactive == 0) as 5th `np.select` rule before Standard Lifecycle.
- **`app.py`:**
  - Expanded campaign profiling from 4 → 5 metric columns (added Active Crown Jewel).
  - Added `📥 Extract Campaign List`: selectbox to pick any campaign → filtered dataframe → CSV download with dynamic filename.
  - Removed zombie Segmentation by WB Tag expander and Registry Status block.
- Docker rebuilt and redeployed on port 8502.

### [Tab Reset Fix - @st.cache_data] - 2026-03-02 - COMPLETED
- **Root cause:** `generate_time_series()`, `generate_rfm_summary()`, `generate_player_master_list()` were executing on every widget interaction, causing slow re-renders and tab resets.
- **`app.py`:** Added `@st.cache_data` wrapper functions (`_cached_time_series`, `_cached_rfm_summary`, `_cached_player_master_list`). Replaced all 6 direct calls in dashboard with cached versions. Results are now memoized by input data — subsequent reruns return instantly.
- Docker rebuilt and redeployed on port 8502.

### [Phase 16.1 - Executive Summary Upgrade] - 2026-03-02 - COMPLETED
- **`app.py`:** Executive Summary tab now includes:
  - Insight text before Cross-Brand Executive Matrix.
  - 🐳 Whale Risk explanation caption below matrix.
  - `> CROSS-BRAND DEMOGRAPHICS_`: 7-metric comparison (Total Active → Neg. Yield) across BB/Rojabet/Latribet with `%,d` formatting.
  - `> CROSS-BRAND VIP HEALTH_`: RFM tier comparison (True VIPs, Churn Risk VIPs, Casuals) across all 3 entities using `_cached_rfm_summary`.
  - Pipeline button now full-width (removed `st.columns` wrapper).
- Docker rebuilt and redeployed on port 8502.

### [Velocity Trigger - Cooling Down Alert] - 2026-03-02 - COMPLETED
- **`src/analytics.py`:**
  - `generate_player_master_list()` now calculates `Last_Month_Turnover` (last bet in chronological order) and `Avg_Monthly_Turnover` (Lifetime_Turnover / Months_Active).
  - Added `📉 Cooling Down` campaign rule: active players (Inactive == 0) whose last month turnover dropped below 50% of their average, with Lifetime_Turnover >= 1000.
- **`app.py`:**
  - CRM tab expanded from 5 → 6 metric columns (added 📉 Cooling Down).
  - Campaign Extractor selectbox automatically includes the new tag.
- Docker rebuilt and redeployed on port 8502.

### [Phase 18 - Visual Cohort Retention Heatmaps] - 2026-03-02 - COMPLETED
- **`src/analytics.py`:**
  - Created `generate_retention_heatmap(df)`: computes cohort × month_index retention matrix, normalizes to %, returns dark-mode Plotly `px.imshow` heatmap (green gradient, x-axis on top).
- **`app.py`:**
  - Added `_cached_retention_heatmap` wrapper (`@st.cache_data`).
  - `> COHORT RETENTION HEATMAP_` section added to: `_render_financial_tab` (Rojabet + Latribet) and Combined Deep-Dive tab.
  - Each tab passes brand-filtered `raw_df` (or full df for Combined).
- Docker rebuilt and redeployed on port 8502.

### [V1.1 Optimization 1] - Vectorized Whale Analysis - COMPLETED
- **`src/analytics.py`:** Removed the `O(N)` `.iterrows()` bottleneck for calculating `top_10_pct_ggr_share`.
- Replaced it with a native pandas vectorized approach using grouped masking and `.nlargest()` / proportional summation.
- Applied this CPU optimization to both `_compute_financial_metrics` and `_build_combined_financial` to ensure the app scales seamlessly to hundreds of thousands of rows without locking the Streamlit UI.

### [V1.1 Optimization 2] - DRY MoM Frontend Math - COMPLETED
- **`app.py`:** Removed redundant manual percentage math (`((c - p) / p) * 100`) inside the Executive Summary's `_brand_mom` and `_bb_mom` functions.
- Directly wired these functions to extract the pre-calculated `_mom_pct` values from the cached `generate_time_series` engine, matching the architecture used for YoY metrics.
- Enforced strict string formatting (`+X.XX%` / `-X.XX%`) prior to Streamlit rendering to prevent Javascript crashes.

### [V1.1 Optimization 3] - Multi-User Concurrency (In-Memory I/O) - COMPLETED
- **`src/ingestion.py`:** Refactored `load_all_data` and `load_campaign_data` to read directly from Streamlit `UploadedFile` buffer objects instead of scanning local disk directories.
- **`src/exporter.py`:** Refactored `export_to_excel` to write the workbook to an in-memory `io.BytesIO()` buffer instead of the local `data/output/` directory.
- **`app.py`:** Updated file uploaders and download buttons to pass and receive buffer objects.
- **Impact:** The terminal is now 100% stateless and safe for simultaneous multi-user web deployment. No files are written to the server's hard drive.
### [Rebuild] - 2026-03-02 - COMPLETED
- Executed \docker build -t drock-financial-terminal .\ to package Phase 4 (True Profitability) and Phase 5 (Geographic Intelligence) updates.
- Stopped conflicting \inancial-terminal\ container mapped to port 8502.
- Deployed fresh \drock-financial-terminal\ container detached on port 8502 (\ .0.0.0:8502->8501/tcp\).
- Application is actively serving the latest updates.

### [Phase 16 - Segment & Campaign ROI Matrix] - 2026-03-02 - COMPLETED
- **`src/ingestion.py`:** Added dynamic column mapping to safely locate `Segment` or `WB tag/flag` and map it uniformly to `segment`.
- **`src/analytics.py`:** Engineered `generate_segment_roi_matrix(df)` to calculate House Margin, NGR per player, and Total NGR by segment.
- **`app.py`:** Built entirely new 📈 Campaigns tab showcasing true segmented profitability with a dynamic color-scaled Plotly bar chart.
- Docker rebuilt and redeployed on port 8503.

### [Phase 17 - Early-Warning VIP Churn Radar] - 2026-03-02 - COMPLETED
- **`src/analytics.py`:** Added `generate_vip_churn_radar(df)` to detect VIPs (>500 NGR or >5000 Turnover last month) whose NGR strictly crashed by >30% and >200 absolute value MoM.
- **`app.py`:** Injected the 📉 EARLY-WARNING VIP CHURN RADAR at the very top of the CRM intelligence tab to instantly alert retention teams of high-value flight risks. Handled missing `month` extraction safely to prevent caching KeyErrors.
- Docker rebuilt and redeployed on port 8503.

### [Phase 18 - PostgreSQL Persistence Layer] - Current
- Replaced temporary RAM/CSV state with a persistent PostgreSQL database (`src/database.py`).
- Implemented schemas: `ops_telemarketing_data`, `financial_data`, `client_mapping`, `contractual_slas`.
- Updated ingestion engines to save directly to DB (`to_sql`) and added cache invalidation (`del st.session_state`) upon upload.

### [Phase 19 - Universal Brand Translator & Quarantine] - Current
- Upgraded Registry to 3-column mapping: Ops Tag (e.g., ROJB) -> Brand Name (Rojabet) -> Client (Offside Gaming).
- Ingestion automatically intercepts tags, translates them to Brand Names, and syncs Ops/Fin data perfectly.
- Added Orphaned Tag detection to warn users of unmapped campaigns and retroactively fix historical DB records upon registry update.

### [Phase 20 - 4-Tier Navigation & Entity-Centric Admin] - Current
- Destroyed flat-tab layout. Implemented 4-tier Sidebar Router: `📊 Dashboard`, `📞 Operations`, `🏦 Financial`, `⚙️ Admin`.
- Built `🏢 Client Hub` in the Admin tier: features a Master Health Board and Drill-Down Profiles per client.
- Built the Inverted Data Completeness Matrix (Months as Rows, Brands as Columns) inside the Client Profile.
- Rebuilt Global Filters to hydrate dynamically from the DB, strip whitespace, and apply RBAC routing.

### [Phase 21 - PostgreSQL RBAC User Management] - Current
- Updated `SPEC.md` with the `users` table entity.
- Removed hardcoded RAM authentication dictionary in `app.py`.
- Implemented PostgreSQL-backed login system storing role and JSONB `allowed_clients`.
- Added `👥 User Management` sub-module inside the Admin workspace for dynamic user CRUD operations.

### [Phase 22 - Ingestion Decoupling & Auto-Analytics] - Current
- Dismantled legacy "Data Control Room" and manual `run_clicked` execution pipeline.
- Decentralized ingestion UI: Financial uploads moved to `📥 Financial Ingestion` tab, Operations to `🗄️ Ops Control Room`.
- Implemented real-time Dashboard Auto-Hydration via `@st.cache_data` wrappers, computing analytical models dynamically from the DB-filtered `_master_df`.

### [Phase 23 - Master Report Generator] - Current
- Upgraded `src/exporter.py` to support standalone Operations reports and Combined Master Reports (Financial + Operations).
- Implemented `@st.cache_data` wrappers returning `.getvalue()` bytes to instantly serialize Excel files without blocking the UI thread.
- Injected highly-contextual `st.download_button` elements directly into the Executive Summary, Financial Deep-Dive, and Operations Command workspaces.

### [Phase 24 - Deterministic Parser & UX Polish] - Current
- Added `financial_format` to `client_mapping` schema.
- Upgraded `src/ingestion.py` to fetch a universal mapping dictionary and explicitly pass `format_type`.
- Overhauled `app.py` Admin Tier. Upgraded Brand Registry to include the parser dropdown. Upgraded User Management to a fully interactive Create/Edit/Delete state machine.
- Renamed "Ops Control Room" to "Operations Ingestion" globally for consistency.

### [Hotfix - Raw Persistence Migration] - Current
- Fixed critical crash `ValueError: DataFrame missing required columns` in analytics engine.
- Replaced `financial_data` aggregate table with `raw_financial_data` table to store full player-level payload.
- Updated `src/ingestion.py` to `to_sql` the entire unified dataframe.
- Updated `app.py` auto-hydration and completeness matrices to read from `raw_financial_data` and remap `player_id -> id`.

### [Hotfix - UI Variable Scope] - Current
Fixed NameError: name '_raw_df' is not defined crash.

Initialized _raw_df = df.copy() at the absolute top of the CRM Intelligence and Campaigns tab blocks before any analytical widgets render.

### [Hotfix - Global Time Frame Filter] - Current
Restored and upgraded the missing UI Date Picker.

Integrated a unified select_slider into the 🌍 GLOBAL FILTERS sidebar.

Date filter now synchronously slices both ops_df (ops_date) and financial_df (report_month) before passing them to the dashboard cache.

### [Hotfix - Importer & Duplicate Protection] - Current
Resolved ImportError in the Admin Hub Completeness tab by wiring the button to the correct load_all_data_from_uploads function.

Upgraded src/ingestion.py to check raw_financial_data for existing report_month + brand combinations prior to parsing. Duplicate files are now safely rejected with a UI warning while the rest of the batch processes normally.

Switched IngestionRegistry() to IngestionRegistry.load() during uploads to prevent registry state wipe.

### [Hotfix - Restored rev_col variable] - Current
Fixed NameError: name 'rev_col' is not defined crash in the Financial Deep-Dive tab.

Restored revenue_mode and rev_col variable declarations that were inadvertently deleted during the Phase 22 legacy code cleanup.

### [Hotfix - Time-Series Continuous Padding] - Current
Fixed sparse dataset distortion where missing months were skipped in charts and MoM comparisons.

Upgraded Global Date Slider to dynamically calculate continuous month ranges.

Injected _pad_missing_months() into src/analytics.py to mathematically reindex missing time frames, zero-filling empty months before generating ratios and aggregates.

### [Hotfix - Operations Date Normalization] - Current
Fixed ValueError: not enough values to unpack crash in the Global Time Frame Filter.

Added dynamic string replacement (_ to -) for ops_date during sidebar hydration to ensure standard YYYY-MM formatting across all continuous zero-padding and slider extraction logic.
[ H o t f i x 
 
 - 
 
 O p e r a t i o n s 
 
 T a b 
 
 S t r i n g 
 
 A l i g n m e n t ] 
 
 - 
 
 C u r r e n t 
 
 F i x e d 
 
 b l a n k 
 
 t a b 
 
 r e n d e r i n g 
 
 b u g 
 
 i n 
 
 t h e 
 
 O p e r a t i o n s 
 
 W o r k s p a c e . 
 
 R e m o v e d 
 
 s t a l e 
 
 t a b _ m a p 
 
 a l i a s e s 
 
 a n d 
 
 a l i g n e d 
 
 t h e 
 
 r e n d e r i n g 
 
 i f 
 
 c o n d i t i o n 
 
 s t r i c t l y 
 
 t o 
 
 t h e 
 
 n e w 
 
 =����  O p e r a t i o n s   I n g e s t i o n 
 
 k e y . 
 
 
Removed stale tab_map aliases and aligned the rendering if condition strictly to the new '??? Operations Ingestion' key.

[Hotfix - Operations Tab UI Verified] - Current
Visual verification confirmed the string match bug is patched and the file uploader is rendering perfectly.

### [Phase 25 - Deployment Prep & Security] - Current
- Extracted hardcoded database credentials from `docker-compose.yml` into `.env` standard.
- Generated `.env.example` template.
- Wrote comprehensive `README.md` detailing architecture, Quickstart Docker instructions, and SDD compliance.

### [Hotfix - Matrix Date Normalization] - Current
Fixed duplicate rows in the Admin Hub Data Completeness Matrix (e.g., 2025_02 and 2025-02 showing as separate rows).

Added .str.replace("_", "-") to the SQL query outputs for both Ops and Fin datasets in the matrix builder to ensure perfect alignment and correctly fused checkmarks.

### [Hotfix - Permanent Date Healing] - Current
Fixed fractured duplicate rows in the Admin Hub Completeness Matrix.

Injected an UPDATE REPLACE query into src/database.py initialization to permanently rewrite all legacy ops_date underscores (_) into hyphens (-).

Updated src/ingestion.py so future Operations uploads are natively saved with hyphens, permanently aligning Ops and Fin datasets at the persistence layer.

### [Feature - Global Client Renaming] - Current
Added "Rename Client" expander to the Admin -> Client Hub -> Manage Profile view.

Engineered a safe SQL-cascade that globally updates client_mapping, raw_financial_data, ops_telemarketing_data, and retroactively updates JSON-encoded RBAC allowed_clients lists in the users table to prevent permission drops upon renaming.

### [Feature - UI/UX Modernization] - Current
Implemented global theme configuration via .streamlit/config.toml applying a modern dark-slate aesthetic.

Injected custom CSS into app.py to modernize button radii, add hover transitions, and introduce drop shadows to UI components for a premium enterprise feel.

### [Feature - Client Onboarding UI] - Current
Injected an "Onboard New Client" form into the ?? Client Hub.

Allows Superadmins to natively create new clients from the UI by defining their anchor brand_code, client_name, and financial_format directly into the client_mapping database table.

### [UI Polish - Header Modernization] - Current
Updated the main application header to D-ROCK DASHBOARD V2.0.

Replaced the legacy pipeline subtitle with Enterprise Business Intelligence & Operations Command to reflect the new real-time PostgreSQL architecture.

### [Feature - Auto-Seeded Client Registry] - Current
Upgraded seed_test_data.py to execute pre_seed_client_mappings() before file ingestion.

Hardcoded exact Client/Tag combinations (LIM, REL, RHN, SIM, PP, INSP, PE, LV) into the seeder to completely eliminate the UNKNOWN manual mapping step during database rebuilds.

### [Hotfix - Client Identity Consolidation] - Current
Fixed UI duplicates in the Global Client matrix (e.g., LIM vs Limitless).

Updated CLIENT_HIERARCHY in src/ingestion.py to natively map sub-brands to their full Enterprise identities.

Created and executed heal_clients.py to run a retroactive SQL cascade, merging all legacy shorthand data into their unified Enterprise client buckets.

### [Hotfix - Sub-Brand Seeder Immunity] - Current
Expanded seed_test_data.py to explicitly declare sub-brand mappings (YU -> Yuugado, BAH -> Bahigo, etc.) ensuring manual UI renames are no longer required after a docker-compose down -v wipe.

Upgraded heal_clients.py to execute a brand-level SQL UPDATE cascade, immediately translating raw tags into pretty brand names in the live UI dropdowns.

### [Feature - Parent-Child Format Inheritance] - Current
Implemented a hardcoded parser override in src/ingestion.py. Any file mapped to LeoVegas Group strictly executes via the LeoVegas parser, and Offside Gaming via the Offside parser, ignoring accidental UI misconfigurations.

Created and executed heal_formats.py to retroactively correct any existing client_mapping entries in the live database.

### [Hotfix - LeoVegas Multi-Brand Parsing] - Current
Modified src/ingestion.py to conditionally respect native brand column data in Master Trackers, preventing the target_brand filename override from destroying granular brand data.

Added Bet UK, BetMGM, Expekt, GoGoCasino, and RoyalPanda to seed_test_data.py.

Wrote and executed heal_leovegas.py to purge corrupted LeoVegas records, inject missing mappings, and headlessly re-ingest the raw files.

### [Feature - SLA Breach Watchdog] - Current
Implemented an intelligent SLA Breach Alert box on the Operations Dashboard.

Resolves the mathematical flaw of averaging monthly volume targets on daily line charts by intelligently aggregating total volume per campaign_type (RND/WB) and cross-referencing against the contractual_slas table. Flashes a dynamic UI warning if minimums are not met.

### [Hotfix & UX - Stability and Loading States] - Current
Fixed a TypeError crash in the Operations SLA Tracker by safely coercing missing target_cac_usd values to 0.0.

Implemented st.spinner UI states during pre-login authentication and global database hydration to provide users with visual I/O feedback and prevent the appearance of application freezing.

### [Hotfix - Snapshot KeyError Fix] - Current
Resolved KeyError: "Column(s) ['Calls', 'KPI1-Conv.'] do not exist" crashing the 12-Month Operations trend chart.

Updated the Pandas aggregation dictionary to query the correct PostgreSQL schema fields (calls, conversions, kpi2_logins) and appended a .rename() step to safely translate them back to the frontend UI labels expected by Plotly.

### [Hotfix - Plotly Render & LI% Fix] - Current
Resolved ValueError in Plotly line charts by synchronizing the .rename() dictionary with the frontend px.line y-axis variable arrays (Calls, KPI1-Conv., KPI2-Login).

Restored li_pct to the snapshot .agg() dictionary (using .mean()) and injected it into the Efficiency Trends chart as LI%.

### [Hotfix - 12-Month Roll-Up KeyError] - Current
Resolved KeyError: "Column(s) ['kpi2_logins', 'li_pct'] do not exist" in the 12-Month Operations tab.

Updated the downstream monthly_trends aggregation dictionary to correctly target the uppercase UI-facing columns (KPI2-Login, LI%) that were renamed upstream in the primary daily_trends dataframe.

### [Feature - Daily API Ingestion Support] - Current
Upgraded the Regex extraction in src/ingestion.py to natively parse YYYY-MM-DD filenames.

Enabled seamless ingestion of daily granular time-series files generated by the automated API pull script.

### [Feature - Integrated API Command Center] - Current
Refactored the daily CallsU extraction script into a robust, object-oriented background worker (src/api_worker.py).

Implemented an asynchronous threading architecture inside the Streamlit Control Room, allowing users to select a date range and trigger the ETL pipeline natively without freezing the UI.

Attached a live streaming Log Console mapping to data/api_sync.log so users can monitor exactly what the detached API worker is querying on the vendor's server.

### [Feature - Smart API Worker Auto-Ingestion] - Current
Upgraded src/api_worker.py to detect existing daily files on disk and automatically execute PostgreSQL database ingestion (to_sql) rather than skipping the day entirely.

Provisioned 2026 raw data directories for manual file seeding.

### [Feature - Granular Data Completeness] - Current
Replaced legacy binary completeness checks with a dynamic, calendar-aware daily evaluation function in app.py.

System now actively counts unique ops_date records per month and compares them against expected elapsed calendar days, surfacing 🟢 Complete, 🟡 Warning (missing < 3 days), 🟠 Partial, and 🔴 Incomplete statuses.

### [Bugfix - API Worker Schema Mismatch] - Current
Resolved CompileError (f405) during autonomous ingestion.

Wired src/api_worker.py directly into src/ingestion.py -> load_operations_data_from_uploads() to ensure raw CallsU API payloads undergo the standard ETL transformation (Brand mapping, CAC calculation, column pruning) before being inserted into PostgreSQL.

### [Feature - API Worker Auto-Retry Queue] - Current
Refactored run_historical_pull in src/api_worker.py to utilize a process_day helper function.

Implemented a Two-Pass Retry Queue: Jobs that timeout or fail connection are added to an internal queue, deferred until the main loop completes, and retried automatically.

Added a 🚨 CRITICAL WARNING terminal alert for jobs that persistently fail after the secondary pass.

Increased internal polling timeout to 10 minutes and reduced log verbosity to prevent terminal clutter.

### [Bugfix - True CAC/Operations Matrix Blank Load] - Current
Debugged an issue where newly injected API daily data was successfully inserted into PostgreSQL (1,842 rows) but failing to render on the `Operations Command` tab, triggering the "No CallsU operations data loaded" fallback state.

**Root Cause 1 (Temporal Filtering):**
* The global sidebar timeframe filter evaluates month boundaries using strings (e.g., `"2026-01" <= end_month`).
* Legacy monthly files passed because they natively mapped to YYYY-MM.
* The new daily system stores native dates (e.g., `"2026-01-14"`). Under Python's strict string comparator, `"2026-01-14"` is evaluated as strictly **greater than** `"2026-01"`. 
* As a result, *all* daily operations summaries were being completely stripped from the UI `filtered_ops` dataframe rendering the view empty.

**Fix 1:**
Appended `"-99"` format strings to the end boundary of the time-frame slicer (e.g., `ops_date <= f"{end_month}-99"`), guaranteeing that granular days successfully fall within the inclusive String boundary comparison.

**Root Cause 2 (Streamlit Persistent Memory Cache):**
* Since the new `api_worker.py` system operates asynchronously via `docker-compose.yml`, data is injected directly into PostgreSQL without passing through Streamlit. 
* Streamlit persistently holds the *old*, pre-sync `pd.read_sql` result in `st.session_state["raw_ops_df"]` until explicitly cleared.

**Fix 2:**
Injected a manual `🔄 Sync with Database` global override button into the Analytics sidebar, which forcibly flushes `raw_ops_df`, `raw_fin_df`, and `st.cache_data` before rerunning the hydration engine, ensuring immediate background worker visibility.

### [Bugfix - Master Health Board & SLA Watchdog SQL Crash] - Current
Debugged an `UndefinedTable` error referencing `contractual_slas` crashing the `⚙️ Admin` tab and silencing the `Operations Command` SLA analyzer.

**Root Cause:**
* During the recent benchmark database refinement, the monolithic `contractual_slas` table was intentionally dropped and split into `contractual_volumes` and `granular_benchmarks` in `src/database.py`.
* Two generic query lines in `app.py` were not updated to target the new tables.

**Fix:**
Replaced `SELECT client_name, brand_code FROM contractual_slas` mapped instances with `contractual_volumes` to restore DB harmony.

### [Phase 2 - UI & Operations Expansion] - Current
- Fixed Financial Matrices missing data issues by shifting `latest_month` evaluations to be loop-intrinsically brand-specific. 
- Injected Elite Date Range Quick-Select Helper mapped into `st.session_state` to default queries to a 7-day memory state for performance and UX. 
- Integrated side-bar dynamic Target Category and Target Segment parsing via Campaign Name heuristic rules. 
- Purged the redundant manual "Sync with Database" button.
- Updated Pie Chart configurations to rigidly enforce green/yellow/red bindings to Deliveries, No Answer, and Issues matrices.
- Added a 90-Day analytical cut timeframe to the Volume trends UI. 
- Transformed the primary volume scale to measure strict row metrics (Records) vs contractual volumes (SLA Minimum), with automated Warning detection overlays. 
- Created and executed `src/seed_slas.py` to seed baseline logic into the backend DB. 
- Aligned Admin `Manage Profile` UI structure for optimal rendering.

### [Feature - Client-Level SLA Pacing Warning] - Current
- Implemented Client + Lifecycle SLA aggregation in `app.py`.
- Injected dynamic pro-rating math `(monthly_goal / 30) * timeframe_days` to accurately gauge pacing and prevent false-positive SLA breach alerts when viewing abbreviated dashboard timeframes.
- Integrated dynamic `st.warning()` UI rendering to alert Operations Managers of under-delivery.

### [Feature - Phase 8: Module Decoupling] - Current
- Enforced strict RBAC by deprecating the unified Dashboard view.
- Migrated all high-level Financial intelligence (Matrices, Cash Flow, Smart Narratives) strictly into the Financial Workspace.
- Migrated all high-level Operations intelligence (Cannibalization, VIP Health, CRM/Campaign tabs) strictly into the Operations Workspace.

### [Feature - Data Completeness & Financial Validation] - Current
- Upgraded the Operations Ingestion tab to include granular YYYY-MM operational aggregations (Records, Logins, Conversions, Calls).
- Implemented a Brand-specific filter dropdown.
- Engineered a cross-database validation function get_financial_completeness to automatically flag missing client financial uploads as Pending, Warning (1 month late), or Issue (2+ months late).
- Added a dynamic daily drill-down sub-table for isolated monthly inspections.

### [Bug Fix - Operations Telemarketing '# Records' Data Loss] - Current
- **Root Cause & Forensics Check**: The user reported that an uploaded file containing "28,816 Records" was displaying 65 instead. Upon querying PostgreSQL, we identified that the telemarketing ingestion engine \`src/ingestion.py\` completely discarded the \`# Records\` CSV column during runtime parsing, leaving the database utterly blind to list volume sizing. The UI had previously masked this architecture flaw by renaming the \`calls\` column to \`Records\` in its charts.
- **DB Alteration**: Executed \`ALTER TABLE ops_telemarketing_snapshots ADD COLUMN records INTEGER DEFAULT 0\` to inject the missing metric natively into PostgreSQL.
- **Python Parser Fix**: Expanded \`load_operations_data_from_uploads\` to isolate and cast \`row['# Records']\` into the \`records_to_insert\` SQL bundle.
- **Completeness Fix**: Rewired \`query_ops\` in \`app.py\` to directly query \`records\` instead of mistakenly aliasing \`d_total\` (65 deliveries).
- User must re-upload any previous files to hydrate this missing system column.

### [Feature - Global Operations Recap] - Current
- Engineered a dual-view "Global Operations Recap" section inside the Admin Client Hub.
- Built `🏢 By Client` aggregation tab displaying all-time sums for Records, Logins, Conversions, and Calls per client, including a Grand Total footer.
- Built `📅 By Month` aggregation tab displaying chronological system-wide volume totals, including a Grand Total footer.

### [Feature - Operations Metrics & Dual-Axis Trends] - Current
- Swapped baseline volume metric from `Calls` to `Records` across the SLA tracker and comparison matrices.
- Upgraded Efficiency Trends to a Plotly dual-axis chart, rendering raw Logins/Conversions as bars on the primary axis and Login%/Conv% as trend lines on the secondary axis.
- Formatted `D Ratio` dynamically as a true percentage (`%.2f%%`) across all Operations tables.

### [Feature - Ingestion Engine Expansion] - Current
- Expanded `load_operations_data_from_uploads` to capture granular costs, verifications (HLRV, 2XRV), SMS/Email funnel metrics, and Opt-outs.
- Updated database schema for `ops_telemarketing_data` and `ops_telemarketing_snapshots` to accept the 20 new columns, preventing data loss during CSV uploads.
- **Historical Backfill Executed**: Created and executed `reingest_ops.py` to truncate all operations tables to prevent duplication and re-ingested 357 raw data files from `data/raw/callsu_daily`. Successfully parsed natively into 28,863 rows without a single duplicate.

### [Feature - Dispositions Backfill] - Current
- Added the 9 missing call dispositions (D+, D, D-, AM, DNC, NA, DX, WN, T) to `src/ingestion.py` and PostgreSQL schema.
- Re-ran the safe `reingest_ops.py` backfill script to fully populate the database.

### [Feature - Omnichannel Quality Engine] - Current
- Engineered the Campaign Execution module utilizing the newly ingested 74-column dataset.
- Implemented Gross vs. Net pacing math to exclude `HLRV` and `2XRV` pre-verification failures from floor completion metrics.
- Built the 3-Pillar Disposition Matrix (Deliveries, NAs, Issues) to instantly isolate script fatigue vs. telecom blocking vs. bad client data.
- Added digital funnel tracking (SMS delivery / Email opens) for cross-channel visibility.

### [Feature - Client SLA Volume Tracker] - Current
- Engineered the Client SLA Volume Tracker table in the Operations Command tab.
- Implemented regex/string mapping to automatically categorize campaigns into ACQ, RET, and WB segments.
- Applied dynamic Pandas conditional formatting (Red/Green) to instantly highlight volume deficits vs. surpluses against hardcoded client SLA targets.

### [Feature - Railway Deployment Prep] - Current
- Prepared codebase for Railway.app cloud deployment.
- Created Procfile to bind Streamlit to Railway's dynamic $PORT.
- Updated requirements.txt for Nixpacks build environment.
- Refactored `src/database.py` to securely consume Railway's `DATABASE_URL` environment variable while maintaining local fallback.

### [Feature - Repository Cleanup & Documentation] - Current
- Audited and organized project directory structure.
- Hardened `.gitignore` to prevent raw data files (CSVs/Excel) and `.env` secrets from leaking to GitHub.
- Drafted a comprehensive `README.md` outlining the project architecture, tech stack, local setup, and SDD development guidelines.
- Sanitized codebase of leftover debug statements.

### [Feature - Codebase Organization] - Current
- Cleaned the root directory by deleting 10+ dead scratchpad, testing, and formatting files.
- Created a `scripts/database/` directory and consolidated all one-off healing and migration scripts.
- Removed duplicate files to ensure a clean deployment structure.

### [Feature - Cold Start Resilience] - Current
- Implemented `try...except` block in `app.py` catching SQLAlchemy `ProgrammingError`.
- Prevented fatal "UndefinedTable" crashes on fresh cloud database deployments.
- Added graceful UI degradation prompting the user to use the Ingestion tab on first boot.

### [Hotfix - Cold Start UI Unblock] - Current
- Removed `st.stop()` from the initial database hydration `except` block.
- Allowed the Streamlit script to continue executing and rendering the main UI tabs with empty DataFrames, unblocking access to the Ingestion dropzones on fresh deployments.

### [Hotfix - Cloud Schema Alignment] - Current
- Upgraded `init_db()` in `src/database.py` to dynamically inject the 29 Cost, Funnel, and Disposition columns into existing tables.
- Resolved silent `to_sql` ingestion failures on fresh cloud deployments.

### [Bugfix - Missing Optout Columns in Schema] - Current
- Investigated empty state and logs using the Debugging Workflow.
- Discovered that 4 new columns (`optouts_all`, `optout_call`, `optout_sms`, `optout_email`) were being scraped by the `ingestion.py` engine but were missing from the production `init_db()` PostgreSQL schema.
- This caused `to_sql(if_exists="append")` to silently fail on upload, completely blocking data ingestion while throwing a terminal-only `UndefinedColumn` error.
- Expanded the safe schema migration loop in `database.py` to inject these 4 columns on boot, finally allowing the pipeline to succeed.

### [Hotfix - Empty UI Tabs (Blank States)] - Current
- Discovered that the `🕵️ CRM Intelligence` and `📈 Campaigns` tabs in `app.py` were nested entirely inside the global `if not _master_df.empty:` financial data check.
- When the cloud database was fresh and lacked financial data, Streamlit skipped the entire rendering block, resulting in a blank white screen when users clicked those tabs.
- Injected a graceful `else:` block that explicitly renders these tabs with a bright yellow warning: `"⚠️ No Financial Data Loaded. Please navigate to the 📥 Financial Ingestion tab..."` instead of leaving the user with a broken UI.

### [Hotfix - Date Slider Crash on Single-Day Datasets] - Current
- The Operations Command tab was crashing with `StreamlitAPIException: Slider min_value must be less than the max_value` when users uploaded exactly one day of data.
- Added a safeguard to the global date range calculation that evaluates `if min_db_date.date() >= max_date.date()` and automatically adds `pd.Timedelta(days=1)` to the `max_date` if true. This ensures Streamlit sliders always have a valid, strict range.

### [UI Enhancement - Trend Chart Legends] - Current
- Updated the "Daily SLA Trends & Performance" charts layout in `app.py`.
- Moved the legends from the default side position to the bottom (`orientation="h", yanchor="bottom", y=-0.5, xanchor="center", x=0.5`).
- Applied explicit bottom padding (`margin=dict(t=40, b=80, l=40, r=40)`) to permanently fix the legends physically overlapping the X-axis dates.
- Debugged the Average Volume line logic (`len(df_filtered) > 0`) to assure it renders even on single-day telemetry, and brightened its opacity to `rgba(255, 255, 255, 0.9)` to guarantee visibility against the dark background.

### [Feature - Daily Operations Automation] - Current
- Created `scripts/jobs/daily_operations_sync.py` to automate the daily CallsU API data pull.
- Engineered safe timezone logic to fetch "yesterday's" data relative to UTC (`datetime.now(timezone.utc) - timedelta(days=1)`).
- Configured exit codes (`sys.exit`) for cloud cron monitoring to ping success (`0`) or failure (`1`).
- Target deployment schedule calculated: `30 3 * * *` (4:30 AM CET corresponds to 03:30 UTC for the Railway container).
- Hard-locked the "Automated CallsU API Sync" manual triggers in the UI (`app.py`) to prevent users from accidentally selecting the current date (which contains incomplete intra-day metrics) or future dates. Forced both the default `value` and `max_value` to strictly default to "yesterday."

### [Infrastructure - Development Lifecycle] - Current
- Created a separate git branch (`dev`) as the default staging environment for all future code modifications.
- Protected the `master` branch to act as the pure production endpoint, requiring explicitly tested PRs/merges before triggering live Railway deployments.

### [UI Enhancement - Sidebar Navigation] - Current
- Cleaned up the app sidebar layout by removing two redundant horizontal divider lines.
- Injected a clean text logo (`### 🦅 CallsU Command`) at the very top of the sidebar.
- Re-oriented the date "Quick Select" preset radio buttons to stack vertically (`horizontal=False`) rather than a clumped horizontal list, drastically improving visual readability.

### [Feature - Benchmarking Phase 1 & 2] - Current
- Updated `src/database.py` schema injection to include `extracted_engagement`, `extracted_segment`, `extracted_lifecycle`, and `country` across both telemetry and snapshot tables.
- Overhauled `src/ingestion.py` and `src/cron_callsu.py` parsing heuristics to accurately separate Lifecycles from Segments and capture LI/NLI engagement markers.

### [Feature - Benchmarking Phase 3] - Current
- Initialized `ops_historical_benchmarks` table in `src/database.py` to store calculated averages.
- Built `scripts/jobs/generate_benchmarks.py` to calculate two-step true daily averages across Brand/Country/Lifecycle/Segment/Engagement signatures.
- Made the benchmark generation script idempotent to safely prevent data duplication.

### [Feature - Benchmarking Phase 4] - Current
- Overhauled the Streamlit sidebar to feature 8 comprehensive search-enabled filters (Client, Brand, Category, Country, Lifecycle, Segment, Engagement, Campaign).
- Implemented Full Country Name mapping and searchable select boxes for improved UX.
- Updated pandas DataFrame filtering logic (`filtered_ops_df`) to perfectly cascade with the new schema columns.
- Performed a global UI text cleanup, correctly renaming legacy "Segment" references to "Lifecycle".

### [Feature - Benchmarking Phase 5] - Current
- Implemented `load_benchmarks()` to cache and pull 6-month historical averages into the frontend.
- Built dynamic matching logic to align the current active sidebar filters with the historical benchmark signatures.
- Upgraded the top-level `st.metric` cards in Operations Command to display dynamic green/red deltas comparing current performance against historical baselines.
# # #   [ F e a t u r e   -   T r u e   C A C   R e f i n e m e n t s ]   -    
 -   E x p a n d e d   \ o p s _ h i s t o r i c a l _ b e n c h m a r k s \   s c h e m a   t o   s t o r e   a v e r a g e   d a i l y   t e l e c o m   c o s t s   a n d   T r u e   C A C   b a s e l i n e s .  
 -   U p g r a d e d   t h e   b e n c h m a r k   g e n e r a t o r   s c r i p t   t o   c a l c u l a t e   h i s t o r i c a l   C A C   s i g n a t u r e s .  
 -   R e f i n e d   t h e   T r u e   C o s t - P e r - O u t c o m e   L e a d e r b o a r d   i n   \  p p . p y \   t o   d i s p l a y   d y n a m i c   \ C A C   D e l t a \   c o l u m n s ,   i n s t a n t l y   h i g h l i g h t i n g   c a m p a i g n s   b l e e d i n g   t e l e c o m   m a r g i n s   v s .   6 - m o n t h   a v e r a g e s .  
  
 ### [Feature - True CAC Refinements] - 2026-03-07
- Expanded `ops_historical_benchmarks` schema to store average daily telecom costs and True CAC baselines.
- Upgraded the benchmark generator script to calculate historical CAC signatures.
- Refined the True Cost-Per-Outcome Leaderboard in `app.py` to display dynamic `CAC Delta` columns, instantly highlighting campaigns bleeding telecom margins vs. 6-month averages.
- Integrated newly registered brands (Wetigo, Hahibi, NitroCasino) into the DB mapping layer and updated category names to full names (Casino, Sportsbook, etc.).


### [Hotfix - Data Hygiene] - 2026-03-07
- Merged redundant tags for Royal Panda (`ROYALPANDA` -> `RP`), Expekt (`EXPEKT` -> `EX`), and Rojabet (`ROJB` -> `ROJA`) directly in PostgreSQL `client_mapping` and historical tables.
- Corrected brand name typos: `Hahibi` -> `Bahibi` and `CASINODAYS` -> `CasinoDays`.
- Cleaned `BRAND_CODE_MAP` and `CLIENT_HIERARCHY` inside `src/ingestion.py` to prevent future drift.


### [Feature - Phase 7 Deliverable A: CRM Engine] - 2026-03-07
- Created `src/analytics/crm_engine.py` to calculate player-level RFM (Recency, Frequency, Monetary) metrics.
- Implemented the 7-tier Smart Profile heuristic array to automatically tag VIPs, Churn Risks, and Rising Stars based on a $500 Lifetime GGR threshold and recency signals.
- Integrated the CRM Engine into `app.py`, rendering dynamic leaderboards in the CRM Intelligence tab.


### [Refactor - SLA Trend Global Sync] - 2026-03-07
- Stripped redundant 7/30/90 day local UI tabs from the "Daily SLA Trends & Performance" section in `app.py`.
- Rewired the SLA charting logic to directly consume the global `filtered_ops_df`, ensuring perfectly synchronized date filtering across the entire dashboard.


### [Feature - Phase 7 Deliverable B: Financial Curves] - 2026-03-07
- Engineered `src/analytics/financial_curves.py` to calculate complex 80/20 Pareto distributions and Cumulative LTV cohort progressions.
- Integrated Plotly visualizations into the `🏦 Financial Deep-Dive` tab in `app.py`, providing Directors with high-level structural revenue analytics.


### [Phase 9 - Final Polish & Launch] - Complete
- Removed lingering debug code from `src/database.py` and `src/ingestion.py`.
- Locked down `requirements.txt` to the exact virtual environment dependencies (Streamlit, Pandas, Plotly, SQLAlchemy, Psycopg2, Openpyxl, Python-Dotenv) to ensure long-term cloud stability.
- Formatted README.md for the Director level with a clear 4-Tier structural overview and handoff guidelines.
- Cleaned filesystem of dummy data in `data/raw` and `data/campaigns/` to prevent false testing loads.
