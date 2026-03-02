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
