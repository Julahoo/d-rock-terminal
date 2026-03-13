# D-ROCK FINANCIAL TERMINAL - TECHNICAL SPECIFICATION (v3.1)

## 1. CORE PRINCIPLES & ARCHITECTURE
- **Goal:** Full-stack CRM intelligence platform for multi-client iGaming operations. Automates financial ETL, telemarketing ops tracking (CallsU), campaign naming convention enforcement, and executive business intelligence.
- **Architecture:** 4-Tier Modular Enterprise Platform (V3.0). ETL Pipeline → PostgreSQL persistence → Streamlit UI with RBAC. Daily automation via Railway cron.
- **Tech Stack:** Python 3.10+, `pandas`, `SQLAlchemy`, `psycopg2`, `openpyxl`/`xlsxwriter`, `streamlit`, `plotly`.
- **UI Theme:** Material Design 3 Dark — Inter font (Google Fonts), Deep Purple primary (`#7C4DFF`), GitHub-dark backgrounds (`#0D1117`/`#161B22`), soft white text (`#E6EDF3`). Custom CSS injection via `st.markdown(unsafe_allow_html=True)` for metric cards with gradient/elevation, pill-style tabs/radio, rounded inputs/buttons with hover glow, and slim scrollbars. Config: `.streamlit/config.toml`.
- **Deployment:** Railway (Docker) with PostgreSQL. Automated daily ops sync at 03:30 UTC.

---

## 2. DATA MODELS (PostgreSQL Persistence Layer)

### 2.1 Database Tables (Single Source of Truth)
- **`ops_telemarketing_data`:** Core telemarketing operational records. Key columns: `campaign_name`, `ops_client`, `ops_brand`, `ops_date`, `records`, `calls`, `conversions`, `total_cost`, `true_cac`, dispositions (`d_plus`, `d_minus`, `d_neutral`, `am`, `dnc`, `na`, `dx`, `wn`, `t`), channel metrics (`sa`, `sd`, `sf`, `sp`, `ev`, `es`, `ed`, `eo`, `ec`, `ef`), optouts, and **8 campaign naming convention components:** `country`, `extracted_lifecycle`, `extracted_segment`, `extracted_engagement`, `extracted_product`, `extracted_language`, `extracted_sublifecycle`. Note: `campaign_name` is NOT unique — multiple rows per campaign per date.
- **`ops_telemarketing_snapshots`:** Mirror of `ops_telemarketing_data` for daily snapshots used by Dashboard Pulse matrices and benchmarks. Same column schema.
- **`raw_financial_data`:** Core financial records populated from ingestion. Columns: `player_id`, `client`, `brand`, `country`, `wb_tag`, `segment`, `bet`, `revenue`, `ngr`, casino/sports splits, `deposits`, `withdrawals`, `bonus_*`, `tax_total`, `report_month`, `reactivation_date`, `campaign_start_date`, `reactivation_days`.
- **`client_mapping`:** The Universal Brand Translator registry. Columns: `brand_code` (Ops Tag), `brand_name`, `client_name`, `financial_format` (Enum: Standard, LeoVegas, Offside). Used to normalize incoming data and intercept/quarantine orphaned tags.
- **`contractual_slas`:** Client-specific SLA thresholds. Columns: `client_name`, `brand_code`, `lifecycle`, `monthly_minimum_records`, `target_cac_usd`.
- **`ops_historical_benchmarks`:** Aggregated benchmark averages per brand/country/lifecycle/segment/engagement.
- **`users`:** RBAC authentication registry. Columns: `username` (UNIQUE), `password`, `role` (Superadmin/Admin/Viewer), `name`, `allowed_clients` (JSONB array).
- **`ops_contact_events`:** Player-level journey events pulled from iWinBack contact APIs. Columns: `box_id`, `contact_id`, `account_number`, `campaign_id`, `brand_id`, `event_type` (login/register/deposit), `event_at`, `extra_data` (JSONB), `ingested_at`. Indexes on `campaign_id`, `account_number`, and `(event_type, event_at)`. Financial linkage via `account_number` → `raw_financial_data.player_id` for True ROI.

### 2.1.1 Deterministic UI-Driven Parsers
- The parser logic is now explicitly mapped in the database via the `financial_format` column, completely replacing the legacy "column sniffing" fallback.
- **LeoVegas Path (V2.0):** Maps `"Player Key"` -> `"id"`, extracts true `"ngr"` from `"NGR (Total) € after Tax"`, and ingests specific Casino/Sports splits. 
- **Offside Gaming Path (Fallback):** Maps `"Player unique identifier"` -> `"id"`. Enforces default fallbacks where `ngr = revenue`, `country = "Global"`, and Casino/Sports vertical splits are zeroed out.

### 2.2 Aggregated Business Entities
- **`MonthlyBrandSummary` & `BothBusinessSummary`:** - *Base:* `Turnover` (Handle), `GGR` (Revenue), `Margin` (Hold %), `Revenue (15%)` (Commission/Net Income).
  - *Demographics:* `Total_Players`, `Profitable_Players` (Winners: Rev > 0), `Negative_Yield_Players` (Losers: Rev < 0), `Flat`.
  - *Lifecycle:* `New_Players`, `Returning_Players`, `Reactivated_Players`, `Conversions` (New + Reactivated).
  - *Ratios:* `GGR_Per_Player`, `Income_Per_Player`.
- **`TimeSeriesMetrics`:** Calculates MoM (Month-over-Month), QoQ, YoY, and YTD changes for all base metrics.
- **`PlayerMasterList` (CRM Engine):** Grouped by `id`. Tracks `Lifetime_GGR`, `Lifetime_Turnover`, `First_Month`, `Last_Month`, `Months_Active`, `Months_Inactive`, `Last_Month_Turnover`, and `Avg_Monthly_Turnover`.

---

## 3. CORE BUSINESS LOGIC & HEURISTICS

### 3.1 Predictive Forecasting
- **EOY Run-Rate:** Calculates End-of-Year projections for Turnover, GGR, and Revenue (15%) using: `(YTD_Total / Current_Month_Index) * 12`.

### 3.2 VIP & RFM Tiering (`generate_rfm_summary`)
- **👑 True VIPs:** Active this month, played 3+ historical months, Lifetime GGR > $500.
- **⚠️ Churn Risk VIPs:** Inactive this month, played 3+ historical months, Lifetime GGR > $500.
- **🐟 Casuals:** Everyone else.

### 3.3 Smart Campaign Profiling (CRM Heuristics)
Evaluated row-by-row in this strict priority order:
- Calculate `Tenure_Months`: The exact difference in months between a player's `First_Month` and `Last_Month`, plus 1.
  - 🏆 **Ironman Legend:** If `Months_Active` >= 6 AND `Months_Inactive` == 0 AND `Months_Active` == `Tenure_Months`. (This means they have played every single month since they joined, for at least 6 months).
  - *(Evaluate this BEFORE Active Crown Jewel).*
1. 👑 **Active Crown Jewel:** `Lifetime_GGR` >= 1000 & `Months_Inactive` == 0
2. 📉 **Cooling Down (Velocity Risk):** `Months_Inactive` == 0 & `Last_Month_Turnover` < (`Avg_Monthly_Turnover` * 0.5) & `Lifetime_Turnover` >= 1000
3. 🛑 **Promo Exclusion (Risk):** `Lifetime_GGR` < 0 & `Lifetime_Turnover` > 5000
4. 🚨 **Early Churn VIP:** `Months_Inactive` == 1 & `Lifetime_GGR` > 500
5. 🌟 **Rising Star:** `Months_Active` <= 2 & `Lifetime_Turnover` > 1000 & `Months_Inactive` == 0
6. 🎯 **Cold Crown Jewel:** `Months_Inactive` >= 3 & `Lifetime_GGR` > 1000
7. ✉️ **Standard Lifecycle:** Everyone else

### 3.4 Cross-Brand Cannibalization (Overlap Analysis)
- **Data Prep (`generate_overlap_stats`):** - Separate the `raw_df` into Rojabet and Latribet datasets.
  - Extract the unique `id`s from both sets.
  - Calculate the intersection (players existing in both sets).
  - Calculate `Overlap_Count` (number of shared players) and `Overlap_GGR` (sum of `revenue` for these specific players across all time).

### 3.5 Cumulative LTV Curves (Lifetime Value)
- **Data Prep (`generate_ltv_curves`):**
  - Identify the `cohort_month` for each `id` (minimum `report_month`).
  - Merge `cohort_month` back into the raw dataset.
  - Calculate `month_index` (integer difference between `report_month` and `cohort_month`).
  - Group by `cohort_month` and `month_index` to get the sum of `revenue`.
  - Calculate the cumulative sum (`cumsum`) of `revenue` along the `month_index` for each distinct `cohort_month`.
- **Visualization:** Use Plotly Express (`px.line`) to plot `month_index` on the X-axis, `cumulative_revenue` on the Y-axis, colored by `cohort_month`.

### 3.6 Program Margin Profiling (`wb_tag` Hold %)
- **Data Prep (`generate_program_summary`):**
  - When grouping raw data by `brand`, `report_month`, and `wb_tag` (Program), aggregate:
    - `GGR`: sum of `revenue`
    - `Turnover`: sum of `bet`
    - `Total_Players`: count of unique `id`
  - Calculate `Margin`: `GGR` / `Turnover` (handle division by zero).

### 3.7 The 80/20 Pareto Curve (Revenue Concentration)
- **Data Prep (`generate_pareto_curve`):**
  - Group raw data by `id` to calculate `Lifetime_GGR`.
  - Sort players by `Lifetime_GGR` in descending order.
  - Calculate the cumulative sum of `Lifetime_GGR`.
  - Calculate the `Cumulative_Player_Pct` (from 0% to 100%) and `Cumulative_GGR_Pct` (from 0% to 100%).
- **Visualization:** Use Plotly Express (`px.area` or `px.line`) to plot `Cumulative_Player_Pct` on the X-axis and `Cumulative_GGR_Pct` on the Y-axis. 
  - Add a dashed horizontal/vertical reference line at the 80/20 mark to easily visualize if 20% of players are generating 80% of the revenue.

---

### 3.8 Campaign Naming Convention (Ops Ingestion)
Campaign names follow: `Brand-Country-Language-Product-Segment-Lifecycle-Sublifecycle-Engagement-Date`.
During ingestion, each component is extracted via token matching:
- **Brand:** First token → lookup in `client_mapping` for `ops_client` and `ops_brand`.
- **Country:** First 2-3 letter alphabetic token not in blocklist → stored as ISO code (e.g., `TR`, `CL`). Default: `Global`.
- **Language:** Smart default from country (`TR→TR`, `BR→PT`, `GB→EN`, `ES/CL/MX/PE/EC/AR/CO→ES`, `JP→JA`, `DE/AT/CH→DE`). Falls back to `UNKNOWN`.
- **Product:** Token in `[SPO, CAS, LIVE, ALL]`. Default: `UNKNOWN`.
- **Segment:** Token in `[HIGH, MID, MED, LOW, VIP, NA, AFF, COH1-COH4]`. Default: `UNKNOWN`.
- **Lifecycle:** Token in `[RND, WB, CS, ROC, FD, OTD, CHU, ACQ, SL, LFC, LOADER]`. Default: `UNKNOWN`.
- **Sublifecycle:** Token in `[J1, J2, J3, BULK]`. Default: `UNKNOWN`.
- **Engagement:** Token in `[NLI, LI]`. Default: `UNKNOWN`.
- **Missing fields:** Default to `"UNKNOWN"`. UNKNOWN values excluded from sidebar dropdowns but included when filter = "All".

---

## 4. FRONTEND APPLICATION (`app.py`)

### 4.1 🧭 4-Tier Navigation Router & Global Filters
- **Sidebar Router:** Role-based radio router (`view_mode`):
  1. `📊 Dashboard`: Executive/CRM intelligence.
  2. `📞 Operations`: Operations Command and Ingestion.
  3. `🏦 Financial`: Financial Deep-Dive and Ingestion dropzones.
  4. `⚙️ Admin`: Client Hub, User Management, Data Maintenance, and File Explorer.
- **🌍 GLOBAL INTELLIGENCE FILTERS (Form-Gated):**
  - All filters wrapped in `st.form("global_filters")` — page only re-renders on "🔍 Apply Filters" click.
  - **Filter Order** (matches campaign naming convention):
    1. 🎯 **Client** — from `ops_client` + `client` columns.
    2. 🏷️ **Brand** — from `ops_brand` + `brand` columns. Display: `CODE — Name`.
    3. 🌍 **Country** — from `country` column, display-mapped to full names.
    4. 🗣️ **Language** — from `extracted_language` column.
    5. 📦 **Product** — from `extracted_product` column, display-mapped (SPO→Sportsbook, CAS→Casino, LIVE→Live).
    6. 🎯 **Segment** — from `extracted_segment` column.
    7. 🔁 **Lifecycle** — from `extracted_lifecycle` column.
    8. 📋 **Sublifecycle** — from `extracted_sublifecycle` column.
    9. 🔥 **Engagement** — from `extracted_engagement` column, display-mapped (LI→Log In, NLI→Not Logged In).
  - All default to "All". Dropdowns dynamically populated from actual DB data. UNKNOWN values hidden.
  - Unified Time Frame Filter (Date Slider): Synchronously slices both Financial and Operations data.

### 4.2 ⚡ Real-Time Auto-Hydration & Ingestion Decoupling
- **Decentralized Ingestion:** Legacy Data Control Room destroyed. Uploaders are now scoped to their specific domains:
  - 🏦 Financial uploads occur inside the `📥 Financial Ingestion` workspace.
  - 📞 Operations uploads occur inside the `🗄️ Operations Ingestion` workspace.
- **Analytical Auto-Hydration:** The dashboard computations (Monthly Summaries, Cohorts, Segmentations, etc.) are instantly generated from the PostgreSQL filtered `_master_df` using `@st.cache_data` wrappers to prevent blocking the main thread.

### 4.2 ⚙️ Admin Tier (4 Modules)
- **Radio selector:** `🏢 Client Hub | 👥 User Management | 🧹 Data Maintenance | 📂 File Explorer`.

#### 4.2.1 🏢 Client Hub
- **Master Health Board:** Dynamic grid showing latest Financial/Operational upload timestamps per client and active SLA counts.
- **Client Detail Profile:** 3-tab dashboard (Completeness, Brand Registry, Contractual SLAs).

#### 4.2.2 👥 User Management
- CRUD interface for the `users` RBAC table.

#### 4.2.3 🧹 Data Maintenance
- **State Metrics:** Local file count, folder size, `ops_telemarketing_data` rows, `ops_telemarketing_snapshots` rows.
- **Purge Local Files:** Deletes `data/raw/callsu_daily/` with expander+checkbox confirmation.
- **Purge Operations DB:** TRUNCATEs both ops tables with expander+checkbox confirmation.
- **📊 Benchmark Snapshots:** Auto-detects completed half-years from `ops_telemarketing_data`. Buttons to ⚡ Generate / 🔄 Regenerate / 🗑️ Delete benchmarks per period. Calls `scripts/jobs/generate_benchmarks.py` which aggregates daily averages by brand/country/lifecycle/segment/engagement into `ops_historical_benchmarks`.

#### 4.2.4 📂 File Explorer
- **Inventory Dashboard:** Metric cards showing file count + total size for `data/` and `docs/` directories.
- **Folder-First Navigation:** Session-state driven drill-down with breadcrumbs (`🏠 → data → raw → callsu_daily`). Subfolders shown as clickable cards (rows of 4) with recursive file count and size. Only current folder's files listed.
- **File Table:** Sortable by name, size, and modification date. Search filter narrows file selector.
- **Online Viewer:** CSV/XLSX render full scrollable dataframe (500px height). `.md` renders as formatted markdown. `.py`, `.json`, `.yaml` etc. render as syntax-highlighted code (50K char limit).
- **Download:** `st.download_button` for `.csv`, `.xlsx`, `.md`, `.txt`, `.json`, `.log`, `.py` files.

### 4.3 Cache Invalidation Strategy
- Rather than persisting RAM-Csvs, all upload handlers save directly to PostgreSQL using `to_sql`. 
- Post-upload, the application explicitly triggers `del st.session_state["raw_fin_df"]` / `raw_ops_df`, forcing `app.py` to seamlessly re-hydrate `pd.read_sql` on the next `st.rerun()`.

### 4.4 Tab 1: 📊 Executive Summary
- **Master Insight:** `> SYSTEM DIAGNOSTIC_` text box generating dynamic AI narrative.
- **Brand vs Brand Trajectory:** Plotly grouped bar chart.
- **Cross-Brand Cannibalization & Pareto Curve:** Sections generating visualization matrices based on combined entity calculations.

### 4.4.1 📡 Operations Pulse Matrices
- **Location:** `📊 Dashboard` tab, below executive headers.
- **Layout:** Two side-by-side matrices via `st.columns(2)`: **LI** (Log In) and **NLI** (Not Logged In).
- **Data Split:** `ops_telemarketing_snapshots` filtered by sidebar globals, split by `extracted_engagement`.
- **Rows (per matrix):** Volume → Login % → Conv % (Volume hidden until data feed is available).
- **Columns:** 7 Days | 14 Days | 30 Days | 90 Days.
- **Cell Content:** `st.metric(avg_value, delta)` + ~60px Plotly sparkline.
  - Delta = current window avg - prior equivalent period avg (e.g., last 7d vs 7d before that).
  - Color: Green ↑ improving, Red ↓ declining, Grey — flat (< 1% change).

### 4.4.2 📊 Dynamic Operational Baseline (H-over-H Comparison)
- **Location:** `📊 Dashboard` tab, below the Operations Pulse Matrices.
- **Data Source:** `ops_telemarketing_snapshots` queried directly from DB. Sidebar filters (client, brand, engagement, lifecycle, segment, country) apply; date range filter is ignored.
- **Baseline Logic:** The prior period is selectable via a **"Compare against:" dropdown** (`st.selectbox`). Available options are auto-detected from snapshot data — only **completed** half-years appear (e.g., H2 2025 only appears after 2025-12-31). Default priority: H2 2025 → H1 2025 → first available. The current period is auto-detected from `datetime.now()` (H1=Jan-Jun, H2=Jul-Dec of current year). The `_render_fixed_benchmark(df, prior_half="H2 2025")` function parses any "HX YYYY" format into date ranges.
- **Metric Groups:**
  - **Volume (raw totals):** Records, Logins, Conversions.
  - **Call Dispositions (% of Records):** D%, NA%, I%.
  - **Email Channel (% of `es`):** ED%, EO%, EC%, EF%.
  - **SMS Channel (% of SS where SS = `sd + sf + sp`):** SD%, SF%, SP%.

#### Layer 1 — KPI Summary Cards (always visible)
- Layout: `st.columns(4)`. Cards: Volume, Call Efficiency, Email Health, SMS Health.

#### Layer 2 — Benchmark Table (always visible)
- Columns: Metric | [Selected Prior] Baseline | Current YTD | Δ (arrow + value).
- Column headers update dynamically to match dropdown selection.

#### Layer 3 — Detailed Charts (expandable)
- Dumbbell charts comparing prior vs current across Volume, Dispositions, Email, and SMS.

### 4.5 Tab 5: 📈 Campaigns & Tab 6: 🕵️ CRM Intelligence
- **Campaigns:** Funnel visualizations and tracking per brand.
- **CRM Intelligence:** Global selectbox, 👑 Crown Jewels vs ⚠️ Bonus Abusers leaderboards, Churn Targeting generator. Note: Smart Campaign Profiling expands to a responsive 2-row grid to accommodate the 6 distinct active VIP heuristics.

### 4.7 📈 Operations Efficiency Trends
- **Layout:** Full-width "Global Volume Trends" chart on top, followed by a 3-column row of dedicated KPI charts below.
- **Chart 1 — Global Volume Trends (Full Width):**
  - Line chart: Daily `Records` count.
  - Dashed overlay: `SLA Minimum` (daily = monthly / 30) when a single brand is active.
  - Dashed overlay: `Average Volume` (mean of Records over the filtered range).
- **Chart 2 — Raw KPI Volume (Column 1/3):**
  - Grouped bar chart: Green bars for `#KPI1-Conv.`, Yellow bars for `#KPI2-Login`.
- **Chart 3 — Login % Trend (Column 2/3):**
  - Yellow line: Daily `Logins%` (`KPI2-Login / Records * 100`).
  - Dashed yellow line: `target_li` benchmark from `granular_benchmarks`.
- **Chart 4 — Conversion % Trend (Column 3/3):**
  - Green line: Daily `Conv%` (`KPI1-Conv. / Records * 100`).
  - Dashed green line: `target_conv` benchmark from `granular_benchmarks`.
- **Theme:** All charts use Matrix dark theme (`paper_bgcolor/plot_bgcolor = transparent`, `font_color = #00FF41`), horizontal legends at bottom.
- **Hover Tooltips:** All charts use dark hoverlabel (`bgcolor: rgba(20,20,20,0.9)`, `font_color: #FFFFFF`) to ensure readability on light-colored traces.
- **Data Integrity Guards:** Conv% and Login% calculations apply `.clip(upper=100)` to cap impossible values. Source data rows where `conversions > records` are zeroed at ingestion/patch level.

### 4.8 📋 Pitch vs. List Scorecard
- **Color-Coded Delivery/Issue Rates:** Deliveries % thresholds (>20% green, 15–19% yellow, 10–14% orange, <10% red). Issues >10% red.
- **Email/SMS Funnel Metrics:** Renamed columns: ED (Email Delivered = `ed / (ed + ef)`), EO (Email Open = `eo / ed`), EC (Email Clicked = `ec / eo`), SD (SMS Delivered = `sd / (sd + sp + sf)`). Each displayed as percentage with color coding: ED/SD >90% green, 85–90% yellow, <85% red. EO/EC >10% green, 5–10% yellow, <5% red.
- **Progress Bars:** Gross % and Net % rendered as `st.column_config.ProgressColumn`. Column order: Campaign, Gross %, Net %, Calls, then details.

### 4.9 💰 Campaign True Cost Ledger
- **"New Data"** replaces legacy "Total Records" label.
- **Conv %** = `Conversions / New Data * 100` (displayed as percentage, not raw count).
- **Contact Rate** = `D / (D + NA + Issues) * 100` (always positive percentage; raw disposition columns hidden from display).

### 4.10 📊 SLA Fulfillment Tracker
- **Compact Card Layout:** Data grouped by brand, displayed as compact `st.metric` cards (4 per row) instead of verbose lists.

### 4.11 🔄 Conversion Funnel (Phase 20 — Planned)
- **Data Source:** `ops_contact_events` table (daily cron pull from iWinBack contact APIs).
- **Visual Funnel:** Per campaign/brand: Calls → Logins → Registrations → FTDs → GGR (linked via `account_number` to financial data).
- **Metrics:** Call-to-Login %, Login-to-Reg %, Reg-to-FTD %, True ROI (GGR ÷ campaign cost).

### 4.12 ⏱️ Time-to-Convert (Phase 20 — Planned)
- **Per Campaign/Brand:** Median hours Call→Login, Login→Register, Register→FTD.
- **Distribution Chart:** Histogram in 24h buckets showing conversion speed patterns.
- **Insight:** Identifies fast-converting vs. slow-burn campaigns needing nurture sequences.

### 4.6 Report Generation Engine
- **Financial Report:** Exported from the Financial Deep-Dive tab.
- **Operations Report:** Exported from the Operations Command tab.
- **Master Report (Combined):** Exported from the Executive Summary tab, appending an Operations Tracker tab to the standard financial workbook.

---

## 5. OPERATIONS API INTEGRATION (iWinBack Native)

### 5.1 Architecture
- **Direct SaaS integration:** 5 iWinBack boxes, each with a unique base URL and Bearer token stored in `.env`.
- **Boxes:** `bhfs2`, `bxq4c`, `bb4p7`, `baj7f`, `bdka4` — each serves a subset of clients/brands (auto-discovered).
- **Module:** `src/iwinback_worker.py` replaces the legacy `src/api_worker.py` middleware at `dashboard.callsu.net`.

### 5.2 Export Flow (per box, per date)
1. `POST /api/exports` — `exportType: "campaign_summary_v3"`, `date_range: "YYYY-MM-DD - YYYY-MM-DD"`.
2. `GET /api/exports/{id}` — Poll until `status == "done"` (max 120 attempts × 5s).
3. `GET /api/exports/{id}/download` — Download Excel file.
4. Parse Excel with `header=1` (row 0 = merged group headers, row 1 = column names).
5. 81 columns across groups: Campaign Quality/ROI, Calls/VoIP, SMS, Email, Costs, Regulatory, Cost Ratios, Status.

### 5.3 SLA Volume Metric
- **Source column:** `"New Data"` (replaces legacy `"# Records"`).
- **DB column:** `ops_telemarketing_data.records` (unchanged schema, new source).
- **Fallback:** `row.get("New Data", 0) or row.get("# Records", 0)` for backward compatibility with legacy files.

### 5.4 Duplicate Prevention
- DB-level guard: `SELECT 1 FROM ops_telemarketing_data WHERE ops_date = :d LIMIT 1` per date before requesting exports.

### 5.5 Credential Storage
```
# .env format
IWINBACK_BOXES=bhfs2,bxq4c,bb4p7,baj7f,bdka4
IWINBACK_{box}_URL=https://{box}.iwinback-saas.com
IWINBACK_{box}_TOKEN=<bearer_token>
```
- Credentials set on **both** Railway services: `d-rock-terminal` (web app) and `cron-job`.

### 5.6 Contact-Level Journey APIs (Phase 20)
- **`GET /api/contact_campaign_association/client_reporting`** — Links `contact_id` to `campaign_id`, `brand_id`, `account_number`.
- **`GET /api/contact_logins`** — Login events with `contact_id`, `login_at`, `type`, `domain`.
- **`GET /api/contact_registers`** — Registration events with `contact_id`, `account_number`.
- **`GET /api/contact_deposits`** — Deposit/FTD events (leading indicator before monthly financials arrive).
- **Backfill:** Historical pull from 2025-01-01 to present, chunked by month.
- **Daily Cron:** Alongside `campaign_summary_v3`, pulls yesterday's events and upserts into `ops_contact_events`.

### 5.7 Historical Data Coverage
- **Operations data (`campaign_summary_v3`):** Backfilled from 2025-01-01 → present across all 5 boxes.
- **Benchmark periods:** H1 2025 (Jan–Jun 2025), H2 2025 (Jul–Dec 2025), H1 2026 (in progress).