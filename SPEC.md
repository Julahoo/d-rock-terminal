# D-ROCK FINANCIAL TERMINAL - TECHNICAL SPECIFICATION

## 1. CORE PRINCIPLES & ARCHITECTURE
- **Goal:** Automate the ingestion of monthly betting CSVs to calculate financial metrics, track player lifecycles, and generate business intelligence via a web application and Excel exports.
- **Architecture:** 4-Tier Modular Enterprise Platform (V2.0). Utilizes an ETL Pipeline saving state to a persistent PostgreSQL database (`src/database.py`).
- **Tech Stack:** Python 3.10+, `pandas`, `SQLAlchemy`, `psycopg2`, `openpyxl`/`xlsxwriter`, `streamlit`, `plotly`.
- **UI Theme:** "Matrix/Terminal" (Pitch Black `#000000`, Secondary `#0D0D0D`, Text/Accent Neon Green `#00FF41`).

---

## 2. DATA MODELS (PostgreSQL Persistence Layer)

### 2.1 Database Tables (Single Source of Truth)
- **`ops_telemarketing_data`:** Core telemarketing operational records. Columns include `ops_hash`, `ops_client`, `ops_brand`, `ops_date`, total dials, connections, promises, etc.
- **`financial_data`:** Core financial records populated from ingestion. Columns match the Smart Adapters (e.g., `brand`, `month`, `ogr`, `ngr`, `deposits`).
- **`client_mapping`:** The Universal Brand Translator registry. Columns: `brand_code` (Ops Tag), `brand_name`, `client_name`, `financial_format` (Enum: Standard, LeoVegas, Offside). Used to normalize incoming data and intercept/quarantine orphaned tags.
- **`contractual_slas`:** Stores client-specific SLA thresholds for Operations command tracking. Columns: `client_name`, `brand_code`, `lifecycle`, `monthly_minimum_records`, `target_cac_usd`, `benchmark_conv_pct`.

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

## 4. FRONTEND APPLICATION (`app.py`)

### 4.1 🧭 4-Tier Navigation Router & Global Filters
- **Sidebar Router:** Replaced flat-tab structure with a role-based radio router (`view_mode`):
  1. `📊 Dashboard`: Executive/CRM intelligence.
  2. `📞 Operations`: Operations Command and Ingestion.
  3. `🏦 Financial`: Financial Deep-Dive and Ingestion dropzones.
  4. `⚙️ Admin`: Client Hub and settings.
- **🌍 GLOBAL INTELLIGENCE FILTERS:**
  - Placed in the sidebar globally. DB-Hydrated logic explicitly strips hidden whitespace to prevent filtering bugs.
  - Cascading logic: **Client** selection perfectly limits the **Brand** dropdown to registered components via SQL `WHERE` clauses on `client_mapping`.
  - UX Trick: Auto-selects the brand if the client only has 1 registered.
  - Unified Time Frame Filter (Date Slider): Synchronously slices both Financial (report_month) and Operations (ops_date) data across all tabs.

### 4.2 ⚡ Real-Time Auto-Hydration & Ingestion Decoupling
- **Decentralized Ingestion:** Legacy Data Control Room destroyed. Uploaders are now scoped to their specific domains:
  - 🏦 Financial uploads occur inside the `📥 Financial Ingestion` workspace.
  - 📞 Operations uploads occur inside the `🗄️ Operations Ingestion` workspace.
- **Analytical Auto-Hydration:** The dashboard computations (Monthly Summaries, Cohorts, Segmentations, etc.) are instantly generated from the PostgreSQL filtered `_master_df` using `@st.cache_data` wrappers to prevent blocking the main thread.

### 4.2 ⚙️ Admin Tier: Client Hub
- **Master Health Board:** Displays a dynamic grid showing the absolute timestamps of the latest Financial and Operational files uploaded per client, alongside their active SLA counts.
- **Client Detail Profile:** Access via "⚙️ Manage Profile". Traps the `st.session_state` to a specific client to view a 3-tab dashboard:
  - **Tab 1: Completeness & Uploads:** Dynamic inverted matrix (Months as Rows, Brands as Columns) tracking data synchronization. Includes an isolated financial up-loader directly injecting state to DB.
  - **Tab 2: Brand Registry:** Manages the `client_mapping` table. Adding a tag dynamically traces and retroactively fixes Orphaned records in `ops_telemarketing_data`.
  - **Tab 3: Contractual SLAs:** Manages the `contractual_slas` config table.

### 4.3 Cache Invalidation Strategy
- Rather than persisting RAM-Csvs, all upload handlers save directly to PostgreSQL using `to_sql`. 
- Post-upload, the application explicitly triggers `del st.session_state["raw_fin_df"]` / `raw_ops_df`, forcing `app.py` to seamlessly re-hydrate `pd.read_sql` on the next `st.rerun()`.

### 4.4 Tab 1: 📊 Executive Summary
- **Master Insight:** `> SYSTEM DIAGNOSTIC_` text box generating dynamic AI narrative.
- **Brand vs Brand Trajectory:** Plotly grouped bar chart.
- **Cross-Brand Cannibalization & Pareto Curve:** Sections generating visualization matrices based on combined entity calculations.

### 4.5 Tab 5: 📈 Campaigns & Tab 6: 🕵️ CRM Intelligence
- **Campaigns:** Funnel visualizations and tracking per brand.
- **CRM Intelligence:** Global selectbox, 👑 Crown Jewels vs ⚠️ Bonus Abusers leaderboards, Churn Targeting generator. Note: Smart Campaign Profiling expands to a responsive 2-row grid to accommodate the 6 distinct active VIP heuristics.

### 4.6 Report Generation Engine
- **Financial Report:** Exported from the Financial Deep-Dive tab.
- **Operations Report:** Exported from the Operations Command tab.
- **Master Report (Combined):** Exported from the Executive Summary tab, appending an Operations Tracker tab to the standard financial workbook.