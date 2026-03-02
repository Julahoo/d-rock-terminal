# D-ROCK FINANCIAL TERMINAL v1.0 - TECHNICAL SPECIFICATION

## 1. CORE PRINCIPLES & ARCHITECTURE
- **Goal:** Automate the ingestion of monthly betting CSVs to calculate financial metrics, track player lifecycles, and generate business intelligence via a web application and Excel exports.
- **Architecture:** ETL Pipeline + Interactive UI Dashboard (Hub & Spoke Model).
- **Tech Stack:** Python 3.10+, `pandas`, `openpyxl`/`xlsxwriter`, `streamlit`, `plotly`.
- **UI Theme:** "Matrix/Terminal" (Pitch Black `#000000`, Secondary `#0D0D0D`, Text/Accent Neon Green `#00FF41`).

---

## 2. DATA MODELS (Single Source of Truth)

### 2.1 Raw Ingestion Entities
- **`PlayerRecord`:** `id`, `brand` (Rojabet/Latribet), `wb_tag` (Program), `bet`, `win`, `revenue`, `report_month`.
- **`CampaignRecord`:** `brand`, `campaign_type`, `records`, `kpi1_conversions`, `kpi2_logins`, `calls`, `emails_sent`, `sms_sent`, `report_month`.

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

### 4.1 Global UI Rules
- **State Management:** Uploaded data and generated DataFrames must be stored in `st.session_state` to prevent Streamlit from reloading/losing data on widget interaction.
- **Formatting:** Percentages format as `%.2f%%`. Currencies format as `$%,.2f`. Counts format as `%,d`. All plots must use the dark Matrix layout background and neon green font.
- **Sidebar:** Contains file uploaders, an execution button (`st.button` with `use_container_width=True`), and a persistent Excel download button (`st.download_button` type "secondary").

### 4.2 Tab 1: 📊 Executive Summary
- **Master Insight:** `> SYSTEM DIAGNOSTIC_` text box generating dynamic AI narrative (GGR trajectory, Margin alerts, Whale Risk alerts).
- **Cross-Brand Matrices:** Side-by-side comparison tables (`Both Business` | `Rojabet` | `Latribet`) for:
  1. *Performance Matrix:* Financial KPIs + Whale Risk.
  2. *Demographics Matrix:* Lifecycle counts (Total, Conversions, Retained, etc.).
  3. *VIP Health Matrix:* RFM counts (True VIPs vs Churn Risk).
- Each matrix is preceded by an italicized business insight. Includes a Brand vs. Brand Trajectory Bar Chart.
- Add a new section `> CROSS-BRAND CANNIBALIZATION (ALL-TIME)_` below the existing matrices.
- Display a dual metric: The total number of overlapping players, and the total lifetime GGR generated by those specific players.
- Add a new section `> REVENUE CONCENTRATION (PARETO CURVE)_` below the Cross-Brand Cannibalization section.
- Display the Plotly chart showing the combined entity's revenue concentration.

### 4.3 Tabs 2, 3, & 4: Deep-Dives (Combined, Rojabet, Latribet)
*These tabs share strict component universality. Everything in Combined must appear in the individual brand tabs.*
- **Top:** 5 KPI metric cards (Turnover, GGR, Margin, Revenue (15%), Total Players).
- **Comparative Intelligence:** - Time-Series `[ FINANCIALS ]` table containing EOY projections. (Includes a `> 🔮 EOY PROJECTIONS` caption).
  - Time-Series `[ PLAYER DEMOGRAPHICS ]` table. Order: Total Active, Conversions, New Players, Reactivated, Retained, Profitable, Neg. Yield.
- **Risk & Value Metrics:**
  - Whale Risk Gauge (Top 10% share > 70% shows warning).
  - Turnover Per Player.
  - Value Composition stacked bar chart (`New_Player_GGR` vs `Returning_Player_GGR`).
- **Visual Cohort Retention Heatmap:** Plotly triangle heatmap showing lifecycle retention percentages over time.
- Add a new subheader `> CUMULATIVE LTV TRAJECTORY_` right above or below the Cohort Retention Heatmap.
- Display the Plotly line chart using `st.plotly_chart(fig, use_container_width=True)`.
- Update the `> SEGMENTATION BY PROGRAM_` section.
- Display the detailed DataFrame containing `Program`, `Total Players`, `Turnover`, `GGR`, and `Margin`.
- Ensure proper Streamlit column formatting: `Margin` as `%.2f%%`, `Turnover` and `GGR` as `$%,.2f`.

### 4.4 Tab 5: 📈 Campaigns
- Displays total aggregated campaign metrics and a `plotly.graph_objects.Funnel` chart mapping Records -> Logins -> Conversions. Applies LI duplication scrubbing rules.

### 4.5 Tab 6: 🕵️ CRM Intelligence
- **Global Filter:** Brand selectbox (anchored with a widget `key`).
- **Leaderboards:** Top 50 "👑 Crown Jewels" (Max GGR) and Top 50 "⚠️ Bonus Abusers" (Max Turnover + Negative Yield).
- **Churn Targeting Generator:** Interactive sliders for Inactivity and Lifetime GGR, displaying a filtered target dataframe with a CSV export button.
- **Smart Campaign Profiling:** 5-column metric display showing counts for the automated campaign heuristics, followed by a dropdown extractor to download specific CSV target lists.
- Update the `> SMART CAMPAIGN PROFILING_` metric display. Because we now have 6 distinct campaigns, change the layout from a single row of 5 columns to a 2-row grid using `st.columns(3)` to keep the UI clean and prevent the text from wrapping awkwardly.