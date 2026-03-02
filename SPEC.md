# BETTING FINANCIAL REPORTS AUTOMATION - TECHNICAL SPECIFICATION

## 1. CORE PRINCIPLES & OBJECTIVES
- **Goal:** Automate the ingestion of monthly betting data (CSV files) to calculate financial/player metrics and generate aggregate summary reports (Excel) separated by brand.
- **Architecture:** ETL (Extract, Transform, Load) Pipeline.
- **Tech Stack:**
  - Language: Python 3.10+
  - Libraries: `pandas` (for data manipulation), `openpyxl` / `xlsxwriter` (for Excel generation), `glob` / `os` (for file management).
- **Constraints:**
  - The script must be able to ingest a new month's CSV without breaking historical data.
  - The tool should handle missing files or empty fields gracefully.
  - Output must match the exact formatting needed for the final "Summary Data" workbook.

## 2. DATA MODELS (Single Source of Truth)

### Entity: `PlayerRecord` (Raw Ingestion)
| Field | Type | Description |
|---|---|---|
| `id` | String/Int | Player unique identifier. |
| `brand` | String | e.g., "Rojabet", "Latribet". |
| `wb_tag` | String | WB tag/flag (segment identifier). |
| `bet` | Float | Amount of money used. |
| `win` | Float | Amount of money won (typically negative in raw data). |
| `revenue` | Float | Difference between bet and win. |
| `report_month` | Date/String | Extracted from the CSV filename (e.g., "August 2025"). |

### Entity: `MonthlyBrandSummary` (Aggregated Output)
| Field | Type | Description |
|---|---|---|
| `month` | String | The reporting month. |
| `brand` | String | The brand being summarized. |
| `losers` | Integer | Count of players where revenue < 0. |
| `winners` | Integer | Count of players where revenue > 0. |
| `flat` | Integer | Count of players where revenue == 0. |
| `total_players` | Integer | Total unique players for the month. |
| `winners_pct` | Float | (winners / total_players) * 100. |
| `ggr` | Float | Gross Gaming Revenue (Sum of all player revenue). |
| `ggr_per_player` | Float | ggr / total_players. |
| `new_players` | Integer | Players who have never appeared in any previous month. |
| `returning_players`| Integer | Players who appeared in previous months. |
| `retention_pct` | Float | (returning_players / total_players) * 100. |

### Entity: `IngestionRegistry` (Tracking File Status)
| Field | Type | Description |
|---|---|---|
| `brand` | String | e.g., "Rojabet", "Latribet". |
| `report_month` | String | The month being tracked (e.g., "2025-08"). |
| `status` | Enum | "COMPLETE", "MISSING". |
| `file_path` | String | Path to the ingested CSV, or null if missing. |
| `ingested_at` | DateTime | Timestamp of when the file was processed. |

### Entity: `CampaignRecord` (Raw Ingestion)
| Field | Type | Description |
|---|---|---|
| `brand` | String | "Rojabet" or "Latribet". |
| `campaign_type` | String | The type/name of the campaign (e.g., "LI"). |
| `records` | Integer | Number of records received. |
| `kpi1_conversions` | Integer | KPI 1 - Conversions. |
| `kpi2_logins` | Integer | KPI 2 - Logins. |
| `calls` | Integer | Calls made. |
| `emails_sent` | Integer | Emails sent (ES). |
| `sms_sent` | Integer | SMS sent (SS). |
| `report_month` | String | Extracted from filename (e.g., "2025-08"). |

### Entity: `CampaignSummary` (Aggregated Output)
| Field | Type | Description |
|---|---|---|
| `month` | String | The reporting month. |
| `brand` | String | The brand being summarized. |
| `total_records` | Integer | Sum of records (after LI scrub). |
| `total_kpi1` | Integer | Sum of conversions. |
| `total_kpi2` | Integer | Sum of logins (after LI scrub). |
| `total_calls` | Integer | Sum of calls. |
| `total_emails` | Integer | Sum of emails. |
| `total_sms` | Integer | Sum of SMS. |

### Entity: `CohortMatrix` (Aggregated Output)
| Field | Type | Description |
|---|---|---|
| `Acquisition Month` | String | The month the cohort of players first joined. |
| `Month 1` | Float | % of the cohort that returned in their 1st month after joining. |
| `Month 2` | Float | % of the cohort that returned in their 2nd month. |
| `Month N` | Float | % of the cohort that returned in their Nth month. |

### Entity: `BothBusinessSummary` (Combined Client Output)
| Field | Type | Description |
|---|---|---|
| `Month` | String | The reporting month. |
| `Turnover` | Float | Sum of `bet` across all brands. |
| `GGR` | Float | Sum of `revenue` across all brands. |
| `Margin` | Float | `GGR` / `Turnover`. |
| `Revenue_Share_Deduction` | Float | `GGR` * 0.15 (Assuming a standard 15% Rev Share). |
| `Net_Income` | Float | `GGR` - `Revenue_Share_Deduction`. |
| `New_Players` | Integer | Sum of new players across both brands. |
| `Returning_Players`| Integer | Sum of returning players across both brands. |
| `Total_Players` | Integer | `New_Players` + `Returning_Players`. |
| `New_Players_Pct` | Float | `New_Players` / `Total_Players`. |
| `Returning_Players_Pct`| Float | `Returning_Players` / `Total_Players`. |
| `GGR_Per_Player` | Float | `GGR` / `Total_Players`. |
| `Income_Per_Player` | Float | `Net_Income` / `Total_Players`. |
| `Profitable_Players` | Integer | Sum of profitable_players across both brands. |
| `Negative_Yield_Players` | Integer | Sum of negative_yield_players across both brands. |

### Entity: `TimeSeriesMetrics` (Calculated for Both Business & Individual Brands)
- **Target Metrics to Process:** `Turnover`, `GGR`, `Margin`, `Revenue_Share_Deduction`, `Total_Players`, `Profitable_Players`, `Negative_Yield_Players`.

## 3. COMPONENT ARCHITECTURE

### A. Data Ingestion Module (`ingestion.py`)
- **Directory Structure:** Expects raw CSV files organized by brand (e.g., `data/raw/Rojabet/`, `data/raw/Latribet/`).
- **File Reader:** Iterates through the directories and reads all CSVs into a unified Pandas DataFrame.
- **Registry Manager (`registry.json` or `registry.csv`):** - Cross-references expected months against found files. 
  - If a specific month is found for Brand A but not for Brand B, it flags Brand B's month as "MISSING".
  - **Notification Trigger:** If a file is marked "MISSING" for a newly detected month, the script must output a standard console warning (e.g., `WARNING: Missing January 2026 data for Rojabet`) and optionally halt execution depending on a strict-mode flag.

### B. Aggregation Engine (`analytics.py`)
- **Financial Calculator:** Groups data by Brand and Month to calculate sum totals (GGR, Bet, Win).
- **Cohort Analyzer:** Tracks historical `id`s to correctly categorize `new_players` vs `returning_players` and calculate retention.

### C. Report Generator (`exporter.py`)
- **Excel Writer:** Outputs the final aggregations into tabs separated by brand (e.g., "Latribet Financial", "Rojabet Financial") matching the legacy "Summary data.xlsx" format.

## 4. DOMAIN RULES (Business Logic)
- **Revenue Logic:** Player Revenue is technically defined as the final revenue yield. 
- **Player Status Definition:** - Winner = Revenue > 0
  - Loser = Revenue < 0
  - Flat = Revenue == 0
- **Cohort Tracking:** A master list of historical `id`s must be maintained in memory (or a lightweight local DB) during processing to accurately determine if a player is "New" or "Returning".
- **Campaign Data Override Rules (For Future Extension):** As per `Instructions For Summary Campai.csv`, rows with "LI" Campaigns must have their "Records" and "KPI 2- Login" data scrubbed to prevent duplication. (Deferred to Phase 4).
- **Ingestion Validation:** The system must evaluate the global minimum and maximum months across ALL files. For every month in that range, every brand must have a corresponding file. If not, the registry marks it as missing and alerts the user.
- **Campaign Duplication Scrub:** Before aggregating campaign data, if `campaign_type` (or campaign name) contains or equals "LI", the `records` and `kpi2_logins` values for that specific row MUST be set to 0 to prevent double-counting.
- **Campaign Output:** The aggregated campaign data should be exported as a new, single tab named "Summary Campaigns" in the final Excel workbook, containing data for both brands side-by-side or stacked.
- **Cohort Matrix Logic:** To calculate the retention matrix, first identify the "Acquisition Month" (the earliest `report_month` each `id` appears in). For every subsequent month the `id` plays, calculate the relative month index (Month 1, Month 2). Group by Brand and Acquisition Month to calculate the retention percentage (active players in Month N / total new players in Acquisition Month).
- **Both Business Aggregation:** When calculating the combined client summary, base additive metrics (Turnover, GGR, New Players, Returning Players) must be summed together from the individual brand data *first*.
- **Both Business Ratio Recalculation:** Averages and percentages (Margin, New/Returning Pct, Per-Player metrics) MUST be recalculated from the combined additive metrics, not averaged from the brand-level percentages.

## 5. DEVELOPMENT PHASES
- **Phase 1: Setup & Scaffolding** (Project init, folder structure, mock data ingestion).
- **Phase 2: Data Ingestion & Transformation** (Parsing the raw CSVs into Pandas DataFrames).
- **Phase 3: Core Analytics Logic** (Calculating Winners/Losers/Flat, GGR, and Cohort tracking for Retention).
- **Phase 4: Output Generation** (Exporting to multi-tab Excel format).
- **Phase 5: Campaign Extension** (Integrate Campaign Summary KPI report rules).

## 6. PHASE 6: STREAMLIT WEB APP (FRONTEND)
- **Framework:** Streamlit (`streamlit` library).
- **Goal:** Provide a web interface for non-technical users to upload CSVs, run the pipeline, and download the resulting Excel report.
- **UI Layout:**
  - **Sidebar:** - File uploaders grouped by Brand (Latribet/Rojabet) and Data Type (Financial/Campaign).
    - Logic to save uploaded files directly into the existing `data/raw/` and `data/campaigns/` folder structures.
  - **Main Window:**
    - A "Run Analytics Pipeline" button.
    - Upon successful run, display a preview of the `MonthlyBrandSummary` dataframe on the screen.
    - A "Download Excel Report" button that serves the generated `Summary_Data_Auto.xlsx` file to the user.

## PHASE 8: TOP-TIER BUSINESS INTELLIGENCE
Update the analytics engine to include the following metrics based on existing data:

1. **Financial Upgrades (`MonthlyBrandSummary`):**
   - Rename `winners` to `profitable_players` and `losers` to `negative_yield_players`.
   - Add `total_handle` (sum of `bet`).
   - Add `hold_pct` (`ggr` / `total_handle`).

2. **Whale Analysis (`MonthlyBrandSummary`):**
   - Add `top_10_pct_ggr_share`: For each brand/month, sort players by revenue descending. Take the top 10% of profitable players. Calculate their sum of revenue divided by total GGR.

3. **Segmentation Matrix (New Output):**
   - Create a new summary dataframe grouping by `brand`, `report_month`, and `wb_tag` to show `ggr` and `total_players` per segment.

4. **Campaign Efficiency (`CampaignSummary`):**
   - Add `kpi1_conversion_rate` (`total_kpi1` / `total_records`).
   - Add `kpi2_login_rate` (`total_kpi2` / `total_records`).

## PHASE 9: INTERACTIVE BI DASHBOARD & CLIENT AGGREGATION

### 1. Client Aggregation Rule:
- The analytics engine must generate a "Combined" brand summary.
- **Additive metrics** (Handle, GGR, Players, Winners, Losers, Campaign Records) must be summed across both brands per month.
- **Ratio metrics** (Hold %, Winners %, Retention %, KPI Conversion Rates) MUST NOT BE AVERAGED. They must be recalculated using the newly summed additive metrics.
- The `generate_cohort_matrix` must also calculate a "Combined" retention matrix by summing the active players from both brands per cohort offset.

### 2. Dashboard UI Rules (`app.py`):
- Use `st.tabs` to separate views for Combined, Latribet, Rojabet, and Campaigns.
- Use `st.metric` to highlight top-line numbers for the most recent month.
- Use `st.dataframe` to display the full historical tables with native Streamlit column formatting (e.g., `st.column_config.NumberColumn` for currencies and percentages).

## PHASE 10: THE MATRIX THEME (UI)
- **Theme Engine:** Streamlit `config.toml`.
- **Colors:**
  - Background: Pitch Black (`#000000`).
  - Secondary Background (Sidebar/Cards): Dark Charcoal (`#0D0D0D`).
  - Text & Primary Color: Matrix Neon Green (`#00FF41`).
- **Typography:** `monospace` font to simulate a hacker terminal.

## PHASE 11: TIME-SERIES INTELLIGENCE & TERMINAL REBRAND
- **App Name:** D-ROCK FINANCIAL TERMINAL v1.0

### Entity: `TimeSeriesMetrics` (Calculated for Both Business & Individual Brands)
Must calculate the absolute value and percentage change for the following base metrics: `GGR`, `Turnover`, `Total_Players`, `Profitable_Players` (Winners), `Negative_Yield_Players` (Losers).

| Timeframe | Calculation Rule |
|---|---|
| **MoM (Month over Month)** | Current Month vs. Previous Month (shift 1). |
| **QoQ (Quarter over Quarter)** | Aggregate months into Quarters (Q1, Q2...). Compare Current Quarter vs Previous Quarter. |
| **YoY (Year over Year)** | Current Month vs. Same Month Last Year (shift 12). |
| **YTD (Year to Date)** | Cumulative sum of the metric from January 1st of the current year up to the current month. |

## PHASE 12: C-SUITE INSIGHTS & LIFECYCLE ROI
### Engine Updates (`analytics.py`):
1. **Turnover Per Player:** Calculate `Turnover / Total_Players` for both brands and the Combined entity.
2. **GGR by Player Status:** When processing New vs. Returning cohorts, sum the `revenue` for the New players and Returning players separately to create `New_Player_GGR` and `Returning_Player_GGR`.
3. **Lifecycle Program Summary:** Create `generate_program_summary(df: pd.DataFrame)` that groups data by `brand`, `report_month`, and `wb_tag` (Program: WB, ACQ, RET) to calculate GGR and Total Players per program.
4. **Time-Series Engine:** Add `New_Players` and `Returning_Players` to the metrics processed for MoM, QoQ, YoY, and YTD calculations.

## PHASE 13: DATA VISUALIZATION MASTERY
Read SPEC.md. We are upgrading the "at a glance" visuals using Plotly.

1. **Install & Import:** Ensure `plotly.express` and `plotly.graph_objects` are used in `app.py`. Apply a standard dark/neon-green layout to all Plotly figures to match the Matrix theme (`paper_bgcolor='rgba(0,0,0,0)'`, `plot_bgcolor='rgba(0,0,0,0)'`, `font_color='#00FF41'`).
2. **Margin Trajectory:** In the Both Business tab, add a `px.line` chart for `Margin` over `Month` right next to the GGR Bar Chart using `st.columns(2)`.
3. **Yield Donut:** Below the KPI cards, add a `px.pie(hole=0.5)` representing the current month's split of Winners, Losers, and Flat players. 
4. **Retention Heatmap:** Instead of just displaying the Cohort Matrix dataframe, use `px.imshow()` to render it as a heatmap. Use a continuous color scale that goes from Black (0%) to Neon Green (#00FF41) (100%).
5. **Campaign Funnel:** In the Campaigns tab, use `plotly.graph_objects.Funnel` to visualize the flow from `total_records` -> `total_kpi2` -> `total_kpi1` for the most recent month.

### UI Rules (`app.py`):
- Update the main header to "D-ROCK FINANCIAL TERMINAL v1.0".
- Below the top-line KPI cards, create a new "COMPARATIVE INTELLIGENCE" section.
- Split this section into two distinct tables/views: **Financials** (GGR, Turnover) and **Player Demographics** (Total Players, Winners, Losers).
- Display the MoM, QoQ, YoY, and YTD metrics clearly with glowing Up/Down arrows (↑/↓).
- **Number Formatting:** All dataframes displayed in the Streamlit app must use `st.column_config` to format columns appropriately:
  - Percentage columns (Margin, Winners %, Retention %, New Players %, etc.) must display as `%.2f%%` (e.g., 79.44%).
  - Currency columns (Turnover, GGR, Net Income, Rev Share, Per Player metrics) must display as `$%,.2f` (e.g., $1,234.56).
  - Integer columns (Players, Winners, Losers, Records) must display as `%,d` (e.g., 1,234).
- Rename the `Revenue_Share_Deduction` display label to `Revenue (15%)` in the UI.
- **KPI Cards:** Display 5 top-line metrics using `st.columns(5)`: Turnover, GGR, Margin, **Revenue (15%)**, and Total Players.
- **Comparative Intelligence:** Add `Revenue (15%)` to the `[ FINANCIALS ]` time-series table so the user can see their exact agency commission growth MoM, QoQ, YoY, and YTD.
- **Player Demographics Update:** Update the Time-Series `[ PLAYER DEMOGRAPHICS ]` table to display exactly this order: Total Active, New Players, Returning Players, Profitable (Winners), Neg. Yield (Losers).
- **New Section:** Add `> RISK & VALUE METRICS_` below the demographics.
- **Whale Gauge:** Display the `top_10_pct_ggr_share`. If it > 70%, display a warning icon.
- **Value Composition Chart:** Add a stacked bar chart showing `New_Player_GGR` vs. `Returning_Player_GGR`.
- **Program Performance:** Display a bar chart of the `wb_tag` summary, showing revenue generated by ACQ vs RET vs WB programs.

## PHASE 14: CONVERSION TRACKING
### Entity Updates (`MonthlyBrandSummary` & `BothBusinessSummary`):
- Add `Reactivated_Players` (Integer): Players who have played in the past, did NOT play last month, but played this month.
- Add `Conversions` (Integer): `New_Players` + `Reactivated_Players`.

### Domain Rules (Business Logic):
- **Conversion Definition:** A player is a "Conversion" if they appear in the current month's data but did NOT appear in the immediately preceding month's data. 
- **Stateful Tracking:** The `generate_monthly_summaries` function must now track `last_month_ids` in addition to the global `seen_ids`. 
  - `New_Players` = `current_ids - seen_ids`
  - `Reactivated_Players` = `(current_ids & seen_ids) - last_month_ids`
  - `Conversions` = `New_Players + Reactivated_Players`

### UI Rules (`app.py`):
- Update the `[ PLAYER DEMOGRAPHICS ]` time-series table to include these metrics in this order: Total Active, **Conversions**, New Players, Reactivated Players, Returning (Retained) Players, Profitable (Winners), Neg. Yield (Losers).

### UI Rules (`app.py`):
- **Comparative Intelligence Universality:** The `> COMPARATIVE INTELLIGENCE_` section (including both the Financials and Player Demographics tables with MoM, QoQ, YoY, and YTD columns) must be displayed on **every** financial tab (Both Business, Rojabet, Latribet), properly filtered for that specific brand.
- The Player Demographics order must remain strictly: Total Active, Conversions, New Players, Reactivated Players, Retained Players, Profitable (Winners), Neg. Yield (Losers).

## PHASE 15: PREDICTIVE & DIAGNOSTIC ANALYTICS (C-SUITE HORIZON)

### 1. Predictive Forecasting (Run Rate)
- **Mathematical Rule:** To calculate the End of Year (EOY) Projection (Run Rate) for GGR and Turnover: `(YTD_Total / Current_Month_Index) * 12`. *(e.g., If August is month 8, YTD / 8 * 12).*
- Add `EOY_Projected_GGR` and `EOY_Projected_Turnover` to the Time Series metrics.

### 2. RFM Player Tiering (Recency, Frequency, Monetary)
- **Entity Update (`RFMSummary`):** Calculate the number of players and their total GGR contribution grouped by the following tiers:
  - **👑 True VIPs:** Played in the current month (Recency), played 3+ months historically (Frequency), and have a lifetime GGR > $500 (Monetary).
  - **⚠️ Churn Risk VIPs:** Did NOT play in the current month, played 3+ months historically, lifetime GGR > $500.
  - **🐟 Casuals:** Everyone else.

### 3. Smart Narratives (System Diagnostic)
- **Logic:** The system must generate a 2-3 sentence summary based on the Combined Time-Series data for the selected month.
  - *Sentence 1 (GGR Trajectory):* State the Combined GGR and if it is up/down MoM.
  - *Sentence 2 (Margin Health):* State the Margin and explicitly warn if it drops below 2.5%.
  - *Sentence 3 (Risk):* Call out the Whale Dependency (Top 10% share) if it exceeds 70%.

### UI Rules (`app.py`):
1. **System Diagnostic:** Place this text box at the very top of the Main Console, immediately below the month selector, formatted as `st.info()` or `st.warning()`.
2. **Predictive Run Rate:** Add the EOY Projected GGR / Turnover to the `[ FINANCIALS ]` table in the Comparative Intelligence section.
3. **RFM Matrix:** Add a new table or metric columns under `> RISK & VALUE METRICS_` showing the count and GGR of True VIPs, Churn Risk VIPs, and Casuals.

### Entity Updates (`MonthlyBrandSummary`):
- Add `Revenue_Share_Deduction` (Float): `ggr` * 0.15. (This must now be calculated at the individual brand level, not just Both Business).

### UI Rules (`app.py`):
- **Player Demographics Order:** Across all tabs, the Player Demographics table MUST display rows in exactly this order: `Total Active`, `Profitable (Winners)`, `Neg. Yield (Losers)`, `Conversions`, `New Players`, `Returning Players`.
- **Financials Universality:** The `Revenue (15%)` column must be present in the `[ FINANCIALS ]` time-series table for Rojabet and Latribet, identical to the Both Business tab.

### UI Rules (`app.py`):
- **Diagnostic Formatting:** The `generate_smart_narrative` output MUST be formatted with double line breaks (`\n\n`) between each sentence so it renders as distinct, readable lines in the Streamlit UI.
- **Insights Universality:** The `> SYSTEM DIAGNOSTIC_` text box and the entire `> RISK & VALUE METRICS_` section (including RFM Tiering, Value Composition, and Segmentation by Program) MUST be rendered on **every** financial tab (Both Business, Rojabet, Latribet), properly filtered for that specific brand.
- **Terminology:** Ensure the segmentation UI header is strictly `> SEGMENTATION BY PROGRAM_` across all tabs, and the `wb_tag` column is properly renamed to `Program`.

## PHASE 16: EXECUTIVE COMMAND CENTER (HUB & SPOKE)

### UI Rules (`app.py`):
1. **Tab Structure:** Update the main tabs to: `["📊 Executive Summary", "🏦 Combined Deep-Dive", "🔴 Rojabet", "🟢 Latribet", "📈 Campaigns"]`.
2. **Executive Summary Layout:**
   - Display the Combined `> SYSTEM DIAGNOSTIC_` smart narrative at the top.
   - Create a **Cross-Brand Comparison Matrix** for the currently selected month.
   - Rows should be the critical KPIs: `Turnover`, `GGR`, `Margin`, `Revenue (15%)`, `Conversions`, `Turnover Per Player`, and `Whale Risk (Top 10%)`.
   - Columns should be the entities: `Metric`, `Combined`, `Rojabet`, `Latribet`.
   - Display a side-by-side visual (e.g., grouped bar chart) comparing Rojabet vs. Latribet GGR and Conversions over the last 3-6 months.
3. **Drill-Downs:** The remaining tabs act as the detailed drill-downs, retaining all the Phase 15 Comparative Intelligence, RFM, and Program Segmentation metrics.

## PHASE 17: CRM INTELLIGENCE ENGINE

### 17.1 Master Player List & Leaderboards
**Entity: `PlayerMasterList`**
- Group the raw input dataframe by `id` and `brand`.
- Calculate:
  - `Lifetime_GGR`: Sum of `revenue`
  - `Lifetime_Turnover`: Sum of `bet`
  - `First_Month`: Minimum `report_month`
  - `Last_Month`: Maximum `report_month`
  - `Months_Active`: Count of unique `report_month`

**UI Rules (`app.py`):**
- Add a new tab at the end of the list: `["... existing tabs ...", "🕵️ CRM Intelligence"]`.
- Inside this tab, add a Brand selector filter (Combined, Rojabet, Latribet).
- Create a layout section `> VIP & RISK LEADERBOARDS_` using `st.columns(2)`.
- **Column 1 (👑 The Crown Jewels):** Filter the Master List to the Top 50 players by `Lifetime_GGR` descending. Display `id`, `Lifetime_GGR`, `Lifetime_Turnover`, and `Last_Month`.
- **Column 2 (⚠️ Bonus Abusers):** Filter to players with `Lifetime_GGR` < 0 (Negative Yield/Winners). Sort by `Lifetime_Turnover` descending. Show the top 50 to identify players beating the house on high volume.

## PHASE 17: CRM INTELLIGENCE & WIN-BACK GENERATOR

### 1. Master Player List (`PlayerMasterList`)
- Group the raw data by `id` and `brand`.
- Calculate:
  - `Lifetime_GGR`: Sum of `revenue`.
  - `Lifetime_Turnover`: Sum of `bet`.
  - `First_Month`: Minimum `report_month`.
  - `Last_Month`: Maximum `report_month`.
  - `Months_Active`: Count of unique `report_month`.
- Calculate `Months_Inactive`: The difference in months between the global maximum `report_month` in the dataset and the player's `Last_Month`.

### 2. UI Rules (`app.py` - CRM Intelligence Tab)
- Create a new tab: `"🕵️ CRM Intelligence"`.
- **Global Filter:** A Brand selectbox at the top ("Both Business", "Rojabet", "Latribet") to filter the `PlayerMasterList`.
- **VIP & Risk Leaderboards:** Side-by-side tables showing Top 50 "Crown Jewels" (highest Lifetime GGR) and Top 50 "Bonus Abusers" (Lifetime GGR < 0, sorted by highest Turnover).
- **Win-Back Generator:** An interactive section with:
  - Sliders for `Minimum Months Inactive` and `Minimum Lifetime GGR`.
  - A dynamic dataframe showing the filtered target list.
  - A native `st.download_button` to export the target list as a CSV.

### UI Rules (`app.py`):
- **Projection Explanation:** Directly below the `[ FINANCIALS ]` Time-Series table, add a subtle text block (e.g., `st.caption` or `st.markdown`) explaining the EOY Run-Rate math to the user: *"🔮 EOY PROJECTIONS: Calculated as a linear run-rate based on current year-to-date performance ((YTD / Current Month) * 12)."*

### 1. Predictive Forecasting (Run Rate)
- **Mathematical Rule:** Calculate run-rates for Turnover, GGR, AND Revenue_Share_Deduction using: `(YTD_Total / Current_Month_Index) * 12`.
- Add `EOY_Projected_GGR`, `EOY_Projected_Turnover`, and `EOY_Projected_Revenue` to the Time Series output.

### UI Rules (`app.py`):
- **STRICT UNIVERSALITY:** The `[ FINANCIALS ]` table (including all 3 EOY Projections and the explanatory caption), the `> SYSTEM DIAGNOSTIC_` smart narrative, and the `> RISK & VALUE METRICS_` section MUST be rendered in the Both Business tab AND the individual Rojabet and Latribet tabs.

### REMOVALS (Phase 17.3 Cleanup):
- Remove the `Segmentation by Program` / `Segmentation by WB Tag` table from all UI tabs.

### UI Rules (`app.py`):
- **Persistent Excel Export:** Once the ETL pipeline completes and generates `Summary_Data_Auto.xlsx`, a native `st.download_button` MUST be permanently displayed in the Sidebar, allowing the user to download the file at any time without re-running the pipeline.

## PHASE 17: CRM INTELLIGENCE & WIN-BACK GENERATOR

### 1. Master Player List (`PlayerMasterList`)
- Group the raw data by `id` and `brand`.
- Calculate:
  - `Lifetime_GGR`: Sum of `revenue`.
  - `Lifetime_Turnover`: Sum of `bet`.
  - `First_Month`: Minimum `report_month`.
  - `Last_Month`: Maximum `report_month`.
  - `Months_Active`: Count of unique `report_month`.
- Calculate `Months_Inactive`: The difference in months between the global maximum `report_month` in the entire dataset and the player's specific `Last_Month`.

### 2. UI Rules (`app.py` - CRM Intelligence Tab)
- Create a new tab: `"🕵️ CRM Intelligence"`.
- **Global Filter:** A Brand selectbox at the top ("Both Business", "Rojabet", "Latribet") to filter the `PlayerMasterList`.
- **VIP & Risk Leaderboards:** Side-by-side tables (`st.columns(2)`) showing Top 50 "Crown Jewels" (highest Lifetime GGR) and Top 50 "Bonus Abusers" (Lifetime GGR < 0, sorted by highest Turnover).
- **Win-Back Generator:** An interactive section below a divider `---` with:
  - Sliders/Number Inputs for `Minimum Months Inactive` and `Minimum Lifetime GGR`.
  - A dynamic dataframe showing the filtered target list.
  - A native `st.download_button` to export the target list as a CSV.

### 3. Smart Campaign Profiling (Heuristics)
- **Entity Update:** Add a new string column called `Recommended_Campaign` to the `PlayerMasterList`.
- **Logic (Applied row by row):**
  - 🛑 **Promo Exclusion (Risk):** If `Lifetime_GGR` < 0 AND `Lifetime_Turnover` > 5000.
  - 🚨 **Early Churn VIP:** If `Months_Inactive` == 1 AND `Lifetime_GGR` > 500.
  - 🌟 **Rising Star:** If `Months_Active` <= 2 AND `Lifetime_Turnover` > 1000 AND `Months_Inactive` == 0.
  - 🎯 **Cold Crown Jewel:** If `Months_Inactive` >= 3 AND `Lifetime_GGR` > 1000.
  - ✉️ **Standard Lifecycle:** Everyone else.

### UI Rules (`app.py` - CRM Intelligence Tab)
- Below the Win-Back Generator, add a divider `---` and a subheader `> SMART CAMPAIGN PROFILING_`.
- Display a summary metric/chart showing the total count of players assigned to each of the 4 special campaigns (exclude Standard Lifecycle).
- Ensure the `Recommended_Campaign` column is included in the Master List, Leaderboards, and CSV exports so the CRM team knows exactly what to do with the downloaded IDs.

### 3. Smart Campaign Profiling (Heuristics)
- **Logic Update (Add to existing rules):**
  - Add 👑 **Active Crown Jewel**: If `Lifetime_GGR` >= 1000 AND `Months_Inactive` == 0. (Must be evaluated before Standard Lifecycle).

### UI Rules (`app.py` - CRM Intelligence Tab)
- **Campaign Extractor:** In the `> SMART CAMPAIGN PROFILING_` section, below the metric summary cards, add an interactive `st.selectbox` allowing the user to select a specific campaign (e.g., "🌟 Rising Star", "👑 Active Crown Jewel"). 
- Display a dataframe of the players who match the selected campaign, and provide a `st.download_button` to export that specific list as a CSV.

### UI Rules (`app.py` - Executive Summary Tab Expansion):
1. **Master Insight:** The top of the tab MUST feature the `> SYSTEM DIAGNOSTIC_` smart narrative.
2. **Whale Risk Explanation:** Directly below the Cross-Brand Performance Matrix, add a caption: *"🐳 **WHALE RISK %:** The percentage of total GGR generated by the top 10% of players. Values > 70% indicate extreme revenue concentration risk."*
3. **Section-Level Insights:** Precede every major section with a brief, italicized markdown insight explaining the business context of the data.
4. **New Section - Executive Demographics:** A cross-brand matrix comparing `Total Active`, `Conversions`, `New Players`, `Reactivated Players`, `Retained Players`, `Profitable (Winners)`, and `Neg. Yield (Losers)` for Combined, Rojabet, and Latribet.
5. **New Section - Executive Risk & VIP Health:** A cross-brand matrix comparing the RFM counts (👑 True VIPs, ⚠️ Churn Risk VIPs, 🐟 Casuals) across Combined, Rojabet, and Latribet.

### 1. Master Player List (`PlayerMasterList`) Updates
- **New Calculations:**
  - `Last_Month_Turnover`: The player's `bet` amount in their absolute most recent month of play.
  - `Avg_Monthly_Turnover`: `Lifetime_Turnover` / `Months_Active`.

### 3. Smart Campaign Profiling (Heuristics) Updates
- **Logic Update (Add to existing rules):**
  - Add 📉 **Cooling Down (Velocity Risk)**: If `Months_Inactive` == 0 AND `Last_Month_Turnover` < (`Avg_Monthly_Turnover` * 0.5) AND `Lifetime_Turnover` >= 1000. 
  - *(Rule Priority: Must be evaluated after Active Crown Jewel but before Standard Lifecycle).*

### UI Rules (`app.py` - CRM Intelligence Tab)
- Update the `> SMART CAMPAIGN PROFILING_` metric display to accommodate 5 columns (`st.columns(5)`) to include the new "📉 Cooling Down" count.

### 4. Visual Cohort Retention Heatmap
- **Data Prep:** 1. Determine the `cohort_month` (the minimum `report_month`) for every `id`.
  2. Merge this back to the raw data and calculate `month_index` (the integer difference in months between `report_month` and `cohort_month`).
  3. Pivot the data: Index = `cohort_month`, Columns = `month_index`, Values = count of unique `id`s.
  4. Divide all columns by the values in `month_index = 0` to get retention percentages.
- **Visualization:** Use Plotly Express (`px.imshow`) to render a heatmap. The color scale MUST be dark-mode compatible (e.g., fading from deep background grey/black to neon green `#00FF41`).
- **UI Placement (`app.py`):** Render this figure in the "🏦 Combined Deep-Dive", "🔴 Rojabet", and "🟢 Latribet" tabs using `st.plotly_chart(fig, use_container_width=True)`.