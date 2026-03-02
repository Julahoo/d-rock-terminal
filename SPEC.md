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

## 5. DEVELOPMENT PHASES
- **Phase 1: Setup & Scaffolding** (Project init, folder structure, mock data ingestion).
- **Phase 2: Data Ingestion & Transformation** (Parsing the raw CSVs into Pandas DataFrames).
- **Phase 3: Core Analytics Logic** (Calculating Winners/Losers/Flat, GGR, and Cohort tracking for Retention).
- **Phase 4: Output Generation** (Exporting to multi-tab Excel format).
- **Phase 5: Campaign Extension** (Integrate Campaign Summary KPI report rules).