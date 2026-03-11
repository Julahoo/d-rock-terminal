# H1-over-H1 Benchmark Table — Design

## Goal
Add a dynamic half-year benchmark table to the Dashboard that automatically compares the current half-year against the same half last year. Sidebar filters apply.

## Auto-detection Logic
- Determine current half: `H1` (Jan-Jun) or `H2` (Jul-Dec) based on today's date.
- **Column 1**: Same half from previous year (e.g., H1 2025).
- **Column 2**: Current half (e.g., H1 2026 YTD).
- **Column 3**: Δ (percentage change).

## Metric Groups

### 1. Volume (raw totals)
| Metric | Formula |
|---|---|
| Records | SUM(records) |
| Logins | SUM(kpi2_logins) |
| Conversions | SUM(conversions) |

### 2. Call Dispositions (% of Records)
| Metric | Formula |
|---|---|
| D % | (d_plus + d_minus + d_neutral) / Records |
| NA % | na / Records |
| I % | (t + dnc + dx + wn + am) / Records |

### 3. Email Channel (% of Emails Sent - `es`)
| Metric | Formula |
|---|---|
| ED % | ed / es |
| EO % | eo / es |
| EC % | ec / es |
| EF % | ef / es |

### 4. SMS Channel (% of SMS Sent - column TBD: `sa` or `ss`)
| Metric | Formula |
|---|---|
| SD % | sd / ss |
| SF % | sf / ss |
| SP % | sp / ss |

## Data Source
- `ops_telemarketing_snapshots` filtered by sidebar globals.
- Note: data starts 2025-01-01; H1 2024 comparison not available until we have 2024 data.

## Location
Dashboard tab, below the LI/NLI Pulse Matrices.
