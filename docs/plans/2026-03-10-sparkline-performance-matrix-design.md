# Sparkline Performance Matrix — Design

## Goal
Add a compact, data-dense "Operations Pulse" section to the **📊 Dashboard** tab showing rolling KPI performance across 7/14/30/90-day windows at a glance. Executives see the numbers here; they drill into **📞 Operations** for daily detail.

## Layout
Two side-by-side matrices split by engagement type:

### `> LI OPERATIONS PULSE_` (Log In campaigns)
| Metric | 7 Days | 14 Days | 30 Days | 90 Days |
|---|---|---|---|---|
| **Volume** | avg + Δ + sparkline | ... | ... | ... |
| **Login %** | avg + Δ + sparkline | ... | ... | ... |
| **Conv %** | avg + Δ + sparkline | ... | ... | ... |

### `> NLI OPERATIONS PULSE_` (Not Logged In campaigns)
Same structure, filtered to `extracted_engagement = 'NLI'`.

## Cell Content
- **Average** of the metric over that window
- **Delta arrow**: ↑ green (improving), ↓ red (declining), — grey (flat, <1% change) vs prior equivalent period
- **Sparkline**: Tiny ~60px Plotly line chart showing daily shape within the window

## Data Source
- `ops_telemarketing_snapshots` (already in DB)
- Pre-filtered by sidebar global filters (Client, Brand, Country, etc.)
- Split by `extracted_engagement` column ('LI' vs 'NLI')
- Conv% = `KPI1-Conv. / Records * 100`
- Login% = `KPI2-Login / Records * 100`
- Volume = sum of `Records` per day

## Volume Row
Placeholder/hidden until new data feed arrives (2026-03-11).

## Implementation Notes
- Use `st.columns(2)` for side-by-side LI/NLI matrices
- Each cell rendered with `st.metric` (value + delta) stacked above a mini `st.plotly_chart` (height ~60px)
- Compute rolling windows relative to `max(ops_date)` in the filtered dataset
- Delta = current window avg - prior window avg (e.g., last 7d avg vs 7d before that)
