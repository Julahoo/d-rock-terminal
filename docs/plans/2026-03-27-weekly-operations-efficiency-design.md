# Weekly Operations Efficiency Charts Design
**Date:** 2026-03-27
**Target:** `app.py` -> Operations Command tab -> `Operations Efficiency Trends`

## Architecture & Components
The `Operations Efficiency Trends` section previously strictly rendered data on a daily basis. We are introducing a tabbed layout to prevent vertical UI bloat while satisfying executive reporting demands.
- **Components:** We wrap the section in `st.tabs(["📅 Daily Trends", "📆 Weekly Trends"])`.
- **Reusability:** The existing logic inside `display_trend_charts()` is completely structurally uncoupled, allowing us to pass `daily_trends` to the first tab, and `weekly_trends` to the second tab, maintaining singular logic execution.

## Data Flow (Weekly)
To ensure analytical consistency with the existing Lifecycle Forensics, we enforce the Friday-to-Thursday "completed week" definition.
1. Determine the latest completed Thursday relative to the current server date.
2. Excise all incoming snapshot data past that date to prevent artificial week drop-offs on the trailing edge of charts.
3. Apply `week_start` assigning every row to the preceding Friday.
4. Group by `week_start` and aggregate absolute metrics via `.agg('sum')`. 

## Error Handling & Percentage Stability
- **Recalculation:** Because aggregating daily percentages is mathematically incorrect (due to varying daily volumes), we structurally recalculate `Logins%` and `Conv%` on the newly aggregated weekly volumetric totals.
- **Guards:** We preserve the `.replace([float('inf'), -float('inf')], 0).fillna(0)` and `.clip(upper=100)` division-by-zero safeguards natively in `display_trend_charts()`.
