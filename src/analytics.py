"""
src/analytics.py – Aggregation Engine (Phase 3 + Phase 8)
=========================================================
Financial Calculator, Cohort Analyzer, Segmentation, and Campaign Analytics.

* Groups ingested data by brand × month.
* Calculates profitable/negative-yield player counts, GGR, Handle, Hold %.
* Whale analysis: top-10% GGR share.
* Tracks player cohorts (New vs Returning) with a stateful
  ``seen_ids`` set per brand, processed in chronological order.
* Segmentation matrix by ``wb_tag``.

Spec refs: §2 MonthlyBrandSummary, §3-B, §4 Cohort Tracking, §8.
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def generate_monthly_summaries(df: pd.DataFrame) -> pd.DataFrame:
    """Take the unified raw DataFrame (PlayerRecord rows) and produce
    a summarised DataFrame matching the ``MonthlyBrandSummary`` entity.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns: ``id``, ``brand``, ``revenue``,
        ``report_month``.

    Returns
    -------
    pd.DataFrame
        One row per brand × month with all summary fields from
        SPEC.md §2.
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to generate_monthly_summaries")
        return _empty_summary()

    required = {"id", "brand", "revenue", "bet", "report_month"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")

    # ── 1. Financial metrics (fully vectorised) --------------------------
    financial = _compute_financial_metrics(df)

    # ── 2. Cohort metrics (stateful per brand) ---------------------------
    cohort = _compute_cohort_metrics(df)

    # ── 3. Merge and derive remaining fields -----------------------------
    summary = financial.merge(cohort, on=["brand", "month"], how="left")

    # retention_pct = (returning_players / total_players) * 100
    summary["retention_pct"] = _safe_pct(
        summary["returning_players"], summary["total_players"]
    )

    # turnover_per_player (Phase 12)
    summary["turnover_per_player"] = summary.apply(
        lambda r: round(r["total_handle"] / r["total_players"], 2)
        if r["total_players"] > 0 else 0.0,
        axis=1,
    )

    # revenue_share_deduction (15% of GGR) for individual brands
    summary["revenue_share_deduction"] = (summary["ggr"] * 0.15).round(2)

    # ── 4. Combined entity (Phase 9) ─────────────────────────────────────
    combined = _build_combined_financial(summary, df)
    summary = pd.concat([summary, combined], ignore_index=True)

    # Ensure clean column order matching the spec
    summary = summary[SUMMARY_COLS]

    # Round float columns to 2dp to prevent precision artifacts in UI
    for c in summary.select_dtypes(include=["float64", "float32"]).columns:
        summary[c] = summary[c].round(2)

    logger.info(
        "Generated %d monthly summaries (%d brands × %d months)",
        len(summary),
        summary["brand"].nunique(),
        summary["month"].nunique(),
    )
    return summary


# ═══════════════════════════════════════════════════════════════════════════
#  Financial Calculator
# ═══════════════════════════════════════════════════════════════════════════
def _compute_financial_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Group by brand × report_month and compute player-status counts,
    GGR, and GGR-per-player.  Fully vectorised — no iterrows."""

    grouped = df.groupby(["brand", "report_month"], sort=False)

    agg = grouped.agg(
        negative_yield_players=("revenue", lambda s: (s < 0).sum()),
        profitable_players=("revenue", lambda s: (s > 0).sum()),
        flat=("revenue", lambda s: (s == 0).sum()),
        total_players=("id", "count"),
        ggr=("revenue", "sum"),
        ngr=("ngr", "sum"),
        total_handle=("bet", "sum"),
        turnover_casino=("bet_casino", "sum"),
        ggr_casino=("revenue_casino", "sum"),
        ngr_casino=("ngr_casino", "sum"),
        turnover_sports=("bet_sports", "sum"),
        ggr_sports=("revenue_sports", "sum"),
        ngr_sports=("ngr_sports", "sum"),
        deposit_count=("deposit_count", "sum"),
        deposits=("deposits", "sum"),
        withdrawals=("withdrawals", "sum"),
        bonus_total=("bonus_total", "sum"),
        bonus_casino=("bonus_casino", "sum"),
        bonus_sports=("bonus_sports", "sum"),
        tax_total=("tax_total", "sum"),
    ).reset_index()

    agg.rename(columns={"report_month": "month"}, inplace=True)
    agg["net_deposits"] = agg["deposits"] - agg["withdrawals"]

    # profitable_pct = (profitable_players / total_players) * 100
    agg["profitable_pct"] = _safe_pct(agg["profitable_players"], agg["total_players"])

    # ggr_per_player = ggr / total_players
    agg["ggr_per_player"] = agg.apply(
        lambda r: round(r["ggr"] / r["total_players"], 6)
        if r["total_players"] > 0
        else 0.0,
        axis=1,
    )

    # hold_pct = (ggr / total_handle) * 100
    agg["hold_pct"] = _safe_pct(agg["ggr"], agg["total_handle"])

    # top_10_pct_ggr_share: whale analysis (vectorised per brand×month)
    def _whale_share(group):
        profitable = group[group["revenue"] > 0].nlargest(max(1, int(len(group[group["revenue"] > 0]) * 0.10)), "revenue")
        return profitable["revenue"].sum()

    whale = (
        df.groupby(["brand", "report_month"])
        .apply(_whale_share, include_groups=False)
        .reset_index(name="_top_rev")
        .rename(columns={"report_month": "month"})
    )
    agg = agg.merge(whale, on=["brand", "month"], how="left")
    agg["top_10_pct_ggr_share"] = np.where(
        agg["ggr"] != 0,
        ((agg["_top_rev"].fillna(0) / agg["ggr"]) * 100).round(6),
        0.0,
    )
    agg.drop(columns=["_top_rev"], inplace=True)

    return agg


# ═══════════════════════════════════════════════════════════════════════════
#  Cohort Analyzer
# ═══════════════════════════════════════════════════════════════════════════
def _compute_cohort_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Track New vs Returning players per brand × month.

    Strategy (per SPEC §4 & user instructions):
      1. Sort all data chronologically by ``report_month``.
      2. Maintain a ``seen_ids`` set **per brand**.
      3. For each brand×month group, any player id NOT in
         ``seen_ids`` is "New"; everything else is "Returning".
      4. After processing the group, add its ids to ``seen_ids``.
    """
    records: list[dict[str, Any]] = []

    # Chronological order is critical for correct cohort assignment
    sorted_months = sorted(df["report_month"].unique())

    brands = sorted(df["brand"].unique())
    seen_ids: dict[str, set] = {b: set() for b in brands}
    last_month_ids: dict[str, set] = {b: set() for b in brands}

    for month in sorted_months:
        month_df = df[df["report_month"] == month]

        for brand in brands:
            brand_month = month_df[month_df["brand"] == brand]
            if brand_month.empty:
                continue

            current_ids = set(brand_month["id"].unique())
            previously_seen = seen_ids[brand]
            prev_month = last_month_ids[brand]

            returning = current_ids & previously_seen
            new = current_ids - previously_seen
            # Reactivated = was seen historically, but NOT last month
            reactivated = returning - prev_month
            conversions = len(new) + len(reactivated)

            records.append(
                {
                    "brand": brand,
                    "month": month,
                    "new_players": len(new),
                    "returning_players": len(returning),
                    "reactivated_players": len(reactivated),
                    "conversions": conversions,
                    "new_player_ggr": round(
                        brand_month[brand_month["id"].isin(new)]["revenue"].sum(), 2
                    ),
                    "returning_player_ggr": round(
                        brand_month[brand_month["id"].isin(returning)]["revenue"].sum(), 2
                    ),
                }
            )

            # Update tracking sets
            seen_ids[brand].update(current_ids)
            last_month_ids[brand] = current_ids

    return pd.DataFrame(records)


# ═══════════════════════════════════════════════════════════════════════════
#  Combined Entity Builder (Phase 9)
# ═══════════════════════════════════════════════════════════════════════════
_ADDITIVE_FINANCIAL_COLS = [
    "negative_yield_players", "profitable_players", "flat",
    "total_players", "ggr", "ngr", "total_handle",
    "turnover_casino", "ggr_casino", "ngr_casino",
    "turnover_sports", "ggr_sports", "ngr_sports",
    "deposit_count", "deposits", "withdrawals", "net_deposits",
    "bonus_total", "bonus_casino", "bonus_sports", "tax_total",
    "new_players", "returning_players",
    "reactivated_players", "conversions",
    "new_player_ggr", "returning_player_ggr",
    "revenue_share_deduction",
]


def _build_combined_financial(
    summary: pd.DataFrame, raw_df: pd.DataFrame
) -> pd.DataFrame:
    """Sum additive metrics across brands per month, recalculate ratios."""
    combined = (
        summary.groupby("month", sort=False)[_ADDITIVE_FINANCIAL_COLS]
        .sum()
        .reset_index()
    )
    combined["brand"] = "Combined"

    # Recalculate ratio metrics from sums (SPEC §9: must NOT average)
    combined["profitable_pct"] = _safe_pct(
        combined["profitable_players"], combined["total_players"]
    )
    combined["hold_pct"] = _safe_pct(combined["ggr"], combined["total_handle"])
    combined["ggr_per_player"] = combined.apply(
        lambda r: round(r["ggr"] / r["total_players"], 6)
        if r["total_players"] > 0 else 0.0,
        axis=1,
    )
    combined["retention_pct"] = _safe_pct(
        combined["returning_players"], combined["total_players"]
    )
    combined["turnover_per_player"] = combined.apply(
        lambda r: round(r["total_handle"] / r["total_players"], 2)
        if r["total_players"] > 0 else 0.0,
        axis=1,
    )

    # Whale analysis for Combined (vectorised across all brands)
    def _whale_share_combined(group):
        profitable = group[group["revenue"] > 0].nlargest(max(1, int(len(group[group["revenue"] > 0]) * 0.10)), "revenue")
        return profitable["revenue"].sum()

    whale_c = (
        raw_df.groupby("report_month")
        .apply(_whale_share_combined, include_groups=False)
        .reset_index(name="_top_rev")
        .rename(columns={"report_month": "month"})
    )
    combined = combined.merge(whale_c, on="month", how="left")
    combined["top_10_pct_ggr_share"] = np.where(
        combined["ggr"] != 0,
        ((combined["_top_rev"].fillna(0) / combined["ggr"]) * 100).round(6),
        0.0,
    )
    combined.drop(columns=["_top_rev"], inplace=True)

    return combined


# ═══════════════════════════════════════════════════════════════════════════
#  Both Business Summary (Phase 9 — SPEC §2 BothBusinessSummary)
# ═══════════════════════════════════════════════════════════════════════════
BOTH_BUSINESS_COLS = [
    "month",
    "turnover",
    "ggr",
    "ngr",
    "margin",
    "turnover_casino",
    "ggr_casino",
    "ngr_casino",
    "turnover_sports",
    "ggr_sports",
    "ngr_sports",
    "revenue_share_deduction",
    "net_income",
    "new_players",
    "returning_players",
    "reactivated_players",
    "conversions",
    "total_players",
    "profitable_players",
    "negative_yield_players",
    "new_players_pct",
    "returning_players_pct",
    "ggr_per_player",
    "turnover_per_player",
    "income_per_player",
    "new_player_ggr",
    "returning_player_ggr",
    "deposit_count",
    "deposits",
    "withdrawals",
    "net_deposits",
    "bonus_total",
    "bonus_casino",
    "bonus_sports",
    "tax_total",
]

REV_SHARE_RATE = 0.15  # 15% revenue share deduction


def generate_both_business_summary(
    brand_summaries_df: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate brand-level summaries into a single combined view.

    **SPEC §2 — BothBusinessSummary entity + §4 Domain Rules:**

    1. Sum additive metrics (Turnover = total_handle, GGR = ggr,
       New_Players, Returning_Players) across both brands per month.
    2. Recalculate all ratios from the combined sums (NEVER average).
    3. Apply 15 % Revenue Share deduction.

    Parameters
    ----------
    brand_summaries_df : pd.DataFrame
        The ``MonthlyBrandSummary`` DataFrame (Phase 3 output).
        Must exclude any pre-existing "Combined" rows.

    Returns
    -------
    pd.DataFrame
        One row per month matching the ``BothBusinessSummary`` entity.
    """
    if brand_summaries_df.empty:
        logger.warning("Empty DataFrame — returning empty Both Business summary")
        return pd.DataFrame(columns=BOTH_BUSINESS_COLS)

    # Only use real brand rows (exclude "Combined" if present)
    real = brand_summaries_df[brand_summaries_df["brand"] != "Combined"]

    bb = (
        real.groupby("month", sort=False)
        .agg(
            turnover=("total_handle", "sum"),
            ggr=("ggr", "sum"),
            ngr=("ngr", "sum"),
            turnover_casino=("turnover_casino", "sum"),
            ggr_casino=("ggr_casino", "sum"),
            ngr_casino=("ngr_casino", "sum"),
            turnover_sports=("turnover_sports", "sum"),
            ggr_sports=("ggr_sports", "sum"),
            ngr_sports=("ngr_sports", "sum"),
            new_players=("new_players", "sum"),
            returning_players=("returning_players", "sum"),
            reactivated_players=("reactivated_players", "sum"),
            conversions=("conversions", "sum"),
            profitable_players=("profitable_players", "sum"),
            negative_yield_players=("negative_yield_players", "sum"),
            new_player_ggr=("new_player_ggr", "sum"),
            returning_player_ggr=("returning_player_ggr", "sum"),
            deposit_count=("deposit_count", "sum"),
            deposits=("deposits", "sum"),
            withdrawals=("withdrawals", "sum"),
            bonus_total=("bonus_total", "sum"),
            bonus_casino=("bonus_casino", "sum"),
            bonus_sports=("bonus_sports", "sum"),
            tax_total=("tax_total", "sum"),
        )
        .reset_index()
        .sort_values("month")
        .reset_index(drop=True)
    )
    
    bb["net_deposits"] = bb["deposits"] - bb["withdrawals"]

    # Derived fields
    bb["margin"] = _safe_pct(bb["ggr"], bb["turnover"])  # as percentage
    bb["revenue_share_deduction"] = (bb["ggr"] * REV_SHARE_RATE).round(2)
    bb["net_income"] = (bb["ggr"] - bb["revenue_share_deduction"]).round(2)
    bb["total_players"] = bb["new_players"] + bb["returning_players"]
    bb["new_players_pct"] = _safe_pct(bb["new_players"], bb["total_players"])
    bb["returning_players_pct"] = _safe_pct(
        bb["returning_players"], bb["total_players"]
    )
    bb["ggr_per_player"] = bb.apply(
        lambda r: round(r["ggr"] / r["total_players"], 2)
        if r["total_players"] > 0 else 0.0,
        axis=1,
    )
    bb["income_per_player"] = bb.apply(
        lambda r: round(r["net_income"] / r["total_players"], 2)
        if r["total_players"] > 0 else 0.0,
        axis=1,
    )
    bb["turnover_per_player"] = bb.apply(
        lambda r: round(r["turnover"] / r["total_players"], 2)
        if r["total_players"] > 0 else 0.0,
        axis=1,
    )

    # Round all float columns to eliminate precision artifacts
    _float_cols = ["turnover", "ggr", "ngr", "margin", "revenue_share_deduction",
                   "net_income", "new_players_pct", "returning_players_pct",
                   "ggr_per_player", "turnover_per_player", "income_per_player",
                   "new_player_ggr", "returning_player_ggr"]
    for c in _float_cols:
        if c in bb.columns:
            bb[c] = bb[c].round(2)

    bb = bb[BOTH_BUSINESS_COLS]

    logger.info(
        "Both Business summary: %d months, latest turnover $%s",
        len(bb),
        f"{bb['turnover'].iloc[-1]:,.0f}" if not bb.empty else "0",
    )
    return bb


# ═══════════════════════════════════════════════════════════════════════════
#  Time-Series Intelligence (Phase 11 — SPEC §11 TimeSeriesMetrics)
# ═══════════════════════════════════════════════════════════════════════════
_TS_METRICS = [
    "turnover",
    "ggr",
    "ngr",
    "revenue_share_deduction",
    "total_players",
    "new_players",
    "returning_players",
    "reactivated_players",
    "conversions",
    "profitable_players",
    "negative_yield_players",
]


def generate_time_series(summary_df: pd.DataFrame) -> dict:
    """Compute MoM, YoY, YTD, and QoQ for key metrics.

    Parameters
    ----------
    summary_df : pd.DataFrame
        A monthly summary DataFrame with a ``month`` column (YYYY-MM str)
        and the metric columns listed in ``_TS_METRICS``.  This can be the
        ``BothBusinessSummary`` (rename turnover→turnover already done) or
        a single-brand slice of ``MonthlyBrandSummary`` (map total_handle→
        turnover externally).

    Returns
    -------
    dict  with keys ``"monthly"`` and ``"quarterly"``.
        ``monthly`` — DataFrame indexed by month with absolute + % change
        columns for MoM, YoY, and YTD.
        ``quarterly`` — DataFrame indexed by quarter with QoQ abs + %.
    """
    if summary_df.empty:
        return {"monthly": pd.DataFrame(), "quarterly": pd.DataFrame()}

    df = summary_df.copy().sort_values("month").reset_index(drop=True)

    # Ensure all target columns exist (brand summaries use total_handle not turnover)
    if "turnover" not in df.columns and "total_handle" in df.columns:
        df["turnover"] = df["total_handle"]

    # Only keep columns we need
    avail = [c for c in _TS_METRICS if c in df.columns]
    ts = df[["month"] + avail].copy()

    # ── MoM (Month-over-Month): shift(1) ─────────────────────────────────
    for col in avail:
        prev = ts[col].shift(1)
        ts[f"{col}_mom_delta"] = ts[col] - prev
        ts[f"{col}_mom_pct"] = _safe_pct(ts[col] - prev, prev)

    # ── YoY (Year-over-Year): shift(12) ──────────────────────────────────
    for col in avail:
        prev12 = ts[col].shift(12)
        ts[f"{col}_yoy_delta"] = ts[col] - prev12
        ts[f"{col}_yoy_pct"] = _safe_pct(ts[col] - prev12, prev12)

    # ── YTD (Year-to-Date): cumsum within each calendar year ─────────────
    ts["_year"] = ts["month"].str[:4]
    ts["_month_int"] = ts["month"].str[5:7].astype(int)
    for col in avail:
        ts[f"{col}_ytd"] = ts.groupby("_year")[col].cumsum()

    # ── EOY Projections: Dual Momentum + Seasonal Engine ───────────────
    for proj_col in ["ggr", "turnover"]:
        ytd_col = f"{proj_col}_ytd"
        if ytd_col not in ts.columns:
            continue

        # Naive fallback
        naive_eoy = ((ts[ytd_col] / ts["_month_int"]) * 12).round(2)

        # Momentum: YTD + rolling_3m_avg * remaining_months
        rolling_3m = ts[proj_col].rolling(3, min_periods=1).mean()
        remaining = 12 - ts["_month_int"]
        ts[f"eoy_momentum_{proj_col}"] = (ts[ytd_col] + rolling_3m * remaining).round(2)

        # Seasonal: current_YTD * (prev_year_total / prev_year_YTD_same_month)
        prev_ytd_same_month = ts[ytd_col].shift(12)
        prev_year_total = ts.groupby("_year")[proj_col].transform("sum").shift(12)
        seasonal_ratio = prev_year_total / prev_ytd_same_month
        ts[f"eoy_seasonal_{proj_col}"] = np.where(
            (prev_ytd_same_month.notna()) & (prev_ytd_same_month != 0),
            (ts[ytd_col] * seasonal_ratio).round(2),
            naive_eoy,
        )

    # Revenue (15% of GGR projections)
    if "eoy_momentum_ggr" in ts.columns:
        ts["eoy_momentum_revenue_share_deduction"] = (ts["eoy_momentum_ggr"] * 0.15).round(2)
    if "eoy_seasonal_ggr" in ts.columns:
        ts["eoy_seasonal_revenue_share_deduction"] = (ts["eoy_seasonal_ggr"] * 0.15).round(2)

    ts.drop(columns=["_year", "_month_int"], inplace=True)

    # ── QoQ (Quarter-over-Quarter) ───────────────────────────────────────
    df["_quarter"] = pd.to_datetime(df["month"] + "-01").dt.to_period("Q").astype(str)
    q_agg = {c: "sum" for c in avail if c in df.columns}
    quarterly = df.groupby("_quarter", sort=True).agg(q_agg).reset_index()
    quarterly.rename(columns={"_quarter": "quarter"}, inplace=True)

    for col in avail:
        if col not in quarterly.columns:
            continue
        prev_q = quarterly[col].shift(1)
        quarterly[f"{col}_qoq_delta"] = quarterly[col] - prev_q
        quarterly[f"{col}_qoq_pct"] = _safe_pct(quarterly[col] - prev_q, prev_q)

    logger.info(
        "Time-series: %d monthly rows, %d quarterly rows, %d metrics",
        len(ts), len(quarterly), len(avail),
    )

    # Round all computed columns to 2dp to prevent float artifacts
    for c in ts.columns:
        if c != "month" and ts[c].dtype in ("float64", "float32"):
            ts[c] = ts[c].round(2)
    for c in quarterly.columns:
        if c != "quarter" and quarterly[c].dtype in ("float64", "float32"):
            quarterly[c] = quarterly[c].round(2)

    return {"monthly": ts, "quarterly": quarterly}


# ═══════════════════════════════════════════════════════════════════════════
#  RFM Summary (Phase 15 — VIP Tiering)
# ═══════════════════════════════════════════════════════════════════════════
def generate_rfm_summary(
    raw_df: pd.DataFrame, target_month: str,
) -> pd.DataFrame:
    """Segment players into VIP tiers using Recency/Frequency/Monetary.

    Tier definitions
    ----------------
    - **True VIP**: Frequency ≥ 3 months, Monetary > 0, played in target_month.
    - **Churn Risk**: Frequency ≥ 3, Monetary > 0, did NOT play in target_month.
    - **Casual**: Everyone else (low frequency or negative monetary).

    Returns a summary DataFrame with columns: Tier, Players, GGR.
    """
    if raw_df.empty:
        return pd.DataFrame(columns=["Tier", "Players", "GGR"])

    # Player-level aggregation
    player = (
        raw_df.groupby("id")
        .agg(
            frequency=("report_month", "nunique"),
            monetary=("revenue", "sum"),
            months=("report_month", lambda x: set(x)),
        )
        .reset_index()
    )
    player["recent"] = player["months"].apply(lambda s: target_month in s)

    def _tier(row):
        if row["frequency"] >= 3 and row["monetary"] > 0 and row["recent"]:
            return "True VIP"
        if row["frequency"] >= 3 and row["monetary"] > 0 and not row["recent"]:
            return "Churn Risk"
        return "Casual"

    player["tier"] = player.apply(_tier, axis=1)

    summary = (
        player.groupby("tier")
        .agg(Players=("id", "count"), GGR=("monetary", "sum"))
        .reset_index()
        .rename(columns={"tier": "Tier"})
    )
    summary["GGR"] = summary["GGR"].round(2)

    # Ensure consistent order
    tier_order = ["True VIP", "Churn Risk", "Casual"]
    summary["Tier"] = pd.Categorical(summary["Tier"], categories=tier_order, ordered=True)
    summary = summary.sort_values("Tier").reset_index(drop=True)

    logger.info("RFM summary for %s: %s", target_month,
                {r["Tier"]: r["Players"] for _, r in summary.iterrows()})
    return summary


# ═══════════════════════════════════════════════════════════════════════════
#  Smart Narrative (Phase 15 — Diagnostic Text)
# ═══════════════════════════════════════════════════════════════════════════
def generate_smart_narrative(
    ts_row: pd.Series,
    margin: float,
    whale_dependency: float,
) -> str:
    """Generate a 3-sentence diagnostic insight from the latest metrics.

    Sentence 1: MoM GGR performance direction.
    Sentence 2: Margin health assessment.
    Sentence 3: Whale dependency risk level.
    """
    # Sentence 1: GGR trend
    mom_pct = ts_row.get("ggr_mom_pct", 0) or 0
    mom_delta = ts_row.get("ggr_mom_delta", 0) or 0
    if mom_pct >= 0:
        s1 = f"GGR grew +{mom_pct:.1f}% month-over-month (${mom_delta:+,.0f}), signalling positive revenue momentum."
    else:
        s1 = f"GGR declined {mom_pct:.1f}% month-over-month (${mom_delta:,.0f}), indicating revenue contraction."

    # Sentence 2: Margin health
    if margin >= 5.0:
        s2 = f"Operating margin is healthy at {margin:.2f}%, well above the 2.5% alert threshold."
    elif margin >= 2.5:
        s2 = f"Operating margin at {margin:.2f}% is adequate but approaching the 2.5% caution zone."
    else:
        s2 = f"⚠️ MARGIN ALERT: Operating margin has dropped to {margin:.2f}%, below the 2.5% threshold."

    # Sentence 3: Whale risk
    if whale_dependency >= 70:
        s3 = f"🚨 HIGH WHALE RISK: Top 10% of players account for {whale_dependency:.1f}% of GGR — extreme concentration."
    elif whale_dependency >= 50:
        s3 = f"Whale dependency is elevated at {whale_dependency:.1f}% — revenue is moderately concentrated in top players."
    else:
        s3 = f"Revenue is well-diversified with top 10% accounting for {whale_dependency:.1f}% of GGR."

    return f"{s1}\n\n{s2}\n\n{s3}"


# ═══════════════════════════════════════════════════════════════════════════
#  Program Summary (Phase 12 — Lifecycle ROI)
# ═══════════════════════════════════════════════════════════════════════════
def generate_program_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Group raw data by brand, report_month, and wb_tag.

    Produces a summary with total GGR, Turnover, Margin, and player count per
    lifecycle program tag (ACQ, RET, WB, etc.) for each brand × month.
    """
    if df.empty or "wb_tag" not in df.columns:
        return pd.DataFrame(columns=["brand", "month", "Program", "ggr", "Turnover", "Margin", "total_players"])

    prog = (
        df.groupby(["brand", "report_month", "wb_tag"])
        .agg(
            ggr=("revenue", "sum"),
            Turnover=("bet", "sum"),
            total_players=("id", "nunique"),
        )
        .reset_index()
        .rename(columns={"report_month": "month", "wb_tag": "Program"})
        .sort_values(["brand", "month", "Program"])
        .reset_index(drop=True)
    )

    # Round
    prog["ggr"] = prog["ggr"].round(2)
    prog["Turnover"] = prog["Turnover"].round(2)
    prog["Margin"] = np.where(prog["Turnover"] != 0, (prog["ggr"] / prog["Turnover"]) * 100, 0.0)
    prog["Margin"] = prog["Margin"].round(2)

    logger.info("Program summary: %d rows, %d tags", len(prog), prog["Program"].nunique())
    return prog


# ═══════════════════════════════════════════════════════════════════════════
#  Player Master List (Phase 17.1 — CRM Intelligence)
# ═══════════════════════════════════════════════════════════════════════════
def generate_player_master_list(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Build a lifetime-level player master list grouped by id and brand.

    Returns DataFrame with columns:
        id, brand, Lifetime_GGR, Lifetime_Turnover,
        First_Month, Last_Month, Months_Active, Months_Inactive.
    """
    if raw_df.empty:
        return pd.DataFrame(columns=[
            "id", "brand", "Lifetime_GGR", "Lifetime_Turnover",
            "First_Month", "Last_Month", "Months_Active", "Months_Inactive",
        ])

    master = (
        raw_df.groupby(["id", "brand"])
        .agg(
            client=("client", "first"),
            Lifetime_GGR=("revenue", "sum"),
            Lifetime_NGR=("ngr", "sum"),
            Lifetime_Turnover=("bet", "sum"),
            Lifetime_Deposits=("deposits", "sum"),
            Lifetime_Deposit_Count=("deposit_count", "sum"),
            Lifetime_Withdrawals=("withdrawals", "sum"),
            Lifetime_Bonus=("bonus_total", "sum"),
            First_Month=("report_month", "min"),
            Last_Month=("report_month", "max"),
            Months_Active=("report_month", "nunique"),
        )
        .reset_index()
    )
    master["Lifetime_GGR"] = master["Lifetime_GGR"].round(2)
    master["Lifetime_NGR"] = master["Lifetime_NGR"].round(2)
    master["Lifetime_Turnover"] = master["Lifetime_Turnover"].round(2)
    master["Lifetime_Deposits"] = master["Lifetime_Deposits"].round(2)
    master["Lifetime_Withdrawals"] = master["Lifetime_Withdrawals"].round(2)
    master["Lifetime_Bonus"] = master["Lifetime_Bonus"].round(2)

    master["Avg_Deposit_Value"] = master.apply(
        lambda x: x["Lifetime_Deposits"] / x["Lifetime_Deposit_Count"] if x.get("Lifetime_Deposit_Count", 0) > 0 else 0, 
        axis=1
    )

    # Months_Inactive: how many months since they last played
    global_max = raw_df["report_month"].max()
    global_max_period = pd.Period(global_max, freq="M")
    master["Months_Inactive"] = master["Last_Month"].apply(
        lambda lm: (global_max_period - pd.Period(lm, freq="M")).n
    )

    # Velocity metrics: Last_Month_Turnover & Avg_Monthly_Turnover
    last_bet = (
        raw_df.sort_values("report_month")
        .groupby(["id", "brand"])["bet"]
        .last()
        .rename("Last_Month_Turnover")
    )
    master = master.merge(last_bet, on=["id", "brand"], how="left")
    master["Last_Month_Turnover"] = master["Last_Month_Turnover"].fillna(0).round(2)
    master["Avg_Monthly_Turnover"] = (
        master["Lifetime_Turnover"] / master["Months_Active"].clip(lower=1)
    ).round(2)

    # Tenure: total months from first to last activity (inclusive)
    master["Tenure_Months"] = master.apply(
        lambda r: (pd.Period(r["Last_Month"], freq="M") - pd.Period(r["First_Month"], freq="M")).n + 1,
        axis=1,
    )

    # Recommended Campaign classification (Phase 17.4)
    conditions = [
        (master["Months_Active"] >= 6) & (master["Months_Inactive"] == 0) & (master["Months_Active"] == master["Tenure_Months"]),
        (master["Lifetime_GGR"] < 0) & (master["Lifetime_Turnover"] > 5000),
        (master["Months_Inactive"] == 1) & (master["Lifetime_GGR"] > 500),
        (master["Months_Active"] <= 2) & (master["Lifetime_Turnover"] > 1000) & (master["Months_Inactive"] == 0),
        (master["Months_Inactive"] >= 3) & (master["Lifetime_GGR"] > 1000),
        (master["Lifetime_GGR"] >= 1000) & (master["Months_Inactive"] == 0),
        (master["Months_Inactive"] == 0) & (master["Last_Month_Turnover"] < (master["Avg_Monthly_Turnover"] * 0.5)) & (master["Lifetime_Turnover"] >= 1000),
    ]
    choices = [
        "🏆 Ironman Legend",
        "🛑 Promo Exclusion",
        "🚨 Early Churn VIP",
        "🌟 Rising Star",
        "🎯 Cold Crown Jewel",
        "👑 Active Crown Jewel",
        "📉 Cooling Down",
    ]
    master["Recommended_Campaign"] = np.select(conditions, choices, default="✉️ Standard Lifecycle")

    logger.info("Player master list: %d players", len(master))
    return master


def generate_overlap_stats(raw_df: pd.DataFrame) -> dict:
    """Identify players active on both brands and their combined GGR."""
    roja_ids = set(raw_df.loc[raw_df["brand"] == "Rojabet", "id"].unique())
    latri_ids = set(raw_df.loc[raw_df["brand"] == "Latribet", "id"].unique())
    shared_ids = roja_ids & latri_ids
    overlap_ggr = float(raw_df.loc[raw_df["id"].isin(shared_ids), "revenue"].sum())
    return {"overlap_count": len(shared_ids), "overlap_ggr": round(overlap_ggr, 2)}


def generate_ltv_curves(df: pd.DataFrame):
    """Build cumulative LTV curves per cohort as a Plotly line chart."""
    import plotly.express as px

    if df.empty:
        return None

    # 1. Cohort month per player
    first_months = df.groupby("id")["report_month"].min().reset_index(name="cohort_month")
    merged = df.merge(first_months, on="id", how="left")

    # 2. Month index
    merged["cohort_dt"] = pd.to_datetime(merged["cohort_month"])
    merged["report_dt"] = pd.to_datetime(merged["report_month"])
    merged["month_index"] = (
        (merged["report_dt"].dt.year - merged["cohort_dt"].dt.year) * 12
        + merged["report_dt"].dt.month - merged["cohort_dt"].dt.month
    )

    # 3. Revenue per cohort × month_index
    cohort_rev = (
        merged.groupby(["cohort_month", "month_index"])["revenue"]
        .sum()
        .reset_index()
        .sort_values(["cohort_month", "month_index"])
    )

    # 4. Cumulative revenue
    cohort_rev["cumulative_revenue"] = (
        cohort_rev.groupby("cohort_month")["revenue"].cumsum()
    )

    # 5. Plotly line chart
    fig = px.line(
        cohort_rev,
        x="month_index",
        y="cumulative_revenue",
        color="cohort_month",
        markers=True,
        labels={"month_index": "Month Index", "cumulative_revenue": "Cumulative GGR", "cohort_month": "Cohort"},
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font_color="#00FF41",
        legend=dict(font=dict(color="#00FF41")),
        xaxis=dict(gridcolor="#1a1a1a"),
        yaxis=dict(gridcolor="#1a1a1a", tickprefix="$", tickformat=",.0f"),
        margin=dict(l=0, r=0, t=30, b=0),
        height=450,
        dragmode=False,
    )
    return fig


def generate_retention_heatmap(df: pd.DataFrame):
    """Build a cohort retention heatmap as a Plotly figure.

    Returns a plotly Figure showing % of players retained per cohort month.
    """
    import plotly.express as px

    if df.empty:
        return None

    # 1. Find each player's cohort (first) month
    first_months = df.groupby("id")["report_month"].min().reset_index(name="cohort_month")
    merged = df.merge(first_months, on="id", how="left")

    # 2. Calculate month_index (months since cohort)
    merged["cohort_dt"] = pd.to_datetime(merged["cohort_month"])
    merged["report_dt"] = pd.to_datetime(merged["report_month"])
    merged["month_index"] = (
        (merged["report_dt"].dt.year - merged["cohort_dt"].dt.year) * 12
        + merged["report_dt"].dt.month - merged["cohort_dt"].dt.month
    )

    # 3. Count unique players per cohort × month_index
    cohort_data = (
        merged.groupby(["cohort_month", "month_index"])["id"]
        .nunique()
        .reset_index(name="players")
    )

    # 4. Pivot and normalize to retention %
    retention = cohort_data.pivot_table(
        index="cohort_month", columns="month_index", values="players"
    )
    retention = retention.div(retention[0], axis=0)

    # 5. Build Plotly heatmap
    fig = px.imshow(
        retention,
        text_auto=".0%",
        aspect="auto",
        color_continuous_scale=["#0E1117", "#00FF41"],
        labels=dict(x="Month Index", y="Cohort", color="Retention"),
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font_color="#00FF41",
        margin=dict(l=0, r=0, t=30, b=0),
        height=450,
        dragmode=False,
    )
    fig.update_xaxes(side="top")

    return fig


# ═══════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════
SUMMARY_COLS = [
    "month",
    "brand",
    "negative_yield_players",
    "profitable_players",
    "flat",
    "total_players",
    "profitable_pct",
    "ggr",
    "ngr",
    "total_handle",
    "turnover_casino",
    "ggr_casino",
    "ngr_casino",
    "turnover_sports",
    "ggr_sports",
    "ngr_sports",
    "hold_pct",
    "ggr_per_player",
    "turnover_per_player",
    "top_10_pct_ggr_share",
    "new_players",
    "returning_players",
    "reactivated_players",
    "conversions",
    "retention_pct",
    "new_player_ggr",
    "returning_player_ggr",
    "revenue_share_deduction",
    "deposit_count",
    "deposits",
    "withdrawals",
    "net_deposits",
    "bonus_total",
    "bonus_casino",
    "bonus_sports",
    "tax_total",
]


def _safe_pct(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Compute (numerator / denominator) * 100, returning 0.0 where
    the denominator is zero to avoid ``ZeroDivisionError``."""
    return numerator.div(denominator).fillna(0.0).mul(100).round(6)


def _empty_summary() -> pd.DataFrame:
    """Return an empty DataFrame with the correct summary columns."""
    return pd.DataFrame(columns=SUMMARY_COLS)


# ═══════════════════════════════════════════════════════════════════════════
#  Campaign Analytics (Phase 5)
# ═══════════════════════════════════════════════════════════════════════════
CAMPAIGN_SUMMARY_COLS = [
    "month",
    "brand",
    "total_records",
    "total_kpi1",
    "total_kpi2",
    "total_calls",
    "total_emails",
    "total_sms",
    "kpi1_conversion_rate",
    "kpi2_login_rate",
]


def generate_campaign_summaries(df: pd.DataFrame) -> pd.DataFrame:
    """Take the raw CampaignRecord DataFrame and produce a summarised
    DataFrame matching the ``CampaignSummary`` entity.

    **CRITICAL — Campaign Duplication Scrub (§4):**
    Before aggregating, any row where ``campaign_type`` contains or equals
    "LI" has its ``records`` and ``kpi2_logins`` set to **0** to prevent
    double-counting.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain CampaignRecord columns: ``brand``, ``campaign_type``,
        ``records``, ``kpi1_conversions``, ``kpi2_logins``, ``calls``,
        ``emails_sent``, ``sms_sent``, ``report_month``.

    Returns
    -------
    pd.DataFrame
        One row per brand × month with aggregated campaign metrics.
    """
    if df.empty:
        logger.warning("Empty campaign DataFrame — returning empty summary")
        return pd.DataFrame(columns=CAMPAIGN_SUMMARY_COLS)

    required = {
        "brand", "campaign_type", "records", "kpi1_conversions",
        "kpi2_logins", "calls", "emails_sent", "sms_sent", "report_month",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Campaign DataFrame missing required columns: {missing}")

    # Work on a copy to avoid mutating the input
    cdf = df.copy()

    # ── §4 Campaign Duplication Scrub ──────────────────────────────────
    # If campaign_type contains "LI", zero out records and kpi2_logins
    li_mask = cdf["campaign_type"].str.upper().str.strip().eq("LI")
    scrubbed_count = li_mask.sum()

    if scrubbed_count > 0:
        cdf.loc[li_mask, "records"] = 0
        cdf.loc[li_mask, "kpi2_logins"] = 0
        logger.info(
            "Campaign LI scrub: zeroed records & kpi2_logins on %d rows",
            scrubbed_count,
        )

    # ── Aggregate by brand × month ────────────────────────────────────
    summary = (
        cdf.groupby(["brand", "report_month"], sort=False)
        .agg(
            total_records=("records", "sum"),
            total_kpi1=("kpi1_conversions", "sum"),
            total_kpi2=("kpi2_logins", "sum"),
            total_calls=("calls", "sum"),
            total_emails=("emails_sent", "sum"),
            total_sms=("sms_sent", "sum"),
        )
        .reset_index()
        .rename(columns={"report_month": "month"})
        .sort_values(["brand", "month"])
        .reset_index(drop=True)
    )

    summary = summary[[c for c in CAMPAIGN_SUMMARY_COLS if c not in ("kpi1_conversion_rate", "kpi2_login_rate")]]

    # ── Phase 8: Campaign efficiency rates ───────────────────────────
    summary["kpi1_conversion_rate"] = _safe_pct(
        summary["total_kpi1"], summary["total_records"]
    )
    summary["kpi2_login_rate"] = _safe_pct(
        summary["total_kpi2"], summary["total_records"]
    )

    summary = summary[CAMPAIGN_SUMMARY_COLS]

    # ── Phase 9: Combined campaign entity ─────────────────────────────
    _ADDITIVE_CAMPAIGN_COLS = [
        "total_records", "total_kpi1", "total_kpi2",
        "total_calls", "total_emails", "total_sms",
    ]
    combined = (
        summary.groupby("month", sort=False)[_ADDITIVE_CAMPAIGN_COLS]
        .sum()
        .reset_index()
    )
    combined["brand"] = "Combined"
    combined["kpi1_conversion_rate"] = _safe_pct(
        combined["total_kpi1"], combined["total_records"]
    )
    combined["kpi2_login_rate"] = _safe_pct(
        combined["total_kpi2"], combined["total_records"]
    )
    combined = combined[CAMPAIGN_SUMMARY_COLS]
    summary = pd.concat([summary, combined], ignore_index=True)

    logger.info(
        "Generated %d campaign summaries (%d brands × %d months)",
        len(summary),
        summary["brand"].nunique(),
        summary["month"].nunique(),
    )
    return summary


# ═══════════════════════════════════════════════════════════════════════════
#  Segmentation Matrix (Phase 8)
# ═══════════════════════════════════════════════════════════════════════════
SEGMENTATION_COLS = [
    "month",
    "brand",
    "wb_tag",
    "total_players",
    "ggr",
]


def generate_segmentation_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Group raw player data by brand × month × wb_tag to produce a
    segmentation matrix showing player count and GGR per segment.

    Parameters
    ----------
    df : pd.DataFrame
        Raw ``PlayerRecord`` DataFrame with ``id``, ``brand``,
        ``report_month``, ``wb_tag``, ``revenue``.

    Returns
    -------
    pd.DataFrame
        One row per brand × month × wb_tag.
    """
    if df.empty:
        logger.warning("Empty DataFrame — returning empty segmentation")
        return pd.DataFrame(columns=SEGMENTATION_COLS)

    required = {"id", "brand", "report_month", "wb_tag", "revenue"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")

    seg = (
        df.groupby(["brand", "report_month", "wb_tag"], sort=False)
        .agg(
            total_players=("id", "count"),
            ggr=("revenue", "sum"),
        )
        .reset_index()
        .rename(columns={"report_month": "month"})
        .sort_values(["brand", "month", "wb_tag"])
        .reset_index(drop=True)
    )

    seg = seg[SEGMENTATION_COLS]

    logger.info(
        "Segmentation: %d rows (%d brands, %d segments)",
        len(seg),
        seg["brand"].nunique(),
        seg["wb_tag"].nunique(),
    )
    return seg


# ═══════════════════════════════════════════════════════════════════════════
#  Cohort Matrix (Phase 7)
# ═══════════════════════════════════════════════════════════════════════════
def generate_cohort_matrix(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build a cohort retention matrix per brand.

    **Algorithm (SPEC §4 – Cohort Matrix Logic):**

    1. For each brand, find each player's *Acquisition Month* — the
       earliest ``report_month`` they appear in.
    2. For every subsequent month the player is active, compute the
       *relative month offset* (Month 1 = 1 month after acquisition, etc.).
    3. Group by Acquisition Month and offset.  Retention % for offset N
       = (unique players active at offset N) / (cohort size) × 100.

    Parameters
    ----------
    df : pd.DataFrame
        Raw ``PlayerRecord`` DataFrame with ``id``, ``brand``, ``report_month``.

    Returns
    -------
    dict[str, pd.DataFrame]
        Keys = brand names.  Values = DataFrames with ``Acquisition Month``
        as index and ``Month 1``, ``Month 2``, … as columns (retention %).
    """
    if df.empty:
        logger.warning("Empty DataFrame — returning empty cohort matrices")
        return {}

    result: dict[str, pd.DataFrame] = {}

    for brand, bdf in df.groupby("brand"):
        # 1. Find acquisition month per player (earliest month)
        acq = (
            bdf.groupby("id")["report_month"]
            .min()
            .reset_index()
            .rename(columns={"report_month": "acq_month"})
        )

        # 2. Merge acquisition month back into activity data
        merged = bdf[["id", "report_month"]].drop_duplicates().merge(acq, on="id")

        # 3. Build a sorted month list and compute offset
        sorted_months = sorted(merged["report_month"].unique())
        month_idx = {m: i for i, m in enumerate(sorted_months)}

        merged["acq_idx"] = merged["acq_month"].map(month_idx)
        merged["act_idx"] = merged["report_month"].map(month_idx)
        merged["offset"] = merged["act_idx"] - merged["acq_idx"]

        # Drop offset 0 (the acquisition month itself — that's always 100%)
        subsequent = merged[merged["offset"] > 0]

        # 4. Cohort sizes
        cohort_sizes = acq.groupby("acq_month")["id"].nunique()

        if subsequent.empty:
            # Only one month of data — no retention to compute
            matrix = pd.DataFrame(index=sorted(cohort_sizes.index))
            matrix.index.name = "Acquisition Month"
            result[str(brand)] = matrix
            continue

        # 5. Count active players per (acq_month, offset)
        active = (
            subsequent.groupby(["acq_month", "offset"])["id"]
            .nunique()
            .reset_index()
            .rename(columns={"id": "active_players"})
        )

        # 6. Calculate retention %
        active["cohort_size"] = active["acq_month"].map(cohort_sizes)
        active["retention_pct"] = (
            active["active_players"] / active["cohort_size"] * 100
        ).round(2)

        # 7. Pivot into matrix form
        matrix = active.pivot(
            index="acq_month",
            columns="offset",
            values="retention_pct",
        )

        # Rename columns to "Month 1", "Month 2", etc.
        matrix.columns = [f"Month {int(c)}" for c in matrix.columns]
        matrix.index.name = "Acquisition Month"
        matrix = matrix.sort_index()

        result[str(brand)] = matrix

        logger.info(
            "Cohort matrix for %s: %d cohorts × %d months",
            brand,
            len(matrix),
            len(matrix.columns),
        )

    # ── Combined cohort matrix (Phase 9) ─────────────────────────────────
    # Treat all players across brands as a single pool
    acq_all = (
        df.groupby("id")["report_month"]
        .min()
        .reset_index()
        .rename(columns={"report_month": "acq_month"})
    )
    merged_all = df[["id", "report_month"]].drop_duplicates().merge(acq_all, on="id")

    sorted_months_all = sorted(merged_all["report_month"].unique())
    month_idx_all = {m: i for i, m in enumerate(sorted_months_all)}

    merged_all["acq_idx"] = merged_all["acq_month"].map(month_idx_all)
    merged_all["act_idx"] = merged_all["report_month"].map(month_idx_all)
    merged_all["offset"] = merged_all["act_idx"] - merged_all["acq_idx"]

    subsequent_all = merged_all[merged_all["offset"] > 0]
    cohort_sizes_all = acq_all.groupby("acq_month")["id"].nunique()

    if not subsequent_all.empty:
        active_all = (
            subsequent_all.groupby(["acq_month", "offset"])["id"]
            .nunique()
            .reset_index()
            .rename(columns={"id": "active_players"})
        )
        active_all["cohort_size"] = active_all["acq_month"].map(cohort_sizes_all)
        active_all["retention_pct"] = (
            active_all["active_players"] / active_all["cohort_size"] * 100
        ).round(2)

        matrix_all = active_all.pivot(
            index="acq_month", columns="offset", values="retention_pct"
        )
        matrix_all.columns = [f"Month {int(c)}" for c in matrix_all.columns]
        matrix_all.index.name = "Acquisition Month"
        matrix_all = matrix_all.sort_index()
    else:
        matrix_all = pd.DataFrame(index=sorted(cohort_sizes_all.index))
        matrix_all.index.name = "Acquisition Month"

    result["Combined"] = matrix_all
    logger.info(
        "Cohort matrix for Combined: %d cohorts × %d months",
        len(matrix_all),
        len(matrix_all.columns),
    )

    return result

# ═══════════════════════════════════════════════════════════════════════════
#  Geographic Intelligence (Phase 9)
# ═══════════════════════════════════════════════════════════════════════════
def generate_geographic_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Group by country to evaluate market penetration and net profitability."""
    if df.empty or "country" not in df.columns:
        return pd.DataFrame()

    agg_dict = {
        "id": "nunique",
        "bet": "sum",
        "revenue": "sum",
        "ngr": "sum"
    }
    
    # Safely include deposits if the dataset has cash flow mapping
    has_deposits = "deposits" in df.columns
    if has_deposits:
        agg_dict["deposits"] = "sum"

    geo = df.groupby("country", sort=False).agg(agg_dict).reset_index()
    
    rename_cols = {
        "id": "total_players",
        "bet": "turnover",
        "revenue": "ggr"
    }
    geo.rename(columns=rename_cols, inplace=True)

    if not has_deposits:
        geo["deposits"] = 0.0

    geo["margin"] = np.where(
        geo["turnover"] != 0,
        (geo["ggr"] / geo["turnover"]) * 100,
        0.0
    ).round(2)

    return geo.sort_values("ngr", ascending=False).reset_index(drop=True)

# ═══════════════════════════════════════════════════════════════════════════
#  Product Affinity Matrix (Phase 12)
# ═══════════════════════════════════════════════════════════════════════════
def generate_affinity_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Segment players into Omnichannel, Casino Only, or Sportsbook Only."""
    if df.empty:
        return pd.DataFrame(columns=["Affinity", "Players", "Total_NGR", "Avg_NGR_per_Player"])

    # Group by id to capture their total interaction with each vertical
    pl = df.groupby("id", sort=False).agg(
        bet_casino=("bet_casino", "sum"),
        bet_sports=("bet_sports", "sum"),
        ngr=("ngr", "sum")
    ).reset_index()

    # Apply strictly defined categorization logic
    def get_affinity(row):
        if row["bet_casino"] > 0 and row["bet_sports"] > 0:
            return "Omnichannel"
        elif row["bet_casino"] > 0:
            return "Casino Only"
        elif row["bet_sports"] > 0:
            return "Sportsbook Only"
        else:
            return "Inactive"

    pl["Affinity"] = pl.apply(get_affinity, axis=1)

    # Summarize segments
    summary = pl.groupby("Affinity", sort=False).agg(
        Players=("id", "count"),
        Total_NGR=("ngr", "sum")
    ).reset_index()

    summary["Avg_NGR_per_Player"] = np.where(
        summary["Players"] > 0,
        summary["Total_NGR"] / summary["Players"],
        0.0
    ).round(2)

    return summary.sort_values("Avg_NGR_per_Player", ascending=False).reset_index(drop=True)

# ═══════════════════════════════════════════════════════════════════════════
#  True Lifecycle & Reactivation Velocity (Phase 13)
# ═══════════════════════════════════════════════════════════════════════════
def generate_reactivation_velocity(df: pd.DataFrame) -> pd.DataFrame:
    """Measure the exact time delay between Campaign Start Date and Reactivation."""
    react_df = df[df["reactivation_days"].notna()].copy()
    if react_df.empty:
        return pd.DataFrame()

    def bucket_velocity(days):
        if days <= 1: return "1. Immediate (0-1 Days)"
        elif days <= 7: return "2. Fast (2-7 Days)"
        elif days <= 14: return "3. Warm (8-14 Days)"
        elif days <= 30: return "4. Slow (15-30 Days)"
        else: return "5. Delayed (31+ Days)"

    react_df["Velocity"] = react_df["reactivation_days"].apply(bucket_velocity)

    summary = react_df.groupby("Velocity", sort=True).agg(
        Reactivated_Players=("id", "nunique"),
        Total_Deposits=("deposits", "sum"),
        Total_NGR=("ngr", "sum")
    ).reset_index().sort_values("Velocity")

    return summary

# ═══════════════════════════════════════════════════════════════════════════
#  Whale Concentration Matrix (Phase 14)
# ═══════════════════════════════════════════════════════════════════════════
def generate_pareto_distribution(df: pd.DataFrame) -> pd.DataFrame:
    # Group by player to get total NGR generated in this dataset slice
    pl = df.groupby("id").agg({"ngr": "sum"}).reset_index()
    
    # Isolate profitable players to calculate true revenue concentration
    pl = pl[pl["ngr"] > 0].sort_values("ngr", ascending=False)
    
    if pl.empty: 
        return pd.DataFrame()
        
    total_players = len(pl)
    total_ngr = pl["ngr"].sum()
    
    # Index thresholds
    top_1_idx = max(1, int(total_players * 0.01))
    top_5_idx = max(top_1_idx + 1, int(total_players * 0.05))
    top_20_idx = max(top_5_idx + 1, int(total_players * 0.20))
    
    # Summing the NGR for each bracket
    top_1_ngr = pl.iloc[:top_1_idx]["ngr"].sum()
    top_5_ngr = pl.iloc[top_1_idx:top_5_idx]["ngr"].sum()
    top_20_ngr = pl.iloc[top_5_idx:top_20_idx]["ngr"].sum()
    bottom_80_ngr = pl.iloc[top_20_idx:]["ngr"].sum()
    
    data = [
        {"Tier": "Top 1% (Super Whales)", "Player_Count": top_1_idx, "NGR_Generated": top_1_ngr, "Revenue_Share": (top_1_ngr / total_ngr) * 100},
        {"Tier": "Next 4% (Core VIPs)", "Player_Count": top_5_idx - top_1_idx, "NGR_Generated": top_5_ngr, "Revenue_Share": (top_5_ngr / total_ngr) * 100},
        {"Tier": "Next 15% (Mid-Tier)", "Player_Count": top_20_idx - top_5_idx, "NGR_Generated": top_20_ngr, "Revenue_Share": (top_20_ngr / total_ngr) * 100},
        {"Tier": "Bottom 80% (Casuals)", "Player_Count": total_players - top_20_idx, "NGR_Generated": bottom_80_ngr, "Revenue_Share": (bottom_80_ngr / total_ngr) * 100},
    ]
    return pd.DataFrame(data)

# ═══════════════════════════════════════════════════════════════════════════
#  Early-Warning VIP Churn Radar (Phase 17)
# ═══════════════════════════════════════════════════════════════════════════
def generate_vip_churn_radar(df: pd.DataFrame) -> pd.DataFrame:
    if "month" not in df.columns or df["month"].nunique() < 2:
        return pd.DataFrame()
        
    # Get the last two months available in the current slice
    sorted_months = sorted(df["month"].unique())
    latest_month = sorted_months[-1]
    prev_month = sorted_months[-2]
    
    # Aggregate player stats strictly for these two months
    monthly_pl = df[df["month"].isin([latest_month, prev_month])].groupby(["id", "brand", "month"]).agg(
        Turnover=("bet", "sum"),
        NGR=("ngr", "sum")
    ).reset_index()
    
    # Pivot to compare Prev vs Latest side-by-side
    pivot_df = monthly_pl.pivot_table(index=["id", "brand"], columns="month", values=["Turnover", "NGR"], fill_value=0).reset_index()
    pivot_df.columns = ["_".join(col).strip("_") if type(col) is tuple else col for col in pivot_df.columns]
    
    prev_ngr_col, curr_ngr_col = f"NGR_{prev_month}", f"NGR_{latest_month}"
    prev_turn_col = f"Turnover_{prev_month}"
    
    if prev_ngr_col not in pivot_df.columns or curr_ngr_col not in pivot_df.columns:
        return pd.DataFrame()
        
    # Define VIP: Generated >= $500 NGR or >= $5000 Turnover in the PREVIOUS month
    vips = pivot_df[(pivot_df[prev_ngr_col] >= 500) | (pivot_df[prev_turn_col] >= 5000)].copy()
    
    # Calculate the drop
    vips["NGR_Drop_Value"] = vips[prev_ngr_col] - vips[curr_ngr_col]
    vips["NGR_Drop_Pct"] = vips.apply(lambda x: (x["NGR_Drop_Value"] / x[prev_ngr_col]) * 100 if x[prev_ngr_col] > 0 else 0, axis=1)
    
    # Flag Churn Risk: Dropped by >= 30% AND lost at least $200 in absolute NGR value
    churn_risk = vips[(vips["NGR_Drop_Pct"] >= 30) & (vips["NGR_Drop_Value"] >= 200)].copy()
    
    churn_risk.rename(columns={prev_ngr_col: "Prev_Month_NGR", curr_ngr_col: "Curr_Month_NGR"}, inplace=True)
    
    return churn_risk.sort_values("NGR_Drop_Value", ascending=False).head(50)

def generate_segment_roi_matrix(df: pd.DataFrame) -> pd.DataFrame:
    if "segment" not in df.columns:
        return pd.DataFrame()
        
    seg = df.groupby("segment").agg(
        Total_Players=("id", "nunique"),
        Total_Turnover=("bet", "sum"),
        Total_GGR=("revenue", "sum"),
        Total_NGR=("ngr", "sum"),
        Total_Bonus=("bonus_total", "sum") if "bonus_total" in df.columns else ("bet", "min")
    ).reset_index()
    
    # Fallback if bonus_total was missing/dummy
    if "bonus_total" not in df.columns:
        seg["Total_Bonus"] = 0.0
        
    seg["Avg_NGR_per_Player"] = seg.apply(lambda x: x["Total_NGR"] / x["Total_Players"] if x["Total_Players"] > 0 else 0, axis=1)
    seg["Margin_%"] = seg.apply(lambda x: (x["Total_GGR"] / x["Total_Turnover"] * 100) if x["Total_Turnover"] > 0 else 0, axis=1)
    
    return seg.sort_values("Total_NGR", ascending=False)
