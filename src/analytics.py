"""
src/analytics.py – Aggregation Engine (Phase 3)
=================================================
Financial Calculator and Cohort Analyzer.

* Groups ingested data by brand × month.
* Calculates Winners / Losers / Flat counts, GGR, GGR-per-player.
* Tracks player cohorts (New vs Returning) with a stateful
  ``seen_ids`` set per brand, processed in chronological order.

Spec refs: §2 MonthlyBrandSummary, §3-B, §4 Cohort Tracking.
"""
from __future__ import annotations

import logging
from typing import Any

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

    required = {"id", "brand", "revenue", "report_month"}
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

    # Ensure clean column order matching the spec
    summary = summary[SUMMARY_COLS]

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
        losers=("revenue", lambda s: (s < 0).sum()),
        winners=("revenue", lambda s: (s > 0).sum()),
        flat=("revenue", lambda s: (s == 0).sum()),
        total_players=("id", "count"),
        ggr=("revenue", "sum"),
    ).reset_index()

    agg.rename(columns={"report_month": "month"}, inplace=True)

    # winners_pct = (winners / total_players) * 100
    agg["winners_pct"] = _safe_pct(agg["winners"], agg["total_players"])

    # ggr_per_player = ggr / total_players
    agg["ggr_per_player"] = agg.apply(
        lambda r: round(r["ggr"] / r["total_players"], 6)
        if r["total_players"] > 0
        else 0.0,
        axis=1,
    )

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

    for month in sorted_months:
        month_df = df[df["report_month"] == month]

        for brand in brands:
            brand_month = month_df[month_df["brand"] == brand]
            if brand_month.empty:
                continue

            current_ids = set(brand_month["id"].unique())
            previously_seen = seen_ids[brand]

            returning = current_ids & previously_seen
            new = current_ids - previously_seen

            records.append(
                {
                    "brand": brand,
                    "month": month,
                    "new_players": len(new),
                    "returning_players": len(returning),
                }
            )

            # Update seen set for subsequent months
            seen_ids[brand].update(current_ids)

    return pd.DataFrame(records)


# ═══════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════
SUMMARY_COLS = [
    "month",
    "brand",
    "losers",
    "winners",
    "flat",
    "total_players",
    "winners_pct",
    "ggr",
    "ggr_per_player",
    "new_players",
    "returning_players",
    "retention_pct",
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

    summary = summary[CAMPAIGN_SUMMARY_COLS]

    logger.info(
        "Generated %d campaign summaries (%d brands × %d months)",
        len(summary),
        summary["brand"].nunique(),
        summary["month"].nunique(),
    )
    return summary
