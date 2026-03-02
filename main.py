"""
main.py – ETL Pipeline Entrypoint
===================================
Runs the ingestion pipeline and prints diagnostics.
Usage:  python main.py [--strict]
"""
from __future__ import annotations

import argparse
import logging
import sys

import pandas as pd

from src.ingestion import load_all_data, load_campaign_data, DATA_DIR
from src.analytics import generate_monthly_summaries, generate_campaign_summaries, generate_cohort_matrix, generate_segmentation_summary, generate_both_business_summary
from src.exporter import export_to_excel

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s %(name)s │ %(message)s",
)
logger = logging.getLogger("main")


def main() -> None:
    parser = argparse.ArgumentParser(description="Betting Financial Reports ETL")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Halt if any brand × month combination is missing.",
    )
    args = parser.parse_args()

    logger.info("═══ Phase 2: Data Ingestion ═══")

    try:
        df, registry = load_all_data(strict=args.strict)
    except RuntimeError as exc:
        logger.error(str(exc))
        sys.exit(1)

    if df.empty:
        logger.error("No data was loaded.  Check data/raw/ directory.")
        sys.exit(1)

    # ── Diagnostics ──────────────────────────────────────────────────────
    logger.info("Total rows loaded: %s", f"{len(df):,}")

    brand_counts = df.groupby("brand")["id"].count()
    logger.info("Rows per brand:")
    for brand, cnt in brand_counts.items():
        logger.info("  %-12s %s", brand, f"{cnt:,}")

    month_counts = df.groupby("report_month")["id"].count().sort_index()
    logger.info("Rows per month:")
    for month, cnt in month_counts.items():
        logger.info("  %-12s %s", month, f"{cnt:,}")

    # ── Registry summary ─────────────────────────────────────────────────
    missing = registry.missing_entries()
    if missing:
        logger.warning(
            "%d missing brand×month slot(s) flagged in registry.json:",
            len(missing),
        )
        for m in missing:
            logger.warning(
                "  %-12s %s", m["brand"].title(), m["report_month"]
            )
    else:
        logger.info("Registry: all brand×month slots COMPLETE ✓")

    logger.info("═══ Ingestion complete ═══")

    # ── Phase 3: Analytics ────────────────────────────────────────────────
    logger.info("═══ Phase 3: Core Analytics ═══")

    summary = generate_monthly_summaries(df)

    logger.info("Summary table (%d rows):", len(summary))
    # Pretty-print the full summary table
    with pd.option_context(
        "display.max_rows", None,
        "display.max_columns", None,
        "display.width", 200,
        "display.float_format", "{:.2f}".format,
    ):
        print("\n" + summary.to_string(index=False))

    logger.info("═══ Analytics complete ═══")

    # ── Phase 5: Campaign Pipeline ────────────────────────────────────────
    logger.info("═══ Phase 5: Campaign Extension ═══")

    campaign_raw = load_campaign_data()
    campaign_summary: pd.DataFrame | None = None

    if campaign_raw.empty:
        logger.info("No campaign data found — skipping campaign analytics.")
    else:
        campaign_summary = generate_campaign_summaries(campaign_raw)
        logger.info("Campaign summary (%d rows):", len(campaign_summary))
        with pd.option_context(
            "display.max_rows", None,
            "display.max_columns", None,
            "display.width", 200,
        ):
            print("\n" + campaign_summary.to_string(index=False))

    # ── Phase 7: Cohort Matrix ────────────────────────────────────────────
    logger.info("═══ Phase 7: Cohort Matrix ═══")
    cohort_matrices = generate_cohort_matrix(df)
    for brand, matrix in cohort_matrices.items():
        logger.info("%s: %d cohorts × %d retention months", brand, len(matrix), len(matrix.columns))

    # ── Phase 8: Segmentation ───────────────────────────────────────────
    logger.info("═══ Phase 8: Segmentation ═══")
    segmentation = generate_segmentation_summary(df)
    logger.info("Segmentation: %d rows (%d segments)", len(segmentation), segmentation["wb_tag"].nunique() if not segmentation.empty else 0)

    # ── Phase 9: Both Business Summary ───────────────────────────────────
    logger.info("═══ Phase 9: Both Business Summary ═══")
    both_business = generate_both_business_summary(summary)
    logger.info("Both Business: %d months", len(both_business))

    # ── Phase 4: Export ──────────────────────────────────────────────────
    logger.info("═══ Phase 4: Output Generation ═══")

    output_dir = DATA_DIR / "output"
    output_path = export_to_excel(
        summary, output_dir,
        campaign_df=campaign_summary,
        cohort_matrices=cohort_matrices,
        segmentation_df=segmentation,
        both_business_df=both_business,
    )

    logger.info("Report written → %s", output_path)
    logger.info("═══ Pipeline complete ═══")


if __name__ == "__main__":
    main()
