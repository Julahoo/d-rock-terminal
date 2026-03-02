"""
src/exporter.py – Report Generator (Phase 4 + Phase 5)
=======================================================
Exports the ``MonthlyBrandSummary`` DataFrame to a multi-tab Excel
workbook with one ``{Brand} Financial`` sheet per brand, and optionally
a ``Summary Campaigns`` tab with campaign KPI metrics.

Uses openpyxl for cell-level formatting:
  • Percentage columns → 0.00 % format
  • Currency / GGR columns → #,##0.00 number format
  • Header row styling (bold, coloured background)

Spec refs: §3-C, §2 MonthlyBrandSummary, §2 CampaignSummary, §4 Campaign Output.
"""
from __future__ import annotations

import calendar
import logging
from pathlib import Path

import pandas as pd
from openpyxl.styles import Alignment, Font, PatternFill, numbers
from openpyxl.utils import get_column_letter

logger = logging.getLogger(__name__)

# ── Output defaults ──────────────────────────────────────────────────────
OUTPUT_FILENAME = "Summary_Data_Auto.xlsx"

# Column display names for the sheet header row
COLUMN_HEADERS: list[str] = [
    "Month",
    "Brand",
    "Losers",
    "Winners",
    "Flat",
    "Total Players",
    "Winners %",
    "GGR",
    "GGR Per Player",
    "New Players",
    "Returning Players",
    "Retention %",
]

# Internal DataFrame column names (same order as COLUMN_HEADERS)
DF_COLS: list[str] = [
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

# Formatting specs  (0-based column index → openpyxl number format)
_PCT_FMT = "0.00%"
_NUM_FMT = "#,##0.00"
_INT_FMT = "#,##0"

# Column indices that need special formatting (0-based)
_FORMAT_MAP: dict[int, str] = {
    2:  _INT_FMT,   # losers
    3:  _INT_FMT,   # winners
    4:  _INT_FMT,   # flat
    5:  _INT_FMT,   # total_players
    6:  _PCT_FMT,   # winners_pct
    7:  _NUM_FMT,   # ggr
    8:  _NUM_FMT,   # ggr_per_player
    9:  _INT_FMT,   # new_players
    10: _INT_FMT,   # returning_players
    11: _PCT_FMT,   # retention_pct
}

# ── Campaign tab config ──────────────────────────────────────────────────
CAMPAIGN_HEADERS: list[str] = [
    "Month",
    "Brand",
    "Records",
    "KPI 1 - Conversions",
    "KPI 2 - Logins",
    "Calls",
    "Emails Sent",
    "SMS Sent",
]

CAMPAIGN_DF_COLS: list[str] = [
    "month",
    "brand",
    "total_records",
    "total_kpi1",
    "total_kpi2",
    "total_calls",
    "total_emails",
    "total_sms",
]

_CAMPAIGN_FORMAT_MAP: dict[int, str] = {
    2: _INT_FMT,  # total_records
    3: _INT_FMT,  # total_kpi1
    4: _INT_FMT,  # total_kpi2
    5: _INT_FMT,  # total_calls
    6: _INT_FMT,  # total_emails
    7: _INT_FMT,  # total_sms
}

# Header styling
_HEADER_FONT = Font(bold=True, color="FFFFFF", size=11)
_HEADER_FILL = PatternFill(start_color="2F5496", end_color="2F5496", fill_type="solid")
_HEADER_ALIGN = Alignment(horizontal="center", vertical="center", wrap_text=True)


# ═══════════════════════════════════════════════════════════════════════════
#  Public API
# ═══════════════════════════════════════════════════════════════════════════
def export_to_excel(
    summary_df: pd.DataFrame,
    output_dir: Path,
    campaign_df: pd.DataFrame | None = None,
) -> Path:
    """Write *summary_df* to a multi-tab Excel workbook.

    One sheet per brand, named ``{Brand} Financial``.  Data is sorted
    chronologically within each sheet.  If *campaign_df* is provided
    and non-empty, a ``Summary Campaigns`` tab is also written.

    Parameters
    ----------
    summary_df : pd.DataFrame
        The ``MonthlyBrandSummary`` DataFrame from Phase 3.
    output_dir : Path
        Directory to write the output file into (created if needed).
    campaign_df : pd.DataFrame | None
        Optional ``CampaignSummary`` DataFrame from Phase 5.

    Returns
    -------
    Path
        Absolute path to the generated Excel file.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / OUTPUT_FILENAME

    brands = sorted(summary_df["brand"].unique())

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # ── Financial tabs (one per brand) ────────────────────────────
        for brand in brands:
            sheet_name = f"{brand} Financial"
            brand_df = (
                summary_df[summary_df["brand"] == brand]
                .sort_values("month")
                .reset_index(drop=True)
            )

            display = _prepare_display_df(brand_df)

            display.to_excel(
                writer,
                sheet_name=sheet_name,
                index=False,
                startrow=1,
                header=False,
            )

            ws = writer.sheets[sheet_name]
            _write_header(ws, COLUMN_HEADERS)
            _apply_formatting(ws, num_data_rows=len(display), fmt_map=_FORMAT_MAP)
            _auto_column_widths(ws, COLUMN_HEADERS)

        # ── Campaign tab (Phase 5) ───────────────────────────────────
        if campaign_df is not None and not campaign_df.empty:
            _write_campaign_tab(writer, campaign_df)
            logger.info("Exported 'Summary Campaigns' tab (%d rows)", len(campaign_df))

        logger.info(
            "Exported %d brand(s) to %s",
            len(brands),
            output_path,
        )

    return output_path


# ═══════════════════════════════════════════════════════════════════════════
#  Internal helpers
# ═══════════════════════════════════════════════════════════════════════════
def _prepare_display_df(brand_df: pd.DataFrame) -> pd.DataFrame:
    """Build a display-ready copy with human-readable month names
    and percentage values divided by 100 (so openpyxl % format works).
    """
    out = brand_df[DF_COLS].copy()

    # Convert "YYYY-MM" → "Month YYYY" (e.g. "2024-08" → "August 2024")
    out["month"] = out["month"].apply(_pretty_month)

    # openpyxl percentage format expects 0–1 range, not 0–100
    out["winners_pct"] = out["winners_pct"] / 100.0
    out["retention_pct"] = out["retention_pct"] / 100.0

    return out


def _write_campaign_tab(writer, campaign_df: pd.DataFrame) -> None:  # noqa: ANN001
    """Write the Summary Campaigns tab to the workbook."""
    sheet_name = "Summary Campaigns"

    display = campaign_df[CAMPAIGN_DF_COLS].copy()
    display = display.sort_values(["month", "brand"]).reset_index(drop=True)
    display["month"] = display["month"].apply(_pretty_month)

    display.to_excel(
        writer,
        sheet_name=sheet_name,
        index=False,
        startrow=1,
        header=False,
    )

    ws = writer.sheets[sheet_name]
    _write_header(ws, CAMPAIGN_HEADERS)
    _apply_formatting(ws, num_data_rows=len(display), fmt_map=_CAMPAIGN_FORMAT_MAP)
    _auto_column_widths(ws, CAMPAIGN_HEADERS)


def _write_header(ws, headers: list[str]) -> None:  # noqa: ANN001
    """Write styled column headers to row 1."""
    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.font = _HEADER_FONT
        cell.fill = _HEADER_FILL
        cell.alignment = _HEADER_ALIGN


def _apply_formatting(  # noqa: ANN001
    ws, num_data_rows: int, fmt_map: dict[int, str]
) -> None:
    """Apply number formats to data cells (rows 2 … N+1)."""
    for row in range(2, num_data_rows + 2):
        for col_0, fmt in fmt_map.items():
            cell = ws.cell(row=row, column=col_0 + 1)
            cell.number_format = fmt
            cell.alignment = Alignment(horizontal="right")


def _auto_column_widths(ws, headers: list[str]) -> None:  # noqa: ANN001
    """Set column widths based on header length (with a reasonable min)."""
    for col_idx, header in enumerate(headers, start=1):
        col_letter = get_column_letter(col_idx)
        width = max(len(header) + 4, 14)
        ws.column_dimensions[col_letter].width = width


def _pretty_month(ym: str) -> str:
    """Convert 'YYYY-MM' → 'Month YYYY'."""
    y, m = (int(x) for x in ym.split("-"))
    return f"{calendar.month_name[m]} {y}"

