"""
src/ingestion.py – Data Ingestion Module (Phase 2)
===================================================
Reads CSV files from brand-specific directories under data/raw/,
normalises them into a unified PlayerRecord DataFrame, and maintains
an IngestionRegistry (registry.json) that cross-references expected
months across all brands and flags any gaps.

Spec refs: §2 PlayerRecord, §2 IngestionRegistry, §3-A, §4 Ingestion Validation.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ── paths ----------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
RAW_DIR      = DATA_DIR / "raw"
CAMPAIGNS_DIR = DATA_DIR / "campaigns"
REGISTRY_PATH = DATA_DIR / "registry.json"

# ── column mapping (raw CSV header → spec field) -------------------------
COL_MAP: dict[str, str] = {
    "Player unique identifier": "id",
    "Brand":                    "brand",
    "WB tag/flag":              "wb_tag",
    "Bet":                      "bet",
    "Win":                      "win",
    "Revenue":                  "revenue",
}

NUMERIC_COLS = ["bet", "win", "revenue"]

# ── campaign column mapping ──────────────────────────────────────────────
CAMPAIGN_COL_MAP: dict[str, str] = {
    "Brand":              "brand",
    "Campaign":           "campaign_type",
    "Records":            "records",
    "KPI 1 - Conversions": "kpi1_conversions",
    "KPI 2 - Logins":     "kpi2_logins",
    "Calls":              "calls",
    "ES":                 "emails_sent",
    "SS":                 "sms_sent",
}

CAMPAIGN_NUMERIC_COLS = [
    "records", "kpi1_conversions", "kpi2_logins",
    "calls", "emails_sent", "sms_sent",
]

# ── filename pattern ─────────────────────────────────────────────────────
#  e.g. latribet_2025_08.csv  →  brand="latribet", year=2025, month=8
FILE_RE = re.compile(
    r"^(?P<brand>[a-z]+)_(?P<year>\d{4})_(?P<month>\d{2})\.csv$",
    re.IGNORECASE,
)


# ═══════════════════════════════════════════════════════════════════════════
#  Registry
# ═══════════════════════════════════════════════════════════════════════════
class IngestionRegistry:
    """Tracks which brand × month combinations have been ingested and
    flags any gaps in the expected month range.

    Persisted as ``data/registry.json``.
    """

    def __init__(self) -> None:
        # brand → { "YYYY-MM": { status, file_path, ingested_at } }
        self._entries: dict[str, dict[str, dict]] = {}

    # ── mutators ----------------------------------------------------------
    def mark_complete(
        self,
        brand: str,
        report_month: str,
        file_path: str,
    ) -> None:
        """Record a successfully-ingested CSV."""
        bucket = self._entries.setdefault(brand, {})
        bucket[report_month] = {
            "status":      "COMPLETE",
            "file_path":   file_path,
            "ingested_at": datetime.now().isoformat(timespec="seconds"),
        }

    def mark_missing(self, brand: str, report_month: str) -> None:
        """Flag an expected month that has no corresponding CSV."""
        bucket = self._entries.setdefault(brand, {})
        if report_month not in bucket:
            bucket[report_month] = {
                "status":      "MISSING",
                "file_path":   None,
                "ingested_at": None,
            }

    # ── queries -----------------------------------------------------------
    def missing_entries(self) -> list[dict[str, str]]:
        """Return a list of ``{brand, report_month}`` dicts for all MISSING
        slots."""
        out: list[dict[str, str]] = []
        for brand, months in sorted(self._entries.items()):
            for rm, info in sorted(months.items()):
                if info["status"] == "MISSING":
                    out.append({"brand": brand, "report_month": rm})
        return out

    # ── cross-check -------------------------------------------------------
    def evaluate_gaps(self) -> list[dict[str, str]]:
        """§4 Ingestion Validation: compute the global min/max month across
        ALL brands. For every month in that range, every brand must have a
        file.  Missing slots are marked and returned.
        """
        all_months: set[str] = set()
        for months in self._entries.values():
            all_months.update(months.keys())

        if not all_months:
            return []

        sorted_months = sorted(all_months)
        full_range = _month_range(sorted_months[0], sorted_months[-1])
        brands = sorted(self._entries.keys())

        gaps: list[dict[str, str]] = []
        for brand in brands:
            for rm in full_range:
                if rm not in self._entries.get(brand, {}):
                    self.mark_missing(brand, rm)
                    gaps.append({"brand": brand, "report_month": rm})
        return gaps

    # ── persistence -------------------------------------------------------
    def save(self, path: Optional[Path] = None) -> None:
        target = path or REGISTRY_PATH
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "w", encoding="utf-8") as fh:
            json.dump(self._entries, fh, indent=2)
        logger.info("Registry saved → %s", target)

    @classmethod
    def load(cls, path: Optional[Path] = None) -> "IngestionRegistry":
        target = path or REGISTRY_PATH
        reg = cls()
        if target.exists():
            with open(target, encoding="utf-8") as fh:
                reg._entries = json.load(fh)
        return reg


# ═══════════════════════════════════════════════════════════════════════════
#  Core ingestion
# ═══════════════════════════════════════════════════════════════════════════
def load_all_data(
    *,
    raw_dir: Path = RAW_DIR,
    strict: bool = False,
) -> tuple[pd.DataFrame, IngestionRegistry]:
    """Read every CSV from every brand sub-directory under *raw_dir*,
    normalise the columns, and return a unified DataFrame together with
    a populated :class:`IngestionRegistry`.

    Parameters
    ----------
    raw_dir : Path
        Root directory containing brand folders (e.g. ``data/raw/``).
    strict : bool
        If *True*, raise ``RuntimeError`` when the registry detects
        missing months.  Otherwise just log warnings.

    Returns
    -------
    (DataFrame, IngestionRegistry)
    """
    registry = IngestionRegistry()
    frames: list[pd.DataFrame] = []

    brand_dirs = sorted(
        d for d in raw_dir.iterdir() if d.is_dir()
    )

    if not brand_dirs:
        logger.warning("No brand directories found in %s", raw_dir)
        return pd.DataFrame(), registry

    for brand_dir in brand_dirs:
        csv_files = sorted(brand_dir.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSVs in %s", brand_dir)
            continue

        for csv_path in csv_files:
            parsed = _parse_filename(csv_path.name)
            if parsed is None:
                logger.warning("Skipping unrecognised file: %s", csv_path.name)
                continue

            brand_key, report_month = parsed

            df = _read_and_clean(csv_path)
            if df is None or df.empty:
                logger.warning("Empty or unreadable: %s", csv_path)
                continue

            df["report_month"] = report_month
            frames.append(df)

            registry.mark_complete(
                brand=brand_key,
                report_month=report_month,
                file_path=str(csv_path.relative_to(raw_dir)),
            )

    if not frames:
        logger.warning("No data loaded.")
        return pd.DataFrame(), registry

    unified = pd.concat(frames, ignore_index=True)

    # § 4 – Ingestion Validation: cross-check all brands × months
    gaps = registry.evaluate_gaps()
    for gap in gaps:
        msg = (
            f"WARNING: Missing {_pretty_month(gap['report_month'])} "
            f"data for {gap['brand'].title()}"
        )
        logger.warning(msg)
        print(msg)

    if strict and gaps:
        raise RuntimeError(
            f"Strict mode: {len(gaps)} missing month(s) detected. "
            "Aborting. Set strict=False to continue with warnings."
        )

    # Persist registry
    registry.save()

    return unified, registry


# ═══════════════════════════════════════════════════════════════════════════
#  Campaign ingestion (Phase 5)
# ═══════════════════════════════════════════════════════════════════════════
def load_campaign_data(
    *,
    campaigns_dir: Path = CAMPAIGNS_DIR,
) -> pd.DataFrame:
    """Read campaign CSVs from brand sub-directories under *campaigns_dir*
    and return a unified DataFrame matching the ``CampaignRecord`` entity.

    Returns an **empty** DataFrame (with correct columns) if the directory
    does not exist or contains no CSVs.

    Parameters
    ----------
    campaigns_dir : Path
        Root directory containing brand folders (e.g. ``data/campaigns/``).

    Returns
    -------
    pd.DataFrame
    """
    empty = pd.DataFrame(columns=list(CAMPAIGN_COL_MAP.values()) + ["report_month"])

    if not campaigns_dir.exists():
        logger.info("Campaign directory %s not found — skipping.", campaigns_dir)
        return empty

    brand_dirs = sorted(d for d in campaigns_dir.iterdir() if d.is_dir())
    if not brand_dirs:
        logger.info("No brand folders in %s — skipping campaigns.", campaigns_dir)
        return empty

    frames: list[pd.DataFrame] = []

    for brand_dir in brand_dirs:
        csv_files = sorted(brand_dir.glob("*.csv"))
        if not csv_files:
            logger.info("No campaign CSVs in %s", brand_dir)
            continue

        for csv_path in csv_files:
            parsed = _parse_filename(csv_path.name)
            if parsed is None:
                logger.warning("Skipping unrecognised campaign file: %s", csv_path.name)
                continue

            _, report_month = parsed

            try:
                df = pd.read_csv(csv_path)
            except Exception:
                logger.exception("Failed to read campaign file %s", csv_path)
                continue

            # Check required columns exist (flexible: try common header variants)
            df = _normalise_campaign_columns(df)
            if df is None:
                logger.warning(
                    "Campaign file %s missing required columns — skipping",
                    csv_path.name,
                )
                continue

            # Coerce numerics
            for col in CAMPAIGN_NUMERIC_COLS:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

            # Normalise brand casing
            df["brand"] = df["brand"].str.strip().str.title()

            df["report_month"] = report_month
            frames.append(df)

    if not frames:
        logger.info("No campaign data loaded.")
        return empty

    unified = pd.concat(frames, ignore_index=True)
    logger.info("Loaded %d campaign rows from %s", len(unified), campaigns_dir)
    return unified


def _normalise_campaign_columns(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Try to map raw campaign CSV columns to spec names.
    Returns None if required columns cannot be found."""
    # Try exact mapping first
    if all(c in df.columns for c in CAMPAIGN_COL_MAP):
        return df.rename(columns=CAMPAIGN_COL_MAP)[list(CAMPAIGN_COL_MAP.values())]

    # Fallback: case-insensitive column matching
    lower_map = {k.lower().strip(): v for k, v in CAMPAIGN_COL_MAP.items()}
    rename = {}
    for col in df.columns:
        normalised = col.lower().strip()
        if normalised in lower_map:
            rename[col] = lower_map[normalised]

    if len(rename) < len(CAMPAIGN_COL_MAP):
        return None

    return df.rename(columns=rename)[list(CAMPAIGN_COL_MAP.values())]


# ═══════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════
def _parse_filename(filename: str) -> Optional[tuple[str, str]]:
    """Extract (brand_key, 'YYYY-MM') from a filename like
    ``latribet_2025_08.csv``."""
    m = FILE_RE.match(filename)
    if not m:
        return None
    brand = m.group("brand").lower()
    year  = m.group("year")
    month = m.group("month")
    return brand, f"{year}-{month}"


def _read_and_clean(csv_path: Path) -> Optional[pd.DataFrame]:
    """Read a single CSV, rename columns to spec names, coerce numerics,
    and drop rows with null player id."""
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        logger.exception("Failed to read %s", csv_path)
        return None

    # Rename
    missing_cols = [c for c in COL_MAP if c not in df.columns]
    if missing_cols:
        logger.warning(
            "%s is missing columns %s – skipping", csv_path.name, missing_cols
        )
        return None

    df = df.rename(columns=COL_MAP)[list(COL_MAP.values())]

    # Coerce numerics
    for col in NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows where player id is null
    df = df.dropna(subset=["id"])

    # Normalise brand casing
    df["brand"] = df["brand"].str.strip().str.title()

    return df


def _month_range(start: str, end: str) -> list[str]:
    """Generate a list of 'YYYY-MM' strings from *start* to *end*
    inclusive."""
    sy, sm = (int(x) for x in start.split("-"))
    ey, em = (int(x) for x in end.split("-"))
    months: list[str] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append(f"{y}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def _pretty_month(ym: str) -> str:
    """Convert 'YYYY-MM' to a human-readable string like 'August 2025'."""
    import calendar
    y, m = (int(x) for x in ym.split("-"))
    return f"{calendar.month_name[m]} {y}"


# ═══════════════════════════════════════════════════════════════════════════
#  In-memory ingestion (concurrent-safe — no disk writes)
# ═══════════════════════════════════════════════════════════════════════════

def load_all_data_from_uploads(
    files: list,
) -> tuple[pd.DataFrame, IngestionRegistry]:
    """Read player CSVs from Streamlit UploadedFile objects in-memory.

    Parameters
    ----------
    files : list
        Flat list of UploadedFile objects (all brands mixed together).

    Returns
    -------
    (DataFrame, IngestionRegistry)
    """
    registry = IngestionRegistry()
    frames: list[pd.DataFrame] = []

    for f in files:
        parsed = _parse_filename(f.name)
        if parsed is None:
            logger.warning("Skipping unrecognised file: %s", f.name)
            continue

        brand_key, report_month = parsed

        try:
            raw = pd.read_csv(f)
        except Exception:
            logger.exception("Failed to read %s", f.name)
            continue

        # Normalise columns
        missing_cols = [c for c in COL_MAP if c not in raw.columns]
        if missing_cols:
            logger.warning("%s is missing columns %s – skipping", f.name, missing_cols)
            continue

        df = raw.rename(columns=COL_MAP)[list(COL_MAP.values())]
        for col in NUMERIC_COLS:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["id"])
        df["brand"] = df["brand"].str.strip().str.title()

        if df.empty:
            logger.warning("Empty after cleaning: %s", f.name)
            continue

        df["report_month"] = report_month
        frames.append(df)
        registry.mark_complete(brand=brand_key, report_month=report_month, file_path=f.name)

    if not frames:
        logger.warning("No data loaded from uploads.")
        return pd.DataFrame(), registry

    unified = pd.concat(frames, ignore_index=True)

    gaps = registry.evaluate_gaps()
    for gap in gaps:
        logger.warning("Missing %s data for %s", _pretty_month(gap["report_month"]), gap["brand"].title())

    return unified, registry


def load_campaign_data_from_uploads(files: list) -> pd.DataFrame:
    """Read campaign CSVs from Streamlit UploadedFile objects in-memory.

    Parameters
    ----------
    files : list
        Flat list of UploadedFile objects (all brands mixed together).

    Returns
    -------
    pd.DataFrame
    """
    empty = pd.DataFrame(columns=list(CAMPAIGN_COL_MAP.values()) + ["report_month"])

    if not files:
        return empty

    frames: list[pd.DataFrame] = []

    for f in files:
        parsed = _parse_filename(f.name)
        if parsed is None:
            logger.warning("Skipping unrecognised campaign file: %s", f.name)
            continue

        _, report_month = parsed

        try:
            df = pd.read_csv(f)
        except Exception:
            logger.exception("Failed to read campaign file %s", f.name)
            continue

        df = _normalise_campaign_columns(df)
        if df is None:
            logger.warning("Campaign file %s missing required columns — skipping", f.name)
            continue

        for col in CAMPAIGN_NUMERIC_COLS:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        df["brand"] = df["brand"].str.strip().str.title()
        df["report_month"] = report_month
        frames.append(df)

    if not frames:
        return empty

    unified = pd.concat(frames, ignore_index=True)
    logger.info("Loaded %d campaign rows from uploads", len(unified))
    return unified
