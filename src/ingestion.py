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
import numpy as np
from sqlalchemy import text
from src.database import engine as db_engine

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

NUMERIC_COLS = [
    "bet", "win", "revenue", "ngr",
    "bet_casino", "revenue_casino", "ngr_casino",
    "bet_sports", "revenue_sports", "ngr_sports",
    "deposit_count", "deposits", "withdrawals",
    "bonus_casino", "bonus_sports", "bonus_total"
]

# ── Operations & Telemarketing Mapping ────────────────────────────────
CLIENT_HIERARCHY = {
    # Map specific brand tags to their Enterprise Client Names
    'BAH': 'Reliato',
    'YW': 'Limitless',
    'VJ': 'Simplicity Malta Limited',
    'PP': 'PowerPlay',
    'RHN': 'Rhino',
    'INSP': 'Magico Games/Interspin',
    'PE': 'PressEnter',
    'LV': 'LeoVegas Group',
    'EX': 'LeoVegas Group',
    'RP': 'LeoVegas Group',
    'GG': 'LeoVegas Group',
    'BETMGM': 'LeoVegas Group',
    'BETUK': 'LeoVegas Group',
    'MRO': 'Limitless',
    'YU': 'Simplicity Malta Limited',
    'LTRB': 'Offside Gaming',
    'ROJA': 'Offside Gaming',
    'ROJB': 'Offside Gaming',
    'FIYYJB': 'Magico Games/Interspin',
    'CASINODAYS': 'Rhino',
    'WG': 'Reliato',
    'BHB': 'Limitless'
}

BRAND_CODE_MAP = {
    'VERA JOHN': 'VJ', 'YUUGADO': 'YU', 'BOABET': 'BOA', 'YOUWIN': 'YW', 
    'BAHIBI': 'BHB', 'MR OYUN': 'MRO', 'BAHIGO': 'BAH', 'WETTIGO': 'WG', 
    'HAPPY LUKE': 'HL', 'LIVE CASINO HOUSE': 'LCH', 'ROYAL PANDA': 'RP', 'ROYALPANDA': 'RP',
    'LEO VEGAS': 'LV', 'LATRIBET': 'LTRB', 'ROJABET': 'ROJA', 'POWERPLAY': 'PP',
    'WETIGO': 'WG', 'WETTIGO': 'WG', 
    'HAHIBI': 'BHB', 'BAHIBI': 'BHB',
    'NITROCASINO': 'PE', 'NITRO': 'PE',
    'EXPEKT': 'EX', 'CASINODAYS': 'CASINODAYS'
}

# ── LeoVegas Group column mapping (Smart Adapter) ────────────────────────
LEOVEGAS_COL_MAP: dict[str, str] = {
    "Player Key":          "id",
    "Brand":               "brand",
    "Segment":             "wb_tag",
    "Turnover (Total) €":  "bet",
    "GGR (Total) €":       "revenue",
    "NGR (Total) € after Tax": "ngr",
    "Country":             "country",
    "Turnover (Casino) €": "bet_casino",
    "GGR (Casino) €":      "revenue_casino",
    "NGR (Casino) €":      "ngr_casino",
    "Turnover (Sports) €": "bet_sports",
    "GGR (Sports) €":      "revenue_sports",
    "NGR (Sports) €":      "ngr_sports",
    "Deposit #":           "deposit_count",
    "Deposit €":           "deposits",
    "Withdrawal €":        "withdrawals",
    "Bonus Cost (Casino) €": "bonus_casino",
    "Bonus Cost (Sports) €": "bonus_sports",
    "Tax €":               "tax_total",
    "Reactivation Date":   "reactivation_date",
    "Campaign Start Date": "campaign_start_date",
}

LEOVEGAS_NUMERIC_RAW = [
    "Turnover (Total) €", "GGR (Total) €", "NGR (Total) € after Tax",
    "Turnover (Casino) €", "GGR (Casino) €", "NGR (Casino) €",
    "Turnover (Sports) €", "GGR (Sports) €", "NGR (Sports) €",
    "Deposit #", "Deposit €", "Withdrawal €",
    "Bonus Cost (Casino) €", "Bonus Cost (Sports) €", "Bonus Cost (Total)  €",
    "Tax €",
]

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
    r"^(?P<brand>[a-z]+)_(?P<year>\d{4})_(?P<month>\d{2})\.(?:csv|xlsx)$",
    re.IGNORECASE,
)

# ── Multi-sheet Excel pattern ────────────────────────────────────────────
#  e.g. sheet "2024-08 rojabet"  →  brand="rojabet", year=2024, month=08
SHEET_RE = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})\s+(?P<brand>.+)$",
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

    # Build client_mapping lookup for deterministic routing
    try:
        mapping_df = pd.read_sql("SELECT brand_code, brand_name, client_name, financial_format FROM client_mapping", db_engine)
        fin_map = {}
        for _, row in mapping_df.iterrows():
            val = {
                'client': row['client_name'],
                'brand': row['brand_name'] if pd.notna(row.get('brand_name')) else row['brand_code'],
                'format': row.get('financial_format', 'Standard')
            }
            if pd.notna(row.get('brand_name')): fin_map[str(row['brand_name']).strip().lower()] = val
            if pd.notna(row.get('brand_code')): fin_map[str(row['brand_code']).strip().lower()] = val
    except Exception:
        fin_map = {}

    # Build set of existing brand+month combos for duplicate prevention
    existing_combos = set()
    try:
        dup_df = pd.read_sql("SELECT DISTINCT brand, report_month FROM raw_financial_data", db_engine)
        for _, r in dup_df.iterrows():
            existing_combos.add((str(r['brand']).strip(), str(r['report_month']).strip()))
    except Exception:
        pass

    for brand_dir in brand_dirs:
        # ── Individual CSV files ──────────────────────────────────────
        csv_files = sorted(brand_dir.glob("*.csv"))
        for csv_path in csv_files:
            parsed = _parse_filename(csv_path.name)
            if parsed is None:
                logger.warning("Skipping unrecognised file: %s", csv_path.name)
                continue

            brand_key, report_month = parsed
            
            # Resolve brand/client/format from client_mapping
            mapped = fin_map.get(brand_key.strip().lower(), {})
            target_format = mapped.get('format', 'Standard')
            target_client = mapped.get('client', 'UNKNOWN')
            target_brand = mapped.get('brand', brand_key.title())
            if target_client == 'LeoVegas Group': target_format = 'LeoVegas'
            elif 'Offside' in target_client: target_format = 'Offside'

            # Duplicate protection: skip if brand+month already in DB
            if (target_brand, report_month) in existing_combos:
                logger.info("Skipping duplicate CSV: %s %s (already in DB)", target_brand, report_month)
                continue

            df = _read_and_clean(csv_path, target_format, target_client, target_brand)
            if df is None or df.empty:
                logger.warning("Empty or unreadable: %s", csv_path)
                continue

            df["report_month"] = report_month
            frames.append(df)
            registry.mark_complete(brand=brand_key, report_month=report_month, file_path=str(csv_path.relative_to(raw_dir)))

        # ── Multi-sheet Excel files ───────────────────────────────────
        xlsx_files = sorted(brand_dir.glob("*.xlsx"))
        for xlsx_path in xlsx_files:
            try:
                xl = pd.ExcelFile(xlsx_path, engine="openpyxl")
            except Exception:
                logger.exception("Failed to open Excel: %s", xlsx_path)
                continue

            for sheet_name in xl.sheet_names:
                sm = SHEET_RE.match(sheet_name.strip())
                if not sm:
                    logger.info("Skipping non-data sheet: '%s' in %s", sheet_name, xlsx_path.name)
                    continue

                year, month = sm.group("year"), sm.group("month")
                brand_key = sm.group("brand").strip().lower()
                report_month = f"{year}-{month}"

                # Resolve brand/client/format from client_mapping
                mapped = fin_map.get(brand_key, {})
                target_format = mapped.get('format', 'Standard')
                target_client = mapped.get('client', 'UNKNOWN')
                target_brand = mapped.get('brand', brand_key.title())
                if target_client == 'LeoVegas Group': target_format = 'LeoVegas'
                elif 'Offside' in target_client: target_format = 'Offside'

                # Duplicate protection: skip if brand+month already in DB
                if (target_brand, report_month) in existing_combos:
                    logger.info("Skipping duplicate: %s %s (already in DB)", target_brand, report_month)
                    continue

                try:
                    raw = xl.parse(sheet_name)
                except Exception:
                    logger.exception("Failed to read sheet '%s' in %s", sheet_name, xlsx_path.name)
                    continue

                df = _normalise_player_columns(raw, f"{xlsx_path.name}:{sheet_name}", target_format, target_client, target_brand)
                if df is None or df.empty:
                    logger.warning("Empty after cleaning: %s [%s]", xlsx_path.name, sheet_name)
                    continue

                df["report_month"] = report_month
                frames.append(df)
                existing_combos.add((target_brand, report_month))  # Prevent intra-file dupes
                registry.mark_complete(brand=brand_key, report_month=report_month, file_path=f"{xlsx_path.relative_to(raw_dir)}:{sheet_name}")

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

    if strict and gaps:
        raise RuntimeError(
            f"Strict mode: {len(gaps)} missing month(s) detected. "
            "Aborting. Set strict=False to continue with warnings."
        )

    # Persist registry
    registry.save()

    # --- PERMANENT DB SAVE FOR DISK-BASED FINANCIAL DATA ---
    if not unified.empty:
        try:
            db_df = unified.rename(columns={"id": "player_id"})
            expected_cols = ["player_id", "client", "brand", "country", "wb_tag", "segment", "bet", "revenue", "ngr", "bet_casino", "revenue_casino", "ngr_casino", "bet_sports", "revenue_sports", "ngr_sports", "deposit_count", "deposits", "withdrawals", "bonus_total", "bonus_casino", "bonus_sports", "tax_total", "report_month", "reactivation_date", "campaign_start_date", "reactivation_days"]
            db_df = db_df[[c for c in expected_cols if c in db_df.columns]]
            db_df.to_sql("raw_financial_data", db_engine, if_exists="append", index=False)
            logger.info("Persisted %d financial rows to raw_financial_data", len(db_df))
        except Exception as e:
            logger.exception("Failed to persist financial data to DB: %s", e)

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
    ``latribet_2025_08.csv``, ``2026-02 latribet.xlsx``, or ``PowerPlay Winback & RND - Jan '26 commission report.xls``."""
    name_lower = filename.lower()
    
    # PowerPlay Intercept: Extract year/month from "Jan '26" or similar
    if "powerplay" in name_lower:
        months_map = {"jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06", "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12"}
        for m_str, m_num in months_map.items():
            if m_str in name_lower:
                import re
                # Look for 2 digit year like '26 or 2026
                yr_match = re.search(r"(?:'|20)(\d{2})\b", name_lower)
                if yr_match:
                    year = "20" + yr_match.group(1)
                    return "powerplay", f"{year}-{m_num}"

    # Try original format first: brand_yyyy_mm.ext
    m = FILE_RE.match(filename)
    if m:
        brand = m.group("brand").lower()
        year  = m.group("year")
        month = m.group("month")
        return brand, f"{year}-{month}"

    # Try yyyy-mm brand.ext format (strip extension, then match SHEET_RE)
    stem = re.sub(r"\.(?:csv|xlsx|xls)$", "", filename, flags=re.IGNORECASE)
    m2 = SHEET_RE.match(stem.strip())
    if m2:
        year  = m2.group("year")
        month = m2.group("month")
        brand = m2.group("brand").strip().lower()
        return brand, f"{year}-{month}"

    return None


def _normalise_player_columns(df: pd.DataFrame, source_label: str, target_format: str = "Standard", target_client: str = "UNKNOWN", target_brand: str = "UNKNOWN") -> Optional[pd.DataFrame]:
    """Deterministic Router: normalise to spec columns based on financial format."""
    # --- SCRUB INVISIBLE BOMs AND WHITESPACE FROM HEADERS ---
    df.columns = [str(c).replace('\ufeff', '').strip() for c in df.columns]

    if target_format == "LeoVegas":
        # ── LeoVegas Group path ──────────────────────────────────────────
        logger.info("[DeterministicRouter] %s → LeoVegas format applied", source_label)

        # Coerce numerics BEFORE rename to handle blank / string cells
        for raw_col in LEOVEGAS_NUMERIC_RAW:
            if raw_col in df.columns:
                df[raw_col] = pd.to_numeric(df[raw_col], errors="coerce").fillna(0)

        # Rename to spec columns
        df = df.rename(columns=LEOVEGAS_COL_MAP)
        
        # HOTFIX: Dynamically hunt for the dirty Bonus Total header BEFORE subsetting
        if "bonus_total" not in df.columns:
            bonus_match = [c for c in df.columns if "Bonus Cost (Total)" in str(c)]
            if bonus_match:
                df.rename(columns={bonus_match[0]: "bonus_total"}, inplace=True)
            else:
                df["bonus_total"] = 0.0

        # HOTFIX: Dynamically hunt for the Segment header (case-insensitive) BEFORE subsetting
        if "wb_tag" not in df.columns: # If LEOVEGAS_COL_MAP failed to map Segment -> wb_tag
            seg_match = [c for c in df.columns if "segment" in str(c).lower()]
            if seg_match:
                df.rename(columns={seg_match[0]: "wb_tag"}, inplace=True)

        spec_cols = list(LEOVEGAS_COL_MAP.values()) + ["bonus_total"]
        # Keep only mapped columns (LeoVegas has no 'win' yet)
        df = df[[c for c in spec_cols if c in df.columns]]
        
        # Mirror wb_tag to segment for Phase 16 Analytics
        if "wb_tag" in df.columns:
            df["segment"] = df["wb_tag"]
        else:
            df["segment"] = "Unknown"
                
        # Ensure the other bonus and tax columns have fallbacks if mapping missed them due to spaces
        for col in ["bonus_casino", "bonus_sports", "tax_total"]:
            if col not in df.columns:
                df[col] = 0.0
                
        # Ensure 'win' column exists for downstream compat
        if "win" not in df.columns:
            df["win"] = df["bet"] - df["revenue"]

        # Fill any missing vertical columns with 0 for LeoVegas just in case
        for vcol in ["bet_casino", "revenue_casino", "ngr_casino", "bet_sports", "revenue_sports", "ngr_sports"]:
            if vcol not in df.columns:
                df[vcol] = 0

        # Final coerce on spec numeric cols
        for col in NUMERIC_COLS:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Drop rows with null id
        df["id"] = df["id"].astype(str).str.strip()
        df = df[df["id"].ne("") & df["id"].ne("nan")]

        # Reactivation Delta parsing (Phase 13)
        df["reactivation_date"] = pd.to_datetime(df["reactivation_date"], format="%d/%m/%Y", errors="coerce")
        df["campaign_start_date"] = pd.to_datetime(df["campaign_start_date"], format="%d/%m/%Y", errors="coerce")
        df["reactivation_days"] = (df["reactivation_date"] - df["campaign_start_date"]).dt.days

        # Tag client entity
        df["client"] = target_client
        # Respect native multi-brand tracker data if present (e.g., LeoVegas trackers)
        if "brand" in df.columns:
            # Ensure empty strings are treated as NA, then fallback to target_brand
            df["brand"] = df["brand"].replace("", pd.NA).fillna(target_brand)
        else:
            df["brand"] = target_brand

    elif target_format == "Offside":
        # ── Offside Gaming path ──────────────────────────────────────────
        logger.info("[DeterministicRouter] %s → Offside Gaming format applied", source_label)

        missing_cols = [c for c in COL_MAP if c not in df.columns]
        if missing_cols:
            logger.warning("%s is missing columns %s – skipping", source_label, missing_cols)
            return None

        df = df.rename(columns=COL_MAP)[list(COL_MAP.values())]

        df = df.dropna(subset=["id"])

        # Mirror wb_tag to segment for Phase 16 Analytics
        if "wb_tag" in df.columns:
            df["segment"] = df["wb_tag"]

        # Tag client entity
        df["client"] = target_client
        df["brand"] = target_brand
        # Fallback NGR to equal GGR (revenue) for clients without tax data
        df["ngr"] = df["revenue"]
        # Fallback Country to 'Global' for clients without geographic data
        df["country"] = "Global"
        
        # Fallback Verticals to 0 for clients without vertical split
        df["bet_casino"] = 0
        df["revenue_casino"] = 0
        df["ngr_casino"] = 0
        df["bet_sports"] = 0
        df["revenue_sports"] = 0
        df["ngr_sports"] = 0

        # Offside Gaming Phase 8 Fallbacks
        df["deposit_count"] = 0
        df["deposits"] = 0
        df["withdrawals"] = 0
        df["bonus_casino"] = 0
        df["bonus_sports"] = 0
        df["bonus_total"] = 0
        df["tax_total"] = 0

        df["reactivation_date"] = pd.NaT
        df["campaign_start_date"] = pd.NaT
        df["reactivation_days"] = pd.NA

        # Coerce numerics AFTER fallback columns are created
        for col in NUMERIC_COLS:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    elif target_format == "PowerPlay":
        # ── PowerPlay path ───────────────────────────────────────────────
        logger.info("[DeterministicRouter] %s → PowerPlay format applied", source_label)
        
        # Determine cohort/segment and country from the sheet name mapped in source_label
        # source_label format during multisheet: "Power...report.xls:React - Feb 2025"
        sheet_name = source_label.split(":")[-1] if ":" in source_label else "UNKNOWN"
        
        country_code = "Global"
        if sheet_name.startswith("ROC"): country_code = "CA-ROC"
        if sheet_name.startswith("ONT"): country_code = "CA-ONT"
        
        # Map IDs and Financials
        if "Player Id" in df.columns: df.rename(columns={"Player Id": "id"}, inplace=True)
        
        # Scan for existing column names
        revenue_col = "Total GGR \u20ac"
        for c in df.columns:
            if "Total GGR" in str(c): revenue_col = c
            
        ngr_col = "Total NGR \u20ac"
        for c in df.columns:
            if "Total NGR" in str(c): ngr_col = c
            
        dep_col = "Deposits \u20ac"
        for c in df.columns:
            if "Deposits" in str(c): dep_col = c
            
        # Rename identified columns
        rename_map = {revenue_col: "revenue", ngr_col: "ngr"}
        if dep_col in df.columns: rename_map[dep_col] = "deposits"
        df.rename(columns=rename_map, inplace=True)
        
        # Drop missing IDs
        if "id" in df.columns:
            df = df.dropna(subset=["id"])
        else:
            return None
            
        # Hardcode missing required standard columns
        df["bet"] = 0.0  # Turnover is completely absent
        df["withdrawals"] = 0.0
        
        df["bet_casino"] = 0
        df["revenue_casino"] = 0
        df["ngr_casino"] = 0
        df["bet_sports"] = 0
        df["revenue_sports"] = 0
        df["ngr_sports"] = 0
        
        df["deposit_count"] = 0
        df["bonus_casino"] = 0
        df["bonus_sports"] = 0
        df["bonus_total"] = 0
        df["tax_total"] = 0
        
        df["client"] = target_client
        df["brand"] = target_brand
        df["country"] = country_code
        df["segment"] = sheet_name.split("-")[0].strip() if "-" in sheet_name else sheet_name
        df["wb_tag"] = sheet_name
        
        df["reactivation_date"] = pd.NaT
        df["campaign_start_date"] = pd.NaT
        df["reactivation_days"] = pd.NA

        # Ensure all numeric columns exist, otherwise fill 0
        for col in NUMERIC_COLS:
            if col not in df.columns:
                df[col] = 0.0
        
        # Coerce numerics 
        for col in NUMERIC_COLS:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    else:
        # ── Unrecognised/Standard ────────────────────────────────────────
        logger.warning("[DeterministicRouter] %s – No valid parser mapped for format: %s", source_label, target_format)
        return None

    return df


def _read_and_clean(csv_path: Path, target_format: str = "Standard", target_client: str = "UNKNOWN", target_brand: str = "UNKNOWN") -> Optional[pd.DataFrame]:
    """Read a single CSV and normalise via the Smart Router."""
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        logger.exception("Failed to read %s", csv_path)
        return None

    return _normalise_player_columns(df, csv_path.name, target_format, target_client, target_brand)


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
    allow_overwrite: bool = False,
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
    registry = IngestionRegistry.load()
    frames: list[pd.DataFrame] = []

    try:
        mapping_df = pd.read_sql("SELECT brand_code, brand_name, client_name, financial_format FROM client_mapping", db_engine)
        fin_map = {}
        for _, row in mapping_df.iterrows():
            val = {
                'client': row['client_name'], 
                'brand': row['brand_name'] if pd.notna(row.get('brand_name')) else row['brand_code'],
                'format': row.get('financial_format', 'Standard')
            }
            # Map by both code and name for bulletproof fallback
            if pd.notna(row.get('brand_name')): fin_map[str(row['brand_name']).strip().lower()] = val
            if pd.notna(row.get('brand_code')): fin_map[str(row['brand_code']).strip().lower()] = val
    except:
        fin_map = {}

    # Build set of existing brand+month combos for duplicate prevention
    existing_combos = set()
    try:
        dup_df = pd.read_sql("SELECT DISTINCT brand, report_month FROM raw_financial_data", db_engine)
        for _, r in dup_df.iterrows():
            existing_combos.add((str(r['brand']).strip(), str(r['report_month']).strip()))
    except Exception:
        pass

    def _ingest_single(brand_key, report_month, raw, source_label):
        """Process a single brand+month DataFrame and append to frames."""
        mapped_info = fin_map.get(brand_key.strip().lower(), {})
        target_format = mapped_info.get('format', 'Standard')
        target_client = mapped_info.get('client', 'UNKNOWN')
        target_brand = mapped_info.get('brand', brand_key.title())
        if target_client == 'LeoVegas Group': target_format = 'LeoVegas'
        elif 'Offside' in target_client: target_format = 'Offside'

        # Duplicate protection
        if (target_brand, report_month) in existing_combos:
            if not allow_overwrite:
                try:
                    import streamlit as st
                    st.warning(f"⚠️ Rejected '{source_label}': Data for {target_brand} ({report_month}) already exists. Enable 'Update existing data' to overwrite.")
                except: pass
                return
            else:
                # Delete existing data for this brand+month before re-ingesting
                try:
                    with db_engine.begin() as conn:
                        conn.execute(text("DELETE FROM raw_financial_data WHERE brand = :b AND report_month = :m"), {"b": target_brand, "m": report_month})
                    import streamlit as st
                    st.info(f"🔄 Updating existing data for {target_brand} ({report_month})...")
                except Exception as e:
                    logger.exception("Failed to delete existing data for %s %s", target_brand, report_month)
                    return

        df = _normalise_player_columns(raw, source_label, target_format, target_client, target_brand)
        if df is None or df.empty:
            logger.warning("Empty after cleaning: %s", source_label)
            return

        df["report_month"] = report_month
        frames.append(df)
        existing_combos.add((target_brand, report_month))
        registry.mark_complete(brand=brand_key, report_month=report_month, file_path=source_label)

    for f in files:
        f_name_lower = f.name.lower()
        is_excel = f_name_lower.endswith(".xlsx") or f_name_lower.endswith(".xls")
        
        # Check if the file is PowerPlay
        is_powerplay = "powerplay" in f_name_lower

        # Try multi-sheet Excel first
        if is_excel:
            try:
                # Use default engine for .xls (xlrd) and openpyxl for .xlsx
                eng = "openpyxl" if f_name_lower.endswith(".xlsx") else None
                xl = pd.ExcelFile(f, engine=eng)
                has_multi = any(SHEET_RE.match(s.strip()) for s in xl.sheet_names)
            except Exception:
                has_multi = False
                xl = None

            # 1. Standard Multi-Sheet Processing
            if has_multi and xl and not is_powerplay:
                for sheet_name in xl.sheet_names:
                    sm = SHEET_RE.match(sheet_name.strip())
                    if not sm:
                        logger.info("Skipping non-data sheet: '%s' in %s", sheet_name, f.name)
                        continue
                    year, month = sm.group("year"), sm.group("month")
                    brand_key = sm.group("brand").strip().lower()
                    report_month = f"{year}-{month}"
                    try:
                        raw = xl.parse(sheet_name)
                    except Exception:
                        logger.exception("Failed to read sheet '%s' in %s", sheet_name, f.name)
                        continue
                    _ingest_single(brand_key, report_month, raw, f"{f.name}:{sheet_name}")
                continue  # Done with this file
                
            # 2. PowerPlay Custom Multi-Sheet Processing
            elif is_powerplay and xl:
                parsed = _parse_filename(f.name)
                if not parsed:
                    logger.warning("Could not parse PowerPlay filename: %s", f.name)
                    continue
                brand_key, report_month = parsed
                
                for sheet_name in xl.sheet_names:
                    # Skip summary and non-data sheets
                    invalid_sheets = ["Commission overview", "Sheet1"]
                    if sheet_name.strip() in invalid_sheets:
                        continue
                        
                    try:
                        raw = xl.parse(sheet_name)
                    except Exception:
                        logger.exception("Failed to read PowerPlay sheet '%s'", sheet_name)
                        continue
                    
                    # _ingest_single handles standardisation and DB routing
                    _ingest_single(brand_key, report_month, raw, f"{f.name}:{sheet_name}")
                continue # Done with this file

        # Single-file path (CSV or single-sheet file)
        parsed = _parse_filename(f.name)
        if parsed is None:
            logger.warning("Skipping unrecognised file: %s", f.name)
            continue

        brand_key, report_month = parsed
        try:
            if is_excel:
                eng = "openpyxl" if f_name_lower.endswith(".xlsx") else None
                raw = pd.read_excel(f, engine=eng)
            else:
                raw = pd.read_csv(f)
        except Exception:
            logger.exception("Failed to read %s", f.name)
            continue

        _ingest_single(brand_key, report_month, raw, f.name)

    if not frames:
        logger.warning("No data loaded from uploads.")
        return pd.DataFrame(), registry

    unified = pd.concat(frames, ignore_index=True)

    gaps = registry.evaluate_gaps()
    for gap in gaps:
        logger.warning("Missing %s data for %s", _pretty_month(gap["report_month"]), gap["brand"].title())

    # --- PERMANENT DB SAVE FOR FINANCIAL DATA ---
    if not unified.empty:
        try:
            # 'unified' already has client, brand, and report_month correctly assigned!
            db_df = unified.rename(columns={"id": "player_id"}) # Rename to avoid PostgreSQL PK conflict
            expected_cols = ["player_id", "client", "brand", "country", "wb_tag", "segment", "bet", "revenue", "ngr", "bet_casino", "revenue_casino", "ngr_casino", "bet_sports", "revenue_sports", "ngr_sports", "deposit_count", "deposits", "withdrawals", "bonus_total", "bonus_casino", "bonus_sports", "tax_total", "report_month", "reactivation_date", "campaign_start_date", "reactivation_days"]
            db_df = db_df[[c for c in expected_cols if c in db_df.columns]]
            db_df.to_sql("raw_financial_data", db_engine, if_exists="append", index=False)
        except Exception as e:
            pass

    # Invalidate Fin Cache so UI fetches fresh DB data
    try:
        import streamlit as st
        if "raw_fin_df" in st.session_state:
            del st.session_state["raw_fin_df"]
    except:
        pass

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

        brand_key, report_month = parsed

        try:
            if f.name.lower().endswith(".xlsx"):
                df = pd.read_excel(f, engine="openpyxl")
            else:
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

def load_operations_data_from_uploads(files: list) -> pd.DataFrame:
    frames = []
    unmapped_tags = set()
    
    # Dynamically fetch the latest Brand Registry from the database
    try:
        mapping_df = pd.read_sql("SELECT brand_code, brand_name, client_name FROM client_mapping", db_engine)
        live_map = {row['brand_code']: {'client': row['client_name'], 'brand': row['brand_name'] if pd.notnull(row.get('brand_name')) else row['brand_code']} for _, row in mapping_df.iterrows()}
    except:
        live_map = {}

    for f in files:
        try:
            f.seek(0)
            if f.name.lower().endswith(".xlsx"): df = pd.read_excel(f, engine="openpyxl")
            else:
                try: df = pd.read_csv(f, encoding="utf-8")
                except: 
                    f.seek(0)
                    df = pd.read_csv(f, encoding="ISO-8859-1")
        except Exception as e:
            continue
            
        df.columns = [str(c).replace('\ufeff', '').replace('"', '').strip() for c in df.columns]
        
        if "Campaign Name" not in df.columns: continue
            
        ops_metrics = ["# Records", "New Data", "Calls", "KPI1-Conv.", "KPI2-Login", "LI%", "Cost Caller", "Cost SIP", "Cost SMS", "Cost Email", 
                       "D", "D+", "D-", "D Ratio", "T", "AM", "DNC", "NA", "DX", "WN",
                       "HLRV", "2XRV", "SA", "SD", "SF", "SP", "EV", "ES", "ED", "EO", "EC", "EF", 
                       "Optouts (All)", "Optout - Call", "Optout - SMS", "Optout - Email"]
        for col in ops_metrics:
            if col not in df.columns: df[col] = 0.0
            else: df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        
        records_to_insert = []
        # --- EXTRACT DATE FROM FILENAME ---
        # Attempt to parse Daily format first (e.g., 2026-02-14 or 2026_02_14)
        daily_match = re.search(r'(20\d{2})[-_](\d{2})[-_](\d{2})', f.name)
        # Fallback for Legacy Monthly format
        monthly_match = re.search(r'(20\d{2})[-_](\d{2})', f.name)
        
        if daily_match:
            report_date = f"{daily_match.group(1)}-{daily_match.group(2)}-{daily_match.group(3)}"
        elif monthly_match:
            # Legacy fallback appends -01
            report_date = f"{monthly_match.group(1)}-{monthly_match.group(2)}-01"
        else:
            continue

        # --- ASSIGN DAILY DATE TO ALL ROWS ---
        if 'Date' in df.columns:
            df['ops_date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
        else:
            # Use the exact daily date parsed from the filename
            df['ops_date'] = report_date

        # --- VECTORIZED METRIC CALCULATION (Phase 10) ---
        df["campaign"] = df.get("Campaign Name", "UNKNOWN").astype(str)
        df["ops_date"] = report_date if 'Date' not in df.columns else pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Upper case and normalize 'WBD' -> 'WB'
        df["normalized_camp"] = df["campaign"].str.upper().str.replace('_', '-').str.replace('-WBD-', '-WB-')
        # Split tokens into a DataFrame of varying lengths
        tokens_df = df["normalized_camp"].str.split('-', expand=True).fillna("")
        
        # Extract Brand Tag (first token)
        df["tag"] = tokens_df[0]
        df["tag"] = np.where(df["tag"] == "", "UNKNOWN", df["tag"])
        
        # Track unmapped tags
        new_unmapped = set(df["tag"].unique()) - set(live_map.keys()) - {"UNKNOWN"}
        if new_unmapped: unmapped_tags.update(new_unmapped)
        
        # Map Client and Brand names vector-wise
        df["ops_client"] = df["tag"].map(lambda x: live_map.get(x, {}).get("client", "UNKNOWN"))
        df["ops_brand"] = df["tag"].map(lambda x: live_map.get(x, {}).get("brand", x))
        
        # Vectorized list searches for Benchmark components
        def _extract_component(allowed_list):
            return tokens_df.isin(allowed_list).idxmax(axis=1).map(lambda idx: tokens_df.lookup(tokens_df.index, [idx])[0] if tokens_df.iloc[:, idx].notna().any() else "UNKNOWN")

        # Fast extraction by creating a stacked token lookup to find the first match
        stacked = tokens_df.stack()
        
        LIFECYCLES = {'RND', 'WB', 'CS', 'ROC', 'FD', 'OTD', 'CHU', 'ACQ', 'SL', 'LFC', 'LOADER'}
        SEGMENTS = {'HIGH', 'MID', 'MED', 'LOW', 'VIP', 'NA', 'AFF', 'COH1', 'COH2', 'COH3', 'COH4'}
        ENGAGEMENTS = {'NLI', 'LI'}
        PRODUCTS = {'SPO', 'CAS', 'LIVE', 'ALL'}
        SUBLIFECYCLES = {'J1', 'J2', 'J3', 'BULK'}
        
        def _vector_extract(target_set):
            # Find the index of the first token in the target set for each row
            mask = tokens_df.isin(target_set)
            # idxmax returns the first True column, if any True exists
            return np.where(mask.any(axis=1), tokens_df.values[np.arange(len(tokens_df)), mask.values.argmax(axis=1)], "UNKNOWN")
            
        df["extracted_lifecycle"] = _vector_extract(LIFECYCLES)
        df["extracted_segment"] = _vector_extract(SEGMENTS)
        df["extracted_engagement"] = _vector_extract(ENGAGEMENTS)
        df["extracted_product"] = _vector_extract(PRODUCTS)
        df["extracted_sublifecycle"] = _vector_extract(SUBLIFECYCLES)
        
        # Country Extraction
        BLOCKLIST = PRODUCTS | LIFECYCLES | SEGMENTS | ENGAGEMENTS | SUBLIFECYCLES | {'DAY', 'A', 'B', 'NEW'}
        
        # Function to find the first 2-3 char iso code not in block_set
        def _find_country(row_tokens, tag):
            block = BLOCKLIST | {str(tag).upper()}
            for t in row_tokens[1:]:
                if str(t) not in block and str(t).isalpha() and 2 <= len(str(t)) <= 3:
                    return str(t)
            return "Global"
            
        df["country"] = [ _find_country(row, tag) for row, tag in zip(tokens_df.values, df["tag"]) ]
        
        _LANG_DEFAULTS = {
            'TR': 'TR', 'ES': 'ES', 'CL': 'ES', 'EC': 'ES', 'MX': 'ES', 'AR': 'ES', 'CO': 'ES', 'PE': 'ES', 
            'BR': 'PT', 'GB': 'EN', 'UK': 'EN', 'IE': 'EN', 'CA': 'EN', 'NZ': 'EN', 'JP': 'JA', 'DE': 'DE', 
            'AT': 'DE', 'CH': 'DE', 'SE': 'SV', 'NO': 'NO', 'DK': 'DA', 'FI': 'FI', 'IT': 'IT', 'FR': 'FR', 'ONT': 'EN'
        }
        df["extracted_language"] = df["country"].map(_LANG_DEFAULTS).fillna("UNKNOWN")
        
        # Calculate Math Arrays
        df["total_cost"] = df["Cost Caller"] + df["Cost SIP"] + df["Cost SMS"] + df["Cost Email"]
        df["true_cac"] = np.where(df["KPI1-Conv."] > 0, df["total_cost"] / df["KPI1-Conv."], 0.0)
        
        df["campaign_name"] = df["campaign"] + "_" + df["ops_date"]
        
        # Resolve 'records' fallback array logic
        if "New Data" in df.columns:
            df["records"] = df["New Data"]
        elif "# Records" in df.columns:
            df["records"] = df["# Records"]
        else:
            df["records"] = 0
            
        df["records"] = df["records"].fillna(0).astype(int)
        
        # Rename standard columns to prep for insertion payload mapping
        df.rename(columns={
            "Calls": "calls", "KPI1-Conv.": "conversions", "D": "d_neutral", "D+": "d_plus", "D-": "d_minus", 
            "D Ratio": "d_ratio", "KPI2-Login": "kpi2_logins", "LI%": "li_pct", "T": "tech_issues",
            "Cost Caller": "cost_caller", "Cost SIP": "cost_sip", "Cost SMS": "cost_sms", "Cost Email": "cost_email",
            "AM": "am", "DNC": "dnc", "NA": "na", "DX": "dx", "WN": "wn", "HLRV": "hlrv", "2XRV": "twoxrv",
            "SA": "sa", "SD": "sd", "SF": "sf", "SP": "sp", "EV": "ev", "ES": "es", "ED": "ed", "EO": "eo", 
            "EC": "ec", "EF": "ef", "Optouts (All)": "optouts_all", "Optout - Call": "optout_call", 
            "Optout - SMS": "optout_sms", "Optout - Email": "optout_email"
        }, inplace=True)
        
        # Additional cleanup of duplicate 'T' column requirement from old array
        df["t"] = df["tech_issues"]
        
        cols_to_keep = [
            "campaign_name", "ops_client", "ops_brand", "ops_date", "records", "calls", "conversions", 
            "total_cost", "true_cac", "d_neutral", "d_plus", "d_minus", "d_ratio", "kpi2_logins", "li_pct", 
            "tech_issues", "t", "am", "dnc", "na", "dx", "wn", "cost_caller", "cost_sip", "cost_sms", "cost_email", 
            "hlrv", "twoxrv", "sa", "sd", "sf", "sp", "ev", "es", "ed", "eo", "ec", "ef", "optouts_all", 
            "optout_call", "optout_sms", "optout_email", "extracted_engagement", "extracted_lifecycle", 
            "extracted_segment", "extracted_product", "extracted_language", "extracted_sublifecycle", "country"
        ]
        
        # Build d_total from neutral as per legacy logic
        df["d_total"] = df["d_neutral"]
        cols_to_keep.append("d_total")
        
        # Filter cleanly to DB payload
        batch_df = df[[c for c in cols_to_keep if c in df.columns]].copy()
        
        # Type enforcement
        int_fields = ["calls", "conversions", "d_neutral", "d_plus", "d_minus", "kpi2_logins", "tech_issues", "t", "am", "dnc", "na", "dx", "wn", "hlrv", "twoxrv", "sa", "sd", "sf", "sp", "ev", "es", "ed", "eo", "ec", "ef", "optouts_all", "optout_call", "optout_sms", "optout_email", "d_total"]
        float_fields = ["total_cost", "true_cac", "d_ratio", "li_pct", "cost_caller", "cost_sip", "cost_sms", "cost_email"]
        
        for field in int_fields:
            if field in batch_df.columns:
                batch_df[field] = pd.to_numeric(batch_df[field], errors='coerce').fillna(0).astype(int)
        for field in float_fields:
            if field in batch_df.columns:
                batch_df[field] = pd.to_numeric(batch_df[field], errors='coerce').fillna(0.0)
        
        # Phase 14.1 FIX: Removed legacy `records_to_insert = True` flag + `pd.DataFrame(records_to_insert)`
        # which was overwriting the valid batch_df with an empty DataFrame, silently dropping all data.
        if not batch_df.empty:
            try:
                batch_df.to_sql("ops_telemarketing_data", db_engine, if_exists="append", index=False)
            except Exception as e:
                pass

            try:
                # Save Snapshot (No unique constraints, always appends a point-in-time record)
                batch_df.to_sql("ops_telemarketing_snapshots", db_engine, if_exists="append", index=False)
            except Exception as e_snap:
                pass
            
            frames.append(batch_df)

    # Push unmapped tags to UI state
    if unmapped_tags:
        import streamlit as st
        if "unmapped_tags" not in st.session_state:
            st.session_state["unmapped_tags"] = set()
        st.session_state["unmapped_tags"].update(unmapped_tags)

    # Invalidate Ops Cache so UI fetches fresh DB data
    try:
        import streamlit as st
        if "raw_ops_df" in st.session_state:
            del st.session_state["raw_ops_df"]
    except:
        pass

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
