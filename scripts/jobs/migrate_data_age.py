"""
One-Time Migration: Add campaign_data_age_days to ops_telemarketing_data
========================================================================
Adds the column, then backfills all existing rows by parsing the campaign date 
from the last 3 tokens of the campaign_name (YYYY-MM-DD split by '-').

Usage: python scripts/jobs/migrate_data_age.py
"""
import os, sys, re
import pandas as pd
from sqlalchemy import text

sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
from src.database import engine as db_engine

def run():
    print("🔧 Adding campaign_data_age_days column if not exists...")
    with db_engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE ops_telemarketing_data
            ADD COLUMN IF NOT EXISTS campaign_data_age_days INTEGER DEFAULT -1
        """))
    print("✅ Column exists.")

    print("📥 Fetching existing rows for backfill...")
    df = pd.read_sql("""
        SELECT id, campaign_name, ops_date
        FROM ops_telemarketing_data
        WHERE campaign_data_age_days IS NULL OR campaign_data_age_days = -1
        LIMIT 500000
    """, db_engine)

    if df.empty:
        print("✅ Nothing to backfill.")
        return

    print(f"📊 Processing {len(df):,} rows...")

    def _extract_campaign_date(cname):
        """Extract date from campaign naming convention: ...YYYY-MM-DD"""
        # The campaign_name has format: OriginalCampaign_ops_date
        # We need the original campaign's date, embedded in the first part
        original = str(cname).rsplit("_", 1)[0]  # Strip the appended ops_date
        tokens = original.upper().replace("_", "-").split("-")
        if len(tokens) >= 3:
            candidate = f"{tokens[-3]}-{tokens[-2]}-{tokens[-1]}"
            try:
                return pd.to_datetime(candidate, format="%Y-%m-%d")
            except (ValueError, TypeError):
                pass
        return pd.NaT

    df["_campaign_date"] = df["campaign_name"].apply(_extract_campaign_date)
    df["_ops_dt"] = pd.to_datetime(df["ops_date"], errors="coerce")
    df["age"] = (df["_ops_dt"] - df["_campaign_date"]).dt.days
    df["age"] = df["age"].fillna(-1).astype(int)

    # Batch update
    updated = 0
    with db_engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                UPDATE ops_telemarketing_data
                SET campaign_data_age_days = :age
                WHERE id = :id
            """), {"age": int(row["age"]), "id": int(row["id"])})
            updated += 1
            if updated % 10000 == 0:
                print(f"   ⏳ {updated:,}/{len(df):,} rows updated...")

    print(f"✅ Backfill complete: {updated:,} rows updated.")

if __name__ == "__main__":
    run()
