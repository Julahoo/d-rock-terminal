"""
One-Time Migration: Add campaign_data_age_days to ops_telemarketing_data
========================================================================
Adds the column, then backfills ALL existing rows using a single server-side
SQL UPDATE that executes entirely within PostgreSQL — no row-by-row Python loop.

Usage: railway run python scripts/jobs/migrate_data_age.py
"""
import os, sys
from sqlalchemy import text

sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
from src.database import engine as db_engine

def run():
    print("🔧 Step 1: Adding campaign_data_age_days column if not exists...")
    with db_engine.begin() as conn:
        conn.execute(text("""
            ALTER TABLE ops_telemarketing_data
            ADD COLUMN IF NOT EXISTS campaign_data_age_days INTEGER DEFAULT -1
        """))
    print("✅ Column exists.")

    print("⚡ Step 2: Server-side batch UPDATE (no Python loop)...")
    print("   Parsing campaign dates from the last 3 tokens of campaign_name...")
    
    with db_engine.begin() as conn:
        # The campaign_name format is: OriginalCampaign_YYYY-MM-DD
        # The appended suffix after '_' is the ops_date. The original campaign
        # contains the campaign date as the last 3 dash-separated tokens (YYYY-MM-DD).
        # We extract them using split_part and regexp matching, all server-side.
        result = conn.execute(text("""
            UPDATE ops_telemarketing_data
            SET campaign_data_age_days = (
                ops_date::date - (
                    -- Extract the original campaign (before the last '_YYYY-MM-DD' suffix)
                    -- Then find the date embedded in it (last 3 dash tokens = YYYY-MM-DD)
                    CASE 
                        WHEN campaign_name ~ '.*-[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                        THEN (regexp_match(campaign_name, '([0-9]{4}-[0-9]{2}-[0-9]{2})_[0-9]{4}-[0-9]{2}-[0-9]{2}$'))[1]::date
                        ELSE NULL
                    END
                )
            )
            WHERE campaign_data_age_days = -1 OR campaign_data_age_days IS NULL
        """))
        print(f"✅ Backfill complete: {result.rowcount:,} rows updated.")

if __name__ == "__main__":
    run()
