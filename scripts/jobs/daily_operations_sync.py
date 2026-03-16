import os
import sys
from datetime import datetime, timedelta, timezone

# Ensure we can import from src
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from src.iwinback_worker import run_historical_pull, log_msg

def run_daily_sync():
    """
    Executes a cron-scheduled pull from the CallsU API for 'yesterday'.
    Target Schedule: 04:30 CET (03:30 UTC).
    
    Phase 14.1 Hardening:
    - Auto-detects gaps between the latest DB data and yesterday
    - Backfills any missed dates BEFORE pulling yesterday's data
    - Post-pull verification to confirm data landed in the DB
    - Cascading ETL materialization to update all frontend views
    """
    log_msg("=====================================================")
    log_msg(f"🕒 DAILY AUTOMATION TRIGGERED AT: {datetime.now(timezone.utc).isoformat()} (UTC)")
    
    try:
        import pandas as pd
        from src.database import engine as db_engine

        yesterday_dt = datetime.now(timezone.utc) - timedelta(days=1)
        yesterday_str = yesterday_dt.strftime("%Y-%m-%d")
        
        log_msg(f"📅 Target Execution Date (Yesterday): {yesterday_str}")

        # ── Phase 14.1: Auto-Gap Detection & Backfill ──
        # Before pulling yesterday, check if any previous days were missed
        try:
            latest_raw = pd.read_sql("SELECT MAX(ops_date) as max_d FROM ops_telemarketing_data", db_engine)
            db_max = pd.to_datetime(latest_raw.iloc[0]['max_d'])
            
            if pd.notna(db_max):
                expected_next = db_max + timedelta(days=1)
                yesterday_ts = pd.Timestamp(yesterday_str)
                
                if expected_next < yesterday_ts:
                    gap_start = expected_next.strftime("%Y-%m-%d")
                    gap_end = (yesterday_ts - timedelta(days=1)).strftime("%Y-%m-%d")
                    gap_days = (yesterday_ts - expected_next).days
                    log_msg(f"⚠️ GAP DETECTED: {gap_days} missing day(s) ({gap_start} → {gap_end})")
                    log_msg(f"🔄 BACKFILLING {gap_days} day(s) before daily pull...")
                    
                    backfill_success = run_historical_pull(start_date=gap_start, end_date=gap_end)
                    if backfill_success:
                        log_msg("✅ GAP BACKFILL COMPLETED SUCCESSFULLY.")
                    else:
                        log_msg("⚠️ GAP BACKFILL HAD FAILURES — will continue with daily pull.")
                else:
                    log_msg("✅ No gaps detected. DB is up to date.")
            else:
                log_msg("ℹ️ No existing data in DB. Proceeding with daily pull.")
        except Exception as gap_err:
            log_msg(f"⚠️ Gap detection failed (non-fatal): {gap_err}")

        # ── Daily Pull: Yesterday ──
        log_msg(f"\n📥 PULLING DAILY DATA: {yesterday_str}")
        success = run_historical_pull(start_date=yesterday_str, end_date=yesterday_str)
        
        if success:
            log_msg("✅ DAILY API PULL COMPLETED SUCCESSFULLY.")
            
            # ── Post-Pull Verification ──
            try:
                verify = pd.read_sql(
                    f"SELECT COUNT(*) as cnt FROM ops_telemarketing_data WHERE ops_date = '{yesterday_str}'",
                    db_engine
                )
                row_count = verify.iloc[0]['cnt']
                if row_count > 0:
                    log_msg(f"✅ VERIFIED: {row_count} rows for {yesterday_str} in database.")
                else:
                    log_msg(f"⚠️ WARNING: 0 rows for {yesterday_str} after pull. Data may not have been available from API.")
            except Exception as v_err:
                log_msg(f"⚠️ Verification query failed: {v_err}")
            
            # ── Cascading ETL Materialization ──
            try:
                log_msg("⚡ TRIGGERING CASCADING ETL MATERIALIZATION...")
                import src.etl_worker as etl_worker
                etl_worker.main()
                log_msg("✅ CASCADING ETL MATERIALIZATION COMPLETED SUCCESSFULLY.")
                sys.exit(0)
            except Exception as etl_err:
                log_msg(f"❌ ETL MATERIALIZATION FAILED: {str(etl_err)}")
                sys.exit(1)
        else:
            log_msg("❌ DAILY AUTOMATION FAILED DURING EXECUTION.")
            sys.exit(1)
            
    except Exception as e:
        log_msg(f"🚨 CRITICAL CRON FAILURE: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_daily_sync()
