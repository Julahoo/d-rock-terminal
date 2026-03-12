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
    """
    log_msg("=====================================================")
    log_msg(f"🕒 DAILY AUTOMATION TRIGGERED AT: {datetime.now(timezone.utc).isoformat()} (UTC)")
    
    try:
        # Calculate exactly "yesterday" relative to currently executing time
        yesterday_dt = datetime.now(timezone.utc) - timedelta(days=1)
        yesterday_str = yesterday_dt.strftime("%Y-%m-%d")
        
        log_msg(f"📅 Target Execution Date (Yesterday): {yesterday_str}")
        
        # Invoke the robust API Worker (handles download, parsing, and DB upsert)
        success = run_historical_pull(start_date=yesterday_str, end_date=yesterday_str)
        
        if success:
            log_msg("✅ DAILY AUTOMATION COMPLETED SUCCESSFULLY.")
            sys.exit(0)  # Signals success to Railway Cron scheduler
        else:
            log_msg("❌ DAILY AUTOMATION FAILED DURING EXECUTION.")
            sys.exit(1)  # Signals failure to Railway Cron scheduler
            
    except Exception as e:
        log_msg(f"🚨 CRITICAL CRON FAILURE: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_daily_sync()
