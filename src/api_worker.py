import requests
import time
import os
import pandas as pd
from datetime import datetime
import logging
from src.ingestion import load_operations_data_from_uploads

# Set up a dedicated logger that writes to a file for the Streamlit UI to read
LOG_FILE = "data/api_sync.log"
os.makedirs("data", exist_ok=True)

logger = logging.getLogger("api_worker")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE, mode='w') # Overwrite log each run
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s', "%H:%M:%S"))
logger.addHandler(file_handler)

API_TOKEN = "204|bUfc0GNczvNg1u3danK8nEreasH9VYl8AjVGTKmY7f07018f"
BASE_URL = "https://dashboard.callsu.net/api/v1/campaign-summary-exports"

def log_msg(msg):
    print(msg)
    logger.info(msg)

def _ingest_local_file(file_path, target_date):
    """Wrapper to route physical files through our Streamlit ingestion payload"""
    try:
        with open(file_path, "rb") as f:
            # The ingestion script expects a file-like object with a .name attribute
            load_operations_data_from_uploads([f])
        log_msg(f"💾 Ingested existing local file {target_date} via ETL pipeline!")
    except Exception as e:
        log_msg(f"❌ DB Ingest error for local file {target_date}: {e}")

def run_historical_pull(start_date, end_date):
    log_msg(f"🚀 Starting API Sync from {start_date} to {end_date}...")

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    })

    base_dir = "data/raw/callsu_daily"
    date_list = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()

    def process_day(target_date):
        month_folder = target_date[:7]
        folder_path = os.path.join(base_dir, month_folder)
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, f"{target_date}.xlsx")

        if os.path.exists(file_path):
            log_msg(f"⏭️ Skipping download for {target_date} (Already exists on disk)")
            try:
                with open(file_path, "rb") as f:
                    from src.ingestion import load_operations_data_from_uploads
                    load_operations_data_from_uploads([f])
                log_msg(f"💾 Ingested existing local file {target_date} via ETL pipeline!")
                return True
            except Exception as e:
                log_msg(f"❌ DB Ingest error for local file {target_date}: {e}")
                return False

        log_msg(f"📅 Requesting data for: {target_date}")
        payload = {
            "date_range_start": target_date, "date_range_end": target_date,
            "client_id": [], "brand_id": [], "campaign_id": [], "adhoc_type": "all"
        }
        
        try:
            response = session.post(BASE_URL, json=payload, timeout=20)
            if response.status_code not in [200, 201]:
                log_msg(f"❌ API Error: {response.status_code}")
                return False
            job_id = response.json()['data']['id']
        except Exception as e:
            log_msg(f"⚠️ Connection blip: {e}")
            return False

        log_msg(f"⏳ Job {job_id} created! Waiting for CallsU server...")
        job_done = False
        
        # Timeout increased to 120 attempts (10 minutes max)
        for attempt in range(120):
            try:
                res = session.get(f"{BASE_URL}/{job_id}", timeout=20)
                if res.status_code == 200:
                    status = res.json()['data'].get('status')
                    if status == 'done':
                        job_done = True
                        break
                    elif status in ['failed', 'error']:
                        log_msg(f"❌ Job {job_id} failed on server.")
                        return False
                    else:
                        # Only print every 60 seconds to keep terminal clean
                        if attempt % 12 == 0: 
                            log_msg(f"   ⏳ Server says: '{status}'... (Attempt {attempt+1}/120)")
                        time.sleep(5)
            except Exception as e:
                time.sleep(5)

        if not job_done:
            log_msg(f"⚠️ Job {job_id} timed out on server.")
            return False

        try:
            dl_res = session.get(f"{BASE_URL}/{job_id}/download", stream=True, timeout=30)
            if dl_res.status_code == 200:
                with open(file_path, 'wb') as f:
                    for chunk in dl_res.iter_content(chunk_size=8192):
                        f.write(chunk)
                log_msg(f"✅ Saved locally: {target_date}.xlsx")
                
                try:
                    with open(file_path, "rb") as f:
                        from src.ingestion import load_operations_data_from_uploads
                        load_operations_data_from_uploads([f])
                    log_msg(f"💾 Ingested {target_date} via ETL pipeline!")
                    return True
                except Exception as e:
                    log_msg(f"❌ DB Ingest error for {target_date}: {e}")
                    return False
            else:
                log_msg(f"❌ Download failed.")
                return False
        except Exception as e:
            log_msg(f"❌ Download error: {e}")
            return False

    # --- MAIN LOOP ---
    retry_queue = []
    for target_date in date_list:
        success = process_day(target_date)
        if not success:
            retry_queue.append(target_date)

    # --- RETRY LOOP ---
    if retry_queue:
        log_msg(f"\n🔄 COMMENCING RETRY QUEUE FOR {len(retry_queue)} FAILED DAYS...")
        time.sleep(10) # Give vendor API a short breather
        
        final_failures = []
        for target_date in retry_queue:
            log_msg(f"\n🔁 RETRYING: {target_date}")
            success = process_day(target_date)
            if not success:
                final_failures.append(target_date)
        
        if final_failures:
            log_msg(f"\n🚨 CRITICAL WARNING: The following days failed after retrying: {final_failures}")
            log_msg("⚠️ Please manually pull these dates from the vendor portal.")
        else:
            log_msg("\n🎉 All retries were successfully recovered!")

    log_msg("\n🏁 ALL REQUESTED PULLS COMPLETE!")
