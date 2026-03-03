import requests
import time
import calendar
import os

# ── 1. Configuration ──────────────────────────────────────────────────────────
# Put your real Callsu API token here
API_TOKEN = "Bearer 204|bUfc0GNczvNg1u3danK8nEreasH9VYl8AjVGTKmY7f07018f"

# List the year and month combinations you want to backfill (e.g., LeoVegas months)
# Format: (Year, Month)
MONTHS_TO_FETCH = [
    (2025, 1),
    (2025, 2),
    (2025, 3),
    (2025, 4),
    (2025, 5),
    (2025, 6),
    (2025, 7),
    (2025, 8),
    (2025, 9),
    (2025, 10),
    (2025, 11),
    (2025, 12),
]

HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

BASE_URL = "https://dashboard.callsu.net/api/v1/campaign-summary-exports"

# ── 2. Helper Functions ───────────────────────────────────────────────────────
def get_month_range(year, month):
    """Returns the first and last day of a given month as strings."""
    start_date = f"{year}-{month:02d}-01"
    last_day = calendar.monthrange(year, month)[1]
    end_date = f"{year}-{month:02d}-{last_day:02d}"
    return start_date, end_date

def create_export_job(start_date, end_date):
    """Sends the POST request to start the export job."""
    payload = {
        "date_range_start": start_date,
        "date_range_end": end_date,
        "client_id": [],
        "brand_id": [],
        "campaign_id": [],
        "adhoc_type": "all"
    }
    response = requests.post(BASE_URL, headers=HEADERS, json=payload)
    if response.status_code in [200, 201]:
        return response.json()['data']['id']
    else:
        print(f"❌ Failed to create job for {start_date}: {response.text}")
        return None

def wait_for_job(job_id):
    """Polls the API every 10 seconds until the job is 'done'."""
    url = f"{BASE_URL}/{job_id}"
    while True:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()['data']
            status = data.get('status')
            if status == 'done':
                return data.get('filename')
            elif status in ['failed', 'error']:
                print(f"❌ Job {job_id} failed on the server.")
                return None
            else:
                print(f"   ⏳ Job {job_id} is {status}... waiting 10s")
                time.sleep(10)
        else:
            print(f"❌ Failed to check status for job {job_id}: {response.text}")
            return None

def download_file(job_id, custom_filename):
    """Downloads the completed file."""
    url = f"{BASE_URL}/{job_id}/download"
    response = requests.get(url, headers=HEADERS, stream=True)
    
    if response.status_code == 200:
        with open(custom_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"   ✅ Saved locally as: {custom_filename}")
    else:
        print(f"❌ Failed to download job {job_id}.")

# ── 3. Main Execution Loop ────────────────────────────────────────────────────
print("🚀 Starting CallsU Historical Backfill...\n")

# Create a folder to keep things clean
os.makedirs("callsu_backfill", exist_ok=True)

for year, month in MONTHS_TO_FETCH:
    start_date, end_date = get_month_range(year, month)
    friendly_name = f"callsu_backfill/CallsU_Operations_{year}_{month:02d}.xlsx"
    
    print(f"📅 Requesting data for: {start_date} to {end_date}")
    
    # Step 1: Trigger Job
    job_id = create_export_job(start_date, end_date)
    if not job_id:
        continue
    print(f"   ✅ Job Created (ID: {job_id})")
    
    # Step 2: Wait for it to build
    server_filename = wait_for_job(job_id)
    if not server_filename:
        continue
    
    # Step 3: Download and rename it
    print(f"   📥 Downloading file...")
    download_file(job_id, friendly_name)
    print("-" * 50)

print("\n🎉 BACKFILL COMPLETE! All files are in the 'callsu_backfill' folder.")