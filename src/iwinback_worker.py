"""
iWinBack Native API Worker
===========================
Direct integration with 5 iWinBack SaaS boxes for operations data.
Replaces the legacy dashboard.callsu.net middleware (api_worker.py).

SPEC §5 — Operations API Integration
"""
import requests
import time
import os
import io
import pandas as pd
from datetime import datetime
import logging
from sqlalchemy import text
from src.ingestion import load_operations_data_from_uploads
from src.database import engine as db_engine

# ═══════════════════════════════════════════════════════════════════════════
#  Logging (same pattern as legacy api_worker.py)
# ═══════════════════════════════════════════════════════════════════════════
LOG_FILE = "data/api_sync.log"
os.makedirs("data", exist_ok=True)

logger = logging.getLogger("iwinback_worker")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILE, mode='w')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s', "%H:%M:%S"))
logger.addHandler(file_handler)


def log_msg(msg):
    print(msg)
    logger.info(msg)


# ═══════════════════════════════════════════════════════════════════════════
#  Box Configuration (loaded from .env)
# ═══════════════════════════════════════════════════════════════════════════
def _load_boxes():
    """Load iWinBack box config from environment variables.
    
    Expected .env format:
        IWINBACK_BOXES=bhfs2,bxq4c,bb4p7,baj7f,bdka4
        IWINBACK_bhfs2_URL=https://bhfs2.iwinback-saas.com
        IWINBACK_bhfs2_TOKEN=<token>
    """
    box_ids = os.environ.get("IWINBACK_BOXES", "").split(",")
    boxes = []
    for box_id in box_ids:
        box_id = box_id.strip()
        if not box_id:
            continue
        url = os.environ.get(f"IWINBACK_{box_id}_URL", "").strip()
        token = os.environ.get(f"IWINBACK_{box_id}_TOKEN", "").strip()
        if url and token:
            boxes.append({"id": box_id, "url": url, "token": token})
        else:
            log_msg(f"⚠️ Box '{box_id}' missing URL or TOKEN in .env — skipped")
    return boxes


# ═══════════════════════════════════════════════════════════════════════════
#  Auto-Discovery (informational logging)
# ═══════════════════════════════════════════════════════════════════════════
def discover_boxes():
    """Hit GET /api/clients and GET /api/brands on each box and log results."""
    boxes = _load_boxes()
    if not boxes:
        log_msg("❌ No iWinBack boxes configured in .env")
        return

    log_msg(f"🔍 Auto-discovering {len(boxes)} iWinBack boxes...\n")

    for box in boxes:
        session = _make_session(box)
        log_msg(f"── Box: {box['id']} ({box['url']}) ──")

        # Clients
        try:
            r = session.get(f"{box['url']}/api/clients", timeout=15)
            if r.status_code == 200:
                data = r.json()
                clients = data.get("data", data) if isinstance(data, dict) else data
                if isinstance(clients, list):
                    names = [c.get("name", c.get("id", "?")) for c in clients]
                    log_msg(f"   Clients: {', '.join(str(n) for n in names)}")
                else:
                    log_msg(f"   Clients: {clients}")
            else:
                log_msg(f"   Clients: HTTP {r.status_code}")
        except Exception as e:
            log_msg(f"   Clients: Error — {e}")

        # Brands
        try:
            r = session.get(f"{box['url']}/api/brands", timeout=15)
            if r.status_code == 200:
                data = r.json()
                brands = data.get("data", data) if isinstance(data, dict) else data
                if isinstance(brands, list):
                    names = [b.get("name", b.get("id", "?")) for b in brands]
                    log_msg(f"   Brands: {', '.join(str(n) for n in names)}")
                else:
                    log_msg(f"   Brands: {brands}")
            else:
                log_msg(f"   Brands: HTTP {r.status_code}")
        except Exception as e:
            log_msg(f"   Brands: Error — {e}")

        log_msg("")


def _make_session(box):
    """Create a requests.Session preconfigured for a given box."""
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {box['token']}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    return session


# ═══════════════════════════════════════════════════════════════════════════
#  Core Export Flow
# ═══════════════════════════════════════════════════════════════════════════
def _export_from_box(session, box, target_date):
    """Request, poll, and download a campaign_summary_v3 export from one box.

    Returns the raw bytes of the downloaded Excel file, or None on failure.
    """
    api_url = f"{box['url']}/api/exports"

    # 1. POST — create export
    payload = {
        "exportType": "campaign_summary_v3",
        "date_range": f"{target_date} - {target_date}",
    }
    try:
        r = session.post(api_url, json=payload, timeout=20)
        if r.status_code not in (200, 201):
            log_msg(f"   ❌ [{box['id']}] POST failed: HTTP {r.status_code}")
            return None
        resp = r.json()
        # Handle both {"id": ...} and {"data": {"id": ...}} response shapes
        job_id = resp.get("id") or resp.get("data", {}).get("id")
        if not job_id:
            log_msg(f"   ❌ [{box['id']}] No job ID in response: {resp}")
            return None
    except Exception as e:
        log_msg(f"   ❌ [{box['id']}] POST error: {e}")
        return None

    log_msg(f"   ⏳ [{box['id']}] Job {job_id} created, polling...")

    # 2. POLL — wait for status == "done"
    for attempt in range(120):
        try:
            r = session.get(f"{api_url}/{job_id}", timeout=20)
            if r.status_code == 200:
                data = r.json()
                inner = data.get("data", data) if isinstance(data, dict) else data
                status = inner.get("status", "unknown") if isinstance(inner, dict) else "unknown"
                if status == "done":
                    break
                if status in ("failed", "error"):
                    log_msg(f"   ❌ [{box['id']}] Job {job_id} failed on server.")
                    return None
                if attempt % 12 == 0:
                    log_msg(f"   ⏳ [{box['id']}] Status: '{status}' (attempt {attempt + 1}/120)")
            time.sleep(5)
        except Exception:
            time.sleep(5)
    else:
        log_msg(f"   ⚠️ [{box['id']}] Job {job_id} timed out.")
        return None

    # 3. DOWNLOAD
    try:
        dl = session.get(f"{api_url}/{job_id}/download", stream=True, timeout=30)
        if dl.status_code == 200:
            buf = io.BytesIO()
            for chunk in dl.iter_content(chunk_size=8192):
                buf.write(chunk)
            buf.seek(0)
            log_msg(f"   ✅ [{box['id']}] Downloaded ({buf.getbuffer().nbytes:,} bytes)")
            return buf
        else:
            log_msg(f"   ❌ [{box['id']}] Download HTTP {dl.status_code}")
            return None
    except Exception as e:
        log_msg(f"   ❌ [{box['id']}] Download error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
#  Main Entry Point
# ═══════════════════════════════════════════════════════════════════════════
def run_historical_pull(start_date, end_date):
    """Pull campaign_summary_v3 from all iWinBack boxes for a date range.

    Same signature as api_worker.run_historical_pull() for drop-in replacement.
    """
    boxes = _load_boxes()
    if not boxes:
        log_msg("❌ No iWinBack boxes configured. Check IWINBACK_* vars in .env")
        return

    log_msg(f"🚀 Starting iWinBack Native Sync from {start_date} to {end_date}")
    log_msg(f"   Boxes: {', '.join(b['id'] for b in boxes)}")

    date_list = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()
    base_dir = "data/raw/callsu_daily"

    def process_day(target_date):
        """Process a single date across all boxes."""
        # DB-level duplicate guard
        try:
            with db_engine.connect() as conn:
                result = conn.execute(
                    text("SELECT 1 FROM ops_telemarketing_data WHERE ops_date = :d LIMIT 1"),
                    {"d": target_date},
                )
                if result.fetchone():
                    log_msg(f"⏭️ Skipping {target_date} — data already exists in database")
                    return True
        except Exception as e:
            log_msg(f"⚠️ DB check failed for {target_date}, proceeding: {e}")

        log_msg(f"\n📅 Processing: {target_date}")

        # Save folder
        month_folder = target_date[:7]
        folder_path = os.path.join(base_dir, month_folder)
        os.makedirs(folder_path, exist_ok=True)

        # Collect Excel buffers from all boxes
        all_buffers = []
        for box in boxes:
            session = _make_session(box)
            buf = _export_from_box(session, box, target_date)
            if buf:
                all_buffers.append((box["id"], buf))

        if not all_buffers:
            log_msg(f"   ⚠️ No data returned for {target_date} from any box")
            return False

        # Merge all box results and save combined file
        combined_frames = []
        for box_id, buf in all_buffers:
            try:
                df = pd.read_excel(buf, engine="openpyxl", header=1)
                # Drop completely empty rows
                df = df.dropna(how="all")
                if not df.empty:
                    combined_frames.append(df)
            except Exception as e:
                log_msg(f"   ⚠️ [{box_id}] Parse error: {e}")

        if not combined_frames:
            log_msg(f"   ⚠️ All box downloads were empty for {target_date}")
            return False

        combined = pd.concat(combined_frames, ignore_index=True)
        combined_path = os.path.join(folder_path, f"{target_date}.xlsx")

        try:
            combined.to_excel(combined_path, index=False, engine="openpyxl")
            log_msg(f"   💾 Saved combined file: {combined_path} ({len(combined)} rows)")
        except Exception as e:
            log_msg(f"   ⚠️ Failed saving combined file: {e}")

        # Ingest via existing ops pipeline
        try:
            with open(combined_path, "rb") as f:
                load_operations_data_from_uploads([f])
            log_msg(f"   ✅ Ingested {target_date} via ETL pipeline!")
            return True
        except Exception as e:
            log_msg(f"   ❌ DB ingest error for {target_date}: {e}")
            return False

    # ── MAIN LOOP ──
    retry_queue = []
    for target_date in date_list:
        success = process_day(target_date)
        if not success:
            retry_queue.append(target_date)

    # ── RETRY LOOP ──
    if retry_queue:
        log_msg(f"\n🔄 RETRY QUEUE: {len(retry_queue)} failed day(s)...")
        time.sleep(10)

        final_failures = []
        for target_date in retry_queue:
            log_msg(f"\n🔁 RETRYING: {target_date}")
            success = process_day(target_date)
            if not success:
                final_failures.append(target_date)

        if final_failures:
            log_msg(f"\n🚨 CRITICAL: Failed after retry: {final_failures}")
            log_msg("⚠️ Please manually check these dates.")
        else:
            log_msg("\n🎉 All retries recovered!")

    log_msg("\n🏁 ALL REQUESTED PULLS COMPLETE!")
    return True
