"""
cron_callsu.py — Automated ETL for CallsU Operations Data → PostgreSQL

This script is designed to be run as a cron job or scheduled task.
It reads CSV/XLSX files from a configured directory, parses campaign strings,
extracts all omnichannel metrics, and upserts into ops_telemarketing_data.

Usage:
    python -m src.cron_callsu --input-dir ./data/raw/ops
"""
import os
import sys
import argparse
import pandas as pd
from datetime import datetime

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import execute_query, engine as db_engine


def process_ops_files(input_dir: str, dry_run: bool = False):
    """Scan input_dir for CSV/XLSX files, parse, and upsert into PostgreSQL."""
    
    if not os.path.isdir(input_dir):
        print(f"❌ Directory not found: {input_dir}")
        return

    # Fetch live registry from DB
    try:
        mapping_df = pd.read_sql("SELECT brand_code, brand_name, client_name FROM client_mapping", db_engine)
        live_map = {row['brand_code']: {'client': row['client_name'], 'brand': row['brand_name'] if pd.notnull(row.get('brand_name')) else row['brand_code']} for _, row in mapping_df.iterrows()}
    except:
        live_map = {}

    files = [f for f in os.listdir(input_dir) if f.lower().endswith(('.csv', '.xlsx'))]
    if not files:
        print(f"⚠️ No CSV/XLSX files found in {input_dir}")
        return

    print(f"📂 Found {len(files)} file(s) in {input_dir}")
    total_inserted = 0

    for filename in files:
        filepath = os.path.join(input_dir, filename)
        print(f"\n--- Processing: {filename} ---")

        try:
            if filename.lower().endswith(".xlsx"):
                df = pd.read_excel(filepath, engine="openpyxl")
            else:
                try:
                    df = pd.read_csv(filepath, encoding="utf-8")
                except Exception:
                    df = pd.read_csv(filepath, encoding="ISO-8859-1")
        except Exception as e:
            print(f"❌ CRASH ON FILE {filename}: {e}")
            continue

        # INDESTRUCTIBLE CLEANING
        df.columns = [str(c).replace('\ufeff', '').replace('"', '').strip() for c in df.columns]

        if "Campaign Name" not in df.columns:
            print(f"⚠️ SKIPPED {filename}: 'Campaign Name' not found!")
            continue

        # Extract Core Metrics
        ops_metrics = ["Calls", "KPI1-Conv.", "Cost Caller", "Cost SIP", "Cost SMS", "Cost Email", 
                       "D", "D+", "D-", "D Ratio", "T", "AM", "DNC", "NA", "DX", "WN"]
        for col in ops_metrics:
            if col not in df.columns:
                df[col] = 0.0
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        # Calculate True CAC
        df["Total_Campaign_Cost"] = df["Cost Caller"] + df["Cost SIP"] + df["Cost SMS"] + df["Cost Email"]
        df["True_CAC"] = df.apply(lambda x: x["Total_Campaign_Cost"] / x["KPI1-Conv."] if x["KPI1-Conv."] > 0 else 0, axis=1)

        # Build records for upsert
        records_to_insert = []
        for _, row in df.iterrows():
            campaign = str(row.get("Campaign Name", "UNKNOWN"))
            tokens = [t for t in campaign.upper().replace('_', '-').split('-') if t]
            tag = tokens[0] if len(tokens) > 0 else "UNKNOWN"
            mapped_info = live_map.get(tag, {})
            client = mapped_info.get('client', "UNKNOWN")
            brand_name = mapped_info.get('brand', tag)
            
            start_date = tokens[8] if len(tokens) > 8 else "UNKNOWN"
            calls = int(row.get("Calls", 0))
            convs = int(row.get("KPI1-Conv.", 0))
            total_cost = float(row.get("Total_Campaign_Cost", 0))
            true_cac = float(row.get("True_CAC", 0))

            records_to_insert.append({
                "campaign_name": f"{campaign}_{start_date}", 
                "ops_client": client,
                "ops_brand": brand_name,
                "ops_date": start_date,
                "calls": calls,
                "conversions": convs,
                "total_cost": total_cost,
                "true_cac": true_cac,
                "d_total": int(row["D"]),
                "d_plus": int(row["D+"]),
                "d_minus": int(row["D-"]),
                "d_ratio": float(row["D Ratio"]),
                "tech_issues": int(row["T"]),
                "am": int(row["AM"]),
                "dnc": int(row["DNC"]),
                "na": int(row["NA"]),
                "dx": int(row["DX"]),
                "wn": int(row["WN"])
            })

        print(f"📊 Parsed {len(records_to_insert)} campaign rows from {filename}")

        if dry_run:
            print("🔍 DRY RUN — skipping database insert")
            continue

        # Upsert into PostgreSQL
        inserted = 0
        for rec in records_to_insert:
            try:
                execute_query(
                    """INSERT INTO ops_telemarketing_data 
                       (campaign_name, ops_client, ops_brand, ops_date, calls, conversions, 
                        total_cost, true_cac, d_total, d_plus, d_minus, d_ratio, 
                        tech_issues, am, dnc, na, dx, wn)
                       VALUES (:campaign_name, :ops_client, :ops_brand, :ops_date, :calls, :conversions,
                               :total_cost, :true_cac, :d_total, :d_plus, :d_minus, :d_ratio,
                               :tech_issues, :am, :dnc, :na, :dx, :wn)
                       ON CONFLICT (campaign_name) DO UPDATE SET
                           calls = EXCLUDED.calls, conversions = EXCLUDED.conversions,
                           total_cost = EXCLUDED.total_cost, true_cac = EXCLUDED.true_cac,
                           d_total = EXCLUDED.d_total, d_plus = EXCLUDED.d_plus, 
                           d_minus = EXCLUDED.d_minus, d_ratio = EXCLUDED.d_ratio,
                           tech_issues = EXCLUDED.tech_issues, am = EXCLUDED.am, 
                           dnc = EXCLUDED.dnc, na = EXCLUDED.na, dx = EXCLUDED.dx, wn = EXCLUDED.wn
                    """,
                    rec
                )
                inserted += 1
            except Exception as e:
                print(f"⚠️ Failed to insert {rec['campaign_name']}: {e}")

        total_inserted += inserted
        print(f"✅ Upserted {inserted}/{len(records_to_insert)} rows from {filename}")

    print(f"\n🏁 ETL Complete: {total_inserted} total rows upserted across {len(files)} file(s)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CallsU Operations ETL → PostgreSQL")
    parser.add_argument("--input-dir", default="./data/raw/ops", help="Directory containing CSV/XLSX ops files")
    parser.add_argument("--dry-run", action="store_true", help="Parse only, don't write to DB")
    args = parser.parse_args()

    print(f"🚀 CallsU ETL starting at {datetime.now().isoformat()}")
    process_ops_files(args.input_dir, dry_run=args.dry_run)
