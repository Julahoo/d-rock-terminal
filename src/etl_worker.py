import os
import sys
import argparse
import pandas as pd
from datetime import datetime
import re as _re

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import execute_query, engine as db_engine

def materialize_ops_base_view():
    print(f"[{datetime.now().isoformat()}] Starting Operations Base Regex Materialization...")
    try:
        df = pd.read_sql("SELECT * FROM ops_telemarketing_data", db_engine)
        if df.empty:
            print("No data found in ops_telemarketing_data.")
            return False

        # Clean generic strings
        if 'ops_client' in df.columns: df['ops_client'] = df['ops_client'].astype(str).str.strip()
        if 'ops_brand' in df.columns: df['ops_brand'] = df['ops_brand'].astype(str).str.strip()
        
        # UI-expected renames
        df.rename(columns={
            "campaign_name": "Campaign Name", "records": "Records", "total_cost": "Total_Campaign_Cost",
            "conversions": "KPI1-Conv.", "kpi2_logins": "KPI2-Login", "li_pct": "LI%",
            "true_cac": "True_CAC", "calls": "Calls", "d_total": "D", "d_plus": "D+",
            "d_minus": "D-", "d_ratio": "D Ratio", "tech_issues": "T", "am": "AM",
            "dnc": "DNC", "na": "NA", "dx": "DX", "wn": "WN"
        }, inplace=True)
        
        # Regex logic
        def _strip_date_suffix(name):
            n = str(name)
            n = _re.sub(r'[_-]\d{4}[_-]\d{2}[_-]\d{2}$', '', n)
            n = _re.sub(r'[_-]\d{2}[A-Z]{3}\d{4}$', '', n)
            n = _re.sub(r'[_-]\d{4}[_-]\d{2}$', '', n)
            return n
        
        df['Core_Signature'] = df['Campaign Name'].apply(_strip_date_suffix)
        df['Base Campaign'] = df['Core_Signature']
        
        # Lifecycle
        df['ops_lifecycle'] = df['Campaign Name'].apply(
            lambda x: 'WB' if '-WB' in str(x).upper() or '_WB' in str(x).upper() or ' WB' in str(x).upper() else (
                'RND' if '-RND' in str(x).upper() or '_RND' in str(x).upper() or ' RND' in str(x).upper() else 'UNKNOWN'
            )
        )
        
        # Standard Strategy Signatures
        df['Strategy_Signature'] = (
            df['ops_brand'].fillna('UNKNOWN') + "-" +
            df['country'].fillna('UNKNOWN') + "-" +
            df['extracted_lifecycle'].fillna('UNKNOWN') + "-" +
            df['extracted_engagement'].fillna('UNKNOWN')
        )
        
        # Granular Campaign Signature
        def get_sig(c):
            c = str(c)
            parts = c.replace("-", "_").split('_')
            if len(parts) >= 3 and parts[-3].isdigit() and parts[-2].isdigit() and parts[-1].isdigit():
                return "_".join(c.split('_')[:-1]) if len(c.split('_')) > 1 else c
            if len(parts) >= 2 and parts[-2].isdigit() and parts[-1].isdigit():
                return "_".join(c.split('_')[:-1]) if len(c.split('_')) > 1 else c
            return c
        
        df['campaign_signature'] = df['Campaign Name'].apply(get_sig)

        df.to_sql('ops_telemarketing_data_materialized', db_engine, if_exists='replace', index=False)
        print(f"[{datetime.now().isoformat()}] Successfully materialized ops_telemarketing_data_materialized ({len(df)} rows).")
        return True
    except Exception as e:
        print(f"❌ Failed to materialize ops base view: {e}")
        return False

def materialize_ops_snapshots_view():
    print(f"[{datetime.now().isoformat()}] Starting Operations Snapshots Materialization...")
    try:
        df = pd.read_sql("SELECT * FROM ops_telemarketing_snapshots", db_engine)
        if df.empty:
            print("No data found in ops_telemarketing_snapshots.")
            return False

        df['Strategy_Signature'] = (
            df['ops_brand'].fillna('UNKNOWN') + "-" +
            df['country'].fillna('UNKNOWN') + "-" +
            df['extracted_lifecycle'].fillna('UNKNOWN') + "-" +
            df['extracted_engagement'].fillna('UNKNOWN')
        )
        
        df.to_sql('ops_telemarketing_snapshots_materialized', db_engine, if_exists='replace', index=False)
        print(f"[{datetime.now().isoformat()}] Successfully materialized ops_telemarketing_snapshots_materialized ({len(df)} rows).")
        return True
    except Exception as e:
        print(f"❌ Failed to materialize ops snapshots view: {e}")
        return False

def materialize_dashboard_pulse():
    print(f"[{datetime.now().isoformat()}] Starting Dashboard Pulse Materialization...")
    try:
        # Load the base operations data
        df = pd.read_sql("SELECT ops_date, extracted_engagement, records, conversions, kpi2_logins FROM ops_telemarketing_data", db_engine)
        if df.empty:
            print("No data found in ops_telemarketing_data.")
            return

        # Ensure ops_date is datetime
        df['ops_date'] = pd.to_datetime(df['ops_date'], errors='coerce')
        
        # We need daily sums by engagement
        # Convert necessary columns to numeric
        for col in ['records', 'conversions', 'kpi2_logins']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Normalize column names to match what pulse matrix expects, though we are doing it in SQL
        daily = df.groupby(['ops_date', 'extracted_engagement']).agg(
            total_records=('records', 'sum'),
            total_conversions=('conversions', 'sum'),
            total_logins=('kpi2_logins', 'sum')
        ).reset_index()

        daily = daily.dropna(subset=['ops_date'])

        # Write to database (replace existing table)
        daily.to_sql('dashboard_pulse_matrix', db_engine, if_exists='replace', index=False)
        print(f"[{datetime.now().isoformat()}] Successfully materialized dashboard_pulse_matrix ({len(daily)} rows).")

    except Exception as e:
        print(f"❌ Failed to materialize dashboard pulse: {e}")

def main():
    print(f"🚀 ETL Worker starting at {datetime.now().isoformat()}")
    success_base = materialize_ops_base_view()
    if success_base:
        materialize_dashboard_pulse()
        
    materialize_ops_snapshots_view()
    print(f"🏁 ETL Worker complete at {datetime.now().isoformat()}")

if __name__ == "__main__":
    main()
