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
        
        # Vectorized Date Suffix Stripping
        core_sig = df['Campaign Name'].astype(str)
        core_sig = core_sig.str.replace(r'[_-]\d{4}[_-]\d{2}[_-]\d{2}$', '', regex=True)
        core_sig = core_sig.str.replace(r'[_-]\d{2}[A-Z]{3}\d{4}$', '', regex=True)
        core_sig = core_sig.str.replace(r'[_-]\d{4}[_-]\d{2}$', '', regex=True)
        
        df['Core_Signature'] = core_sig
        df['Base Campaign'] = df['Core_Signature']
        
        # Vectorized Lifecycle Extraction
        camp_upper = core_sig.str.upper()
        df['ops_lifecycle'] = 'UNKNOWN'
        df.loc[camp_upper.str.contains(r'[-_ ]WB', regex=True, na=False), 'ops_lifecycle'] = 'WB'
        df.loc[camp_upper.str.contains(r'[-_ ]RND', regex=True, na=False), 'ops_lifecycle'] = 'RND'
        
        # Standard Strategy Signatures
        df['Strategy_Signature'] = (
            df['ops_brand'].fillna('UNKNOWN') + "-" +
            df['country'].fillna('UNKNOWN') + "-" +
            df['extracted_lifecycle'].fillna('UNKNOWN') + "-" +
            df['extracted_engagement'].fillna('UNKNOWN')
        )
        
        # Vectorized Granular Campaign Signature
        camp_str = df['Campaign Name'].astype(str)
        norm_camp = camp_str.str.replace("-", "_")
        mask_3 = norm_camp.str.contains(r'_\d+_\d+_\d+$', regex=True)
        mask_2 = norm_camp.str.contains(r'_\d+_\d+$', regex=True)
        
        sig = camp_str.copy()
        sig.loc[(mask_3 | mask_2) & camp_str.str.contains('_')] = camp_str.str.rsplit('_', n=1).str[0]
        df['campaign_signature'] = sig

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

def materialize_crm_intelligence():
    print(f"[{datetime.now().isoformat()}] Starting CRM Intelligence Pre-Computation...")
    try:
        df = pd.read_sql("SELECT * FROM raw_financial_data", db_engine)
        if df.empty:
            print("No data found in raw_financial_data.")
            return False

        # Apply UI standardizations 
        df.rename(columns={"player_id": "id"}, inplace=True)
        if 'client' in df.columns: df['client'] = df['client'].astype(str).str.strip()
        if 'brand' in df.columns: df['brand'] = df['brand'].astype(str).str.strip()

        from src.analytics.base import generate_cohort_matrix, generate_ltv_curves, generate_retention_heatmap, generate_tier_summary
        
        # 1. Cohort Matrix
        print(" > Computing Cohort Matrix...")
        matrices = generate_cohort_matrix(df)
        cohort_records = []
        for brand, matrix_df in matrices.items():
            cohort_records.append({
                "brand": brand,
                "matrix_json": matrix_df.to_json(orient="split")
            })
        if cohort_records:
            pd.DataFrame(cohort_records).to_sql('cache_cohort_matrices', db_engine, if_exists='replace', index=False)
            
        # 2. LTV Curves
        print(" > Computing LTV Curves...")
        ltv_fig = generate_ltv_curves(df)
        ltv_json = ltv_fig.to_json() if ltv_fig else None
        
        # 3. Retention Heatmap
        print(" > Computing Retention Heatmap...")
        heatmap_fig = generate_retention_heatmap(df)
        heatmap_json = heatmap_fig.to_json() if heatmap_fig else None
        
        figs_df = pd.DataFrame([
            {"visualization": "ltv_curves", "figure_json": ltv_json},
            {"visualization": "retention_heatmap", "figure_json": heatmap_json}
        ])
        figs_df.to_sql('cache_financial_figures', db_engine, if_exists='replace', index=False)
        
        # 4. VIP Tier Summary
        print(" > Computing VIP Tier Summary...")
        active_brands = sorted([b for b in df["brand"].unique() if b != "Combined" and pd.notna(b)])
        
        def _get_true_latest(brand_df):
            if "report_month" in brand_df.columns:
                return brand_df["report_month"].max()
            return pd.Timestamp.today().strftime('%Y-%m')

        tier_records = []
        latest_all = _get_true_latest(df)
        tier_rfm_all = generate_tier_summary(df, latest_all)
        tier_records.append({
            "brand": "Combined",
            "tier_json": tier_rfm_all.to_json(orient="split")
        })
        
        for brand in active_brands:
            b_df = df[df["brand"] == brand]
            latest_b = _get_true_latest(b_df)
            tier_rfm_b = generate_tier_summary(b_df, latest_b)
            tier_records.append({
                "brand": brand,
                "tier_json": tier_rfm_b.to_json(orient="split")
            })
            
        if tier_records:
            pd.DataFrame(tier_records).to_sql('cache_tier_summaries', db_engine, if_exists='replace', index=False)
            
        print(f"[{datetime.now().isoformat()}] Successfully materialized CRM Intelligence Caches.")
        return True
    except Exception as e:
        print(f"❌ Failed to materialize CRM Intelligence: {e}")
        return False

def main():
    print(f"🚀 ETL Worker starting at {datetime.now().isoformat()}")
    success_base = materialize_ops_base_view()
    if success_base:
        materialize_dashboard_pulse()
        
    materialize_ops_snapshots_view()
    materialize_crm_intelligence()
    print(f"🏁 ETL Worker complete at {datetime.now().isoformat()}")

if __name__ == "__main__":
    main()
