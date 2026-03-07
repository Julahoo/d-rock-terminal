import sys
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import text

# Add root directory to python path for local execution
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.database import engine as db_engine

def generate_benchmarks(start_date: str, end_date: str, period_name: str):
    """
    Generates 6-month historical daily averages based on newly extracted schema columns.
    """
    print(f"🔄 Generating Benchmarks for period: {period_name} ({start_date} to {end_date})")
    
    # Step A: Fetch data from ops_telemarketing_data
    query = f"""
        SELECT 
            ops_date, ops_brand as brand, country, extracted_lifecycle, 
            extracted_segment, extracted_engagement, 
            records, calls, kpi2_logins, conversions, 
            (d_plus + d_neutral + d_minus) as deliveries,
            (cost_sip + cost_sms + cost_email) as telecom_cost
        FROM ops_telemarketing_data
        WHERE ops_date >= '{start_date}' AND ops_date <= '{end_date}'
    """
    
    try:
        df = pd.read_sql(query, db_engine)
    except Exception as e:
        print(f"❌ Failed to fetch data: {e}")
        return
        
    if df.empty:
        print("⚠️ No data found for the specified date range.")
        return
        
    print(f"📊 Loaded {len(df)} raw ops records.")
    
    # Fill NAs
    signature_cols = ['brand', 'country', 'extracted_lifecycle', 'extracted_segment', 'extracted_engagement']
    df[signature_cols] = df[signature_cols].fillna("UNKNOWN")
    
    # Step B: Two-step aggregation
    
    # First Grouping: Group by ops_date AND the signature
    daily_totals = df.groupby(['ops_date'] + signature_cols).agg({
        'records': 'sum',
        'calls': 'sum',
        'kpi2_logins': 'sum',
        'conversions': 'sum',
        'deliveries': 'sum',
        'telecom_cost': 'sum'
    }).reset_index()
    
    # Second Grouping: Group by signature and calculate mean (Daily Averages)
    benchmarks = daily_totals.groupby(signature_cols).agg({
        'records': 'mean',
        'calls': 'mean',
        'kpi2_logins': 'mean',
        'conversions': 'mean',
        'deliveries': 'mean',
        'telecom_cost': 'mean'
    }).reset_index()
    
    # Step C: Rename metric columns mapped to schema
    benchmarks = benchmarks.rename(columns={
        'records': 'avg_daily_records',
        'calls': 'avg_daily_calls',
        'kpi2_logins': 'avg_daily_logins',
        'conversions': 'avg_daily_conversions',
        'deliveries': 'avg_daily_deliveries',
        'telecom_cost': 'avg_daily_telecom_cost'
    })
    
    import numpy as np
    benchmarks['avg_daily_true_cac'] = np.where(
        benchmarks['avg_daily_conversions'] > 0,
        benchmarks['avg_daily_telecom_cost'] / benchmarks['avg_daily_conversions'],
        0.0
    )
    
    # Step D: Add the period_name column
    benchmarks['benchmark_period'] = period_name
    
    # Step E: Idempotency check - delete existing period
    try:
        with db_engine.begin() as conn:
            conn.execute(text("DELETE FROM ops_historical_benchmarks WHERE benchmark_period = :period"), {'period': period_name})
    except Exception as e:
         print(f"⚠️ Could not delete existing benchmarks: {e}")
        
    # Step F: Save fresh benchmarks to PostgreSQL
    try:
        benchmarks.to_sql('ops_historical_benchmarks', db_engine, if_exists='append', index=False)
        print(f"✅ Successfully generated and saved {len(benchmarks)} benchmark signatures for {period_name}.")
    except Exception as e:
        print(f"❌ Failed to insert benchmarks into database: {e}")

if __name__ == "__main__":
    # Test execution for a dummy window
    generate_benchmarks("2026-01-01", "2026-06-30", "H1 2026")
