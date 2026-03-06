import pandas as pd
from sqlalchemy import create_engine
import numpy as np

DB_URL = "postgresql://postgres:supersecretpassword@localhost:5432/drock"
_filter_engine = create_engine(DB_URL)

try:
    raw_ops = pd.read_sql("SELECT * FROM ops_telemarketing_data", _filter_engine)
except: 
    raw_ops = pd.DataFrame()

print(f"1. raw_ops shape after pull: {raw_ops.shape}")
print(f"ops_date values at start:\n{raw_ops['ops_date'].head()}")

if not raw_ops.empty: 
    raw_ops['ops_client'] = raw_ops['ops_client'].astype(str).str.strip()
    raw_ops['ops_brand'] = raw_ops['ops_brand'].astype(str).str.strip()

print(f"2. raw_ops shape after strip: {raw_ops.shape}")

selected_client = "All"
selected_brand = "All"

filtered_ops = raw_ops.copy() if not raw_ops.empty else pd.DataFrame()

if selected_client != "All":
    filtered_ops = filtered_ops[filtered_ops['ops_client'] == selected_client]
if selected_brand != "All":
    filtered_ops = filtered_ops[filtered_ops['ops_brand'] == selected_brand]

print(f"3. filtered_ops shape after client/brand filter: {filtered_ops.shape}")

start_month = "2026-01"
end_month = "2026-01"

if start_month and end_month:
    if not filtered_ops.empty and 'ops_date' in filtered_ops.columns:
        # What is the exact dtype of ops_date right now?
        print(f"Dtype of ops_date before Time Filter: {filtered_ops['ops_date'].dtype}")
        
        # Are there any NaN values?
        print(f"NaNs in ops_date before filter: {filtered_ops['ops_date'].isna().sum()}")
        
        filtered_ops = filtered_ops[(filtered_ops['ops_date'] >= start_month) & (filtered_ops['ops_date'] <= f"{end_month}-99")]
        
print(f"4. filtered_ops shape after Time filter: {filtered_ops.shape}")
print(f"5. Final sample:\n{filtered_ops.head(3)}")
