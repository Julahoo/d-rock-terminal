import os
import pandas as pd
from sqlalchemy import create_engine

# 1. Connect to DB
DB_URL = "postgresql://postgres:supersecretpassword@localhost:5432/drock"
engine = create_engine(DB_URL)

try:
    print("Reading ops_telemarketing_data...")
    raw_ops = pd.read_sql("SELECT * FROM ops_telemarketing_data", engine)
    print(f"raw_ops shape: {raw_ops.shape}")
    raw_fin = pd.read_sql("SELECT * FROM raw_financial_data", engine)
    print(f"raw_fin shape: {raw_fin.shape}")
except Exception as e:
    print(f"Exception: {e}")
    raw_ops = pd.DataFrame()
    raw_fin = pd.DataFrame()

# 2. Simulate User Filter Selection
selected_client = "All"
selected_brand = "All"
start_month = "2024-05"
end_month = "2026-02"

# 3. Apply Filters
filtered_ops = raw_ops.copy() if not raw_ops.empty else pd.DataFrame()

print(f"Before client filter: {filtered_ops.shape}")
if selected_client != "All":
    if not filtered_ops.empty and 'ops_client' in filtered_ops.columns: 
        filtered_ops = filtered_ops[filtered_ops['ops_client'] == selected_client]
        
print(f"Before brand filter: {filtered_ops.shape}")
if selected_brand != "All":
    if not filtered_ops.empty and 'ops_brand' in filtered_ops.columns: 
        filtered_ops = filtered_ops[filtered_ops['ops_brand'] == selected_brand]

print(f"Before date filter: {filtered_ops.shape}")
if start_month and end_month:
    if not filtered_ops.empty and 'ops_date' in filtered_ops.columns:
        # Note: Added -99 to end_month to include all dates in that month
        filtered_ops = filtered_ops[(filtered_ops['ops_date'] >= start_month) & (filtered_ops['ops_date'] <= f"{end_month}-99")]

print(f"After date filter: {filtered_ops.shape}")

# 4. Check for ops_df rendering
if not filtered_ops.empty:
    ops_df = filtered_ops.copy()
    print("ops_df successfully created! Count: ", len(ops_df))
else:
    print("ops_df is EMPTY at rendering stage.")
