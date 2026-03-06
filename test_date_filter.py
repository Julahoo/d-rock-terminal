import pandas as pd
from sqlalchemy import create_engine
DB_URL = "postgresql://postgres:supersecretpassword@localhost:5432/drock"
_filter_engine = create_engine(DB_URL)

try:
    print("Hydrating raw_ops_df...")
    raw_ops = pd.read_sql("SELECT * FROM ops_telemarketing_data", _filter_engine)
    print(f"raw_ops shape: {raw_ops.shape}")
except Exception as e:
    print(f"FAILED: {e}")
    raw_ops = pd.DataFrame()

# Clean hidden whitespace
if not raw_ops.empty: 
    raw_ops['ops_client'] = raw_ops['ops_client'].astype(str).str.strip()
    raw_ops['ops_brand'] = raw_ops['ops_brand'].astype(str).str.strip()

filtered_ops = raw_ops.copy()

print("Applying Time Frame Filter exactly as app.py...")
start_month = "2024-05"
end_month = "2026-02"

print(f"ops_date dtypes: {filtered_ops['ops_date'].dtype}")
print(f"First 5 ops_date values:")
print(filtered_ops['ops_date'].head())

try:
    filtered_ops = filtered_ops[(filtered_ops['ops_date'] >= start_month) & (filtered_ops['ops_date'] <= f"{end_month}-99")]
    print(f"After date filter: {filtered_ops.shape}")
except Exception as e:
    print(f"Date filter failed: {e}")

