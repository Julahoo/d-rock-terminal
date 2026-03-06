import pandas as pd
try:
    from src.database import engine as _filter_engine
    print("Engine imported successfully.")
    
    try:
        print("Executing read_sql...")
        raw_ops = pd.read_sql("SELECT * FROM ops_telemarketing_data", _filter_engine)
        print(f"Success! Shape: {raw_ops.shape}")
    except Exception as e:
        print(f"READ_SQL FAILED: {e}")
except Exception as e:
    print(f"IMPORT FAILED: {e}")
