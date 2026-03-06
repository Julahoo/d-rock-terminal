import pandas as pd
from sqlalchemy import create_engine
DB_URL = "postgresql://postgres:supersecretpassword@localhost:5432/drock"
_filter_engine = create_engine(DB_URL)

try:
    print("Trying `ops_telemarketing_data`...")
    df1 = pd.read_sql("SELECT * FROM ops_telemarketing_data", _filter_engine)
    print("Success 1!")
    
    print("Trying `ops_telemarketing_snapshots`...")
    df2 = pd.read_sql("SELECT * FROM ops_telemarketing_snapshots", _filter_engine)
    print("Success 2!")
except Exception as e:
    print(f"Exception caught: {e}")
