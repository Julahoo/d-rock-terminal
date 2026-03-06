import pandas as pd
from sqlalchemy import create_engine
import os

engine = create_engine(os.environ.get("DATABASE_URL", "postgresql://user:password@db:5432/financial_db"))
try:
    ops = pd.read_sql("SELECT DISTINCT ops_date FROM ops_telemarketing_data", engine)
    fin = pd.read_sql("SELECT DISTINCT report_month FROM raw_financial_data", engine)
    print("OPS DATES:", ops['ops_date'].tolist())
    print("FIN DATES:", fin['report_month'].tolist())
except Exception as e:
    print("Error:", e)
