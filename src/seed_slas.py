import os
import sys
import pandas as pd
from sqlalchemy import text

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.database import engine

def seed_slas():
    query = """
    INSERT INTO contractual_volumes (client_name, brand_code, lifecycle, monthly_minimum_records)
    VALUES 
    ('LeoVegas Group', 'LV', 'RND', 6500),
    ('LeoVegas Group', 'LV', 'WB', 6500)
    ON CONFLICT (client_name, brand_code, lifecycle) DO UPDATE 
    SET monthly_minimum_records = EXCLUDED.monthly_minimum_records;
    """
    
    with engine.begin() as conn:
        conn.execute(text(query))
        
    print("SLA successfully seeded into contractual_volumes.")

if __name__ == "__main__":
    seed_slas()
