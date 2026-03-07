from src.database import engine
from sqlalchemy import text
import pandas as pd

def merge_brands():
    with engine.begin() as conn:
        # 1. Merge Royal Panda
        conn.execute(text("UPDATE ops_telemarketing_data SET ops_brand = 'RP' WHERE ops_brand = 'ROYALPANDA'"))
        conn.execute(text("UPDATE ops_telemarketing_snapshots SET ops_brand = 'RP' WHERE ops_brand = 'ROYALPANDA'"))
        conn.execute(text("UPDATE ops_historical_benchmarks SET brand = 'RP' WHERE brand = 'ROYALPANDA'"))
        conn.execute(text("DELETE FROM client_mapping WHERE brand_code = 'ROYALPANDA'"))
        
        # 2. Merge Expekt
        conn.execute(text("UPDATE ops_telemarketing_data SET ops_brand = 'EX' WHERE ops_brand = 'EXPEKT'"))
        conn.execute(text("UPDATE ops_telemarketing_snapshots SET ops_brand = 'EX' WHERE ops_brand = 'EXPEKT'"))
        conn.execute(text("UPDATE ops_historical_benchmarks SET brand = 'EX' WHERE brand = 'EXPEKT'"))
        conn.execute(text("DELETE FROM client_mapping WHERE brand_code = 'EXPEKT'"))
        
        # 3. Merge Rojabet
        conn.execute(text("UPDATE ops_telemarketing_data SET ops_brand = 'ROJA' WHERE ops_brand = 'ROJB'"))
        conn.execute(text("UPDATE ops_telemarketing_snapshots SET ops_brand = 'ROJA' WHERE ops_brand = 'ROJB'"))
        conn.execute(text("UPDATE ops_historical_benchmarks SET brand = 'ROJA' WHERE brand = 'ROJB'"))
        conn.execute(text("DELETE FROM client_mapping WHERE brand_code = 'ROJB'"))

        # 4. Fix CASINODAYS Name
        conn.execute(text("UPDATE client_mapping SET brand_name = 'CasinoDays' WHERE brand_code = 'CASINODAYS'"))
        
        # 5. Fix Bahibi Name (from Hahibi)
        conn.execute(text("UPDATE client_mapping SET brand_name = 'Bahibi' WHERE brand_code = 'BHB'"))
        
        # 6. Ensure RP and EX have correct readable names instead of relying on default Init_DB
        conn.execute(text("UPDATE client_mapping SET brand_name = 'Royal Panda' WHERE brand_code = 'RP'"))
        conn.execute(text("UPDATE client_mapping SET brand_name = 'Expekt' WHERE brand_code = 'EX'"))
        conn.execute(text("UPDATE client_mapping SET brand_name = 'Rojabet' WHERE brand_code = 'ROJA'"))

    # Print the resulting directory
    query = "SELECT client_name as Client, COALESCE(brand_name, 'Unknown') as Brand, brand_code as Tag FROM client_mapping ORDER BY client_name, brand_code;"
    df = pd.read_sql(query, engine)
    
    print("\n=== Current Brand Directory ===")
    print(df.to_string(index=False))
    print("===============================\n")

if __name__ == "__main__":
    merge_brands()
