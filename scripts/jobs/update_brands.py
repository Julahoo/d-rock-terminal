from src.database import engine
from sqlalchemy import text
import pandas as pd
from src.ingestion import BRAND_CODE_MAP

def update_and_show():
    # 1. First trigger the init_db to seed the client_names from CLIENT_HIERARCHY
    from src.database import init_db
    init_db()
    
    # 2. Update brand_names based on the reversed BRAND_CODE_MAP
    # E.g. BRAND_CODE_MAP maps 'WETIGO' to 'WG'. So reverse is 'WG' -> 'WETIGO'.
    # For nicer casing, we will just use the available tags. 
    # But explicitly set the 3 new ones as requested to proper casing:
    explicit_names = {
        'WG': 'Wetigo',
        'BHB': 'Hahibi',
        'PE': 'NitroCasino',
        'VJ': 'Vera John',
        'YU': 'Yuugado',
        'BOA': 'Boabet',
        'YW': 'YouWin',
        'MRO': 'Mr Oyun',
        'BAH': 'Bahigo',
        'HL': 'Happy Luke',
        'LCH': 'Live Casino House',
        'RP': 'Royal Panda',
        'LV': 'Leo Vegas',
        'LTRB': 'Latribet',
        'PP': 'PowerPlay'
    }
    
    with engine.begin() as conn:
        for tag, brand_name in explicit_names.items():
            conn.execute(
                text("UPDATE client_mapping SET brand_name = :n WHERE brand_code = :c"),
                {"n": brand_name, "c": tag}
            )
            
    # 3. Print the resulting directory
    query = "SELECT client_name as Client, COALESCE(brand_name, 'Unknown') as Brand, brand_code as Tag FROM client_mapping ORDER BY client_name, brand_code;"
    df = pd.read_sql(query, engine)
    
    print("\n=== Current Brand Directory ===")
    print(df.to_string(index=False))
    print("===============================\n")

if __name__ == "__main__":
    update_and_show()
