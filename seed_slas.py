import os
import sys
from sqlalchemy import text

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from src.database import engine

def seed_slas():
    print("⚙️ Seeding Contractual SLAs...")
    
    # Client Name, Brand Code, Lifecycle, Monthly Minimum
    slas = [
        # Limitless (LIM -> YW, MRO)
        ("Limitless", "YW", "RND", 5000),
        ("Limitless", "YW", "WB", 5000),
        ("Limitless", "MRO", "RND", 5000),
        ("Limitless", "MRO", "WB", 5000),
        
        # Reliato (REL -> BAH)
        ("Reliato", "BAH", "RND", 2500),
        ("Reliato", "BAH", "WB", 10000),
        
        # Simplicity Malta Limited (SIM -> VJ, YU)
        ("Simplicity Malta Limited", "VJ", "RND", 6500),
        ("Simplicity Malta Limited", "VJ", "WB", 6500),
        ("Simplicity Malta Limited", "YU", "RND", 6500),
        ("Simplicity Malta Limited", "YU", "WB", 6500),
        
        # Leo Vegas (LV -> LV, BETUK, BETMGM, EXPEKT, GOGO, ROYALPANDA)
        # Assuming the 6500 applies to the master tag for now, or spread? The image says "LV RND 6500". 
        ("LeoVegas Group", "LV", "RND", 6500),
        ("LeoVegas Group", "LV", "WB", 6500),
        ("LeoVegas Group", "BETUK", "RND", 6500),
        ("LeoVegas Group", "BETUK", "WB", 6500),
        ("LeoVegas Group", "BETMGM", "RND", 6500),
        ("LeoVegas Group", "BETMGM", "WB", 6500),
        ("LeoVegas Group", "EXPEKT", "RND", 6500),
        ("LeoVegas Group", "EXPEKT", "WB", 6500),
        ("LeoVegas Group", "GOGO", "RND", 6500),
        ("LeoVegas Group", "GOGO", "WB", 6500),
        ("LeoVegas Group", "ROYALPANDA", "RND", 6500),
        ("LeoVegas Group", "ROYALPANDA", "WB", 6500),
        
        # Offside (OFF -> LTRB/ROJAB) - LTRB is now Latribet, ROJAB is Rojabet
        ("Offside Gaming", "Latribet", "WB", 5000),
        ("Offside Gaming", "Rojabet", "WB", 5000),
        
        # Magico Games (INSP -> INSP)
        ("Magico Games/Interspin", "INSP", "WB", 5000),
        
        # PressEnter (PE -> NTR)
        ("PressEnter", "NTR", "RND", 6000),
        ("PressEnter", "NTR", "WB", 6000)
    ]
    
    try:
        with engine.begin() as conn:
            # Clear existing to prevent duplicates during testing
            conn.execute(text("DELETE FROM contractual_slas"))
            
            for client, brand, lifecycle, minimum in slas:
                # Default target CAC and Conv% can be null or 0 for now
                conn.execute(text("""
                    INSERT INTO contractual_slas (client_name, brand_code, lifecycle, monthly_minimum_records)
                    VALUES (:c, :b, :l, :m)
                """), {"c": client, "b": brand, "l": lifecycle, "m": minimum})
        print("✅ Contractual SLAs Seeded Successfully!")
    except Exception as e:
        print(f"❌ Failed to seed SLAs: {e}")

if __name__ == "__main__":
    seed_slas()
