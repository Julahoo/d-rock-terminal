import os
import sys
from sqlalchemy import text

# Ensure we can import from src
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from src.database import engine

def heal_formats():
    print("⚙️ Aligning Brand Registry Formats...")
    try:
        with engine.begin() as conn:
            # Force LeoVegas format
            conn.execute(text("UPDATE client_mapping SET financial_format = 'LeoVegas' WHERE client_name = 'LeoVegas Group'"))
            # Force Offside format
            conn.execute(text("UPDATE client_mapping SET financial_format = 'Offside' WHERE client_name ILIKE '%Offside%'"))

        print("✅ Financial formats successfully aligned to parent clients!")
    except Exception as e:
        print(f"❌ Healing failed: {e}")

if __name__ == "__main__":
    heal_formats()
