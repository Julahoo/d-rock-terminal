import os
import sys
from sqlalchemy import text

# Ensure we can import from src
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from src.database import engine

def heal_database():
    print("🩹 Starting Live Database Healing...")

    mappings = {
        "LIM": "Limitless",
        "REL": "Reliato",
        "SIM": "Simplicity Malta Limited",
        "RHN": "Rhino",
        "PP": "PowerPlay",
        "INSP": "Magico Games/Interspin",
        "PE": "PressEnter",
        "LV": "LeoVegas Group"
    }

    brand_mappings = {
        "YU": "Yuugado",
        "VJ": "VeraJohn",
        "BAH": "Bahigo",
        "YW": "Youwin",
        "MRO": "Mr Oyun",
        "ROJA": "Rojabet"
    }

    try:
        with engine.begin() as conn:
            for old_tag, full_name in mappings.items():
                print(f"🔄 Merging '{old_tag}' -> '{full_name}'...")
                # 1. Update Client Mapping Table
                conn.execute(text("UPDATE client_mapping SET client_name = :new WHERE client_name = :old"), {"new": full_name, "old": old_tag})

                # 2. Update Operations Data
                conn.execute(text("UPDATE ops_telemarketing_data SET ops_client = :new WHERE ops_client = :old"), {"new": full_name, "old": old_tag})

                # 3. Update Financial Data
                conn.execute(text("UPDATE raw_financial_data SET client = :new WHERE client = :old"), {"new": full_name, "old": old_tag})

            # --- HEAL BRAND NAMES ---
            for old_tag, pretty_brand in brand_mappings.items():
                print(f"✨ Translating Brand Tag '{old_tag}' -> '{pretty_brand}'...")

                # Update Ops Brand
                conn.execute(text("UPDATE ops_telemarketing_data SET ops_brand = :new WHERE ops_brand = :old"), {"new": pretty_brand, "old": old_tag})

                # Update Fin Brand
                conn.execute(text("UPDATE raw_financial_data SET brand = :new WHERE brand = :old"), {"new": pretty_brand, "old": old_tag})

                # Ensure Client Mapping table has the pretty name
                conn.execute(text("UPDATE client_mapping SET brand_name = :new WHERE brand_code = :old"), {"new": pretty_brand, "old": old_tag})

        print("✅ Database successfully healed! All duplicates consolidated.")
    except Exception as e:
        print(f"❌ Healing failed: {e}")

if __name__ == "__main__":
    heal_database()
