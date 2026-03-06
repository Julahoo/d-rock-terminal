import os
import sys
from sqlalchemy import text

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from src.database import engine

def heal_ltrb():
    print("🩹 Starting LTRB Operations Healing...")
    try:
        with engine.begin() as conn:
            print("🔄 Merging 'LTRB' -> 'Latribet' in Operations table...")
            conn.execute(text("UPDATE ops_telemarketing_data SET ops_brand = 'Latribet' WHERE ops_brand = 'LTRB'"))
        print("✅ LTRB Operations Healing Complete!")
    except Exception as e:
        print(f"❌ Healing failed: {e}")

if __name__ == "__main__":
    heal_ltrb()
