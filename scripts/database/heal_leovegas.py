import os
import sys
import glob
from sqlalchemy import text
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from src.database import engine
from src.ingestion import load_all_data_from_uploads

import io

class MockUploadedFile(io.BytesIO):
    def __init__(self, filepath):
        with open(filepath, "rb") as f:
            super().__init__(f.read())
        self.name = os.path.basename(filepath)

def heal_leovegas():
    print("🩹 Starting LeoVegas Live Database Healing...")

    # 1. Scrub broken LeoVegas records
    with engine.begin() as conn:
        print("🧹 Scrubbing old LeoVegas financial data...")
        conn.execute(text("DELETE FROM raw_financial_data WHERE client = 'LeoVegas Group'"))

        print("⚙️ Injecting LeoVegas Sub-Brand Mappings...")
        new_mappings = [
            {"tag": "BETUK", "brand": "Bet UK"},
            {"tag": "BETMGM", "brand": "BetMGM"},
            {"tag": "EXPEKT", "brand": "Expekt"},
            {"tag": "GOGO", "brand": "GoGoCasino"},
            {"tag": "ROYALPANDA", "brand": "RoyalPanda"}
        ]
        for m in new_mappings:
            conn.execute(text("""
                INSERT INTO client_mapping (brand_code, brand_name, client_name, financial_format) 
                VALUES (:tag, :brand, 'LeoVegas Group', 'LeoVegas') 
                ON CONFLICT (brand_code) DO UPDATE 
                SET brand_name = :brand, client_name = 'LeoVegas Group', financial_format = 'LeoVegas'
            """), m)

    # 2. Re-ingest the data headlessly
    leovegas_files = glob.glob("data/raw/*leovegas*.xlsx") + glob.glob("data/raw/leovegas/*.xlsx")
    if not leovegas_files:
        print("⚠️ No local LeoVegas files found to re-ingest. Please upload manually via the UI.")
        return

    print(f"🚀 Found {len(leovegas_files)} LeoVegas files. Re-ingesting...")
    mock_files = [MockUploadedFile(f) for f in leovegas_files]

    # Run ingestion
    load_all_data_from_uploads(mock_files)
    print("✅ LeoVegas Healing Complete! Granular brands are restored.")

if __name__ == "__main__":
    heal_leovegas()
