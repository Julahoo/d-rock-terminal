import io
import os
import sys

# Ensure we can import from src
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.ingestion import load_all_data_from_uploads, load_operations_data_from_uploads
from src.database import engine
from sqlalchemy import text

class MockUploadedFile(io.BytesIO):
    """Mocks Streamlit's UploadedFile object for headless backend testing."""
    def __init__(self, filepath):
        with open(filepath, "rb") as f:
            super().__init__(f.read())
        self.name = os.path.basename(filepath)

def pre_seed_client_mappings():
    """Injects predefined client mappings so the parser instantly recognizes them."""
    print("⚙️ Pre-seeding Client Registry...")
    mappings = [
        # Primary Clients / Master Tags
        {"tag": "LIM", "brand": "Limitless", "client": "Limitless", "fmt": "Standard"},
        {"tag": "REL", "brand": "Reliato", "client": "Reliato", "fmt": "Standard"},
        {"tag": "RHN", "brand": "Rhino", "client": "Rhino", "fmt": "Standard"},
        {"tag": "SIM", "brand": "Simplicity Malta Limited", "client": "Simplicity Malta Limited", "fmt": "Standard"},
        {"tag": "PP", "brand": "PowerPlay", "client": "PowerPlay", "fmt": "Standard"},
        {"tag": "INSP", "brand": "Magico Games/Interspin", "client": "Magico Games/Interspin", "fmt": "Standard"},
        {"tag": "PE", "brand": "NitroCasino", "client": "PressEnter", "fmt": "Standard"},
        {"tag": "LV", "brand": "LeoVegas Group", "client": "LeoVegas Group", "fmt": "LeoVegas"},
        
        # Sub-Brand Explicit Definitions
        {"tag": "YU", "brand": "Yuugado", "client": "Simplicity Malta Limited", "fmt": "Standard"},
        {"tag": "VJ", "brand": "VeraJohn", "client": "Simplicity Malta Limited", "fmt": "Standard"},
        {"tag": "BAH", "brand": "Bahigo", "client": "Reliato", "fmt": "Standard"},
        {"tag": "ROJA", "brand": "Rojabet", "client": "Reliato", "fmt": "Standard"},
        {"tag": "YW", "brand": "Youwin", "client": "Limitless", "fmt": "Standard"},
        {"tag": "MRO", "brand": "Mr Oyun", "client": "Limitless", "fmt": "Standard"},
        {"tag": "BETUK", "brand": "Bet UK", "client": "LeoVegas Group", "fmt": "LeoVegas"},
        {"tag": "BETMGM", "brand": "BetMGM", "client": "LeoVegas Group", "fmt": "LeoVegas"},
        {"tag": "EXPEKT", "brand": "Expekt", "client": "LeoVegas Group", "fmt": "LeoVegas"},
        {"tag": "GOGO", "brand": "GoGoCasino", "client": "LeoVegas Group", "fmt": "LeoVegas"},
        {"tag": "ROYALPANDA", "brand": "RoyalPanda", "client": "LeoVegas Group", "fmt": "LeoVegas"},
        {"tag": "WG", "brand": "Wetigo", "client": "Reliato", "fmt": "Standard"},
        {"tag": "BHB", "brand": "Bahibi", "client": "Limitless", "fmt": "Standard"}
    ]

    with engine.begin() as conn:
        for m in mappings:
            conn.execute(text("""
                INSERT INTO client_mapping (brand_code, brand_name, client_name, financial_format) 
                VALUES (:tag, :brand, :client, :fmt) 
                ON CONFLICT (brand_code) DO UPDATE 
                SET brand_name = :brand, client_name = :client, financial_format = :fmt
            """), m)
    print("✅ Client Registry Pre-Seeded!")

def seed_database():
    fin_file_path = "data/raw/leovegas/leovegas_2025_02.xlsx"
    ops_file_path = "data/raw/callsu intel/CallsU_Operations_2025_01.xlsx"
    
    print("🚀 Starting Headless DB Injection...")

    try:
        from src.database import init_db
        init_db()
        pre_seed_client_mappings() # Maps must be seeded AFTER tables are created
        
        if os.path.exists(fin_file_path):
            print(f"📥 Injecting Financial Data: {fin_file_path}")
            fin_mock = MockUploadedFile(fin_file_path)
            # The signature returns (df, registry), we only care about executing it
            load_all_data_from_uploads([fin_mock])
        else:
            print(f"⚠️ Financial test file not found at {fin_file_path}")

        if os.path.exists(ops_file_path):
            print(f"📥 Injecting Operations Data: {ops_file_path}")
            ops_mock = MockUploadedFile(ops_file_path)
            # Signature returns dataframe
            load_operations_data_from_uploads([ops_mock])
        else:
            print(f"⚠️ Operations test file not found at {ops_file_path}")
            
        print("✅ Database Seeded Successfully! Subagent may proceed with UI validation.")
        
    except Exception as e:
        print(f"❌ Injection Failed: {e}")

if __name__ == "__main__":
    seed_database()
