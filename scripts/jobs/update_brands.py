import sys
import os

# Append project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from src.database import engine

BRAND_MAPPING = {
    "BAH": "Reliato",
    "YW": "Limitless",
    "VJ": "Simplicity Malta Limited",
    "PP": "PowerPlay",
    "RHN": "Rhino",
    "INSP": "Magico",
    "PE": "Magico",
    "LV": "LeoVegas Group",
    "EX": "LeoVegas Group",
    "RP": "LeoVegas Group",
    "GG": "LeoVegas Group",
    "BETMGM": "LeoVegas Group",
    "BETUK": "LeoVegas Group",
    "MRO": "Offside Gaming",
    "YU": "Offside Gaming",
    "LTRB": "Offside Gaming",
    "ROJA": "Offside Gaming",
    "CASINODAYS": "Rhino",
    "WG": "Reliato",
    "BHB": "Limitless"
}

def update_brands():
    print("Starting Brand Mapping Seeding...")
    with engine.begin() as conn:
        for code, name in BRAND_MAPPING.items():
            conn.execute(
                text("UPDATE client_mapping SET brand_name = :name WHERE brand_code = :code"),
                {"name": name, "code": code}
            )
    print("Successfully mapped all brands in the client_mapping table.")

if __name__ == "__main__":
    update_brands()
