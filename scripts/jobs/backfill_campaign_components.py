import sys
import os

# Append project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import pandas as pd
from sqlalchemy import text
from src.database import engine

def backfill():
    print("Starting Historical Campaign Data Backfill...")
    
    # Fetch all records with potentially missing extraction
    query = """
    SELECT id, campaign_name 
    FROM ops_telemarketing_data 
    WHERE extracted_lifecycle = 'UNKNOWN' OR extracted_lifecycle IS NULL
       OR extracted_engagement = 'UNKNOWN' OR extracted_engagement IS NULL
    """
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("✅ No records require backfilling for ops_telemarketing_data.")
    else:
        print(f"🔧 Found {len(df)} records in ops_telemarketing_data to backfill.")
        
        updates = []
        for _, row in df.iterrows():
            campaign = str(row['campaign_name'])
            tokens = [t for t in campaign.upper().replace('_', '-').split('-') if t]
            tokens = ['WB' if t == 'WBD' else t for t in tokens]
            
            extracted_lifecycle = next((t for t in tokens if t in ['RND', 'WB', 'CS', 'ROC', 'FD', 'OTD', 'CHU', 'ACQ', 'SL', 'LFC', 'LOADER']), "UNKNOWN")
            extracted_segment = next((t for t in tokens if t in ['HIGH', 'MID', 'MED', 'LOW', 'VIP', 'NA', 'AFF', 'COH1', 'COH2', 'COH3', 'COH4']), "UNKNOWN")
            extracted_engagement = next((t for t in tokens if t in ['NLI', 'LI']), "UNKNOWN")
            
            blocklist = ['SPO', 'CAS', 'LIVE', 'ALL', 'DAY', 'A', 'B', 'J1', 'J2', 'J3', 'NLI', 'LI', 'NEW'] + \
                        ['RND', 'WB', 'CS', 'ROC', 'FD', 'OTD', 'CHU', 'ACQ', 'SL', 'LFC', 'LOADER'] + \
                        ['HIGH', 'MID', 'MED', 'LOW', 'VIP', 'NA', 'AFF', 'COH1', 'COH2', 'COH3', 'COH4'] 
            
            country = "Global"
            for t in tokens[1:]:
                if t not in blocklist and t.isalpha() and 2 <= len(t) <= 3:
                    country = t
                    break
                    
            updates.append({
                "id": row["id"],
                "l": extracted_lifecycle,
                "s": extracted_segment,
                "e": extracted_engagement,
                "c": country
            })
            
        # Execute Batch Update
        with engine.begin() as conn:
            for u in updates:
                conn.execute(text("""
                    UPDATE ops_telemarketing_data 
                    SET extracted_lifecycle = :l, extracted_segment = :s, 
                        extracted_engagement = :e, country = :c
                    WHERE id = :id
                """), u)
        print(f"✅ Successfully backfilled ops_telemarketing_data.")

    # Apply identical backfill to snapshots
    snap_query = """
    SELECT id, campaign_name 
    FROM ops_telemarketing_snapshots 
    WHERE extracted_lifecycle = 'UNKNOWN' OR extracted_lifecycle IS NULL
       OR extracted_engagement = 'UNKNOWN' OR extracted_engagement IS NULL
    """
    try:
        snap_df = pd.read_sql(snap_query, engine)
        if snap_df.empty:
            print("✅ No records require backfilling for ops_telemarketing_snapshots.")
        else:
            print(f"🔧 Found {len(snap_df)} records in ops_telemarketing_snapshots to backfill.")
            
            snap_updates = []
            for _, row in snap_df.iterrows():
                campaign = str(row['campaign_name'])
                tokens = [t for t in campaign.upper().replace('_', '-').split('-') if t]
                tokens = ['WB' if t == 'WBD' else t for t in tokens]
                
                extracted_lifecycle = next((t for t in tokens if t in ['RND', 'WB', 'CS', 'ROC', 'FD', 'OTD', 'CHU', 'ACQ', 'SL', 'LFC', 'LOADER']), "UNKNOWN")
                extracted_segment = next((t for t in tokens if t in ['HIGH', 'MID', 'MED', 'LOW', 'VIP', 'NA', 'AFF', 'COH1', 'COH2', 'COH3', 'COH4']), "UNKNOWN")
                extracted_engagement = next((t for t in tokens if t in ['NLI', 'LI']), "UNKNOWN")
                
                blocklist = ['SPO', 'CAS', 'LIVE', 'ALL', 'DAY', 'A', 'B', 'J1', 'J2', 'J3', 'NLI', 'LI', 'NEW'] + \
                            ['RND', 'WB', 'CS', 'ROC', 'FD', 'OTD', 'CHU', 'ACQ', 'SL', 'LFC', 'LOADER'] + \
                            ['HIGH', 'MID', 'MED', 'LOW', 'VIP', 'NA', 'AFF', 'COH1', 'COH2', 'COH3', 'COH4'] 
                
                country = "Global"
                for t in tokens[1:]:
                    if t not in blocklist and t.isalpha() and 2 <= len(t) <= 3:
                        country = t
                        break
                        
                snap_updates.append({
                    "id": row["id"],
                    "l": extracted_lifecycle,
                    "s": extracted_segment,
                    "e": extracted_engagement,
                    "c": country
                })
                
            # Execute Batch Update
            with engine.begin() as conn:
                for u in snap_updates:
                    conn.execute(text("""
                        UPDATE ops_telemarketing_snapshots 
                        SET extracted_lifecycle = :l, extracted_segment = :s, 
                            extracted_engagement = :e, country = :c
                        WHERE id = :id
                    """), u)
            print(f"✅ Successfully backfilled ops_telemarketing_snapshots.")
    except Exception as e:
        print(f"⚠️ Error during snapshots backfill (may not exist yet): {e}")

if __name__ == "__main__":
    backfill()
