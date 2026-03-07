import pandas as pd
from sqlalchemy import text
from src.database import engine

def main():
    print("🚀 Starting Historical Backfill of Schema Dimensions...")
    
    with engine.connect() as conn:
        # Fetch all distinct campaigns
        result = conn.execute(text("SELECT DISTINCT campaign_name FROM ops_telemarketing_data"))
        campaigns = [row[0] for row in result]
        
        print(f"📦 Found {len(campaigns)} distinct campaigns to parse.")
        
        updated_count = 0
        for campaign in campaigns:
            if not campaign:
                continue
                
            camp_str = str(campaign)
            tokens = [t for t in camp_str.upper().replace('_', '-').split('-') if t]
            tokens = ['WB' if t == 'WBD' else t for t in tokens]
            tag = tokens[0] if len(tokens) > 0 else "UNKNOWN"
            
            extracted_lifecycle = next((t for t in tokens if t in ['RND', 'WB', 'CS', 'ROC', 'FD', 'OTD', 'CHU', 'ACQ', 'SL', 'LFC', 'LOADER']), "UNKNOWN")
            extracted_segment = next((t for t in tokens if t in ['HIGH', 'MID', 'MED', 'LOW', 'VIP', 'NA', 'AFF', 'COH1', 'COH2', 'COH3', 'COH4']), "UNKNOWN")
            extracted_engagement = next((t for t in tokens if t in ['NLI', 'LI']), "UNKNOWN")
            
            blocklist = ['SPO', 'CAS', 'LIVE', 'ALL', 'DAY', 'A', 'B', 'J1', 'J2', 'J3', 'NLI', 'LI', 'NEW'] + \
                        ['RND', 'WB', 'CS', 'ROC', 'FD', 'OTD', 'CHU', 'ACQ', 'SL', 'LFC', 'LOADER'] + \
                        ['HIGH', 'MID', 'MED', 'LOW', 'VIP', 'NA', 'AFF', 'COH1', 'COH2', 'COH3', 'COH4'] + \
                        [tag.upper()]
            
            country = "Global"
            if len(tokens) > 1:
                # Need to iterate through tokens AFTER the first to avoid mistaking brand for country
                for t in tokens[1:]:
                    if t not in blocklist and t.isalpha() and 2 <= len(t) <= 3:
                        country = t
                        break

            # Update the table
            stmt = text("""
                UPDATE ops_telemarketing_data 
                SET extracted_lifecycle = :lc, 
                    extracted_segment = :seg, 
                    extracted_engagement = :eng,
                    country = :cty
                WHERE campaign_name = :camp
            """)
            
            conn.execute(stmt, {
                "lc": extracted_lifecycle,
                "seg": extracted_segment,
                "eng": extracted_engagement,
                "cty": country,
                "camp": campaign
            })
            updated_count += 1
            
        conn.commit()
    print(f"✅ Successfully backfilled {updated_count} campaign signatures into ops_telemarketing_data.")
    
    print("📈 Triggering Benchmark Generator to calculate Delta Metrics...")
    import sys
    from scripts.jobs.generate_benchmarks import generate_benchmarks
    try:
        generate_benchmarks()
        print("✅ Delta Metrics generated and stored in ops_historical_benchmarks.")
    except Exception as e:
        print(f"❌ Failed to calculate benchmarks: {e}")

if __name__ == "__main__":
    main()
