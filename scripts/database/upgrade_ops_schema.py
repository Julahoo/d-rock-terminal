import os
import sys
from sqlalchemy import text

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from src.database import engine

def upgrade_schema():
    print("⚙️ Upgrading Operations Schema with Snapshots...")
    try:
        with engine.begin() as conn:
            # Add KPI2 and LI% columns if they don't exist
            conn.execute(text("ALTER TABLE ops_telemarketing_data ADD COLUMN IF NOT EXISTS kpi2_logins NUMERIC DEFAULT 0"))
            conn.execute(text("ALTER TABLE ops_telemarketing_data ADD COLUMN IF NOT EXISTS li_pct NUMERIC DEFAULT 0"))
            
            # Create Operations Snapshots Table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ops_telemarketing_snapshots (
                    id SERIAL PRIMARY KEY,
                    campaign_name VARCHAR(255),
                    ops_client VARCHAR(100),
                    ops_brand VARCHAR(50),
                    ops_date VARCHAR(50),
                    calls INT,
                    conversions INT,
                    total_cost DECIMAL(10, 2),
                    true_cac DECIMAL(10, 2),
                    d_total INT DEFAULT 0,
                    d_plus INT DEFAULT 0,
                    d_minus INT DEFAULT 0,
                    d_ratio DECIMAL(10, 4) DEFAULT 0,
                    kpi2_logins NUMERIC DEFAULT 0,
                    li_pct NUMERIC DEFAULT 0,
                    tech_issues INT DEFAULT 0,
                    am INT DEFAULT 0,
                    dnc INT DEFAULT 0,
                    na INT DEFAULT 0,
                    dx INT DEFAULT 0,
                    wn INT DEFAULT 0,
                    snapshot_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            # Index for faster timeseries querying
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ops_snapshots_date ON ops_telemarketing_snapshots(ops_date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ops_snapshots_campaign ON ops_telemarketing_snapshots(campaign_name)"))

        print("✅ Schema successfully upgraded with Snapshots, KPI2, and LI%!")
    except Exception as e:
        print(f"❌ Schema upgrade failed: {e}")

if __name__ == "__main__":
    upgrade_schema()
