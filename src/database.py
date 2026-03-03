import os
import pandas as pd
from sqlalchemy import create_engine, text

# Fetch the Database URL from Docker environment variables, fallback to local if needed
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:supersecretpassword@localhost:5432/drock")
engine = create_engine(DB_URL)

def init_db():
    """Creates the core tables if they do not exist."""
    with engine.connect() as conn:
        # 1. Client & Brand Mapping
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS client_mapping (
                id SERIAL PRIMARY KEY,
                brand_code VARCHAR(50) UNIQUE NOT NULL,
                brand_name VARCHAR(100),
                client_name VARCHAR(100) NOT NULL
            )
        """))
        conn.execute(text("ALTER TABLE client_mapping ADD COLUMN IF NOT EXISTS brand_name VARCHAR(100)"))
        
        # 2. Contractual SLAs
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS contractual_slas (
                id SERIAL PRIMARY KEY,
                client_name VARCHAR(100) NOT NULL,
                brand_code VARCHAR(50) NOT NULL,
                lifecycle VARCHAR(50) NOT NULL,
                monthly_minimum_records INT NOT NULL,
                target_cac_usd DECIMAL(10, 2),
                benchmark_conv_pct DECIMAL(5, 4),
                UNIQUE(client_name, brand_code, lifecycle)
            )
        """))
        
        # 3. Telemarketing Historical Data (CallsU)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ops_telemarketing_data (
                id SERIAL PRIMARY KEY,
                campaign_name VARCHAR(255) UNIQUE,
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
                tech_issues INT DEFAULT 0,
                am INT DEFAULT 0,
                dnc INT DEFAULT 0,
                na INT DEFAULT 0,
                dx INT DEFAULT 0,
                wn INT DEFAULT 0,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Safe Migration: Add columns to existing table if they don't exist
        new_columns = [
            "d_total INT DEFAULT 0", "d_plus INT DEFAULT 0", "d_minus INT DEFAULT 0", 
            "d_ratio DECIMAL(10,4) DEFAULT 0", "tech_issues INT DEFAULT 0", 
            "am INT DEFAULT 0", "dnc INT DEFAULT 0", "na INT DEFAULT 0", 
            "dx INT DEFAULT 0", "wn INT DEFAULT 0"
        ]
        for col_def in new_columns:
            conn.execute(text(f"ALTER TABLE ops_telemarketing_data ADD COLUMN IF NOT EXISTS {col_def}"))

        # 4. Financial Historical Data
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS financial_data (
                id SERIAL PRIMARY KEY,
                client VARCHAR(100) NOT NULL,
                brand VARCHAR(50) NOT NULL,
                month VARCHAR(50) NOT NULL,
                ngr DECIMAL(15, 2) DEFAULT 0.0,
                deposits DECIMAL(15, 2) DEFAULT 0.0,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(client, brand, month)
            )
        """))

        conn.commit()
        print("✅ Database initialized successfully.")

    # --- Auto-Seed Legacy Brand Dictionary ---
    try:
        from src.ingestion import CLIENT_HIERARCHY
        for brand, client in CLIENT_HIERARCHY.items():
            execute_query(
                "INSERT INTO client_mapping (brand_code, client_name) VALUES (:b, :c) ON CONFLICT (brand_code) DO NOTHING",
                {"b": brand, "c": client}
            )
        print("✅ Legacy brand dictionary synced and COMMITTED to PostgreSQL.")
    except Exception as e:
        print(f"⚠️ Warning: Could not auto-seed brands: {e}")

def execute_query(query, params=None):
    """Helper to execute raw SQL safely."""
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        conn.commit()
        return result
