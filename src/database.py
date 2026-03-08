import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Fetch the Database URL from Docker or Railway environment variables, fallback to local if needed
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:supersecretpassword@localhost:5432/drock")

# Railway's Postgres URL might start with postgres:// instead of postgresql:// (SQLAlchemy requires postgresql://)
if DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

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
        conn.execute(text("ALTER TABLE client_mapping ADD COLUMN IF NOT EXISTS financial_format VARCHAR(50) DEFAULT 'Standard'"))
        
        # 2. Dual-Layer SLAs & Benchmarks
        # Drop legacy single-table logic
        conn.execute(text("DROP TABLE IF EXISTS contractual_slas CASCADE"))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS contractual_volumes (
                id SERIAL PRIMARY KEY,
                client_name VARCHAR(100) NOT NULL,
                brand_code VARCHAR(50) NOT NULL,
                lifecycle VARCHAR(50) NOT NULL,
                monthly_minimum_records INT NOT NULL,
                UNIQUE(client_name, brand_code, lifecycle)
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS granular_benchmarks (
                id SERIAL PRIMARY KEY,
                client_name VARCHAR(100) NOT NULL,
                brand_code VARCHAR(50) NOT NULL,
                campaign_signature VARCHAR(150) UNIQUE NOT NULL,
                target_conv_pct DECIMAL(5, 4),
                target_li_pct DECIMAL(5, 4),
                target_cac_usd DECIMAL(10, 2)
            )
        """))
        
        # 3. Telemarketing Historical Data (CallsU)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ops_historical_benchmarks (
                id SERIAL PRIMARY KEY,
                benchmark_period VARCHAR(100),
                brand VARCHAR(50),
                country VARCHAR(50),
                extracted_lifecycle VARCHAR(50),
                extracted_segment VARCHAR(50),
                extracted_engagement VARCHAR(50),
                avg_daily_records DECIMAL(10, 2),
                avg_daily_calls DECIMAL(10, 2),
                avg_daily_logins DECIMAL(10, 2),
                avg_daily_conversions DECIMAL(10, 2),
                avg_daily_deliveries DECIMAL(10, 2),
                avg_daily_telecom_cost DECIMAL(10, 2) DEFAULT 0,
                avg_daily_true_cac DECIMAL(10, 2) DEFAULT 0
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ops_telemarketing_data (
                id SERIAL PRIMARY KEY,
                campaign_name VARCHAR(255) UNIQUE,
                ops_client VARCHAR(100),
                ops_brand VARCHAR(50),
                ops_date VARCHAR(50),
                records INT DEFAULT 0,
                calls INT,
                conversions INT,
                total_cost DECIMAL(10, 2),
                true_cac DECIMAL(10, 2),
                d_total INT DEFAULT 0,
                d_plus INT DEFAULT 0,
                d_minus INT DEFAULT 0,
                d_neutral INT DEFAULT 0,
                d_ratio DECIMAL(10, 4) DEFAULT 0,
                kpi2_logins INT DEFAULT 0,
                li_pct DECIMAL(10, 4) DEFAULT 0,
                tech_issues INT DEFAULT 0,
                t INT DEFAULT 0,
                am INT DEFAULT 0,
                dnc INT DEFAULT 0,
                na INT DEFAULT 0,
                dx INT DEFAULT 0,
                wn INT DEFAULT 0,
                cost_caller DECIMAL(10, 2) DEFAULT 0,
                cost_sip DECIMAL(10, 2) DEFAULT 0,
                cost_sms DECIMAL(10, 2) DEFAULT 0,
                cost_email DECIMAL(10, 2) DEFAULT 0,
                hlrv INT DEFAULT 0,
                twoxrv INT DEFAULT 0,
                sa INT DEFAULT 0,
                sd INT DEFAULT 0,
                sf INT DEFAULT 0,
                sp INT DEFAULT 0,
                ev INT DEFAULT 0,
                es INT DEFAULT 0,
                ed INT DEFAULT 0,
                eo INT DEFAULT 0,
                ec INT DEFAULT 0,
                ef INT DEFAULT 0,
                optouts_all INT DEFAULT 0,
                optout_call INT DEFAULT 0,
                optout_sms INT DEFAULT 0,
                optout_email INT DEFAULT 0,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # --- HEAL LEGACY OPERATIONS DATES (2025_02 -> 2025-02) ---
        conn.execute(text("UPDATE ops_telemarketing_data SET ops_date = REPLACE(ops_date, '_', '-')"))
        
        # Safe Migration: Add columns to existing tables if they don't exist
        new_columns = [
            "records INT DEFAULT 0", "d_total INT DEFAULT 0", "d_plus INT DEFAULT 0", 
            "d_minus INT DEFAULT 0", "d_neutral INT DEFAULT 0", "d_ratio DECIMAL(10,4) DEFAULT 0", 
            "kpi2_logins INT DEFAULT 0", "li_pct DECIMAL(10,4) DEFAULT 0", 
            "tech_issues INT DEFAULT 0", "t INT DEFAULT 0", "am INT DEFAULT 0", 
            "dnc INT DEFAULT 0", "na INT DEFAULT 0", "dx INT DEFAULT 0", "wn INT DEFAULT 0",
            "cost_caller DECIMAL(10, 2) DEFAULT 0", "cost_sip DECIMAL(10, 2) DEFAULT 0", 
            "cost_sms DECIMAL(10, 2) DEFAULT 0", "cost_email DECIMAL(10, 2) DEFAULT 0",
            "hlrv INT DEFAULT 0", "twoxrv INT DEFAULT 0", "sa INT DEFAULT 0", 
            "sd INT DEFAULT 0", "sf INT DEFAULT 0", "sp INT DEFAULT 0", 
            "ev INT DEFAULT 0", "es INT DEFAULT 0", "ed INT DEFAULT 0", 
            "eo INT DEFAULT 0", "ec INT DEFAULT 0", "ef INT DEFAULT 0",
            "optouts_all INT DEFAULT 0", "optout_call INT DEFAULT 0",
            "optout_sms INT DEFAULT 0", "optout_email INT DEFAULT 0",
            "extracted_engagement VARCHAR(50)", "extracted_lifecycle VARCHAR(50)", 
            "extracted_segment VARCHAR(50)", "country VARCHAR(50)"
        ]
        # We need to make sure the snapshots table exists before we try to ALTER it
        conn.execute(text("CREATE TABLE IF NOT EXISTS ops_telemarketing_snapshots (id SERIAL PRIMARY KEY)"))
        
        for col_def in new_columns:
            conn.execute(text(f"ALTER TABLE ops_telemarketing_data ADD COLUMN IF NOT EXISTS {col_def}"))
            conn.execute(text(f"ALTER TABLE ops_telemarketing_snapshots ADD COLUMN IF NOT EXISTS {col_def}"))

        # Safe Migration for historical benchmarks
        benchmark_columns = [
            "avg_daily_telecom_cost DECIMAL(10, 2) DEFAULT 0",
            "avg_daily_true_cac DECIMAL(10, 2) DEFAULT 0"
        ]
        conn.execute(text("CREATE TABLE IF NOT EXISTS ops_historical_benchmarks (id SERIAL PRIMARY KEY)"))
        for col_def in benchmark_columns:
            conn.execute(text(f"ALTER TABLE ops_historical_benchmarks ADD COLUMN IF NOT EXISTS {col_def}"))

        # 4. Raw Financial Historical Data
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw_financial_data (
                db_id SERIAL PRIMARY KEY,
                player_id VARCHAR(255),
                client VARCHAR(100),
                brand VARCHAR(50),
                country VARCHAR(50),
                wb_tag VARCHAR(100),
                segment VARCHAR(100),
                bet DECIMAL(15, 2),
                revenue DECIMAL(15, 2),
                ngr DECIMAL(15, 2),
                bet_casino DECIMAL(15, 2),
                revenue_casino DECIMAL(15, 2),
                ngr_casino DECIMAL(15, 2),
                bet_sports DECIMAL(15, 2),
                revenue_sports DECIMAL(15, 2),
                ngr_sports DECIMAL(15, 2),
                deposit_count INT,
                deposits DECIMAL(15, 2),
                withdrawals DECIMAL(15, 2),
                bonus_total DECIMAL(15, 2),
                bonus_casino DECIMAL(15, 2),
                bonus_sports DECIMAL(15, 2),
                tax_total DECIMAL(15, 2),
                report_month VARCHAR(50),
                reactivation_date TIMESTAMP,
                campaign_start_date TIMESTAMP,
                reactivation_days INT,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # 5. User Authentication Registry
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL,
                role VARCHAR(50) NOT NULL,
                name VARCHAR(100),
                allowed_clients JSONB NOT NULL
            )
        """))

        # Auto-Seed Default Superadmin
        import json
        conn.execute(text("""
            INSERT INTO users (username, password, role, name, allowed_clients) 
            VALUES ('superadmin', '123', 'Superadmin', 'Global Overlord', :ac)
            ON CONFLICT (username) DO NOTHING
        """), {"ac": json.dumps(["All"])})

        conn.commit()

    # --- Auto-Seed Legacy Brand Dictionary ---
    try:
        from src.ingestion import CLIENT_HIERARCHY
        for brand, client in CLIENT_HIERARCHY.items():
            execute_query(
                "INSERT INTO client_mapping (brand_code, client_name) VALUES (:b, :c) ON CONFLICT (brand_code) DO NOTHING",
                {"b": brand, "c": client}
            )
            
        # Ensure full corporate names are injected directly to Railway DB
        BRAND_MAPPING = {
            "BAH": "Reliato", "YW": "Limitless", "VJ": "Simplicity Malta Limited",
            "PP": "PowerPlay", "RHN": "Rhino", "INSP": "Magico", "PE": "Magico",
            "LV": "LeoVegas Group", "EX": "LeoVegas Group", "RP": "LeoVegas Group",
            "GG": "LeoVegas Group", "BETMGM": "LeoVegas Group", "BETUK": "LeoVegas Group",
            "MRO": "Offside Gaming", "YU": "Offside Gaming", "LTRB": "Offside Gaming",
            "ROJA": "Offside Gaming", "CASINODAYS": "Rhino", "WG": "Reliato", "BHB": "Limitless"
        }
        for code, name in BRAND_MAPPING.items():
            execute_query(
                "UPDATE client_mapping SET brand_name = :n WHERE brand_code = :c",
                {"n": name, "c": code}
            )
    except Exception as e:
        pass

def execute_query(query, params=None):
    """Helper to execute raw SQL safely."""
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        conn.commit()
        return result
