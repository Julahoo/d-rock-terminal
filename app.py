"""
app.py – Streamlit Web App (Phase 6)
======================================
Web frontend for the Betting Financial Reports ETL pipeline.

Run with:  streamlit run app.py
"""
from __future__ import annotations

import logging
from pathlib import Path

import threading
import time
import os
import calendar
from datetime import datetime, timedelta
from src.iwinback_worker import run_historical_pull

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

# Increase Pandas Styler limits to prevent crashes on large ledgers
pd.set_option("styler.render.max_elements", 1_500_000)

from src.ingestion import load_all_data_from_uploads, load_campaign_data_from_uploads
from src.analytics import generate_monthly_summaries, generate_campaign_summaries, generate_cohort_matrix, generate_segmentation_summary, generate_both_business_summary, generate_time_series, generate_program_summary, generate_rfm_summary, generate_smart_narrative, generate_player_master_list, generate_retention_heatmap, generate_overlap_stats, generate_ltv_curves, generate_tier_summary
from src.exporter import export_to_excel
from src.database import init_db, execute_query, engine
from sqlalchemy.exc import ProgrammingError

# ── Material Design 3 Dark Theme ─────────────────────────────────────────
_MATERIAL_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* ── Global Typography ── */
html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
}
h1, h2, h3, h4, h5, h6 {
    font-family: 'Inter', sans-serif !important;
    font-weight: 600 !important;
    letter-spacing: -0.02em !important;
}
h1 { font-size: 1.8rem !important; }
h2 { font-size: 1.4rem !important; }
h3 { font-size: 1.15rem !important; }

/* ── Main Container ── */
.main .block-container {
    padding: 2rem 2.5rem !important;
    max-width: 100% !important;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0D1117 0%, #161B22 100%) !important;
    border-right: 1px solid rgba(124, 77, 255, 0.15) !important;
}
section[data-testid="stSidebar"] .stRadio label,
section[data-testid="stSidebar"] .stSelectbox label {
    font-size: 0.85rem !important;
    font-weight: 500 !important;
    color: #8B949E !important;
}

/* ── Cards / Containers ── */
div[data-testid="stExpander"] {
    background: #161B22 !important;
    border: 1px solid #30363D !important;
    border-radius: 12px !important;
    overflow: hidden;
}
div[data-testid="stExpander"] summary {
    font-weight: 500 !important;
    color: #E6EDF3 !important;
}

/* ── Metric Cards ── */
div[data-testid="stMetric"] {
    background: linear-gradient(135deg, #161B22 0%, #1C2333 100%) !important;
    border: 1px solid #30363D !important;
    border-radius: 12px !important;
    padding: 1rem 1.2rem !important;
    transition: all 0.2s ease !important;
}
div[data-testid="stMetric"]:hover {
    border-color: rgba(124, 77, 255, 0.4) !important;
    box-shadow: 0 4px 16px rgba(124, 77, 255, 0.08) !important;
    transform: translateY(-1px);
}
div[data-testid="stMetric"] label {
    color: #8B949E !important;
    font-size: 0.75rem !important;
    font-weight: 500 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    font-size: 1.6rem !important;
    font-weight: 700 !important;
    color: #E6EDF3 !important;
}
div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
    font-size: 0.8rem !important;
    font-weight: 500 !important;
}

/* ── Buttons ── */
.stButton > button {
    border-radius: 8px !important;
    font-weight: 500 !important;
    font-size: 0.85rem !important;
    padding: 0.5rem 1.2rem !important;
    transition: all 0.15s ease !important;
    border: 1px solid #30363D !important;
    background: #21262D !important;
    color: #E6EDF3 !important;
}
.stButton > button:hover {
    background: #30363D !important;
    border-color: #7C4DFF !important;
    box-shadow: 0 2px 8px rgba(124, 77, 255, 0.15) !important;
}
.stButton > button:active {
    transform: scale(0.98) !important;
}
/* Primary buttons */
.stButton > button[kind="primary"],
.stDownloadButton > button {
    background: linear-gradient(135deg, #7C4DFF 0%, #651FFF 100%) !important;
    border: none !important;
    color: white !important;
}
.stDownloadButton > button:hover {
    box-shadow: 0 4px 12px rgba(124, 77, 255, 0.3) !important;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    gap: 0 !important;
    background: #161B22 !important;
    border-radius: 10px !important;
    padding: 4px !important;
    border: 1px solid #30363D !important;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px !important;
    padding: 8px 16px !important;
    font-weight: 500 !important;
    font-size: 0.85rem !important;
    color: #8B949E !important;
    background: transparent !important;
    border: none !important;
}
.stTabs [aria-selected="true"] {
    background: #7C4DFF !important;
    color: white !important;
}

/* ── Radio Buttons (Horizontal) ── */
.stRadio [role="radiogroup"] {
    gap: 0 !important;
    background: #161B22 !important;
    border-radius: 10px !important;
    padding: 4px !important;
    border: 1px solid #30363D !important;
}
.stRadio [role="radiogroup"] label {
    border-radius: 8px !important;
    padding: 6px 14px !important;
    font-size: 0.82rem !important;
    font-weight: 500 !important;
    transition: all 0.15s ease !important;
}
.stRadio [role="radiogroup"] label[data-checked="true"],
.stRadio [role="radiogroup"] label:has(input:checked) {
    background: rgba(124, 77, 255, 0.2) !important;
    color: #B794F6 !important;
}

/* ── Dataframes & Tables ── */
div[data-testid="stDataFrame"] {
    border: 1px solid #30363D !important;
    border-radius: 10px !important;
    overflow: hidden;
}

/* ── Selectbox & Inputs ── */
.stSelectbox, .stTextInput, .stNumberInput, .stDateInput {
    font-size: 0.85rem !important;
}
div[data-baseweb="select"] > div,
div[data-baseweb="input"] > div {
    border-radius: 8px !important;
    border-color: #30363D !important;
    background: #0D1117 !important;
}
div[data-baseweb="select"] > div:hover,
div[data-baseweb="input"] > div:hover {
    border-color: #7C4DFF !important;
}

/* ── Dividers ── */
hr {
    border-color: #21262D !important;
    margin: 1rem 0 !important;
}

/* ── Alerts & Info boxes ── */
div[data-testid="stAlert"] {
    border-radius: 10px !important;
    border: none !important;
    font-size: 0.85rem !important;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: #30363D; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #484F58; }

/* ── Success/Error Toasts ── */
.stSuccess, .stError, .stWarning, .stInfo {
    border-radius: 10px !important;
}

/* ── Caption text ── */
.stCaption, small {
    color: #8B949E !important;
    font-size: 0.78rem !important;
}
</style>
"""
st.markdown(_MATERIAL_CSS, unsafe_allow_html=True)

# ── Cached wrappers to prevent recomputation on Streamlit rerun ───────────
import plotly.io as pio

@st.cache_data(show_spinner=False)
def _cached_time_series(data):
    return generate_time_series(data)

@st.cache_data(ttl="15m", show_spinner=False)
def _cached_tier_summary(brand, target_month):
    try:
        from src.database import engine as _db
        import pandas as pd
        from sqlalchemy import inspect
        import io
        if not inspect(_db).has_table("cache_tier_summaries"): return pd.DataFrame()
        res = pd.read_sql(f"SELECT tier_json FROM cache_tier_summaries WHERE brand = '{brand}'", _db)
        if not res.empty: return pd.read_json(io.StringIO(res.iloc[0]["tier_json"]), orient="split")
    except Exception: pass
    return pd.DataFrame()

@st.cache_data(show_spinner=False)
def _cached_player_master_list(raw_df):
    return generate_player_master_list(raw_df)

@st.cache_data(ttl="15m", show_spinner=False)
def _cached_retention_heatmap():
    try:
        from src.database import engine as _db
        import pandas as pd
        from sqlalchemy import inspect
        if not inspect(_db).has_table("cache_financial_figures"): return None
        res = pd.read_sql("SELECT figure_json FROM cache_financial_figures WHERE visualization = 'retention_heatmap'", _db)
        if not res.empty and res.iloc[0]["figure_json"]: return pio.from_json(res.iloc[0]["figure_json"])
    except Exception: pass
    return None

@st.cache_data(ttl="15m", show_spinner=False)
def _cached_ltv_curves():
    try:
        from src.database import engine as _db
        import pandas as pd
        from sqlalchemy import inspect
        if not inspect(_db).has_table("cache_financial_figures"): return None
        res = pd.read_sql("SELECT figure_json FROM cache_financial_figures WHERE visualization = 'ltv_curves'", _db)
        if not res.empty and res.iloc[0]["figure_json"]: return pio.from_json(res.iloc[0]["figure_json"])
    except Exception: pass
    return None

@st.cache_data(show_spinner=False)
def _cached_monthly_summaries(df, start=None, end=None): 
    return generate_monthly_summaries(df, force_start=start, force_end=end)

@st.cache_data(ttl="15m", show_spinner=False)
def _cached_cohort_matrix(): 
    try:
        from src.database import engine as _db
        import pandas as pd
        from sqlalchemy import inspect
        import io
        if not inspect(_db).has_table("cache_cohort_matrices"): return {}
        res = pd.read_sql("SELECT brand, matrix_json FROM cache_cohort_matrices", _db)
        return {row["brand"]: pd.read_json(io.StringIO(row["matrix_json"]), orient="split") for _, row in res.iterrows()}
    except Exception: return {}

@st.cache_data(show_spinner=False)
def _cached_segmentation(df): return generate_segmentation_summary(df)

@st.cache_data(show_spinner=False)
def _cached_both_business(summary_df): return generate_both_business_summary(summary_df)

@st.cache_data(show_spinner=False)
def _cached_program_summary(df): return generate_program_summary(df)

@st.cache_data(show_spinner=False)
def _get_financial_excel_bytes(summary_df, cohort_matrices, segmentation, both_business):
    from src.exporter import export_to_excel
    buf = export_to_excel(summary_df, cohort_matrices=cohort_matrices, segmentation_df=segmentation, both_business_df=both_business)
    return buf.getvalue()

@st.cache_data(show_spinner=False)
def _get_ops_excel_bytes(ops_df):
    from src.exporter import export_ops_to_excel
    buf = export_ops_to_excel(ops_df)
    return buf.getvalue()

@st.cache_data(ttl="15m", show_spinner=False)
def load_benchmarks():
    from src.database import engine as _bench_engine
    import pandas as pd
    try:
        return pd.read_sql("SELECT * FROM ops_historical_benchmarks", _bench_engine)
    except Exception:
        return pd.DataFrame()

# --- 24H CACHED DATA ACCESS LAYER ---
@st.cache_data(ttl="15m", show_spinner=False)
def fetch_ops_data():
    from src.database import engine
    import pandas as pd
    try:
        return pd.read_sql("SELECT * FROM ops_telemarketing_data_materialized", engine)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl="15m", show_spinner=False)
def fetch_ops_snapshots_data():
    from src.database import engine
    import pandas as pd
    try:
        return pd.read_sql("SELECT * FROM ops_telemarketing_snapshots_materialized", engine)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl="15m", show_spinner=False)
def fetch_dashboard_pulse_data():
    from src.database import engine
    import pandas as pd
    try:
        df = pd.read_sql("SELECT * FROM dashboard_pulse_matrix", engine)
        if not df.empty and 'ops_date' in df.columns:
            df['ops_date'] = pd.to_datetime(df['ops_date'], errors='coerce')
            df.rename(columns={
                'total_records': 'Records',
                'total_conversions': 'KPI1-Conv.',
                'total_logins': 'KPI2-Login'
            }, inplace=True)
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl="15m", show_spinner=False)
def fetch_financial_data():
    from src.database import engine
    import pandas as pd
    try:
        df = pd.read_sql("SELECT * FROM raw_financial_data", engine)
        if not df.empty:
            df.rename(columns={"player_id": "id"}, inplace=True)
            if 'client' in df.columns: df['client'] = df['client'].astype(str).str.strip()
            if 'brand' in df.columns: df['brand'] = df['brand'].astype(str).str.strip()
        return df
    except Exception:
        return pd.DataFrame()
        
@st.cache_data(ttl="15m", show_spinner=False)
def fetch_config_tables(query):
    from src.database import engine
    import pandas as pd
    try:
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl="15m", show_spinner=False)
def fetch_ops_snapshots():
    from src.database import engine
    import pandas as pd
    try:
        return pd.read_sql("SELECT * FROM ops_telemarketing_snapshots", engine)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl="15m", show_spinner=False)
def _cached_sidebar_filters():
    raw_ops = fetch_ops_data()
    raw_fin = fetch_financial_data()
    
    db_clients = set()
    if not raw_ops.empty and 'ops_client' in raw_ops.columns: db_clients.update(raw_ops['ops_client'].unique())
    if not raw_fin.empty and 'client' in raw_fin.columns: db_clients.update(raw_fin['client'].unique())
    
    db_brands = set()
    if not raw_ops.empty and 'ops_brand' in raw_ops.columns: db_brands.update(raw_ops['ops_brand'].unique())
    if not raw_fin.empty and 'brand' in raw_fin.columns: db_brands.update(raw_fin['brand'].unique())
    
    avail_countries_raw = []
    if not raw_ops.empty and 'country' in raw_ops.columns:
        avail_countries_raw = sorted([str(c).upper() for c in raw_ops['country'].dropna().unique() if str(c) != ""])
        
    avail_products = sorted([str(c) for c in raw_ops['extracted_product'].dropna().unique() if c and c != "UNKNOWN"]) if not raw_ops.empty and 'extracted_product' in raw_ops.columns else []
    avail_languages = sorted([str(c) for c in raw_ops['extracted_language'].dropna().unique() if c and c != "UNKNOWN"]) if not raw_ops.empty and 'extracted_language' in raw_ops.columns else []
    avail_lifecycles = sorted([str(c) for c in raw_ops['extracted_lifecycle'].dropna().unique() if c and c != "UNKNOWN"]) if not raw_ops.empty and 'extracted_lifecycle' in raw_ops.columns else []
    avail_segments = sorted([str(c) for c in raw_ops['extracted_segment'].dropna().unique() if c and c != "UNKNOWN"]) if not raw_ops.empty and 'extracted_segment' in raw_ops.columns else []
    avail_sublifecycles = sorted([str(c) for c in raw_ops['extracted_sublifecycle'].dropna().unique() if c and c != "UNKNOWN"]) if not raw_ops.empty and 'extracted_sublifecycle' in raw_ops.columns else []
    avail_engagements = sorted([str(c) for c in raw_ops['extracted_engagement'].dropna().unique() if c and c != "UNKNOWN"]) if not raw_ops.empty and 'extracted_engagement' in raw_ops.columns else []

    return (
        sorted(list(db_clients)),
        sorted(list(db_brands)),
        avail_countries_raw,
        avail_products,
        avail_languages,
        avail_lifecycles,
        avail_segments,
        avail_sublifecycles,
        avail_engagements
    )

@st.cache_data(show_spinner=False)
def _get_master_excel_bytes(summary_df, cohort_matrices, segmentation, both_business):
    from src.exporter import export_to_excel
    buf = export_to_excel(summary_df, cohort_matrices=cohort_matrices, segmentation_df=segmentation, both_business_df=both_business)
    return buf.getvalue()

# ── Config ───────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)

BRANDS = ["latribet", "rojabet"]

# ── Page config ──────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Betting Financial Reports",
    page_icon="📊",
    layout="wide",
)

# --- UI/UX MODERNIZATION ---
st.markdown("""
    <style>
    /* Modernize Buttons */
    div.stButton > button {
        border-radius: 8px;
        border: 1px solid #3b82f6;
        transition: all 0.3s ease;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    }
    div.stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
        border: 1px solid #60a5fa;
    }

    /* Modernize DataFrames/Tables */
    div[data-testid="stDataFrame"] {
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }

    /* Modernize Expanders */
    div[data-testid="stExpander"] {
        border-radius: 8px;
        border: 1px solid #334155;
    }

    /* Subtly style the sidebar */
    section[data-testid="stSidebar"] {
        border-right: 1px solid #334155;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize persistent database tables
init_db()

# --- GLOBAL STATE HYDRATION (DB to RAM) ---
# This ensures all global dropdowns and legacy tabs populate instantly from the persistent RAM cache
try:
    # Hydrate Brand Mapping
    mapping_df = fetch_config_tables("SELECT brand_code, brand_name FROM client_mapping")
    if not mapping_df.empty:
        st.session_state["brand_mapping_dict"] = dict(zip(mapping_df["brand_code"], mapping_df["brand_name"]))
    else:
        st.session_state["brand_mapping_dict"] = {}

    # Hydrate Benchmarks
    st.session_state["benchmarks_df"] = load_benchmarks()

except Exception as e:
    st.sidebar.warning(f"Could not sync configuration to RAM: {e}")
# ------------------------------------------

# ── Matrix theme: neon glow CSS ──────────────────────────────────────────
st.markdown(
    """
    <style>
    /* Neon green glow on metric numbers */
    [data-testid="stMetricValue"] {
        text-shadow: 0 0 7px #00FF41, 0 0 14px #00FF4180;
    }
    /* Glow on metric delta text */
    [data-testid="stMetricDelta"] {
        text-shadow: 0 0 5px #00FF4160;
    }
    /* Subtle glow on headers */
    h1, h2, h3, h4 {
        text-shadow: 0 0 10px #00FF4140;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("📊 D-ROCK DASHBOARD V2.0")
st.markdown("*Enterprise Business Intelligence & Operations Command*")
st.markdown("---")

# ── 🔐 Enterprise Authentication & Data Security RBAC ──────────────────

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False
    st.session_state["user_role"] = None
    st.session_state["user_name"] = None
    st.session_state["allowed_clients"] = []

if not st.session_state["authenticated"]:
    st.markdown("---")
    c1, c2, c3 = st.columns([1, 1, 1])
    with c2:
        st.markdown("### 🔐 Secure Login Required")
        with st.form("login_form"):
            username = st.text_input("Username").strip().lower()
            password = st.text_input("Password", type="password")
            submit = st.form_submit_button("Authenticate")
            
            if submit:
                from src.database import engine
                import pandas as pd
                import json
                
                with st.spinner("Authenticating and securely retrieving configuration..."):
                    try:
                        import hashlib
                        pw_hash = hashlib.sha256(password.encode()).hexdigest()
                        query = "SELECT * FROM users WHERE username = %(u)s AND password_hash = %(p)s"
                        user_df = pd.read_sql(query, engine, params={"u": username, "p": pw_hash})
                        
                        if not user_df.empty:
                            user_record = user_df.iloc[0]
                            st.session_state["authenticated"] = True
                            st.session_state["user_role"] = user_record["role"]
                            st.session_state["user_name"] = user_record["name"]
                            
                            # Parse JSONB allowed_clients safely
                            raw_clients = user_record["allowed_clients"]
                            st.session_state["allowed_clients"] = json.loads(raw_clients) if isinstance(raw_clients, str) else raw_clients
                            
                            st.rerun()
                        else:
                            st.error("❌ Invalid username or password.")
                    except Exception as e:
                        st.error(f"Database connection error: {e}")
            st.stop() # CRITICAL: Halts execution of the rest of the app until logged in

# ── Session State Initialization ───────────────────────────────────────
if "data_loaded" not in st.session_state:
    st.session_state["data_loaded"] = False

# ═══════════════════════════════════════════════════════════════════════════
#  Data Control Room & Pipeline Execution
# ═══════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("### 🦅 CallsU Command **(v2.0.1)**")

    st.markdown("### 🧭 NAVIGATION")
    nav_options = ["📊 Dashboard"]
    if st.session_state.get("user_role") in ["Superadmin", "Admin", "Operations"]:
        nav_options.append("📞 Operations")
    if st.session_state.get("user_role") in ["Superadmin", "Admin", "Financial"]: 
        nav_options.append("🏦 Financial")
    if st.session_state.get("user_role") in ["Superadmin", "Admin"]:
        nav_options.append("⚙️ Admin")
        
    view_mode = st.radio("Go to:", nav_options)

    # --- 1. HYDRATE RAW DATA FROM CACHE ---
    import pandas as pd
    
    # Load from the 15m RAM cache instead of hitting PostgreSQL directly
    with st.spinner("Hydrating data from RAM cache..."):
        st.session_state["raw_ops_df"] = fetch_ops_data()
        st.session_state["raw_ops_snapshots_df"] = fetch_ops_snapshots_data()
        st.session_state["raw_fin_df"] = fetch_financial_data()
        st.session_state["raw_pulse_df"] = fetch_dashboard_pulse_data()
        
        # Also populate legacy global state pointers
        st.session_state["ops_df"] = st.session_state["raw_ops_df"]
        st.session_state["financial_df"] = st.session_state["raw_fin_df"]

        if st.session_state["raw_ops_df"].empty and st.session_state["raw_fin_df"].empty:
            st.warning("⚠️ The database is currently empty. Please navigate to the 🗄️ Operations Ingestion tab and upload your CSV files to initialize the schema.")

    raw_ops = st.session_state["raw_ops_df"]
    raw_fin = st.session_state["raw_fin_df"]

    # --- 2. SIDEBAR GLOBAL FILTERS & RBAC ---
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🌍 GLOBAL FILTERS")

    # --- MASSIVE PERFORMANCE BOOST ---
    # Fetch unique categories directly from the isolated memory cache instead of making Pandas 
    # extract distinct rows from 350,000+ string arrays on every generic UI button click.
    (
        db_clients, sorted_brands, avail_countries_raw, avail_products, 
        avail_languages, avail_lifecycles, avail_segments, avail_sublifecycles, avail_engagements
    ) = _cached_sidebar_filters()

    allowed = st.session_state.get("allowed_clients", ["All"])
    if "All" not in allowed:
        db_clients = [c for c in db_clients if c in allowed]

    # Pre-compute all dropdown options BEFORE the form
    b_map = st.session_state.get("brand_mapping_dict", {})
    
    country_map = {
        'PE': 'Peru', 'CL': 'Chile', 'EC': 'Ecuador', 'MX': 'Mexico', 'BR': 'Brazil',
        'AR': 'Argentina', 'CO': 'Colombia', 'ES': 'Spain', 'NZ': 'New Zealand', 
        'CA': 'Canada', 'IE': 'Ireland', 'DK': 'Denmark', 'SE': 'Sweden', 'NO': 'Norway',
        'FI': 'Finland', 'DE': 'Germany', 'AT': 'Austria', 'CH': 'Switzerland', 
        'UK': 'United Kingdom', 'GB': 'United Kingdom', 'IT': 'Italy', 'FR': 'France',
        'GLOBAL': 'Global', 'JP': 'Japan', 'TR': 'Turkey', 'ONT': 'Canada-Ontario'
    }
    
    PRODUCT_MAP = {'SPO': 'Sportsbook', 'CAS': 'Casino', 'LIVE': 'Live', 'ALL': 'All Products'}
    ENGAGEMENT_MAP = {'LI': 'Log In', 'NLI': 'Not Logged In'}
    
    # Default values — overridden inside the form if the dropdown renders
    selected_country = "All"
    selected_language = "All"
    selected_category = "All"
    selected_lifecycle = "All"
    selected_segment = "All"
    selected_sublifecycle = "All"
    selected_engagement = "All"
    selected_campaign = ""

    # ── FORM: Prevents reloads until Submit is clicked ──
    # Order matches campaign naming convention: Client-Brand-Country-Language-Product-Segment-Lifecycle-Sublifecycle-Engagement
    with st.sidebar.form("global_filters"):
        client_options = ["All"] + db_clients if db_clients else ["All"]
        selected_client = st.selectbox("🎯 Client", client_options)

        brand_options = ["All"] + sorted_brands if sorted_brands else ["All"]
        selected_brand = st.selectbox(
            "🏷️ Brand", 
            brand_options, 
            format_func=lambda x: f"{x} — {b_map[x]}" if x != "All" and x in b_map else x
        )

        if avail_countries_raw:
            display_countries = [country_map.get(c, c) for c in avail_countries_raw]
            selected_country_display = st.selectbox("🌍 Country", ["All"] + display_countries)
            if selected_country_display != "All":
                inv_map = {v: k for k, v in country_map.items()}
                selected_country = inv_map.get(selected_country_display, selected_country_display)

        if avail_languages:
            selected_language = st.selectbox("🗣️ Language", ["All"] + avail_languages)

        if avail_products:
            display_products = [PRODUCT_MAP.get(c, c) for c in avail_products]
            selected_product_display = st.selectbox("📦 Product", ["All"] + display_products)
            inv_map = {v: k for k, v in PRODUCT_MAP.items()}
            selected_category = inv_map.get(selected_product_display, selected_product_display) if selected_product_display != "All" else "All"

        if avail_segments:
            selected_segment = st.selectbox("🎯 Segment", ["All"] + avail_segments)

        if avail_lifecycles:
            selected_lifecycle = st.selectbox("🔁 Lifecycle", ["All"] + avail_lifecycles)

        if avail_sublifecycles:
            selected_sublifecycle = st.selectbox("📋 Sublifecycle", ["All"] + avail_sublifecycles)

        if avail_engagements:
            display_engagements = [ENGAGEMENT_MAP.get(e, e) for e in avail_engagements]
            selected_engagement_display = st.selectbox("🔥 Engagement", ["All"] + display_engagements)
            if selected_engagement_display != "All":
                inv_eng_map = {v: k for k, v in ENGAGEMENT_MAP.items()}
                selected_engagement = inv_eng_map.get(selected_engagement_display, selected_engagement_display)

        selected_campaign = st.text_input("🎯 Campaign", placeholder="Type to search campaigns...", help="Filter by campaign name (partial match)")

        _filters_submitted = st.form_submit_button("🔍 Apply Filters", type="primary")

    # 5. Elite Date Range Quick-Select Helper
    import re
    from datetime import timedelta

    raw_ops_snapshots = st.session_state.get("raw_ops_snapshots_df", pd.DataFrame())
    # Track boundaries across all datasets
    valid_mins = []
    valid_maxs = []
    
    if not raw_ops.empty and 'ops_date' in raw_ops.columns:
        valid_maxs.append(pd.to_datetime(raw_ops['ops_date']).max())
        valid_mins.append(pd.to_datetime(raw_ops['ops_date']).min())
        
    if not raw_fin.empty and 'report_month' in raw_fin.columns:
        fin_dates = pd.to_datetime(raw_fin['report_month'], format='mixed', errors='coerce')
        if not fin_dates.empty and not fin_dates.isna().all():
             # Push the maximum limit safely to the end of the month
             valid_maxs.append(fin_dates.max() + pd.offsets.MonthEnd(0))
             valid_mins.append(fin_dates.min())

    max_date = max([d for d in valid_maxs if pd.notnull(d)]) if valid_maxs and any(pd.notnull(valid_maxs)) else pd.Timestamp.today()
    min_db_date = min([d for d in valid_mins if pd.notnull(d)]) if valid_mins and any(pd.notnull(valid_mins)) else pd.Timestamp("2024-01-01")

    if pd.isna(min_db_date): min_db_date = pd.Timestamp("2024-01-01")
    if pd.isna(max_date): max_date = pd.Timestamp.today()

    # Streamlit slider min_value must be STRICTLY less than max_value
    if min_db_date.date() >= max_date.date():
        max_date = min_db_date + pd.Timedelta(days=1)

    def update_slider():
        preset = st.session_state["date_preset"]
        if preset == "Custom": return
        
        calc_start = None
        calc_end = max_date
        if preset == "Last 7 Days":
            calc_start = calc_end - pd.Timedelta(days=7)
        elif preset == "Last 30 Days":
            calc_start = calc_end - pd.Timedelta(days=30)
        elif preset == "Last 90 Days":
            calc_start = calc_end - pd.Timedelta(days=90)
        elif preset == "Current Month":
            calc_start = calc_end.replace(day=1)
        elif preset == "Last Month":
            calc_end = calc_end.replace(day=1) - pd.Timedelta(days=1)
            calc_start = calc_end.replace(day=1)

        if calc_start is not None:
            # Bound the calculated dates to the db min/max range to prevent Streamlit slider crash
            calc_start = max(calc_start, pd.to_datetime(min_db_date))
            calc_end = min(calc_end, pd.to_datetime(max_date))
            # Fallback if range inversion happens due to clamping
            if calc_start > calc_end:
                calc_start = calc_end
            st.session_state["date_slider_val"] = (calc_start.date(), calc_end.date())

    def update_preset():
        st.session_state["date_preset"] = "Custom"

    if "date_preset" not in st.session_state:
        st.session_state["date_preset"] = "Last 90 Days"
        update_slider()

    options = ["Custom", "Last 7 Days", "Last 30 Days", "Last 90 Days", "Current Month", "Last Month"]
    st.sidebar.radio("Quick Select", options, horizontal=False, key="date_preset", on_change=update_slider)

    if "date_slider_val" not in st.session_state:
        st.session_state["date_slider_val"] = (min_db_date.date(), max_date.date())

    start_date_val, end_date_val = st.sidebar.slider(
        "📅 Analysis Window",
        min_value=min_db_date.date(),
        max_value=max_date.date(),
        key="date_slider_val",
        format="YYYY-MM-DD",
        on_change=update_preset
    )

    start_date_str = start_date_val.strftime("%Y-%m-%d")
    end_date_str = end_date_val.strftime("%Y-%m-%d")
    start_month = start_date_val.strftime("%Y-%m")
    end_month = end_date_val.strftime("%Y-%m")

    # --- 3. APPLY FILTERS TO TABS ---
    filtered_ops = raw_ops.copy() if not raw_ops.empty else pd.DataFrame()
    filtered_ops_snapshots = raw_ops_snapshots.copy() if not raw_ops_snapshots.empty else pd.DataFrame()
    filtered_fin = raw_fin.copy() if not raw_fin.empty else pd.DataFrame()

    if selected_client != "All":
        if not filtered_ops.empty and 'ops_client' in filtered_ops.columns: 
            filtered_ops = filtered_ops[filtered_ops['ops_client'] == selected_client]
        if not filtered_ops_snapshots.empty and 'ops_client' in filtered_ops_snapshots.columns:
            filtered_ops_snapshots = filtered_ops_snapshots[filtered_ops_snapshots['ops_client'] == selected_client]
        if not filtered_fin.empty and 'client' in filtered_fin.columns: 
            filtered_fin = filtered_fin[filtered_fin['client'] == selected_client]

    if selected_brand != "All":
        if not filtered_ops.empty and 'ops_brand' in filtered_ops.columns: 
            filtered_ops = filtered_ops[filtered_ops['ops_brand'] == selected_brand]
        if not filtered_ops_snapshots.empty and 'ops_brand' in filtered_ops_snapshots.columns:
            filtered_ops_snapshots = filtered_ops_snapshots[filtered_ops_snapshots['ops_brand'] == selected_brand]
        if not filtered_fin.empty and 'brand' in filtered_fin.columns: 
            filtered_fin = filtered_fin[filtered_fin['brand'] == selected_brand]
            
    if selected_category != "All":
        if not filtered_ops.empty and 'extracted_product' in filtered_ops.columns:
            filtered_ops = filtered_ops[filtered_ops['extracted_product'] == selected_category]
        if not filtered_ops_snapshots.empty and 'extracted_product' in filtered_ops_snapshots.columns:
            filtered_ops_snapshots = filtered_ops_snapshots[filtered_ops_snapshots['extracted_product'] == selected_category]
            
    if selected_country != "All":
        if not filtered_ops.empty and 'country' in filtered_ops.columns:
            filtered_ops = filtered_ops[filtered_ops['country'].str.upper() == selected_country]
    
    if selected_language != "All":
        if not filtered_ops.empty and 'extracted_language' in filtered_ops.columns:
            filtered_ops = filtered_ops[filtered_ops['extracted_language'] == selected_language]
            
    if selected_lifecycle != "All":
        if not filtered_ops.empty and 'extracted_lifecycle' in filtered_ops.columns:
            filtered_ops = filtered_ops[filtered_ops['extracted_lifecycle'] == selected_lifecycle]
            
    if selected_segment != "All":
        if not filtered_ops.empty and 'extracted_segment' in filtered_ops.columns:
            filtered_ops = filtered_ops[filtered_ops['extracted_segment'] == selected_segment]

    if selected_sublifecycle != "All":
        if not filtered_ops.empty and 'extracted_sublifecycle' in filtered_ops.columns:
            filtered_ops = filtered_ops[filtered_ops['extracted_sublifecycle'] == selected_sublifecycle]
            
    if selected_engagement != "All":
        if not filtered_ops.empty and 'extracted_engagement' in filtered_ops.columns:
            filtered_ops = filtered_ops[filtered_ops['extracted_engagement'] == selected_engagement]

    if selected_campaign:
        if not filtered_ops.empty and 'campaign_name' in filtered_ops.columns:
            filtered_ops = filtered_ops[filtered_ops['campaign_name'].astype(str).str.contains(selected_campaign, case=False, na=False)]
        if not filtered_ops_snapshots.empty and 'campaign_name' in filtered_ops_snapshots.columns:
            filtered_ops_snapshots = filtered_ops_snapshots[filtered_ops_snapshots['campaign_name'].astype(str).str.contains(selected_campaign, case=False, na=False)]

    # Apply Time Frame Filter
    if start_date_val and end_date_val:
        if not filtered_ops.empty and 'ops_date' in filtered_ops.columns:
            filtered_ops = filtered_ops[(filtered_ops['ops_date'] >= start_date_str) & (filtered_ops['ops_date'] <= end_date_str)]
        if not filtered_ops_snapshots.empty and 'ops_date' in filtered_ops_snapshots.columns:
            filtered_ops_snapshots['ops_date'] = filtered_ops_snapshots['ops_date'].astype(str)
            filtered_ops_snapshots = filtered_ops_snapshots[(filtered_ops_snapshots['ops_date'] >= start_date_str) & (filtered_ops_snapshots['ops_date'] <= end_date_str)]
            
        if not filtered_fin.empty and 'report_month' in filtered_fin.columns:
            filtered_fin = filtered_fin[(filtered_fin['report_month'] >= start_month) & (filtered_fin['report_month'] <= end_month)]

    st.session_state["ops_df"] = filtered_ops
    st.session_state["ops_snapshots_df"] = filtered_ops_snapshots
    st.session_state["financial_df"] = filtered_fin

    # Master DF for Dashboard auto-hydration
    _master_df = filtered_fin

    # --- BOTTOM SIDEBAR: AUTHENTICATION ---
    # Use vertical space to push this to the bottom of the sidebar visually
    st.sidebar.markdown("<br>" * 10, unsafe_allow_html=True) 
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"👤 **Role:** {st.session_state['user_role']}")
    
    if st.sidebar.button("🚪 Logout", width='stretch'):
        st.session_state["authenticated"] = False
        st.session_state["user_role"] = None
        st.session_state["user_name"] = None
        st.session_state["allowed_clients"] = []
        st.rerun()

# ═══════════════════════════════════════════════════════════════════════════
#  System Settings View (Full-Screen, Superadmin Only)
# ═══════════════════════════════════════════════════════════════════════════
if view_mode == "⚙️ Admin":
    admin_mode = st.radio("Admin Modules:", ["🏢 Client Hub", "👥 User Management", "🧹 Data Maintenance", "📂 File Explorer"], horizontal=True)
    
    if admin_mode == "🏢 Client Hub":
        # Initialize router state
        if "managing_client" not in st.session_state:
            st.session_state["managing_client"] = None

        if st.session_state["managing_client"] is None:
            # ==========================================
            # VIEW A: THE MASTER HEALTH BOARD
            # ==========================================
            st.markdown("## 🏢 CLIENT HUB: Master Health Board")
            st.markdown("*Insight: Centralized view of client data completeness, SLAs, and active configurations.*")
            st.markdown("---")

            # --- ONBOARD NEW CLIENT ---
            with st.expander("➕ Onboard New Client"):
                with st.form("onboard_client_form"):
                    st.markdown("Register a brand new client by defining their first brand mapping.")
                    col1, col2 = st.columns(2)
                    with col1:
                        new_client = st.text_input("New Client Name (e.g., Betsson Group)").strip()
                        new_brand = st.text_input("First Brand Name (e.g., Betsson)").strip()
                    with col2:
                        new_tag = st.text_input("First Ops Tag (e.g., BETS)").strip().upper()
                        new_format = st.selectbox("Financial Format", ["Standard", "LeoVegas", "Offside"])
                    
                    if st.form_submit_button("Create Client"):
                        if new_client and new_brand and new_tag:
                            try:
                                execute_query(
                                    """INSERT INTO client_mapping (brand_code, brand_name, client_name, financial_format) 
                                       VALUES (:t, :b, :c, :f) 
                                       ON CONFLICT (brand_code) DO UPDATE SET brand_name = :b, client_name = :c, financial_format = :f""", 
                                    {"t": new_tag, "b": new_brand, "c": new_client, "f": new_format}
                                )
                                st.cache_data.clear()
                                st.success(f"Successfully onboarded {new_client}!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error onboarding client: {e}")
                        else:
                            st.warning("Please fill out the Client Name, Brand Name, and Ops Tag.")
            st.markdown("---")
    
            try:
                from src.database import engine
                import pandas as pd
            
                # Fetch base data for health calculations
                registry = pd.read_sql("SELECT client_name, brand_code FROM client_mapping", engine)
                slas = pd.read_sql("SELECT client_name, brand_code FROM contractual_volumes", engine)
                ops = pd.read_sql("SELECT ops_client as client, MAX(ops_date) as last_ops FROM ops_telemarketing_data GROUP BY ops_client", engine)
            
                try: fin = pd.read_sql("SELECT client, MAX(report_month) as last_fin FROM raw_financial_data GROUP BY client", engine)
                except: fin = pd.DataFrame(columns=['client', 'last_fin'])
            
                # Identify all unique clients
                all_clients = sorted(list(set(registry['client_name'].tolist() + ops['client'].tolist() + fin['client'].tolist())))
            
                if all_clients:
                    # Router UI
                    c1, c2, _ = st.columns([2, 1, 3])
                    target_client = c1.selectbox("Select Client Profile to Manage:", all_clients)
                    c2.markdown("<br>", unsafe_allow_html=True)
                    if c2.button("⚙️ Manage Profile", width='stretch'):
                        st.session_state["managing_client"] = target_client
                        st.rerun()
                    
                    st.markdown("### 📊 Global Client Health")
                
                    health_records = []
                    for c in all_clients:
                        client_brands = registry[registry['client_name'] == c]['brand_code'].nunique()
                        client_slas = slas[slas['client_name'] == c]['brand_code'].nunique()
                        l_ops = ops[ops['client'] == c]['last_ops'].max() if c in ops['client'].values else "Missing"
                        l_fin = fin[fin['client'] == c]['last_fin'].max() if c in fin['client'].values else "Missing"
                    
                        health_records.append({
                            "Client": c,
                            "Active Brands": client_brands,
                            "SLAs Configured": f"{client_slas} / {client_brands}",
                            "Last Ops Data": l_ops,
                            "Last Fin Data": l_fin,
                            "Status": "🟢 Healthy" if l_ops != "Missing" and l_fin != "Missing" else "🟡 Action Needed"
                        })
                
                    st.dataframe(pd.DataFrame(health_records), width='stretch', hide_index=True)
                    
                    # --- GLOBAL OPERATIONS RECAP ---
                    st.markdown("---")
                    st.subheader("🌍 Global Operations Recap")
                    recap_tabs = st.tabs(["🏢 By Client", "📅 By Month"])
                    
                    try:
                        # Fetch all historical ops
                        recap_df = pd.read_sql("SELECT ops_client, ops_date, records, kpi2_logins as logins, conversions, calls FROM ops_telemarketing_snapshots", engine)
                        
                        if not recap_df.empty and 'ops_date' in recap_df.columns:
                            # 1. By Client View
                            with recap_tabs[0]:
                                client_agg = recap_df.groupby('ops_client').agg(
                                    records=('records', 'sum'),
                                    logins=('logins', 'sum'),
                                    conversions=('conversions', 'sum'),
                                    calls=('calls', 'sum')
                                ).reset_index()
                                client_agg.columns = ['Client', 'Total Records', 'Total Logins', 'Total Conversions', 'Total Calls']
                                
                                # Add Grand Total Row
                                total_row_client = pd.DataFrame([{
                                    'Client': 'GRAND TOTAL',
                                    'Total Records': client_agg['Total Records'].sum(),
                                    'Total Logins': client_agg['Total Logins'].sum(),
                                    'Total Conversions': client_agg['Total Conversions'].sum(),
                                    'Total Calls': client_agg['Total Calls'].sum()
                                }])
                                client_agg = pd.concat([client_agg, total_row_client], ignore_index=True)
                                
                                st.dataframe(client_agg, width='stretch', hide_index=True)
                                
                            # 2. By Month View
                            with recap_tabs[1]:
                                recap_df['Month_Str'] = pd.to_datetime(recap_df['ops_date']).dt.strftime('%Y-%m')
                                month_agg = recap_df.groupby('Month_Str').agg(
                                    records=('records', 'sum'),
                                    logins=('logins', 'sum'),
                                    conversions=('conversions', 'sum'),
                                    calls=('calls', 'sum')
                                ).reset_index().sort_values('Month_Str', ascending=False)
                                month_agg.columns = ['Month', 'Total Records', 'Total Logins', 'Total Conversions', 'Total Calls']
                                
                                # Add Grand Total Row
                                total_row_month = pd.DataFrame([{
                                    'Month': 'GRAND TOTAL',
                                    'Total Records': month_agg['Total Records'].sum(),
                                    'Total Logins': month_agg['Total Logins'].sum(),
                                    'Total Conversions': month_agg['Total Conversions'].sum(),
                                    'Total Calls': month_agg['Total Calls'].sum()
                                }])
                                month_agg = pd.concat([month_agg, total_row_month], ignore_index=True)
                                
                                st.dataframe(month_agg, width='stretch', hide_index=True)
                        else:
                            st.info("No Operations data found to recap.")
                            
                    except Exception as e:
                        st.error(f"Error loading Operations Recap: {e}")
                        
                else:
                    st.info("No clients found in the database. Upload Ops data to seed the system.")
                
            except Exception as e:
                st.error(f"Error loading Master Health Board: {e}")

        else:
            # ==========================================
            # VIEW B: THE CLIENT DETAIL PROFILE
            # ==========================================
            client = st.session_state["managing_client"]
        
            c1, c2 = st.columns([4, 1])
            c1.markdown(f"## ⚙️ Profile: {client}")
            if c2.button("⬅️ Back to Hub", width='stretch'):
                st.session_state["managing_client"] = None
                st.rerun()
            
            with st.expander("✏️ Rename Client globally"):
                with st.form("rename_client_form"):
                    st.warning("⚠️ Renaming a client will retroactively update all historical Operations data, Financial data, Brand Mappings, and User Access lists.")
                    new_client_name = st.text_input("New Client Name", value=client).strip()
                    if st.form_submit_button("Cascade Rename"):
                        if new_client_name and new_client_name != client:
                            with st.spinner("Executing multi-table cascade update..."):
                                # 1. Update Mappings
                                execute_query("UPDATE client_mapping SET client_name = :new WHERE client_name = :old", {"new": new_client_name, "old": client})
                                # 2. Update Financial Data
                                execute_query("UPDATE raw_financial_data SET client = :new WHERE client = :old", {"new": new_client_name, "old": client})
                                # 3. Update Operations Data
                                execute_query("UPDATE ops_telemarketing_data SET ops_client = :new WHERE ops_client = :old", {"new": new_client_name, "old": client})

                                # 4. Update RBAC User Permissions (Safely parse JSON arrays)
                                try:
                                    import json
                                    users_df = pd.read_sql("SELECT username, allowed_clients FROM users", engine)
                                    for _, u_row in users_df.iterrows():
                                        u_name = u_row['username']
                                        u_clients_str = u_row['allowed_clients']
                                        if isinstance(u_clients_str, str):
                                            try:
                                                u_clients = json.loads(u_clients_str)
                                                if client in u_clients:
                                                    # Replace old client name with new one in their access list
                                                    u_clients = [new_client_name if c == client else c for c in u_clients]
                                                    execute_query("UPDATE users SET allowed_clients = :ac WHERE username = :u", {"ac": json.dumps(u_clients), "u": u_name})
                                            except: pass
                                except Exception as e:
                                    pass

                                # Clear Cache and Reload
                                st.cache_data.clear()
                                st.session_state["managing_client"] = new_client_name
                                st.success(f"Successfully renamed to {new_client_name}!")
                                st.rerun()

            st.markdown("---")
        
            from src.database import engine, execute_query
            import pandas as pd
        
            t_comp, t_reg, t_sla = st.tabs(["📊 Completeness & Uploads", "🏷️ Brand Registry", "⚖️ Contractual SLAs"])
        
            # ==========================================
            # TAB 1: COMPLETENESS & UPLOADS
            # ==========================================
            with t_comp:
                st.markdown(f"### 🗃️ Data Completeness: {client}")
                try:
                    brand_df = pd.read_sql(f"SELECT DISTINCT ops_brand FROM ops_telemarketing_snapshots WHERE ops_client = '{client}'", engine)
                    available_brands = ["All"] + sorted([b for b in brand_df['ops_brand'].tolist() if b and b.strip()])
                    selected_comp_brand = st.selectbox(f"Select Brand for {client}", options=available_brands, key="comp_brand_sel")
                    
                    # Operations Data SQL
                    query_ops = f"SELECT ops_date, records, kpi2_logins as logins, conversions, calls FROM ops_telemarketing_snapshots WHERE ops_client = '{client}'"
                    if selected_comp_brand != "All":
                        query_ops += f" AND ops_brand = '{selected_comp_brand}'"
                    ops_df = pd.read_sql(query_ops, engine)

                    # Financial Validation SQL
                    query_fin = f"SELECT DISTINCT report_month FROM raw_financial_data WHERE client = '{client}'"
                    if selected_comp_brand != "All":
                        query_fin += f" AND brand = '{selected_comp_brand}'"
                    fin_months_df = pd.read_sql(query_fin, engine)
                    financial_months_set = set(fin_months_df['report_month'].tolist())

                    def get_financial_completeness(target_month_str, financial_months_set):
                        if target_month_str in financial_months_set:
                            return "🟢 Complete"
                        
                        now = datetime.now()
                        try:
                            y, m = map(int, target_month_str.split('-'))
                            target_date = datetime(y, m, 1)
                            current_date_start = datetime(now.year, now.month, 1)
                            diff_months = (current_date_start.year - target_date.year) * 12 + (current_date_start.month - target_date.month)
                            
                            if diff_months <= 0:
                                return "⚪ Pending"
                            elif diff_months == 1:
                                return "🟡 Warning (Missing Last Month)"
                            else:
                                return "🔴 Issue (Missing +2 Months)"
                        except:
                            return "⚪ Unknown"

                    def get_completeness_status(year_month_str, actual_days_count):
                        """Evaluates completeness based on calendar days and current date."""
                        try:
                            year, month = map(int, year_month_str.split('-'))
                            _, total_days_in_month = calendar.monthrange(year, month)
                            now = datetime.now()
                            # Calculate Expected Days
                            if now.year == year and now.month == month:
                                expected_days = max(0, now.day - 1)
                            elif datetime(year, month, 1) > now:
                                expected_days = 0
                            else:
                                expected_days = total_days_in_month
                                
                            if expected_days == 0: return "⚪ N/A (Future)"
                            if actual_days_count == 0: return "🔴 Incomplete"
                            if actual_days_count >= expected_days: return "🟢 Complete"
                            if expected_days - actual_days_count <= 2: return f"🟡 Warning ({actual_days_count}/{expected_days} days)"
                            return f"🟠 Partial ({actual_days_count}/{expected_days} days)"
                        except Exception:
                            return "⚪ Unknown"

                    st.markdown("### 📊 Granular Data Completeness")
                    if not ops_df.empty and 'ops_date' in ops_df.columns:
                        ops_df['Month_Str'] = pd.to_datetime(ops_df['ops_date']).dt.strftime('%Y-%m')
                        
                        completeness_df = ops_df.groupby('Month_Str').agg(
                            actual_days=('ops_date', 'nunique'),
                            records=('records', 'sum'),
                            logins=('logins', 'sum'),
                            conversions=('conversions', 'sum'),
                            calls=('calls', 'sum')
                        ).reset_index()
                        
                        completeness_df.columns = ['Month', 'Actual Days Logged', 'Records', 'Logins', 'Conversions', 'Calls']
                        
                        completeness_df['Ops Status'] = completeness_df.apply(lambda row: get_completeness_status(row['Month'], row['Actual Days Logged']), axis=1)
                        completeness_df['Financial Status'] = completeness_df['Month'].apply(lambda m: get_financial_completeness(m, financial_months_set))
                        
                        completeness_df = completeness_df[['Month', 'Actual Days Logged', 'Ops Status', 'Records', 'Logins', 'Conversions', 'Calls', 'Financial Status']]
                        
                        st.dataframe(completeness_df.sort_values('Month', ascending=False), width='stretch', hide_index=True)
                        
                        st.markdown("#### 📅 Daily Ingestion Drill-Down")
                        available_months = sorted(completeness_df['Month'].tolist(), reverse=True)
                        if available_months:
                            selected_month = st.selectbox("Select Month for Daily Breakdown", options=available_months)
                            if selected_month:
                                daily_df = ops_df[ops_df['Month_Str'] == selected_month].groupby('ops_date').agg(
                                    records=('records', 'sum'),
                                    logins=('logins', 'sum'),
                                    conversions=('conversions', 'sum'),
                                    calls=('calls', 'sum')
                                ).reset_index()
                                daily_df.columns = ['Date', 'Records', 'Logins', 'Conversions', 'Calls']
                                st.dataframe(daily_df.sort_values('Date', ascending=False), width='stretch', hide_index=True)
                    else:
                        st.info("No Operations data found for this client to evaluate completeness.")
                        
                except Exception as e:
                    st.error(f"Completeness evaluation error: {e}")

                st.markdown("---")
                st.markdown(f"#### 📥 Upload {client} Financials")
                st.markdown("*Upload the NGR/Deposits file directly for this client.*")
                
                col_fin1, col_fin2 = st.columns([3, 1])
                with col_fin1:
                    fin_files = st.file_uploader("Upload Financial Files", accept_multiple_files=True, type=["csv", "xlsx"], key="client_fin_upload")
                with col_fin2:
                    st.markdown("<br>", unsafe_allow_html=True)
                    if st.button("🔄 Force Refresh Cache", key="refresh_fin"):
                        fetch_financial_data.clear()
                        fetch_config_tables.clear()
                        st.rerun()

                if st.button("Run Financial Ingestion", width='stretch') and fin_files:
                    from src.ingestion import load_all_data_from_uploads
                    with st.spinner("Processing..."):
                        df, reg = load_all_data_from_uploads(fin_files)
                        if not df.empty:
                            st.session_state["registry"] = reg
                            # Invalidate the global cache before rerunning
                            fetch_financial_data.clear()
                            fetch_config_tables.clear()
                            if "raw_fin_df" in st.session_state: del st.session_state["raw_fin_df"]
                            st.success(f"Successfully saved financials for {client}!")
                            st.rerun()

            # ==========================================
            # TAB 2: BRAND REGISTRY
            # ==========================================
            with t_reg:
                st.markdown(f"### 🏷️ Brand Registry: {client}")
                try:
                    registry_df = pd.read_sql(f"SELECT brand_name as \"Brand Name\", brand_code as \"Ops Tag\" FROM client_mapping WHERE client_name = '{client}' ORDER BY brand_name", engine)
                    st.dataframe(registry_df, width='stretch', hide_index=True)
                except Exception as e:
                    st.info("No brands registered yet.")
                
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("#### ➕ Add Tag")
                    with st.form("add_client_tag_form"):
                        new_tag = st.text_input("Ops Tag (e.g., ROJB, LV)")
                        new_brand = st.text_input("Brand Name (e.g., Rojabet, Leovegas)")
                        new_format = st.selectbox("Financial Format", ["Standard", "LeoVegas", "Offside"])
                        if st.form_submit_button("Register Tag"):
                            execute_query("""INSERT INTO client_mapping (brand_code, brand_name, client_name, financial_format) 
                                             VALUES (:t, :b, :c, :f) 
                                             ON CONFLICT (brand_code) DO UPDATE SET brand_name = :b, client_name = :c, financial_format = :f""", 
                                          {"t": new_tag.upper().strip(), "b": new_brand.strip(), "c": client, "f": new_format})

                            execute_query("UPDATE ops_telemarketing_data SET ops_client = :c, ops_brand = :b WHERE ops_client = 'UNKNOWN' AND ops_brand = :t",
                                          {"c": client, "b": new_brand.strip(), "t": new_tag.upper().strip()})

                            if "unmapped_tags" in st.session_state and new_tag.upper().strip() in st.session_state.get("unmapped_tags", set()):
                                st.session_state["unmapped_tags"].remove(new_tag.upper().strip())
                            st.cache_data.clear()
                            st.success(f"Saved {new_tag.upper()}!")
                            st.rerun()
                with c2:
                    st.markdown("#### 🗑️ Delete Tag")
                    with st.form("del_client_tag_form"):
                        del_tag = st.text_input("Ops Tag to Remove")
                        if st.form_submit_button("Delete Tag"):
                            execute_query("DELETE FROM client_mapping WHERE brand_code = :t AND client_name = :c", {"t": del_tag.upper().strip(), "c": client})
                            if "raw_ops_df" in st.session_state: del st.session_state["raw_ops_df"]
                            st.success("Deleted!")
                            st.rerun()

            # ==========================================
            # TAB 3: SLA & BENCHMARKS
            # ==========================================
            with t_sla:
                st.markdown(f"### ⚖️ SLAs & Benchmarks: {client}")
                
                # --- Sub-Tab 1: Monthly Volumes ---
                st.markdown("#### 1️⃣ Monthly Volume Minimums")
                try:
                    vol_df = pd.read_sql(f"SELECT brand_code as \"Brand\", lifecycle as \"Lifecycle\", monthly_minimum_records as \"Min Records\" FROM contractual_volumes WHERE client_name = '{client}'", engine)
                    st.dataframe(vol_df, width='stretch', hide_index=True)
                except Exception as e:
                    st.info("No Volume Targets set.")
                
                c3, c4 = st.columns(2)
                with c3:
                    with st.form("add_client_vol_form"):
                        vol_brand = st.text_input("Brand (e.g., Rojabet)")
                        vol_lifecycle = st.selectbox("Lifecycle", ["RND", "WB", "AFF", "ALL"])
                        vol_min = st.number_input("Monthly Min Records", min_value=0, step=100)
                        if st.form_submit_button("Set Volume Minimum"):
                            execute_query("""INSERT INTO contractual_volumes (client_name, brand_code, lifecycle, monthly_minimum_records) 
                                             VALUES (:c, :b, :l, :m) ON CONFLICT (client_name, brand_code, lifecycle) 
                                             DO UPDATE SET monthly_minimum_records = :m""",
                                          {"c": client, "b": vol_brand.strip(), "l": vol_lifecycle.upper(), "m": vol_min})
                            st.success("Volume Saved!")
                            st.rerun()
                with c4:
                    with st.form("del_client_vol_form"):
                        del_vol_brand = st.text_input("Brand Name to Remove")
                        del_vol_lc = st.selectbox("Lifecycle to Remove", ["RND", "WB", "AFF", "ALL"])
                        if st.form_submit_button("Delete Volume"):
                            execute_query("DELETE FROM contractual_volumes WHERE client_name = :c AND brand_code = :b AND lifecycle = :l", 
                                          {"c": client, "b": del_vol_brand.strip(), "l": del_vol_lc.upper()})
                            st.success("Deleted!")
                            st.rerun()

                st.markdown("---")
                
                # --- Sub-Tab 2: Granular Benchmarks ---
                st.markdown("#### 2️⃣ Granular Campaign Benchmarks")
                try:
                    bench_df = pd.read_sql(f"SELECT brand_code as \"Brand\", campaign_signature as \"Campaign Signature\", target_conv_pct * 100 as \"Target Conv%\", target_li_pct * 100 as \"Target LI%\", target_cac_usd as \"Target True CAC ($)\" FROM granular_benchmarks WHERE client_name = '{client}'", engine)
                    st.dataframe(bench_df, width='stretch', hide_index=True)
                except Exception as e:
                    st.info("No Efficiency Benchmarks set.")
                
                c5, c6 = st.columns(2)
                with c5:
                    with st.form("add_client_bench_form"):
                        bench_brand = st.text_input("Brand")
                        bench_sig = st.text_input("Campaign Signature (e.g., BAH-CH-ALL-RND-LI)")
                        bench_conv = st.number_input("Target Conv (%)", min_value=0.0, step=0.1)
                        bench_li = st.number_input("Target LI (%)", min_value=0.0, step=0.1)
                        bench_cac = st.number_input("Target CAC ($)", min_value=0.0, step=0.5)
                        if st.form_submit_button("Set Benchmark"):
                            execute_query("""INSERT INTO granular_benchmarks (client_name, brand_code, campaign_signature, target_conv_pct, target_li_pct, target_cac_usd) 
                                             VALUES (:c, :b, :s, :p, :li, :t) ON CONFLICT (campaign_signature) 
                                             DO UPDATE SET target_conv_pct = :p, target_li_pct = :li, target_cac_usd = :t, brand_code = :b, client_name = :c""",
                                          {"c": client, "b": bench_brand.strip(), "s": bench_sig.strip().upper(), "p": bench_conv / 100.0, "li": bench_li / 100.0, "t": bench_cac})
                            st.success("Benchmark Saved!")
                            st.rerun()
                with c6:
                    with st.form("del_client_bench_form"):
                        del_bench_sig = st.text_input("Signature to Remove")
                        if st.form_submit_button("Delete Benchmark"):
                            execute_query("DELETE FROM granular_benchmarks WHERE client_name = :c AND campaign_signature = :s", 
                                          {"c": client, "s": del_bench_sig.strip().upper()})
                            st.success("Deleted!")
                            st.rerun()
            
    elif admin_mode == "👥 User Management":
        st.markdown("## 👥 USER MANAGEMENT")
        st.markdown("*Insight: Control system access and restrict data visibility via Role-Based Access Control (RBAC).*")
        st.markdown("---")
        
        from src.database import engine, execute_query
        import pandas as pd
        import json
        
        # Show existing users
        st.markdown("### 📋 Active Users")
        try:
            users_df = pd.read_sql("SELECT username, name, role, allowed_clients FROM users", engine)
            # Format JSON for display
            users_df["allowed_clients"] = users_df["allowed_clients"].apply(lambda x: ", ".join(json.loads(x)) if isinstance(x, str) else ", ".join(x))
            st.dataframe(users_df, width='stretch', hide_index=True)
        except Exception as e:
            st.error(f"Could not load users: {e}")
            
        st.markdown("---")
        st.markdown("---")

        # Extract existing users for selectboxes
        try:
            existing_users = users_df["username"].tolist() if not users_df.empty else []
            non_super_users = [u for u in existing_users if u != "superadmin"]
        except:
            existing_users, non_super_users = [], []

        c1, c2 = st.columns(2)

        with c1:
            st.markdown("#### ⚙️ User Settings")
            mode = st.radio("Action", ["➕ Create New User", "✏️ Edit Existing"], horizontal=True)

            target_user = ""
            default_role = "Operations"
            default_name = ""
            default_clients = ["All"]

            if mode == "✏️ Edit Existing" and non_super_users:
                target_user = st.selectbox("Select User to Edit", non_super_users)
                if target_user:
                    # Hydrate form with DB data
                    user_row = users_df[users_df["username"] == target_user].iloc[0]
                    default_role = user_row["role"]
                    default_name = user_row["name"] if pd.notna(user_row["name"]) else ""
                    raw_ac = user_row["allowed_clients"]
                    default_clients = [c.strip() for c in raw_ac.split(",")] if isinstance(raw_ac, str) else ["All"]
            elif mode == "✏️ Edit Existing" and not non_super_users:
                st.info("No standard users to edit.")

            with st.form("user_crud_form"):
                u_username = st.text_input("Username (lowercase)", value=target_user, disabled=(mode == "✏️ Edit Existing")).strip().lower()
                u_password = st.text_input("Password", placeholder="Leave blank to keep existing password" if mode == "✏️ Edit Existing" else "")
                u_name = st.text_input("Display Name", value=default_name)

                role_index = ["Superadmin", "Admin", "Operations", "Financial"].index(default_role) if default_role in ["Superadmin", "Admin", "Operations", "Financial"] else 2
                u_role = st.selectbox("Role", ["Superadmin", "Admin", "Operations", "Financial"], index=role_index)

                try:
                    clients_df = pd.read_sql("SELECT DISTINCT client_name FROM client_mapping", engine)
                    available_clients = ["All"] + clients_df["client_name"].tolist()
                except:
                    available_clients = ["All"]

                # Ensure defaults exist in available options
                safe_defaults = [c for c in default_clients if c in available_clients]
                if not safe_defaults: safe_defaults = ["All"]

                u_clients = st.multiselect("Allowed Clients", available_clients, default=safe_defaults)

                if st.form_submit_button("Save User"):
                    if u_username:
                        if mode == "✏️ Edit Existing" and not u_password:
                            # Update without touching password
                            execute_query("""UPDATE users SET role = :r, name = :n, allowed_clients = :ac WHERE username = :u""",
                                          {"u": u_username, "r": u_role, "n": u_name, "ac": json.dumps(u_clients)})
                        elif len(u_password) < 4:
                            st.error("⚠️ Password must be at least 4 characters.")
                            st.stop()
                        else:
                            import hashlib
                            pw_hash = hashlib.sha256(u_password.encode()).hexdigest()
                            # Full UPSERT
                            execute_query(
                                """INSERT INTO users (username, password_hash, role, name, allowed_clients) 
                                   VALUES (:u, :p, :r, :n, :ac) 
                                   ON CONFLICT (username) DO UPDATE SET 
                                   password_hash = :p, role = :r, name = :n, allowed_clients = :ac""",
                                {"u": u_username, "p": pw_hash, "r": u_role, "n": u_name, "ac": json.dumps(u_clients)}
                            )
                        st.success(f"User {u_username} saved!")
                        st.rerun()
                    else:
                        st.error("Username required.")

        with c2:
            st.markdown("#### 🗑️ Revoke Access")
            with st.form("delete_user_form"):
                del_user = st.selectbox("Select User to Delete", non_super_users) if non_super_users else None
                if st.form_submit_button("Delete User") and del_user:
                    execute_query("DELETE FROM users WHERE username = :u", {"u": del_user})
                    st.success(f"User {del_user} deleted!")
                    st.rerun()

    elif admin_mode == "🧹 Data Maintenance":
        st.markdown("## 🧹 DATA MAINTENANCE")
        st.markdown("*Purge cached files or database records to force a clean re-sync from the CallsU API.*")
        st.markdown("---")

        import shutil
        from src.database import engine as _maint_engine, execute_query as _maint_exec

        callsu_dir = "data/raw/callsu_daily"

        # --- Show current state ---
        file_count = 0
        folder_size_mb = 0.0
        if os.path.exists(callsu_dir):
            for root, dirs, files in os.walk(callsu_dir):
                for f in files:
                    fp = os.path.join(root, f)
                    file_count += 1
                    folder_size_mb += os.path.getsize(fp) / (1024 * 1024)

        try:
            ops_count = pd.read_sql("SELECT COUNT(*) as cnt FROM ops_telemarketing_data", _maint_engine).iloc[0]['cnt']
            snap_count = pd.read_sql("SELECT COUNT(*) as cnt FROM ops_telemarketing_snapshots", _maint_engine).iloc[0]['cnt']
        except:
            ops_count, snap_count = 0, 0

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("📁 Local Files", f"{file_count:,}")
        m2.metric("💾 Folder Size", f"{folder_size_mb:.1f} MB")
        m3.metric("🗃️ Ops DB Rows", f"{ops_count:,}")
        m4.metric("📸 Snapshot Rows", f"{snap_count:,}")

        st.markdown("---")

        pc1, pc2 = st.columns(2)

        # --- PURGE LOCAL FILES ---
        with pc1:
            st.markdown("### 📁 Purge Local Files")
            st.markdown(f"Deletes all `.xlsx` files from `{callsu_dir}/`.")
            st.markdown("The API sync will re-download them on next trigger.")
            with st.expander("⚠️ Confirm File Purge", expanded=False):
                confirm_files = st.checkbox("I understand this will delete all cached CallsU files", key="confirm_purge_files")
                if st.button("🗑️ Purge All Local Files", disabled=not confirm_files, width='stretch', type="primary"):
                    if os.path.exists(callsu_dir):
                        shutil.rmtree(callsu_dir)
                        os.makedirs(callsu_dir, exist_ok=True)
                        st.success(f"✅ Purged {file_count} files ({folder_size_mb:.1f} MB freed)")
                        st.rerun()
                    else:
                        st.info("No files to purge.")

        # --- PURGE DATABASE ---
        with pc2:
            st.markdown("### 🗃️ Purge Operations Database")
            st.markdown("Truncates `ops_telemarketing_data` and `ops_telemarketing_snapshots`.")
            st.markdown("The API sync will re-ingest on next trigger.")
            with st.expander("⚠️ Confirm Database Purge", expanded=False):
                confirm_db = st.checkbox("I understand this will permanently delete all operations data", key="confirm_purge_db")
                if st.button("🗑️ Purge Operations DB", disabled=not confirm_db, width='stretch', type="primary"):
                    try:
                        _maint_exec("TRUNCATE TABLE ops_telemarketing_data RESTART IDENTITY")
                        _maint_exec("TRUNCATE TABLE ops_telemarketing_snapshots RESTART IDENTITY")
                        # Clear cached session state
                        for key in ["raw_ops_df", "raw_ops_snapshots_df", "ops_df", "benchmarks_df"]:
                            if key in st.session_state:
                                del st.session_state[key]
                        st.success(f"✅ Purged {ops_count:,} ops rows + {snap_count:,} snapshot rows")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Purge failed: {e}")

        st.markdown("---")
        st.markdown("### 📊 Benchmark Snapshots")
        st.markdown("*Generate historical daily-average benchmarks from completed half-year periods. These power the H-over-H comparison table in the Dashboard.*")

        # Detect completed half-years from ops_telemarketing_data
        try:
            date_range = pd.read_sql("SELECT MIN(ops_date) as min_d, MAX(ops_date) as max_d FROM ops_telemarketing_data", _maint_engine)
            existing_benchmarks = pd.read_sql("SELECT DISTINCT benchmark_period FROM ops_historical_benchmarks", _maint_engine)['benchmark_period'].tolist()
        except:
            date_range = pd.DataFrame()
            existing_benchmarks = []

        if not date_range.empty and pd.notna(date_range.iloc[0]['min_d']):
            from datetime import datetime as _dt
            min_date = pd.to_datetime(date_range.iloc[0]['min_d'])
            max_date = pd.to_datetime(date_range.iloc[0]['max_d'])
            now = _dt.now()

            # Build list of completed half-years
            completed_halves = []
            year = min_date.year
            while year <= max_date.year:
                # H1: Jan-Jun
                h1_end = _dt(year, 6, 30)
                if h1_end < now and min_date <= h1_end:
                    completed_halves.append({
                        "label": f"H1 {year}",
                        "start": f"{year}-01-01",
                        "end": f"{year}-06-30",
                        "generated": f"H1 {year}" in existing_benchmarks
                    })
                # H2: Jul-Dec
                h2_end = _dt(year, 12, 31)
                if h2_end < now and min_date <= h2_end:
                    completed_halves.append({
                        "label": f"H2 {year}",
                        "start": f"{year}-07-01",
                        "end": f"{year}-12-31",
                        "generated": f"H2 {year}" in existing_benchmarks
                    })
                year += 1

            if completed_halves:
                for h in completed_halves:
                    bc1, bc2, bc3 = st.columns([3, 2, 2])
                    with bc1:
                        status = "✅ Generated" if h["generated"] else "⬜ Not generated"
                        st.markdown(f"**{h['label']}** ({h['start']} → {h['end']}) — {status}")
                    with bc2:
                        btn_label = "🔄 Regenerate" if h["generated"] else "⚡ Generate"
                        if st.button(btn_label, key=f"gen_bench_{h['label']}", width='stretch'):
                            with st.spinner(f"Generating benchmarks for {h['label']}..."):
                                from scripts.jobs.generate_benchmarks import generate_benchmarks
                                generate_benchmarks(h["start"], h["end"], h["label"])
                                st.success(f"✅ Benchmarks generated for {h['label']}!")
                                st.rerun()
                    with bc3:
                        if h["generated"]:
                            if st.button("🗑️ Delete", key=f"del_bench_{h['label']}", width='stretch'):
                                _maint_exec(f"DELETE FROM ops_historical_benchmarks WHERE benchmark_period = '{h['label']}'")
                                st.success(f"Deleted benchmarks for {h['label']}")
                                st.rerun()
            else:
                st.info("No completed half-year periods found in the data yet.")
        else:
            st.info("No operations data available to generate benchmarks from.")

    elif admin_mode == "📂 File Explorer":
        st.markdown("## 📂 FILE EXPLORER")
        st.markdown("*Browse application data and documentation files.*")
        st.markdown("---")

        from datetime import datetime as _fe_dt

        _EXPLORER_ROOTS = {"data": "data", "docs": "docs"}

        def _fmt_size(size_bytes):
            if size_bytes >= 1_048_576:
                return f"{size_bytes / 1_048_576:.1f} MB"
            elif size_bytes >= 1024:
                return f"{size_bytes / 1024:.1f} KB"
            return f"{size_bytes} B"

        def _count_recursive(path):
            count, size = 0, 0
            if os.path.exists(path):
                for dp, _, fns in os.walk(path):
                    for f in fns:
                        count += 1
                        try: size += os.path.getsize(os.path.join(dp, f))
                        except: pass
            return count, size

        # --- INVENTORY DASHBOARD ---
        inv_cols = st.columns(len(_EXPLORER_ROOTS))
        for i, (label, path) in enumerate(_EXPLORER_ROOTS.items()):
            cnt, sz = _count_recursive(path)
            with inv_cols[i]:
                st.metric(f"📁 {label}/", f"{cnt:,} files")
                st.caption(f"💾 {_fmt_size(sz)}")

        st.markdown("---")

        # --- FOLDER NAVIGATION STATE ---
        if "fe_current_path" not in st.session_state:
            st.session_state["fe_current_path"] = None

        current_path = st.session_state["fe_current_path"]

        if current_path is None:
            st.markdown("#### Select a root folder:")
            for label, path in _EXPLORER_ROOTS.items():
                cnt, sz = _count_recursive(path)
                if st.button(f"📁 {label}/ — {cnt:,} files ({_fmt_size(sz)})", key=f"fe_root_{label}", width='stretch'):
                    st.session_state["fe_current_path"] = path
                    st.rerun()
        else:
            # --- BREADCRUMB BAR ---
            parts = current_path.replace("\\", "/").split("/")
            bc_cols = st.columns(len(parts) + 1)
            with bc_cols[0]:
                if st.button("🏠", key="fe_bc_home", help="Back to root"):
                    st.session_state["fe_current_path"] = None
                    st.rerun()
            for idx, part in enumerate(parts):
                with bc_cols[idx + 1]:
                    is_last = (idx == len(parts) - 1)
                    if is_last:
                        st.markdown(f"**📂 {part}/**")
                    else:
                        if st.button(f"📂 {part}/", key=f"fe_bc_{idx}"):
                            st.session_state["fe_current_path"] = "/".join(parts[:idx + 1])
                            st.rerun()

            st.markdown("---")

            if not os.path.exists(current_path):
                st.warning(f"Path `{current_path}` does not exist.")
                if st.button("← Back to root"):
                    st.session_state["fe_current_path"] = None
                    st.rerun()
            else:
                entries = sorted(os.listdir(current_path))
                subdirs = [e for e in entries if os.path.isdir(os.path.join(current_path, e))]
                files = [e for e in entries if os.path.isfile(os.path.join(current_path, e))]

                # --- SUBFOLDERS ---
                if subdirs:
                    st.markdown(f"##### 📁 Folders ({len(subdirs)})")
                    for row_start in range(0, len(subdirs), 4):
                        row_dirs = subdirs[row_start:row_start + 4]
                        folder_cols = st.columns(4)
                        for j, d in enumerate(row_dirs):
                            full_d = os.path.join(current_path, d)
                            cnt, sz = _count_recursive(full_d)
                            with folder_cols[j]:
                                if st.button(f"📂 {d}/\n{cnt} files • {_fmt_size(sz)}", key=f"fe_dir_{d}", width='stretch'):
                                    st.session_state["fe_current_path"] = full_d.replace("\\", "/")
                                    st.rerun()

                # --- FILES ---
                if files:
                    st.markdown(f"##### 📄 Files ({len(files)})")
                    file_data = []
                    for fname in files:
                        fpath = os.path.join(current_path, fname)
                        try:
                            stat = os.stat(fpath)
                            file_data.append({
                                "Name": fname, "Size": _fmt_size(stat.st_size),
                                "Modified": _fe_dt.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                                "_abs": fpath
                            })
                        except: pass

                    if file_data:
                        files_df = pd.DataFrame(file_data)
                        st.dataframe(
                            files_df[["Name", "Size", "Modified"]], width='stretch', hide_index=True,
                            column_config={
                                "Name": st.column_config.TextColumn("📄 Name", width="large"),
                                "Size": st.column_config.TextColumn("💾 Size"),
                                "Modified": st.column_config.TextColumn("📅 Modified")
                            },
                            height=min(len(files_df) * 35 + 40, 400)
                        )

                        file_names = files_df["Name"].tolist()
                        _search = st.text_input("🔎 Search:", placeholder="Filter files...", key="fe_search")
                        if _search:
                            file_names = [f for f in file_names if _search.lower() in f.lower()]
                            st.caption(f"{len(file_names)} match(es)")

                        sel = st.selectbox("🔍 Open file:", ["— Select —"] + file_names, key="fe_file_sel")
                        if sel != "— Select —":
                            row = files_df[files_df["Name"] == sel].iloc[0]
                            abs_path = row["_abs"]
                            ext = os.path.splitext(abs_path)[1].lower()
                            st.markdown(f"**{sel}** — {row['Size']} • {row['Modified']}")

                            _mime = {".csv": "text/csv", ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                     ".xls": "application/vnd.ms-excel", ".md": "text/markdown", ".txt": "text/plain",
                                     ".json": "application/json", ".log": "text/plain", ".py": "text/x-python"}
                            if ext in _mime:
                                with open(abs_path, "rb") as _dl:
                                    st.download_button(f"⬇️ Download {sel}", _dl.read(), sel, _mime[ext], key=f"dl_{sel}")

                            try:
                                if ext == ".csv":
                                    df = pd.read_csv(abs_path)
                                    st.caption(f"📊 {len(df):,} rows × {len(df.columns)} cols")
                                    st.dataframe(df, width='stretch', hide_index=True, height=500)
                                elif ext in [".xlsx", ".xls"]:
                                    df = pd.read_excel(abs_path)
                                    st.caption(f"📊 {len(df):,} rows × {len(df.columns)} cols")
                                    st.dataframe(df, width='stretch', hide_index=True, height=500)
                                elif ext == ".md":
                                    with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
                                        st.markdown("---"); st.markdown(f.read())
                                elif ext in [".txt", ".log", ".json", ".yml", ".yaml", ".toml", ".env", ".py"]:
                                    with open(abs_path, "r", encoding="utf-8", errors="replace") as f:
                                        content = f.read(50000)
                                    lang = {".py": "python", ".json": "json", ".yml": "yaml", ".yaml": "yaml"}.get(ext, "text")
                                    if len(content) >= 50000: st.caption("First 50K chars")
                                    st.code(content, language=lang)
                                else:
                                    st.info(f"No viewer for `{ext}` files.")
                            except Exception as e:
                                st.error(f"Could not open: {e}")

                if not subdirs and not files:
                    st.info("This folder is empty.")

    st.stop()  # Don't render Main Workspace below

# 🌍 FINANCIAL DATA FILTERS (for legacy Financial/CRM tabs)
st.session_state["data_loaded"] = not _master_df.empty
selected_country = "All"
revenue_mode = st.sidebar.radio("Revenue Metric", ["GGR", "NGR"], horizontal=True) if not _master_df.empty else "GGR"
rev_col = "ggr" if revenue_mode == "GGR" else "ngr"

# ── ROUTED VIEWS ──
tab_map = {}
run_clicked = False


# --- GLOBAL BENCHMARK RENDER HELPER ---
def _render_fixed_benchmark(df, prior_half="H2 2025"):
    """Render half-year baseline comparison with selectable prior period."""
    from datetime import datetime

    if df.empty or 'ops_date' not in df.columns:
        st.caption("No data available for benchmark.")
        return

    df['ops_date'] = pd.to_datetime(df['ops_date'], errors='coerce')

    now = datetime.now()
    current_year = now.year
    current_month = now.month

    # Current half-year (auto-detected)
    if current_month <= 6:
        half_label = "H1"
        curr_start, curr_end = f"{current_year}-01-01", f"{current_year}-06-30"
    else:
        half_label = "H2"
        curr_start, curr_end = f"{current_year}-07-01", f"{current_year}-12-31"

    # Parse selected prior half (e.g. "H2 2025" → 2025-07-01, 2025-12-31)
    prior_parts = prior_half.split()
    prior_h = prior_parts[0]  # H1 or H2
    prior_year = int(prior_parts[1])
    if prior_h == "H1":
        prior_start, prior_end = f"{prior_year}-01-01", f"{prior_year}-06-30"
    else:
        prior_start, prior_end = f"{prior_year}-07-01", f"{prior_year}-12-31"

    curr_label = f"{half_label} {current_year}"
    prior_label = f"{prior_half} Baseline"

    curr_df = df[(df['ops_date'] >= curr_start) & (df['ops_date'] <= curr_end)]
    prior_df = df[(df['ops_date'] >= prior_start) & (df['ops_date'] <= prior_end)]

    if curr_df.empty and prior_df.empty:
        st.caption("No data available for either half-year period.")
        return

    def safe_sum(frame, col):
        return frame[col].sum() if col in frame.columns else 0

    def safe_pct(num, denom):
        return (num / denom * 100) if denom > 0 else 0

    def fmt_num(val):
        if val >= 1_000_000:
            return f"{val/1_000_000:.2f}M"
        elif val >= 1_000:
            return f"{val/1_000:.1f}K"
        return f"{val:,.0f}"

    def fmt_pct(val):
        return f"{val:.1f}%"

    def calc_delta(curr_val, prior_val, is_pct=False):
        if is_pct:
            diff = curr_val - prior_val
            arrow = "↑" if diff > 0 else "↓" if diff < 0 else "→"
            return f"{arrow} {abs(diff):.1f}pp"
        else:
            if prior_val == 0:
                return "N/A"
            change = ((curr_val - prior_val) / prior_val) * 100
            arrow = "↑" if change > 0 else "↓" if change < 0 else "→"
            return f"{arrow} {abs(change):.1f}%"

    # Aggregate sums
    agg_cols = ['records', 'kpi2_logins', 'conversions',
         'd_plus', 'd_minus', 'd_neutral', 'na', 't', 'dnc', 'dx', 'wn', 'am',
         'es', 'ed', 'eo', 'ec', 'ef', 'sd', 'sf', 'sp']
    c = {col: safe_sum(curr_df, col) for col in agg_cols}
    p = {col: safe_sum(prior_df, col) for col in agg_cols}

    # Derive SS (SMS Sent) = sd + sf + sp
    c['ss'] = c['sd'] + c['sf'] + c['sp']
    p['ss'] = p['sd'] + p['sf'] + p['sp']

    # Pre-compute rates for reuse in cards and charts
    c_d = safe_pct(c['d_plus'] + c['d_minus'] + c['d_neutral'], c['records'])
    p_d = safe_pct(p['d_plus'] + p['d_minus'] + p['d_neutral'], p['records'])
    c_na = safe_pct(c['na'], c['records'])
    p_na = safe_pct(p['na'], p['records'])
    c_i = safe_pct(c['t'] + c['dnc'] + c['dx'] + c['wn'] + c['am'], c['records'])
    p_i = safe_pct(p['t'] + p['dnc'] + p['dx'] + p['wn'] + p['am'], p['records'])
    c_ed = safe_pct(c['ed'], c['es']); p_ed = safe_pct(p['ed'], p['es'])
    c_eo = safe_pct(c['eo'], c['es']); p_eo = safe_pct(p['eo'], p['es'])
    c_ec = safe_pct(c['ec'], c['es']); p_ec = safe_pct(p['ec'], p['es'])
    c_ef = safe_pct(c['ef'], c['es']); p_ef = safe_pct(p['ef'], p['es'])
    c_sd = safe_pct(c['sd'], c['ss']); p_sd = safe_pct(p['sd'], p['ss'])
    c_sf = safe_pct(c['sf'], c['ss']); p_sf = safe_pct(p['sf'], p['ss'])

    # ── LAYER 1: KPI SUMMARY CARDS ──
    kc1, kc2, kc3, kc4 = st.columns(4)
    with kc1:
        st.markdown("##### 📞 Volume")
        st.metric("Records", fmt_num(c['records']), calc_delta(c['records'], p['records']))
        st.caption(f"Logins: {fmt_num(c['kpi2_logins'])} ({calc_delta(c['kpi2_logins'], p['kpi2_logins'])})")
        st.caption(f"Conv: {fmt_num(c['conversions'])} ({calc_delta(c['conversions'], p['conversions'])})")
    with kc2:
        st.markdown("##### ☎️ Call Efficiency")
        st.metric("Calls Delivered %", fmt_pct(c_d), calc_delta(c_d, p_d, True))
        st.caption(f"No Answer %: {fmt_pct(c_na)} ({calc_delta(c_na, p_na, True)})")
        st.caption(f"Invalid %: {fmt_pct(c_i)} ({calc_delta(c_i, p_i, True)})")
    with kc3:
        st.markdown("##### 📧 Email Health")
        st.metric("Email Delivered %", fmt_pct(c_ed), calc_delta(c_ed, p_ed, True))
        st.caption(f"Email Opened %: {fmt_pct(c_eo)} ({calc_delta(c_eo, p_eo, True)})")
        st.caption(f"Email Clicked %: {fmt_pct(c_ec)} ({calc_delta(c_ec, p_ec, True)})")
    with kc4:
        st.markdown("##### 📱 SMS Health")
        st.metric("SMS Delivered %", fmt_pct(c_sd), calc_delta(c_sd, p_sd, True))
        st.caption(f"SMS Failed %: {fmt_pct(c_sf)} ({calc_delta(c_sf, p_sf, True)})")

    st.markdown("---")

    # ── LAYER 2: BENCHMARK TABLE ──
    rows = []
    # Volume
    rows.append(("📞 **Volume**", "", "", ""))
    rows.append(("Records", fmt_num(p['records']), fmt_num(c['records']), calc_delta(c['records'], p['records'])))
    rows.append(("Logins", fmt_num(p['kpi2_logins']), fmt_num(c['kpi2_logins']), calc_delta(c['kpi2_logins'], p['kpi2_logins'])))
    rows.append(("Conversions", fmt_num(p['conversions']), fmt_num(c['conversions']), calc_delta(c['conversions'], p['conversions'])))

    # Dispositions
    rows.append(("☎️ **Dispositions**", "", "", ""))
    rows.append(("Calls Delivered %", fmt_pct(p_d), fmt_pct(c_d), calc_delta(c_d, p_d, True)))
    rows.append(("No Answer %", fmt_pct(p_na), fmt_pct(c_na), calc_delta(c_na, p_na, True)))
    rows.append(("Invalid %", fmt_pct(p_i), fmt_pct(c_i), calc_delta(c_i, p_i, True)))

    # Email (% of es)
    rows.append(("📧 **Email**", "", "", ""))
    for label, c_val, p_val in [("Email Delivered %", c_ed, p_ed), ("Email Opened %", c_eo, p_eo), ("Email Clicked %", c_ec, p_ec), ("Email Failed %", c_ef, p_ef)]:
        rows.append((label, fmt_pct(p_val), fmt_pct(c_val), calc_delta(c_val, p_val, True)))

    # SMS (% of SS = sd + sf + sp)
    rows.append(("📱 **SMS**", "", "", ""))
    for label, c_val, p_val in [("SMS Delivered %", c_sd, p_sd), ("SMS Failed %", c_sf, p_sf), ("SMS Pending %", safe_pct(c['sp'], c['ss']), safe_pct(p['sp'], p['ss']))]:
        rows.append((label, fmt_pct(p_val), fmt_pct(c_val), calc_delta(c_val, p_val, True)))

    bench_df = pd.DataFrame(rows, columns=["Metric", prior_label, curr_label, "Δ"])
    st.dataframe(bench_df, hide_index=True, width='stretch', height=(len(rows) + 1) * 35 + 3)

    # ── LAYER 3: EXPANDABLE DETAIL CHARTS ──
    import plotly.graph_objects as go

    def _make_dumbbell(title, labels, prior_vals, curr_vals, fmt_fn=fmt_pct, is_pct=True):
        """Build a horizontal dumbbell chart comparing baseline vs current."""
        fig = go.Figure()
        n = len(labels)

        for i, (lbl, pv, cv) in enumerate(zip(labels, prior_vals, curr_vals)):
            color = 'rgba(0, 200, 100, 0.7)' if (cv >= pv if lbl not in ['NA%', 'I%', 'EF%', 'SF%', 'SP%'] else cv <= pv) else 'rgba(255, 80, 80, 0.7)'
            # Connector line
            fig.add_trace(go.Scatter(
                x=[pv, cv], y=[lbl, lbl], mode='lines',
                line=dict(color=color, width=3),
                showlegend=False, hoverinfo='skip'
            ))
            # Baseline dot (teal)
            fig.add_trace(go.Scatter(
                x=[pv], y=[lbl], mode='markers+text',
                marker=dict(color='rgba(100, 160, 180, 0.9)', size=12, line=dict(width=1, color='white')),
                text=[fmt_fn(pv)], textposition='top center',
                name=prior_label if i == 0 else None,
                showlegend=(i == 0), legendgroup='baseline',
                hovertemplate=f'{lbl}: {fmt_fn(pv)}<extra>{prior_label}</extra>'
            ))
            # Current dot (cyan)
            fig.add_trace(go.Scatter(
                x=[cv], y=[lbl], mode='markers+text',
                marker=dict(color='rgba(0, 200, 220, 0.9)', size=12, line=dict(width=1, color='white')),
                text=[fmt_fn(cv)], textposition='bottom center',
                name=curr_label if i == 0 else None,
                showlegend=(i == 0), legendgroup='current',
                hovertemplate=f'{lbl}: {fmt_fn(cv)}<extra>{curr_label}</extra>'
            ))
            # Delta annotation
            delta_text = calc_delta(cv, pv, is_pct)
            max_val = max(pv, cv)
            fig.add_annotation(
                x=max_val, y=lbl,
                text=f"<b>{delta_text}</b>",
                showarrow=False, xanchor='left', xshift=15,
                font=dict(size=12, color=color)
            )

        fig.update_layout(
            title=title,
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            height=55 * n + 100,
            margin=dict(l=140, r=80, t=45, b=25),
            font=dict(size=11),
            yaxis=dict(autorange='reversed'),
            xaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)'),
            legend=dict(orientation='h', y=-0.2)
        )
        return fig

    with st.expander("📊 H2 2025 Baseline vs Current Charts"):
        # Row 1: Volume + Dispositions
        rc1, rc2 = st.columns(2)
        with rc1:
            st.plotly_chart(_make_dumbbell(
                "📞 Volume",
                ['Records', 'Logins', 'Conversions'],
                [p['records'], p['kpi2_logins'], p['conversions']],
                [c['records'], c['kpi2_logins'], c['conversions']],
                fmt_fn=fmt_num, is_pct=False
            ), width='stretch')
        with rc2:
            st.plotly_chart(_make_dumbbell(
                "☎️ Call Dispositions",
                ['Calls Delivered %', 'No Answer %', 'Invalid %'],
                [p_d, p_na, p_i],
                [c_d, c_na, c_i]
            ), width='stretch')

        # Row 2: Email + SMS
        rc3, rc4 = st.columns(2)
        with rc3:
            st.plotly_chart(_make_dumbbell(
                "📧 Email Performance",
                ['Email Delivered %', 'Email Opened %', 'Email Clicked %', 'Email Failed %'],
                [p_ed, p_eo, p_ec, p_ef],
                [c_ed, c_eo, c_ec, c_ef]
            ), width='stretch')
        with rc4:
            st.plotly_chart(_make_dumbbell(
                "📱 SMS Performance",
                ['SMS Delivered %', 'SMS Pending %', 'SMS Failed %'],
                [p_sd, safe_pct(p['sp'], p['ss']), p_sf],
                [c_sd, safe_pct(c['sp'], c['ss']), c_sf]
            ), width='stretch')

if view_mode == "📊 Dashboard":
    tabs = ["📊 Dashboard"]
    created_tabs = st.tabs(tabs)
    tab_map = dict(zip(tabs, created_tabs))
    with tab_map["📊 Dashboard"]:
        st.markdown("#### > 📡 OPERATIONS PULSE_")
        st.markdown("*Rolling KPI windows — at a glance.*")
        
        if "raw_pulse_df" in st.session_state and not st.session_state["raw_pulse_df"].empty:
            _pulse_ops = st.session_state["raw_pulse_df"]
            
            def _render_pulse_matrix(df, title, engagement_type):
                """Render a 3-row x 4-col sparkline performance matrix for a given engagement type."""
                if 'extracted_engagement' in df.columns:
                    edf = df[df['extracted_engagement'].str.upper() == engagement_type.upper()].copy()
                else:
                    edf = df.copy()
                
                if edf.empty or 'ops_date' not in edf.columns:
                    st.caption(f"No {engagement_type} data available.")
                    return
                
                max_date = edf['ops_date'].max()
                windows = [7, 14, 30, 90]
                window_labels = ["7 Days", "14 Days", "30 Days", "90 Days"]
                
                # Use daily right out of the precomputed matrix payload
                daily = edf.sort_values('ops_date')
                
                # Calculate daily rates
                daily['Conv%'] = ((daily['KPI1-Conv.'] / daily['Records']).replace([float('inf'), -float('inf')], 0).fillna(0) * 100).clip(upper=100)
                daily['Login%'] = ((daily['KPI2-Login'] / daily['Records']).replace([float('inf'), -float('inf')], 0).fillna(0) * 100).clip(upper=100) if 'KPI2-Login' in daily.columns else 0
                
                st.markdown(f"**> {title}_**")
                
                # Define metrics to render: Volume → Login % → Conv %
                metrics = [
                    ("Volume", "Records", "sum", "", "#AAAAAA"),
                    ("Login %", "Login%", "mean", "%", "#eab308"),
                    ("Conv %", "Conv%", "mean", "%", "#22c55e"),
                ]
                
                for metric_label, col_name, agg_fn, suffix, color in metrics:
                    cols = st.columns(4)
                    for i, (window, wlabel) in enumerate(zip(windows, window_labels)):
                        with cols[i]:
                            # Current period
                            current_mask = (daily['ops_date'] > (max_date - pd.Timedelta(days=window))) & (daily['ops_date'] <= max_date)
                            current_data = daily.loc[current_mask, col_name]
                            
                            # Prior period
                            prior_mask = (daily['ops_date'] > (max_date - pd.Timedelta(days=window*2))) & (daily['ops_date'] <= (max_date - pd.Timedelta(days=window)))
                            prior_data = daily.loc[prior_mask, col_name]
                            
                            if agg_fn == "mean":
                                current_val = current_data.mean() if len(current_data) > 0 else 0
                                prior_val = prior_data.mean() if len(prior_data) > 0 else 0
                            else:
                                current_val = current_data.sum() if len(current_data) > 0 else 0
                                prior_val = prior_data.sum() if len(prior_data) > 0 else 0
                            
                            delta_val = current_val - prior_val
                            
                            # Format display
                            if suffix == "%":
                                display_val = f"{current_val:.1f}%"
                                display_delta = f"{delta_val:+.1f}%"
                            else:
                                display_val = f"{current_val:,.0f}"
                                display_delta = f"{delta_val:+,.0f}"
                            
                            st.metric(
                                label=f"{metric_label} ({wlabel})",
                                value=display_val,
                                delta=display_delta
                            )
                            
                            # Sparkline
                            spark_data = daily.loc[current_mask].copy()
                            if len(spark_data) > 1:
                                fig_spark = px.line(spark_data, x='ops_date', y=col_name)
                                fig_spark.update_traces(line_color=color, line_width=2)
                                fig_spark.update_layout(
                                    height=60,
                                    margin=dict(l=0, r=0, t=0, b=0),
                                    paper_bgcolor="rgba(0,0,0,0)",
                                    plot_bgcolor="rgba(0,0,0,0)",
                                    showlegend=False,
                                    xaxis=dict(visible=False),
                                    yaxis=dict(visible=False)
                                )
                                st.plotly_chart(fig_spark, width='stretch', config={'displayModeBar': False}, key=f'spark_{title}_{metric_label}_{wlabel}')
            
            # Render side-by-side LI / NLI matrices
            col_li, col_nli = st.columns(2)
            with col_li:
                _render_pulse_matrix(_pulse_ops, "LI OPERATIONS PULSE", "LI")
            with col_nli:
                _render_pulse_matrix(_pulse_ops, "NLI OPERATIONS PULSE", "NLI")
            
            # ── HALF-YEAR OPERATIONAL BASELINE ──
            st.markdown("---")
            
        else:
            st.info("No operations data loaded. Navigate to 📞 Operations to upload data.")
    
elif view_mode == "📞 Operations":
    st.markdown("## 📞 Operations Workspace")
    tabs = ["📞 Operations Command", "📉 Historical Benchmarks", "🕵️ CRM Intelligence", "📈 Campaigns", "🗄️ Operations Ingestion"]
    created_tabs = st.tabs(tabs)
    tab_map = dict(zip(tabs, created_tabs))
    st.info("Operations Reports and Uploads will be consolidated here.")
    
elif view_mode == "🏦 Financial":
    st.markdown("## 🏦 Financial Workspace")
    tabs = ["🏦 Financial Deep-Dive", "📥 Financial Ingestion"]
    created_tabs = st.tabs(tabs)
    tab_map = dict(zip(tabs, created_tabs))
    st.info("Financial Reports and NGR/Deposits Ingestion will be consolidated here.")

if "📥 Financial Ingestion" in tab_map:
    with tab_map["📥 Financial Ingestion"]:
        st.markdown("### 📥 FINANCIAL DATA INGESTION")
        st.markdown("*Upload NGR, Deposits, and Player Activity reports here. Data saves directly to the secure database.*")
        
        # --- COMPLIANCE GRID ---
        if "registry" not in st.session_state or st.session_state["registry"] is None:
            from src.ingestion import IngestionRegistry
            st.session_state["registry"] = IngestionRegistry.load()
        registry = st.session_state.get("registry")
        if registry and registry._entries:
            st.markdown("#### 📅 Compliance & Import Grid")
            all_months = set()
            for b, m_dict in registry._entries.items(): all_months.update(m_dict.keys())
            sorted_months = sorted(list(all_months), reverse=True)
            grid_data = {"Month": sorted_months}
            for b in sorted(registry._entries.keys()):
                grid_data[f"{b.title()}"] = ["🟢 IMPORTED" if registry._entries[b].get(m, {}).get("status") == "COMPLETE" else "🔴 PENDING" for m in sorted_months]
            st.dataframe(pd.DataFrame(grid_data), width='stretch', hide_index=True)
        
        # --- UPLOADERS ---
        st.markdown("---")
        st.markdown("#### 📁 File Dropzones")
        col_gfin1, col_gfin2 = st.columns([3, 1])
        with col_gfin1:
            fin_files = st.file_uploader("Upload Financial Files (CSV/XLSX)", type=["csv", "xlsx"], key="global_fin_upload", accept_multiple_files=True)
        with col_gfin2:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🔄 Force Refresh Cache", key="global_refresh_fin"):
                fetch_financial_data.clear()
                fetch_config_tables.clear()
                st.rerun()

        if st.button("Process Financial Data", width='stretch') and fin_files:
            with st.spinner("Saving securely to PostgreSQL..."):
                from src.ingestion import load_all_data_from_uploads
                df, reg = load_all_data_from_uploads(fin_files)
                st.session_state["registry"] = reg
                
                # Invalidate RAM cache since new data was appended to DB
                fetch_financial_data.clear()
                fetch_config_tables.clear()
                if "raw_fin_df" in st.session_state: del st.session_state["raw_fin_df"]
                
                st.success("Successfully ingested to PostgreSQL!")
                st.rerun()

if "🗄️ Operations Ingestion" in tab_map:
    with tab_map["🗄️ Operations Ingestion"]:
        st.markdown("### 📡 OPERATIONS DATA INGESTION")
        st.markdown("*Upload CallsU or Telemarketing daily summaries here.*")

        st.markdown("### 📡 Automated CallsU API Sync")
        st.write("Fetch daily operations data directly from the CallsU servers in the background.")

        yesterday_date = datetime.now().date() - timedelta(days=1)
        col_date1, col_date2, col_btn = st.columns([2, 2, 2])
        with col_date1:
            sync_start = st.date_input("Start Date", value=yesterday_date, max_value=yesterday_date)
        with col_date2:
            sync_end = st.date_input("End Date", value=yesterday_date, max_value=yesterday_date)

        with col_btn:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🚀 Trigger Background Sync", width='stretch'):
                if "sync_thread" not in st.session_state or not st.session_state.sync_thread.is_alive():
                    # Clear old log
                    open("data/api_sync.log", "w").close()

                    # Launch detached thread
                    t = threading.Thread(target=run_historical_pull, args=(sync_start.strftime('%Y-%m-%d'), sync_end.strftime('%Y-%m-%d')))
                    t.add_script_run_ctx = True
                    t.start()
                    st.session_state.sync_thread = t
                    st.success("Background worker launched!")
                else:
                    st.warning("A sync is already running in the background!")

        # Live Telemetry Console
        st.markdown("**Live Terminal Output**")
        log_container = st.empty()

        # Read log file
        log_content = "No active logs."
        if os.path.exists("data/api_sync.log"):
            with open("data/api_sync.log", "r") as f:
                log_content = f.read()

        log_container.code(log_content[-2000:], language="bash") # Show last 2000 chars

        if "sync_thread" in st.session_state and st.session_state.sync_thread.is_alive():
            st.info("🔄 Worker is currently active. Click 'Refresh Log' to see latest terminal output.")

        if st.button("🔄 Refresh Log Terminal"):
            st.rerun()
        st.markdown("---")

        col_ops1, col_ops2 = st.columns([3, 1])
        with col_ops1:
            ops_files = st.file_uploader("Upload Ops Files (CSV/XLSX)", type=["csv", "xlsx"], key="ops_upload", accept_multiple_files=True)
        with col_ops2:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🔄 Force Refresh Cache", key="refresh_ops"):
                fetch_ops_data.clear()
                fetch_ops_snapshots_data.clear()
                for k in ["raw_ops_df", "raw_ops_snapshots_df", "raw_pulse_df", "ops_df", "ops_snapshots_df"]:
                    if k in st.session_state: del st.session_state[k]
                st.rerun()

        if st.button("Process Operations Data", width='stretch') and ops_files:
            with st.spinner("Saving securely to PostgreSQL..."):
                from src.ingestion import load_operations_data_from_uploads
                load_operations_data_from_uploads(ops_files)
                
                # Invalidate the RAM cache since new data was appended to the database
                fetch_ops_data.clear()
                fetch_ops_snapshots_data.clear()
                for k in ["raw_ops_df", "raw_ops_snapshots_df", "raw_pulse_df", "ops_df", "ops_snapshots_df"]:
                    if k in st.session_state: del st.session_state[k]
                
                st.success("Successfully ingested to PostgreSQL!")
                st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
#  BI Dashboard — reads from session state
# ══════════════════════════════════════════════════════════════════════════════
if not _master_df.empty:
    df = _master_df
    # Auto-compute analytics using cached wrappers (instantaneous!)
    financial_summary = _cached_monthly_summaries(df, start=start_month, end=end_month)
    cohort_matrices = _cached_cohort_matrix()
    segmentation = _cached_segmentation(df)
    both_business = _cached_both_business(financial_summary)
    program_summary = _cached_program_summary(df)

    # ══════════════════════════════════════════════════════════════════════
    #  BI Dashboard (Phase 9)
    # ══════════════════════════════════════════════════════════════════════
    st.markdown("---")

    # ── Helper: render a financial brand tab ──────────────────────────────
    def _render_financial_tab(brand_key: str, emoji: str) -> None:
        """Render KPIs, GGR chart, full table, and cohort matrix for a brand."""
        bdf = (
            financial_summary[financial_summary["brand"] == brand_key]
            .sort_values("month")
            .reset_index(drop=True)
        )
        if bdf.empty:
            st.warning(f"No data for {brand_key}")
            return

        latest = bdf.iloc[-1]
        prev = bdf.iloc[-2] if len(bdf) > 1 else None

        def _delta(col: str) -> float | None:
            if prev is not None and col in bdf.columns:
                return float(latest[col] - prev[col])
            return None

        # ── Top-line KPI cards ────────────────────────────────────────────
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        with k1:
            st.metric("GGR", f"${latest['ggr']:,.0f}", delta=f"${_delta('ggr'):,.0f}" if _delta('ggr') is not None else None)
        with k2:
            st.metric("Total Handle", f"${latest['total_handle']:,.0f}", delta=f"${_delta('total_handle'):,.0f}" if _delta('total_handle') is not None else None)
        with k3:
            st.metric("Hold %", f"{latest['hold_pct']:.2f}%")
        with k4:
            st.metric("Total Players", f"{int(latest['total_players']):,}", delta=f"{int(_delta('total_players')):,}" if _delta('total_players') is not None else None)
        with k5:
            st.metric("Retention %", f"{latest['retention_pct']:.1f}%")
        with k6:
            st.metric("Profitable %", f"{latest['profitable_pct']:.1f}%")

        # ── GGR Trend Chart ───────────────────────────────────────────────
        st.markdown("#### 📈 GGR Month-over-Month")
        chart_data = bdf[["month", "ggr"]].set_index("month")
        st.bar_chart(chart_data, width='stretch')

        # ── Player Demographics Chart ─────────────────────────────────────
        st.markdown(f"#### > {brand_key.upper()} PLAYER DEMOGRAPHICS (MONTH OVER MONTH)_")
        demo_df = bdf[["month", "total_players", "profitable_players", "negative_yield_players"]].copy()
        demo_df = demo_df.rename(columns={
            "total_players": "Total Players",
            "profitable_players": "Profitable (Winners)",
            "negative_yield_players": "Neg. Yield (Losers)",
        })
        st.line_chart(
            demo_df.set_index("month"),
            width='stretch',
            color=["#AAAAAA", "#00FF41", "#FF4444"],
        )

        # ── Comparative Intelligence (brand-level) ────────────────────────
        # Map total_handle→turnover for time-series compatibility
        bdf_ts = bdf.rename(columns={"total_handle": "turnover"})
        brand_ts = _cached_time_series(bdf_ts)
        brand_ts_m = brand_ts["monthly"]
        brand_ts_q = brand_ts["quarterly"]

        if not brand_ts_m.empty:
            # ── Smart Narrative (brand-level) ───────────────────────
            b_whale = float(latest.get("top_10_pct_ggr_share", 0))
            b_margin = float(latest.get("hold_pct", 0))  # hold_pct = margin for brands
            b_narrative = generate_smart_narrative(brand_ts_m.iloc[-1], b_margin, b_whale)
            if b_margin < 2.5 or b_whale >= 70:
                st.warning(b_narrative)
            else:
                st.info(b_narrative)

            st.markdown(f"#### > {brand_key.upper()} COMPARATIVE INTELLIGENCE_")
            b_latest_m = brand_ts_m.iloc[-1]
            b_latest_q = brand_ts_q.iloc[-1] if not brand_ts_q.empty else None

            def _b_arrow(val):
                if pd.isna(val): return "—"
                return f"↑ {val:+,.0f}" if val >= 0 else f"↓ {val:,.0f}"

            def _b_arrow_pct(val):
                if pd.isna(val): return "—"
                return f"↑ {val:+.1f}%" if val >= 0 else f"↓ {val:.1f}%"

            # [ FINANCIALS ]
            st.markdown("##### 💰 Financials")
            b_fin_cols = ["turnover", "ggr", "revenue_share_deduction"]
            b_fin_labels = ["Turnover", "GGR", "Revenue (15%)"]
            b_fin_rows = []
            for col, label in zip(b_fin_cols, b_fin_labels):
                row = {"Metric": label}
                row["MoM Δ"] = _b_arrow(b_latest_m.get(f"{col}_mom_delta"))
                row["MoM %"] = _b_arrow_pct(b_latest_m.get(f"{col}_mom_pct"))
                row["YoY Δ"] = _b_arrow(b_latest_m.get(f"{col}_yoy_delta"))
                row["YoY %"] = _b_arrow_pct(b_latest_m.get(f"{col}_yoy_pct"))
                row["YTD"] = f"${b_latest_m.get(f'{col}_ytd', 0):,.0f}"
                if b_latest_q is not None:
                    row["QoQ Δ"] = _b_arrow(b_latest_q.get(f"{col}_qoq_delta"))
                    row["QoQ %"] = _b_arrow_pct(b_latest_q.get(f"{col}_qoq_pct"))
                b_fin_rows.append(row)
            st.dataframe(pd.DataFrame(b_fin_rows), width='stretch', hide_index=True)

            # EOY Projected metrics — Dual Engine (brand-level)
            b_eoy_rows = []
            for proj_col, proj_label in [("ggr", "GGR"), ("turnover", "Turnover"), ("revenue_share_deduction", "Revenue 15%")]:
                for eng_label, prefix in [("Seasonal", "eoy_seasonal"), ("Momentum", "eoy_momentum")]:
                    eoy_key = f"{prefix}_{proj_col}"
                    eoy_val = b_latest_m.get(eoy_key, 0) or 0
                    b_eoy_rows.append({"Metric": f"EOY {proj_label} ({eng_label})", "MoM Δ": "—", "MoM %": "—",
                                       "YoY Δ": "—", "YoY %": "—",
                                       "YTD": f"${eoy_val:,.0f}"})
            if b_eoy_rows:
                st.dataframe(pd.DataFrame(b_eoy_rows), width='stretch', hide_index=True)
            st.caption("🔮 **EOY PROJECTIONS:** Seasonal uses prior-year proportional scaling. Momentum uses 3-month rolling average × remaining months.")

            # [ PLAYER DEMOGRAPHICS ]
            st.markdown("##### 👥 Player Demographics")
            b_plr_cols = ["total_players", "profitable_players", "negative_yield_players", "conversions", "new_players", "reactivated_players", "returning_players"]
            b_plr_labels = ["Total Active", "Profitable (Winners)", "Neg. Yield (Losers)", "Conversions", "New Players", "Reactivated", "Returning (Retained)"]
            b_plr_rows = []
            for col, label in zip(b_plr_cols, b_plr_labels):
                row = {"Metric": label}
                row["MoM Δ"] = _b_arrow(b_latest_m.get(f"{col}_mom_delta"))
                row["MoM %"] = _b_arrow_pct(b_latest_m.get(f"{col}_mom_pct"))
                row["YoY Δ"] = _b_arrow(b_latest_m.get(f"{col}_yoy_delta"))
                row["YoY %"] = _b_arrow_pct(b_latest_m.get(f"{col}_yoy_pct"))
                row["YTD"] = f"{int(b_latest_m.get(f'{col}_ytd', 0)):,}"
                if b_latest_q is not None:
                    row["QoQ Δ"] = _b_arrow(b_latest_q.get(f"{col}_qoq_delta"))
                    row["QoQ %"] = _b_arrow_pct(b_latest_q.get(f"{col}_qoq_pct"))
                b_plr_rows.append(row)
            st.dataframe(pd.DataFrame(b_plr_rows), width='stretch', hide_index=True)

        # ── Risk & Value Metrics (brand-level) ─────────────────────
        st.markdown(f"#### > {brand_key.upper()} RISK & VALUE METRICS_")
        brv1, brv2 = st.columns(2)
        with brv1:
            st.metric("Turnover Per Player",
                      f"${float(latest.get('turnover_per_player', 0)):,.2f}")
        with brv2:
            st.metric("Whale Dependency (Top 10% GGR)",
                      f"{float(latest.get('top_10_pct_ggr_share', 0)):.2f}%")

        # Revenue Composition (brand)
        if "new_player_ggr" in bdf.columns and "returning_player_ggr" in bdf.columns:
            st.markdown("##### 📊 Revenue Composition: New vs Returning Player GGR")
            b_rev = bdf[["month", "new_player_ggr", "returning_player_ggr"]].copy()
            b_rev = b_rev.rename(columns={"month": "Month", "new_player_ggr": "New_Player_GGR", "returning_player_ggr": "Returning_Player_GGR"})
            b_rev["New (Profit)"] = b_rev["New_Player_GGR"].clip(lower=0)
            b_rev["New (Loss)"] = b_rev["New_Player_GGR"].clip(upper=0)
            b_rev["Returning (Profit)"] = b_rev["Returning_Player_GGR"].clip(lower=0)
            b_rev["Returning (Loss)"] = b_rev["Returning_Player_GGR"].clip(upper=0)
            st.bar_chart(b_rev, x="Month",
                         y=["New (Profit)", "New (Loss)", "Returning (Profit)", "Returning (Loss)"],
                         color=["#00FF41", "#FF0000", "#CCCCCC", "#804040"])

        # RFM Tiering (brand-filtered)
        b_latest_month = bdf["month"].max()
        brand_raw = df[df["brand"] == brand_key]
        b_rfm = _cached_rfm_summary(brand_raw, b_latest_month)
        if not b_rfm.empty:
            st.markdown(f"##### 🏆 VIP Tiering — RFM Segmentation ({b_latest_month})")
            bt1, bt2, bt3 = st.columns(3)
            for col_w, tier_name in [(bt1, "True VIP"), (bt2, "Churn Risk"), (bt3, "Casual")]:
                tier_row = b_rfm[b_rfm["Tier"] == tier_name]
                players = int(tier_row["Players"].iloc[0]) if not tier_row.empty else 0
                ggr_v = float(tier_row["GGR"].iloc[0]) if not tier_row.empty else 0.0
                with col_w:
                    st.metric(tier_name, f"{players:,} players")
                    st.caption(f"GGR: ${ggr_v:,.2f}")
            st.dataframe(b_rfm, width='stretch', hide_index=True,
                         column_config={
                             "Tier": st.column_config.TextColumn("Tier"),
                             "Players": st.column_config.NumberColumn("Players", format="%d"),
                             "GGR": st.column_config.NumberColumn("GGR", format="$%.2f"),
                         })

        # ── Full Data Table ───────────────────────────────────────────────
        with st.expander(f"📋 {brand_key} — Full Financial Data ({len(bdf)} months)", expanded=False):
            st.dataframe(
                bdf,
                width='stretch',
                hide_index=True,
                column_config={
                    "month": st.column_config.TextColumn("Month"),
                    "brand": st.column_config.TextColumn("Brand"),
                    "negative_yield_players": st.column_config.NumberColumn("Losers", format="%d"),
                    "profitable_players": st.column_config.NumberColumn("Winners", format="%d"),
                    "flat": st.column_config.NumberColumn("Flat", format="%d"),
                    "total_players": st.column_config.NumberColumn("Total Players", format="%d"),
                    "profitable_pct": st.column_config.NumberColumn("Winners %", format="%.2f%%"),
                    "ggr": st.column_config.NumberColumn("GGR", format="$%.2f"),
                    "total_handle": st.column_config.NumberColumn("Turnover", format="$%.2f"),
                    "hold_pct": st.column_config.NumberColumn("Hold %", format="%.2f%%"),
                    "ggr_per_player": st.column_config.NumberColumn("GGR/Player", format="$%.2f"),
                    "top_10_pct_ggr_share": st.column_config.NumberColumn("Top 10% GGR", format="%.1f%%"),
                    "new_players": st.column_config.NumberColumn("New Players", format="%d"),
                    "returning_players": st.column_config.NumberColumn("Returning", format="%d"),
                    "retention_pct": st.column_config.NumberColumn("Retention %", format="%.2f%%"),
                },
            )

        # ── Cohort Matrix ─────────────────────────────────────────────────
        if cohort_matrices and brand_key in cohort_matrices:
            matrix = cohort_matrices[brand_key]
            if not matrix.empty:
                with st.expander(f"🔄 {brand_key} — Cohort Retention Matrix", expanded=False):
                    st.dataframe(
                        matrix.style.format("{:.1f}%", na_rep="—"),
                        width='stretch',
                    )

        # ── Cohort Retention Heatmap (Phase 18) ──────────────────────────
        st.markdown("---")
        st.markdown("#### > COHORT RETENTION HEATMAP_")
        brand_raw = df[df["brand"] == brand_key]
        heatmap_fig = _cached_retention_heatmap()
        if heatmap_fig is not None:
            st.plotly_chart(heatmap_fig, width='stretch', config={"scrollZoom": False})
        else:
            st.info("Not enough data to generate a retention heatmap.")

        # ── Cumulative LTV Curves ────────────────────────────────────
        st.markdown("---")
        st.markdown("#### > CUMULATIVE LTV TRAJECTORY_")
        st.markdown("*Insight: Tracks the cumulative revenue generation of player cohorts over time to determine break-even points and long-term value.*")
        ltv_fig = _cached_ltv_curves()
        if ltv_fig is not None:
            st.plotly_chart(ltv_fig, width='stretch', config={"scrollZoom": False})
        else:
            st.info("Not enough data to generate LTV curves.")

        # ── Segmentation by Program ─────────────────────────────────
        if program_summary is not None and not program_summary.empty:
            brand_progs = program_summary[program_summary["brand"] == brand_key]
            if not brand_progs.empty:
                st.markdown("---")
                st.markdown("#### > SEGMENTATION BY PROGRAM_")
                st.markdown("*Insight: Evaluates the financial efficiency and house edge (Margin) across different marketing programs (ACQ, RET, WB).*")
                st.dataframe(
                    brand_progs,
                    width='stretch',
                    hide_index=True,
                    column_config={
                        "brand": st.column_config.TextColumn("Brand"),
                        "month": st.column_config.TextColumn("Month"),
                        "Program": st.column_config.TextColumn("Program"),
                        "ggr": st.column_config.NumberColumn("GGR", format="$%d"),
                        "Turnover": st.column_config.NumberColumn("Turnover", format="$%d"),
                        "Margin": st.column_config.NumberColumn("Margin", format="%.2f%%"),
                        "total_players": st.column_config.NumberColumn("Players", format="%d"),
                    },
                )



    # ═════════════════════════════════════════════════════════════════════
    #  TAB: Executive Summary (Phase 16)
    # ═════════════════════════════════════════════════════════════════════
    if "🏦 Financial Deep-Dive" in tab_map:
        with tab_map["🏦 Financial Deep-Dive"]:
            # ── System Diagnostic (Combined) ──────────────────────────────────
            if not both_business.empty:
                active_bb = both_business[both_business["total_players"] > 0]
                exec_bb = active_bb.iloc[-1] if not active_bb.empty else both_business.iloc[-1]
                exec_ts = _cached_time_series(both_business)
                exec_ts_m = exec_ts["monthly"]
                
                # Use active trailing month for text insight and MoM calculations
                exec_ts_m_active = exec_ts_m[exec_ts_m["total_players"] > 0] if "total_players" in exec_ts_m.columns else exec_ts_m
                exec_latest = exec_ts_m_active.iloc[-1] if not exec_ts_m_active.empty else (exec_ts_m.iloc[-1] if not exec_ts_m.empty else pd.Series())

                if not exec_latest.empty:
                    combined_fin = financial_summary[
                        financial_summary["brand"] == "Combined"
                    ].sort_values("month")
                    com_fin_active = combined_fin[combined_fin["total_players"] > 0]
                    c_fin_latest = com_fin_active.iloc[-1] if not com_fin_active.empty else (combined_fin.iloc[-1] if not combined_fin.empty else pd.Series())
                    
                    e_whale = float(c_fin_latest.get("top_10_pct_ggr_share", 0))
                    e_margin = float(exec_bb.get("margin", 0))
                    e_narrative = generate_smart_narrative(exec_latest, e_margin, e_whale)
                    if e_margin < 2.5 or e_whale >= 70:
                        st.warning(e_narrative)
                    else:
                        st.info(e_narrative)

                # --- DYNAMIC BRAND DETECTION ---
                active_brands = sorted([b for b in financial_summary["brand"].unique() if b != "Combined"])
                combined_label = "All Business" if len(active_brands) > 2 else "Both Business"

                # ── Cross-Brand Executive Matrix ─────────────────────────────
                st.markdown("#### > CROSS-BRAND EXECUTIVE MATRIX_")
                st.markdown("*Insight: Tracks core revenue generation, operating margin safety, and top-line agency commissions across all entities.*")

                if not df.empty:
                    master_excel = _get_master_excel_bytes(financial_summary, cohort_matrices, segmentation, both_business)
                    st.download_button("📥 Download Master Report", data=master_excel, file_name=f"Master_Report_{selected_client}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")

                latest_month = both_business["month"].max()

                _mom_map = {
                    "Turnover": "total_handle", "GGR": "ggr", "Margin %": "hold_pct",
                    "Revenue (15%)": "revenue_share_deduction", "Conversions": "conversions",
                    "Turnover / Player": "turnover_per_player", "Whale Risk %": None,
                }

                def _get_true_latest(brand_df):
                    """Filters out padded structural zeros to find the real latest active month."""
                    active = brand_df[brand_df["total_players"] > 0]
                    return active["month"].max() if not active.empty else None

                def _brand_snapshot(brand_name: str) -> dict:
                    brand_all = financial_summary[financial_summary["brand"] == brand_name]
                    if brand_all.empty: return {}
                    b_latest_month = _get_true_latest(brand_all)
                    if not b_latest_month: return {}
                    bdata = brand_all[brand_all["month"] == b_latest_month]
                    if bdata.empty: return {}
                    row = bdata.iloc[0]
                    return {
                        "Turnover": float(row.get("total_handle", 0)), "GGR": float(row.get("ggr", 0)),
                        "Margin %": float(row.get("hold_pct", 0)), "Revenue (15%)": float(row.get("revenue_share_deduction", 0)),
                        "Conversions": int(row.get("conversions", 0)), "Turnover / Player": float(row.get("turnover_per_player", 0)),
                        "Whale Risk %": float(row.get("top_10_pct_ggr_share", 0)),
                    }

                def _brand_mom(brand_name: str) -> list:
                    bdata = financial_summary[financial_summary["brand"] == brand_name].sort_values("month")
                    if bdata.empty: return ["-"] * len(metrics_list)
                    brand_ts_m = _cached_time_series(bdata).get("monthly", pd.DataFrame())
                    active_ts = brand_ts_m[brand_ts_m["total_players"] > 0] if "total_players" in brand_ts_m.columns else brand_ts_m
                    if active_ts.empty: return ["-"] * len(metrics_list)
                    latest = active_ts.iloc[-1]
                    return [f"{latest.get(f'{_mom_map.get(m)}_mom_pct'):+.1f}%" if pd.notna(latest.get(f"{_mom_map.get(m)}_mom_pct")) else "-" for m in metrics_list]

                def _bb_mom() -> list:
                    if exec_ts_m.empty: return ["-"] * len(metrics_list)
                    bb_ts_map = {"Turnover": "turnover", "GGR": "ggr", "Margin %": "margin", "Revenue (15%)": "revenue_share_deduction", "Conversions": "conversions", "Turnover / Player": "turnover_per_player"}
                    return [f"{exec_latest.get(f'{bb_ts_map.get(m)}_mom_pct'):+.1f}%" if pd.notna(exec_latest.get(f"{bb_ts_map.get(m)}_mom_pct")) else "-" for m in metrics_list]

                def _brand_yoy(brand_name: str) -> list:
                    bdata = financial_summary[financial_summary["brand"] == brand_name].sort_values("month")
                    if bdata.empty: return ["-"] * len(metrics_list)
                    brand_ts_m = _cached_time_series(bdata).get("monthly", pd.DataFrame())
                    active_ts = brand_ts_m[brand_ts_m["total_players"] > 0] if "total_players" in brand_ts_m.columns else brand_ts_m
                    if active_ts.empty: return ["-"] * len(metrics_list)
                    latest = active_ts.iloc[-1]
                    return [f"{latest.get(f'{_mom_map.get(m)}_yoy_pct'):+.1f}%" if pd.notna(latest.get(f"{_mom_map.get(m)}_yoy_pct")) else "-" for m in metrics_list]

                def _bb_yoy() -> list:
                    if exec_ts_m.empty: return ["-"] * len(metrics_list)
                    bb_ts_map = {"Turnover": "turnover", "GGR": "ggr", "Margin %": "margin", "Revenue (15%)": "revenue_share_deduction", "Conversions": "conversions", "Turnover / Player": "turnover_per_player"}
                    return [f"{exec_latest.get(f'{bb_ts_map.get(m)}_yoy_pct'):+.1f}%" if pd.notna(exec_latest.get(f"{bb_ts_map.get(m)}_yoy_pct")) else "-" for m in metrics_list]

                bb_snap = {
                    "Turnover": float(exec_bb.get("turnover", 0)), "GGR": float(exec_bb.get("ggr", 0)),
                    "Margin %": float(exec_bb.get("margin", 0)), "Revenue (15%)": float(exec_bb.get("revenue_share_deduction", 0)),
                    "Conversions": int(exec_bb.get("conversions", 0)), "Turnover / Player": float(exec_bb.get("turnover_per_player", 0)),
                    "Whale Risk %": e_whale,
                }

                metrics_list = ["Turnover", "GGR", "Margin %", "Revenue (15%)", "Conversions", "Turnover / Player", "Whale Risk %"]
            
                # Dynamically build the dictionary
                matrix_data = {
                    "Metric": metrics_list,
                    combined_label: [bb_snap.get(m, 0) for m in metrics_list],
                    f"{combined_label} MoM": _bb_mom(),
                    f"{combined_label} YoY": _bb_yoy(),
                }
            
                for brand in active_brands:
                    snap = _brand_snapshot(brand)
                    matrix_data[brand] = [snap.get(m, 0) for m in metrics_list]
                    matrix_data[f"{brand} MoM"] = _brand_mom(brand)
                    matrix_data[f"{brand} YoY"] = _brand_yoy(brand)

                # Dynamic column config mapping
                cfg = {"Metric": st.column_config.TextColumn("Metric"), combined_label: st.column_config.NumberColumn(combined_label, format="%.2f")}
                for brand in active_brands:
                    cfg[brand] = st.column_config.NumberColumn(brand, format="%.2f")

                st.dataframe(pd.DataFrame(matrix_data), width='stretch', hide_index=True, column_config=cfg)

                # ── Brand vs Brand Trajectory ─────────────────────────────────
                st.markdown("#### > BRAND vs BRAND TRAJECTORY_")
                fig = go.Figure()
            
                # Expanded color palette for up to 15 brands
                colors = ["#FF4444", "#00FF41", "#1E90FF", "#FFD700", "#FF1493", "#9400D3", 
                          "#00FFFF", "#FF8C00", "#7CFC00", "#FF00FF", "#00CED1", "#DC143C",
                          "#B8860B", "#ADFF2F", "#8B008B"]
            
                # Fix the unpacking crash by using enumerate()
                for i, brand in enumerate(active_brands):
                    b_ts = financial_summary[financial_summary["brand"] == brand][["month", "ggr"]].sort_values("month")
                    fig.add_trace(go.Bar(
                        x=b_ts["month"], 
                        y=b_ts["ggr"], 
                        name=brand, 
                        marker_color=colors[i % len(colors)]
                    ))
                
                # Dynamic Barmode: Group for 1-3 brands, Stack for 4+ brands
                dynamic_barmode = "stack" if len(active_brands) > 3 else "group"
                
                fig.update_layout(
                    barmode=dynamic_barmode, 
                    paper_bgcolor="rgba(0,0,0,0)", 
                    plot_bgcolor="rgba(0,0,0,0)", 
                    font_color="#00FF41",
                    legend=dict(font=dict(color="#00FF41")), 
                    xaxis=dict(gridcolor="#1a1a1a"), 
                    yaxis=dict(gridcolor="#1a1a1a", tickprefix="$", tickformat=",.0f"), 
                    margin=dict(l=0, r=0, t=30, b=0), 
                    height=400
                )
                st.plotly_chart(fig, width='stretch', config={"scrollZoom": False})

                # ── Cross-Brand Cannibalization ───────────────────────────────
                st.markdown("---")
                st.subheader("⚔️ Cross-Brand Cannibalization")
                st.markdown("*Insight: Identifies players active on both primary brands (Rojabet and Latribet) and their shared revenue footprint.*")
                
                from src.analytics import generate_overlap_stats
                overlap_m = generate_overlap_stats(raw_fin)
                
                o1, o2 = st.columns(2)
                with o1:
                    st.metric(
                        "Overlapping Players", 
                        f"{int(overlap_m['overlap_count']):,}", 
                        help="Unique players who have placed bets on BOTH Rojabet and Latribet."
                    )
                with o2:
                    st.metric(
                        "Shared GGR Footprint", 
                        f"${overlap_m['overlap_ggr']:,.2f}", 
                        help="The combined Lifetime GGR generated by these specific overlapping players."
                    )

                # ── Cross-Brand Demographics ─────────────────────────────────
                st.markdown("---")
                st.markdown("#### > CROSS-BRAND DEMOGRAPHICS_")
                demo_metrics = [("Total Active", "total_players"), ("Conversions", "conversions"), ("New Players", "new_players"), 
                                ("Reactivated", "reactivated_players"), ("Retained", "returning_players"), ("Profitable", "profitable_players"), ("Neg. Yield", "negative_yield_players")]

                demo_data = {"Metric": [label for label, _ in demo_metrics], combined_label: [int(exec_bb.get(col, 0)) for _, col in demo_metrics]}
                for brand in active_brands:
                    brand_all = financial_summary[financial_summary["brand"] == brand]
                    b_latest = _get_true_latest(brand_all)
                    bdata = brand_all[brand_all["month"] == b_latest] if b_latest else pd.DataFrame()
                    # --- SAFE DEMOGRAPHICS UNPACKING ---
                    demo_data[brand] = []
                    for _, col in demo_metrics:
                        if not bdata.empty and col in bdata.columns:
                            demo_data[brand].append(int(bdata.iloc[0].get(col, 0)))
                        else:
                            demo_data[brand].append(0)
            
                cfg_demo = {"Metric": st.column_config.TextColumn("Metric"), combined_label: st.column_config.NumberColumn(combined_label, format="%d")}
                for brand in active_brands: cfg_demo[brand] = st.column_config.NumberColumn(brand, format="%d")
                st.dataframe(pd.DataFrame(demo_data), width='stretch', hide_index=True, column_config=cfg_demo)

                # ── Cross-Brand Cash Flow & Promo ─────────────────────────────
                if "LeoVegas Group" in df["client"].unique():
                    st.markdown("---")
                    st.markdown("#### > CASH FLOW & PROMO EFFICIENCY_")
                    st.markdown("*Insight: Tracks actual liquidity (Net Deposits) vs. the Bonus Cost required to acquire the revenue.*")

                    cf_metrics = [("Net Deposits", "net_deposits"), ("Total Deposits", "deposits"), ("Withdrawals", "withdrawals"), ("Bonus Cost", "bonus_total")]

                    cf_data = {"Metric": [label for label, _ in cf_metrics], combined_label: [float(exec_bb.get(col, 0)) for _, col in cf_metrics]}
                    for brand in active_brands:
                        brand_all = financial_summary[financial_summary["brand"] == brand]
                        b_latest = _get_true_latest(brand_all)
                        bdata = brand_all[brand_all["month"] == b_latest] if b_latest else pd.DataFrame()
                        # --- SAFE CASH FLOW UNPACKING ---
                        cf_data[brand] = []
                        for _, col in cf_metrics:
                            if not bdata.empty and col in bdata.columns:
                                cf_data[brand].append(float(bdata.iloc[0].get(col, 0.0)))
                            else:
                                cf_data[brand].append(0.0)

                    cfg_cf = {"Metric": st.column_config.TextColumn("Metric"), combined_label: st.column_config.NumberColumn(combined_label, format="$%.2f")}
                    for brand in active_brands: cfg_cf[brand] = st.column_config.NumberColumn(brand, format="$%.2f")
                    st.dataframe(pd.DataFrame(cf_data), width='stretch', hide_index=True, column_config=cfg_cf)
                    st.caption("⚠️ **Note:** Cash Flow and Promo data is currently only provided by LeoVegas Group. Offside Gaming brands will reflect $0.00.")

                # ── Geographic Intelligence ─────────────────────────────
                st.markdown("---")
                st.markdown("#### > GEOGRAPHIC MARKET MATRIX_")
                st.markdown("*Insight: Evaluates market penetration, player volume, and net profitability by country.*")
            
                from src.analytics import generate_geographic_summary
                geo_df = generate_geographic_summary(df)
            
                if not geo_df.empty and len(geo_df[geo_df["country"] != "Global"]) > 0:
                    # Filter out the 'Global' fallback from Offside Gaming for the visual
                    visual_geo = geo_df[geo_df["country"] != "Global"].copy()
                
                    # 1. The Treemap
                    fig_tree = px.treemap(
                        visual_geo, 
                        path=["country"], 
                        values="ngr",
                        color="margin",
                        color_continuous_scale="Viridis",
                        title="True NGR by Market (Color = House Margin %)"
                    )
                    fig_tree.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)", 
                        plot_bgcolor="rgba(0,0,0,0)",
                        font_color="#00FF41",
                        margin=dict(t=30, l=0, r=0, b=0)
                    )
                    st.plotly_chart(fig_tree, width='stretch')
                
                # 2. The Market Leaderboard
                st.dataframe(
                    geo_df,
                    width='stretch',
                    hide_index=True,
                    column_config={
                        "country": st.column_config.TextColumn("Market"),
                        "total_players": st.column_config.NumberColumn("Active Players", format="%d"),
                        "turnover": st.column_config.NumberColumn("Turnover", format="$%.2f"),
                        "ggr": st.column_config.NumberColumn("GGR", format="$%.2f"),
                        "ngr": st.column_config.NumberColumn("NGR", format="$%.2f"),
                        "deposits": st.column_config.NumberColumn("Deposits", format="$%.2f"),
                        "margin": st.column_config.NumberColumn("Margin", format="%.2f%%"),
                    }
                )

            else:
                st.warning("No financial data available for Executive Summary.")

    if "🏦 Financial Deep-Dive" in tab_map:
        with tab_map["🏦 Financial Deep-Dive"]:
            _raw_df = _master_df.copy()
        
            # Recalculate summaries for the specific slice
            filtered_monthly = generate_monthly_summaries(_raw_df)
            filtered_both = generate_both_business_summary(filtered_monthly)
        
            if not filtered_both.empty:
                bb_latest = filtered_both.iloc[-1]
                bb_prev = filtered_both.iloc[-2] if len(filtered_both) > 1 else None

                def _bb_delta(col: str):
                    if bb_prev is not None and col in filtered_both.columns:
                        return float(bb_latest[col] - bb_prev[col])
                    return None

                # --- 1. KPI CARDS ---
                k1, k2, k3, k4, k5 = st.columns(5)
                with k1:
                    st.metric("Turnover", f"${bb_latest['turnover']:,.2f}",
                              delta=f"${_bb_delta('turnover'):,.2f}" if _bb_delta('turnover') is not None else None)
                with k2:
                    st.metric("GGR", f"${bb_latest['ggr']:,.2f}",
                              delta=f"${_bb_delta('ggr'):,.2f}" if _bb_delta('ggr') is not None else None)
                with k3:
                    st.metric("Margin", f"{bb_latest.get('margin', 0):.2f}%")
                with k4:
                    st.metric("Total Players", f"{int(bb_latest['total_players']):,}",
                              delta=f"{int(_bb_delta('total_players')):,}" if _bb_delta('total_players') is not None else None)
                with k5:
                    st.metric("Turnover Per Player", f"${bb_latest.get('turnover_per_player', 0):,.2f}")

                # --- 2. REVENUE TREND ---
                st.markdown("#### 📈 Revenue Trend (Month-over-Month)")
                
                fin_excel = _get_financial_excel_bytes(filtered_monthly, cohort_matrices, segmentation, filtered_both)
                st.download_button("📥 Download Financial Detail Report", data=fin_excel, file_name=f"Financial_Detail_{selected_client}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

                chart_data = filtered_both[["month", rev_col]].set_index("month")
                st.bar_chart(chart_data, width='stretch')

                # --- 3. VERTICAL COMPOSITION (CASINO VS SPORTS) ---
                # Only show the vertical chart if LeoVegas data actually exists in the current slice
                if "LeoVegas Group" in _raw_df["client"].unique():
                    st.markdown("#### > VERTICAL COMPOSITION (CASINO vs SPORTS)_")
                    v_casino = "ggr_casino" if revenue_mode == "GGR" else "ngr_casino"
                    v_sports = "ggr_sports" if revenue_mode == "GGR" else "ngr_sports"
                
                    if v_casino in filtered_both.columns and v_sports in filtered_both.columns:
                        vert_df = filtered_both[["month", v_casino, v_sports]].copy()
                        vert_df.rename(columns={"month": "Month", v_casino: "Casino", v_sports: "Sportsbook"}, inplace=True)
                        st.bar_chart(vert_df, x="Month", y=["Casino", "Sportsbook"], color=["#00FF41", "#1E90FF"])

                # --- 3.5 GROSS TO NET WATERFALL (TAX & BONUS) ---
                if "LeoVegas Group" in _raw_df["client"].unique():
                    st.markdown("---")
                    st.markdown("#### > GROSS-TO-NET REVENUE WATERFALL_")
                    st.markdown("*Insight: Visualizes the capital bleed from Gross Gaming Revenue down to True Net Income.*")
                
                    # Get the latest month's totals from the filtered slice
                    ggr_val = bb_latest.get("ggr", 0)
                    bonus_val = -bb_latest.get("bonus_total", 0)
                    tax_val = -bb_latest.get("tax_total", 0)
                    ngr_val = bb_latest.get("ngr", 0)
                
                    fig_waterfall = go.Figure(go.Waterfall(
                        name="Revenue Flow", orientation="v",
                        measure=["relative", "relative", "relative", "total"],
                        x=["Gross Revenue (GGR)", "Bonus Cost", "Tax Burden", "Net Income (NGR)"],
                        textposition="outside",
                        text=[f"${ggr_val:,.0f}", f"${bonus_val:,.0f}", f"${tax_val:,.0f}", f"${ngr_val:,.0f}"],
                        y=[ggr_val, bonus_val, tax_val, ngr_val],
                        connector={"line": {"color": "#333333", "width": 2}},
                        decreasing={"marker": {"color": "#FF4444"}},
                        increasing={"marker": {"color": "#00FF41"}},
                        totals={"marker": {"color": "#1E90FF"}}
                    ))
                
                    fig_waterfall.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)", 
                        plot_bgcolor="rgba(0,0,0,0)",
                        font_color="#00FF41",
                        margin=dict(t=30, l=0, r=0, b=0),
                        height=450,
                        yaxis=dict(gridcolor="#1a1a1a", tickprefix="$")
                    )
                    st.plotly_chart(fig_waterfall, width='stretch', config={"scrollZoom": False})

                # --- 3.6 PRODUCT AFFINITY & CROSS-SELLING ---
                if "LeoVegas Group" in _raw_df["client"].unique():
                    st.markdown("---")
                    st.markdown("#### > PRODUCT AFFINITY & CROSS-SELLING_")
                    st.markdown("*Insight: Categorizes players by their betting behavior to reveal the high-value Omnichannel segment.*")

                    from src.analytics import generate_affinity_matrix
                    affinity_df = generate_affinity_matrix(_raw_df)

                    if not affinity_df.empty:
                        aff1, aff2 = st.columns([1, 1.5])

                        with aff1:
                            fig_donut = px.pie(
                                affinity_df,
                                names="Affinity",
                                values="Players",
                                hole=0.6,
                                color="Affinity",
                                color_discrete_map={
                                    "Omnichannel": "#FFD700", 
                                    "Casino Only": "#00FF41", 
                                    "Sportsbook Only": "#1E90FF", 
                                    "Inactive": "#555555"
                                }
                            )
                            fig_donut.update_layout(
                                paper_bgcolor="rgba(0,0,0,0)",
                                plot_bgcolor="rgba(0,0,0,0)",
                                font_color="#00FF41",
                                margin=dict(t=30, l=0, r=0, b=0),
                                showlegend=True
                            )
                            st.plotly_chart(fig_donut, width='stretch')

                        with aff2:
                            st.dataframe(
                                affinity_df,
                                width='stretch',
                                hide_index=True,
                                column_config={
                                    "Affinity": st.column_config.TextColumn("Segment"),
                                    "Players": st.column_config.NumberColumn("Player Count", format="%d"),
                                    "Total_NGR": st.column_config.NumberColumn("Total NGR", format="$%.2f"),
                                    "Avg_NGR_per_Player": st.column_config.NumberColumn("Avg NGR / Player", format="$%.2f")
                                }
                            )

                # --- 4. FULL DATA TABLE ---
                with st.expander(f"📋 Raw Data Table ({len(filtered_both)} months)", expanded=True):
                    st.dataframe(filtered_both, width='stretch', hide_index=True)

                # ── Player Demographics Chart ───────────────────────────────
                st.markdown("#### > COMBINED PLAYER DEMOGRAPHICS (MONTH OVER MONTH)_")
                demo_bb = filtered_both[["month", "total_players", "profitable_players", "negative_yield_players"]].copy()
                demo_bb = demo_bb.rename(columns={
                    "total_players": "Total Players",
                    "profitable_players": "Profitable (Winners)",
                    "negative_yield_players": "Neg. Yield (Losers)",
                })
                st.line_chart(
                    demo_bb.set_index("month"),
                    width='stretch',
                    color=["#AAAAAA", "#00FF41", "#FF4444"],
                )

                # ── Comparative Intelligence (Phase 11) ───────────────────
                ts = _cached_time_series(filtered_both)
                ts_m = ts["monthly"]
                ts_q = ts["quarterly"]

                if not ts_m.empty:
                    st.markdown("#### > COMPARATIVE INTELLIGENCE_")
                    latest_m = ts_m.iloc[-1]
                    latest_q = ts_q.iloc[-1] if not ts_q.empty else None

                    # ── Smart Narrative (Phase 15) ────────────────────────
                    combined_fin_latest = financial_summary[
                        financial_summary["brand"] == "Combined"
                    ].sort_values("month").iloc[-1]
                    whale_dep = float(combined_fin_latest.get("top_10_pct_ggr_share", 0))
                    margin_val = float(bb_latest.get("margin", 0))
                    narrative = generate_smart_narrative(latest_m, margin_val, whale_dep)
                    if margin_val < 2.5 or whale_dep >= 70:
                        st.warning(narrative)
                    else:
                        st.info(narrative)

                    def _arrow(val):
                        if pd.isna(val): return "—"
                        return f"↑ {val:+,.0f}" if val >= 0 else f"↓ {val:,.0f}"

                    def _arrow_pct(val):
                        if pd.isna(val): return "—"
                        return f"↑ {val:+.1f}%" if val >= 0 else f"↓ {val:.1f}%"

                    # Financials group
                    st.markdown("##### 💰 Financials")
                    fin_cols = ["turnover", "ggr", "revenue_share_deduction"]
                    fin_labels = ["Turnover", "GGR", "Revenue (15%)"]
                    fin_rows = []
                    for col, label in zip(fin_cols, fin_labels):
                        row = {"Metric": label}
                        row["MoM Δ"] = _arrow(latest_m.get(f"{col}_mom_delta"))
                        row["MoM %"] = _arrow_pct(latest_m.get(f"{col}_mom_pct"))
                        row["YoY Δ"] = _arrow(latest_m.get(f"{col}_yoy_delta"))
                        row["YoY %"] = _arrow_pct(latest_m.get(f"{col}_yoy_pct"))
                        row["YTD"] = f"${latest_m.get(f'{col}_ytd', 0):,.0f}"
                        if latest_q is not None:
                            row["QoQ Δ"] = _arrow(latest_q.get(f"{col}_qoq_delta"))
                            row["QoQ %"] = _arrow_pct(latest_q.get(f"{col}_qoq_pct"))
                        fin_rows.append(row)

                    # EOY Projected metrics — Dual Engine (Phase 15 upgrade)
                    eoy_rows = []
                    for proj_col, proj_label in [("ggr", "GGR"), ("turnover", "Turnover"), ("revenue_share_deduction", "Revenue 15%")]:
                        for eng_label, prefix in [("Seasonal", "eoy_seasonal"), ("Momentum", "eoy_momentum")]:
                            eoy_key = f"{prefix}_{proj_col}"
                            eoy_val = latest_m.get(eoy_key, 0) or 0
                            fin_rows.append({"Metric": f"EOY {proj_label} ({eng_label})", "MoM Δ": "—", "MoM %": "—",
                                             "YoY Δ": "—", "YoY %": "—",
                                             "YTD": f"${eoy_val:,.0f}"})
                    st.dataframe(pd.DataFrame(fin_rows), width='stretch', hide_index=True)
                    st.caption("🔮 **EOY PROJECTIONS:** Seasonal uses prior-year proportional scaling. Momentum uses 3-month rolling average × remaining months.")

                    # Player Demographics group
                    st.markdown("##### 👥 Player Demographics")
                    plr_cols = ["total_players", "profitable_players", "negative_yield_players", "conversions", "new_players", "reactivated_players", "returning_players"]
                    plr_labels = ["Total Active", "Profitable (Winners)", "Neg. Yield (Losers)", "Conversions", "New Players", "Reactivated Players", "Returning Players"]
                    plr_rows = []
                    for col, label in zip(plr_cols, plr_labels):
                        row = {"Metric": label}
                        row["MoM Δ"] = _arrow(latest_m.get(f"{col}_mom_delta"))
                        row["MoM %"] = _arrow_pct(latest_m.get(f"{col}_mom_pct"))
                        row["YoY Δ"] = _arrow(latest_m.get(f"{col}_yoy_delta"))
                        row["YoY %"] = _arrow_pct(latest_m.get(f"{col}_yoy_pct"))
                        row["YTD"] = f"{int(latest_m.get(f'{col}_ytd', 0)):,}"
                        if latest_q is not None:
                            row["QoQ Δ"] = _arrow(latest_q.get(f"{col}_qoq_delta"))
                            row["QoQ %"] = _arrow_pct(latest_q.get(f"{col}_qoq_pct"))
                        plr_rows.append(row)
                    st.dataframe(pd.DataFrame(plr_rows), width='stretch', hide_index=True)

                # ── Risk & Value Metrics (Phase 12) ─────────────────────────
                st.markdown("#### > RISK & VALUE METRICS_")
                rv1, rv2 = st.columns(2)
                with rv1:
                    st.metric("Turnover Per Player",
                              f"${bb_latest['turnover_per_player']:,.2f}")
                with rv2:
                    top10 = financial_summary[
                        financial_summary["brand"] == "Combined"
                    ].sort_values("month").iloc[-1].get("top_10_pct_ggr_share", 0)
                    st.metric("Whale Dependency (Top 10% GGR)",
                              f"{top10:.2f}%")

                # Revenue Composition chart
                st.markdown("##### 📊 Revenue Composition: New vs Returning Player GGR")
                rev_comp = filtered_both[["month", "new_player_ggr", "returning_player_ggr"]].copy()
                rev_comp = rev_comp.rename(columns={"month": "Month", "new_player_ggr": "New_Player_GGR", "returning_player_ggr": "Returning_Player_GGR"})
                rev_comp["New (Profit)"] = rev_comp["New_Player_GGR"].clip(lower=0)
                rev_comp["New (Loss)"] = rev_comp["New_Player_GGR"].clip(upper=0)
                rev_comp["Returning (Profit)"] = rev_comp["Returning_Player_GGR"].clip(lower=0)
                rev_comp["Returning (Loss)"] = rev_comp["Returning_Player_GGR"].clip(upper=0)
                st.bar_chart(rev_comp, x="Month",
                             y=["New (Profit)", "New (Loss)", "Returning (Profit)", "Returning (Loss)"],
                             color=["#00FF41", "#FF0000", "#CCCCCC", "#804040"])

                # ── VIP Tiering (Phase 15 - RFM) ─────────────────────────
                try:
                    latest_month_str = filtered_both["month"].max()
                    rfm = _cached_rfm_summary(_raw_df, latest_month_str)
                    if not rfm.empty:
                        st.markdown(f"##### 🏆 VIP Tiering — RFM Segmentation ({latest_month_str})")
                        t1, t2, t3 = st.columns(3)
                        for i, (col_widget, tier_name, color) in enumerate([
                            (t1, "True VIP", "#00FF41"),
                            (t2, "Churn Risk", "#FF4444"),
                            (t3, "Casual", "#AAAAAA"),
                        ]):
                            tier_row = rfm[rfm["Tier"] == tier_name]
                            players = int(tier_row["Players"].iloc[0]) if not tier_row.empty else 0
                            ggr = float(tier_row["GGR"].iloc[0]) if not tier_row.empty else 0.0
                            with col_widget:
                                st.metric(tier_name, f"{players:,} players")
                                st.caption(f"GGR: ${ggr:,.2f}")
                        st.dataframe(
                            rfm,
                            width='stretch',
                            hide_index=True,
                            column_config={
                                "Tier": st.column_config.TextColumn("Tier"),
                                "Players": st.column_config.NumberColumn("Players", format="%d"),
                                "GGR": st.column_config.NumberColumn("GGR", format="$%.2f"),
                            },
                        )
                except (NameError, Exception):
                    pass  # RFM Phase 15 not yet implemented

                # Full Both Business table
                with st.expander(f"📋 Both Business Summary ({len(filtered_both)} months)", expanded=True):
                    st.dataframe(
                        filtered_both,
                        width='stretch',
                        hide_index=True,
                        column_config={
                            "month": st.column_config.TextColumn("Month"),
                            "turnover": st.column_config.NumberColumn("Turnover", format="$%.2f"),
                            "ggr": st.column_config.NumberColumn("GGR", format="$%.2f"),
                            "margin": st.column_config.NumberColumn("Margin %", format="%.2f%%"),
                            "revenue_share_deduction": st.column_config.NumberColumn("Rev Share (15%)", format="$%.2f"),
                            "net_income": st.column_config.NumberColumn("Net Income", format="$%.2f"),
                            "new_players": st.column_config.NumberColumn("New Players", format="%d"),
                            "returning_players": st.column_config.NumberColumn("Returning", format="%d"),
                            "reactivated_players": st.column_config.NumberColumn("Reactivated", format="%d"),
                            "conversions": st.column_config.NumberColumn("Conversions", format="%d"),
                            "total_players": st.column_config.NumberColumn("Total Players", format="%d"),
                            "profitable_players": st.column_config.NumberColumn("Winners", format="%d"),
                            "negative_yield_players": st.column_config.NumberColumn("Losers", format="%d"),
                            "new_players_pct": st.column_config.NumberColumn("New %", format="%.2f%%"),
                            "returning_players_pct": st.column_config.NumberColumn("Returning %", format="%.2f%%"),
                            "ggr_per_player": st.column_config.NumberColumn("GGR/Player", format="$%.2f"),
                            "turnover_per_player": st.column_config.NumberColumn("Turnover/Player", format="$%.2f"),
                            "income_per_player": st.column_config.NumberColumn("Income/Player", format="$%.2f"),
                            "new_player_ggr": st.column_config.NumberColumn("New Player GGR", format="$%.2f"),
                            "returning_player_ggr": st.column_config.NumberColumn("Ret. Player GGR", format="$%.2f"),
                        },
                    )

                # Combined cohort matrix
                if cohort_matrices and "Combined" in cohort_matrices:
                    matrix = cohort_matrices["Combined"]
                    if not matrix.empty:
                        with st.expander("🔄 Combined Cohort Retention Matrix", expanded=False):
                            st.dataframe(
                                matrix.style.format("{:.1f}%", na_rep="—"),
                                width='stretch',
                            )

                # ── Cohort Retention Heatmap (Phase 18) ──────────────────────
                st.markdown("---")
                st.markdown("#### > COHORT RETENTION HEATMAP_")
                heatmap_fig = _cached_retention_heatmap()
                if heatmap_fig is not None:
                    st.plotly_chart(heatmap_fig, width='stretch', config={"scrollZoom": False})
                else:
                    st.info("Not enough data to generate a retention heatmap.")

                # ── Segmentation by Program ─────────────────────────────
                if program_summary is not None and not program_summary.empty:
                    st.markdown("---")
                    st.markdown("#### > SEGMENTATION BY PROGRAM_")
                    st.markdown("*Insight: Evaluates the financial efficiency and house edge (Margin) across different marketing programs (ACQ, RET, WB).*")
                    st.dataframe(
                        program_summary,
                        width='stretch',
                        hide_index=True,
                        column_config={
                            "brand": st.column_config.TextColumn("Brand"),
                            "month": st.column_config.TextColumn("Month"),
                            "Program": st.column_config.TextColumn("Program"),
                            "ggr": st.column_config.NumberColumn("GGR", format="$%d"),
                            "Turnover": st.column_config.NumberColumn("Turnover", format="$%d"),
                            "Margin": st.column_config.NumberColumn("Margin", format="%.2f%%"),
                            "total_players": st.column_config.NumberColumn("Players", format="%d"),
                        },
                    )

                # ── Advanced Structural Analytics (Phase 7B) ────────────────
                st.markdown("---")
                st.subheader("📈 Advanced Structural Analytics")
                
                from src.analytics import generate_pareto_curve, generate_ltv_curves
                
                ltv_df = generate_ltv_curves(_raw_df)
                pareto_df = generate_pareto_curve(_raw_df)
                
                asa1, asa2 = st.columns(2)
                
                with asa1:
                    st.markdown("##### 🐋 80/20 Pareto Distribution")
                    st.markdown("*Insight: Plots the cumulative % of revenue driven by the cumulative % of players to visually identify whale concentration.*")
                    if not pareto_df.empty:
                        fig_par = px.area(
                            pareto_df, 
                            x="cumulative_players_pct", 
                            y="cumulative_ggr_pct",
                            title="Cumulative GGR vs Player Base"
                        )
                        # Add the 80/20 anchor lines
                        fig_par.add_vline(x=20, line_dash="dash", line_color="#FF4444")
                        fig_par.add_hline(y=80, line_dash="dash", line_color="#FF4444")
                        
                        fig_par.update_layout(
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(0,0,0,0)",
                            font_color="#00FF41",
                            xaxis_title="% of Total Players",
                            yaxis_title="% of Total GGR",
                            margin=dict(l=0, r=0, t=40, b=0)
                        )
                        st.plotly_chart(fig_par, width='stretch')
                    else:
                        st.info("Not enough profitable players to calculate a distribution.")
                        
                with asa2:
                    st.markdown("##### 📈 Cumulative Cohort LTV")
                    st.markdown("*Insight: Tracks the cumulative revenue progression of monthly cohorts.*")
                    if not ltv_df.empty:
                        fig_ltv = px.line(
                            ltv_df, 
                            x="month_index", 
                            y="Cumulative_GGR", 
                            color="cohort_month",
                            title="LTV Progression by Cohort"
                        )
                        fig_ltv.update_layout(
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(0,0,0,0)",
                            font_color="#00FF41",
                            xaxis_title="Months Since Acquisition",
                            yaxis_title="Cumulative GGR ($)",
                            margin=dict(l=0, r=0, t=40, b=0),
                            legend_title="Acquisition Cohort"
                        )
                        st.plotly_chart(fig_ltv, width='stretch')
                    else:
                        st.info("Not enough data to generate LTV curves.")

            else:
                st.warning("No Both Business data available.")

    if "🕵️ CRM Intelligence" in tab_map:
        with tab_map["🕵️ CRM Intelligence"]:
            _raw_df = df.copy()  # <-- ADDED DECLARATION

            # ── Early-Warning VIP Churn Radar ────────────────────────────────
            st.markdown("#### > 📉 EARLY-WARNING VIP CHURN RADAR_")
            st.markdown("*Insight: Proactively flags high-value VIPs whose month-over-month NGR trajectory has crashed by >30%. Call them before they leave.*")

            from src.analytics import generate_vip_churn_radar
            churn_df = generate_vip_churn_radar(_raw_df)

            if not churn_df.empty:
                st.error(f"🚨 {len(churn_df)} VIPs are exhibiting severe flight risk behaviors in the latest month.")
                st.dataframe(
                    churn_df[["id", "brand", "Prev_Month_NGR", "Curr_Month_NGR", "NGR_Drop_Value", "NGR_Drop_Pct"]],
                    width='stretch',
                    hide_index=True,
                    column_config={
                        "id": st.column_config.TextColumn("VIP Player ID"),
                        "brand": st.column_config.TextColumn("Brand"),
                        "Prev_Month_NGR": st.column_config.NumberColumn("Previous Month NGR", format="$%.2f"),
                        "Curr_Month_NGR": st.column_config.NumberColumn("Current Month NGR", format="$%.2f"),
                        "NGR_Drop_Value": st.column_config.NumberColumn("Absolute NGR Lost", format="-$%.2f"),
                        "NGR_Drop_Pct": st.column_config.NumberColumn("Flight Risk Severity", format="%.1f%%")
                    }
                )
            else:
                if "month" not in _raw_df.columns or _raw_df["month"].nunique() < 2:
                    st.info("Requires at least 2 months of data to calculate month-over-month churn trajectories.")
                else:
                    st.success("✅ All VIPs are maintaining stable or growing month-over-month trajectories. No immediate flight risks detected.")
        
            st.markdown("---")

            # Initialize tracking scope variables formerly tied to the generic Dashboard setup
            active_brands = sorted([b for b in _raw_df["brand"].unique() if b != "Combined"]) if "brand" in _raw_df.columns else []
            combined_label = "All Business" if len(active_brands) > 2 else "Both Business"
            latest_month = _raw_df["month"].max() if "month" in _raw_df.columns and not _raw_df.empty else pd.Timestamp.today().strftime('%Y-%m')

            # ── Cross-Brand VIP Health ────────────────────────────────────
            st.markdown("---")
            st.markdown("#### > CROSS-BRAND VIP HEALTH_")
            tier_labels, tier_search = ["True VIPs", "Churn Risk VIPs", "Casuals"], ["True VIP", "Churn Risk", "Casual"]

            def _vip_snap(raw_subset):
                b_latest = raw_subset["month"].max() if "month" in raw_subset.columns and not raw_subset.empty else latest_month
                b_name = raw_subset["brand"].iloc[0] if "brand" in raw_subset.columns and not raw_subset.empty else "Combined"
                rfm = _cached_tier_summary(b_name, b_latest)
                if rfm.empty: return [0] * len(tier_labels)
                return [int(rfm.loc[rfm.iloc[:, 0].str.contains(s, na=False, case=False), rfm.columns[1]].sum()) if rfm.iloc[:, 0].str.contains(s, na=False, case=False).any() else 0 for s in tier_search]

            vip_data = {"Tier": tier_labels, combined_label: _vip_snap(df)}
            for brand in active_brands:
                b_vip = _vip_snap(df[df["brand"] == brand])
                vip_data[brand] = b_vip if b_vip else [0] * len(tier_labels)

            cfg_vip = {"Tier": st.column_config.TextColumn("Tier"), combined_label: st.column_config.NumberColumn(combined_label, format="%d")}
            for brand in active_brands: cfg_vip[brand] = st.column_config.NumberColumn(brand, format="%d")
            st.dataframe(pd.DataFrame(vip_data), width='stretch', hide_index=True, column_config=cfg_vip)


            # ── Cross-Brand Cannibalization ──────────────────────────────
            st.markdown("---")
            st.markdown("#### > CROSS-BRAND CANNIBALIZATION (ALL-TIME)_")
            st.markdown("*Insight: Identifies players active on both platforms to expose duplicate customer acquisition costs and shared revenue dependency.*")

            from src.analytics import generate_overlap_stats
            overlap = generate_overlap_stats(df)
            ov1, ov2 = st.columns(2)
            with ov1:
                st.metric("Shared Players (Overlap)", f"{overlap['overlap_count']:,}")
            with ov2:
                st.metric("Shared Lifetime GGR", f"${overlap['overlap_ggr']:,.2f}")


            st.markdown("#### > VIP & RISK LEADERBOARDS_")
            st.caption(f"Currently viewing CRM targets for: {selected_client} | {selected_brand} | {selected_country}")
        
            # Load data through the CRM Engine heuristics
            from src.analytics import generate_rfm_summary, generate_smart_profiles
            
            @st.cache_data(ttl=900)
            def _get_crm_intel(df_in):
                rfm = generate_rfm_summary(df_in)
                smart = generate_smart_profiles(rfm)
                # Align to legacy column expectations internally
                if 'Smart_Profile' in smart.columns:
                    smart = smart.rename(columns={'Smart_Profile': 'Recommended_Campaign'})
                return smart
                
            master_df = _get_crm_intel(_raw_df)
            filtered_master = master_df.copy()
        
            if filtered_master.empty:
                st.warning("No player data available.")
            else:
                st.caption(f"{len(filtered_master):,} players loaded")

                _lb_col_config = {
                    "id": st.column_config.TextColumn("Player ID"),
                    "brand": st.column_config.TextColumn("Brand"),
                    "Lifetime_GGR": st.column_config.NumberColumn("Lifetime GGR", format="$%.2f"),
                    "Lifetime_Turnover": st.column_config.NumberColumn("Lifetime Turnover", format="$%.2f"),
                    "First_Month": st.column_config.TextColumn("First Month"),
                    "Last_Month": st.column_config.TextColumn("Last Month"),
                    "Months_Active": st.column_config.NumberColumn("Months Active", format="%d"),
                    "Months_Inactive": st.column_config.NumberColumn("Months Inactive", format="%d"),
                }

                lb1, lb2 = st.columns(2)
                with lb1:
                    st.markdown("##### 👑 The Crown Jewels (Top 50 GGR)")
                    top50 = filtered_master.nlargest(50, "Lifetime_GGR")
                    st.dataframe(
                        top50,
                        width='stretch',
                        hide_index=True,
                        column_config=_lb_col_config,
                    )

                with lb2:
                    st.markdown("##### ⚠️ Bonus Abusers (High Volume, Negative GGR)")
                    abusers = (
                        filtered_master[filtered_master["Lifetime_GGR"] < 0]
                        .nlargest(50, "Lifetime_Turnover")
                    )
                    if abusers.empty:
                        st.info("No negative-GGR players found — clean book.")
                    else:
                        st.dataframe(
                            abusers,
                            width='stretch',
                            hide_index=True,
                            column_config=_lb_col_config,
                        )

                # ── Advanced Promo Exploitation Radar (LeoVegas Only) ────────────────
                if "LeoVegas Group" in filtered_master["client"].unique():
                    st.markdown("---")
                    st.markdown("#### > 🚨 PROMO EXPLOITATION RADAR (LEOVEGAS)_")
                    st.markdown("*Insight: Flags toxic players who extract high bonus value, successfully withdraw cash, and generate negative Net Gaming Revenue for the house.*")

                    # Define a toxic player: Negative NGR, >$50 in Bonuses, and >$0 Withdrawn
                    promo_abusers = filtered_master[
                        (filtered_master["Lifetime_NGR"] < 0) & 
                        (filtered_master["Lifetime_Bonus"] > 50) & 
                        (filtered_master["Lifetime_Withdrawals"] > 0)
                    ].sort_values("Lifetime_NGR", ascending=True).head(50)

                    if promo_abusers.empty:
                        st.success("✅ No critical promo abusers detected matching the toxic profile.")
                    else:
                        st.dataframe(
                            promo_abusers[["id", "brand", "Lifetime_NGR", "Lifetime_Bonus", "Lifetime_Withdrawals", "Lifetime_Turnover", "Months_Active"]],
                            width='stretch',
                            hide_index=True,
                            column_config={
                                "id": st.column_config.TextColumn("Player ID"),
                                "brand": st.column_config.TextColumn("Brand"),
                                "Lifetime_NGR": st.column_config.NumberColumn("Net Loss to House", format="$%.2f"),
                                "Lifetime_Bonus": st.column_config.NumberColumn("Bonus Extracted", format="$%.2f"),
                                "Lifetime_Withdrawals": st.column_config.NumberColumn("Cash Withdrawn", format="$%.2f"),
                                "Lifetime_Turnover": st.column_config.NumberColumn("Turnover", format="$%.2f"),
                                "Months_Active": st.column_config.NumberColumn("Months Active", format="%d")
                            }
                        )

                # ── Payment Gateway Friction Radar (LeoVegas Only) ────────────────
                if "LeoVegas Group" in filtered_master["client"].unique():
                    st.markdown("---")
                    st.markdown("#### > 💳 PAYMENT GATEWAY FRICTION (FEE BLEED)_")
                    st.markdown("*Insight: Flags players making excessive micro-deposits. Every transaction incurs a fixed payment provider fee, eroding true margins.*")

                    # Define High Friction: >= 10 total deposits with an average value of <= $25
                    high_friction = filtered_master[
                        (filtered_master["Lifetime_Deposit_Count"] >= 10) & 
                        (filtered_master["Avg_Deposit_Value"] <= 25) &
                        (filtered_master["Avg_Deposit_Value"] > 0)
                    ].sort_values("Lifetime_Deposit_Count", ascending=False).head(50)

                    if high_friction.empty:
                        st.success("✅ No high-friction micro-depositors detected.")
                    else:
                        st.warning(f"⚠️ {len(high_friction)} players flagged for high transaction fee bleed. (>=10 deposits, avg <= $25)")
                        st.dataframe(
                            high_friction[["id", "brand", "Lifetime_Deposit_Count", "Lifetime_Deposits", "Avg_Deposit_Value", "Lifetime_NGR"]],
                            width='stretch',
                            hide_index=True,
                            column_config={
                                "id": st.column_config.TextColumn("Player ID"),
                                "brand": st.column_config.TextColumn("Brand"),
                                "Lifetime_Deposit_Count": st.column_config.NumberColumn("Total Transactions", format="%d"),
                                "Lifetime_Deposits": st.column_config.NumberColumn("Total Deposited", format="$%.2f"),
                                "Avg_Deposit_Value": st.column_config.NumberColumn("Avg Deposit Value (ADV)", format="$%.2f"),
                                "Lifetime_NGR": st.column_config.NumberColumn("Net Income (NGR)", format="$%.2f")
                            }
                        )

                # ── Reactivation Velocity (LeoVegas Only) ────────────────
                if "LeoVegas Group" in filtered_master["client"].unique():
                    st.markdown("---")
                    st.markdown("#### > ⏳ CAMPAIGN REACTIVATION VELOCITY_")
                    st.markdown("*Insight: Measures the exact time delay between a Campaign Start Date and the player's actual Reactivation Date.*")

                    from src.analytics import generate_reactivation_velocity
                    # We use the raw df for this because it needs the un-aggregated dates
                    vel_df = generate_reactivation_velocity(_raw_df[_raw_df["client"] == "LeoVegas Group"])

                    if not vel_df.empty:
                        v1, v2 = st.columns([1.5, 1])
                    
                        with v1:
                            fig_vel = px.bar(
                                vel_df, 
                                x="Velocity", 
                                y="Reactivated_Players",
                                color="Total_NGR",
                                color_continuous_scale="Blues",
                                text="Reactivated_Players",
                                title="Players Reactivated by Time Delay"
                            )
                            fig_vel.update_layout(
                                paper_bgcolor="rgba(0,0,0,0)",
                                plot_bgcolor="rgba(0,0,0,0)",
                                font_color="#00FF41",
                                xaxis_title="",
                                yaxis_title="Player Count"
                            )
                            st.plotly_chart(fig_vel, width='stretch')
                        
                        with v2:
                            st.dataframe(
                                vel_df,
                                width='stretch',
                                hide_index=True,
                                column_config={
                                    "Velocity": st.column_config.TextColumn("Response Speed"),
                                    "Reactivated_Players": st.column_config.NumberColumn("Players", format="%d"),
                                    "Total_Deposits": st.column_config.NumberColumn("Deposits Generated", format="$%.2f"),
                                    "Total_NGR": st.column_config.NumberColumn("NGR Generated", format="$%.2f")
                                }
                            )
                    else:
                        st.info("No valid Campaign/Reactivation date pairs found in the current dataset.")

                # ── Churn Targeting Generator (Phase 17.2) ────────────────────
                st.markdown("---")
                st.markdown("#### > CHURN TARGETING GENERATOR_")

                max_inactive = int(filtered_master["Months_Inactive"].max()) if not filtered_master.empty else 12

                # Streamlit sliders crash if min_value == max_value. 
                # This forces the slider to have a minimum range of 1 to 2, even for 1-month datasets.
                if max_inactive <= 1:
                    max_inactive = 2

                ct1, ct2 = st.columns(2)
                with ct1:
                    min_inactive = st.slider(
                        "Minimum Months Inactive",
                        min_value=1, max_value=max(max_inactive, 1), value=min(3, max_inactive),
                        key="churn_min_inactive",
                    )
                with ct2:
                    min_ggr = st.number_input(
                        "Minimum Lifetime GGR ($)",
                        min_value=0.0, value=500.0, step=100.0,
                        key="churn_min_ggr",
                    )

                target_df = filtered_master[
                    (filtered_master["Months_Inactive"] >= min_inactive)
                    & (filtered_master["Lifetime_GGR"] >= min_ggr)
                ].sort_values("Lifetime_GGR", ascending=False)

                st.metric(label="🎯 TARGET ACQUIRED (Players Found)", value=f"{len(target_df):,}")

                if not target_df.empty:
                    display_cols = ["id", "brand", "Last_Month", "Months_Inactive", "Lifetime_GGR", "Lifetime_Turnover", "Recommended_Campaign"]
                    st.dataframe(
                        target_df[display_cols],
                        width='stretch',
                        hide_index=True,
                        column_config={
                            "id": st.column_config.TextColumn("Player ID"),
                            "brand": st.column_config.TextColumn("Brand"),
                            "Last_Month": st.column_config.TextColumn("Last Month"),
                            "Months_Inactive": st.column_config.NumberColumn("Months Inactive", format="%d"),
                            "Lifetime_GGR": st.column_config.NumberColumn("Lifetime GGR", format="$%.2f"),
                            "Lifetime_Turnover": st.column_config.NumberColumn("Lifetime Turnover", format="$%.2f"),
                            "Recommended_Campaign": st.column_config.TextColumn("Campaign"),
                        },
                    )
                    st.download_button(
                        label="⬇️ DOWNLOAD TARGET LIST (CSV)",
                        data=target_df[display_cols].to_csv(index=False).encode("utf-8"),
                        file_name="winback_targets.csv",
                        mime="text/csv",
                        width='stretch',
                    )
                else:
                    st.info("No players match the current filters. Adjust the sliders above.")

                # ── Smart Campaign Profiling (Phase 17.4) ─────────────────────
                st.markdown("---")
                st.markdown("#### > SMART CAMPAIGN PROFILING_")

                special_campaigns = filtered_master[
                    filtered_master["Recommended_Campaign"] != "✉️ Standard Lifecycle"
                ]
                campaign_counts = (
                    special_campaigns["Recommended_Campaign"]
                    .value_counts()
                    .reindex(["🏆 Ironman Legend", "🛑 Promo Exclusion", "🚨 Early Churn VIP", "🌟 Rising Star", "🎯 Cold Crown Jewel", "👑 Active Crown Jewel", "📉 Cooling Down"], fill_value=0)
                )

                row1 = st.columns(4)
                row1[0].metric("🏆 Ironman Legend", f"{campaign_counts.get('🏆 Ironman Legend', 0):,}")
                row1[1].metric("🛑 Promo Exclusion", f"{campaign_counts.get('🛑 Promo Exclusion', 0):,}")
                row1[2].metric("🚨 Early Churn VIP", f"{campaign_counts.get('🚨 Early Churn VIP', 0):,}")
                row1[3].metric("🌟 Rising Star", f"{campaign_counts.get('🌟 Rising Star', 0):,}")
                row2 = st.columns(4)
                row2[0].metric("🎯 Cold Crown Jewel", f"{campaign_counts.get('🎯 Cold Crown Jewel', 0):,}")
                row2[1].metric("👑 Active Crown Jewel", f"{campaign_counts.get('👑 Active Crown Jewel', 0):,}")
                row2[2].metric("📉 Cooling Down", f"{campaign_counts.get('📉 Cooling Down', 0):,}")
                row2[3].write("")

                st.dataframe(
                    special_campaigns[["id", "brand", "Last_Month", "Months_Inactive", "Lifetime_GGR", "Recommended_Campaign"]].sort_values("Lifetime_GGR", ascending=False),
                    width='stretch',
                    hide_index=True,
                )

                st.caption(f"{len(special_campaigns):,} players flagged for specialized campaigns out of {len(filtered_master):,} total.")

                # ── Campaign Extraction (Phase 17.5) ──────────────────────────
                st.markdown("### 📥 Extract Campaign List")
                all_campaigns = sorted(filtered_master["Recommended_Campaign"].unique().tolist())
                selected_campaign = st.selectbox(
                    "Select Campaign",
                    all_campaigns,
                    key="crm_campaign_dropdown",
                )
                campaign_extract_df = filtered_master[
                    filtered_master["Recommended_Campaign"] == selected_campaign
                ].sort_values("Lifetime_GGR", ascending=False)

                st.caption(f"{len(campaign_extract_df):,} players in **{selected_campaign}**")

                if not campaign_extract_df.empty:
                    extract_cols = ["id", "brand", "First_Month", "Last_Month", "Months_Active", "Months_Inactive", "Lifetime_GGR", "Lifetime_Turnover", "Recommended_Campaign"]
                    st.dataframe(
                        campaign_extract_df[extract_cols],
                        width='stretch',
                        hide_index=True,
                        column_config={
                            "id": st.column_config.TextColumn("Player ID"),
                            "brand": st.column_config.TextColumn("Brand"),
                            "First_Month": st.column_config.TextColumn("First Month"),
                            "Last_Month": st.column_config.TextColumn("Last Month"),
                            "Months_Active": st.column_config.NumberColumn("Active", format="%d"),
                            "Months_Inactive": st.column_config.NumberColumn("Inactive", format="%d"),
                            "Lifetime_GGR": st.column_config.NumberColumn("Lifetime GGR", format="$%.2f"),
                            "Lifetime_Turnover": st.column_config.NumberColumn("Lifetime Turnover", format="$%.2f"),
                            "Recommended_Campaign": st.column_config.TextColumn("Campaign"),
                        },
                    )
                    safe_name = selected_campaign.replace(" ", "_").replace(".", "").lower()
                    st.download_button(
                        label=f"⬇️ Download {selected_campaign} List",
                        data=campaign_extract_df[extract_cols].to_csv(index=False).encode("utf-8"),
                        file_name=f"campaign_{safe_name}.csv",
                        mime="text/csv",
                        width='stretch',
                    )
                else:
                    st.info(f"No players in {selected_campaign}.")

    # ==========================================
    # 📈 TAB: CAMPAIGNS & LIFECYCLE ROI (Now correctly un-nested!)
    # ==========================================
    if "📈 Campaigns" in tab_map:
        with tab_map["📈 Campaigns"]:
            _raw_df = df.copy()  # <-- ADDED DECLARATION

            st.markdown("#### > 🎯 LIFECYCLE & CAMPAIGN ROI MATRIX_")
            st.markdown("*Insight: Evaluates the true profitability and player quality of distinct marketing lifecycles and acquisition channels.*")

            from src.analytics import generate_segment_roi_matrix
            segment_df = generate_segment_roi_matrix(_raw_df)

            if not segment_df.empty:
                s1, s2 = st.columns([1, 1.5])
                
                with s1:
                    # Color scale shifts from Red (Negative NGR) to Green (Highly Profitable)
                    fig_seg = px.bar(
                        segment_df,
                        x="segment",
                        y="Actual_Earning",
                        color="Actual_Earning",
                        color_continuous_scale="RdYlGn",
                        title="True Net Profit by Lifecycle"
                    )
                    fig_seg.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        font_color="#00FF41",
                        xaxis_title="",
                        yaxis_title="Total NGR ($)",
                        coloraxis_showscale=False,
                        margin=dict(l=0, r=0, t=40, b=0)
                    )
                    st.plotly_chart(fig_seg, width='stretch')
                    
                with s2:
                    st.dataframe(
                        segment_df,
                        width='stretch',
                        hide_index=True,
                        column_config={
                            "segment": st.column_config.TextColumn("Marketing Lifecycle"),
                            "Total_Players": st.column_config.NumberColumn("Acquired Players", format="%d"),
                            "Total_Turnover": st.column_config.NumberColumn("Turnover", format="$%.2f"),
                            "Total_NGR": st.column_config.NumberColumn("Net Income (NGR)", format="$%.2f"),
                            "Rev_Share_15": st.column_config.NumberColumn("Rev Share (15%)", format="-$%.2f"),
                            "Actual_Earning": st.column_config.NumberColumn("Actual Earning", format="$%.2f"),
                            "Total_Bonus": st.column_config.NumberColumn("Bonus Burn", format="$%.2f"),
                            "Avg_NGR_per_Player": st.column_config.NumberColumn("Avg NGR / Player", format="$%.2f"),
                            "Margin_%": st.column_config.NumberColumn("House Margin", format="%.2f%%")
                        }
                    )
            else:
                st.info("No lifecycle data available in the current slice.")

else:
    # --- GRACEFUL DEGRADATION FOR EMPTY DATABASES ---
    if "🕵️ CRM Intelligence" in tab_map:
        with tab_map["🕵️ CRM Intelligence"]:
            st.warning("⚠️ **No Financial Data Loaded**. Please navigate to the **📥 Financial Ingestion** tab and upload your player reports to unlock CRM Intelligence.")
            
    if "📈 Campaigns" in tab_map:
        with tab_map["📈 Campaigns"]:
            st.warning("⚠️ **No Financial Data Loaded**. Please navigate to the **📥 Financial Ingestion** tab and upload your player reports to unlock Campaign ROI metrics.")
            
    if "🏦 Financial Deep-Dive" in tab_map:
        with tab_map["🏦 Financial Deep-Dive"]:
            st.warning("⚠️ **No Financial Data Loaded**. Please navigate to the **📥 Financial Ingestion** tab and upload your player reports to unlock the Financial Dashboard.")


# ==========================================
# 📞 TAB: HISTORICAL BENCHMARKS
# ==========================================
if "📉 Historical Benchmarks" in tab_map:
    with tab_map["📉 Historical Benchmarks"]:
        # Load benchmark data using the globally cached function
        try:
            _bench_df = fetch_ops_snapshots()
            if not _bench_df.empty:
                # Apply sidebar filters (same as global filter logic, minus date range)
                if selected_client != "All" and 'ops_client' in _bench_df.columns:
                    _bench_df = _bench_df[_bench_df['ops_client'] == selected_client]
                if selected_brand != "All" and 'ops_brand' in _bench_df.columns:
                    _bench_df = _bench_df[_bench_df['ops_brand'] == selected_brand]
                if selected_engagement != "All" and 'extracted_engagement' in _bench_df.columns:
                    _bench_df = _bench_df[_bench_df['extracted_engagement'] == selected_engagement]
                if selected_lifecycle != "All" and 'extracted_lifecycle' in _bench_df.columns:
                    _bench_df = _bench_df[_bench_df['extracted_lifecycle'] == selected_lifecycle]
                if selected_segment != "All" and 'extracted_segment' in _bench_df.columns:
                    _bench_df = _bench_df[_bench_df['extracted_segment'] == selected_segment]
                if selected_country != "All" and 'country' in _bench_df.columns:
                    _bench_df = _bench_df[_bench_df['country'].str.upper() == selected_country]
                if selected_category != "All" and 'campaign_name' in _bench_df.columns:
                    _bench_df = _bench_df[_bench_df['campaign_name'].str.upper().str.contains(selected_category, na=False)]
                if selected_campaign != "All" and 'Core_Signature' in _bench_df.columns:
                    _bench_df = _bench_df[_bench_df['Core_Signature'] == selected_campaign]
                
                # Build available half-year options from snapshot data
                _bench_df['ops_date'] = pd.to_datetime(_bench_df['ops_date'], errors='coerce')
                _bench_dates = _bench_df.dropna(subset=['ops_date'])
                _available_halves = []
                if not _bench_dates.empty:
                    from datetime import datetime as _bdt
                    _b_now = _bdt.now()
                    _b_min_yr = _bench_dates['ops_date'].min().year
                    _b_max_yr = _bench_dates['ops_date'].max().year
                    for _yr in range(_b_min_yr, _b_max_yr + 1):
                        h1_end = _bdt(_yr, 6, 30)
                        if h1_end < _b_now and not _bench_dates[(_bench_dates['ops_date'] >= f"{_yr}-01-01") & (_bench_dates['ops_date'] <= f"{_yr}-06-30")].empty:
                            _available_halves.append(f"H1 {_yr}")
                        h2_end = _bdt(_yr, 12, 31)
                        if h2_end < _b_now and not _bench_dates[(_bench_dates['ops_date'] >= f"{_yr}-07-01") & (_bench_dates['ops_date'] <= f"{_yr}-12-31")].empty:
                            _available_halves.append(f"H2 {_yr}")
                
                # Default selection priority: H2 2025 → H1 2025 → first available
                _default_halves = ["H2 2025", "H1 2025"]
                _all_options = _default_halves + [h for h in _available_halves if h not in _default_halves]
                # Only keep options that exist in data
                _valid_options = [h for h in _all_options if h in _available_halves]
                if not _valid_options:
                    _valid_options = _available_halves if _available_halves else ["H2 2025"]
                
                _hdr_col, _dd_col = st.columns([3, 2])
                with _hdr_col:
                    st.markdown("#### > 📊 COMPARATIVE BASELINE BENCHMARKS_")
                    st.markdown("*Compare current half-year YTD telemetry against a prior baseline across active filtered traffic.*")
                with _dd_col:
                    _selected_prior = st.selectbox("Compare against:", _valid_options, key="bench_prior_half_tab")
                
                _render_fixed_benchmark(_bench_df, prior_half=_selected_prior)
            else:
                st.caption("No snapshot data available.")
        except Exception as e:
            st.caption(f"Could not load benchmark data: {e}")

# ==========================================
# 📞 TAB: OPERATIONS COMMAND (Phase 3 - Telemarketing)
# ==========================================
if "📞 Operations Command" in tab_map:
    with tab_map["📞 Operations Command"]:
        st.markdown("#### > 📡 CALLSU & OPERATIONS COMMAND_")
        st.markdown("*Insight: Tracks True CAC, Lead Quality, and Contractual SLA Fulfillment.*")
        
        if "ops_df" in st.session_state and not st.session_state["ops_df"].empty:
            ops_df = st.session_state["ops_df"].copy()
            # Fetch SLAs from persistent DB (Cached 24h)
            vol_df = fetch_config_tables("SELECT * FROM contractual_volumes")
            bench_df = fetch_config_tables("SELECT * FROM granular_benchmarks")
            
            # Determine selected_client for the filename
            unique_clients = ops_df["ops_client"].unique()
            if len(unique_clients) == 1:
                selected_client = unique_clients[0]
            else:
                selected_client = "All" # Or prompt user to select if multiple

            @st.cache_data(show_spinner=False)
            def _get_ops_csv_bytes(df):
                return df.to_csv(index=False).encode('utf-8')

            ops_csv = _get_ops_csv_bytes(ops_df)
            st.download_button(
                "📥 Download Operations Ledger (CSV)", 
                data=ops_csv, 
                file_name=f"Operations_Ledger_{selected_client}.csv", 
                mime="text/csv", 
                type="primary"
            )

            st.markdown("---")
            st.markdown("##### ⚖️ SLA Fulfillment Tracker (Volume vs. Contract)")
            
            if not vol_df.empty:
                # Calculate number of days in the current slice to scale the monthly SLA
                num_days = ops_df['ops_date'].nunique() if 'ops_date' in ops_df.columns else 1
                sla_scale_factor = num_days / 30.0
                # Aggregate actuals to match Volume SLAs
                actuals = ops_df.groupby(["ops_client", "ops_brand", "ops_lifecycle"]).agg(
                    Actual_Records=("Records", "sum")
                ).reset_index()
                
                # Merge uploaded data with Volume rules
                merged_vol = pd.merge(
                    actuals, vol_df, 
                    left_on=["ops_client", "ops_brand", "ops_lifecycle"], 
                    right_on=["client_name", "brand_code", "lifecycle"], 
                    how="inner"
                )
                
                if not merged_vol.empty:
                    # Build per-brand cards: group lifecycles under each brand
                    from itertools import groupby as _groupby
                    brand_data = {}
                    for _, row in merged_vol.sort_values(['client_name', 'brand_code', 'lifecycle']).iterrows():
                        target = max(1, int(row["monthly_minimum_records"] * sla_scale_factor))
                        actual = int(row["Actual_Records"])
                        pct = min(actual / target, 1.0) if target > 0 else 0
                        key = (row['client_name'], row['brand_code'])
                        if key not in brand_data:
                            brand_data[key] = {'lifecycles': [], 'total_actual': 0, 'total_target': 0}
                        brand_data[key]['lifecycles'].append({'lc': row['lifecycle'], 'actual': actual, 'target': target, 'pct': pct})
                        brand_data[key]['total_actual'] += actual
                        brand_data[key]['total_target'] += target
                    
                    # Render 3-column grid of brand cards
                    brand_keys = list(brand_data.keys())
                    for row_start in range(0, len(brand_keys), 3):
                        row_keys = brand_keys[row_start:row_start + 3]
                        cols = st.columns(3)
                        for i, (client, brand) in enumerate(row_keys):
                            bd = brand_data[(client, brand)]
                            overall_pct = min(bd['total_actual'] / bd['total_target'], 1.0) if bd['total_target'] > 0 else 0
                            status = "🟢" if overall_pct >= 0.9 else ("🟡" if overall_pct >= 0.5 else "🔴")
                            with cols[i]:
                                st.markdown(f"{status} **{brand}** · {client}")
                                st.progress(overall_pct)
                                st.caption(f"**{bd['total_actual']:,} / {bd['total_target']:,}** ({num_days}d)")
                                for lc in bd['lifecycles']:
                                    lc_icon = "✅" if lc['pct'] >= 0.9 else "⚠️"
                                    st.caption(f"  {lc_icon} {lc['lc']}: {lc['actual']:,} / {lc['target']:,}")
                    st.markdown("---")
                else:
                    st.info("No active Volume SLAs match the currently loaded operations data. Add them in System Settings.")
            else:
                st.info("No Volume SLAs configured in System Settings.")

            # --- Upgraded Top Level Metrics & Charts ---
            st.markdown("##### 💸 True CAC & Telecom Burn")
            total_spend = ops_df["Total_Campaign_Cost"].sum()
            total_conv = ops_df["KPI1-Conv."].sum()
            true_cac = total_spend / total_conv if total_conv > 0 else 0
            
            total_calls = ops_df["Calls"].sum()
            
            # Safely extract new metrics
            total_d = ops_df["D"].sum() if "D" in ops_df.columns else 0
            contact_rate = (total_d / total_calls * 100) if total_calls > 0 else 0

            # Phase 5: Dynamic Benchmark Deltas
            b_calls_delta, b_conv_delta, b_cr_delta = None, None, None
            if "benchmarks_df" in st.session_state and not st.session_state["benchmarks_df"].empty:
                b_df = st.session_state["benchmarks_df"].copy()
                
                # Filter by active sidebar selections
                if selected_brand != "All": b_df = b_df[b_df['brand'] == selected_brand]
                if selected_country != "All": b_df = b_df[b_df['country'].str.upper() == selected_country]
                if selected_lifecycle != "All": b_df = b_df[b_df['extracted_lifecycle'] == selected_lifecycle]
                if selected_segment != "All": b_df = b_df[b_df['extracted_segment'] == selected_segment]
                if selected_engagement != "All": b_df = b_df[b_df['extracted_engagement'] == selected_engagement]
                
                if not b_df.empty:
                    num_days = ops_df['ops_date'].nunique() if 'ops_date' in ops_df.columns else 1
                    
                    exp_daily_calls = b_df['avg_daily_calls'].sum()
                    exp_daily_convs = b_df['avg_daily_conversions'].sum()
                    exp_daily_delivs = b_df['avg_daily_deliveries'].sum()
                    
                    exp_tot_calls = exp_daily_calls * num_days
                    exp_tot_convs = exp_daily_convs * num_days
                    exp_cr = (exp_daily_delivs / exp_daily_calls * 100) if exp_daily_calls > 0 else 0
                    
                    b_calls_delta = f"{int(total_calls - exp_tot_calls):,} vs 6mo Avg"
                    b_conv_delta = f"{int(total_conv - exp_tot_convs):,} vs 6mo Avg"
                    b_cr_delta = f"{contact_rate - exp_cr:.1f}% vs 6mo Avg"
            
            total_new_records = ops_df["Records"].sum()
            o0, o1, o2, o3, o4, o5 = st.columns(6)
            o0.metric("📋 New Records", f"{int(total_new_records):,}")
            o1.metric("Total Telecom Spend", f"${total_spend:,.2f}")
            o2.metric("Total SIP Calls", f"{int(total_calls):,}", delta=b_calls_delta)
            o3.metric("Contact Rate (D Ratio)", f"{contact_rate:.1f}%", delta=b_cr_delta)
            o4.metric("Total Conversions", f"{int(total_conv):,}", delta=b_conv_delta)
            o5.metric("Global True CAC", f"${true_cac:,.2f}")
            
            st.markdown("---")
            st.markdown("### 📊 Campaign Performance Distributions")
            pie_col1, pie_col2, pie_col3 = st.columns(3)

            import plotly.express as px
            if not ops_df.empty:
                # Calculate aggregates for pies
                tot_d_plus = ops_df['D+'].sum() if 'D+' in ops_df.columns else 0
                tot_d_minus = ops_df['D-'].sum() if 'D-' in ops_df.columns else 0
                tot_d_neutral = ops_df['D'].sum() if 'D' in ops_df.columns else 0
                tot_deliveries = tot_d_plus + tot_d_minus + tot_d_neutral

                tot_na = ops_df['NA'].sum() if 'NA' in ops_df.columns else 0
                tot_wn = ops_df['WN'].sum() if 'WN' in ops_df.columns else 0
                tot_dnc = ops_df['DNC'].sum() if 'DNC' in ops_df.columns else 0
                tot_dx = ops_df['DX'].sum() if 'DX' in ops_df.columns else 0
                tot_t = ops_df['T'].sum() if 'T' in ops_df.columns else 0
                tot_issues = tot_wn + tot_dnc + tot_dx + tot_t

                with pie_col1:
                    st.markdown("**Overall Outcomes**")
                    pie_df1 = pd.DataFrame({'Outcome': ['Deliveries', 'No Answer', 'Issues'], 'Value': [tot_deliveries, tot_na, tot_issues]})
                    fig1 = px.pie(pie_df1, names='Outcome', values='Value', hole=0.4, color='Outcome', 
                                  color_discrete_map={'Deliveries': '#22c55e', 'No Answer': '#eab308', 'Issues': '#ef4444'})
                    fig1.update_layout(margin=dict(t=0, b=0, l=0, r=0), showlegend=False, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#00FF41")
                    st.plotly_chart(fig1, width='stretch')

                with pie_col2:
                    st.markdown("**Deliveries Breakdown**")
                    pie_df2 = pd.DataFrame({'Outcome': ['D+', 'D', 'D-'], 'Value': [tot_d_plus, tot_d_neutral, tot_d_minus]})
                    fig2 = px.pie(pie_df2, names='Outcome', values='Value', hole=0.4, color='Outcome',
                                  color_discrete_map={'D+': '#22c55e', 'D': '#16a34a', 'D-': '#86efac'})
                    fig2.update_layout(margin=dict(t=0, b=0, l=0, r=0), showlegend=False, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#00FF41")
                    st.plotly_chart(fig2, width='stretch')

                with pie_col3:
                    st.markdown("**Issues Breakdown**")
                    issue_names = ['WN', 'DNC', 'DX', 'T']
                    issue_vals = [tot_wn, tot_dnc, tot_dx, tot_t]
                    i_names, i_vals = zip(*[(n, v) for n, v in zip(issue_names, issue_vals) if v > 0]) if sum(issue_vals) > 0 else (['None'], [1])
                    pie_df3 = pd.DataFrame({'Outcome': list(i_names), 'Value': list(i_vals)})
                    fig3 = px.pie(pie_df3, names='Outcome', values='Value', hole=0.4, color='Outcome',
                                  color_discrete_map={'WN': '#ef4444', 'DNC': '#dc2626', 'DX': '#b91c1c', 'T': '#991b1b', 'None': '#333333'})
                    fig3.update_layout(margin=dict(t=0, b=0, l=0, r=0), showlegend=False, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#00FF41")
                    st.plotly_chart(fig3, width='stretch')

            st.markdown("---")
            
            st.markdown("### 📈 Daily SLA Trends & Performance")
            
            # --- SLA BREACH WATCHDOG ---
            if not ops_df.empty:
                try:
                    timeframe_days = (end_date_val - start_date_val).days + 1
                    
                    # 1. Fetch Contractual SLAs from DB & Aggregate by Client/Lifecycle (Cached)
                    slas_df = fetch_config_tables("SELECT client_name, lifecycle, SUM(monthly_minimum_records) as monthly_minimum_records FROM contractual_volumes WHERE monthly_minimum_records > 0 GROUP BY client_name, lifecycle")
                    
                    breaches = []
                    # 2. Check active clients in the current filtered view
                    active_clients = ops_df['ops_client'].unique()
                    
                    for client_target in active_clients:
                        client_slas = slas_df[slas_df['client_name'] == client_target]
                        if client_slas.empty: continue
                        
                        client_data = ops_df[ops_df['ops_client'] == client_target]
                        
                        for _, sla_row in client_slas.iterrows():
                            c_type = sla_row['lifecycle']  # e.g., 'RND' or 'WB'
                            aggregated_monthly_minimum = sla_row['monthly_minimum_records']
                            
                            # Filter data by campaign type (matching the string in campaign_name)
                            type_data = client_data[client_data['campaign_name'].str.contains(c_type, case=False, na=False)]
                            actual_vol = type_data['Records'].sum() if 'Records' in type_data.columns else type_data['records'].sum()
                            
                            # Calculate the Pro-Rated Minimum Target:
                            pro_rated_target = (aggregated_monthly_minimum / 30) * timeframe_days
                            
                            # Compare the pacing:
                            if actual_vol < pro_rated_target:
                                breaches.append(f"**{client_target} ({c_type})**: Delivered {int(actual_vol):,} / {pro_rated_target:,.0f} expected records for this timeframe (Monthly Goal: {int(aggregated_monthly_minimum):,}).")
                    
                    # 3. Render the Alert Box if breaches exist
                    if breaches:
                        st.warning(f"⚠️ **Contractual SLA Breach Detected (Current Timeframe - {timeframe_days} days)**")
                        for b in breaches:
                            st.markdown(f"- {b}")
                        st.markdown("---")
                except Exception as e:
                    pass # Fail silently if SLA table is empty or missing

            if not ops_df.empty and 'ops_date' in ops_df.columns:
                # Group by exact daily date directly from globally filtered ops_df
                latest_snaps = ops_df.sort_values('snapshot_timestamp').drop_duplicates(subset=['Campaign Name', 'ops_date'], keep='last') if 'snapshot_timestamp' in ops_df.columns else ops_df
                
                u_sigs = latest_snaps['campaign_signature'].unique() if 'campaign_signature' in latest_snaps.columns else []
                active_sig = u_sigs[0] if len(u_sigs) == 1 else None
                
                # Fetch Benchmark for active sig
                target_cac, target_conv, target_li = None, None, None
                if active_sig and not bench_df.empty:
                    b_row = bench_df[bench_df['campaign_signature'] == active_sig]
                    if not b_row.empty:
                        target_cac = b_row.iloc[0]['target_cac_usd']
                        target_conv = b_row.iloc[0]['target_conv_pct'] * 100 if pd.notnull(b_row.iloc[0]['target_conv_pct']) else None
                        target_li = b_row.iloc[0]['target_li_pct'] * 100 if pd.notnull(b_row.iloc[0]['target_li_pct']) else None
                
                # 1. Group by exact daily date using standard UI names
                agg_dict = {'Records': 'sum'}
                if 'Calls' in latest_snaps.columns:
                    agg_dict['Calls'] = 'sum'
                if 'KPI1-Conv.' in latest_snaps.columns:
                    agg_dict['KPI1-Conv.'] = 'sum'
                if 'KPI2-Login' in latest_snaps.columns:
                    agg_dict['KPI2-Login'] = 'sum'
                if 'LI%' in latest_snaps.columns:
                    agg_dict['LI%'] = 'mean'
                    
                daily_trends = latest_snaps.groupby('ops_date').agg(agg_dict).reset_index().sort_values('ops_date')

                def display_trend_charts(df_filtered):
                    if len(df_filtered) > 0:
                        # ---- FULL-WIDTH: Global Volume Trends ----
                        active_b = ops_df['ops_brand'].unique() if not ops_df.empty else []
                        vol_y_cols = ['Records']
                        
                        # Setup target overlays if exactly 1 brand
                        if len(active_b) == 1:
                            sla_min = 0
                            try:
                                slas_df = fetch_config_tables(f"SELECT monthly_minimum_records FROM contractual_volumes WHERE brand_code = '{active_b[0]}'")
                                if not slas_df.empty: 
                                    sla_min = slas_df['monthly_minimum_records'].sum()
                            except: pass
                            
                            if sla_min > 0:
                                df_filtered['SLA Minimum'] = sla_min / 30.0
                                vol_y_cols.append('SLA Minimum')
                                    
                        # Add Average Line
                        if 'Records' in df_filtered.columns and len(df_filtered) > 0:
                            df_filtered['Average Volume'] = df_filtered['Records'].mean()
                            vol_y_cols.append('Average Volume')
                                
                        fig_trend_vol = px.line(df_filtered, x='ops_date', y=vol_y_cols, 
                                                labels={'value': 'Volume', 'ops_date': 'Date', 'variable': 'Metric'}, title="Global Volume Trends")
                        fig_trend_vol.update_layout(
                            paper_bgcolor="rgba(0,0,0,0)", 
                            plot_bgcolor="rgba(0,0,0,0)", 
                            font_color="#00FF41",
                            legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5, title_text=""),
                            margin=dict(t=40, b=60, l=40, r=40)
                        )
                        
                        for trace in fig_trend_vol.data:
                            if 'SLA' in trace.name or 'Benchmark' in trace.name or 'Average' in trace.name:
                                trace.line.dash = 'dash'
                                
                            if 'Average Volume' in trace.name:
                                trace.line.color = 'rgba(255, 255, 255, 0.9)'
                                
                        st.plotly_chart(fig_trend_vol, width='stretch')
                        
                        # ---- Calculate efficiency percentages (vs New Data) ----
                        df_filtered['Conv%'] = ((df_filtered['KPI1-Conv.'] / df_filtered['Records']).replace([float('inf'), -float('inf')], 0).fillna(0) * 100).clip(upper=100) if 'KPI1-Conv.' in df_filtered.columns else 0
                        df_filtered['Logins%'] = ((df_filtered['KPI2-Login'] / df_filtered['Records']).replace([float('inf'), -float('inf')], 0).fillna(0) * 100).clip(upper=100) if 'KPI2-Login' in df_filtered.columns else 0
                        
                        # ---- 3-COLUMN ROW: Raw Volume, LI%, Conv% ----
                        tc1, tc2, tc3 = st.columns(3)
                        
                        # Chart 1: Raw KPI Volume (Bars)
                        with tc1:
                            import plotly.graph_objects as go
                            fig_raw = go.Figure()
                            if 'KPI1-Conv.' in df_filtered.columns:
                                fig_raw.add_trace(go.Bar(x=df_filtered['ops_date'], y=df_filtered['KPI1-Conv.'], name="Conversions", marker_color='rgba(34, 197, 94, 0.6)'))
                            if 'KPI2-Login' in df_filtered.columns:
                                fig_raw.add_trace(go.Bar(x=df_filtered['ops_date'], y=df_filtered['KPI2-Login'], name="Logins", marker_color='rgba(234, 179, 8, 0.6)'))
                            fig_raw.update_layout(
                                title="Raw KPI Volume",
                                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#00FF41",
                                barmode='group',
                                yaxis_title="Count", xaxis_title="",
                                margin=dict(t=40, b=60, l=40, r=20),
                                legend=dict(orientation="h", yanchor="bottom", y=-0.4, xanchor="center", x=0.5, title_text=""),
                                showlegend=True,
                                hoverlabel=dict(bgcolor="rgba(20,20,20,0.9)", font_color="#FFFFFF")
                            )
                            fig_raw.update_yaxes(showgrid=False)
                            st.plotly_chart(fig_raw, width='stretch')
                        
                        # Chart 2: Login % Trend
                        with tc2:
                            fig_li = go.Figure()
                            if 'KPI2-Login' in df_filtered.columns:
                                fig_li.add_trace(go.Scatter(x=df_filtered['ops_date'], y=df_filtered['Logins%'], name="Login %", mode='lines+markers', line=dict(color='#eab308'), hovertemplate='Login: %{y:.2f}%<extra></extra>'))
                                avg_login = df_filtered['Logins%'].mean()
                                fig_li.add_hline(y=avg_login, line_dash="dot", line_color="#fbbf24", opacity=0.5, annotation_text=f"Avg: {avg_login:.2f}%", annotation_position="bottom right", annotation_font_color="#fbbf24")
                            if target_li is not None:
                                fig_li.add_hline(y=target_li, line_dash="dash", line_color="#eab308", opacity=0.6, annotation_text=f"Benchmark: {target_li:.1f}%", annotation_position="top left", annotation_font_color="#eab308")
                            fig_li.update_layout(
                                title="Login % Trend",
                                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#00FF41",
                                yaxis_title="Login %", xaxis_title="",
                                margin=dict(t=40, b=60, l=40, r=20),
                                legend=dict(orientation="h", yanchor="bottom", y=-0.4, xanchor="center", x=0.5, title_text=""),
                                showlegend=True,
                                hoverlabel=dict(bgcolor="rgba(20,20,20,0.9)", font_color="#FFFFFF")
                            )
                            fig_li.update_yaxes(showgrid=False)
                            st.plotly_chart(fig_li, width='stretch')
                        
                        # Chart 3: Conversion % Trend
                        with tc3:
                            fig_conv = go.Figure()
                            fig_conv.add_trace(go.Scatter(x=df_filtered['ops_date'], y=df_filtered['Conv%'], name="Conv %", mode='lines+markers', line=dict(color='#22c55e'), hovertemplate='Conv: %{y:.2f}%<extra></extra>'))
                            avg_conv = df_filtered['Conv%'].mean()
                            fig_conv.add_hline(y=avg_conv, line_dash="dot", line_color="#4ade80", opacity=0.5, annotation_text=f"Avg: {avg_conv:.2f}%", annotation_position="bottom right", annotation_font_color="#4ade80")
                            if target_conv is not None:
                                fig_conv.add_hline(y=target_conv, line_dash="dash", line_color="#22c55e", opacity=0.6, annotation_text=f"Benchmark: {target_conv:.1f}%", annotation_position="top left", annotation_font_color="#22c55e")
                            fig_conv.update_layout(
                                title="Conversion % Trend",
                                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#00FF41",
                                yaxis_title="Conv %", xaxis_title="",
                                margin=dict(t=40, b=60, l=40, r=20),
                                legend=dict(orientation="h", yanchor="bottom", y=-0.4, xanchor="center", x=0.5, title_text=""),
                                showlegend=True,
                                hoverlabel=dict(bgcolor="rgba(20,20,20,0.9)", font_color="#FFFFFF")
                            )
                            fig_conv.update_yaxes(showgrid=False)
                            st.plotly_chart(fig_conv, width='stretch')
                    else:
                        st.info("Not enough data to display trend charts for the selected range.")

                # Render directly without tabs
                display_trend_charts(daily_trends.copy())

            # --- 🎯 Pitch vs. List Scorecard ---
            st.markdown("---")
            st.markdown("### 🎯 Pitch vs. List Scorecard")
            st.markdown("*Insight: Analyzes the raw list quality vs. the dial floor execution. Isolates script fatigue from telecom blocking and bad data.*")

            if not ops_df.empty:
                # Ensure previously unmapped columns default to 0 to prevent KeyError
                req_cols = ["Records", "Calls", "hlrv", "twoxrv", "D+", "d_neutral", "D-", "NA", "AM", "DNC", "DX", "WN", "T", "sa", "sd", "sf", "sp", "ev", "es", "ed", "ef", "eo", "ec", "D"]
                for c in req_cols:
                    if c not in ops_df.columns:
                        ops_df[c] = 0

                agg_dict = {c: 'sum' for c in req_cols}
                scorecard_df = ops_df.groupby("Core_Signature").agg(agg_dict).reset_index()
                # Net Records excludes HLRV & 2XRV
                scorecard_df["Net_Records"] = scorecard_df["Records"] - scorecard_df["hlrv"] - scorecard_df["twoxrv"]
                scorecard_df["Net_Records"] = scorecard_df["Net_Records"].apply(lambda x: max(x, 1)) # Prevent Div by Zero

                # Completion %
                scorecard_df["Gross_Completion"] = (scorecard_df["Calls"] / scorecard_df["Records"].replace(0, 1)).clip(0, 1)
                scorecard_df["Net_Completion"] = (scorecard_df["Calls"] / scorecard_df["Net_Records"]).clip(0, 1)

                # Quality Matrix
                # We use d_neutral as requested, or D if d_neutral is empty for backward compat.
                scorecard_df["Deliveries"] = scorecard_df["D+"] + scorecard_df["d_neutral"] + scorecard_df["D-"]
                scorecard_df["Issues"] = scorecard_df["AM"] + scorecard_df["DNC"] + scorecard_df["DX"] + scorecard_df["WN"] + scorecard_df["T"]

                # Render All Campaigns by Calls
                rendered_scorecard = scorecard_df.sort_values(by="Calls", ascending=False).copy()

                # Calculate percentages for rendering
                rendered_scorecard["Gross_Completion_%"] = rendered_scorecard["Gross_Completion"] * 100
                rendered_scorecard["Net_Completion_%"] = rendered_scorecard["Net_Completion"] * 100
                rendered_scorecard["Deliveries_%"] = (rendered_scorecard["Deliveries"] / rendered_scorecard["Calls"].replace(0, 1)) * 100
                rendered_scorecard["NA_%"] = (rendered_scorecard["NA"] / rendered_scorecard["Calls"].replace(0, 1)) * 100
                rendered_scorecard["Issues_%"] = (rendered_scorecard["Issues"] / rendered_scorecard["Calls"].replace(0, 1)) * 100

                # Render with color-coded styling
                def _style_deliveries(val):
                    if pd.isna(val): return ''
                    if val >= 20: return 'color: #4ade80; font-weight: bold;'
                    if val >= 15: return 'color: #facc15; font-weight: bold;'
                    if val >= 10: return 'color: #fb923c; font-weight: bold;'
                    return 'color: #f87171; font-weight: bold;'
                
                def _style_issues(val):
                    if pd.isna(val): return ''
                    if val > 10: return 'color: #f87171; font-weight: bold;'
                    return ''
                
                def _style_delivery_rate(val):
                    if pd.isna(val): return ''
                    if val >= 90: return 'color: #4ade80; font-weight: bold;'
                    if val >= 85: return 'color: #facc15; font-weight: bold;'
                    return 'color: #f87171; font-weight: bold;'
                
                def _style_engagement(val):
                    if pd.isna(val): return ''
                    if val >= 10: return 'color: #4ade80; font-weight: bold;'
                    if val >= 5: return 'color: #facc15; font-weight: bold;'
                    return 'color: #f87171; font-weight: bold;'

                # Calculate Email & SMS funnel percentages
                rendered_scorecard['Email Delivered'] = (rendered_scorecard['ed'] / (rendered_scorecard['ed'] + rendered_scorecard['ef']).replace(0, float('nan')) * 100).fillna(0)
                rendered_scorecard['Email Open'] = (rendered_scorecard['eo'] / rendered_scorecard['ed'].replace(0, float('nan')) * 100).fillna(0)
                rendered_scorecard['Email Clicked'] = (rendered_scorecard['ec'] / rendered_scorecard['eo'].replace(0, float('nan')) * 100).fillna(0)
                rendered_scorecard['SMS Delivered'] = (rendered_scorecard['sd'] / (rendered_scorecard['sd'] + rendered_scorecard['sp'] + rendered_scorecard['sf']).replace(0, float('nan')) * 100).fillna(0)

                display_sc = rendered_scorecard[['Core_Signature', 'Gross_Completion_%', 'Net_Completion_%', 'Calls', 'Deliveries_%', 'NA_%', 'Issues_%', 'Email Delivered', 'Email Open', 'Email Clicked', 'SMS Delivered']].copy()
                display_sc.rename(columns={
                    'Core_Signature': 'Campaign',
                    'Gross_Completion_%': 'Gross %',
                    'Net_Completion_%': 'Net %',
                    'Deliveries_%': 'Deliveries %',
                    'NA_%': 'No Answers %',
                    'Issues_%': 'Issues %',
                }, inplace=True)

                styled_sc = display_sc.style.format({
                    'Calls': '{:,.0f}',
                    'Deliveries %': '{:.1f}%',
                    'No Answers %': '{:.1f}%',
                    'Issues %': '{:.1f}%',
                    'Email Delivered': '{:.1f}%',
                    'Email Open': '{:.1f}%',
                    'Email Clicked': '{:.1f}%',
                    'SMS Delivered': '{:.1f}%',
                }).map(_style_deliveries, subset=['Deliveries %']
                ).map(_style_issues, subset=['Issues %']
                ).map(_style_delivery_rate, subset=['Email Delivered', 'SMS Delivered']
                ).map(_style_engagement, subset=['Email Open', 'Email Clicked'])

                st.dataframe(styled_sc, width='stretch', hide_index=True,
                    column_config={
                        "Gross %": st.column_config.ProgressColumn("Gross %", format="%.1f%%", min_value=0, max_value=100),
                        "Net %": st.column_config.ProgressColumn("Net %", format="%.1f%%", min_value=0, max_value=100),
                    }
                )


            # --- 📦 Client SLA Volume Fulfillment ---
            st.markdown("---")
            st.subheader("📦 Client SLA Volume Fulfillment")
            
            # 1. Extensible SLA Config Dictionary
            sla_targets = {
                "LeoVegas Group": {"WB": 6500, "ACQ": 2000, "RET": 2000}
            }
            default_sla_target = 5000

            if not ops_df.empty:
                sla_ops_df = ops_df.copy()
                
                # 2. Lifecycle Mapping Logic
                # Use the extracted_lifecycle directly from the database schema
                if "extracted_lifecycle" in sla_ops_df.columns:
                    sla_ops_df["SLA_Lifecycle"] = sla_ops_df["extracted_lifecycle"]
                else:
                    sla_ops_df["SLA_Lifecycle"] = "UNKNOWN"
                
                # Filter out UNKNOWN lifecycles as they don't count towards these specific SLAs
                sla_ops_df = sla_ops_df[sla_ops_df["SLA_Lifecycle"] != "UNKNOWN"]
                
                if not sla_ops_df.empty:
                    # Calculate number of days in the current slice to scale the monthly SLA
                    num_days_sla = sla_ops_df['ops_date'].nunique() if 'ops_date' in sla_ops_df.columns else 1
                    local_sla_scale_factor = max(num_days_sla / 30.0, 1.0) if num_days_sla >= 28 else (num_days_sla / 30.0)
                    
                    # 3. Group by Client and Lifecycle
                    sla_agg = sla_ops_df.groupby(["ops_client", "SLA_Lifecycle"]).agg({"Records": "sum"}).reset_index()
                    
                    # 4. Math & Target Mapping
                    def get_sla_target(row):
                        client = row["ops_client"]
                        lifecycle = row["SLA_Lifecycle"]
                        raw_target = default_sla_target
                        if client in sla_targets and lifecycle in sla_targets[client]:
                            raw_target = sla_targets[client][lifecycle]
                        
                        # Scale the monthly target to represent the current timeframe slice
                        return max(1, int(raw_target * local_sla_scale_factor))
                    
                    sla_agg["SLA Target"] = sla_agg.apply(get_sla_target, axis=1)
                    sla_agg["Fulfillment %"] = (sla_agg["Records"] / sla_agg["SLA Target"].replace(0, 1)) * 100
                    
                    # Rename columns for final display dataframe
                    sla_agg.rename(columns={
                        "ops_client": "Client",
                        "SLA_Lifecycle": "Lifecycle",
                        "Records": "Records Received"
                    }, inplace=True)
                    
                    # 5. Conditional Red/Green Styling
                    def style_fulfillment(val):
                        if pd.isna(val):
                            return ''
                        if val >= 100:
                            return 'color: #4ade80; font-weight: bold;' # Green text (Tailwind green-400)
                        else:
                            return 'color: #f87171; font-weight: bold;' # Red text (Tailwind red-400)
                    
                    styled_sla_df = sla_agg.style.format({
                        "Records Received": "{:,.0f}",
                        "SLA Target": "{:,.0f}",
                        "Fulfillment %": "{:.1f}%"
                    }).map(style_fulfillment, subset=["Fulfillment %"])
                    
                    # 6. Render
                    st.dataframe(styled_sla_df, width='stretch', hide_index=True)
                else:
                    st.info("No recognizable SLA lifecycles (WB, ACQ, RET, RND) found in the current dataset.")


            # --- Campaign Comparison Matrix ---
            st.markdown("---")
            st.markdown("### 🔍 Campaign Comparison Matrix")
            if not ops_df.empty:
                comp_df = ops_df.copy()
                
                if 'ops_date' in comp_df.columns:
                    try:
                        comp_df['Week'] = pd.to_datetime(comp_df['ops_date']).dt.isocalendar().week
                        comp_df['Month'] = pd.to_datetime(comp_df['ops_date']).dt.month
                    except:
                        comp_df['Week'] = 1
                        comp_df['Month'] = 1
                else:
                    comp_df['Week'] = 1
                    comp_df['Month'] = 1
                    
                group_by = st.selectbox("Compare Timeframe By:", ["Month", "Week", "Overall"])
                
                pivot_cols = ['Base Campaign']
                if group_by != "Overall":
                    pivot_cols.append(group_by)
                    
                matrix = comp_df.groupby(pivot_cols).agg({
                    'Records': 'sum',
                    'Calls': 'sum',
                    'KPI1-Conv.': 'sum',
                    'Total_Campaign_Cost': 'sum',
                    'D Ratio': 'mean'
                }).reset_index()
                
                matrix['True_CAC'] = matrix['Total_Campaign_Cost'] / matrix['KPI1-Conv.']
                matrix['True_CAC'] = matrix['True_CAC'].replace([float('inf'), -float('inf')], 0).fillna(0)
                matrix['D Ratio'] = matrix['D Ratio'] * 100
                
                st.dataframe(
                    matrix.sort_values(by=pivot_cols),
                    width='stretch', hide_index=True,
                    column_config={
                        "Records": st.column_config.NumberColumn("Total Records", format="%d"),
                        "Total_Campaign_Cost": st.column_config.NumberColumn("Total Spend", format="$%.2f"),
                        "True_CAC": st.column_config.NumberColumn("True CAC", format="$%.2f"),
                        "D Ratio": st.column_config.NumberColumn("Avg Contact Rate", format="%.2f%%")
                    }
                )
            else:
                st.info("No campaign data available for comparison.")

                
            st.markdown("---")
            st.markdown("##### 📋 Campaign True Cost Ledger")
            ledger_agg_cols = {'Records': 'sum', 'Calls': 'sum', 'Total_Campaign_Cost': 'sum', 'KPI1-Conv.': 'sum'}
            # Add disposition columns for Contact Rate calc
            for c in ['D', 'NA', 'AM', 'DNC', 'DX', 'WN', 'T']:
                if c in ops_df.columns: ledger_agg_cols[c] = 'sum'
            ledger_group = ['Core_Signature', 'ops_client', 'ops_brand']
            ledger_group = [c for c in ledger_group if c in ops_df.columns]
            ledger_df = ops_df.groupby(ledger_group).agg(ledger_agg_cols).reset_index()
            ledger_df['True_CAC'] = (ledger_df['Total_Campaign_Cost'] / ledger_df['KPI1-Conv.']).replace([float('inf'), -float('inf')], 0).fillna(0)
            
            # Conv % = Conversions / New Data
            ledger_df['Conv %'] = (ledger_df['KPI1-Conv.'] / ledger_df['Records'].replace(0, float('nan')) * 100).fillna(0)
            
            # Contact Rate = D / (D + NA + Issues) where Issues = AM + DNC + DX + WN + T
            if 'D' in ledger_df.columns and 'NA' in ledger_df.columns:
                issues = ledger_df.get('AM', 0) + ledger_df.get('DNC', 0) + ledger_df.get('DX', 0) + ledger_df.get('WN', 0) + ledger_df.get('T', 0)
                total = ledger_df['D'] + ledger_df['NA'] + issues
                ledger_df['Contact Rate'] = (ledger_df['D'] / total.replace(0, float('nan')) * 100).fillna(0)
            else:
                ledger_df['Contact Rate'] = 0.0
            
            ledger_df.rename(columns={'Core_Signature': 'Campaign Name'}, inplace=True)
            
            # Additional signature columns needed for joining benchmarks
            sig_cols = ["country", "extracted_lifecycle", "extracted_segment", "extracted_engagement"]
            display_cols = ["Campaign Name", "ops_client", "ops_brand", "Records", "Calls", "Contact Rate", "Conv %", "Total_Campaign_Cost", "True_CAC"]
            display_cols = [c for c in display_cols if c in ledger_df.columns]
                
            # Cross-reference with benchmarks
            if "benchmarks_df" in st.session_state and not st.session_state["benchmarks_df"].empty:
                b_df = st.session_state["benchmarks_df"].copy()
                b_df.rename(columns={'brand': 'ops_brand'}, inplace=True)
                
                # We need to make sure the merge keys exist in ledger_df
                merge_keys = ['ops_brand'] + [c for c in sig_cols if c in ledger_df.columns]
                
                ledger_df = pd.merge(ledger_df, b_df[merge_keys + ['avg_daily_true_cac']],
                                     on=merge_keys, how='left')
                ledger_df.rename(columns={'avg_daily_true_cac': 'Benchmark CAC'}, inplace=True)
                ledger_df['Benchmark CAC'] = ledger_df['Benchmark CAC'].fillna(0)
                ledger_df['CAC Delta'] = ledger_df['True_CAC'] - ledger_df['Benchmark CAC']
            else:
                ledger_df['Benchmark CAC'] = 0.0
                ledger_df['CAC Delta'] = 0.0

            # Drop signature columns for clean display
            final_display_df = ledger_df.drop(columns=[c for c in sig_cols + ['D', 'NA', 'AM', 'DNC', 'DX', 'WN', 'T', 'KPI1-Conv.'] if c in ledger_df.columns]).sort_values("True_CAC", ascending=False)

            def style_cac_delta(val):
                if pd.isna(val) or val == 0:
                    return ''
                if val > 0:
                    return 'color: #f87171; font-weight: bold;' # Red text (More expensive)
                else:
                    return 'color: #4ade80; font-weight: bold;' # Green text (Cheaper or equal)
            
            styled_ledger = final_display_df.style.format({
                    "Records": "{:,.0f}",
                    "Total_Campaign_Cost": "${:,.2f}",
                    "True_CAC": "${:,.2f}",
                    "Benchmark CAC": "${:,.2f}",
                    "CAC Delta": "${:,.2f}",
                    "Contact Rate": "{:.1f}%",
                    "Conv %": "{:.1f}%",
            }).map(style_cac_delta, subset=["CAC Delta"])
                
            st.dataframe(
                styled_ledger,
                width='stretch', hide_index=True,
                column_config={
                    "Records": st.column_config.NumberColumn("New Data"),
                    "Total_Campaign_Cost": st.column_config.NumberColumn("Total Spend"),
                    "True_CAC": st.column_config.NumberColumn("True CAC"),
                    "Benchmark CAC": st.column_config.NumberColumn("Benchmark CAC"),
                    "CAC Delta": st.column_config.NumberColumn("CAC Delta"),
                    "D Ratio": st.column_config.NumberColumn("Contact Rate")
                }
            )
        else:
            st.warning("⚠️ **No Operations Data Loaded**.")
            st.info("Please navigate to the **🗄️ Operations Ingestion** tab and upload your daily `CSV/XLSX` reports or trigger a CallsU API sync.")
