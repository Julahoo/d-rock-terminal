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
from datetime import datetime
from src.api_worker import run_historical_pull

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

from src.ingestion import load_all_data_from_uploads, load_campaign_data_from_uploads
from src.analytics import generate_monthly_summaries, generate_campaign_summaries, generate_cohort_matrix, generate_segmentation_summary, generate_both_business_summary, generate_time_series, generate_program_summary, generate_rfm_summary, generate_smart_narrative, generate_player_master_list, generate_retention_heatmap, generate_overlap_stats, generate_ltv_curves
from src.exporter import export_to_excel
from src.database import init_db, execute_query, engine
from sqlalchemy.exc import ProgrammingError

# ── Cached wrappers to prevent recomputation on Streamlit rerun ───────────
@st.cache_data(show_spinner=False)
def _cached_time_series(data):
    return generate_time_series(data)

@st.cache_data(show_spinner=False)
def _cached_rfm_summary(raw_df, target_month):
    return generate_rfm_summary(raw_df, target_month)

@st.cache_data(show_spinner=False)
def _cached_player_master_list(raw_df):
    return generate_player_master_list(raw_df)

@st.cache_data(show_spinner=False)
def _cached_retention_heatmap(raw_df):
    return generate_retention_heatmap(raw_df)

@st.cache_data(show_spinner=False)
def _cached_ltv_curves(raw_df):
    return generate_ltv_curves(raw_df)

@st.cache_data(show_spinner=False)
def _cached_monthly_summaries(df, start=None, end=None): 
    return generate_monthly_summaries(df, force_start=start, force_end=end)

@st.cache_data(show_spinner=False)
def _cached_cohort_matrix(df): return generate_cohort_matrix(df)

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

@st.cache_data(show_spinner=False)
def _get_master_excel_bytes(summary_df, cohort_matrices, segmentation, both_business, ops_df):
    from src.exporter import export_to_excel
    buf = export_to_excel(summary_df, cohort_matrices=cohort_matrices, segmentation_df=segmentation, both_business_df=both_business, ops_df=ops_df)
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
# This ensures all global dropdowns and legacy tabs populate instantly from the persistent database
try:
    import pandas as pd
    from src.database import engine as _hydrate_engine
    
    # Hydrate Operations Data
    try:
        ops_query = "SELECT * FROM ops_telemarketing_data"
        global_ops_df = pd.read_sql(ops_query, _hydrate_engine)
        if not global_ops_df.empty:
            st.session_state["ops_df"] = global_ops_df
    except Exception as e:
        pass # Handle gracefully if table is empty

    # Hydrate Financial Data
    try:
        fin_query = "SELECT * FROM raw_financial_data"
        global_fin_df = pd.read_sql(fin_query, _hydrate_engine)
        if not global_fin_df.empty:
            global_fin_df.rename(columns={"player_id": "id"}, inplace=True)
            st.session_state["financial_df"] = global_fin_df
    except Exception as e:
        pass # Handle gracefully if table is empty

except Exception as e:
    st.sidebar.warning(f"Could not sync database to RAM: {e}")
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
            submit = st.form_submit_button("Authenticate", use_container_width=True)
            
            if submit:
                from src.database import engine
                import pandas as pd
                import json
                
                with st.spinner("Authenticating and securely retrieving configuration..."):
                    try:
                        query = "SELECT * FROM users WHERE username = %(u)s AND password = %(p)s"
                        user_df = pd.read_sql(query, engine, params={"u": username, "p": password})
                        
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
    st.markdown("---")

    st.markdown("---")
    st.markdown("### 🧭 NAVIGATION")
    nav_options = ["📊 Dashboard"]
    if st.session_state.get("user_role") in ["Superadmin", "Operations"]:
        nav_options.append("📞 Operations")
    if st.session_state.get("user_role") in ["Superadmin", "Financial"]: 
        nav_options.append("🏦 Financial")
    if st.session_state.get("user_role") == "Superadmin":
        nav_options.append("⚙️ Admin")
        
    view_mode = st.radio("Go to:", nav_options)

    # --- 1. HYDRATE RAW DATA FROM DATABASE ---
    import pandas as pd
    from src.database import engine as _filter_engine
    
    # Always force-sync from the DB on load to prevent stale session states
    # especially after a successful Phase 23 injection bypassing the UI
    with st.spinner("Hydrating data from cluster..."):
        try: 
            st.session_state["raw_ops_df"] = pd.read_sql("SELECT * FROM ops_telemarketing_data", _filter_engine)
            st.session_state["raw_ops_snapshots_df"] = pd.read_sql("SELECT * FROM ops_telemarketing_snapshots", _filter_engine)
        except ProgrammingError as e:
            st.session_state["raw_ops_df"] = pd.DataFrame()
            st.session_state["raw_ops_snapshots_df"] = pd.DataFrame()
            st.warning("⚠️ The database is currently empty. Please navigate to the 🗄️ Operations Ingestion tab and upload your CSV files to initialize the schema.")
            st.stop()
        except Exception as e:
            st.error(f"FATAL: Could not read operations data from DB: {e}")
            print(f"FATAL READ_SQL ERROR: {e}")
            st.session_state["raw_ops_df"] = pd.DataFrame()
            st.session_state["raw_ops_snapshots_df"] = pd.DataFrame()
                
        if "raw_fin_df" not in st.session_state:
            try: 
                raw_fin = pd.read_sql("SELECT * FROM raw_financial_data", _filter_engine)
                if not raw_fin.empty:
                    raw_fin.rename(columns={"player_id": "id"}, inplace=True)
                st.session_state["raw_fin_df"] = raw_fin
            except: 
                st.session_state["raw_fin_df"] = pd.DataFrame()

    raw_ops = st.session_state["raw_ops_df"]
    raw_fin = st.session_state["raw_fin_df"]

    # --- 2. SIDEBAR GLOBAL FILTERS & RBAC ---
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🌍 GLOBAL FILTERS")
    
    # 1. Clean hidden whitespace to prevent Pandas filtering bugs
    if not raw_ops.empty: 
        raw_ops['ops_client'] = raw_ops['ops_client'].astype(str).str.strip()
        raw_ops['ops_brand'] = raw_ops['ops_brand'].astype(str).str.strip()
    if not raw_fin.empty and 'client' in raw_fin.columns:
        raw_fin['client'] = raw_fin['client'].astype(str).str.strip()
        raw_fin['brand'] = raw_fin['brand'].astype(str).str.strip()

    # 2. Extract Clients
    db_clients = set()
    if not raw_ops.empty: db_clients.update(raw_ops['ops_client'].unique())
    if not raw_fin.empty and 'client' in raw_fin.columns: db_clients.update(raw_fin['client'].unique())
    db_clients = sorted(list(db_clients))

    allowed = st.session_state.get("allowed_clients", ["All"])
    if "All" not in allowed:
        db_clients = [c for c in db_clients if c in allowed]

    client_options = ["All"] + db_clients if db_clients else ["All"]
    selected_client = st.sidebar.selectbox("🎯 Target Client", client_options)

    # 3. Extract Brands safely based on exact Client match
    db_brands = set()
    if selected_client == "All":
        if not raw_ops.empty: db_brands.update(raw_ops['ops_brand'].unique())
        if not raw_fin.empty and 'brand' in raw_fin.columns: db_brands.update(raw_fin['brand'].unique())
    else:
        if not raw_ops.empty: db_brands.update(raw_ops[raw_ops['ops_client'] == selected_client]['ops_brand'].unique())
        if not raw_fin.empty and 'brand' in raw_fin.columns: db_brands.update(raw_fin[raw_fin['client'] == selected_client]['brand'].unique())
        
    sorted_brands = sorted(list(db_brands))
    brand_options = ["All"] + sorted_brands if sorted_brands else ["All"]
    
    # UX Trick: Auto-select the brand if there is only 1 available for this client
    default_brand_index = 1 if len(sorted_brands) == 1 else 0
    selected_brand = st.sidebar.selectbox("🏷️ Target Brand", brand_options, index=default_brand_index)

    # 4. Extract Category and Segment using heuristics from Campaign Name
    selected_category = "All"
    selected_segment = "All"
    
    if not raw_ops.empty and 'campaign_name' in raw_ops.columns:
        CATEGORY_LIST = ['SPO', 'CAS', 'LIVE', 'ALL']
        SEGMENT_LIST = ['HIGH', 'MID', 'LOW', 'VIP', 'NA', 'AFF', 'COH1', 'COH2', 'COH3', 'COH4']
        
        def parse_tokens(x, target_list):
            if pd.isna(x): return None
            import re
            tokens = re.split(r'[-_ ]+', str(x).upper())
            for t in tokens:
                if t in target_list: return t
            return None
        
        if '__extracted_category' not in raw_ops.columns:
            raw_ops['__extracted_category'] = raw_ops['campaign_name'].apply(lambda x: parse_tokens(x, CATEGORY_LIST))
            raw_ops['__extracted_segment'] = raw_ops['campaign_name'].apply(lambda x: parse_tokens(x, SEGMENT_LIST))
            
        avail_categories = sorted([c for c in raw_ops['__extracted_category'].dropna().unique() if c])
        avail_segments = sorted([s for s in raw_ops['__extracted_segment'].dropna().unique() if s])
        
        if avail_categories:
            selected_category = st.sidebar.selectbox("📦 Target Category", ["All"] + avail_categories)
        if avail_segments:
            selected_segment = st.sidebar.selectbox("🎯 Target Segment", ["All"] + avail_segments)

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
    st.sidebar.radio("Quick Select", options, horizontal=True, key="date_preset", on_change=update_slider)

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
        if not filtered_ops.empty and '__extracted_category' in filtered_ops.columns:
            filtered_ops = filtered_ops[filtered_ops['__extracted_category'] == selected_category]
        if not filtered_ops_snapshots.empty and 'campaign_name' in filtered_ops_snapshots.columns:
            filtered_ops_snapshots = filtered_ops_snapshots[filtered_ops_snapshots['campaign_name'].str.upper().str.contains(selected_category)]
            
    if selected_segment != "All":
        if not filtered_ops.empty and '__extracted_segment' in filtered_ops.columns:
            filtered_ops = filtered_ops[filtered_ops['__extracted_segment'] == selected_segment]
        if not filtered_ops_snapshots.empty and 'campaign_name' in filtered_ops_snapshots.columns:
            filtered_ops_snapshots = filtered_ops_snapshots[filtered_ops_snapshots['campaign_name'].str.upper().str.contains(selected_segment)]

    # Apply Time Frame Filter
    if start_date_val and end_date_val:
        if not filtered_ops.empty and 'ops_date' in filtered_ops.columns:
            # We must use proper string comparisons for SQLite / Postgres dates. Since they are YYYY-MM-DD strings usually:
            filtered_ops = filtered_ops[(filtered_ops['ops_date'] >= start_date_str) & (filtered_ops['ops_date'] <= end_date_str)]
        if not filtered_ops_snapshots.empty and 'ops_date' in filtered_ops_snapshots.columns:
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
    
    if st.sidebar.button("🚪 Logout", use_container_width=True):
        st.session_state["authenticated"] = False
        st.session_state["user_role"] = None
        st.session_state["user_name"] = None
        st.session_state["allowed_clients"] = []
        st.rerun()

# ═══════════════════════════════════════════════════════════════════════════
#  System Settings View (Full-Screen, Superadmin Only)
# ═══════════════════════════════════════════════════════════════════════════
if view_mode == "⚙️ Admin":
    admin_mode = st.radio("Admin Modules:", ["🏢 Client Hub", "👥 User Management"], horizontal=True)
    
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
                    if c2.button("⚙️ Manage Profile", use_container_width=True):
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
                
                    st.dataframe(pd.DataFrame(health_records), use_container_width=True, hide_index=True)
                    
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
                                
                                st.dataframe(client_agg, use_container_width=True, hide_index=True)
                                
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
                                
                                st.dataframe(month_agg, use_container_width=True, hide_index=True)
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
            if c2.button("⬅️ Back to Hub", use_container_width=True):
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
                        
                        st.dataframe(completeness_df.sort_values('Month', ascending=False), use_container_width=True, hide_index=True)
                        
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
                                st.dataframe(daily_df.sort_values('Date', ascending=False), use_container_width=True, hide_index=True)
                    else:
                        st.info("No Operations data found for this client to evaluate completeness.")
                        
                except Exception as e:
                    st.error(f"Completeness evaluation error: {e}")

                st.markdown("---")
                st.markdown(f"#### 📥 Upload {client} Financials")
                st.markdown("*Upload the NGR/Deposits file directly for this client.*")
                fin_files = st.file_uploader("Upload Financial Files", accept_multiple_files=True, type=["csv", "xlsx"], key="client_fin_upload")
                if st.button("Run Financial Ingestion") and fin_files:
                    from src.ingestion import load_all_data_from_uploads
                    with st.spinner("Processing..."):
                        df, reg = load_all_data_from_uploads(fin_files)
                        if not df.empty:
                            st.session_state["registry"] = reg
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
                    st.dataframe(registry_df, use_container_width=True, hide_index=True)
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
                    st.dataframe(vol_df, use_container_width=True, hide_index=True)
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
                    st.dataframe(bench_df, use_container_width=True, hide_index=True)
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
            st.dataframe(users_df, use_container_width=True, hide_index=True)
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
                        else:
                            # Full UPSERT
                            execute_query(
                                """INSERT INTO users (username, password, role, name, allowed_clients) 
                                   VALUES (:u, :p, :r, :n, :ac) 
                                   ON CONFLICT (username) DO UPDATE SET 
                                   password = :p, role = :r, name = :n, allowed_clients = :ac""",
                                {"u": u_username, "p": u_password, "r": u_role, "n": u_name, "ac": json.dumps(u_clients)}
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

    st.stop()  # Don't render Main Workspace below

# 🌍 FINANCIAL DATA FILTERS (for legacy Financial/CRM tabs)
st.session_state["data_loaded"] = not _master_df.empty
selected_country = "All"
revenue_mode = st.sidebar.radio("Revenue Metric", ["GGR", "NGR"], horizontal=True) if not _master_df.empty else "GGR"
rev_col = "ggr" if revenue_mode == "GGR" else "ngr"

# ── ROUTED VIEWS ──
tab_map = {}
run_clicked = False

if view_mode == "📊 Dashboard":
    tabs = ["📊 Dashboard"]
    created_tabs = st.tabs(tabs)
    tab_map = dict(zip(tabs, created_tabs))
    with tab_map["📊 Dashboard"]:
        st.info("Dashboard centralized view is currently under construction. Please navigate to your dedicated workspace.")
    
elif view_mode == "📞 Operations":
    st.markdown("## 📞 Operations Workspace")
    tabs = ["📞 Operations Command", "🕵️ CRM Intelligence", "📈 Campaigns", "🗄️ Operations Ingestion"]
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
            st.dataframe(pd.DataFrame(grid_data), use_container_width=True, hide_index=True)
        
        # --- UPLOADERS ---
        st.markdown("---")
        st.markdown("#### 📁 File Dropzones")
        fin_files = st.file_uploader("Upload Financial Files (CSV/XLSX)", type=["csv", "xlsx"], accept_multiple_files=True)
        if st.button("Process Financial Data", use_container_width=True) and fin_files:
            with st.spinner("Saving securely to PostgreSQL..."):
                from src.ingestion import load_all_data_from_uploads
                df, reg = load_all_data_from_uploads(fin_files)
                st.session_state["registry"] = reg
                st.success("Successfully ingested to PostgreSQL!")
                st.rerun()

if "🗄️ Operations Ingestion" in tab_map:
    with tab_map["🗄️ Operations Ingestion"]:
        st.markdown("### 📡 OPERATIONS DATA INGESTION")
        st.markdown("*Upload CallsU or Telemarketing daily summaries here.*")

        st.markdown("### 📡 Automated CallsU API Sync")
        st.write("Fetch daily operations data directly from the CallsU servers in the background.")

        col_date1, col_date2, col_btn = st.columns([2, 2, 2])
        with col_date1:
            sync_start = st.date_input("Start Date")
        with col_date2:
            sync_end = st.date_input("End Date")

        with col_btn:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🚀 Trigger Background Sync", use_container_width=True):
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

        ops_files = st.file_uploader("Upload Ops Files (CSV/XLSX)", type=["csv", "xlsx"], accept_multiple_files=True)
        if st.button("Process Operations Data", use_container_width=True) and ops_files:
            with st.spinner("Saving securely to PostgreSQL..."):
                from src.ingestion import load_operations_data_from_uploads
                load_operations_data_from_uploads(ops_files)
                st.success("Successfully ingested to PostgreSQL!")
                st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
#  BI Dashboard — reads from session state
# ══════════════════════════════════════════════════════════════════════════════
if not _master_df.empty:
    df = _master_df
    # Auto-compute analytics using cached wrappers (instantaneous!)
    financial_summary = _cached_monthly_summaries(df, start=start_month, end=end_month)
    cohort_matrices = _cached_cohort_matrix(df)
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
        st.bar_chart(chart_data, use_container_width=True)

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
            use_container_width=True,
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
            st.dataframe(pd.DataFrame(b_fin_rows), use_container_width=True, hide_index=True)

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
                st.dataframe(pd.DataFrame(b_eoy_rows), use_container_width=True, hide_index=True)
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
            st.dataframe(pd.DataFrame(b_plr_rows), use_container_width=True, hide_index=True)

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
            st.dataframe(b_rfm, use_container_width=True, hide_index=True,
                         column_config={
                             "Tier": st.column_config.TextColumn("Tier"),
                             "Players": st.column_config.NumberColumn("Players", format="%d"),
                             "GGR": st.column_config.NumberColumn("GGR", format="$%.2f"),
                         })

        # ── Full Data Table ───────────────────────────────────────────────
        with st.expander(f"📋 {brand_key} — Full Financial Data ({len(bdf)} months)", expanded=False):
            st.dataframe(
                bdf,
                use_container_width=True,
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
                        use_container_width=True,
                    )

        # ── Cohort Retention Heatmap (Phase 18) ──────────────────────────
        st.markdown("---")
        st.markdown("#### > COHORT RETENTION HEATMAP_")
        brand_raw = df[df["brand"] == brand_key]
        heatmap_fig = _cached_retention_heatmap(brand_raw)
        if heatmap_fig is not None:
            st.plotly_chart(heatmap_fig, use_container_width=True, config={"scrollZoom": False})
        else:
            st.info("Not enough data to generate a retention heatmap.")

        # ── Cumulative LTV Curves ────────────────────────────────────
        st.markdown("---")
        st.markdown("#### > CUMULATIVE LTV TRAJECTORY_")
        st.markdown("*Insight: Tracks the cumulative revenue generation of player cohorts over time to determine break-even points and long-term value.*")
        ltv_fig = _cached_ltv_curves(brand_raw)
        if ltv_fig is not None:
            st.plotly_chart(ltv_fig, use_container_width=True, config={"scrollZoom": False})
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
                    use_container_width=True,
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
                    master_excel = _get_master_excel_bytes(financial_summary, cohort_matrices, segmentation, both_business, st.session_state.get("ops_df", pd.DataFrame()))
                    st.download_button("📥 Download Master Report (Fin + Ops)", data=master_excel, file_name=f"Master_Report_{selected_client}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")

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

                st.dataframe(pd.DataFrame(matrix_data), use_container_width=True, hide_index=True, column_config=cfg)

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
                st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": False})

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
                st.dataframe(pd.DataFrame(demo_data), use_container_width=True, hide_index=True, column_config=cfg_demo)

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
                    st.dataframe(pd.DataFrame(cf_data), use_container_width=True, hide_index=True, column_config=cfg_cf)
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
                    st.plotly_chart(fig_tree, use_container_width=True)
                
                # 2. The Market Leaderboard
                st.dataframe(
                    geo_df,
                    use_container_width=True,
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
                st.bar_chart(chart_data, use_container_width=True)

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
                    st.plotly_chart(fig_waterfall, use_container_width=True, config={"scrollZoom": False})

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
                            st.plotly_chart(fig_donut, use_container_width=True)

                        with aff2:
                            st.dataframe(
                                affinity_df,
                                use_container_width=True,
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
                    st.dataframe(filtered_both, use_container_width=True, hide_index=True)

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
                    use_container_width=True,
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
                    st.dataframe(pd.DataFrame(fin_rows), use_container_width=True, hide_index=True)
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
                    st.dataframe(pd.DataFrame(plr_rows), use_container_width=True, hide_index=True)

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
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Tier": st.column_config.TextColumn("Tier"),
                            "Players": st.column_config.NumberColumn("Players", format="%d"),
                            "GGR": st.column_config.NumberColumn("GGR", format="$%.2f"),
                        },
                    )

                # Full Both Business table
                with st.expander(f"📋 Both Business Summary ({len(filtered_both)} months)", expanded=True):
                    st.dataframe(
                        filtered_both,
                        use_container_width=True,
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
                                use_container_width=True,
                            )

                # ── Cohort Retention Heatmap (Phase 18) ──────────────────────
                st.markdown("---")
                st.markdown("#### > COHORT RETENTION HEATMAP_")
                heatmap_fig = _cached_retention_heatmap(_raw_df)
                if heatmap_fig is not None:
                    st.plotly_chart(heatmap_fig, use_container_width=True, config={"scrollZoom": False})
                else:
                    st.info("Not enough data to generate a retention heatmap.")

                # ── Cumulative LTV Curves ────────────────────────────────
                st.markdown("---")
                st.markdown("#### > CUMULATIVE LTV TRAJECTORY_")
                st.markdown("*Insight: Tracks the cumulative revenue generation of player cohorts over time to determine break-even points and long-term value.*")
                ltv_fig = _cached_ltv_curves(_raw_df)
                if ltv_fig is not None:
                    st.plotly_chart(ltv_fig, use_container_width=True, config={"scrollZoom": False})
                else:
                    st.info("Not enough data to generate LTV curves.")

                # ── Segmentation by Program ─────────────────────────────
                if program_summary is not None and not program_summary.empty:
                    st.markdown("---")
                    st.markdown("#### > SEGMENTATION BY PROGRAM_")
                    st.markdown("*Insight: Evaluates the financial efficiency and house edge (Margin) across different marketing programs (ACQ, RET, WB).*")
                    st.dataframe(
                        program_summary,
                        use_container_width=True,
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

                # --- 3.7 WHALE CONCENTRATION MATRIX (PARETO RISK) ---
                st.markdown("---")
                st.markdown("#### > 🐋 WHALE CONCENTRATION MATRIX (PARETO RISK)_")
                st.markdown("*Insight: Analyzes the revenue dependency on your top percentile VIPs to expose structural churn risk.*")

                from src.analytics import generate_pareto_distribution
                pareto_df = generate_pareto_distribution(_raw_df)

                if not pareto_df.empty:
                    p1, p2 = st.columns([1, 1])

                    with p1:
                        fig_pareto = px.bar(
                            pareto_df, 
                            y="Tier", 
                            x="Revenue_Share", 
                            orientation='h',
                            color="Tier",
                            color_discrete_map={
                                "Top 1% (Super Whales)": "#FF4444", 
                                "Next 4% (Core VIPs)": "#FFD700", 
                                "Next 15% (Mid-Tier)": "#1E90FF", 
                                "Bottom 80% (Casuals)": "#00FF41"
                            },
                            text_auto='.1f'
                        )
                        fig_pareto.update_traces(textposition='outside', texttemplate='%{x:.1f}%')
                        fig_pareto.update_layout(
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="rgba(0,0,0,0)",
                            font_color="#00FF41",
                            xaxis_title="Share of Total NGR (%)",
                            yaxis_title="",
                            showlegend=False,
                            margin=dict(l=0, r=0, t=10, b=0)
                        )
                        st.plotly_chart(fig_pareto, use_container_width=True)

                    with p2:
                        st.dataframe(
                            pareto_df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "Tier": st.column_config.TextColumn("Player Tier"),
                                "Player_Count": st.column_config.NumberColumn("Player Count", format="%d"),
                                "NGR_Generated": st.column_config.NumberColumn("NGR Generated", format="$%.2f"),
                                "Revenue_Share": st.column_config.NumberColumn("Revenue Share", format="%.2f%%")
                            }
                        )
                else:
                    st.info("Not enough profitable players to calculate a distribution.")

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
                    use_container_width=True,
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
                rfm = _cached_rfm_summary(raw_subset, b_latest)
                if rfm.empty: return [0] * len(tier_labels)
                return [int(rfm.loc[rfm.iloc[:, 0].str.contains(s, na=False, case=False), rfm.columns[1]].sum()) if rfm.iloc[:, 0].str.contains(s, na=False, case=False).any() else 0 for s in tier_search]

            vip_data = {"Tier": tier_labels, combined_label: _vip_snap(df)}
            for brand in active_brands:
                b_vip = _vip_snap(df[df["brand"] == brand])
                vip_data[brand] = b_vip if b_vip else [0] * len(tier_labels)

            cfg_vip = {"Tier": st.column_config.TextColumn("Tier"), combined_label: st.column_config.NumberColumn(combined_label, format="%d")}
            for brand in active_brands: cfg_vip[brand] = st.column_config.NumberColumn(brand, format="%d")
            st.dataframe(pd.DataFrame(vip_data), use_container_width=True, hide_index=True, column_config=cfg_vip)


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
        
            # Use the globally filtered raw data
            master_df = _cached_player_master_list(_raw_df)
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
                        use_container_width=True,
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
                            use_container_width=True,
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
                            use_container_width=True,
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
                            use_container_width=True,
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
                            st.plotly_chart(fig_vel, use_container_width=True)
                        
                        with v2:
                            st.dataframe(
                                vel_df,
                                use_container_width=True,
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
                        use_container_width=True,
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
                        use_container_width=True,
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
                    use_container_width=True,
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
                        use_container_width=True,
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
                        use_container_width=True,
                    )
                else:
                    st.info(f"No players in {selected_campaign}.")

    # ==========================================
    # 📈 TAB: CAMPAIGNS & SEGMENT ROI (Now correctly un-nested!)
    # ==========================================
    if "📈 Campaigns" in tab_map:
        with tab_map["📈 Campaigns"]:
            _raw_df = df.copy()  # <-- ADDED DECLARATION

            st.markdown("#### > 🎯 SEGMENT & CAMPAIGN ROI MATRIX_")
            st.markdown("*Insight: Evaluates the true profitability and player quality of distinct marketing segments and acquisition channels.*")

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
                        title="True Net Profit by Segment"
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
                    st.plotly_chart(fig_seg, use_container_width=True)
                    
                with s2:
                    st.dataframe(
                        segment_df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "segment": st.column_config.TextColumn("Marketing Segment"),
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
                st.info("No segment data available in the current slice.")

# ==========================================
# 📞 TAB: OPERATIONS COMMAND (Phase 3 - Telemarketing)
# ==========================================
if "📞 Operations Command" in tab_map:
    with tab_map["📞 Operations Command"]:
        st.markdown("#### > 📡 CALLSU & OPERATIONS COMMAND_")
        st.markdown("*Insight: Tracks True CAC, Lead Quality, and Contractual SLA Fulfillment.*")
        
        if "ops_df" in st.session_state and not st.session_state["ops_df"].empty:
            ops_df = st.session_state["ops_df"].copy()
            # Map DB column names back to UI-expected names
            ops_df.rename(columns={
                "campaign_name": "Campaign Name",
                "records": "Records",
                "total_cost": "Total_Campaign_Cost",
                "conversions": "KPI1-Conv.",
                "true_cac": "True_CAC",
                "calls": "Calls",
                "d_total": "D",
                "d_plus": "D+",
                "d_minus": "D-",
                "d_ratio": "D Ratio",
                "tech_issues": "T",
                "am": "AM",
                "dnc": "DNC",
                "na": "NA",
                "dx": "DX",
                "wn": "WN",
            }, inplace=True)

            # Fetch SLAs from persistent DB
            from src.database import engine as _db_engine
            try:
                vol_df = pd.read_sql("SELECT * FROM contractual_volumes", _db_engine)
            except:
                vol_df = pd.DataFrame()
                
            try:
                bench_df = pd.read_sql("SELECT * FROM granular_benchmarks", _db_engine)
            except:
                bench_df = pd.DataFrame()
            
            # Determine selected_client for the filename
            unique_clients = ops_df["ops_client"].unique()
            if len(unique_clients) == 1:
                selected_client = unique_clients[0]
            else:
                selected_client = "All" # Or prompt user to select if multiple

            ops_excel = _get_ops_excel_bytes(ops_df)
            st.download_button("📥 Download Operations Ledger", data=ops_excel, file_name=f"Operations_Ledger_{selected_client}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")

            st.markdown("---")
            st.markdown("##### ⚖️ SLA Fulfillment Tracker (Volume vs. Contract)")
            
            if not vol_df.empty:
                # Calculate number of days in the current slice to scale the monthly SLA
                num_days = ops_df['ops_date'].nunique() if 'ops_date' in ops_df.columns else 1
                sla_scale_factor = num_days / 30.0

                # Derive ops_lifecycle from Campaign Name (RND vs WB)
                ops_df['ops_lifecycle'] = ops_df['Campaign Name'].apply(
                    lambda x: 'WB' if '-WB' in str(x).upper() or '_WB' in str(x).upper() or ' WB' in str(x).upper() else (
                        'RND' if '-RND' in str(x).upper() or '_RND' in str(x).upper() or ' RND' in str(x).upper() else 'UNKNOWN'
                    )
                )
                
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
                    for _, row in merged_vol.iterrows():
                        # Scale the volume minimum by the time slice
                        target_min_records = max(1, int(row["monthly_minimum_records"] * sla_scale_factor))
                        
                        # Calculations
                        pct_complete = min(row["Actual_Records"] / target_min_records, 1.0) if target_min_records > 0 else 0
                        
                        # UI Rendering
                        st.markdown(f"**{row['client_name']} - {row['brand_code']} ({row['lifecycle']})** — *{num_days}-Day Target*")
                        
                        progress_color = "normal" if pct_complete >= 0.9 else "error"
                        st.progress(pct_complete, text=f"{int(row['Actual_Records']):,} / {target_min_records:,} Minimum Records Received (Scaled from {int(row['monthly_minimum_records']):,}/mo)")
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
            
            o1, o2, o3, o4, o5 = st.columns(5)
            o1.metric("Total Telecom Spend", f"${total_spend:,.2f}")
            o2.metric("Total SIP Calls", f"{int(total_calls):,}")
            o3.metric("Contact Rate (D Ratio)", f"{contact_rate:.1f}%")
            o4.metric("Total Conversions", f"{int(total_conv):,}")
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
                    st.plotly_chart(fig1, use_container_width=True)

                with pie_col2:
                    st.markdown("**Deliveries Breakdown**")
                    pie_df2 = pd.DataFrame({'Outcome': ['D+', 'D', 'D-'], 'Value': [tot_d_plus, tot_d_neutral, tot_d_minus]})
                    fig2 = px.pie(pie_df2, names='Outcome', values='Value', hole=0.4, color='Outcome',
                                  color_discrete_map={'D+': '#22c55e', 'D': '#16a34a', 'D-': '#86efac'})
                    fig2.update_layout(margin=dict(t=0, b=0, l=0, r=0), showlegend=False, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#00FF41")
                    st.plotly_chart(fig2, use_container_width=True)

                with pie_col3:
                    st.markdown("**Issues Breakdown**")
                    issue_names = ['WN', 'DNC', 'DX', 'T']
                    issue_vals = [tot_wn, tot_dnc, tot_dx, tot_t]
                    i_names, i_vals = zip(*[(n, v) for n, v in zip(issue_names, issue_vals) if v > 0]) if sum(issue_vals) > 0 else (['None'], [1])
                    pie_df3 = pd.DataFrame({'Outcome': list(i_names), 'Value': list(i_vals)})
                    fig3 = px.pie(pie_df3, names='Outcome', values='Value', hole=0.4, color='Outcome',
                                  color_discrete_map={'WN': '#ef4444', 'DNC': '#dc2626', 'DX': '#b91c1c', 'T': '#991b1b', 'None': '#333333'})
                    fig3.update_layout(margin=dict(t=0, b=0, l=0, r=0), showlegend=False, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#00FF41")
                    st.plotly_chart(fig3, use_container_width=True)

            st.markdown("---")
            
            # Fetch snapshots for historical trendlines (bypass start_date filter for 90/365 lookback)
            raw_snaps = st.session_state.get("raw_ops_snapshots_df", pd.DataFrame())
            if not raw_snaps.empty:
                try:
                    # Re-apply dimension filters (Client, Brand, Category, Segment)
                    if selected_client != "All" and 'ops_client' in raw_snaps.columns:
                        raw_snaps = raw_snaps[raw_snaps['ops_client'] == selected_client]
                    if selected_brand != "All" and 'ops_brand' in raw_snaps.columns:
                        raw_snaps = raw_snaps[raw_snaps['ops_brand'] == selected_brand]
                    if selected_category != "All" and 'campaign_name' in raw_snaps.columns:
                        raw_snaps = raw_snaps[raw_snaps['campaign_name'].str.upper().str.contains(selected_category)]
                    if selected_segment != "All" and 'campaign_name' in raw_snaps.columns:
                        raw_snaps = raw_snaps[raw_snaps['campaign_name'].str.upper().str.contains(selected_segment)]
                    # Apply upper-bound date filter only
                    end_str = end_date_val.strftime("%Y-%m-%d")
                    raw_snaps = raw_snaps[raw_snaps['ops_date'] <= end_str]
                except Exception as e:
                    pass
                snap_df = raw_snaps.copy()
            else:
                snap_df = ops_df.copy() # Fallback
            
            st.markdown("### 📈 Daily SLA Trends & Performance")
            
            # --- SLA BREACH WATCHDOG ---
            if not filtered_ops.empty:
                try:
                    timeframe_days = (end_date_val - start_date_val).days + 1
                    
                    # 1. Fetch Contractual SLAs from DB & Aggregate by Client/Lifecycle
                    slas_df = pd.read_sql("SELECT client_name, lifecycle, SUM(monthly_minimum_records) as monthly_minimum_records FROM contractual_volumes WHERE monthly_minimum_records > 0 GROUP BY client_name, lifecycle", _db_engine)
                    
                    breaches = []
                    # 2. Check active clients in the current filtered view
                    active_clients = filtered_ops['ops_client'].unique()
                    
                    for client_target in active_clients:
                        client_slas = slas_df[slas_df['client_name'] == client_target]
                        if client_slas.empty: continue
                        
                        client_data = filtered_ops[filtered_ops['ops_client'] == client_target]
                        
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

            if not snap_df.empty and 'ops_date' in snap_df.columns:
                # Group by exact daily date using the latest Snapshot record per campaign/date
                latest_snaps = snap_df.sort_values('snapshot_timestamp').drop_duplicates(subset=['campaign_name', 'ops_date'], keep='last') if 'snapshot_timestamp' in snap_df.columns else snap_df
                
                # Identify granular campaign signature for benchmark targeting
                def get_sig(c):
                    parts = c.replace("-", "_").split('_')
                    if len(parts) >= 3 and parts[-3].isdigit() and parts[-2].isdigit() and parts[-1].isdigit():
                        return "_".join(c.split('_')[:-1]) if len(c.split('_')) > 1 else c
                    if len(parts) >= 2 and parts[-2].isdigit() and parts[-1].isdigit():
                        return "_".join(c.split('_')[:-1]) if len(c.split('_')) > 1 else c
                    return c
                
                camp_col = 'Campaign Name' if 'Campaign Name' in latest_snaps.columns else 'campaign_name'
                latest_snaps['campaign_signature'] = latest_snaps[camp_col].apply(get_sig)
                u_sigs = latest_snaps['campaign_signature'].unique()
                active_sig = u_sigs[0] if len(u_sigs) == 1 else None
                
                # Fetch Benchmark for active sig
                target_cac, target_conv, target_li = None, None, None
                if active_sig and not bench_df.empty:
                    b_row = bench_df[bench_df['campaign_signature'] == active_sig]
                    if not b_row.empty:
                        target_cac = b_row.iloc[0]['target_cac_usd']
                        target_conv = b_row.iloc[0]['target_conv_pct'] * 100 if pd.notnull(b_row.iloc[0]['target_conv_pct']) else None
                        target_li = b_row.iloc[0]['target_li_pct'] * 100 if pd.notnull(b_row.iloc[0]['target_li_pct']) else None
                # Determine if we are using the fallback ops_df (which has already been renamed)
                is_fallback = 'Calls' in latest_snaps.columns
                
                c_calls = 'Calls' if is_fallback else 'calls'
                c_conv = 'KPI1-Conv.' if is_fallback else 'conversions'
                c_logins = 'KPI2-Login' if is_fallback else 'kpi2_logins'
                c_li = 'LI%' if is_fallback else 'li_pct'
                
                # 1. Group by exact daily date
                daily_trends = latest_snaps.groupby('ops_date').agg({
                    c_calls: 'sum',
                    c_conv: 'sum',
                    c_logins: 'sum' if c_logins in latest_snaps.columns else lambda x: 0,
                    c_li: 'mean' if c_li in latest_snaps.columns else lambda x: 0
                }).reset_index().sort_values('ops_date')
                
                # 2. Rename back to UI standard for Plotly and downstream efficiency math
                daily_trends.rename(columns={
                    c_calls: 'Records', 
                    c_conv: 'KPI1-Conv.',
                    c_logins: 'KPI2-Login',
                    c_li: 'LI%'
                }, inplace=True)

                # Helper function to generate and display trend charts
                def display_trend_charts(df_filtered, duration):
                    if len(df_filtered) > 0:
                        tc1, tc2 = st.columns(2)
                        with tc1:
                            # Volume Trends
                            active_b = filtered_ops['ops_brand'].unique() if not filtered_ops.empty else []
                            vol_y_cols = ['Records']
                            
                            # Setup target overlays if exactly 1 brand
                            if len(active_b) == 1:
                                sla_min = 0
                                try:
                                    slas_df = pd.read_sql(f"SELECT monthly_minimum_records FROM contractual_volumes WHERE brand_code = '{active_b[0]}'", _db_engine)
                                    if not slas_df.empty: 
                                        sla_min = slas_df['monthly_minimum_records'].sum()
                                except: pass
                                
                                if sla_min > 0:
                                    df_filtered['SLA Minimum'] = sla_min / 30.0
                                    vol_y_cols.append('SLA Minimum')
                                    
                            fig_trend_vol = px.line(df_filtered, x='ops_date', y=vol_y_cols, 
                                                    labels={'value': 'Volume', 'ops_date': 'Date', 'variable': 'Metric'}, title=f"{duration} Volume Trends")
                            fig_trend_vol.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#00FF41")
                            
                            for trace in fig_trend_vol.data:
                                if 'SLA' in trace.name or 'Benchmark' in trace.name:
                                    trace.line.dash = 'dash'
                                    
                            st.plotly_chart(fig_trend_vol, use_container_width=True)
                        with tc2:
                            # Efficiency Trends (Dual-Axis)
                            from plotly.subplots import make_subplots
                            import plotly.graph_objects as go
                            
                            df_filtered['Conv%'] = (df_filtered['KPI1-Conv.'] / df_filtered['Records']).replace([float('inf'), -float('inf')], 0).fillna(0) * 100
                            df_filtered['Logins%'] = (df_filtered['KPI2-Login'] / df_filtered['Records']).replace([float('inf'), -float('inf')], 0).fillna(0) * 100
                            
                            fig_trend_pct = make_subplots(specs=[[{"secondary_y": True}]])
                            
                            # Add bars for raw numbers (primary y-axis)
                            if 'KPI2-Login' in df_filtered.columns:
                                fig_trend_pct.add_trace(go.Bar(x=df_filtered['ops_date'], y=df_filtered['KPI2-Login'], name="Logins", marker_color='rgba(234, 179, 8, 0.4)', hovertemplate='Logins: %{y} (%{customdata:.2f}%)<extra></extra>', customdata=df_filtered['Logins%']), secondary_y=False)
                            if 'KPI1-Conv.' in df_filtered.columns:
                                fig_trend_pct.add_trace(go.Bar(x=df_filtered['ops_date'], y=df_filtered['KPI1-Conv.'], name="Conversions", marker_color='rgba(34, 197, 94, 0.4)', hovertemplate='Conversions: %{y} (%{customdata:.2f}%)<extra></extra>', customdata=df_filtered['Conv%']), secondary_y=False)
                                
                            # Add lines for percentages (secondary y-axis)
                            fig_trend_pct.add_trace(go.Scatter(x=df_filtered['ops_date'], y=df_filtered['Logins%'], name="Login %", mode='lines+markers', line=dict(color='#eab308'), hovertemplate='Login %: %{y:.2f}%<extra></extra>'), secondary_y=True)
                            fig_trend_pct.add_trace(go.Scatter(x=df_filtered['ops_date'], y=df_filtered['Conv%'], name="Conversion %", mode='lines+markers', line=dict(color='#22c55e'), hovertemplate='Conversion %: %{y:.2f}%<extra></extra>'), secondary_y=True)
                            
                            if target_conv is not None:
                                fig_trend_pct.add_trace(go.Scatter(x=df_filtered['ops_date'], y=[target_conv]*len(df_filtered), name="Target Conv%", mode='lines', line=dict(color='#22c55e', dash='dash'), hovertemplate='Target Conv: %{y:.2f}%<extra></extra>'), secondary_y=True)
                            if target_li is not None and 'LI%' in df_filtered.columns:
                                fig_trend_pct.add_trace(go.Scatter(x=df_filtered['ops_date'], y=[target_li]*len(df_filtered), name="Target LI%", mode='lines', line=dict(color='#eab308', dash='dash'), hovertemplate='Target LI: %{y:.2f}%<extra></extra>'), secondary_y=True)
                            
                            fig_trend_pct.update_layout(
                                title=f"{duration} Efficiency Trends",
                                paper_bgcolor="rgba(0,0,0,0)", 
                                plot_bgcolor="rgba(0,0,0,0)", 
                                font_color="#00FF41",
                                barmode='group',
                                hovermode='x unified',
                                margin=dict(t=40, b=20, l=40, r=40)
                            )
                            fig_trend_pct.update_yaxes(title_text="Volume (Raw)", secondary_y=False, showgrid=False)
                            fig_trend_pct.update_yaxes(title_text="Efficiency (%)", secondary_y=True, showgrid=False)
                            
                            st.plotly_chart(fig_trend_pct, use_container_width=True)
                    else:
                        st.info(f"Not enough data for {duration} trend.")

                # Calculate max date to base 7, 30, and 365 day periods off of
                max_date = pd.to_datetime(daily_trends['ops_date']).max()

                t1, t2, t3, t4 = st.tabs(["7-Day", "30-Day", "90-Day", "12-Month"])
                with t1:
                    seven_days_ago = max_date - pd.Timedelta(days=7)
                    df_7d = daily_trends[pd.to_datetime(daily_trends['ops_date']) >= seven_days_ago].copy()
                    display_trend_charts(df_7d, "7-Day")

                with t2:
                    thirty_days_ago = max_date - pd.Timedelta(days=30)
                    df_30d = daily_trends[pd.to_datetime(daily_trends['ops_date']) >= thirty_days_ago].copy()
                    display_trend_charts(df_30d, "30-Day")
                    
                with t3:
                    ninety_days_ago = max_date - pd.Timedelta(days=90)
                    df_90d = daily_trends[pd.to_datetime(daily_trends['ops_date']) >= ninety_days_ago].copy()
                    display_trend_charts(df_90d, "90-Day")
                    
                with t4:
                    twelve_months_ago = max_date - pd.Timedelta(days=365)
                    df_12m = daily_trends[pd.to_datetime(daily_trends['ops_date']) >= twelve_months_ago].copy()
                    
                    # Roll up to monthly granularity for cleaner 12-month charts
                    if len(df_12m) > 0:
                        df_12m['Month'] = pd.to_datetime(df_12m['ops_date']).dt.to_period('M').astype(str)
                        # --- SAFE MONTHLY ROLL-UP ---
                        # Group by Month using the newly renamed UI-facing columns inherited from daily_trends
                        monthly_trends = df_12m.groupby('Month').agg({
                            'Records': 'sum', 
                            'KPI1-Conv.': 'sum', 
                            'KPI2-Login': 'sum', 
                            'LI%': 'mean'
                        }).reset_index()
                        # Reuse renamed column axis
                        monthly_trends.rename(columns={'Month': 'ops_date'}, inplace=True)
                        display_trend_charts(monthly_trends, "12-Month")
                    else:
                        st.info("Not enough data for 12-Month trend.")

            # --- 🎯 Pitch vs. List Scorecard ---
            st.markdown("---")
            st.markdown("### 🎯 Pitch vs. List Scorecard")
            st.markdown("*Insight: Analyzes the raw list quality vs. the dial floor execution. Isolates script fatigue from telecom blocking and bad data.*")

            if not ops_df.empty:
                # Ensure previously unmapped columns default to 0 to prevent KeyError
                req_cols = ["Records", "Calls", "hlrv", "twoxrv", "D+", "d_neutral", "D-", "NA", "AM", "DNC", "DX", "WN", "T", "sa", "sd", "sf", "ev", "es", "ed", "eo", "ec", "D"]
                for c in req_cols:
                    if c not in ops_df.columns:
                        ops_df[c] = 0

                agg_dict = {c: 'sum' for c in req_cols}
                scorecard_df = ops_df.groupby("Campaign Name").agg(agg_dict).reset_index()

                # Calculate Engine Metrics
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

                # Render Dataframe
                st.dataframe(
                    rendered_scorecard, 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={
                        "Campaign Name": st.column_config.TextColumn("Campaign"),
                        "Calls": st.column_config.NumberColumn("Calls", format="%d"),
                        "Gross_Completion_%": st.column_config.ProgressColumn("Gross Completion", format="%.1f%%", min_value=0, max_value=100),
                        "Net_Completion_%": st.column_config.ProgressColumn("Net Completion", format="%.1f%%", min_value=0, max_value=100),
                        "Deliveries_%": st.column_config.NumberColumn("Deliveries", format="%.1f%%"),
                        "NA_%": st.column_config.NumberColumn("No Answers", format="%.1f%%"),
                        "Issues_%": st.column_config.NumberColumn("Issues", format="%.1f%%"),
                        "sa": st.column_config.NumberColumn("SMS", format="%d"),
                        "es": st.column_config.NumberColumn("Email", format="%d")
                    },
                    column_order=["Campaign Name", "Gross_Completion_%", "Net_Completion_%", "Deliveries_%", "NA_%", "Issues_%", "sa", "es"]
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
                
                # 2. Segment Mapping Logic
                def map_sla_segment(campaign_name):
                    c_upper = str(campaign_name).upper()
                    if "-WB" in c_upper or "_WB" in c_upper or " WB" in c_upper:
                        return "WB"
                    elif "-ACQ" in c_upper or "_ACQ" in c_upper or " ACQ" in c_upper:
                        return "ACQ"
                    elif "-RET" in c_upper or "_RET" in c_upper or " RET" in c_upper:
                        return "RET"
                    elif "-RND" in c_upper or "_RND" in c_upper or " RND" in c_upper:
                        return "WB"  # Map RND to WB functionally
                    return "UNKNOWN"

                sla_ops_df["SLA_Segment"] = sla_ops_df["Campaign Name"].apply(map_sla_segment)
                
                # Filter out UNKNOWN segments as they don't count towards these specific SLAs
                sla_ops_df = sla_ops_df[sla_ops_df["SLA_Segment"] != "UNKNOWN"]
                
                if not sla_ops_df.empty:
                    # Calculate number of days in the current slice to scale the monthly SLA
                    num_days_sla = sla_ops_df['ops_date'].nunique() if 'ops_date' in sla_ops_df.columns else 1
                    local_sla_scale_factor = max(num_days_sla / 30.0, 1.0) if num_days_sla >= 28 else (num_days_sla / 30.0)
                    
                    # 3. Group by Client and Segment
                    sla_agg = sla_ops_df.groupby(["ops_client", "SLA_Segment"]).agg({"Records": "sum"}).reset_index()
                    
                    # 4. Math & Target Mapping
                    def get_sla_target(row):
                        client = row["ops_client"]
                        segment = row["SLA_Segment"]
                        raw_target = default_sla_target
                        if client in sla_targets and segment in sla_targets[client]:
                            raw_target = sla_targets[client][segment]
                        
                        # Scale the monthly target to represent the current timeframe slice
                        return max(1, int(raw_target * local_sla_scale_factor))
                    
                    sla_agg["SLA Target"] = sla_agg.apply(get_sla_target, axis=1)
                    sla_agg["Fulfillment %"] = (sla_agg["Records"] / sla_agg["SLA Target"].replace(0, 1)) * 100
                    
                    # Rename columns for final display dataframe
                    sla_agg.rename(columns={
                        "ops_client": "Client",
                        "SLA_Segment": "Segment",
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
                    st.dataframe(styled_sla_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No recognizable SLA segments (WB, ACQ, RET, RND) found in the current dataset.")


            # --- Campaign Comparison Matrix ---
            st.markdown("---")
            st.markdown("### 🔍 Campaign Comparison Matrix")
            if not ops_df.empty:
                # Strip date suffix (e.g., _2025_02 or _2025-02-15) to aggregate generic campaign logic
                def strip_campaign_date(c):
                    parts = c.replace("-", "_").split('_')
                    # Check for YYYY_MM_DD
                    if len(parts) >= 3 and parts[-3].isdigit() and parts[-2].isdigit() and parts[-1].isdigit():
                        return "_".join(c.split('_')[:-1]) if len(c.split('_')) > 1 else c
                    # Check for YYYY_MM
                    if len(parts) >= 2 and parts[-2].isdigit() and parts[-1].isdigit():
                        return "_".join(c.split('_')[:-1]) if len(c.split('_')) > 1 else c
                    return c
                
                comp_df = ops_df.copy()
                comp_df['Base Campaign'] = comp_df['Campaign Name'].apply(strip_campaign_date)
                
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
                    use_container_width=True, hide_index=True,
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
            display_cols = ["Campaign Name", "ops_client", "ops_brand", "Records", "Calls", "D Ratio", "Total_Campaign_Cost", "KPI1-Conv.", "True_CAC"]
            # Only display columns that actually exist in the dataframe
            display_cols = [c for c in display_cols if c in ops_df.columns]
            
            ledger_df = ops_df[display_cols].copy()
            if "D Ratio" in ledger_df.columns:
                ledger_df["D Ratio"] = ledger_df["D Ratio"] * 100
                
            st.dataframe(
                ledger_df.sort_values("True_CAC", ascending=False),
                use_container_width=True, hide_index=True,
                column_config={
                    "Records": st.column_config.NumberColumn("Total Records", format="%d"),
                    "Total_Campaign_Cost": st.column_config.NumberColumn("Total Spend", format="$%.2f"),
                    "True_CAC": st.column_config.NumberColumn("True CAC", format="$%.2f"),
                    "D Ratio": st.column_config.NumberColumn("Contact Rate", format="%.2f%%")
                }
            )
        else:
            st.info("No CallsU operations data loaded. Upload an Internal Campaigns file in the Control Room.")

