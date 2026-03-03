"""
app.py – Streamlit Web App (Phase 6)
======================================
Web frontend for the Betting Financial Reports ETL pipeline.

Run with:  streamlit run app.py
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

from src.ingestion import load_all_data_from_uploads, load_campaign_data_from_uploads
from src.analytics import generate_monthly_summaries, generate_campaign_summaries, generate_cohort_matrix, generate_segmentation_summary, generate_both_business_summary, generate_time_series, generate_program_summary, generate_rfm_summary, generate_smart_narrative, generate_player_master_list, generate_retention_heatmap, generate_overlap_stats, generate_ltv_curves
from src.exporter import export_to_excel
from src.database import init_db, execute_query, engine

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

# ── Config ───────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)

BRANDS = ["latribet", "rojabet"]

# ── Page config ──────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Betting Financial Reports",
    page_icon="📊",
    layout="wide",
)

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
        fin_query = "SELECT * FROM financial_data"
        global_fin_df = pd.read_sql(fin_query, _hydrate_engine)
        if not global_fin_df.empty:
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

st.title("D-ROCK FINANCIAL TERMINAL v1.0")
st.caption("Upload CSVs → Run Pipeline → Download Intel.")

# ── 🔐 Enterprise Authentication & Data Security RBAC ──────────────────
USERS = {
    "superadmin": {"password": "123", "role": "Superadmin", "name": "Global Overlord", "allowed_clients": ["All"]},
    "admin_lv": {"password": "123", "role": "Admin", "name": "LeoVegas Admin", "allowed_clients": ["LeoVegas Group"]},
    "finance_off": {"password": "123", "role": "Finance", "name": "Offside Finance", "allowed_clients": ["Offside Gaming"]},
    "ops_agency": {"password": "123", "role": "Operations", "name": "Head of Ops", "allowed_clients": ["REL", "LIM", "SIM", "RHN"]}
}

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
                if username in USERS and USERS[username]["password"] == password:
                    st.session_state["authenticated"] = True
                    st.session_state["user_role"] = USERS[username]["role"]
                    st.session_state["user_name"] = USERS[username]["name"]
                    st.session_state["allowed_clients"] = USERS[username]["allowed_clients"]
                    st.rerun()
                else:
                    st.error("❌ Invalid username or password.")
    st.stop() # CRITICAL: Halts execution of the rest of the app until logged in

# ── Session State Initialization ───────────────────────────────────────
if "data_loaded" not in st.session_state:
    st.session_state["data_loaded"] = False

# ═══════════════════════════════════════════════════════════════════════════
#  Data Control Room & Pipeline Execution
# ═══════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("---")
    st.header("⚙️ System Controls")
    if "excel_buffer" in st.session_state and st.session_state["excel_buffer"] is not None:
        st.download_button(
            label="Download Excel Report",
            data=st.session_state["excel_buffer"],
            file_name="Summary_Data_Auto.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            use_container_width=True,
        )

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
    
    if "raw_ops_df" not in st.session_state:
        try: st.session_state["raw_ops_df"] = pd.read_sql("SELECT * FROM ops_telemarketing_data", _filter_engine)
        except: st.session_state["raw_ops_df"] = pd.DataFrame()
            
    if "raw_fin_df" not in st.session_state:
        try: st.session_state["raw_fin_df"] = pd.read_sql("SELECT * FROM financial_data", _filter_engine)
        except: st.session_state["raw_fin_df"] = pd.DataFrame()

    raw_ops = st.session_state["raw_ops_df"]
    raw_fin = st.session_state["raw_fin_df"]

    # --- 2. SIDEBAR GLOBAL FILTERS & RBAC ---
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

    # --- 3. APPLY FILTERS TO TABS ---
    filtered_ops = raw_ops.copy() if not raw_ops.empty else pd.DataFrame()
    filtered_fin = raw_fin.copy() if not raw_fin.empty else pd.DataFrame()

    if selected_client != "All":
        if not filtered_ops.empty: filtered_ops = filtered_ops[filtered_ops['ops_client'] == selected_client]
        if not filtered_fin.empty: filtered_fin = filtered_fin[filtered_fin['client'] == selected_client]

    if selected_brand != "All":
        if not filtered_ops.empty: filtered_ops = filtered_ops[filtered_ops['ops_brand'] == selected_brand]
        if not filtered_fin.empty: filtered_fin = filtered_fin[filtered_fin['brand'] == selected_brand]

    st.session_state["ops_df"] = filtered_ops
    st.session_state["financial_df"] = filtered_fin

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
if view_mode == "🏢 Client Hub":
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
        
        try:
            from src.database import engine
            import pandas as pd
            
            # Fetch base data for health calculations
            registry = pd.read_sql("SELECT client_name, brand_code FROM client_mapping", engine)
            slas = pd.read_sql("SELECT client_name, brand_code FROM contractual_slas", engine)
            ops = pd.read_sql("SELECT ops_client as client, MAX(ops_date) as last_ops FROM ops_telemarketing_data GROUP BY ops_client", engine)
            
            try: fin = pd.read_sql("SELECT client, MAX(month) as last_fin FROM financial_data GROUP BY client", engine)
            except: fin = pd.DataFrame(columns=['client', 'last_fin'])
            
            # Identify all unique clients
            all_clients = sorted(list(set(registry['client_name'].tolist() + ops['client'].tolist() + fin['client'].tolist())))
            
            if all_clients:
                # Router UI
                c1, c2, _ = st.columns([2, 1, 3])
                target_client = c1.selectbox("Select Client Profile to Manage:", all_clients)
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
                ops_data = pd.read_sql(f"SELECT ops_brand as brand, ops_date as date FROM ops_telemarketing_data WHERE ops_client = '{client}' GROUP BY ops_brand, ops_date", engine)
                try: fin_data = pd.read_sql(f"SELECT brand, month as date FROM financial_data WHERE client = '{client}' GROUP BY brand, month", engine)
                except: fin_data = pd.DataFrame(columns=['brand', 'date'])
                    
                brands_query = f"SELECT DISTINCT COALESCE(brand_name, brand_code) as brand FROM client_mapping WHERE client_name = '{client}'"
                all_brands = pd.read_sql(brands_query, engine)
                
                data_brands = pd.concat([ops_data[['brand']], fin_data[['brand']]]).drop_duplicates()
                if not data_brands.empty:
                    all_brands = pd.concat([all_brands, data_brands]).drop_duplicates(subset=['brand'])
                    
                all_brands_list = sorted(all_brands['brand'].dropna().unique().tolist())
                all_dates = sorted(list(set(ops_data['date'].dropna().tolist() + fin_data['date'].dropna().tolist())), reverse=True)
                
                if all_dates and all_brands_list:
                    col_tuples = []
                    for b in all_brands_list:
                        col_tuples.extend([(b, 'Fin'), (b, 'Ops')])
                        
                    # Months as Rows, Brands -> Fin/Ops as Columns
                    matrix_df = pd.DataFrame(index=all_dates, columns=pd.MultiIndex.from_tuples(col_tuples))
                    matrix_df.index.name = "Month"
                    
                    for d in all_dates:
                        ops_b = ops_data[ops_data['date'] == d]['brand'].tolist()
                        fin_b = fin_data[fin_data['date'] == d]['brand'].tolist()
                        
                        for b in all_brands_list:
                            matrix_df.loc[d, (b, 'Fin')] = '✅' if b in fin_b else '❌'
                            matrix_df.loc[d, (b, 'Ops')] = '✅' if b in ops_b else '❌'
                            
                    st.dataframe(matrix_df, use_container_width=True)
                else:
                    st.info(f"No ops or financial data found for {client}.")
            except Exception as e:
                st.error(f"Matrix error: {e}")

            st.markdown("---")
            st.markdown(f"#### 📥 Upload {client} Financials")
            st.markdown("*Upload the NGR/Deposits file directly for this client.*")
            fin_files = st.file_uploader("Upload Financial Files", accept_multiple_files=True, type=["csv", "xlsx"], key="client_fin_upload")
            if st.button("Run Financial Ingestion") and fin_files:
                from src.ingestion import load_financial_data_from_uploads
                with st.spinner("Processing..."):
                    df = load_financial_data_from_uploads(fin_files)
                    if not df.empty:
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
                    new_tag = st.text_input("Ops Tag (e.g., ROJB)")
                    new_brand = st.text_input("Brand Name (e.g., Rojabet)")
                    if st.form_submit_button("Register Tag"):
                        execute_query("INSERT INTO client_mapping (brand_code, brand_name, client_name) VALUES (:t, :b, :c) ON CONFLICT (brand_code) DO UPDATE SET brand_name = :b, client_name = :c", 
                                      {"t": new_tag.upper().strip(), "b": new_brand.strip(), "c": client})
                        
                        # Retroactively fix ops data since they are adding a tag
                        execute_query("UPDATE ops_telemarketing_data SET ops_client = :c, ops_brand = :b WHERE ops_client = 'UNKNOWN' AND ops_brand = :t",
                                      {"c": client, "b": new_brand.strip(), "t": new_tag.upper().strip()})
                        
                        if "unmapped_tags" in st.session_state and new_tag.upper().strip() in st.session_state.get("unmapped_tags", set()):
                            st.session_state["unmapped_tags"].remove(new_tag.upper().strip())
                        if "raw_ops_df" in st.session_state: del st.session_state["raw_ops_df"]
                            
                        st.success(f"Saved {new_tag.upper()}! Historical ops data fixed retroactively if orphaned.")
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
        # TAB 3: CONTRACTUAL SLAS
        # ==========================================
        with t_sla:
            st.markdown(f"### ⚖️ Contractual SLAs: {client}")
            try:
                sla_df = pd.read_sql(f"SELECT brand_code as \"Brand\", lifecycle as \"Lifecycle\", monthly_minimum_records as \"Min Records\", target_cac_usd as \"Target CAC ($)\", benchmark_conv_pct * 100 as \"Target Conv (%)\" FROM contractual_slas WHERE client_name = '{client}'", engine)
                st.dataframe(sla_df, use_container_width=True, hide_index=True)
            except Exception as e:
                st.info("No SLAs set.")
                
            c3, c4 = st.columns(2)
            with c3:
                st.markdown("#### ➕ Add SLA")
                with st.form("add_client_sla_form"):
                    sla_brand = st.text_input("Brand (e.g., Rojabet)")
                    sla_lifecycle = st.selectbox("Lifecycle", ["RND", "WB", "AFF", "ALL"])
                    sla_min = st.number_input("Monthly Min Records", min_value=0, step=100)
                    sla_cac = st.number_input("Target CAC ($)", min_value=0.0, step=0.5)
                    sla_conv = st.number_input("Benchmark Conv (%)", min_value=0.0, step=0.1)
                    if st.form_submit_button("Set SLA"):
                        execute_query("""INSERT INTO contractual_slas (client_name, brand_code, lifecycle, monthly_minimum_records, target_cac_usd, benchmark_conv_pct) 
                                         VALUES (:c, :b, :l, :m, :t, :p) ON CONFLICT (client_name, brand_code, lifecycle) 
                                         DO UPDATE SET monthly_minimum_records = :m, target_cac_usd = :t, benchmark_conv_pct = :p""",
                                      {"c": client, "b": sla_brand.strip(), "l": sla_lifecycle.upper(), "m": sla_min, "t": sla_cac, "p": sla_conv / 100.0})
                        st.success("SLA Saved!")
                        st.rerun()
            with c4:
                st.markdown("#### 🗑️ Delete SLA")
                with st.form("del_client_sla_form"):
                    del_sla_brand = st.text_input("Brand Name to Remove")
                    del_sla_lc = st.selectbox("Lifecycle to Remove", ["RND", "WB", "AFF", "ALL"])
                    if st.form_submit_button("Delete SLA"):
                        execute_query("DELETE FROM contractual_slas WHERE client_name = :c AND brand_code = :b AND lifecycle = :l", 
                                      {"c": client, "b": del_sla_brand.strip(), "l": del_sla_lc.upper()})
                        st.success("Deleted!")
                        st.rerun()
            
    st.stop()  # Don't render Main Workspace below

# 🌍 FINANCIAL DATA FILTERS (for legacy Financial/CRM tabs)
if st.session_state.get("data_loaded"):
    _secure_df = st.session_state["df"].copy()
    if "client" in _secure_df.columns:
        if "All" not in st.session_state["allowed_clients"]:
            _secure_df = _secure_df[_secure_df["client"].isin(st.session_state["allowed_clients"])]
else:
    _secure_df = pd.DataFrame()

unique_countries = list(_secure_df["country"].unique()) if not _secure_df.empty and "country" in _secure_df.columns else []
selected_country = "All"

revenue_mode = st.sidebar.radio("Revenue Metric", ["GGR", "NGR"], horizontal=True) if not _secure_df.empty else "GGR"
rev_col = "ggr" if revenue_mode == "GGR" else "ngr"

# TIME FRAME FILTER
unique_months = sorted(_secure_df["report_month"].unique().tolist()) if not _secure_df.empty and "report_month" in _secure_df.columns else []
if len(unique_months) > 1:
    start_month, end_month = st.select_slider(
        "Select Analysis Window",
        options=unique_months,
        value=(unique_months[0], unique_months[-1])
    )
elif len(unique_months) == 1:
    start_month, end_month = unique_months[0], unique_months[0]
else:
    start_month, end_month = None, None

# CREATE CENTRALIZED FILTERED DATAFRAME FOR ALL TABS
if st.session_state.get("data_loaded"):
    _master_df = _secure_df.copy()
    if start_month and end_month and "report_month" in _master_df.columns:
        _master_df = _master_df[(_master_df["report_month"] >= start_month) & (_master_df["report_month"] <= end_month)]
    if selected_client != "All" and "client" in _master_df.columns: _master_df = _master_df[_master_df["client"] == selected_client]
    if selected_brand != "All" and "brand" in _master_df.columns: _master_df = _master_df[_master_df["brand"] == selected_brand]
    if selected_country != "All" and "country" in _master_df.columns: _master_df = _master_df[_master_df["country"] == selected_country]
else:
    _master_df = pd.DataFrame()

# ── ROUTED VIEWS ──
tab_map = {}
run_clicked = False

if view_mode == "📊 Dashboard":
    tabs = ["📊 Executive Summary", "🕵️ CRM Intelligence", "📈 Campaigns"]
    created_tabs = st.tabs(tabs)
    tab_map = dict(zip(tabs, created_tabs))
    
elif view_mode == "📞 Operations":
    st.markdown("## 📞 Operations Workspace")
    tabs = ["🗄️ Ops Control Room", "📞 Operations Command"]
    created_tabs = st.tabs(tabs)
    tab_map = dict(zip(tabs, created_tabs))
    st.info("Operations Reports and Uploads will be consolidated here.")
    # Temporary mapping to not break existing code block dependencies below
    tab_map["🗄️ Data Control Room"] = tab_map["🗄️ Ops Control Room"] 
    
elif view_mode == "🏦 Financial":
    st.markdown("## 🏦 Financial Workspace")
    tabs = ["🏦 Financial Deep-Dive", "📥 Financial Ingestion"]
    created_tabs = st.tabs(tabs)
    tab_map = dict(zip(tabs, created_tabs))
    st.info("Financial Reports and NGR/Deposits Ingestion will be consolidated here.")

if "🗄️ Data Control Room" in tab_map or "🗄️ Ops Control Room" in tab_map:
    with tab_map.get("🗄️ Data Control Room", tab_map.get("🗄️ Ops Control Room")):
        st.markdown("### > SYSTEM STATUS: DETECTING DATA PACKETS...")
        if "unmapped_tags" in st.session_state and st.session_state["unmapped_tags"]:
            st.error(f"🚨 **Action Required: Unmapped Campaigns Detected!** The following Ops Tags are not assigned to a Client/Brand: `{', '.join(st.session_state['unmapped_tags'])}`. Go to **⚙️ System Settings** to link them.")
        st.markdown("---")
    
        st.markdown("#### 📅 COMPLIANCE & IMPORT GRID")
        st.markdown("*Insight: Tracks expected monthly deliveries against actual files.*")
    
        # --- ADD THIS: Load from disk if RAM was cleared by a browser refresh ---
        if "registry" not in st.session_state or st.session_state["registry"] is None:
            from src.ingestion import IngestionRegistry
            st.session_state["registry"] = IngestionRegistry.load()

        # --- DYNAMIC COMPLIANCE GRID ---
        registry = st.session_state.get("registry")
        if registry and registry._entries:
            all_months = set()
            for brand, months in registry._entries.items():
                all_months.update(months.keys())
        
            # Sort months chronologically
            sorted_months = sorted(list(all_months), reverse=True)
        
            grid_data = {"Month": sorted_months}
            for brand in sorted(registry._entries.keys()):
                statuses = []
                for m in sorted_months:
                    status = registry._entries[brand].get(m, {}).get("status", "MISSING")
                    statuses.append("🟢 IMPORTED" if status == "COMPLETE" else "🔴 PENDING")
                grid_data[f"{brand.title()} (Financials)"] = statuses
            
            st.dataframe(pd.DataFrame(grid_data), use_container_width=True, hide_index=True)
        
            # Check for gaps using the backend registry logic
            gaps = registry.missing_entries()
            if gaps:
                st.error(f"⚠️ DETECTED {len(gaps)} DATA GAP(S): Missing months detected in the timeline. Please upload them to ensure accurate Time-Series and YoY tracking.")
        else:
            st.info("Awaiting Data. Upload files below and click 'Run Analytics Pipeline' to populate the tracking grid.")

        st.markdown("---")
        st.markdown("#### 📥 DATA INGESTION ZONES")
    
        st.markdown("##### 📁 OFFSIDE GAMING")
        up_offside_fin = st.file_uploader("Financial CSV/XLSX (Latribet, Rojabet)", type=["csv", "xlsx"], accept_multiple_files=True, key="up_offside_fin")
    
        st.markdown("---")
        st.markdown("##### 📁 LEOVEGAS GROUP")
        up_leovegas_fin = st.file_uploader("Financial CSV/XLSX (Bet UK, BetMGM, LV)", type=["csv", "xlsx"], accept_multiple_files=True, key="up_leovegas_fin")

        st.markdown("---")
        st.markdown("##### 📁 INTERNAL CAMPAIGNS (CALLSU)")
        up_agency_ops = st.file_uploader("Daily Trends & CES Reports CSV/XLSX", type=["csv", "xlsx"], accept_multiple_files=True, key="up_agency_ops")


        run_clicked = st.button("🚀 Run Analytics Pipeline", type="primary", use_container_width=True)

if run_clicked:
    with st.status("⏳ Executing ETL Pipeline...", expanded=True) as status:
        
        # Combine the uploaded RAM buffers
        fin_files = (st.session_state.get("up_offside_fin") or []) + (st.session_state.get("up_leovegas_fin") or [])
        crm_files = [] # Legacy CRM files deprecated in favor of CallsU integration

        # ── Phase 2: Ingestion ───────────────────────────────────────────
        st.write("> Ingesting financial data...")
        df = pd.DataFrame()
        registry = None

        if fin_files:
            try:
                df, registry = load_all_data_from_uploads(fin_files)
                if not df.empty:
                    st.write(f"> Loaded {len(df):,} player records across {df['report_month'].nunique()} months.")
                else:
                    st.warning("No financial data found after processing uploads.")
            except Exception as exc:
                st.error(f"Ingestion failed: {exc}")
        else:
            st.info("No financial data uploaded. Skipping financial phases.")

        # ── Phase 5: Campaigns (Legacy) ───────────────────────────────────
        st.write("> Processing campaign data...")
        campaign_raw = load_campaign_data_from_uploads(crm_files)
        campaign_summary: pd.DataFrame | None = None

        if campaign_raw.empty:
            st.write("No campaign data found — skipping.")
        else:
            campaign_summary = generate_campaign_summaries(campaign_raw)
            st.write(f"Campaign summary: {len(campaign_summary)} rows.")

        # ── Phase 20: Agency Operations ──────────────────────────
        st.write("> Processing CallsU operations data...")
        from src.ingestion import load_operations_data_from_uploads
        agency_files = st.session_state.get("up_agency_ops") or []
        ops_df = load_operations_data_from_uploads(agency_files)
        if not ops_df.empty:
            st.write(f"Operations summary: {len(ops_df)} campaign rows parsed.")
        else:
            st.write("No operations data found — skipping.")
            
        # Save to session state
        st.session_state["ops_df"] = ops_df

        # ── Financial Analytics (only if financial data was loaded) ────────
        financial_summary = pd.DataFrame()
        both_business = pd.DataFrame()
        cohort_matrices = {}
        segmentation = pd.DataFrame()
        program_summary = pd.DataFrame()
        excel_buffer = None

        if not df.empty:
            st.write("> Computing financial summaries...")
            financial_summary = generate_monthly_summaries(df)

            st.write("> Building cohort retention matrices...")
            cohort_matrices = generate_cohort_matrix(df)

            st.write("> Building segmentation matrix...")
            segmentation = generate_segmentation_summary(df)

            st.write("> Building Both Business summary...")
            both_business = generate_both_business_summary(financial_summary)

            st.write("> Building program summary...")
            program_summary = generate_program_summary(df)

            st.write("> Generating Excel report...")
            excel_buffer = export_to_excel(
                financial_summary,
                campaign_df=campaign_summary,
                cohort_matrices=cohort_matrices,
                segmentation_df=segmentation,
                both_business_df=both_business,
            )

        status.update(label="✅ Pipeline complete! Report written.", state="complete", expanded=False)

    # Save to session state so dashboard survives reruns
    st.session_state["df"] = df
    st.session_state["registry"] = registry
    st.session_state["financial_summary"] = financial_summary
    st.session_state["campaign_summary"] = campaign_summary
    st.session_state["cohort_matrices"] = cohort_matrices
    st.session_state["segmentation"] = segmentation
    st.session_state["both_business"] = both_business
    st.session_state["program_summary"] = program_summary
    st.session_state["excel_buffer"] = excel_buffer
    st.session_state["data_loaded"] = True
    
    # Instantly show the grid
    st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
#  BI Dashboard — reads from session state
# ══════════════════════════════════════════════════════════════════════════════
if st.session_state["data_loaded"]:
    df = st.session_state["df"]
    registry = st.session_state["registry"]
    financial_summary = st.session_state["financial_summary"]
    campaign_summary = st.session_state["campaign_summary"]
    cohort_matrices = st.session_state["cohort_matrices"]
    segmentation = st.session_state["segmentation"]
    both_business = st.session_state["both_business"]
    program_summary = st.session_state["program_summary"]

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
    if "📊 Executive Summary" in tab_map:
        with tab_map["📊 Executive Summary"]:
            # ── System Diagnostic (Combined) ──────────────────────────────────
            if not both_business.empty:
                exec_bb = both_business.iloc[-1]
                exec_ts = _cached_time_series(both_business)
                exec_ts_m = exec_ts["monthly"]

                if not exec_ts_m.empty:
                    exec_latest = exec_ts_m.iloc[-1]
                    combined_fin = financial_summary[
                        financial_summary["brand"] == "Combined"
                    ].sort_values("month").iloc[-1]
                    e_whale = float(combined_fin.get("top_10_pct_ggr_share", 0))
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

                latest_month = both_business["month"].max()

                _mom_map = {
                    "Turnover": "total_handle", "GGR": "ggr", "Margin %": "hold_pct",
                    "Revenue (15%)": "revenue_share_deduction", "Conversions": "conversions",
                    "Turnover / Player": "turnover_per_player", "Whale Risk %": None,
                }

                def _brand_snapshot(brand_name: str) -> dict:
                    bdata = financial_summary[(financial_summary["brand"] == brand_name) & (financial_summary["month"] == latest_month)]
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
                    if brand_ts_m.empty: return ["-"] * len(metrics_list)
                    latest = brand_ts_m.iloc[-1]
                    return [f"{latest.get(f'{_mom_map.get(m)}_mom_pct'):+.1f}%" if pd.notna(latest.get(f"{_mom_map.get(m)}_mom_pct")) else "-" for m in metrics_list]

                def _bb_mom() -> list:
                    if exec_ts_m.empty: return ["-"] * len(metrics_list)
                    bb_ts_map = {"Turnover": "turnover", "GGR": "ggr", "Margin %": "margin", "Revenue (15%)": "revenue_share_deduction", "Conversions": "conversions", "Turnover / Player": "turnover_per_player"}
                    return [f"{exec_latest.get(f'{bb_ts_map.get(m)}_mom_pct'):+.1f}%" if pd.notna(exec_latest.get(f"{bb_ts_map.get(m)}_mom_pct")) else "-" for m in metrics_list]

                def _brand_yoy(brand_name: str) -> list:
                    bdata = financial_summary[financial_summary["brand"] == brand_name].sort_values("month")
                    if bdata.empty: return ["-"] * len(metrics_list)
                    brand_ts_m = _cached_time_series(bdata).get("monthly", pd.DataFrame())
                    if brand_ts_m.empty: return ["-"] * len(metrics_list)
                    latest = brand_ts_m.iloc[-1]
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
            
                # Expanded color palette for 6+ brands
                colors = ["#FF4444", "#00FF41", "#1E90FF", "#FFD700", "#FF1493", "#9400D3"]
            
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
                    bdata = financial_summary[(financial_summary["brand"] == brand) & (financial_summary["month"] == latest_month)]
                    demo_data[brand] = [int(bdata.iloc[0].get(col, 0)) if not bdata.empty else 0 for _, col in demo_metrics]
            
                cfg_demo = {"Metric": st.column_config.TextColumn("Metric"), combined_label: st.column_config.NumberColumn(combined_label, format="%d")}
                for brand in active_brands: cfg_demo[brand] = st.column_config.NumberColumn(brand, format="%d")
                st.dataframe(pd.DataFrame(demo_data), use_container_width=True, hide_index=True, column_config=cfg_demo)

                # ── Cross-Brand VIP Health ────────────────────────────────────
                st.markdown("---")
                st.markdown("#### > CROSS-BRAND VIP HEALTH_")
                tier_labels, tier_search = ["True VIPs", "Churn Risk VIPs", "Casuals"], ["True VIP", "Churn Risk", "Casual"]

                def _vip_snap(raw_subset):
                    rfm = _cached_rfm_summary(raw_subset, latest_month)
                    if rfm.empty: return [0] * len(tier_labels)
                    return [int(rfm.loc[rfm.iloc[:, 0].str.contains(s, na=False, case=False), rfm.columns[1]].sum()) if rfm.iloc[:, 0].str.contains(s, na=False, case=False).any() else 0 for s in tier_search]

                vip_data = {"Tier": tier_labels, combined_label: _vip_snap(df)}
                for brand in active_brands:
                    vip_data[brand] = _vip_snap(df[df["brand"] == brand])

                cfg_vip = {"Tier": st.column_config.TextColumn("Tier"), combined_label: st.column_config.NumberColumn(combined_label, format="%d")}
                for brand in active_brands: cfg_vip[brand] = st.column_config.NumberColumn(brand, format="%d")
                st.dataframe(pd.DataFrame(vip_data), use_container_width=True, hide_index=True, column_config=cfg_vip)

                # ── Cross-Brand Cash Flow & Promo ─────────────────────────────
                if "LeoVegas Group" in df["client"].unique():
                    st.markdown("---")
                    st.markdown("#### > CASH FLOW & PROMO EFFICIENCY_")
                    st.markdown("*Insight: Tracks actual liquidity (Net Deposits) vs. the Bonus Cost required to acquire the revenue.*")

                    cf_metrics = [("Net Deposits", "net_deposits"), ("Total Deposits", "deposits"), ("Withdrawals", "withdrawals"), ("Bonus Cost", "bonus_total")]

                    cf_data = {"Metric": [label for label, _ in cf_metrics], combined_label: [float(exec_bb.get(col, 0)) for _, col in cf_metrics]}
                    for brand in active_brands:
                        bdata = financial_summary[(financial_summary["brand"] == brand) & (financial_summary["month"] == latest_month)]
                        cf_data[brand] = [float(bdata.iloc[0].get(col, 0)) if not bdata.empty else 0.0 for _, col in cf_metrics]

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

                # ── Cross-Brand Cannibalization ──────────────────────────────
                st.markdown("---")
                st.markdown("#### > CROSS-BRAND CANNIBALIZATION (ALL-TIME)_")
                st.markdown("*Insight: Identifies players active on both platforms to expose duplicate customer acquisition costs and shared revenue dependency.*")

                overlap = generate_overlap_stats(df)
                ov1, ov2 = st.columns(2)
                with ov1:
                    st.metric("Shared Players (Overlap)", f"{overlap['overlap_count']:,}")
                with ov2:
                    st.metric("Shared Lifetime GGR", f"${overlap['overlap_ggr']:,.2f}")
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

            st.markdown("#### > VIP & RISK LEADERBOARDS_")
            st.caption(f"Currently viewing CRM targets for: {selected_client} | {selected_brand} | {selected_country}")
        
            # Use the globally filtered raw data
            _raw_df = _master_df.copy()
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
                    slas_df = pd.read_sql("SELECT * FROM contractual_slas", _db_engine)
                except:
                    slas_df = pd.DataFrame()
                
                st.markdown("---")
                st.markdown("##### ⚖️ SLA Fulfillment Tracker (Volume vs. Contract)")
                
                if not slas_df.empty:
                    # Aggregate actuals to match SLAs
                    actuals = ops_df.groupby(["ops_client", "ops_brand", "ops_lifecycle"]).agg(
                        Actual_Calls=("Calls", "sum"),
                        Actual_Convs=("KPI1-Conv.", "sum"),
                        Total_Spend=("Total_Campaign_Cost", "sum")
                    ).reset_index()
                    
                    # Merge uploaded data with DB rules
                    merged_sla = pd.merge(
                        actuals, slas_df, 
                        left_on=["ops_client", "ops_brand", "ops_lifecycle"], 
                        right_on=["client_name", "brand_code", "lifecycle"], 
                        how="inner"
                    )
                    
                    if not merged_sla.empty:
                        for _, row in merged_sla.iterrows():
                            # Calculations
                            pct_complete = min(row["Actual_Calls"] / row["monthly_minimum_records"], 1.0) if row["monthly_minimum_records"] > 0 else 0
                            actual_cac = row["Total_Spend"] / row["Actual_Convs"] if row["Actual_Convs"] > 0 else 0
                            target_cac = row["target_cac_usd"]
                            
                            actual_conv_pct = (row["Actual_Convs"] / row["Actual_Calls"] * 100) if row["Actual_Calls"] > 0 else 0
                            target_conv_pct = row["benchmark_conv_pct"] * 100 if pd.notnull(row["benchmark_conv_pct"]) else 0
                            
                            # UI Rendering
                            st.markdown(f"**{row['client_name']} - {row['brand_code']} ({row['lifecycle']})**")
                            c1, c2, c3 = st.columns([2, 1, 1])
                            
                            with c1:
                                # Determine color based on progress (Red if under 50%, Yellow if under 90%, Green if 90%+)
                                progress_color = "normal" if pct_complete >= 0.9 else "error"
                                st.progress(pct_complete, text=f"{int(row['Actual_Calls']):,} / {int(row['monthly_minimum_records']):,} Minimum Calls Dialed")
                            
                            with c2:
                                delta_cac = actual_cac - target_cac
                                st.metric("True CAC", f"${actual_cac:.2f}", delta=f"${delta_cac:.2f} vs Target", delta_color="inverse")
                            
                            with c3:
                                delta_conv = actual_conv_pct - target_conv_pct
                                st.metric("Conversion Rate", f"{actual_conv_pct:.2f}%", delta=f"{delta_conv:.2f}% vs Target", delta_color="normal")
                                
                        st.markdown("---")
                    else:
                        st.info("No active SLAs match the currently loaded operations data. Add them in System Settings.")
                else:
                    st.info("No SLAs configured in System Settings.")

                # --- Upgraded Top Level Metrics & Charts ---
                st.markdown("##### 💸 True CAC & Telecom Burn")
                total_spend = ops_df["Total_Campaign_Cost"].sum()
                total_conv = ops_df["KPI1-Conv."].sum()
                true_cac = total_spend / total_conv if total_conv > 0 else 0
                
                total_calls = ops_df["Calls"].sum()
                
                # Safely extract new funnel metrics (fallback to 0 if not present in older data)
                total_d = ops_df["D"].sum() if "D" in ops_df.columns else 0
                total_d_plus = ops_df["D+"].sum() if "D+" in ops_df.columns else 0
                contact_rate = (total_d / total_calls * 100) if total_calls > 0 else 0
                
                o1, o2, o3, o4, o5 = st.columns(5)
                o1.metric("Total Telecom Spend", f"${total_spend:,.2f}")
                o2.metric("Total SIP Calls", f"{int(total_calls):,}")
                o3.metric("Contact Rate (D Ratio)", f"{contact_rate:.1f}%")
                o4.metric("Total Conversions", f"{int(total_conv):,}")
                o5.metric("Global True CAC", f"${true_cac:,.2f}")
                
                st.markdown("---")
                v1, v2 = st.columns([1, 1])
                
                with v1:
                    # 1. The Delivery Funnel
                    import plotly.express as px
                    funnel_data = dict(
                        Stage=["1. Total Dialed", "2. Delivered / Answered (D)", "3. Interested (D+)", "4. Converted (KPI1)"],
                        Volume=[total_calls, total_d, total_d_plus, total_conv]
                    )
                    fig_funnel = px.funnel(funnel_data, x='Volume', y='Stage', title="The Delivery Funnel (Drop-off Analysis)")
                    fig_funnel.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#00FF41")
                    fig_funnel.update_traces(marker=dict(color=["#4444FF", "#00AAFF", "#00FF88", "#00FF41"]))
                    st.plotly_chart(fig_funnel, use_container_width=True)
                    
                with v2:
                    # 2. Expanded Wasted SIP Dials Pie Chart
                    safe_sum = lambda col: ops_df[col].sum() if col in ops_df.columns else 0
                    decay_data = {
                        "Disposition": ["DNC (Do Not Call)", "WN (Wrong Number)", "NA (No Answer)", "AM (Voicemail)", "DX (Disconnected)", "T (Tech Issues)"],
                        "Volume": [safe_sum("DNC"), safe_sum("WN"), safe_sum("NA"), safe_sum("AM"), safe_sum("DX"), safe_sum("T")]
                    }
                    # Filter out zeros to keep the pie chart clean
                    filtered_decay = pd.DataFrame(decay_data)
                    filtered_decay = filtered_decay[filtered_decay["Volume"] > 0]
                    
                    if not filtered_decay.empty:
                        fig_decay = px.pie(filtered_decay, names="Disposition", values="Volume", hole=0.5, title="Lead Quality Decay (Wasted Dials)", 
                                           color_discrete_sequence=["#FF4444", "#FFA500", "#555555", "#888888", "#AA0000", "#FF00FF"])
                        fig_decay.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="#00FF41")
                        st.plotly_chart(fig_decay, use_container_width=True)
                    else:
                        st.info("No disposition data available in the current dataset to render the Decay chart.")
                    
                st.markdown("##### 📋 Campaign True Cost Ledger")
                display_cols = ["Campaign Name", "ops_client", "ops_brand", "Calls", "D Ratio", "Total_Campaign_Cost", "KPI1-Conv.", "True_CAC"]
                # Only display columns that actually exist in the dataframe
                display_cols = [c for c in display_cols if c in ops_df.columns]
                
                st.dataframe(
                    ops_df[display_cols].sort_values("True_CAC", ascending=False),
                    use_container_width=True, hide_index=True,
                    column_config={
                        "Total_Campaign_Cost": st.column_config.NumberColumn("Total Spend", format="$%.2f"),
                        "True_CAC": st.column_config.NumberColumn("True CAC", format="$%.2f"),
                        "D Ratio": st.column_config.NumberColumn("Contact Rate", format="%.2f")
                    }
                )
            else:
                st.info("No CallsU operations data loaded. Upload an Internal Campaigns file in the Control Room.")

