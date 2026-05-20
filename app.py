import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
import io
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# GOOGLE SHEETS HELPERS
# ─────────────────────────────────────────────
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSHEETS_AVAILABLE = True
except ImportError:
    GSHEETS_AVAILABLE = False

try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

GSHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

@st.cache_resource
def get_gsheet_client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=GSHEETS_SCOPES
    )
    return gspread.authorize(creds)

def open_spreadsheet(client, name):
    """Open an existing spreadsheet — NEVER creates one. Raises clear error if not found."""
    try:
        return client.open(name)
    except gspread.SpreadsheetNotFound:
        raise Exception(
            f"Spreadsheet '{name}' not found. "
            f"Please create it in Google Drive and share it with: "
            f"{st.secrets['gcp_service_account']['client_email']}"
        )

def get_or_add_tab(sh, title):
    """Get existing tab or add a new one to the spreadsheet."""
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=50000, cols=50)

def save_sheet(client, spreadsheet_name, sheet_name, df_to_save, key_cols=None):
    """
    Append new rows to a tab in an existing spreadsheet.
    - Never creates the spreadsheet (must exist and be shared)
    - Adds tab if it doesn't exist yet
    - Deduplicates by key_cols before appending
    Returns (added, skipped, total_in_sheet)
    """
    if df_to_save is None or df_to_save.empty:
        return 0, 0, 0

    sh = open_spreadsheet(client, spreadsheet_name)
    ws = get_or_add_tab(sh, sheet_name)

    # Clean dataframe
    df_clean = df_to_save.copy()
    for col in df_clean.columns:
        if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
            df_clean[col] = df_clean[col].dt.strftime("%Y-%m-%d")
    df_clean = df_clean.fillna("").astype(str)

    # Read existing data
    existing_data = ws.get_all_records()

    if not existing_data:
        # Sheet tab is empty — write header + all rows
        ws.update([df_clean.columns.tolist()] + df_clean.values.tolist())
        return len(df_clean), 0, len(df_clean)

    # Deduplicate against existing rows
    existing_df = pd.DataFrame(existing_data).astype(str)
    if key_cols and all(k in existing_df.columns for k in key_cols) and        all(k in df_clean.columns for k in key_cols):
        ex_keys   = existing_df[key_cols].apply("_".join, axis=1)
        new_keys  = df_clean[key_cols].apply("_".join, axis=1)
        truly_new = df_clean[~new_keys.isin(ex_keys)]
    else:
        truly_new = df_clean

    skipped = len(df_clean) - len(truly_new)
    if len(truly_new) > 0:
        ws.append_rows(truly_new.values.tolist(), value_input_option="USER_ENTERED")

    return len(truly_new), skipped, len(existing_data) + len(truly_new)

# ─────────────────────────────────────────────
# SNOWFLAKE HELPERS
# ─────────────────────────────────────────────
def get_snowflake_conn():
    """Connect to Snowflake using Streamlit secrets."""
    sf = st.secrets["snowflake"]
    return snowflake.connector.connect(
        account   = sf["account"],
        user      = sf["user"],
        password  = sf["password"],
        warehouse = sf.get("warehouse", "COMPUTE_WH"),
        database  = sf.get("database", ""),
        schema    = sf.get("schema", "PUBLIC"),
        role      = sf.get("role", ""),
    )

def save_to_snowflake(conn, df, table_name, database, schema, if_exists="append"):
    """
    Save dataframe to a Snowflake table.
    if_exists='append' → upsert/append rows
    if_exists='replace' → drop and recreate table
    """
    if df is None or df.empty:
        return 0

    df_clean = df.copy()
    # Snowflake column names must be uppercase, no spaces
    df_clean.columns = [c.upper().replace(" ","_").replace("/","_").replace("-","_").replace("(","").replace(")","") for c in df_clean.columns]
    # Convert datetime cols
    for col in df_clean.columns:
        if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
            df_clean[col] = df_clean[col].dt.strftime("%Y-%m-%d")
    df_clean = df_clean.fillna("").astype(str)

    cursor = conn.cursor()
    full_table = f"{database}.{schema}.{table_name}"

    if if_exists == "replace":
        cursor.execute(f"DROP TABLE IF EXISTS {full_table}")

    # Create table if not exists
    cols_ddl = ", ".join([f'"{c}" VARCHAR' for c in df_clean.columns])
    cursor.execute(f'CREATE TABLE IF NOT EXISTS {full_table} ({cols_ddl})')

    # Write using write_pandas (bulk load)
    success, nchunks, nrows, _ = write_pandas(
        conn, df_clean, table_name.upper(),
        database=database, schema=schema,
        auto_create_table=True, overwrite=(if_exists=="replace")
    )
    cursor.close()
    return nrows


st.set_page_config(
    page_title="Flipkart Business Intelligence Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────
# CONSTANTS & CONFIG
# ─────────────────────────────────────────────
FRAG_KW = ['fragrance','perfume','deodorant','deo','edt','edp','attar','body mist','body spray','rollon','roll on']
BRAND_COLORS = {
    "BELLAVITA": "#9B59B6",
    "Kenaz": "#3498DB",
    "Embarouge": "#E74C3C",
    "HipHop Skincare": "#2ECC71",
    "Bella vita organic": "#F39C12",
    "Started with Guzz": "#1ABC9C"
}
PALETTE = ["#9B59B6","#3498DB","#2ECC71","#E74C3C","#F39C12","#1ABC9C","#E91E63","#FF5722","#00BCD4","#8BC34A"]
MONTH_ORDER = []
MONTH_LABELS = {}

# ─────────────────────────────────────────────
# FORMATTING HELPERS
# ─────────────────────────────────────────────
def indian_fmt(n):
    try:
        n = float(n)
        if pd.isna(n): return "—"
        neg = n < 0
        n = abs(int(round(n)))
        s = str(n)
        if len(s) <= 3: r = s
        else:
            last3 = s[-3:]
            rest = s[:-3]
            groups = []
            while len(rest) > 2:
                groups.append(rest[-2:])
                rest = rest[:-2]
            if rest: groups.append(rest)
            r = ",".join(reversed(groups)) + "," + last3
        return ("-₹" if neg else "₹") + r
    except: return str(n)

def badge(val, good_threshold=0, inverse=False):
    try:
        v = float(val)
        good = (v >= good_threshold) if not inverse else (v <= good_threshold)
        color = "#2ecc71" if good else "#e74c3c"
        arrow = "▲" if v >= 0 else "▼"
        return f'<span style="color:{color};font-weight:700">{arrow} {abs(v):.1f}%</span>'
    except: return "—"

# ─────────────────────────────────────────────
# CSS STYLING
# ─────────────────────────────────────────────
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
html,body,.main,.stApp{background:#07071a!important;font-family:'Inter',sans-serif!important;color:#e0e0f0!important}
.block-container{padding:1.5rem 2rem!important;max-width:100%!important}
section[data-testid="stSidebar"]{background:linear-gradient(180deg,#0d0d24,#111130)!important;min-width:280px!important;max-width:280px!important;border-right:1px solid #1e1e40!important}
.stButton>button{background:linear-gradient(135deg,#6C3483,#9B59B6)!important;color:white!important;border:none!important;border-radius:8px!important;font-weight:600!important}
div[data-testid="stDataFrame"]{border-radius:10px!important;overflow:hidden!important;border:1px solid #1e1e3a!important}
.stSelectbox>div>div,.stMultiSelect>div>div{background:#0f0f2a!important;border:1px solid #2a2a4a!important;color:white!important;border-radius:8px!important}
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-thumb{background:#2a2a4a;border-radius:3px}
::-webkit-scrollbar-thumb:hover{background:#6C3483}
#MainMenu,footer{visibility:hidden}
.metric-card{background:linear-gradient(135deg,#0f0f28,#161638);padding:18px 20px;border-radius:14px;border:1px solid #1e1e40;border-left:4px solid #6C3483;margin-bottom:10px;box-shadow:0 4px 20px rgba(0,0,0,0.4)}
.metric-label{color:#7777aa;font-size:11px;font-weight:600;letter-spacing:0.8px;text-transform:uppercase;margin-bottom:5px}
.metric-value{color:#fff;font-size:22px;font-weight:800;letter-spacing:-0.5px}
.metric-delta{font-size:12px;margin-top:3px}
.section-header{background:linear-gradient(90deg,rgba(108,52,131,0.15),transparent);border-left:3px solid #9B59B6;padding:10px 16px;border-radius:0 8px 8px 0;margin:30px 0 16px 0}
.section-title{color:#D7BDE2;font-size:18px;font-weight:700;margin:0}
.insight-box{background:rgba(108,52,131,0.1);border:1px solid rgba(108,52,131,0.35);border-radius:10px;padding:14px 18px;margin:10px 0}
.warning-box{background:rgba(231,76,60,0.1);border:1px solid rgba(231,76,60,0.35);border-radius:10px;padding:14px 18px;margin:10px 0}
.success-box{background:rgba(46,204,113,0.1);border:1px solid rgba(46,204,113,0.35);border-radius:10px;padding:14px 18px;margin:10px 0}
.info-box{background:rgba(52,152,219,0.1);border:1px solid rgba(52,152,219,0.35);border-radius:10px;padding:14px 18px;margin:10px 0}
.metric-value{font-size:clamp(13px,1.5vw,22px)!important}
.metric-label{font-size:clamp(9px,0.75vw,11px)!important}
.metric-card{padding:14px 10px!important}
div[data-testid="column"]{min-width:0!important}
</style>""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# DATA LOADING
# ─────────────────────────────────────────────
@st.cache_data(show_spinner="Loading data...")
def load_data(eb, sb, mb, lb=None, ib=None,
              eb_name="", sb_name="", mb_name="", lb_name="", ib_name=""):

    def smart_read(b, name=""):
        """Read bytes as CSV or Excel based on file extension, with fallback."""
        if b is None:
            return None
        buf = io.BytesIO(b)
        if str(name).lower().endswith(".csv"):
            try:    return pd.read_csv(buf)
            except: buf.seek(0); return pd.read_excel(buf)
        else:
            try:    return pd.read_excel(buf)
            except: buf.seek(0); return pd.read_csv(buf)

    earn = smart_read(eb, eb_name)
    earn['Order Date'] = pd.to_datetime(earn['Order Date'])
    earn['Month'] = earn['Order Date'].dt.to_period('M').astype(str)
    earn['Week'] = earn['Order Date'].dt.to_period('W').apply(lambda r: r.start_time.strftime('%Y-%m-%d'))
    earn['Channel'] = earn['Vertical'].apply(lambda v: 'Shopsy' if str(v).lower().startswith('shopsy') else 'National')
    earn['Type'] = earn['Category'].apply(lambda c: 'Fragrance' if any(k in str(c).lower() for k in FRAG_KW) else 'Non-Fragrance')
    earn['Brand'] = earn['Brand'].replace({'Bellavita':'BELLAVITA','bella vita':'BELLAVITA','BELLA VITA ORGANIC':'Bella vita organic'})
    earn['Cancel_Rate'] = (earn['Cancellation Amount'] / (earn['Final Sale Amount'] + earn['Cancellation Amount'])).fillna(0) * 100
    earn['Return_Rate'] = (earn['Return Amount'] / (earn['Final Sale Amount'] + earn['Return Amount'])).fillna(0) * 100

    if sb is not None:
        search = smart_read(sb, sb_name)
        search['Impression Date'] = pd.to_datetime(search['Impression Date'], errors='coerce')
        search['Month'] = search['Impression Date'].dt.to_period('M').astype(str)
        search['Brand'] = search['Brand'].replace({'Bellavita':'BELLAVITA'})
    else:
        search = pd.DataFrame(columns=['Month','Brand','Product Views','Product Clicks','Sales','Revenue','Click Through Rate','Conversion Rate','SKU Id'])

    if mb is not None:
        master = smart_read(mb, mb_name)
        # Strip all column names
        master = master.rename(columns=lambda c: str(c).strip())
        # Flexible rename — handle variations in column naming
        rename_map = {}
        for col in master.columns:
            cl = col.lower().replace(' ','').replace('_','').replace('/','')
            if 'fsubcat' in cl or ('subcat' in cl and 'subcat2' not in cl and '2' not in cl):
                rename_map[col] = 'F_Subcat'
            elif 'mastercategory' in cl or col == 'Master Category':
                rename_map[col] = 'Master_Category'
            elif 'npd' in cl or 'epd' in cl or 'exclusive' in cl:
                rename_map[col] = 'Exclusive'
            elif 'shortform' in cl or col in ['Short Form','Short_Form']:
                rename_map[col] = 'Short_Form'
            elif 'fulfilment' in cl or 'fulfillment' in cl:
                rename_map[col] = 'Fulfilment_Type'
            elif 'subcat2' in cl or '2' in cl and 'subcat' in cl:
                rename_map[col] = 'Subcat 2'
            elif col in ['F  Subcat','F Subcat']:
                rename_map[col] = 'F_Subcat'
        master = master.rename(columns=rename_map)
        # Ensure SKU ID column exists
        if 'SKU ID' not in master.columns:
            for col in master.columns:
                if 'sku' in col.lower() or 'fsn' in col.lower() or 'product' in col.lower():
                    master = master.rename(columns={col: 'SKU ID'})
                    break
    else:
        master = pd.DataFrame(columns=['SKU ID','Exclusive','Range','F_Subcat','Subcat 2','Master_Category','Gender','Active/Discontinued'])

    if lb is not None:
        listing = smart_read(lb, lb_name)
        listing.columns = listing.columns.str.strip()
    else:
        listing = pd.DataFrame(columns=['SKU ID','Total Inventory','Fulfillment Type'])

    if ib is not None:
        live_inv = smart_read(ib, ib_name)
        live_inv.columns = live_inv.columns.str.strip()
    else:
        live_inv = pd.DataFrame(columns=['SKU ID','FBF Inventory','B2B Scheduled Inventory'])

    return earn, search, master, listing, live_inv

earn = search = master = listing = live_inv = None

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("""<div style='text-align:center;padding:16px 0 8px 0'>
        <div style='font-size:28px'>🛒</div>
        <div style='font-size:16px;font-weight:800;color:#D7BDE2;margin-top:4px'>Flipkart BI Dashboard</div>
        <div style='font-size:10px;color:#5555aa;letter-spacing:1.5px;text-transform:uppercase;margin-top:2px'>Enterprise Intelligence</div>
    </div><hr style='border-color:#1e1e40'>""", unsafe_allow_html=True)

    st.markdown("### 📁 Upload Data Files")
    f_earn    = st.file_uploader("📊 EarnMore Report",           type=["xlsx","xls","csv"])
    f_search  = st.file_uploader("🔍 Search Traffic Report",     type=["xlsx","xls","csv"])
    f_master  = st.file_uploader("📋 Master FSNs / Mapping",     type=["xlsx","xls","csv"])
    f_listing = st.file_uploader("📦 Listing File (Total Inv)",  type=["xlsx","xls","csv"])
    f_live    = st.file_uploader("🏭 Live Inventory (FBF/B2B)",  type=["xlsx","xls","csv"])

    if not f_earn:
        st.info("⬆️ Upload EarnMore Report to begin")
        st.stop()

    earn, search, master, listing, live_inv = load_data(
        f_earn.read(),
        f_search.read()  if f_search  else None,
        f_master.read()  if f_master  else None,
        f_listing.read() if f_listing else None,
        f_live.read()    if f_live    else None,
        eb_name=f_earn.name,
        sb_name=f_search.name  if f_search  else "",
        mb_name=f_master.name  if f_master  else "",
        lb_name=f_listing.name if f_listing else "",
        ib_name=f_live.name    if f_live    else "",
    )

    MONTH_ORDER.clear(); MONTH_LABELS.clear()
    for m in sorted(earn['Month'].unique()):
        MONTH_ORDER.append(m)
        MONTH_LABELS[m] = pd.Period(m, freq='M').strftime('%b %y')

    # ── GOOGLE SHEETS SAVE ───────────────────────────────────────
    st.markdown("<hr style='border-color:#1e1e40'>", unsafe_allow_html=True)
    st.markdown("### 🗄️ Append to Google Sheets")

    if not GSHEETS_AVAILABLE:
        st.warning("Install `gspread` and `google-auth` to enable Google Sheets sync.")
    elif "gcp_service_account" not in st.secrets:
        st.info("💡 Add `gcp_service_account` to Streamlit secrets to enable Google Sheets.")
    else:
        gs_name = st.text_input("📋 Spreadsheet Name", "Flipkart_Sales_DB", key="gs_name")

        st.markdown("""<div style='background:rgba(52,152,219,0.1);border:1px solid rgba(52,152,219,0.3);
            border-radius:8px;padding:8px 12px;font-size:11px;color:#85C1E9;margin:6px 0'>
            ℹ️ The spreadsheet must already exist in Google Drive and be shared with the service account.
            New rows are appended only — existing data is never overwritten.
        </div>""", unsafe_allow_html=True)

        # Which tabs to append to
        st.markdown("<div style='font-size:12px;color:#aaa;margin:6px 0 3px 0'>Select tabs to append:</div>", unsafe_allow_html=True)
        save_earn_cb    = st.checkbox("📊 EarnMore Report → EarnMore_Report tab",  value=True,  key="cb_earn")
        save_search_cb  = st.checkbox("🔍 Search Traffic → Search_Traffic tab",    value=True,  key="cb_search")
        save_master_cb  = st.checkbox("📋 Master FSNs → Master_FSNs tab",          value=False, key="cb_master")
        save_listing_cb = st.checkbox("📦 Listing File → Listing_File tab",        value=False, key="cb_listing")
        save_live_cb    = st.checkbox("🏭 Live Inventory → Live_Inventory tab",    value=False, key="cb_live")

        if st.button("💾 Append to Google Sheets", type="primary", key="gs_save"):
            with st.spinner("Saving month-wise..."):
                try:
                    client = get_gsheet_client()
                    results = []

                    if save_earn_cb and not earn.empty:
                        earn_save = earn.copy()
                        drop_cols = [c for c in ['Cancel_Rate','Return_Rate','Week','Channel','Type'] if c in earn_save.columns]
                        earn_save = earn_save.drop(columns=drop_cols)
                        # Save each month to its own tab: e.g. "Earn_Jan 26"
                        months_in_data = sorted(earn_save['Month'].unique()) if 'Month' in earn_save.columns else []
                        earn_save_clean = earn_save.drop(columns=['Month'], errors='ignore')
                        for mon in months_in_data:
                            mon_df = earn_save[earn_save['Month'] == mon].drop(columns=['Month'], errors='ignore')
                            mon_label = pd.Period(mon, freq='M').strftime('%b %y')  # e.g. "Jan 26"
                            tab_name  = f"Earn_{mon_label}"                         # e.g. "Earn_Jan 26"
                            added, skipped, total = save_sheet(
                                client, gs_name, tab_name, mon_df,
                                key_cols=["Product Id","SKU ID","Order Date"]
                            )
                            results.append(f"📊 {tab_name}: +{added:,} new | {skipped:,} dupes | {total:,} total")

                    if save_search_cb and not search.empty:
                        search_save = search.copy()
                        months_in_search = sorted(search_save['Month'].unique()) if 'Month' in search_save.columns else []
                        for mon in months_in_search:
                            mon_df = search_save[search_save['Month'] == mon].drop(columns=['Month'], errors='ignore')
                            mon_label = pd.Period(mon, freq='M').strftime('%b %y')
                            tab_name  = f"Search_{mon_label}"
                            added, skipped, total = save_sheet(
                                client, gs_name, tab_name, mon_df,
                                key_cols=["SKU Id","Impression Date"]
                            )
                            results.append(f"🔍 {tab_name}: +{added:,} new | {skipped:,} dupes | {total:,} total")

                    if save_master_cb and not master.empty:
                        added, skipped, total = save_sheet(
                            client, gs_name, "Master_FSNs", master,
                            key_cols=["SKU ID"]
                        )
                        results.append(f"📋 Master_FSNs: +{added:,} new | {skipped:,} dupes | {total:,} total")

                    if save_listing_cb and not listing.empty:
                        # Listing is a snapshot — tab named with today's date
                        import datetime
                        today_label = datetime.date.today().strftime("%d %b %y")
                        tab_name = f"Listing_{today_label}"
                        added, skipped, total = save_sheet(
                            client, gs_name, tab_name, listing,
                            key_cols=None
                        )
                        results.append(f"📦 {tab_name}: {added:,} rows saved")

                    if save_live_cb and not live_inv.empty:
                        import datetime
                        today_label = datetime.date.today().strftime("%d %b %y")
                        tab_name = f"LiveInv_{today_label}"
                        added, skipped, total = save_sheet(
                            client, gs_name, tab_name, live_inv,
                            key_cols=None
                        )
                        results.append(f"🏭 {tab_name}: {added:,} rows saved")

                    if results:
                        st.success(f"✅ Done! {len(results)} tab(s) updated.")
                        for r in results:
                            st.markdown(f"<div style='font-size:11px;color:#2ecc71;margin:2px 0'>• {r}</div>", unsafe_allow_html=True)
                        try:
                            sh2 = get_gsheet_client().open(gs_name)
                            st.markdown(f"<a href='{sh2.url}' target='_blank' style='font-size:12px;color:#3498db'>🔗 Open Spreadsheet</a>", unsafe_allow_html=True)
                        except Exception:
                            pass
                    else:
                        st.warning("No tabs selected.")

                except Exception as e:
                    st.error(f"❌ {e}")

    # ── SNOWFLAKE SAVE ───────────────────────────────────────────
    st.markdown("<hr style='border-color:#1e1e40'>", unsafe_allow_html=True)
    st.markdown("### ❄️ Save to Snowflake")

    if not SNOWFLAKE_AVAILABLE:
        st.info("Add `snowflake-connector-python` to requirements.txt to enable Snowflake.")
    elif "snowflake" not in st.secrets:
        st.markdown("""<div style='background:rgba(41,182,246,0.1);border:1px solid rgba(41,182,246,0.3);
            border-radius:8px;padding:10px 12px;font-size:11px;color:#81D4FA;margin:6px 0'>
            💡 Add <b>[snowflake]</b> to Streamlit secrets:<br>
            <code>account = "your-account"</code><br>
            <code>user = "your-user"</code><br>
            <code>password = "your-password"</code><br>
            <code>warehouse = "COMPUTE_WH"</code><br>
            <code>database = "RAW_FLIPKART"</code><br>
            <code>schema = "PUBLIC"</code>
        </div>""", unsafe_allow_html=True)
    else:
        sf_secrets = st.secrets["snowflake"]
        sf_db     = st.text_input("❄️ Database",  sf_secrets.get("database","RAW_FLIPKART"), key="sf_db")
        sf_schema = st.text_input("📂 Schema",    sf_secrets.get("schema","PUBLIC"),         key="sf_schema")

        st.markdown("<div style='font-size:12px;color:#aaa;margin:6px 0 3px 0'>Select tables to save (month-wise):</div>", unsafe_allow_html=True)
        sf_earn_cb   = st.checkbox("📊 EarnMore → EARN_[MON] tables",   value=True,  key="sf_earn")
        sf_search_cb = st.checkbox("🔍 Search → SEARCH_[MON] tables",   value=True,  key="sf_search")
        sf_master_cb = st.checkbox("📋 Master FSNs → MASTER_FSNS",      value=False, key="sf_master")
        sf_list_cb   = st.checkbox("📦 Listing → LISTING_SNAPSHOT",     value=False, key="sf_listing")
        sf_live_cb   = st.checkbox("🏭 Live Inv → LIVE_INV_SNAPSHOT",   value=False, key="sf_live")

        if st.button("❄️ Save to Snowflake", type="primary", key="sf_save"):
            with st.spinner("Connecting to Snowflake..."):
                try:
                    conn = get_snowflake_conn()
                    sf_results = []

                    if sf_earn_cb and not earn.empty:
                        earn_sf = earn.copy()
                        drop_cols = [c for c in ['Cancel_Rate','Return_Rate','Week','Channel','Type'] if c in earn_sf.columns]
                        earn_sf = earn_sf.drop(columns=drop_cols)
                        months_in = sorted(earn_sf['Month'].unique()) if 'Month' in earn_sf.columns else []
                        for mon in months_in:
                            mon_label = pd.Period(mon, freq='M').strftime('%b_%y').upper()  # JAN_26
                            table_name = f"EARN_{mon_label}"
                            mon_df = earn_sf[earn_sf['Month']==mon].drop(columns=['Month'], errors='ignore')
                            nrows = save_to_snowflake(conn, mon_df, table_name, sf_db, sf_schema, if_exists="append")
                            sf_results.append(f"📊 {sf_db}.{sf_schema}.{table_name}: {nrows:,} rows")

                    if sf_search_cb and not search.empty:
                        search_sf = search.copy()
                        months_in = sorted(search_sf['Month'].unique()) if 'Month' in search_sf.columns else []
                        for mon in months_in:
                            mon_label = pd.Period(mon, freq='M').strftime('%b_%y').upper()
                            table_name = f"SEARCH_{mon_label}"
                            mon_df = search_sf[search_sf['Month']==mon].drop(columns=['Month'], errors='ignore')
                            nrows = save_to_snowflake(conn, mon_df, table_name, sf_db, sf_schema, if_exists="append")
                            sf_results.append(f"🔍 {sf_db}.{sf_schema}.{table_name}: {nrows:,} rows")

                    if sf_master_cb and not master.empty:
                        nrows = save_to_snowflake(conn, master, "MASTER_FSNS", sf_db, sf_schema, if_exists="replace")
                        sf_results.append(f"📋 {sf_db}.{sf_schema}.MASTER_FSNS: {nrows:,} rows")

                    if sf_list_cb and not listing.empty:
                        nrows = save_to_snowflake(conn, listing, "LISTING_SNAPSHOT", sf_db, sf_schema, if_exists="replace")
                        sf_results.append(f"📦 {sf_db}.{sf_schema}.LISTING_SNAPSHOT: {nrows:,} rows")

                    if sf_live_cb and not live_inv.empty:
                        nrows = save_to_snowflake(conn, live_inv, "LIVE_INV_SNAPSHOT", sf_db, sf_schema, if_exists="replace")
                        sf_results.append(f"🏭 {sf_db}.{sf_schema}.LIVE_INV_SNAPSHOT: {nrows:,} rows")

                    conn.close()

                    if sf_results:
                        st.success(f"✅ Saved to Snowflake! {len(sf_results)} table(s) updated.")
                        for r in sf_results:
                            st.markdown(f"<div style='font-size:11px;color:#29B6F6;margin:2px 0'>• {r}</div>", unsafe_allow_html=True)
                    else:
                        st.warning("No tables selected.")

                except Exception as e:
                    st.error(f"❌ Snowflake error: {e}")

    st.markdown("### 🔍 Global Filters")
    months_available = sorted(earn['Month'].unique())
    sel_months = st.multiselect("📅 Months", months_available, default=months_available,
                                 format_func=lambda m: MONTH_LABELS.get(m, m))
    brands_avail = ['All'] + sorted(earn['Brand'].unique().tolist())
    sel_brand    = st.selectbox("🏷️ Brand", brands_avail)
    sel_channel  = st.selectbox("📡 Channel", ['All','National','Shopsy'])
    cat_avail    = ['All'] + sorted(earn['Category'].unique().tolist())
    sel_cat      = st.selectbox("📦 Category", cat_avail)
    sel_ftype    = st.selectbox("🌸 Type", ['All','Fragrance','Non-Fragrance'])

    st.markdown("<hr style='border-color:#1e1e40'>", unsafe_allow_html=True)
    st.markdown("### 📍 Navigate")
    nav_sections = [
        ("🏢 Executive Overview","exec"),
        ("📊 Brand Intelligence","brand"),
        ("📦 Category Analysis","category"),
        ("🌸 Frag vs Non-Frag","frag"),
        ("🔍 Search Analytics","search"),
        ("🏭 Fulfillment (FBF)","fbf"),
        ("❌ Returns & Cancels","rnc"),
        ("🏆 SKU Intelligence","sku"),
        ("📈 Growth Diagnostics","growth"),
        ("⭐ Exclusives & Range","excl"),
        ("📦 Inventory Intelligence","inventory"),
        ("🎯 Action Recommendations","actions"),
    ]
    for label, anchor in nav_sections:
        st.markdown(f"<a href='#{anchor}' style='display:block;padding:6px 12px;margin:2px 0;color:#C39BD3;text-decoration:none;font-size:12px;font-weight:500;border-radius:6px;background:rgba(108,52,131,0.08)'>{label}</a>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# APPLY FILTERS
# ─────────────────────────────────────────────
df = earn.copy()
if sel_months:           df = df[df['Month'].isin(sel_months)]
if sel_brand != 'All':   df = df[df['Brand'] == sel_brand]
if sel_channel != 'All': df = df[df['Channel'] == sel_channel]
if sel_cat != 'All':     df = df[df['Category'] == sel_cat]
if sel_ftype != 'All':   df = df[df['Type'] == sel_ftype]

df_search_f = search.copy()
if sel_months:           df_search_f = df_search_f[df_search_f['Month'].isin(sel_months)]
if sel_brand != 'All':   df_search_f = df_search_f[df_search_f['Brand'] == sel_brand]

# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────
def metric_card(label, value, delta=None, prefix="₹", suffix="", color="#6C3483"):
    if prefix == "₹":
        fmt_val = indian_fmt(value)  # already includes ₹ symbol
        prefix = ""
    elif isinstance(value, float):
        fmt_val = f"{value:.1f}"
    else:
        fmt_val = str(value)
    delta_html = f"<div class='metric-delta'>{delta}</div>" if delta else ""
    st.markdown(f"""<div class="metric-card" style="border-left-color:{color}">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{prefix}{fmt_val}{suffix}</div>{delta_html}</div>""", unsafe_allow_html=True)

def section_header(title, anchor, emoji=""):
    st.markdown(f"<div id='{anchor}'></div>", unsafe_allow_html=True)
    st.markdown(f"<div class='section-header'><div class='section-title'>{emoji} {title}</div></div>", unsafe_allow_html=True)

def dark_fig(fig, height=380):
    fig.update_layout(template="plotly_dark", height=height,
                      paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(15,15,35,0.6)',
                      font=dict(family='Inter', color='#c0c0e0', size=12),
                      margin=dict(l=10,r=10,t=40,b=10),
                      legend=dict(bgcolor='rgba(0,0,0,0)', font=dict(size=11)))
    fig.update_xaxes(gridcolor='rgba(255,255,255,0.05)', showgrid=True)
    fig.update_yaxes(gridcolor='rgba(255,255,255,0.05)', showgrid=True)
    return fig

def ind_axis(fig, col_data, axis='y'):
    max_v = col_data.max() if hasattr(col_data,'max') else col_data
    if max_v > 0:
        ticks = [max_v * i / 5 for i in range(6)]
        labels = [indian_fmt(v) for v in ticks]
        if axis == 'y': fig.update_yaxes(tickvals=ticks, ticktext=labels)
        else:           fig.update_xaxes(tickvals=ticks, ticktext=labels)
    return fig

def compute_mom_growth(df_in, brand=None):
    d = df_in.copy()
    if brand: d = d[d['Brand'] == brand]
    by_month = d.groupby('Month')['Final Sale Amount'].sum().reindex(MONTH_ORDER, fill_value=0)
    last_two = by_month.dropna()
    if len(last_two) >= 2:
        latest = last_two.iloc[-1]; prev = last_two.iloc[-2]
        if prev > 0: return (latest - prev) / prev * 100
    return None

def compute_mtd_extrapolation(df_in, brand=None):
    d = df_in.copy()
    if brand: d = d[d['Brand'] == brand]
    all_months   = sorted(d['Month'].unique())
    latest_month = all_months[-1] if all_months else None
    prev_month   = all_months[-2] if len(all_months) >= 2 else None
    may = d[d['Month'] == latest_month] if latest_month else d.iloc[0:0]
    apr = d[d['Month'] == prev_month]   if prev_month   else d.iloc[0:0]
    days_so_far = may['Order Date'].nunique()
    mtd_rev     = may['Final Sale Amount'].sum()
    daily_rate  = mtd_rev / days_so_far if days_so_far > 0 else 0
    projected   = daily_rate * 31
    apr_rev     = apr['Final Sale Amount'].sum()
    proj_growth = (projected - apr_rev) / apr_rev * 100 if apr_rev > 0 else None
    return mtd_rev, daily_rate, projected, apr_rev, proj_growth

# ═══════════════════════════════════════════════════════════════
# HEADER BANNER
# ═══════════════════════════════════════════════════════════════
total_rev    = df['Final Sale Amount'].sum()
total_units  = df['Final Sale Units'].sum()
total_nsv    = df['NSV'].sum()
total_gmv    = df['GMV'].sum()
total_cancel = df['Cancellation Amount'].sum()
total_returns= df['Return Amount'].sum()
leakage      = (total_cancel + total_returns) / total_gmv * 100 if total_gmv > 0 else 0
cancel_rate  = total_cancel / (total_rev + total_cancel) * 100 if (total_rev + total_cancel) > 0 else 0
return_rate  = total_returns / (total_rev + total_returns) * 100 if (total_rev + total_returns) > 0 else 0
nsv_margin   = total_nsv / total_rev * 100 if total_rev > 0 else 0
fbf_rev      = df[df['Fulfillment Type'] == 'FBF']['Final Sale Amount'].sum()
fbf_pct      = fbf_rev / total_rev * 100 if total_rev > 0 else 0

active_filters = []
if sel_brand != 'All':   active_filters.append(f"Brand: {sel_brand}")
if sel_channel != 'All': active_filters.append(f"Channel: {sel_channel}")
if sel_cat != 'All':     active_filters.append(f"Category: {sel_cat}")
if sel_ftype != 'All':   active_filters.append(f"Type: {sel_ftype}")
filter_str = " · ".join(active_filters) if active_filters else "All Data"
months_str = " | ".join([MONTH_LABELS.get(m,m) for m in (sel_months or [])])

st.markdown(f"""
<div style='background:linear-gradient(135deg,#0f051e,#0a1535,#051520);border-radius:16px;
            padding:28px 32px;margin-bottom:28px;border:1px solid #1e1e40;box-shadow:0 8px 32px rgba(0,0,0,0.5)'>
    <div style='display:flex;align-items:center;gap:14px;margin-bottom:12px'>
        <span style='font-size:36px'>🛒</span>
        <div>
            <div style='font-size:26px;font-weight:900;color:#fff;letter-spacing:-0.5px'>Flipkart Business Intelligence Dashboard</div>
            <div style='font-size:12px;color:#6666bb;margin-top:3px;font-weight:500'>Enterprise Command Center · One Guardian</div>
        </div>
    </div>
    <div style='display:flex;gap:10px;flex-wrap:wrap;margin-top:12px'>
        <div style='background:rgba(108,52,131,0.2);border:1px solid rgba(108,52,131,0.4);border-radius:7px;padding:4px 12px;font-size:11px;color:#C39BD3;font-weight:600'>📅 {months_str}</div>
        <div style='background:rgba(52,152,219,0.2);border:1px solid rgba(52,152,219,0.4);border-radius:7px;padding:4px 12px;font-size:11px;color:#85C1E9;font-weight:600'>🎯 {filter_str}</div>
        <div style='background:rgba(46,204,113,0.15);border:1px solid rgba(46,204,113,0.3);border-radius:7px;padding:4px 12px;font-size:11px;color:#82E0AA;font-weight:600'>📊 {len(df):,} rows</div>
        <div style='background:rgba(243,156,18,0.15);border:1px solid rgba(243,156,18,0.3);border-radius:7px;padding:4px 12px;font-size:11px;color:#FAD7A0;font-weight:600'>🏭 FBF {fbf_pct:.1f}%</div>
    </div>
</div>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# SECTION 1: EXECUTIVE OVERVIEW
# ═══════════════════════════════════════════════════════════════
section_header("Executive Overview — Command Center", "exec", "🏢")

c1,c2,c3 = st.columns(3)
with c1: metric_card("Total Revenue", total_rev, color="#9B59B6")
with c2: metric_card("Total Units", total_units, prefix="", color="#3498DB")
with c3: metric_card("Net Sales Value", total_nsv, color="#2ECC71")
c4,c5,c6 = st.columns(3)
with c4: metric_card("Gross GMV", total_gmv, color="#F39C12")
with c5: metric_card("Revenue Leakage", leakage, prefix="", suffix="%", color="#E74C3C")
with c6: metric_card("FBF Revenue", fbf_rev, color="#1ABC9C")

st.markdown("<br>", unsafe_allow_html=True)
mtd, dr, proj, apr_r, proj_g = compute_mtd_extrapolation(df)
ca,cb,cc = st.columns(3)
with ca: metric_card("MTD Revenue", mtd, color="#9B59B6")
with cb: metric_card("Daily Run Rate", dr, color="#3498DB")
with cc: metric_card("Projected Month", proj, color="#2ECC71")
cd,ce,_ = st.columns(3)
with cd: metric_card("Prev Month", apr_r, color="#F39C12")
with ce: metric_card("Proj. Growth", round(proj_g or 0,1), prefix="", suffix="%", color="#E74C3C" if (proj_g or 0)<0 else "#2ECC71")

st.markdown("<br>", unsafe_allow_html=True)
col_l, col_r = st.columns([2,1])
with col_l:
    month_df = df.groupby('Month').agg(
        Revenue=('Final Sale Amount','sum'), Units=('Final Sale Units','sum'),
        NSV=('NSV','sum'), GMV=('GMV','sum'),
        Cancel=('Cancellation Amount','sum'), Returns=('Return Amount','sum')
    ).reset_index()
    month_df = month_df[month_df['Month'].isin(MONTH_ORDER)]
    month_df['Month_Label'] = month_df['Month'].map(MONTH_LABELS)
    month_df['MoM_Growth']  = month_df['Revenue'].pct_change() * 100
    month_df['Cancel_Rate'] = (month_df['Cancel'] / (month_df['Revenue'] + month_df['Cancel'])).fillna(0) * 100

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=month_df['Month_Label'], y=month_df['Revenue'], name="Revenue",
                         marker_color="#9B59B6", opacity=0.9,
                         text=[indian_fmt(v) for v in month_df['Revenue']],
                         textposition='inside', textfont=dict(color='white', size=10)), secondary_y=False)
    fig.add_trace(go.Scatter(x=month_df['Month_Label'], y=month_df['Cancel_Rate'],
                             name="Cancel Rate %", line=dict(color="#E74C3C", width=2.5),
                             mode='lines+markers+text',
                             text=[f"{v:.1f}%" for v in month_df['Cancel_Rate']],
                             textposition='top center', textfont=dict(size=10, color='#E74C3C')), secondary_y=True)
    fig.update_layout(title="📅 Monthly Revenue & Cancellation Rate", template="plotly_dark",
                      height=380, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(15,15,35,0.6)',
                      font=dict(color='#c0c0e0'), margin=dict(l=10,r=10,t=40,b=10),
                      legend=dict(orientation='h', y=1.12))
    fig.update_yaxes(title_text="Revenue (₹)", secondary_y=False)
    fig.update_yaxes(title_text="Cancel Rate %", secondary_y=True,
                     range=[0, max(month_df['Cancel_Rate'].max()*2, 10)])
    ind_axis(fig, month_df['Revenue'])
    st.plotly_chart(fig, use_container_width=True)

with col_r:
    brand_pie = df.groupby('Brand')['Final Sale Amount'].sum().reset_index().sort_values('Final Sale Amount', ascending=False)
    fig_pie = px.pie(brand_pie, values='Final Sale Amount', names='Brand',
                     title="Revenue by Brand", color='Brand',
                     color_discrete_map=BRAND_COLORS, hole=0.45)
    dark_fig(fig_pie, 380)
    fig_pie.update_traces(textposition='inside', textinfo='percent+label', textfont_size=10)
    st.plotly_chart(fig_pie, use_container_width=True)

col_a, col_b = st.columns(2)
with col_a:
    ch_month = df.groupby(['Month','Channel'])['Final Sale Amount'].sum().reset_index()
    ch_month = ch_month[ch_month['Month'].isin(MONTH_ORDER)]
    ch_month['Month_Label'] = ch_month['Month'].map(MONTH_LABELS)
    fig_ch = px.area(ch_month, x='Month_Label', y='Final Sale Amount', color='Channel',
                     title="National vs Shopsy — Monthly Revenue",
                     color_discrete_map={'National':'#2E86C1','Shopsy':'#E67E22'}, markers=True)
    dark_fig(fig_ch); ind_axis(fig_ch, ch_month['Final Sale Amount'])
    st.plotly_chart(fig_ch, use_container_width=True)

with col_b:
    wf = go.Figure(go.Waterfall(
        orientation="v", measure=["absolute","relative","relative","total"],
        x=["Gross GMV","Cancellations","Returns","Final Sale"],
        y=[total_gmv, -total_cancel, -total_returns, total_rev],
        text=[indian_fmt(total_gmv), f"-{indian_fmt(total_cancel)}", f"-{indian_fmt(total_returns)}", indian_fmt(total_rev)],
        textposition="outside",
        decreasing=dict(marker_color="#E74C3C"), increasing=dict(marker_color="#2ECC71"),
        totals=dict(marker_color="#9B59B6"),
        connector=dict(line=dict(color="#333366", width=1.5, dash="dot"))
    ))
    wf.update_layout(title="💧 GMV Leakage Waterfall", template="plotly_dark", height=380,
                     paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(15,15,35,0.6)',
                     font=dict(color='#c0c0e0'), margin=dict(l=10,r=10,t=40,b=10))
    st.plotly_chart(wf, use_container_width=True)

col1, col2 = st.columns(2)
with col1:
    mom_growth   = month_df['MoM_Growth'].iloc[-1] if len(month_df) > 1 else 0
    growth_signal = "🟢 Positive" if mom_growth > 0 else "🔴 Declining"
    st.markdown(f"""<div class='insight-box'>
    <div style='color:#C39BD3;font-weight:700;font-size:14px;margin-bottom:8px'>📊 Executive Business Pulse</div>
    <div style='color:#aaa;font-size:13px;line-height:1.7'>
    • Total revenue: <b style='color:white'>{indian_fmt(total_rev)}</b> across selected period<br>
    • NSV realization: <b style='color:#2ecc71'>{nsv_margin:.1f}%</b> (target &gt;80%)<br>
    • Revenue leakage: <b style='color:#e74c3c'>{leakage:.1f}%</b> of GMV<br>
    • FBF penetration: <b style='color:#3498db'>{fbf_pct:.1f}%</b> — {'above' if fbf_pct>65 else 'below'} 65% benchmark<br>
    • Latest MoM: <b style='color:{"#2ecc71" if mom_growth>0 else "#e74c3c"}'>{growth_signal} ({mom_growth:+.1f}%)</b><br>
    • Projected month: <b style='color:#9b59b6'>{indian_fmt(proj)}</b> ({proj_g:+.1f}% vs prev)
    </div></div>""", unsafe_allow_html=True)

with col2:
    top_brand       = brand_pie.iloc[0]['Brand'] if len(brand_pie) > 0 else "N/A"
    top_brand_rev   = brand_pie.iloc[0]['Final Sale Amount'] if len(brand_pie) > 0 else 0
    top_brand_share = top_brand_rev / total_rev * 100 if total_rev > 0 else 0
    risks = []
    if cancel_rate > 18:     risks.append(f"⚠️ Cancel rate {cancel_rate:.1f}% critically high (>18%)")
    if fbf_pct < 60:         risks.append(f"⚠️ FBF at {fbf_pct:.1f}% — conversion risk")
    if top_brand_share > 90: risks.append(f"⚠️ {top_brand} at {top_brand_share:.1f}% — concentration risk")
    if not risks:            risks.append("✅ All headline metrics within range")
    opps = ["🚀 Non-Fragrance growing — accelerate expansion",
            "📈 Kenaz momentum — scale FBF & search",
            f"🎯 Projected {indian_fmt(proj)} — push promotions"]
    st.markdown(f"""<div class='warning-box'>
    <div style='color:#E74C3C;font-weight:700;font-size:14px;margin-bottom:8px'>🔴 Risks & Opportunities</div>
    <div style='color:#aaa;font-size:13px;line-height:1.7'>{"<br>".join(risks)}<br><br>{"<br>".join(opps)}</div>
    </div>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# SECTION 2: BRAND INTELLIGENCE
# ═══════════════════════════════════════════════════════════════
section_header("Brand Intelligence — Deep Dive", "brand", "📊")

brand_sum = df.groupby('Brand').agg(
    Revenue=('Final Sale Amount','sum'), Units=('Final Sale Units','sum'),
    NSV=('NSV','sum'), GMV=('GMV','sum'),
    Cancel=('Cancellation Amount','sum'), Returns=('Return Amount','sum'),
    Cancel_Units=('Cancellation Units','sum'), Return_Units=('Return Units','sum'),
    Gross_Units=('Gross Units','sum')
).reset_index()
brand_sum['ASP']           = (brand_sum['Revenue'] / brand_sum['Units']).round(0)
brand_sum['Cancel_Rate%']  = (brand_sum['Cancel'] / (brand_sum['Revenue'] + brand_sum['Cancel'])).fillna(0) * 100
brand_sum['Return_Rate%']  = (brand_sum['Returns'] / (brand_sum['Revenue'] + brand_sum['Returns'])).fillna(0) * 100
brand_sum['NSV_Margin%']   = (brand_sum['NSV'] / brand_sum['Revenue']).fillna(0) * 100
brand_sum['Revenue_Share%']= (brand_sum['Revenue'] / brand_sum['Revenue'].sum() * 100).round(1)
brand_sum['FBF_Rev']       = df[df['Fulfillment Type']=='FBF'].groupby('Brand')['Final Sale Amount'].sum().reindex(brand_sum['Brand']).values
brand_sum['FBF%']          = (brand_sum['FBF_Rev'] / brand_sum['Revenue'] * 100).fillna(0).round(1)
brand_sum = brand_sum.sort_values('Revenue', ascending=False)

col1, col2 = st.columns([3,2])
with col1:
    fig_bb = px.bar(brand_sum, x='Brand', y='Revenue', color='Brand',
                    color_discrete_map=BRAND_COLORS, title="Brand Revenue Comparison",
                    text=brand_sum['Revenue'].apply(indian_fmt))
    dark_fig(fig_bb); ind_axis(fig_bb, brand_sum['Revenue'])
    fig_bb.update_traces(textposition='outside')
    st.plotly_chart(fig_bb, use_container_width=True)
with col2:
    fig_asp = px.scatter(brand_sum, x='Cancel_Rate%', y='NSV_Margin%', size='Revenue',
                         color='Brand', color_discrete_map=BRAND_COLORS,
                         title="Cancel Rate vs NSV Margin (bubble=Revenue)",
                         hover_data=['Revenue','Units','FBF%'])
    dark_fig(fig_asp)
    fig_asp.add_hline(y=80, line_dash="dash", line_color="#2ecc71", annotation_text="80% NSV target")
    fig_asp.add_vline(x=18, line_dash="dash", line_color="#e74c3c", annotation_text="18% cancel threshold")
    st.plotly_chart(fig_asp, use_container_width=True)

brand_monthly = df.groupby(['Month','Brand']).agg(Revenue=('Final Sale Amount','sum')).reset_index()
brand_monthly = brand_monthly[brand_monthly['Month'].isin(MONTH_ORDER)]
brand_monthly['Month_Label'] = brand_monthly['Month'].map(MONTH_LABELS)
fig_bm = px.line(brand_monthly, x='Month_Label', y='Revenue', color='Brand',
                 color_discrete_map=BRAND_COLORS, markers=True, title="Brand Monthly Revenue Trend")
dark_fig(fig_bm); ind_axis(fig_bm, brand_monthly['Revenue'])
st.plotly_chart(fig_bm, use_container_width=True)

st.markdown("**Brand Performance Matrix**")
display_cols  = ['Brand','Revenue','Units','ASP','NSV_Margin%','Cancel_Rate%','Return_Rate%','FBF%','Revenue_Share%']
brand_display = brand_sum[display_cols].copy()
brand_display['Revenue'] = brand_display['Revenue'].apply(indian_fmt)
brand_display['ASP']     = brand_display['ASP'].apply(lambda x: f"₹{x:,.0f}")
for p in ['NSV_Margin%','Cancel_Rate%','Return_Rate%','FBF%','Revenue_Share%']:
    brand_display[p] = brand_display[p].apply(lambda x: f"{x:.1f}%")
st.dataframe(brand_display, use_container_width=True, hide_index=True)

col3, col4 = st.columns(2)
with col3:
    bch = df.groupby(['Brand','Channel'])['Final Sale Amount'].sum().reset_index()
    fig_bch = px.bar(bch, x='Brand', y='Final Sale Amount', color='Channel', barmode='group',
                     title="Brand Revenue: National vs Shopsy",
                     color_discrete_map={'National':'#2E86C1','Shopsy':'#E67E22'})
    dark_fig(fig_bch); ind_axis(fig_bch, bch['Final Sale Amount'])
    st.plotly_chart(fig_bch, use_container_width=True)
with col4:
    bfbf = df.groupby(['Brand','Fulfillment Type'])['Final Sale Amount'].sum().reset_index()
    fig_bfbf = px.bar(bfbf, x='Brand', y='Final Sale Amount', color='Fulfillment Type', barmode='group',
                      title="Brand Revenue: FBF vs Non-FBF",
                      color_discrete_map={'FBF':'#9B59B6','NON_FBF':'#3498DB'})
    dark_fig(fig_bfbf); ind_axis(fig_bfbf, bfbf['Final Sale Amount'])
    st.plotly_chart(fig_bfbf, use_container_width=True)

# ═══════════════════════════════════════════════════════════════
# SECTION 3: CATEGORY ANALYSIS
# ═══════════════════════════════════════════════════════════════
section_header("Category Analysis — Revenue & Funnel", "category", "📦")

cat_sum = df.groupby(['Category']).agg(
    Revenue=('Final Sale Amount','sum'), Units=('Final Sale Units','sum'),
    NSV=('NSV','sum'), Cancel=('Cancellation Amount','sum'), Returns=('Return Amount','sum')
).reset_index().sort_values('Revenue', ascending=False)
cat_sum['Cancel_Rate%'] = (cat_sum['Cancel']/(cat_sum['Revenue']+cat_sum['Cancel'])).fillna(0)*100
cat_sum['Return_Rate%'] = (cat_sum['Returns']/(cat_sum['Revenue']+cat_sum['Returns'])).fillna(0)*100
cat_sum['Share%'] = (cat_sum['Revenue']/cat_sum['Revenue'].sum()*100).round(1)

col1, col2 = st.columns([3,2])
with col1:
    fig_cat = px.bar(cat_sum.head(8), x='Revenue', y='Category', orientation='h',
                     color='Cancel_Rate%', color_continuous_scale=['#2ecc71','#f39c12','#e74c3c'],
                     title="Category Revenue (color = Cancel Rate)",
                     text=cat_sum.head(8)['Revenue'].apply(indian_fmt))
    dark_fig(fig_cat, 360); ind_axis(fig_cat, cat_sum['Revenue'], 'x')
    fig_cat.update_traces(textposition='inside')
    fig_cat.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig_cat, use_container_width=True)
with col2:
    fig_cp = px.pie(cat_sum.head(6), values='Revenue', names='Category',
                    title="Category Revenue Share", hole=0.4, color_discrete_sequence=PALETTE)
    dark_fig(fig_cp, 360)
    st.plotly_chart(fig_cp, use_container_width=True)

cat_mom = df.groupby(['Month','Category'])['Final Sale Amount'].sum().reset_index()
cat_mom = cat_mom[cat_mom['Month'].isin(MONTH_ORDER)]
cat_mom['Month_Label'] = cat_mom['Month'].map(MONTH_LABELS)
top_cats = cat_sum.head(5)['Category'].tolist()
cat_mom_top = cat_mom[cat_mom['Category'].isin(top_cats)]
fig_ct = px.line(cat_mom_top, x='Month_Label', y='Final Sale Amount', color='Category',
                 markers=True, title="Top Category Monthly Trend", color_discrete_sequence=PALETTE)
dark_fig(fig_ct); ind_axis(fig_ct, cat_mom_top['Final Sale Amount'])
st.plotly_chart(fig_ct, use_container_width=True)

heat_df = df.groupby(['Brand','Category'])['Final Sale Amount'].sum().reset_index()
top_cats_h = heat_df.groupby('Category')['Final Sale Amount'].sum().nlargest(8).index
heat_df    = heat_df[heat_df['Category'].isin(top_cats_h)]
heat_piv   = heat_df.pivot(index='Brand', columns='Category', values='Final Sale Amount').fillna(0)
if not heat_piv.empty:
    fig_heat = px.imshow(heat_piv, color_continuous_scale=['#07071a','#2a1a4a','#6C3483','#D7BDE2'],
                         title="Brand × Category Revenue Heatmap", aspect='auto', text_auto=False)
    ann = [dict(x=j,y=i,text=indian_fmt(heat_piv.loc[b,c]),showarrow=False,font=dict(size=8,color='white'))
           for i,b in enumerate(heat_piv.index) for j,c in enumerate(heat_piv.columns) if heat_piv.loc[b,c]>0]
    fig_heat.update_layout(annotations=ann, height=300, template='plotly_dark',
                           paper_bgcolor='rgba(0,0,0,0)', font=dict(color='#c0c0e0'),
                           margin=dict(l=10,r=10,t=40,b=10))
    fig_heat.update_xaxes(tickangle=30)
    st.plotly_chart(fig_heat, use_container_width=True)

# ═══════════════════════════════════════════════════════════════
# SECTION 4: FRAGRANCE vs NON-FRAGRANCE
# ═══════════════════════════════════════════════════════════════
section_header("Fragrance vs Non-Fragrance — Growth Intelligence", "frag", "🌸")

frag_monthly = df.groupby(['Month','Type']).agg(Revenue=('Final Sale Amount','sum')).reset_index()
frag_monthly = frag_monthly[frag_monthly['Month'].isin(MONTH_ORDER)]
frag_monthly['Month_Label'] = frag_monthly['Month'].map(MONTH_LABELS)
frag_total = frag_monthly.groupby('Month')['Revenue'].sum().reset_index().rename(columns={'Revenue':'Total'})
frag_monthly = frag_monthly.merge(frag_total, on='Month')
frag_monthly['Share%'] = (frag_monthly['Revenue']/frag_monthly['Total']*100).round(1)

frag_sum = df.groupby('Type').agg(Revenue=('Final Sale Amount','sum'), Units=('Final Sale Units','sum')).reset_index()
frag_rev = frag_sum[frag_sum['Type']=='Fragrance']['Revenue'].sum()
nf_rev   = frag_sum[frag_sum['Type']=='Non-Fragrance']['Revenue'].sum()
frag_share = frag_rev / total_rev * 100
nf_share   = nf_rev   / total_rev * 100

c1,c2,c3,c4 = st.columns(4)
with c1: metric_card("Fragrance Revenue", frag_rev, color="#C39BD3")
with c2: metric_card("Non-Frag Revenue",  nf_rev,   color="#2ECC71")
with c3: metric_card("Fragrance Share", round(frag_share,1), prefix="", suffix="%", color="#C39BD3")
with c4: metric_card("Non-Frag Share",  round(nf_share,1),  prefix="", suffix="%", color="#2ECC71")

col1, col2 = st.columns(2)
with col1:
    fig_ft = px.area(frag_monthly, x='Month_Label', y='Revenue', color='Type',
                     title="Fragrance vs Non-Frag Monthly Revenue",
                     color_discrete_map={'Fragrance':'#C39BD3','Non-Fragrance':'#2ECC71'}, markers=True)
    dark_fig(fig_ft); ind_axis(fig_ft, frag_monthly['Revenue'])
    st.plotly_chart(fig_ft, use_container_width=True)
with col2:
    nf_share_trend = frag_monthly[frag_monthly['Type']=='Non-Fragrance']
    target_line = 25
    fig_nfs = px.line(nf_share_trend, x='Month_Label', y='Share%', markers=True,
                      title="Non-Frag Share % — Growth Tracker")
    fig_nfs.add_hline(y=target_line, line_dash='dash', line_color='#2ecc71',
                      annotation_text=f"Target: {target_line}%")
    fig_nfs.update_traces(line_color='#2ECC71', line_width=2.5, marker=dict(size=8))
    dark_fig(fig_nfs)
    latest_nf = nf_share_trend['Share%'].iloc[-1] if len(nf_share_trend) > 0 else 0
    gap = target_line - latest_nf
    st.plotly_chart(fig_nfs, use_container_width=True)
    color_gap = "#e74c3c" if gap > 0 else "#2ecc71"
    st.markdown(f"<div class='insight-box'>Non-Frag share: <b style='color:{color_gap}'>{latest_nf:.1f}%</b> | Gap to target: <b style='color:{color_gap}'>{gap:+.1f}pp</b></div>", unsafe_allow_html=True)

bt = df.groupby(['Brand','Type'])['Final Sale Amount'].sum().reset_index()
fig_bt = px.bar(bt, x='Brand', y='Final Sale Amount', color='Type', barmode='group',
                title="Brand: Fragrance vs Non-Frag Split",
                color_discrete_map={'Fragrance':'#C39BD3','Non-Fragrance':'#2ECC71'})
dark_fig(fig_bt); ind_axis(fig_bt, bt['Final Sale Amount'])
st.plotly_chart(fig_bt, use_container_width=True)

# ═══════════════════════════════════════════════════════════════
# SECTION 5: SEARCH ANALYTICS
# ═══════════════════════════════════════════════════════════════
section_header("Search Traffic Analytics — Funnel Intelligence", "search", "🔍")

total_views  = df_search_f['Product Views'].sum()
total_clicks = df_search_f['Product Clicks'].sum()
total_s_sales= df_search_f['Sales'].sum()
avg_ctr = (total_clicks/total_views*100) if total_views > 0 else 0
avg_cvr = (total_s_sales/total_clicks*100) if total_clicks > 0 else 0

c1,c2,c3,c4,c5 = st.columns(5)
with c1: metric_card("Total Views",   total_views,   prefix="", color="#3498DB")
with c2: metric_card("Total Clicks",  total_clicks,  prefix="", color="#9B59B6")
with c3: metric_card("Search Sales",  total_s_sales, prefix="", color="#2ECC71")
with c4: metric_card("Avg CTR", round(avg_ctr,2), prefix="", suffix="%", color="#F39C12")
with c5: metric_card("Avg CVR", round(avg_cvr,2), prefix="", suffix="%", color="#E74C3C")

col1, col2 = st.columns([2,1])
with col1:
    search_brand = df_search_f.groupby('Brand').agg(
        Views=('Product Views','sum'), Clicks=('Product Clicks','sum'), Sales=('Sales','sum'),
        CTR=('Click Through Rate','mean'), CVR=('Conversion Rate','mean'), Revenue=('Revenue','sum')
    ).reset_index().sort_values('Revenue', ascending=False)
    search_brand['CTR'] = search_brand['CTR'].round(2)
    search_brand['CVR'] = search_brand['CVR'].round(2)
    search_brand['Rev_per_Click'] = (search_brand['Revenue']/search_brand['Clicks'].replace(0,np.nan)).fillna(0).round(1)
    fig_sb = px.scatter(search_brand, x='CTR', y='CVR', size='Revenue', color='Brand',
                        color_discrete_map=BRAND_COLORS,
                        title="Search Efficiency Matrix: CTR vs CVR (bubble=Revenue)",
                        hover_data=['Views','Clicks','Sales','Rev_per_Click'])
    dark_fig(fig_sb)
    fig_sb.add_hline(y=3, line_dash='dash', line_color='#2ecc71', annotation_text="CVR 3% target")
    fig_sb.add_vline(x=15, line_dash='dash', line_color='#f39c12', annotation_text="CTR 15% avg")
    st.plotly_chart(fig_sb, use_container_width=True)
with col2:
    fig_funnel = go.Figure(go.Funnel(
        y=["Product Views","Product Clicks","Sales"],
        x=[total_views, total_clicks, total_s_sales],
        textinfo="value+percent initial",
        marker_color=["#3498DB","#9B59B6","#2ECC71"]
    ))
    fig_funnel.update_layout(title="Search Funnel", template="plotly_dark", height=380,
                             paper_bgcolor='rgba(0,0,0,0)', font=dict(color='#c0c0e0'),
                             margin=dict(l=10,r=10,t=40,b=10))
    st.plotly_chart(fig_funnel, use_container_width=True)

search_monthly = df_search_f.groupby('Month').agg(
    Views=('Product Views','sum'), Clicks=('Product Clicks','sum'), Sales=('Sales','sum')
).reset_index()
search_monthly = search_monthly[search_monthly['Month'].isin(MONTH_ORDER)]
search_monthly['Month_Label'] = search_monthly['Month'].map(MONTH_LABELS)
search_monthly['CTR'] = (search_monthly['Clicks']/search_monthly['Views']*100).fillna(0)
search_monthly['CVR'] = (search_monthly['Sales']/search_monthly['Clicks']*100).fillna(0)
fig_sm = make_subplots(specs=[[{"secondary_y": True}]])
fig_sm.add_trace(go.Bar(x=search_monthly['Month_Label'], y=search_monthly['Views'], name="Views",
                        marker_color="#3498DB", opacity=0.7), secondary_y=False)
fig_sm.add_trace(go.Bar(x=search_monthly['Month_Label'], y=search_monthly['Clicks'], name="Clicks",
                        marker_color="#9B59B6", opacity=0.8), secondary_y=False)
fig_sm.add_trace(go.Scatter(x=search_monthly['Month_Label'], y=search_monthly['CVR'], name="CVR%",
                            line=dict(color="#2ECC71", width=2.5), mode='lines+markers'), secondary_y=True)
fig_sm.update_layout(title="Monthly Search Traffic & CVR Trend", template="plotly_dark", height=380,
                     barmode='group', paper_bgcolor='rgba(0,0,0,0)', font=dict(color='#c0c0e0'),
                     margin=dict(l=10,r=10,t=40,b=10), legend=dict(orientation='h',y=1.12))
fig_sm.update_yaxes(title_text="Views / Clicks", secondary_y=False)
fig_sm.update_yaxes(title_text="CVR %", secondary_y=True)
st.plotly_chart(fig_sm, use_container_width=True)

sku_search = df_search_f.groupby(['SKU Id','Brand']).agg(
    Views=('Product Views','sum'), Clicks=('Product Clicks','sum'),
    Sales=('Sales','sum'), Revenue=('Revenue','sum'),
    CTR=('Click Through Rate','mean'), CVR=('Conversion Rate','mean')
).reset_index()
sku_search['CTR'] = sku_search['CTR'].round(2)
sku_search['CVR'] = sku_search['CVR'].round(2)
sku_search['Rev_per_Click'] = (sku_search['Revenue']/sku_search['Clicks'].replace(0,np.nan)).fillna(0).round(1)
top_search_skus = sku_search[sku_search['Revenue'] > 100000].nlargest(15,'Revenue')

with st.expander("🔍 Top SKUs by Search Revenue"):
    display_s = top_search_skus[['SKU Id','Brand','Revenue','Views','Clicks','Sales','CTR','CVR','Rev_per_Click']].copy()
    display_s['Revenue']      = display_s['Revenue'].apply(indian_fmt)
    display_s['Views']        = display_s['Views'].apply(lambda x: f"{x:,}")
    display_s['Clicks']       = display_s['Clicks'].apply(lambda x: f"{x:,}")
    display_s['CTR']          = display_s['CTR'].apply(lambda x: f"{x:.2f}%")
    display_s['CVR']          = display_s['CVR'].apply(lambda x: f"{x:.2f}%")
    display_s['Rev_per_Click']= display_s['Rev_per_Click'].apply(lambda x: f"₹{x:.1f}")
    st.dataframe(display_s, use_container_width=True, hide_index=True)
    inefficient = sku_search[(sku_search['Views']>50000)&(sku_search['CVR']<2)]
    if not inefficient.empty:
        st.markdown(f"<div class='warning-box'><b style='color:#e74c3c'>⚠️ High Traffic, Low Conversion SKUs ({len(inefficient)})</b><br><span style='color:#aaa;font-size:12px'>Listing/content/pricing issues — immediate action needed.</span></div>", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# SECTION 6: FBF ANALYSIS
# ═══════════════════════════════════════════════════════════════
section_header("Fulfillment Analysis — FBF vs Non-FBF", "fbf", "🏭")

fbf_sum = df.groupby('Fulfillment Type').agg(
    Revenue=('Final Sale Amount','sum'), Units=('Final Sale Units','sum'),
    Cancel=('Cancellation Amount','sum'), Returns=('Return Amount','sum'), GMV=('GMV','sum')
).reset_index()
fbf_sum['Cancel_Rate%'] = (fbf_sum['Cancel']/(fbf_sum['Revenue']+fbf_sum['Cancel'])).fillna(0)*100
fbf_sum['Return_Rate%'] = (fbf_sum['Returns']/(fbf_sum['Revenue']+fbf_sum['Returns'])).fillna(0)*100
fbf_sum['Recovery%']    = (fbf_sum['Revenue']/fbf_sum['GMV'].replace(0,np.nan)).fillna(0)*100

fbf_row  = fbf_sum[fbf_sum['Fulfillment Type']=='FBF'].iloc[0]    if len(fbf_sum[fbf_sum['Fulfillment Type']=='FBF'])    > 0 else None
nfbf_row = fbf_sum[fbf_sum['Fulfillment Type']=='NON_FBF'].iloc[0] if len(fbf_sum[fbf_sum['Fulfillment Type']=='NON_FBF']) > 0 else None

c1,c2,c3,c4 = st.columns(4)
with c1: metric_card("FBF Revenue",      fbf_row['Revenue']      if fbf_row  is not None else 0, color="#9B59B6")
with c2: metric_card("Non-FBF Revenue",  nfbf_row['Revenue']     if nfbf_row is not None else 0, color="#3498DB")
with c3: metric_card("FBF Cancel Rate",  round(fbf_row['Cancel_Rate%'],1)  if fbf_row  is not None else 0, prefix="", suffix="%", color="#E74C3C")
with c4: metric_card("NFBF Cancel Rate", round(nfbf_row['Cancel_Rate%'],1) if nfbf_row is not None else 0, prefix="", suffix="%", color="#F39C12")

col1,col2,col3 = st.columns(3)
with col1:
    fig_fp = px.pie(fbf_sum, values='Revenue', names='Fulfillment Type',
                    title="Revenue: FBF vs Non-FBF", hole=0.4,
                    color_discrete_map={'FBF':'#9B59B6','NON_FBF':'#3498DB'})
    dark_fig(fig_fp, 320); st.plotly_chart(fig_fp, use_container_width=True)
with col2:
    fig_fc = px.bar(fbf_sum, x='Fulfillment Type', y='Cancel_Rate%',
                    color='Cancel_Rate%', color_continuous_scale=['#2ecc71','#f39c12','#e74c3c'],
                    title="Cancel Rate: FBF vs Non-FBF", text='Cancel_Rate%')
    fig_fc.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
    dark_fig(fig_fc, 320); st.plotly_chart(fig_fc, use_container_width=True)
with col3:
    fig_fr = px.bar(fbf_sum, x='Fulfillment Type', y='Return_Rate%',
                    color='Return_Rate%', color_continuous_scale=['#2ecc71','#f39c12','#e74c3c'],
                    title="Return Rate: FBF vs Non-FBF", text='Return_Rate%')
    fig_fr.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
    dark_fig(fig_fr, 320); st.plotly_chart(fig_fr, use_container_width=True)

fbf_brand_monthly = df.groupby(['Month','Brand','Fulfillment Type'])['Final Sale Amount'].sum().reset_index()
fbf_brand_monthly = fbf_brand_monthly[fbf_brand_monthly['Month'].isin(MONTH_ORDER)]
fbf_brand_monthly['Month_Label'] = fbf_brand_monthly['Month'].map(MONTH_LABELS)
fbf_only = fbf_brand_monthly[fbf_brand_monthly['Fulfillment Type']=='FBF']
fig_fbt  = px.line(fbf_only, x='Month_Label', y='Final Sale Amount', color='Brand',
                   markers=True, title="FBF Revenue by Brand — Monthly Trend",
                   color_discrete_map=BRAND_COLORS)
dark_fig(fig_fbt); ind_axis(fig_fbt, fbf_only['Final Sale Amount'])
st.plotly_chart(fig_fbt, use_container_width=True)

if fbf_row is not None and nfbf_row is not None:
    fbf_cr = fbf_row['Cancel_Rate%']; nfbf_cr = nfbf_row['Cancel_Rate%']
    cancel_diff = nfbf_cr - fbf_cr
    st.markdown(f"""<div class='insight-box'><b style='color:#D7BDE2'>🏭 Fulfillment Intelligence</b><br>
    <span style='color:#aaa;font-size:13px'>
    • FBF: <b style='color:#9B59B6'>{fbf_pct:.1f}%</b> — {'strong' if fbf_pct>65 else 'needs improvement'}<br>
    • FBF cancel {fbf_cr:.1f}% vs Non-FBF {nfbf_cr:.1f}% — FBF is {abs(cancel_diff):.1f}pp {'better' if cancel_diff>0 else 'worse'}<br>
    • Action: Migrate high-velocity Non-FBF SKUs to FBF
    </span></div>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# SECTION 7: RETURNS & CANCELLATIONS
# ═══════════════════════════════════════════════════════════════
section_header("Returns & Cancellation Command Center", "rnc", "❌")

c1,c2,c3,c4 = st.columns(4)
with c1: metric_card("Revenue Lost (Cancel)",  total_cancel,  color="#E74C3C")
with c2: metric_card("Revenue Lost (Returns)", total_returns, color="#E67E22")
with c3: metric_card("Overall Cancel Rate", round(cancel_rate,1), prefix="", suffix="%", color="#E74C3C")
with c4: metric_card("Overall Return Rate", round(return_rate,1), prefix="", suffix="%", color="#E67E22")

col1, col2 = st.columns(2)
with col1:
    fig_crb = px.bar(brand_sum.sort_values('Cancel_Rate%',ascending=False),
                     x='Brand', y='Cancel_Rate%', color='Cancel_Rate%',
                     color_continuous_scale=['#2ecc71','#f39c12','#e74c3c'],
                     title="Cancel Rate % by Brand", text='Cancel_Rate%')
    fig_crb.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
    fig_crb.add_hline(y=18, line_dash='dash', line_color='#e74c3c', annotation_text="18% alert")
    dark_fig(fig_crb); st.plotly_chart(fig_crb, use_container_width=True)
with col2:
    rnc_monthly = df.groupby('Month').agg(
        Revenue=('Final Sale Amount','sum'), Cancel=('Cancellation Amount','sum'), Returns=('Return Amount','sum')
    ).reset_index()
    rnc_monthly = rnc_monthly[rnc_monthly['Month'].isin(MONTH_ORDER)]
    rnc_monthly['Month_Label'] = rnc_monthly['Month'].map(MONTH_LABELS)
    rnc_monthly['Cancel_Rate'] = (rnc_monthly['Cancel']/(rnc_monthly['Revenue']+rnc_monthly['Cancel']))*100
    rnc_monthly['Return_Rate'] = (rnc_monthly['Returns']/(rnc_monthly['Revenue']+rnc_monthly['Returns']))*100
    fig_rnc = px.line(rnc_monthly, x='Month_Label', y=['Cancel_Rate','Return_Rate'], markers=True,
                      title="Monthly Cancel & Return Rate Trend",
                      color_discrete_map={'Cancel_Rate':'#E74C3C','Return_Rate':'#E67E22'})
    dark_fig(fig_rnc)
    fig_rnc.add_hline(y=18, line_dash='dash', line_color='#e74c3c', annotation_text="18% threshold")
    st.plotly_chart(fig_rnc, use_container_width=True)

with st.expander("🔍 High Cancellation SKUs — Risk Register"):
    sku_cancel = df.groupby(['SKU ID','Brand','Category']).agg(
        Revenue=('Final Sale Amount','sum'), Cancel=('Cancellation Amount','sum')
    ).reset_index()
    sku_cancel['Cancel_Rate%'] = (sku_cancel['Cancel']/(sku_cancel['Revenue']+sku_cancel['Cancel'])).fillna(0)*100
    sku_cancel = sku_cancel[sku_cancel['Revenue']>=10000].sort_values('Cancel_Rate%',ascending=False).head(20)
    sku_cancel['Revenue'] = sku_cancel['Revenue'].apply(indian_fmt)
    sku_cancel['Cancel']  = sku_cancel['Cancel'].apply(indian_fmt)
    sku_cancel['Cancel_Rate%'] = sku_cancel['Cancel_Rate%'].apply(lambda x: f"{x:.1f}%")
    st.dataframe(sku_cancel, use_container_width=True, hide_index=True)

cat_cancel = df.groupby('Category').agg(
    Revenue=('Final Sale Amount','sum'), Cancel=('Cancellation Amount','sum')
).reset_index()
cat_cancel['Cancel_Rate%'] = (cat_cancel['Cancel']/(cat_cancel['Revenue']+cat_cancel['Cancel'])).fillna(0)*100
cat_cancel = cat_cancel[cat_cancel['Revenue']>50000].sort_values('Cancel_Rate%',ascending=False).head(10)
fig_cc = px.bar(cat_cancel, x='Category', y='Cancel_Rate%', color='Cancel_Rate%',
                color_continuous_scale=['#2ecc71','#f39c12','#e74c3c'],
                title="Category Cancel Rate % (min ₹50K revenue)", text='Cancel_Rate%')
fig_cc.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
fig_cc.update_xaxes(tickangle=30)
dark_fig(fig_cc, 320)
fig_cc.add_hline(y=18, line_dash='dash', line_color='#e74c3c')
st.plotly_chart(fig_cc, use_container_width=True)

# ═══════════════════════════════════════════════════════════════
# SECTION 8: SKU INTELLIGENCE
# ═══════════════════════════════════════════════════════════════
section_header("SKU Intelligence & Health Scorecard", "sku", "🏆")

sku_df = df.groupby(['SKU ID','Brand','Category','Vertical']).agg(
    Revenue=('Final Sale Amount','sum'), Units=('Final Sale Units','sum'),
    NSV=('NSV','sum'), GMV=('GMV','sum'),
    Cancel=('Cancellation Amount','sum'), Returns=('Return Amount','sum'),
    Gross_Units=('Gross Units','sum')
).reset_index()
sku_df['ASP']        = (sku_df['Revenue']/sku_df['Units'].replace(0,np.nan)).fillna(0).round(0)
sku_df['Cancel_Rate']= (sku_df['Cancel']/(sku_df['Revenue']+sku_df['Cancel'])).fillna(0)*100
sku_df['Return_Rate']= (sku_df['Returns']/(sku_df['Revenue']+sku_df['Returns'])).fillna(0)*100
sku_df['NSV_Margin'] = (sku_df['NSV']/sku_df['Revenue'].replace(0,np.nan)).fillna(0)*100

fbf_sku = df[df['Fulfillment Type']=='FBF'].groupby('SKU ID')['Final Sale Amount'].sum().reset_index()
fbf_sku.columns = ['SKU ID','FBF_Rev']
sku_df = sku_df.merge(fbf_sku, on='SKU ID', how='left')
sku_df['FBF_Rev'] = sku_df['FBF_Rev'].fillna(0)
sku_df['FBF%']    = (sku_df['FBF_Rev']/sku_df['Revenue'].replace(0,np.nan)).fillna(0)*100

sku_min = sku_df[sku_df['Revenue'] >= 5000].copy()
if len(sku_min) > 5:
    sku_min['Rev_Score']    = pd.qcut(sku_min['Revenue'].rank(method='first'), 5, labels=[1,2,3,4,5]).astype(float)
    sku_min['Cancel_Score'] = pd.qcut(sku_min['Cancel_Rate'].rank(method='first',ascending=False), 5, labels=[1,2,3,4,5]).astype(float)
    sku_min['Return_Score'] = pd.qcut(sku_min['Return_Rate'].rank(method='first',ascending=False), 5, labels=[1,2,3,4,5]).astype(float)
    sku_min['FBF_Score']    = sku_min['FBF%'].apply(lambda x: 5 if x>80 else (4 if x>60 else (3 if x>40 else (2 if x>20 else 1))))
    sku_min['Health_Score'] = ((sku_min['Rev_Score']*0.35+sku_min['Cancel_Score']*0.3+sku_min['Return_Score']*0.2+sku_min['FBF_Score']*0.15)*20).round(0).astype(int)
    sku_min['Status']       = sku_min['Health_Score'].apply(lambda x: '🟢 Healthy' if x>=70 else ('🟡 Watch' if x>=45 else '🔴 Critical'))

    c1,c2,c3 = st.columns(3)
    with c1: metric_card("🟢 Healthy SKUs", len(sku_min[sku_min['Status']=='🟢 Healthy']), prefix="", color="#2ECC71")
    with c2: metric_card("🟡 Watch SKUs",   len(sku_min[sku_min['Status']=='🟡 Watch']),   prefix="", color="#F39C12")
    with c3: metric_card("🔴 Critical SKUs",len(sku_min[sku_min['Status']=='🔴 Critical']),prefix="", color="#E74C3C")

    status_f = st.selectbox("Filter SKU Status", ["All","🟢 Healthy","🟡 Watch","🔴 Critical"])
    sku_show = sku_min if status_f=="All" else sku_min[sku_min['Status']==status_f]
    sku_show = sku_show.sort_values('Health_Score',ascending=False).head(30)
    sku_display = sku_show[['SKU ID','Brand','Category','Revenue','Units','ASP','Cancel_Rate','Return_Rate','FBF%','Health_Score','Status']].copy()
    sku_display['Revenue'] = sku_display['Revenue'].apply(indian_fmt)
    sku_display['ASP']     = sku_display['ASP'].apply(lambda x: f"₹{x:.0f}")
    for c in ['Cancel_Rate','Return_Rate','FBF%']:
        sku_display[c] = sku_display[c].apply(lambda x: f"{x:.1f}%")
    st.dataframe(sku_display, use_container_width=True, hide_index=True)

col1, col2 = st.columns(2)
with col1:
    top15 = sku_df.nlargest(15,'Revenue')
    fig_top = px.bar(top15, x='Revenue', y='SKU ID', orientation='h',
                     color='Brand', color_discrete_map=BRAND_COLORS,
                     title="Top 15 SKUs by Revenue", text=top15['Revenue'].apply(indian_fmt))
    fig_top.update_traces(textposition='inside')
    fig_top.update_layout(yaxis=dict(autorange="reversed"))
    dark_fig(fig_top, 500); ind_axis(fig_top, top15['Revenue'], 'x')
    st.plotly_chart(fig_top, use_container_width=True)
with col2:
    sku_scatter = sku_df[sku_df['Revenue']>50000]
    fig_scat = px.scatter(sku_scatter, x='Cancel_Rate', y='Return_Rate',
                          size='Revenue', color='Brand', color_discrete_map=BRAND_COLORS,
                          title="SKU Risk Map: Cancel vs Return Rate",
                          hover_data=['SKU ID','Revenue','Units'])
    dark_fig(fig_scat, 500)
    fig_scat.add_hline(y=5, line_dash='dash', line_color='#e67e22', annotation_text="5% return threshold")
    fig_scat.add_vline(x=18, line_dash='dash', line_color='#e74c3c', annotation_text="18% cancel threshold")
    st.plotly_chart(fig_scat, use_container_width=True)

# ═══════════════════════════════════════════════════════════════
# SECTION 9: GROWTH DIAGNOSTICS
# ═══════════════════════════════════════════════════════════════
section_header("Growth Diagnostics — Winners vs Losers", "growth", "📈")

df_sku_weekly = df.groupby(['SKU ID','Brand','Category','Week'])['Final Sale Amount'].sum().reset_index()
weeks_sorted  = sorted(df_sku_weekly['Week'].unique())
if len(weeks_sorted) >= 2:
    last_wk = df_sku_weekly[df_sku_weekly['Week']==weeks_sorted[-1]].groupby(['SKU ID','Brand','Category'])['Final Sale Amount'].sum().reset_index()
    prev_wk = df_sku_weekly[df_sku_weekly['Week']==weeks_sorted[-2]].groupby('SKU ID')['Final Sale Amount'].sum().reset_index()
    last_wk.columns = ['SKU ID','Brand','Category','This_Week']
    prev_wk.columns = ['SKU ID','Last_Week']
    wow_skus = last_wk.merge(prev_wk, on='SKU ID', how='outer').fillna(0)
    wow_skus['WoW%'] = ((wow_skus['This_Week']-wow_skus['Last_Week'])/wow_skus['Last_Week'].replace(0,np.nan)*100).round(1)
    wow_skus = wow_skus[wow_skus['Last_Week']>=1000]
    growing_skus  = wow_skus[wow_skus['WoW%']>0].nlargest(10,'WoW%')
    declining_skus= wow_skus[wow_skus['WoW%']<0].nsmallest(10,'WoW%')

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("<div class='success-box'><b style='color:#2ecc71'>📈 Top 10 Growing SKUs (WoW)</b></div>", unsafe_allow_html=True)
        fig_grow = px.bar(growing_skus, x='WoW%', y='SKU ID', orientation='h',
                          color='Brand', color_discrete_map=BRAND_COLORS,
                          title=f"Growing SKUs (vs w/c {weeks_sorted[-2]})")
        fig_grow.update_layout(yaxis=dict(autorange="reversed"))
        dark_fig(fig_grow, 400); st.plotly_chart(fig_grow, use_container_width=True)
    with col2:
        st.markdown("<div class='warning-box'><b style='color:#e74c3c'>📉 Top 10 Declining SKUs (WoW)</b></div>", unsafe_allow_html=True)
        fig_dec = px.bar(declining_skus, x='WoW%', y='SKU ID', orientation='h',
                         color='Brand', color_discrete_map=BRAND_COLORS, title="Declining SKUs")
        fig_dec.update_layout(yaxis=dict(autorange="reversed"))
        dark_fig(fig_dec, 400); st.plotly_chart(fig_dec, use_container_width=True)

brand_weekly = df.groupby(['Week','Brand'])['Final Sale Amount'].sum().reset_index()
bw_piv = brand_weekly.pivot(index='Brand', columns='Week', values='Final Sale Amount').fillna(0)
if not bw_piv.empty:
    fig_bwh = px.imshow(bw_piv, color_continuous_scale=['#07071a','#2a1a4a','#6C3483','#D7BDE2'],
                        title="Brand × Week Revenue Heatmap", aspect='auto')
    ann_bw = [dict(x=j,y=i,text=indian_fmt(bw_piv.loc[b,w]),showarrow=False,font=dict(size=7,color='white'))
              for i,b in enumerate(bw_piv.index) for j,w in enumerate(bw_piv.columns) if bw_piv.loc[b,w]>0]
    fig_bwh.update_layout(annotations=ann_bw, height=250, template='plotly_dark',
                           paper_bgcolor='rgba(0,0,0,0)', font=dict(color='#c0c0e0'),
                           margin=dict(l=10,r=10,t=40,b=10))
    fig_bwh.update_xaxes(tickangle=45, tickfont=dict(size=8))
    st.plotly_chart(fig_bwh, use_container_width=True)

mom_brand = df.groupby(['Month','Brand'])['Final Sale Amount'].sum().reset_index()
mom_brand = mom_brand[mom_brand['Month'].isin(MONTH_ORDER)]
mom_piv   = mom_brand.pivot(index='Brand', columns='Month', values='Final Sale Amount').fillna(0)
mom_piv.columns = [MONTH_LABELS.get(c,c) for c in mom_piv.columns]
if len(mom_piv.columns) >= 2:
    last_col = mom_piv.columns[-1]; prev_col = mom_piv.columns[-2]
    mom_piv['MoM Growth%'] = ((mom_piv[last_col]-mom_piv[prev_col])/mom_piv[prev_col].replace(0,np.nan)*100).round(1)
mom_display = mom_piv.copy()
for c in mom_display.columns:
    if c != 'MoM Growth%': mom_display[c] = mom_display[c].apply(indian_fmt)
    else: mom_display[c] = mom_display[c].apply(lambda x: f"{x:+.1f}%" if not pd.isna(x) else "—")
st.markdown("**Brand MoM Revenue Matrix**")
st.dataframe(mom_display, use_container_width=True)

# ═══════════════════════════════════════════════════════════════
# SECTION 10: EXCLUSIVES & RANGE
# ═══════════════════════════════════════════════════════════════
section_header("Exclusives & Range Portfolio Analysis", "excl", "⭐")

# Debug: show master columns so user can verify mapping
with st.expander("🔧 Master File Column Debug (expand if Exclusives section errors)", expanded=False):
    st.markdown(f"**Columns detected in Master file:** `{list(master.columns)}`")
    st.markdown("**Expected columns:** `SKU ID, Exclusive, Range, F_Subcat, Subcat 2, Master_Category, Gender, Active/Discontinued`")
    missing = [c for c in ['SKU ID','Exclusive','Range','F_Subcat','Subcat 2','Master_Category','Gender','Active/Discontinued'] if c not in master.columns]
    if missing:
        st.warning(f"Missing columns (will show as blank): {missing}")
    else:
        st.success("✅ All expected columns found.")


# Only select master columns that actually exist after rename
_master_want = ['SKU ID','Exclusive','Range','F_Subcat','Subcat 2','Master_Category','Gender','Active/Discontinued']
_master_have = [c for c in _master_want if c in master.columns]
master_slim  = master[_master_have].copy() if _master_have else pd.DataFrame(columns=['SKU ID'])
if 'SKU ID' in master_slim.columns and 'SKU ID' in df.columns:
    df_merged = df.merge(master_slim, on='SKU ID', how='left')
else:
    df_merged = df.copy()
# Fill any missing master columns with None so downstream code never crashes
for _mc in _master_want:
    if _mc not in df_merged.columns:
        df_merged[_mc] = None

excl_sum = df_merged.groupby('Exclusive').agg(
    Revenue=('Final Sale Amount','sum'), Units=('Final Sale Units','sum'),
    Cancel=('Cancellation Amount','sum'), Returns=('Return Amount','sum')
).reset_index()
excl_sum['Cancel_Rate%'] = (excl_sum['Cancel']/(excl_sum['Revenue']+excl_sum['Cancel'])).fillna(0)*100
excl_sum['Return_Rate%'] = (excl_sum['Returns']/(excl_sum['Revenue']+excl_sum['Returns'])).fillna(0)*100
excl_sum = excl_sum.dropna(subset=['Exclusive'])

col1, col2 = st.columns(2)
with col1:
    if not excl_sum.empty:
        fig_excl = px.pie(excl_sum, values='Revenue', names='Exclusive',
                          title="Revenue by Exclusive Type", hole=0.4, color_discrete_sequence=PALETTE)
        dark_fig(fig_excl, 340); st.plotly_chart(fig_excl, use_container_width=True)
    else:
        st.info("Exclusive breakdown not available after filter")
with col2:
    range_sum = df_merged.groupby('Range').agg(Revenue=('Final Sale Amount','sum'), Units=('Final Sale Units','sum')).reset_index().dropna(subset=['Range']).nlargest(12,'Revenue')
    if not range_sum.empty:
        fig_range = px.bar(range_sum, x='Revenue', y='Range', orientation='h',
                           color='Revenue', color_continuous_scale=['#1a0a3a','#9B59B6','#D7BDE2'],
                           title="Top 12 Ranges by Revenue", text=range_sum['Revenue'].apply(indian_fmt))
        fig_range.update_traces(textposition='inside')
        fig_range.update_layout(yaxis=dict(autorange="reversed"))
        dark_fig(fig_range, 340); ind_axis(fig_range, range_sum['Revenue'], 'x')
        st.plotly_chart(fig_range, use_container_width=True)

fsubcat_sum = df_merged.groupby('F_Subcat').agg(
    Revenue=('Final Sale Amount','sum'), Units=('Final Sale Units','sum'), Cancel=('Cancellation Amount','sum')
).reset_index().dropna(subset=['F_Subcat']).nlargest(12,'Revenue')
fsubcat_sum['Cancel_Rate%'] = (fsubcat_sum['Cancel']/(fsubcat_sum['Revenue']+fsubcat_sum['Cancel'])).fillna(0)*100
if not fsubcat_sum.empty:
    fig_fsc = px.bar(fsubcat_sum, x='F_Subcat', y='Revenue', color='Cancel_Rate%',
                     color_continuous_scale=['#2ecc71','#f39c12','#e74c3c'],
                     title="F-Subcategory Revenue (color = Cancel Rate)",
                     text=fsubcat_sum['Revenue'].apply(indian_fmt))
    fig_fsc.update_traces(textposition='outside'); fig_fsc.update_xaxes(tickangle=35)
    dark_fig(fig_fsc, 360); ind_axis(fig_fsc, fsubcat_sum['Revenue'])
    st.plotly_chart(fig_fsc, use_container_width=True)

gender_sum = df_merged.groupby('Gender').agg(Revenue=('Final Sale Amount','sum')).reset_index().dropna()
if not gender_sum.empty and len(gender_sum) > 1:
    col1, col2 = st.columns([1,2])
    with col1:
        fig_gen = px.pie(gender_sum, values='Revenue', names='Gender',
                         title="Revenue by Gender", hole=0.4, color_discrete_sequence=PALETTE)
        dark_fig(fig_gen, 280); st.plotly_chart(fig_gen, use_container_width=True)
    with col2:
        range_by_cat = df_merged.groupby(['Master_Category','Range'])['Final Sale Amount'].sum().reset_index().dropna().nlargest(15,'Final Sale Amount')
        fig_rbc = px.bar(range_by_cat, x='Range', y='Final Sale Amount', color='Master_Category',
                         title="Top Ranges by Master Category", color_discrete_sequence=PALETTE)
        dark_fig(fig_rbc, 280); ind_axis(fig_rbc, range_by_cat['Final Sale Amount'])
        fig_rbc.update_xaxes(tickangle=35); st.plotly_chart(fig_rbc, use_container_width=True)

# ═══════════════════════════════════════════════════════════════
# SECTION 11: INVENTORY INTELLIGENCE
# ═══════════════════════════════════════════════════════════════
section_header("Inventory Intelligence — Stock Health & Days Cover", "inventory", "📦")

def find_col(df_in, keywords):
    for c in df_in.columns:
        if any(k.lower() in c.lower() for k in keywords):
            return c
    return None

if listing.empty:
    st.info("⬆️ Upload Listing File and Live Inventory Report in the sidebar to unlock inventory analysis.")
else:
    list_sku_col = find_col(listing, ['SKU ID','SKU Id','sku_id','FSN','listing id'])
    list_inv_col = find_col(listing, ['Total Inventory','total_inventory','inventory','qty','quantity','stock'])

    if not list_sku_col or not list_inv_col:
        st.warning(f"⚠️ Could not auto-detect columns in Listing File. Columns found: {list(listing.columns)}")
        st.info("Expected columns: SKU ID, Total Inventory (or similar names)")
    else:
        inv = listing[[list_sku_col, list_inv_col]].copy()
        inv.columns = ['SKU ID', 'Total_Inv']
        inv['SKU ID']    = inv['SKU ID'].astype(str).str.strip()
        inv['Total_Inv'] = pd.to_numeric(inv['Total_Inv'], errors='coerce').fillna(0)

        # Merge live inventory
        if not live_inv.empty:
            inv_sku_col = find_col(live_inv, ['SKU ID','SKU Id','sku_id','FSN'])
            inv_fbf_col = find_col(live_inv, ['FBF','fbf_inventory','FBF Inventory','fbf qty','fbf stock'])
            inv_b2b_col = find_col(live_inv, ['B2B','b2b_scheduled','Scheduled','scheduled','transit','b2b'])
            if inv_sku_col:
                li = live_inv[[inv_sku_col]].copy()
                li.columns = ['SKU ID']
                li['SKU ID'] = li['SKU ID'].astype(str).str.strip()
                if inv_fbf_col: li['FBF_Inv'] = pd.to_numeric(live_inv[inv_fbf_col], errors='coerce').fillna(0)
                else:           li['FBF_Inv'] = 0
                if inv_b2b_col: li['B2B_Inv'] = pd.to_numeric(live_inv[inv_b2b_col], errors='coerce').fillna(0)
                else:           li['B2B_Inv'] = 0
                inv = inv.merge(li, on='SKU ID', how='left')
                inv['FBF_Inv'] = inv['FBF_Inv'].fillna(0)
                inv['B2B_Inv'] = inv['B2B_Inv'].fillna(0)
            else:
                inv['FBF_Inv'] = 0; inv['B2B_Inv'] = 0
        else:
            inv['FBF_Inv'] = 0; inv['B2B_Inv'] = 0

        # NFBF = Total - FBF - B2B
        inv['NFBF_Inv'] = (inv['Total_Inv'] - inv['FBF_Inv'] - inv['B2B_Inv']).clip(lower=0)

        # Velocity from earn data
        sku_vel = df.groupby('SKU ID').agg(
            Revenue=('Final Sale Amount','sum'), Units=('Final Sale Units','sum'),
            Brand=('Brand','first'), Category=('Category','first')
        ).reset_index()
        sku_vel['SKU ID'] = sku_vel['SKU ID'].astype(str).str.strip()
        days_in_period = max(df['Order Date'].nunique(), 1)
        sku_vel['Daily_Units'] = (sku_vel['Units'] / days_in_period).round(2)
        sku_vel['Daily_Rev']   = (sku_vel['Revenue'] / days_in_period).round(0)

        inv_m = inv.merge(sku_vel, on='SKU ID', how='left')
        inv_m['Brand']       = inv_m['Brand'].fillna('Unknown')
        inv_m['Category']    = inv_m['Category'].fillna('Unknown')
        inv_m['Daily_Units'] = inv_m['Daily_Units'].fillna(0)
        inv_m['Daily_Rev']   = inv_m['Daily_Rev'].fillna(0)

        # Days Cover
        inv_m['FBF_Days']   = (inv_m['FBF_Inv']   / inv_m['Daily_Units'].replace(0,np.nan)).fillna(999).round(0)
        inv_m['NFBF_Days']  = (inv_m['NFBF_Inv']  / inv_m['Daily_Units'].replace(0,np.nan)).fillna(999).round(0)
        inv_m['Total_Days'] = (inv_m['Total_Inv']  / inv_m['Daily_Units'].replace(0,np.nan)).fillna(999).round(0)

        def inv_status(row):
            if row['Total_Inv'] == 0:    return '⚫ No Stock'
            if row['Total_Days'] < 7:    return '🔴 OOS Risk (<7d)'
            if row['Total_Days'] < 14:   return '🟡 Low Stock (<14d)'
            if row['Total_Days'] > 90:   return '🔵 Overstocked (>90d)'
            return '🟢 Healthy'
        inv_m['Stock_Status'] = inv_m.apply(inv_status, axis=1)

        # KPIs
        oos_risk   = len(inv_m[inv_m['Stock_Status']=='🔴 OOS Risk (<7d)'])
        low_stock  = len(inv_m[inv_m['Stock_Status']=='🟡 Low Stock (<14d)'])
        overstock  = len(inv_m[inv_m['Stock_Status']=='🔵 Overstocked (>90d)'])
        no_stock   = len(inv_m[inv_m['Stock_Status']=='⚫ No Stock'])
        healthy_ct = len(inv_m[inv_m['Stock_Status']=='🟢 Healthy'])
        total_fbf_u = inv_m['FBF_Inv'].sum()
        total_b2b_u = inv_m['B2B_Inv'].sum()
        total_nfbf_u= inv_m['NFBF_Inv'].sum()

        i1,i2,i3,i4,i5,i6 = st.columns(6)
        with i1: metric_card("Total SKUs",     len(inv_m),   prefix="", color="#9B59B6")
        with i2: metric_card("🔴 OOS Risk",    oos_risk,     prefix="", color="#E74C3C")
        with i3: metric_card("🟡 Low Stock",   low_stock,    prefix="", color="#F39C12")
        with i4: metric_card("🟢 Healthy",     healthy_ct,   prefix="", color="#2ECC71")
        with i5: metric_card("FBF Units",      int(total_fbf_u),  prefix="", color="#9B59B6")
        with i6: metric_card("B2B Scheduled",  int(total_b2b_u),  prefix="", color="#1ABC9C")

        col1, col2 = st.columns(2)
        with col1:
            inv_split = pd.DataFrame({
                'Type':  ['FBF','Non-FBF (Seller)','B2B Scheduled'],
                'Units': [total_fbf_u, total_nfbf_u, total_b2b_u]
            })
            fig_ip = px.pie(inv_split, values='Units', names='Type',
                            title="Inventory Split: FBF / NFBF / B2B",
                            color_discrete_map={'FBF':'#9B59B6','Non-FBF (Seller)':'#3498DB','B2B Scheduled':'#2ECC71'},
                            hole=0.4)
            dark_fig(fig_ip, 320); st.plotly_chart(fig_ip, use_container_width=True)
        with col2:
            sc = inv_m['Stock_Status'].value_counts().reset_index()
            sc.columns = ['Status','Count']
            fig_sc = px.bar(sc, x='Status', y='Count', color='Status',
                            title="SKU Stock Health Distribution",
                            color_discrete_map={
                                '🔴 OOS Risk (<7d)':'#E74C3C','🟡 Low Stock (<14d)':'#F39C12',
                                '🟢 Healthy':'#2ECC71','🔵 Overstocked (>90d)':'#3498DB','⚫ No Stock':'#555555'
                            })
            dark_fig(fig_sc, 320); st.plotly_chart(fig_sc, use_container_width=True)

        # Brand inventory stack
        brand_inv = inv_m.groupby('Brand').agg(
            Total_Inv=('Total_Inv','sum'), FBF_Inv=('FBF_Inv','sum'),
            NFBF_Inv=('NFBF_Inv','sum'),  B2B_Inv=('B2B_Inv','sum'),
            SKUs=('SKU ID','nunique')
        ).reset_index().sort_values('Total_Inv', ascending=False)
        brand_inv['FBF%'] = (brand_inv['FBF_Inv']/brand_inv['Total_Inv'].replace(0,np.nan)*100).fillna(0).round(1)

        fig_binv = px.bar(brand_inv, x='Brand', y=['FBF_Inv','NFBF_Inv','B2B_Inv'],
                          barmode='stack', title="Brand-wise Inventory Stack (FBF / NFBF / B2B)",
                          color_discrete_map={'FBF_Inv':'#9B59B6','NFBF_Inv':'#3498DB','B2B_Inv':'#2ECC71'},
                          labels={'value':'Units','variable':'Type'})
        dark_fig(fig_binv); st.plotly_chart(fig_binv, use_container_width=True)

        # OOS Risk table
        with st.expander("🔴 OOS Risk SKUs — Immediate Replenishment Needed"):
            oos_df = inv_m[inv_m['Stock_Status'].isin(['🔴 OOS Risk (<7d)','⚫ No Stock'])].sort_values('Daily_Rev', ascending=False).head(30)
            if not oos_df.empty:
                show_cols = ['SKU ID','Brand','Category','Total_Inv','FBF_Inv','NFBF_Inv','B2B_Inv','Total_Days','Daily_Units','Stock_Status']
                oos_show = oos_df[show_cols].copy()
                oos_show['Daily_Units'] = oos_show['Daily_Units'].apply(lambda x: f"{x:.1f}")
                oos_show['Total_Days']  = oos_show['Total_Days'].apply(lambda x: f"{int(x)}d" if x < 999 else "∞")
                st.dataframe(oos_show, use_container_width=True, hide_index=True)
                rev_at_risk = oos_df['Daily_Rev'].sum() * 7
                st.markdown(f"""<div class='warning-box'>
                <b style='color:#e74c3c'>⚠️ {len(oos_df)} SKUs at OOS risk</b><br>
                <span style='color:#aaa;font-size:12px'>Revenue at risk over next 7 days: <b style='color:#e74c3c'>{indian_fmt(rev_at_risk)}</b>. 
                Raise FBF replenishment and B2B scheduled orders immediately.</span></div>""", unsafe_allow_html=True)
            else:
                st.success("✅ No OOS risk SKUs detected.")

        # Overstocked table
        with st.expander("🔵 Overstocked SKUs — Capital Locked"):
            over_df = inv_m[inv_m['Stock_Status']=='🔵 Overstocked (>90d)'].sort_values('Total_Inv', ascending=False).head(30)
            if not over_df.empty:
                show_cols = ['SKU ID','Brand','Category','Total_Inv','FBF_Inv','NFBF_Inv','Total_Days','Daily_Units']
                over_show = over_df[show_cols].copy()
                over_show['Daily_Units'] = over_show['Daily_Units'].apply(lambda x: f"{x:.1f}")
                over_show['Total_Days']  = over_show['Total_Days'].apply(lambda x: f"{int(x)}d" if x < 999 else "∞")
                st.dataframe(over_show, use_container_width=True, hide_index=True)
            else:
                st.success("✅ No overstocked SKUs.")

        # FBF Days Cover — low FBF despite demand
        with st.expander("⚠️ Low FBF Coverage — High Velocity SKUs"):
            low_fbf = inv_m[(inv_m['Daily_Units'] > 0) & (inv_m['FBF_Days'] < 14) & (inv_m['Total_Days'] >= 14)].sort_values('Daily_Rev', ascending=False).head(25)
            if not low_fbf.empty:
                lf_show = low_fbf[['SKU ID','Brand','Category','Total_Inv','FBF_Inv','NFBF_Inv','B2B_Inv','FBF_Days','Total_Days','Daily_Units']].copy()
                lf_show['Daily_Units'] = lf_show['Daily_Units'].apply(lambda x: f"{x:.1f}")
                lf_show['FBF_Days']   = lf_show['FBF_Days'].apply(lambda x: f"{int(x)}d" if x < 999 else "∞")
                lf_show['Total_Days'] = lf_show['Total_Days'].apply(lambda x: f"{int(x)}d" if x < 999 else "∞")
                st.dataframe(lf_show, use_container_width=True, hide_index=True)
                st.markdown(f"<div class='insight-box'><b style='color:#f39c12'>{len(low_fbf)} SKUs</b> have total stock but <14 days FBF cover — move NFBF stock to FBF urgently to protect conversion.</div>", unsafe_allow_html=True)
            else:
                st.success("✅ All high-velocity SKUs have adequate FBF cover.")

        # Full inventory table
        with st.expander("📋 Full Inventory Table — All SKUs"):
            full_show = inv_m[['SKU ID','Brand','Category','Total_Inv','FBF_Inv','NFBF_Inv','B2B_Inv','FBF_Days','NFBF_Days','Total_Days','Daily_Units','Stock_Status']].copy()
            full_show['Daily_Units'] = full_show['Daily_Units'].apply(lambda x: f"{x:.1f}")
            for dc in ['FBF_Days','NFBF_Days','Total_Days']:
                full_show[dc] = full_show[dc].apply(lambda x: f"{int(x)}d" if x < 999 else "∞")
            st.dataframe(full_show.sort_values('Stock_Status'), use_container_width=True, hide_index=True)

        # Search + Inventory correlation
        if not sku_search.empty:
            st.markdown("**🔗 Search Traffic vs Inventory Correlation**")
            sku_search_inv = sku_search.merge(
                inv_m[['SKU ID','Total_Inv','FBF_Inv','Total_Days','Stock_Status']],
                left_on='SKU Id', right_on='SKU ID', how='inner'
            )
            if not sku_search_inv.empty:
                fig_corr = px.scatter(sku_search_inv, x='Views', y='Total_Inv',
                                      size='Revenue', color='Stock_Status',
                                      color_discrete_map={
                                          '🔴 OOS Risk (<7d)':'#E74C3C','🟡 Low Stock (<14d)':'#F39C12',
                                          '🟢 Healthy':'#2ECC71','🔵 Overstocked (>90d)':'#3498DB','⚫ No Stock':'#555555'
                                      },
                                      title="Search Views vs Inventory (bubble=Revenue) — Spot High-Traffic Low-Stock SKUs",
                                      hover_data=['SKU Id','Brand','CVR'])
                dark_fig(fig_corr, 420); st.plotly_chart(fig_corr, use_container_width=True)

                danger = sku_search_inv[
                    (sku_search_inv['Views'] > sku_search_inv['Views'].quantile(0.75)) &
                    (sku_search_inv['Total_Days'] < 14)
                ]
                if not danger.empty:
                    st.markdown(f"""<div class='warning-box'>
                    <b style='color:#e74c3c'>🚨 {len(danger)} High-Traffic SKUs with &lt;14 days cover</b><br>
                    <span style='color:#aaa;font-size:12px'>Top 25% search visibility but critically low stock. 
                    Revenue at risk: <b style='color:#e74c3c'>{indian_fmt(danger['Revenue'].sum())}</b>. Replenish FBF immediately.</span>
                    </div>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════
# SECTION 12: ACTION RECOMMENDATIONS
# ═══════════════════════════════════════════════════════════════
section_header("Strategic Action Recommendations — RCA Engine", "actions", "🎯")

actions = []

latest_month_data = df[df['Month'] == df['Month'].max()]
prev_month_data   = df[df['Month'] == sorted(df['Month'].unique())[-2]] if df['Month'].nunique() > 1 else pd.DataFrame()
latest_rev = latest_month_data['Final Sale Amount'].sum()
prev_rev   = prev_month_data['Final Sale Amount'].sum() if not prev_month_data.empty else 0
mom_g = (latest_rev - prev_rev)/prev_rev*100 if prev_rev > 0 else 0

if mom_g > 10:
    actions.append(("🟢 HIGH PRIORITY","Revenue Acceleration Opportunity",
                    f"Revenue grew {mom_g:.1f}% MoM. Scale FBF replenishment for top 10 SKUs, increase ad budget 20%, expand winning categories.","#2ecc71"))
elif mom_g < -5:
    actions.append(("🔴 CRITICAL","Revenue Decline — Immediate Action",
                    f"Revenue declined {abs(mom_g):.1f}% MoM. Check top 5 SKU stock, compare cancel rates, review search visibility.","#e74c3c"))

if cancel_rate > 20:
    actions.append(("🔴 CRITICAL","Cancellation Rate Crisis",
                    f"Cancel rate {cancel_rate:.1f}% — severely above 18% threshold. Revenue leaking {indian_fmt(total_cancel)}. Audit top-cancel SKUs, push FBF migration.","#e74c3c"))
elif cancel_rate > 15:
    actions.append(("🟡 WATCH","Elevated Cancellation Rate",
                    f"Cancel rate {cancel_rate:.1f}% above 15% safe threshold. Focus on top 10 cancel-rate SKUs, migrate to FBF.","#f39c12"))

if fbf_pct < 60:
    actions.append(("🔴 CRITICAL","FBF Penetration Below Target",
                    f"FBF at {fbf_pct:.1f}% vs 65%+ target. NFBF cancel rate {(nfbf_row['Cancel_Rate%'] if nfbf_row is not None else 0):.1f}% vs FBF {(fbf_row['Cancel_Rate%'] if fbf_row is not None else 0):.1f}%. Migrate top 20 NFBF SKUs.","#e74c3c"))

if nf_share < 20:
    actions.append(("🟡 WATCH","Non-Fragrance Underperforming",
                    f"Non-Frag at {nf_share:.1f}% vs 25% target. Launch non-frag exclusives, push skincare bundles via Shopsy.","#f39c12"))

kenaz_rev = df[df['Brand']=='Kenaz']['Final Sale Amount'].sum()
kenaz_pct = kenaz_rev / total_rev * 100
if kenaz_pct > 2:
    kenaz_ctr_vals = search_brand[search_brand['Brand']=='Kenaz']['CTR'].values
    kenaz_ctr_str  = f"{kenaz_ctr_vals[0]:.1f}%" if len(kenaz_ctr_vals) > 0 else "N/A"
    actions.append(("🟢 OPPORTUNITY","Kenaz Showing Strong Growth Signal",
                    f"Kenaz at {kenaz_pct:.1f}% revenue share, search CTR {kenaz_ctr_str}. Scale FBF, boost ad spend 30%, launch on Shopsy.","#2ecc71"))

if avg_cvr < 3:
    actions.append(("🟡 WATCH","Search Conversion Below Benchmark",
                    f"CVR {avg_cvr:.2f}% vs 3% benchmark. {len(sku_search[(sku_search['Views']>50000)&(sku_search['CVR']<2)])} high-traffic low-conversion SKUs — fix listing quality, pricing, images.","#f39c12"))

if proj_g and proj_g > 5:
    actions.append(("🟢 OPPORTUNITY","Current Month on Track for Record",
                    f"Projected {indian_fmt(proj)} ({proj_g:+.1f}% vs prev). Push flash sales, stock FBF top 5 SKUs, activate search ads.","#2ecc71"))

if return_rate > 5:
    actions.append(("🟡 WATCH","Return Rate Needs Attention",
                    f"Return rate {return_rate:.1f}%. Run return reason analysis, quality audit on top-return SKUs.","#f39c12"))

if top_brand_share > 90:
    actions.append(("🟡 WATCH","Portfolio Concentration Risk",
                    f"{top_brand} at {top_brand_share:.1f}% — over-concentrated. Build Kenaz to ₹50L/month, HipHop to ₹20L, Embarouge to ₹15L.","#f39c12"))

# Inventory actions
if not listing.empty and 'inv_m' in dir():
    if oos_risk > 0:
        actions.append(("🔴 CRITICAL","OOS Risk — Immediate Replenishment",
                        f"{oos_risk} SKUs have <7 days stock cover. Revenue at risk: {indian_fmt(inv_m[inv_m['Stock_Status']=='🔴 OOS Risk (<7d)']['Daily_Rev'].sum()*7)} over next 7 days. Raise FBF & B2B orders now.","#e74c3c"))
    if overstock > 0:
        actions.append(("🟡 WATCH","Overstocked SKUs — Free Up Capital",
                        f"{overstock} SKUs have >90 days cover. Review pricing, run promotions, or liquidate slow movers to free working capital.","#f39c12"))

st.markdown(f"**{len(actions)} Strategic Actions Generated Based on Current Data**")
for priority, title, rec, color in actions:
    st.markdown(f"""
    <div style='background:rgba(0,0,0,0.2);border:1px solid {color}44;border-left:4px solid {color};
                border-radius:10px;padding:16px 20px;margin:10px 0'>
        <div style='display:flex;align-items:center;gap:10px;margin-bottom:8px'>
            <span style='background:{color}22;border:1px solid {color}66;color:{color};font-size:10px;
                         font-weight:700;padding:2px 8px;border-radius:4px;letter-spacing:1px'>{priority}</span>
            <span style='color:white;font-weight:700;font-size:14px'>{title}</span>
        </div>
        <div style='color:#aaa;font-size:13px;line-height:1.7'>{rec}</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>**Brand-Level Action Plans**", unsafe_allow_html=True)
for brand_name in brand_sum['Brand'].head(4).tolist():
    b_df  = df[df['Brand']==brand_name]
    b_rev = b_df['Final Sale Amount'].sum()
    b_cr  = (b_df['Cancellation Amount'].sum()/(b_rev+b_df['Cancellation Amount'].sum()))*100 if b_rev > 0 else 0
    b_fbf = df[(df['Brand']==brand_name)&(df['Fulfillment Type']=='FBF')]['Final Sale Amount'].sum()/b_rev*100 if b_rev > 0 else 0
    b_mom = compute_mom_growth(df, brand_name)
    b_actions = []
    if b_cr > 18:           b_actions.append(f"⚠️ Cancel rate {b_cr:.1f}% — audit SKUs, push FBF")
    if b_fbf < 60:          b_actions.append(f"📦 FBF {b_fbf:.1f}% — migrate high-velocity SKUs")
    if b_mom and b_mom<-5:  b_actions.append(f"📉 MoM decline {b_mom:.1f}% — investigate demand drop")
    if b_mom and b_mom>10:  b_actions.append(f"🚀 MoM growth {b_mom:.1f}% — scale inventory & ads")
    if not b_actions:       b_actions.append("✅ Metrics stable — focus on growth levers")
    with st.expander(f"{brand_name} — {indian_fmt(b_rev)} | Cancel {b_cr:.1f}% | FBF {b_fbf:.1f}%"):
        for a in b_actions: st.markdown(f"• {a}")

# ═══════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════
st.markdown("<br><hr style='border-color:#1e1e40'>", unsafe_allow_html=True)
inv_status_line = f" · {len(inv_m):,} inventory SKUs" if not listing.empty and 'inv_m' in dir() else ""
st.markdown(f"""
<div style='text-align:center;color:#444466;font-size:11px;padding:10px 0'>
    Flipkart Business Intelligence Dashboard · One Guardian · Enterprise Analytics Engine<br>
    {len(earn):,} earn records · {len(search):,} search records · {len(master):,} master SKUs{inv_status_line}
</div>""", unsafe_allow_html=True)
