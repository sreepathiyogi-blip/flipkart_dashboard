import streamlit as st
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings("ignore")

st.set_page_config(page_title="Flipkart Sales Dashboard", page_icon="🛒", layout="wide")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
REQUIRED_COLS = ["Product Id","SKU ID","Category","Brand","Vertical","Order Date","Fulfillment Type",
                 "Location Id","Gross Units","GMV","Cancellation Units","Cancellation Amount",
                 "Return Units","Return Amount","Final Sale Units","Final Sale Amount"]
NUMERIC_COLS = ["Gross Units","GMV","Cancellation Units","Cancellation Amount",
                "Return Units","Return Amount","Final Sale Units","Final Sale Amount"]
BRAND_COLORS = {"Bellavita":"#6C3483","Kenaz":"#1A5276","Embarouge":"#C0392B","HipHop Skincare":"#117A65","Guzz":"#D4AC0D"}
BELLAVITA_NAMES = ["BELLAVITA","Bella vita organic","Bellavita","bella vita","BELLA VITA ORGANIC","bellavita"]
CHANNEL_COLORS = {"Shopsy":"#E67E22","National":"#2E86C1"}

# ─── NORMALIZERS ──────────────────────────────────────────────────────────────
def normalize_brands(df):
    df = df.copy()
    df["Brand"] = df["Brand"].astype(str).str.strip()
    df["Brand"] = df["Brand"].apply(lambda x: "Bellavita" if x in BELLAVITA_NAMES else x)
    return df

def add_channel(df):
    """Derive Channel from Vertical: starts with 'Shopsy' → Shopsy, else National"""
    df = df.copy()
    df["Channel"] = df["Vertical"].astype(str).apply(
        lambda v: "Shopsy" if v.strip().lower().startswith("shopsy") else "National"
    )
    return df

# ─── GOOGLE SHEETS ────────────────────────────────────────────────────────────
@st.cache_resource
def get_gsheet_client():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    return gspread.authorize(creds)

def get_or_create_sheet(client, name):
    try: return client.open(name)
    except gspread.SpreadsheetNotFound:
        sh = client.create(name)
        sh.share(st.secrets["gcp_service_account"]["client_email"], perm_type="user", role="writer")
        return sh

def clean_df(df):
    for col in df.columns:
        if df[col].dtype == object or str(df[col].dtype) == "string":
            df[col] = df[col].fillna("").astype(str)
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).replace([float('inf'),float('-inf')],0)
    return df

@st.cache_data(ttl=300)
def load_data(spreadsheet_name):
    try:
        client = get_gsheet_client()
        ws = get_or_create_sheet(client, spreadsheet_name).sheet1
        data = ws.get_all_records()
        if not data: return pd.DataFrame(columns=REQUIRED_COLS)
        df = pd.DataFrame(data)
        df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce")
        for col in NUMERIC_COLS:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        df = normalize_brands(df)
        df = add_channel(df)
        return df
    except Exception as e:
        st.error(f"Load error: {e}"); return pd.DataFrame(columns=REQUIRED_COLS)

def save_data(client, new_df, spreadsheet_name):
    sh = get_or_create_sheet(client, spreadsheet_name)
    ws = sh.sheet1
    existing = ws.get_all_records()
    new_df = clean_df(normalize_brands(add_channel(new_df.copy())))
    if not existing:
        ws.update([new_df.columns.tolist()] + new_df.astype(str).values.tolist())
        return len(new_df), 0
    ex = normalize_brands(add_channel(pd.DataFrame(existing)))
    ex["Order Date"] = pd.to_datetime(ex["Order Date"], errors="coerce")
    new_df["Order Date"] = pd.to_datetime(new_df["Order Date"], errors="coerce")
    keys = ["Product Id","SKU ID","Order Date","Brand"]
    ex_keys = ex[keys].astype(str).apply("_".join, axis=1)
    new_keys = new_df[keys].astype(str).apply("_".join, axis=1)
    truly_new = new_df[~new_keys.isin(ex_keys)]
    if len(truly_new) == 0: return 0, len(new_df)
    all_cols = list(dict.fromkeys(ex.columns.tolist() + truly_new.columns.tolist()))
    ex = ex.reindex(columns=all_cols, fill_value="")
    truly_new = truly_new.reindex(columns=all_cols, fill_value="")
    combined = pd.concat([ex, truly_new], ignore_index=True)
    combined["Order Date"] = pd.to_datetime(combined["Order Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    combined = clean_df(combined)
    ws.clear()
    ws.update([combined.columns.tolist()] + combined.astype(str).values.tolist())
    return len(truly_new), len(new_df) - len(truly_new)

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def safe_pct(new, old): return round((new-old)/old*100,1) if old != 0 else None

def pct_badge(pct, inverse=False):
    if pct is None: return "<span style='color:#888'>—</span>"
    good = (pct>=0) if not inverse else (pct<=0)
    c = "#2ecc71" if good else "#e74c3c"
    s = "▲" if pct>=0 else "▼"
    return f"<span style='color:{c};font-weight:600'>{s} {abs(pct):.1f}%</span>"

def metric_card(label, val, delta="", prefix="₹", suffix=""):
    vs = f"{prefix}{val:,.0f}{suffix}"
    st.markdown(f"""<div style='background:#1a1a2e;padding:14px 18px;border-radius:10px;
        border-left:4px solid #6C3483;margin-bottom:6px'>
        <div style='color:#aaa;font-size:11px'>{label}</div>
        <div style='color:#fff;font-size:20px;font-weight:700'>{vs}</div>
        <div style='font-size:11px;margin-top:3px'>{delta}</div></div>""", unsafe_allow_html=True)

def sec_hdr(title, anchor):
    st.markdown(f"<h2 id='{anchor}' style='color:#D7BDE2;border-bottom:2px solid #6C3483;"
                f"padding-bottom:6px;margin-top:30px'>{title}</h2>", unsafe_allow_html=True)

def combined_chart(data, x, title):
    fig = make_subplots(specs=[[{"secondary_y":True}]])
    fig.add_trace(go.Bar(x=data[x],y=data["Final_Sale"],name="Final Sale (₹)",
                         marker_color="#6C3483",opacity=0.85), secondary_y=False)
    fig.add_trace(go.Scatter(x=data[x],y=data["Cancellation"],name="Cancellation (₹)",
                             line=dict(color="#e74c3c",width=2.5),mode="lines+markers"), secondary_y=True)
    fig.add_trace(go.Scatter(x=data[x],y=data["Returns"],name="Returns (₹)",
                             line=dict(color="#e67e22",width=2.5,dash="dot"),mode="lines+markers"), secondary_y=True)
    fig.update_layout(title=title,template="plotly_dark",height=400,
                      legend=dict(orientation="h",y=1.12),hovermode="x unified")
    fig.update_yaxes(title_text="Final Sale (₹)",secondary_y=False)
    fig.update_yaxes(title_text="Cancel + Returns (₹)",secondary_y=True)
    return fig

def pct_color(val):
    try:
        v = float(str(val).replace("%",""))
        if v > 0: return "color: #2ecc71; font-weight:600"
        if v < 0: return "color: #e74c3c; font-weight:600"
    except: pass
    return ""

def render_table(df, fmt, pct_cols=[]):
    styled = df.style.format(fmt, na_rep="—")
    for col in pct_cols:
        if col in df.columns:
            fn = getattr(styled, "map", None) or getattr(styled, "applymap", None)
            styled = fn(lambda v: pct_color(v), subset=[col])
    st.dataframe(styled, use_container_width=True, hide_index=True)

# ─── AGGREGATIONS ─────────────────────────────────────────────────────────────
def daily_agg(df):
    df = df.copy(); df["Order Date"] = pd.to_datetime(df["Order Date"])
    return df.groupby("Order Date").agg(
        Final_Sale=("Final Sale Amount","sum"),
        Cancellation=("Cancellation Amount","sum"),
        Returns=("Return Amount","sum"),
        Sale_Units=("Final Sale Units","sum")
    ).reset_index().sort_values("Order Date")

def dod_data(df):
    d = daily_agg(df)
    d["DoD_Sale_%"] = d["Final_Sale"].pct_change()*100
    d["DoD_Cancel_%"] = d["Cancellation"].pct_change()*100
    d["DoD_Return_%"] = d["Returns"].pct_change()*100
    return d

def wow_data(df):
    df = df.copy(); df["Order Date"] = pd.to_datetime(df["Order Date"])
    df["Week"] = df["Order Date"].dt.to_period("W").apply(lambda r: r.start_time)
    w = df.groupby("Week").agg(
        Final_Sale=("Final Sale Amount","sum"),
        Cancellation=("Cancellation Amount","sum"),
        Returns=("Return Amount","sum")
    ).reset_index().sort_values("Week")
    w["WoW_Sale_%"] = w["Final_Sale"].pct_change()*100
    w["WoW_Cancel_%"] = w["Cancellation"].pct_change()*100
    w["WoW_Return_%"] = w["Returns"].pct_change()*100
    return w

def mom_data(df):
    df = df.copy(); df["Order Date"] = pd.to_datetime(df["Order Date"])
    df["Month"] = df["Order Date"].dt.to_period("M").apply(lambda r: r.start_time)
    m = df.groupby("Month").agg(
        Final_Sale=("Final Sale Amount","sum"),
        Cancellation=("Cancellation Amount","sum"),
        Returns=("Return Amount","sum")
    ).reset_index().sort_values("Month")
    m["MoM_Sale_%"] = m["Final_Sale"].pct_change()*100
    m["MoM_Cancel_%"] = m["Cancellation"].pct_change()*100
    m["MoM_Return_%"] = m["Returns"].pct_change()*100
    return m

def declining_skus(df, top_n=15):
    try:
        df = df.copy(); df["Order Date"] = pd.to_datetime(df["Order Date"])
        df["Week"] = df["Order Date"].dt.to_period("W").apply(lambda r: r.start_time)
        weeks = sorted(df["Week"].unique())
        if len(weeks) < 2: return pd.DataFrame()
        lw = df[df["Week"]==weeks[-1]].groupby("SKU ID")["Final Sale Amount"].sum()
        pw = df[df["Week"]==weeks[-2]].groupby("SKU ID")["Final Sale Amount"].sum()
        lw.index = lw.index.astype(str); pw.index = pw.index.astype(str)
        cmp = pd.DataFrame({"Last Week":lw,"Prev Week":pw}).fillna(0)
        cmp["Change %"] = ((cmp["Last Week"]-cmp["Prev Week"])/cmp["Prev Week"].replace(0,np.nan)*100).round(1)
        cmp = cmp[cmp["Prev Week"]>0].sort_values("Change %")
        cmp = cmp[cmp["Change %"]<0].head(top_n).reset_index()
        cmp.columns = ["SKU ID","Last Week","Prev Week","Change %"]
        meta = df[["SKU ID","Brand","Category","Channel"]].drop_duplicates("SKU ID").copy()
        meta["SKU ID"] = meta["SKU ID"].astype(str)
        return cmp.merge(meta, on="SKU ID", how="left")
    except: return pd.DataFrame()

def action_points(df):
    actions = []
    df = df.copy(); df["Order Date"] = pd.to_datetime(df["Order Date"])
    dates = sorted(df["Order Date"].unique())
    if len(dates)>=2:
        td=df[df["Order Date"]==dates[-1]]; yd=df[df["Order Date"]==dates[-2]]
        ts,tc,tr = td["Final Sale Amount"].sum(),td["Cancellation Amount"].sum(),td["Return Amount"].sum()
        ys,yc,yr = yd["Final Sale Amount"].sum(),yd["Cancellation Amount"].sum(),yd["Return Amount"].sum()
        sp=safe_pct(ts,ys)
        if sp and sp<-15: actions.append(f"🔴 **Sales dropped {abs(sp):.1f}% DoD** (₹{ys:,.0f}→₹{ts:,.0f}). Check top SKU stock & listing.")
        elif sp and sp>20: actions.append(f"🟢 **Sales grew {sp:.1f}% DoD** (₹{ys:,.0f}→₹{ts:,.0f}). Identify driver & scale.")
        cp=safe_pct(tc,yc)
        if cp and cp>20: actions.append(f"🔴 **Cancellations spiked {cp:.1f}% DoD**. Check pricing, promise dates & inventory.")
        rp=safe_pct(tr,yr)
        if rp and rp>20: actions.append(f"🟡 **Returns up {rp:.1f}% DoD**. Review return reasons.")
        if ts+tc>0:
            cr=tc/(ts+tc)*100
            if cr>15: actions.append(f"🔴 **Cancellation rate {cr:.1f}%** on latest day. Investigate fill rate.")
    dec=declining_skus(df,5)
    if not dec.empty:
        skus=", ".join(dec["SKU ID"].head(3).astype(str).tolist())
        actions.append(f"📉 **Top declining SKUs (WoW):** {skus}. Run ads boost or price correction.")
    sg=df.groupby("SKU ID").agg(sale=("Final Sale Amount","sum"),cancel=("Cancellation Amount","sum"))
    sg["cr"]=sg["cancel"]/(sg["sale"]+sg["cancel"]).replace(0,np.nan)
    bad=sg[(sg["cr"]>0.3)&(sg["sale"]>1000)]
    if not bad.empty: actions.append(f"⚠️ **{len(bad)} SKUs with >30% cancel rate**: {', '.join(bad.index.astype(str)[:3].tolist())}")
    months=sorted(df["Order Date"].dt.to_period("M").unique())
    if len(months)>=2:
        m1=df[df["Order Date"].dt.to_period("M")==months[-1]]["Final Sale Amount"].sum()
        m0=df[df["Order Date"].dt.to_period("M")==months[-2]]["Final Sale Amount"].sum()
        mp=safe_pct(m1,m0)
        if mp and mp<-10: actions.append(f"📉 **MoM sales declined {abs(mp):.1f}%**. Review brand contribution & push promos.")
    # Channel-specific
    if "Channel" in df.columns:
        ch=df.groupby("Channel")["Final Sale Amount"].sum()
        for ch_name, ch_val in ch.items():
            pct_share = ch_val/df["Final Sale Amount"].sum()*100
            actions.append(f"📊 **{ch_name} channel:** ₹{ch_val:,.0f} ({pct_share:.1f}% of total sale)")
    if not actions: actions.append("✅ All metrics look healthy. Push exclusives scale-up & monitor DoD.")
    return actions

# ─── CHANNEL SECTION ──────────────────────────────────────────────────────────
def render_channel_section(df, channel_name, anchor):
    ch_df = df[df["Channel"]==channel_name].copy()
    if ch_df.empty:
        st.info(f"No {channel_name} data in selected date range.")
        return
    st.markdown(f"<div id='{anchor}'></div>", unsafe_allow_html=True)
    sec_hdr(f"{'🛍️' if channel_name=='Shopsy' else '🏪'} {channel_name} Channel", anchor)

    dates = sorted(pd.to_datetime(ch_df["Order Date"]).unique())
    td = ch_df[pd.to_datetime(ch_df["Order Date"])==dates[-1]] if dates else pd.DataFrame()
    yd = ch_df[pd.to_datetime(ch_df["Order Date"])==dates[-2]] if len(dates)>=2 else pd.DataFrame()
    ts = td["Final Sale Amount"].sum() if not td.empty else 0
    tc = td["Cancellation Amount"].sum() if not td.empty else 0
    tr = td["Return Amount"].sum() if not td.empty else 0
    ys = yd["Final Sale Amount"].sum() if not yd.empty else 0
    yc = yd["Cancellation Amount"].sum() if not yd.empty else 0
    yr = yd["Return Amount"].sum() if not yd.empty else 0
    cr = tc/(ts+tc)*100 if (ts+tc)>0 else 0

    c1,c2,c3,c4 = st.columns(4)
    with c1: metric_card(f"Today's Sale ({channel_name})", ts, pct_badge(safe_pct(ts,ys)))
    with c2: metric_card("Cancellation", tc, pct_badge(safe_pct(tc,yc), inverse=True))
    with c3: metric_card("Returns", tr, pct_badge(safe_pct(tr,yr), inverse=True))
    with c4: metric_card("Cancel Rate", cr, prefix="", suffix="%")

    # Brand-wise within channel
    bg = ch_df.groupby("Brand").agg(
        Final_Sale=("Final Sale Amount","sum"),
        Cancellation=("Cancellation Amount","sum"),
        Returns=("Return Amount","sum"),
        Units=("Final Sale Units","sum")
    ).reset_index().sort_values("Final_Sale", ascending=False)
    bg["Cancel Rate %"] = (bg["Cancellation"]/(bg["Final_Sale"]+bg["Cancellation"]).replace(0,np.nan)*100).round(1)

    ca, cb = st.columns([3,2])
    with ca:
        st.plotly_chart(px.bar(bg,x="Brand",y=["Final_Sale","Cancellation","Returns"],barmode="group",
            template="plotly_dark",title=f"{channel_name}: Brand-wise Sale vs Cancel vs Returns",
            color_discrete_map={"Final_Sale":"#6C3483","Cancellation":"#e74c3c","Returns":"#e67e22"},
            labels={"value":"₹","variable":"Metric"}), use_container_width=True)
    with cb:
        st.plotly_chart(px.pie(bg,values="Final_Sale",names="Brand",title=f"{channel_name}: Sale Share",
            template="plotly_dark",color_discrete_sequence=px.colors.sequential.Purples_r), use_container_width=True)

    render_table(bg.rename(columns={"Final_Sale":"Final Sale (₹)","Cancellation":"Cancel (₹)","Returns":"Returns (₹)","Units":"Units Sold"}),
                 {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Returns (₹)":"₹{:,.0f}","Units Sold":"{:,.0f}","Cancel Rate %":"{:.1f}%"})

    # Daily trend
    st.plotly_chart(combined_chart(daily_agg(ch_df),"Order Date",
                    f"{channel_name}: Daily Final Sale (Bar) | Cancel & Returns (Line)"), use_container_width=True)

    # DoD table
    with st.expander(f"📅 {channel_name} DoD % Change Table"):
        dod = dod_data(ch_df)
        dd = dod.copy(); dd["Order Date"] = dd["Order Date"].dt.strftime("%d %b %Y")
        dd = dd.rename(columns={"Final_Sale":"Final Sale (₹)","DoD_Sale_%":"DoD Sale %",
                                 "Cancellation":"Cancel (₹)","DoD_Cancel_%":"DoD Cancel %",
                                 "Returns":"Returns (₹)","DoD_Return_%":"DoD Return %","Sale_Units":"Units"})
        render_table(dd[["Order Date","Final Sale (₹)","DoD Sale %","Cancel (₹)","DoD Cancel %","Returns (₹)","DoD Return %","Units"]],
                     {"Final Sale (₹)":"₹{:,.0f}","DoD Sale %":"{:.1f}%","Cancel (₹)":"₹{:,.0f}",
                      "DoD Cancel %":"{:.1f}%","Returns (₹)":"₹{:,.0f}","DoD Return %":"{:.1f}%","Units":"{:,.0f}"},
                     pct_cols=["DoD Sale %","DoD Cancel %","DoD Return %"])

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    st.markdown("""<style>
    .main,.stApp{background:#0f0f1a;color:white}
    .block-container{padding-top:1rem}
    div[data-testid="stSidebarContent"]{background:#12122a}
    </style>""", unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("## 🛒 Flipkart Dashboard\n**One Guardian**")
        st.markdown("---")
        spreadsheet_name = st.text_input("Google Sheet Name", "Flipkart_Sales_DB")

        st.markdown("### 📤 Upload Data")
        uploaded = st.file_uploader("Earn More Report (.xlsx)", type=["xlsx","xls"])
        if uploaded:
            try:
                raw = pd.read_excel(uploaded)
                missing = [c for c in REQUIRED_COLS if c not in raw.columns]
                if missing: st.error(f"Missing: {missing}")
                else:
                    raw["Order Date"] = pd.to_datetime(raw["Order Date"],errors="coerce").dt.strftime("%Y-%m-%d")
                    raw = clean_df(raw)
                    raw = normalize_brands(raw)
                    raw = add_channel(raw)
                    extra = [c for c in raw.columns if c not in REQUIRED_COLS+["Channel"]]
                    st.success(f"✅ {len(raw)} rows | {raw['Order Date'].min()} → {raw['Order Date'].max()}")
                    ch_counts = raw["Channel"].value_counts()
                    st.info(f"🏪 National: {ch_counts.get('National',0):,} rows | 🛍️ Shopsy: {ch_counts.get('Shopsy',0):,} rows")
                    if extra: st.info(f"📌 New columns: {extra}")
                    if st.button("💾 Save to Google Sheets", type="primary"):
                        with st.spinner("Saving..."):
                            client = get_gsheet_client()
                            added, dupes = save_data(client, raw, spreadsheet_name)
                        st.success(f"✅ {added} new rows. {dupes} duplicates skipped.")
                        st.cache_data.clear()
            except Exception as e: st.error(f"Error: {e}")

        st.markdown("---")
        st.markdown("### 🔧 Database Tools")
        if st.button("➕ Add Channel column to Sheet", help="One-time fix: adds National/Shopsy to all existing rows"):
            with st.spinner("Updating Google Sheet..."):
                try:
                    client = get_gsheet_client()
                    sh = get_or_create_sheet(client, spreadsheet_name)
                    ws = sh.sheet1
                    data = ws.get_all_records()
                    if not data:
                        st.warning("Sheet is empty.")
                    else:
                        df_migrate = pd.DataFrame(data)
                        df_migrate["Channel"] = df_migrate["Vertical"].apply(
                            lambda v: "Shopsy" if str(v).strip().lower().startswith("shopsy") else "National"
                        )
                        df_migrate = df_migrate.fillna("").replace(["nan","NaT"], "")
                        ws.clear()
                        ws.update([df_migrate.columns.tolist()] + df_migrate.astype(str).values.tolist())
                        ch = df_migrate["Channel"].value_counts()
                        st.success(f"✅ Done! {len(df_migrate):,} rows updated.\n\n"
                                   f"🏪 National: {ch.get('National',0):,} | 🛍️ Shopsy: {ch.get('Shopsy',0):,}")
                        st.cache_data.clear()
                except Exception as e:
                    st.error(f"Migration error: {e}")

        df_raw = load_data(spreadsheet_name)
        if df_raw.empty:
            st.info("No data yet. Upload a file above.")
            st.stop()

        st.markdown("---\n### 🔍 Filters")

        # Date filter
        df_raw["Order Date"] = pd.to_datetime(df_raw["Order Date"])
        min_date = df_raw["Order Date"].min().date()
        max_date = df_raw["Order Date"].max().date()
        date_range = st.date_input("📅 Date Range", value=(min_date, max_date),
                                   min_value=min_date, max_value=max_date)
        if isinstance(date_range, (list,tuple)) and len(date_range)==2:
            start_date, end_date = date_range[0], date_range[1]
        else:
            start_date, end_date = min_date, max_date
        df_raw = df_raw[(df_raw["Order Date"].dt.date>=start_date) & (df_raw["Order Date"].dt.date<=end_date)]

        # Channel filter
        channel_f = st.selectbox("📡 Channel", ["All","National","Shopsy"])

        # Brand filter
        brands = ["All"] + sorted(df_raw["Brand"].dropna().unique().tolist())
        brand_f = st.selectbox("🏷️ Brand", brands)

        # Category filter
        frag_kw = ["fragrance","perfume","deodorant","deo","edt","edp","attar","body mist","body spray"]
        if brand_f == "Bellavita":
            bv_cats = df_raw[df_raw["Brand"]=="Bellavita"]["Category"].dropna().unique().tolist()
            frag_cats = [c for c in bv_cats if any(k in str(c).lower() for k in frag_kw)]
            nonfrag_cats = [c for c in bv_cats if c not in frag_cats]
            cat_f = st.selectbox("📦 Category (Bellavita)", ["All","Fragrance","Non-Fragrance"])
        else:
            all_cats = ["All"] + sorted(df_raw["Category"].dropna().unique().tolist())
            cat_f = st.selectbox("📦 Category", all_cats)
            frag_cats, nonfrag_cats = [], []

        st.markdown("---\n### 📍 Jump To")
        nav_items = [
            ("📊 Overview","overview"),
            ("🏪 National Channel","national"),
            ("🛍️ Shopsy Channel","shopsy"),
            ("📅 DoD Analysis","dod"),
            ("📆 WoW Analysis","wow"),
            ("🗓️ MoM Analysis","mom"),
            ("📉 Declining SKUs","declining"),
            ("🎯 Action Points","actions"),
        ]
        if brand_f == "Bellavita": nav_items.insert(1,("🌸 Fragrance vs Non-Frag","fragrance"))
        excl_col = next((c for c in df_raw.columns if "exclusive" in c.lower()), None)
        if excl_col: nav_items.append(("⭐ Exclusives","exclusives"))
        for label, anchor in nav_items:
            st.markdown(f"<a href='#{anchor}' style='color:#A569BD;text-decoration:none;font-size:13px'>→ {label}</a>",
                        unsafe_allow_html=True)

    # ── APPLY ALL FILTERS ─────────────────────────────────────────────────────
    df = df_raw.copy()
    disp = df.copy()
    if channel_f != "All": disp = disp[disp["Channel"]==channel_f]
    if brand_f != "All": disp = disp[disp["Brand"]==brand_f]
    if cat_f != "All":
        if brand_f == "Bellavita":
            disp = disp[disp["Category"].isin(frag_cats if cat_f=="Fragrance" else nonfrag_cats)]
        else:
            disp = disp[disp["Category"]==cat_f]

    disp["Order Date"] = pd.to_datetime(disp["Order Date"])
    dates = sorted(disp["Order Date"].unique())
    has_wow = disp["Order Date"].dt.to_period("W").nunique()>=2
    has_mom = disp["Order Date"].dt.to_period("M").nunique()>=2

    # ── HEADER ────────────────────────────────────────────────────────────────
    st.title(f"🛒 Flipkart Dashboard — {brand_f} | {channel_f}")
    if dates:
        ch_counts = df["Channel"].value_counts()
        st.caption(
            f"📅 {dates[0].strftime('%d %b %Y')} → {dates[-1].strftime('%d %b %Y')} | "
            f"{len(disp):,} rows | "
            f"🏪 National: {ch_counts.get('National',0):,} | "
            f"🛍️ Shopsy: {ch_counts.get('Shopsy',0):,}"
        )

    # ════════════════════════════════════════════════════════════════════
    # 1. OVERVIEW
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='overview'></div>", unsafe_allow_html=True)
    sec_hdr("📊 Overview","overview")

    if dates:
        td=disp[disp["Order Date"]==dates[-1]]
        yd=disp[disp["Order Date"]==dates[-2]] if len(dates)>=2 else pd.DataFrame()
        ts,tc,tr=td["Final Sale Amount"].sum(),td["Cancellation Amount"].sum(),td["Return Amount"].sum()
        ys=yd["Final Sale Amount"].sum() if not yd.empty else 0
        yc=yd["Cancellation Amount"].sum() if not yd.empty else 0
        yr=yd["Return Amount"].sum() if not yd.empty else 0
        cr=tc/(ts+tc)*100 if (ts+tc)>0 else 0
        c1,c2,c3,c4,c5=st.columns(5)
        with c1: metric_card("Today's Final Sale",ts,pct_badge(safe_pct(ts,ys)))
        with c2: metric_card("Today's Cancellation",tc,pct_badge(safe_pct(tc,yc),inverse=True))
        with c3: metric_card("Today's Returns",tr,pct_badge(safe_pct(tr,yr),inverse=True))
        with c4: metric_card("Cancel Rate",cr,prefix="",suffix="%")
        with c5: metric_card("Total Sale (Period)",disp["Final Sale Amount"].sum())

    # Channel split KPIs
    st.markdown("#### 📡 National vs Shopsy — Total Period")
    ch_grp = df.groupby("Channel").agg(
        Final_Sale=("Final Sale Amount","sum"),
        Cancellation=("Cancellation Amount","sum"),
        Returns=("Return Amount","sum"),
        Units=("Final Sale Units","sum")
    ).reset_index()
    ch_grp["Cancel Rate %"]=(ch_grp["Cancellation"]/(ch_grp["Final_Sale"]+ch_grp["Cancellation"]).replace(0,np.nan)*100).round(1)

    ca,cb=st.columns(2)
    with ca:
        st.plotly_chart(px.bar(ch_grp,x="Channel",y=["Final_Sale","Cancellation","Returns"],
            barmode="group",template="plotly_dark",title="National vs Shopsy: Sale, Cancel, Returns",
            color_discrete_map={"Final_Sale":"#6C3483","Cancellation":"#e74c3c","Returns":"#e67e22"},
            labels={"value":"₹","variable":"Metric"}), use_container_width=True)
    with cb:
        st.plotly_chart(px.pie(ch_grp,values="Final_Sale",names="Channel",
            title="Sale Share: National vs Shopsy",template="plotly_dark",
            color_discrete_map={"National":"#2E86C1","Shopsy":"#E67E22"}), use_container_width=True)
    render_table(ch_grp.rename(columns={"Final_Sale":"Final Sale (₹)","Cancellation":"Cancel (₹)","Returns":"Returns (₹)","Units":"Units Sold"}),
                 {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Returns (₹)":"₹{:,.0f}","Units Sold":"{:,.0f}","Cancel Rate %":"{:.1f}%"})

    # Brand-wise
    st.markdown("#### 🏷️ Brand-wise Performance")
    bg=df.groupby(["Brand","Channel"]).agg(Final_Sale=("Final Sale Amount","sum"),Cancellation=("Cancellation Amount","sum"),Returns=("Return Amount","sum")).reset_index()
    st.plotly_chart(px.bar(bg,x="Brand",y="Final_Sale",color="Channel",barmode="group",
        template="plotly_dark",title="Brand-wise Final Sale by Channel",
        color_discrete_map=CHANNEL_COLORS,labels={"Final_Sale":"Final Sale (₹)"}), use_container_width=True)

    bg2=df.groupby("Brand").agg(Final_Sale=("Final Sale Amount","sum"),Cancellation=("Cancellation Amount","sum"),Returns=("Return Amount","sum"),Units=("Final Sale Units","sum")).reset_index().sort_values("Final_Sale",ascending=False)
    bg2["Cancel Rate %"]=(bg2["Cancellation"]/(bg2["Final_Sale"]+bg2["Cancellation"]).replace(0,np.nan)*100).round(1)
    render_table(bg2.rename(columns={"Final_Sale":"Final Sale (₹)","Cancellation":"Cancel (₹)","Returns":"Returns (₹)","Units":"Units Sold"}),
                 {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Returns (₹)":"₹{:,.0f}","Units Sold":"{:,.0f}","Cancel Rate %":"{:.1f}%"})

    # Daily trend
    st.markdown("#### 📈 Daily Trend")
    st.plotly_chart(combined_chart(daily_agg(disp),"Order Date",
                    "Daily: Final Sale (Bar) | Cancel & Returns (Line)"), use_container_width=True)

    # Daily trend by channel
    dt_ch = df.copy(); dt_ch["Order Date"]=pd.to_datetime(dt_ch["Order Date"])
    dt_ch = dt_ch.groupby(["Order Date","Channel"]).agg(Final_Sale=("Final Sale Amount","sum")).reset_index()
    st.plotly_chart(px.line(dt_ch,x="Order Date",y="Final_Sale",color="Channel",
        title="Daily Final Sale: National vs Shopsy",template="plotly_dark",
        color_discrete_map=CHANNEL_COLORS,labels={"Final_Sale":"Final Sale (₹)"}), use_container_width=True)

    # ════════════════════════════════════════════════════════════════════
    # 2. CHANNEL SECTIONS
    # ════════════════════════════════════════════════════════════════════
    render_channel_section(df, "National", "national")
    render_channel_section(df, "Shopsy", "shopsy")

    # ════════════════════════════════════════════════════════════════════
    # 3. BELLAVITA FRAG vs NON-FRAG
    # ════════════════════════════════════════════════════════════════════
    if brand_f == "Bellavita":
        st.markdown("<div id='fragrance'></div>", unsafe_allow_html=True)
        sec_hdr("🌸 Fragrance vs Non-Fragrance","fragrance")
        bv=df[df["Brand"]=="Bellavita"].copy(); bv["Order Date"]=pd.to_datetime(bv["Order Date"])
        bv["Type"]=bv["Category"].apply(lambda c:"Fragrance" if any(k in str(c).lower() for k in frag_kw) else "Non-Fragrance")
        tg=bv.groupby("Type").agg(Final_Sale=("Final Sale Amount","sum"),Cancellation=("Cancellation Amount","sum"),Returns=("Return Amount","sum"),Units=("Final Sale Units","sum")).reset_index()
        c1,c2=st.columns(2)
        with c1:
            st.plotly_chart(px.bar(tg,x="Type",y=["Final_Sale","Cancellation","Returns"],barmode="group",
                template="plotly_dark",title="Fragrance vs Non-Frag",
                color_discrete_map={"Final_Sale":"#6C3483","Cancellation":"#e74c3c","Returns":"#e67e22"}), use_container_width=True)
        with c2:
            st.plotly_chart(px.pie(tg,values="Final_Sale",names="Type",title="Sale Share",template="plotly_dark"), use_container_width=True)
        # By channel too
        tg_ch=bv.groupby(["Type","Channel"]).agg(Final_Sale=("Final Sale Amount","sum")).reset_index()
        st.plotly_chart(px.bar(tg_ch,x="Type",y="Final_Sale",color="Channel",barmode="group",
            template="plotly_dark",title="Fragrance vs Non-Frag by Channel",
            color_discrete_map=CHANNEL_COLORS,labels={"Final_Sale":"Final Sale (₹)"}), use_container_width=True)
        dt2=bv.groupby(["Order Date","Type"]).agg(Final_Sale=("Final Sale Amount","sum")).reset_index()
        st.plotly_chart(px.line(dt2,x="Order Date",y="Final_Sale",color="Type",
            title="Daily: Fragrance vs Non-Fragrance",template="plotly_dark"), use_container_width=True)
        cg=bv.groupby(["Type","Category"]).agg(Final_Sale=("Final Sale Amount","sum"),Cancellation=("Cancellation Amount","sum")).reset_index().sort_values("Final_Sale",ascending=False)
        cg["Cancel Rate %"]=(cg["Cancellation"]/(cg["Final_Sale"]+cg["Cancellation"]).replace(0,np.nan)*100).round(1)
        render_table(cg.rename(columns={"Final_Sale":"Final Sale (₹)","Cancellation":"Cancel (₹)"}),
                     {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Cancel Rate %":"{:.1f}%"})

    # ════════════════════════════════════════════════════════════════════
    # 4. DOD
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='dod'></div>", unsafe_allow_html=True)
    sec_hdr("📅 Day-on-Day (DoD) Analysis","dod")
    dod=dod_data(disp)
    st.plotly_chart(combined_chart(dod,"Order Date","DoD: Final Sale (Bar) | Cancel & Returns (Line)"), use_container_width=True)
    st.markdown("#### DoD % Change Table")
    dd=dod.copy(); dd["Order Date"]=dd["Order Date"].dt.strftime("%d %b %Y")
    dd=dd.rename(columns={"Final_Sale":"Final Sale (₹)","DoD_Sale_%":"DoD Sale %","Cancellation":"Cancel (₹)",
                           "DoD_Cancel_%":"DoD Cancel %","Returns":"Returns (₹)","DoD_Return_%":"DoD Return %","Sale_Units":"Units"})
    render_table(dd[["Order Date","Final Sale (₹)","DoD Sale %","Cancel (₹)","DoD Cancel %","Returns (₹)","DoD Return %","Units"]],
                 {"Final Sale (₹)":"₹{:,.0f}","DoD Sale %":"{:.1f}%","Cancel (₹)":"₹{:,.0f}",
                  "DoD Cancel %":"{:.1f}%","Returns (₹)":"₹{:,.0f}","DoD Return %":"{:.1f}%","Units":"{:,.0f}"},
                 pct_cols=["DoD Sale %","DoD Cancel %","DoD Return %"])

    # ════════════════════════════════════════════════════════════════════
    # 5. WOW
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='wow'></div>", unsafe_allow_html=True)
    sec_hdr("📆 Week-on-Week (WoW) Analysis","wow")
    if not has_wow:
        st.info("Need at least 2 weeks of data.")
    else:
        wow=wow_data(disp); wow["Week_Str"]=wow["Week"].dt.strftime("W/C %d %b")
        st.plotly_chart(combined_chart(wow,"Week_Str","WoW: Final Sale (Bar) | Cancel & Returns (Line)"), use_container_width=True)
        wd=wow.copy(); wd["Week"]=wd["Week"].dt.strftime("W/C %d %b %Y")
        wd=wd.rename(columns={"Final_Sale":"Final Sale (₹)","WoW_Sale_%":"WoW Sale %","Cancellation":"Cancel (₹)",
                               "WoW_Cancel_%":"WoW Cancel %","Returns":"Returns (₹)","WoW_Return_%":"WoW Return %"})
        render_table(wd[["Week","Final Sale (₹)","WoW Sale %","Cancel (₹)","WoW Cancel %","Returns (₹)","WoW Return %"]],
                     {"Final Sale (₹)":"₹{:,.0f}","WoW Sale %":"{:.1f}%","Cancel (₹)":"₹{:,.0f}",
                      "WoW Cancel %":"{:.1f}%","Returns (₹)":"₹{:,.0f}","WoW Return %":"{:.1f}%"},
                     pct_cols=["WoW Sale %","WoW Cancel %","WoW Return %"])

    # ════════════════════════════════════════════════════════════════════
    # 6. MOM
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='mom'></div>", unsafe_allow_html=True)
    sec_hdr("🗓️ Month-on-Month (MoM) Analysis","mom")
    if not has_mom:
        st.info("📊 MoM activates with 2+ months of data.")
    else:
        mom=mom_data(disp); mom["Month_Str"]=mom["Month"].dt.strftime("%b %Y")
        st.plotly_chart(combined_chart(mom,"Month_Str","MoM: Final Sale (Bar) | Cancel & Returns (Line)"), use_container_width=True)
        md=mom.copy(); md["Month"]=md["Month"].dt.strftime("%b %Y")
        md=md.rename(columns={"Final_Sale":"Final Sale (₹)","MoM_Sale_%":"MoM Sale %","Cancellation":"Cancel (₹)",
                               "MoM_Cancel_%":"MoM Cancel %","Returns":"Returns (₹)","MoM_Return_%":"MoM Return %"})
        render_table(md[["Month","Final Sale (₹)","MoM Sale %","Cancel (₹)","MoM Cancel %","Returns (₹)","MoM Return %"]],
                     {"Final Sale (₹)":"₹{:,.0f}","MoM Sale %":"{:.1f}%","Cancel (₹)":"₹{:,.0f}",
                      "MoM Cancel %":"{:.1f}%","Returns (₹)":"₹{:,.0f}","MoM Return %":"{:.1f}%"},
                     pct_cols=["MoM Sale %","MoM Cancel %","MoM Return %"])

    # ════════════════════════════════════════════════════════════════════
    # 7. DECLINING SKUs
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='declining'></div>", unsafe_allow_html=True)
    sec_hdr("📉 Declining SKUs","declining")
    dec=declining_skus(disp)
    if dec.empty:
        st.info("Need at least 2 weeks of data.")
    else:
        st.plotly_chart(px.bar(dec.head(15),x="SKU ID",y="Change %",color="Brand",template="plotly_dark",
                               title="Top Declining SKUs (WoW %)",color_discrete_map=BRAND_COLORS), use_container_width=True)
        cols_show = [c for c in ["SKU ID","Brand","Category","Channel","Prev Week","Last Week","Change %"] if c in dec.columns]
        render_table(dec[cols_show],
                     {"Prev Week":"₹{:,.0f}","Last Week":"₹{:,.0f}","Change %":"{:.1f}%"},
                     pct_cols=["Change %"])

    # ════════════════════════════════════════════════════════════════════
    # 8. ACTION POINTS
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='actions'></div>", unsafe_allow_html=True)
    sec_hdr("🎯 Action Points","actions")
    st.caption("Auto-generated daily based on your data. Updates with every upload.")
    for i,a in enumerate(action_points(disp),1): st.markdown(f"**{i}.** {a}")

    st.markdown("#### Brand-wise Actions")
    for b in sorted(df["Brand"].unique()):
        bdf=df[df["Brand"]==b]
        bdates=sorted(pd.to_datetime(bdf["Order Date"]).unique())
        if bdates:
            td2=bdf[pd.to_datetime(bdf["Order Date"])==bdates[-1]]
            s2=td2["Final Sale Amount"].sum(); c2=td2["Cancellation Amount"].sum(); r2=td2["Return Amount"].sum()
            with st.expander(f"**{b}** — ₹{s2:,.0f} sale | ₹{c2:,.0f} cancel | ₹{r2:,.0f} returns"):
                for a in action_points(bdf): st.markdown(f"• {a}")

    # ════════════════════════════════════════════════════════════════════
    # 9. EXCLUSIVES (if column exists)
    # ════════════════════════════════════════════════════════════════════
    excl_col = next((c for c in disp.columns if "exclusive" in c.lower()), None)
    if excl_col:
        st.markdown("<div id='exclusives'></div>", unsafe_allow_html=True)
        sec_hdr("⭐ Exclusives Analysis","exclusives")
        ex=disp.copy(); ex["Is_Excl"]=ex[excl_col].astype(str).str.lower().isin(["yes","true","1","y","exclusive"])
        eg=ex.groupby("Is_Excl").agg(Final_Sale=("Final Sale Amount","sum"),Cancellation=("Cancellation Amount","sum"),Returns=("Return Amount","sum"),Units=("Final Sale Units","sum")).reset_index()
        eg["Label"]=eg["Is_Excl"].map({True:"Exclusive",False:"Non-Exclusive"})
        c1,c2=st.columns(2)
        with c1: st.plotly_chart(px.bar(eg,x="Label",y=["Final_Sale","Cancellation","Returns"],barmode="group",template="plotly_dark",title="Exclusive vs Non-Exclusive"), use_container_width=True)
        with c2: st.plotly_chart(px.pie(eg,values="Final_Sale",names="Label",title="Sale Share",template="plotly_dark"), use_container_width=True)
        render_table(eg[["Label","Final_Sale","Cancellation","Returns","Units"]].rename(
            columns={"Label":"Type","Final_Sale":"Final Sale (₹)","Cancellation":"Cancel (₹)","Returns":"Returns (₹)","Units":"Units Sold"}),
            {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Returns (₹)":"₹{:,.0f}","Units Sold":"{:,.0f}"})

    # ════════════════════════════════════════════════════════════════════
    # 10. EXTRA COLUMNS
    # ════════════════════════════════════════════════════════════════════
    extra_num = [c for c in disp.columns if c not in REQUIRED_COLS+["Order Date","Channel"]
                 and pd.api.types.is_numeric_dtype(disp[c])]
    if extra_num:
        st.markdown("---")
        st.markdown("#### 📌 Additional Metrics")
        cols = st.columns(min(len(extra_num),4))
        for i,col in enumerate(extra_num):
            with cols[i%4]: metric_card(col, disp[col].sum())

if __name__ == "__main__":
    main()
