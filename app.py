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

st.set_page_config(
    page_title="Flipkart Sales Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={"Get Help": None, "Report a bug": None, "About": "One Guardian — Flipkart Sales Dashboard"}
)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
REQUIRED_COLS = ["Product Id","SKU ID","Category","Brand","Vertical","Order Date","Fulfillment Type",
                 "Location Id","Gross Units","GMV","Cancellation Units","Cancellation Amount",
                 "Return Units","Return Amount","Final Sale Units","Final Sale Amount"]
NUMERIC_COLS = ["Gross Units","GMV","Cancellation Units","Cancellation Amount",
                "Return Units","Return Amount","Final Sale Units","Final Sale Amount"]
BRAND_COLORS = {"Bellavita":"#6C3483","Kenaz":"#1A5276","Embarouge":"#C0392B","HipHop Skincare":"#117A65","Guzz":"#D4AC0D"}
BELLAVITA_NAMES = ["BELLAVITA","Bella vita organic","Bellavita","bella vita","BELLA VITA ORGANIC","bellavita"]
CHANNEL_COLORS = {"Shopsy":"#E67E22","National":"#2E86C1"}

def indian_fmt(n):
    """Format number in Indian number system: 1,00,000 / 10,00,000 / 1,00,00,000"""
    try:
        n = float(n)
        if n < 0:
            return "-" + indian_fmt(-n)
        n = int(round(n))
        s = str(n)
        if len(s) <= 3:
            return s
        # Last 3 digits, then groups of 2
        last3 = s[-3:]
        rest = s[:-3]
        groups = []
        while len(rest) > 2:
            groups.append(rest[-2:])
            rest = rest[:-2]
        if rest:
            groups.append(rest)
        return ",".join(reversed(groups)) + "," + last3
    except:
        return str(n)

def indian_rupee(n):
    return "₹" + indian_fmt(n)

def normalize_brands(df):
    df = df.copy()
    df["Brand"] = df["Brand"].astype(str).str.strip()
    df["Brand"] = df["Brand"].apply(lambda x: "Bellavita" if x in BELLAVITA_NAMES else x)
    return df

def add_channel(df):
    df = df.copy()
    df["Channel"] = df["Vertical"].astype(str).apply(
        lambda v: "Shopsy" if v.strip().lower().startswith("shopsy") else "National"
    )
    return df

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

TEXT_COLS = {"Product Id", "SKU ID", "Category", "Brand", "Vertical",
             "Order Date", "Fulfillment Type", "Location Id", "Channel"}

def clean_df(df):
    for col in df.columns:
        if col in TEXT_COLS:
            df[col] = df[col].fillna("").astype(str).replace("nan", "").replace("0.0", "")
        elif col in NUMERIC_COLS:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).replace([float("inf"),float("-inf")],0)
        else:
            non_null = df[col].dropna()
            try:
                pd.to_numeric(non_null, errors="raise")
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            except (ValueError, TypeError):
                df[col] = df[col].fillna("").astype(str)
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
    combined = pd.concat([ex.reindex(columns=all_cols, fill_value=""),
                          truly_new.reindex(columns=all_cols, fill_value="")], ignore_index=True)
    combined["Order Date"] = pd.to_datetime(combined["Order Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    combined = clean_df(combined)
    combined = combined.sort_values("Order Date", ascending=False)
    ws.clear()
    ws.update([combined.columns.tolist()] + combined.astype(str).values.tolist())
    return len(truly_new), len(new_df) - len(truly_new)

def safe_pct(new, old): return round((new-old)/old*100,1) if old != 0 else None

def pct_badge(pct, inverse=False):
    if pct is None: return "<span style='color:#888'>—</span>"
    good = (pct>=0) if not inverse else (pct<=0)
    c = "#2ecc71" if good else "#e74c3c"
    s = "▲" if pct>=0 else "▼"
    return f"<span style='color:{c};font-weight:600'>{s} {abs(pct):.1f}%</span>"

def metric_card(label, val, delta="", prefix="₹", suffix=""):
    vs = f"{prefix}{indian_fmt(val)}{suffix}"
    st.markdown(f"""
    <div style='background:linear-gradient(135deg,#13132a,#1a1a35);padding:18px 20px;border-radius:14px;
                border:1px solid #2a2a4a;border-left:4px solid #6C3483;margin-bottom:8px;
                box-shadow:0 4px 20px rgba(0,0,0,0.3)'>
        <div style='color:#8888aa;font-size:11px;font-weight:500;letter-spacing:0.5px;
                    text-transform:uppercase;margin-bottom:6px'>{label}</div>
        <div style='color:#ffffff;font-size:22px;font-weight:800;letter-spacing:-0.5px;margin-bottom:4px'>{vs}</div>
        <div style='font-size:12px;margin-top:2px'>{delta}</div>
    </div>""", unsafe_allow_html=True)

def sec_hdr(title, anchor):
    st.markdown(f"""
    <div id='{anchor}' style='margin-top:40px;margin-bottom:20px'>
        <h2 style='color:#D7BDE2;font-size:22px;font-weight:700;margin:0;padding-bottom:10px;
                   border-bottom:2px solid;border-image:linear-gradient(90deg,#6C3483,#2E86C1) 1;
                   letter-spacing:-0.3px'>{title}</h2>
    </div>""", unsafe_allow_html=True)

def ind_tick(val, _):
    """Indian format for plotly axis ticks"""
    if val >= 1e7: return f"₹{val/1e7:.1f}Cr"
    if val >= 1e5: return f"₹{val/1e5:.1f}L"
    if val >= 1e3: return f"₹{val/1e3:.0f}K"
    return f"₹{int(val):,}"

def combined_chart(data, x, title):
    fig = make_subplots(specs=[[{"secondary_y":True}]])
    fig.add_trace(go.Bar(
        x=data[x], y=data["Final_Sale"], name="Final Sale (₹)",
        marker_color="#6C3483", opacity=0.85,
        hovertemplate="<b>Final Sale</b><br>₹%{customdata}<extra></extra>",
        customdata=["<br>₹".join([indian_fmt(v)]) if False else indian_fmt(v) for v in data["Final_Sale"]]
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=data[x], y=data["Cancellation"], name="Cancellation (₹)",
        line=dict(color="#e74c3c",width=2.5), mode="lines+markers",
        hovertemplate="<b>Cancellation</b><br>₹%{customdata}<extra></extra>",
        customdata=[indian_fmt(v) for v in data["Cancellation"]]
    ), secondary_y=True)
    fig.add_trace(go.Scatter(
        x=data[x], y=data["Returns"], name="Returns (₹)",
        line=dict(color="#e67e22",width=2.5,dash="dot"), mode="lines+markers",
        hovertemplate="<b>Returns</b><br>₹%{customdata}<extra></extra>",
        customdata=[indian_fmt(v) for v in data["Returns"]]
    ), secondary_y=True)
    fig.update_layout(title=title, template="plotly_dark", height=400,
                      legend=dict(orientation="h",y=1.12), hovermode="x unified")
    fig.update_yaxes(title_text="Final Sale (₹)", secondary_y=False,
                     tickformat=",.0f", tickprefix="₹")
    fig.update_yaxes(title_text="Cancel + Returns (₹)", secondary_y=True,
                     tickformat=",.0f", tickprefix="₹")
    return fig

def pct_color(val):
    try:
        v = float(str(val).replace("%",""))
        if v > 0: return "color: #2ecc71; font-weight:600"
        if v < 0: return "color: #e74c3c; font-weight:600"
    except: pass
    return ""

def fmt_inr(v):
    try: return "₹" + indian_fmt(float(v))
    except: return str(v)

def fmt_units(v):
    try: return indian_fmt(float(v))
    except: return str(v)

def fmt_pct(v):
    try: return f"{float(v):.1f}%"
    except: return str(v)

def render_table(df, fmt, pct_cols=[]):
    # Replace ₹{:,.0f} formatters with Indian rupee format
    new_fmt = {}
    for col, f in fmt.items():
        if "₹" in str(f):
            new_fmt[col] = fmt_inr
        elif "%" in str(f):
            new_fmt[col] = fmt_pct
        elif "{:,.0f}" in str(f):
            new_fmt[col] = fmt_units
        else:
            new_fmt[col] = f
    styled = df.style.format(new_fmt, na_rep="—")
    for col in pct_cols:
        if col in df.columns:
            fn = getattr(styled, "map", None) or getattr(styled, "applymap", None)
            styled = fn(lambda v: pct_color(v), subset=[col])
    st.dataframe(styled, use_container_width=True, hide_index=True)

def daily_agg(df):
    df = df.copy(); df["Order Date"] = pd.to_datetime(df["Order Date"])
    return df.groupby("Order Date").agg(
        Final_Sale=("Final Sale Amount","sum"), Cancellation=("Cancellation Amount","sum"),
        Returns=("Return Amount","sum"), Sale_Units=("Final Sale Units","sum")
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
    w = df.groupby("Week").agg(Final_Sale=("Final Sale Amount","sum"),
        Cancellation=("Cancellation Amount","sum"), Returns=("Return Amount","sum")
    ).reset_index().sort_values("Week")
    w["WoW_Sale_%"] = w["Final_Sale"].pct_change()*100
    w["WoW_Cancel_%"] = w["Cancellation"].pct_change()*100
    w["WoW_Return_%"] = w["Returns"].pct_change()*100
    return w

def mom_data(df):
    df = df.copy(); df["Order Date"] = pd.to_datetime(df["Order Date"])
    df["Month"] = df["Order Date"].dt.to_period("M").apply(lambda r: r.start_time)
    m = df.groupby("Month").agg(Final_Sale=("Final Sale Amount","sum"),
        Cancellation=("Cancellation Amount","sum"), Returns=("Return Amount","sum")
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
    if not bad.empty: actions.append(f"⚠️ **{len(bad)} SKUs with >30% cancel rate**: {", ".join(bad.index.astype(str)[:3].tolist())}")
    months=sorted(df["Order Date"].dt.to_period("M").unique())
    if len(months)>=2:
        m1=df[df["Order Date"].dt.to_period("M")==months[-1]]["Final Sale Amount"].sum()
        m0=df[df["Order Date"].dt.to_period("M")==months[-2]]["Final Sale Amount"].sum()
        mp=safe_pct(m1,m0)
        if mp and mp<-10: actions.append(f"📉 **MoM sales declined {abs(mp):.1f}%**. Review brand contribution & push promos.")
    if "Channel" in df.columns:
        total_sale = df["Final Sale Amount"].sum()
        if total_sale > 0:
            ch=df.groupby("Channel")["Final Sale Amount"].sum()
            for ch_name, ch_val in ch.items():
                actions.append(f"📊 **{ch_name} channel:** ₹{ch_val:,.0f} ({ch_val/total_sale*100:.1f}% of total sale)")
    if not actions: actions.append("✅ All metrics look healthy. Push exclusives scale-up & monitor DoD.")
    return actions

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
    bg = ch_df.groupby("Brand").agg(Final_Sale=("Final Sale Amount","sum"),
        Cancellation=("Cancellation Amount","sum"), Returns=("Return Amount","sum"),
        Units=("Final Sale Units","sum")).reset_index().sort_values("Final_Sale", ascending=False)
    bg["Cancel Rate %"] = (bg["Cancellation"]/(bg["Final_Sale"]+bg["Cancellation"]).replace(0,np.nan)*100).round(1)
    ca, cb = st.columns([3,2])
    with ca:
        fig_chb = px.bar(bg,x="Brand",y=["Final_Sale","Cancellation","Returns"],barmode="group",
            template="plotly_dark",title=f"{channel_name}: Brand-wise Sale vs Cancel vs Returns",
            color_discrete_map={"Final_Sale":"#6C3483","Cancellation":"#e74c3c","Returns":"#e67e22"},
            labels={"value":"₹","variable":"Metric"})
        max_chb = bg[["Final_Sale","Cancellation","Returns"]].max().max() if not bg.empty else 1
        ticks_chb = [max_chb*i/5 for i in range(6)]
        fig_chb.update_yaxes(tickvals=ticks_chb, ticktext=["₹"+indian_fmt(v) for v in ticks_chb])
        st.plotly_chart(fig_chb, use_container_width=True)
    with cb:
        st.plotly_chart(px.pie(bg,values="Final_Sale",names="Brand",title=f"{channel_name}: Sale Share",
            template="plotly_dark",color_discrete_sequence=px.colors.sequential.Purples_r), use_container_width=True)
    render_table(bg.rename(columns={"Final_Sale":"Final Sale (₹)","Cancellation":"Cancel (₹)","Returns":"Returns (₹)","Units":"Units Sold"}),
                 {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Returns (₹)":"₹{:,.0f}","Units Sold":"{:,.0f}","Cancel Rate %":"{:.1f}%"})
    st.plotly_chart(combined_chart(daily_agg(ch_df),"Order Date",
                    f"{channel_name}: Daily Final Sale (Bar) | Cancel & Returns (Line)"), use_container_width=True)
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

def apply_indian_yaxis(fig, max_val=None, secondary=False):
    """Apply Indian number format to plotly y-axis ticks"""
    if max_val is None or max_val == 0:
        return fig
    ticks = [max_val * i / 5 for i in range(7)]
    labels = ["₹" + indian_fmt(v) for v in ticks]
    fig.update_yaxes(tickvals=ticks, ticktext=labels)
    return fig

def ind_px_bar(df, **kwargs):
    """px.bar with Indian y-axis format"""
    fig = px.bar(df, **kwargs)
    y_col = kwargs.get("y")
    if isinstance(y_col, str) and y_col in df.columns:
        max_v = df[y_col].max()
    elif isinstance(y_col, list):
        max_v = df[y_col].max().max()
    else:
        max_v = None
    if max_v and max_v > 0:
        ticks = [max_v * i / 5 for i in range(7)]
        fig.update_yaxes(tickvals=ticks, ticktext=["₹"+indian_fmt(v) for v in ticks])
    fig.update_traces(hovertemplate="%{x}<br>₹%{y:,.0f}<extra></extra>")
    return fig

def ind_px_line(df, **kwargs):
    """px.line with Indian y-axis format"""
    fig = px.line(df, **kwargs)
    y_col = kwargs.get("y")
    if isinstance(y_col, str) and y_col in df.columns:
        max_v = df[y_col].max()
        if max_v and max_v > 0:
            ticks = [max_v * i / 5 for i in range(7)]
            fig.update_yaxes(tickvals=ticks, ticktext=["₹"+indian_fmt(v) for v in ticks])
    return fig

def main():
    st.markdown("""<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    html,body,.main,.stApp{background:#0a0a14!important;font-family:'Inter',sans-serif!important;color:#e8e8f0!important}
    .block-container{padding:1.5rem 2rem!important;max-width:100%!important;transition:all 0.3s ease!important}

    /* ── SIDEBAR ── */
    section[data-testid="stSidebar"]{
        background:linear-gradient(180deg,#0d0d1f,#111128)!important;
        min-width:270px!important;
        max-width:270px!important;
    }
    /* Style the collapse toggle button — make it visible and clean */
    button[data-testid="baseButton-headerNoPadding"]{
        background:#1a1a35!important;
        border:1px solid #2a2a4a!important;
        border-radius:6px!important;
        color:#C39BD3!important;
    }
    [data-testid="collapsedControl"]{
        background:#1a1a35!important;
        border-right:1px solid #2a2a4a!important;
        display:flex!important;
        visibility:visible!important;
        opacity:1!important;
    }
    [data-testid="collapsedControl"] button{
        background:#6C3483!important;
        color:white!important;
        border-radius:0 8px 8px 0!important;
        width:28px!important;
        height:60px!important;
    }
    section[data-testid="stSidebar"]{
        transition:all 0.3s ease!important;
    }

    .stButton>button{background:linear-gradient(135deg,#6C3483,#9B59B6)!important;color:white!important;
        border:none!important;border-radius:8px!important;font-weight:600!important;padding:.5rem 1.2rem!important}
    .stButton>button:hover{background:linear-gradient(135deg,#7D3C98,#AF7AC5)!important;
        box-shadow:0 4px 15px rgba(108,52,131,0.4)!important}
    div[data-testid="stDataFrame"]{border-radius:10px!important;overflow:hidden!important;border:1px solid #1e1e3a!important}
    .stTextInput>div>div>input,.stSelectbox>div>div,.stDateInput>div>div>input{
        background:#13132a!important;border:1px solid #2a2a4a!important;color:white!important;border-radius:8px!important}
    .stFileUploader>div{background:#13132a!important;border:2px dashed #2a2a4a!important;border-radius:10px!important}
    section[data-testid="stSidebar"] label,section[data-testid="stSidebar"] p{color:#aaa!important}
    #MainMenu,footer{visibility:hidden}
    header{visibility:visible!important}
    ::-webkit-scrollbar{width:6px;height:6px}
    ::-webkit-scrollbar-thumb{background:#2a2a4a;border-radius:3px}
    ::-webkit-scrollbar-thumb:hover{background:#6C3483}
    </style>""", unsafe_allow_html=True)

    # ── SIDEBAR ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("""<div style='text-align:center;padding:16px 0 8px 0'>
            <div style='font-size:26px'>🛒</div>
            <div style='font-size:17px;font-weight:800;color:#D7BDE2'>Flipkart Dashboard</div>
            <div style='font-size:11px;color:#6666aa;letter-spacing:1px;text-transform:uppercase;margin-top:3px'>One Guardian</div>
        </div>""", unsafe_allow_html=True)
        st.markdown("---")
        spreadsheet_name = st.text_input("Google Sheet Name", "Flipkart_Sales_DB")

        st.markdown("### 📤 Upload Data")
        uploaded = st.file_uploader("Earn More Report (.xlsx / .csv)", type=["xlsx","xls","csv"])
        if uploaded:
            try:
                raw = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
                missing = [c for c in REQUIRED_COLS if c not in raw.columns]
                if missing: st.error(f"Missing: {missing}")
                else:
                    raw["Order Date"] = pd.to_datetime(raw["Order Date"],errors="coerce").dt.strftime("%Y-%m-%d")
                    raw = clean_df(normalize_brands(add_channel(raw)))
                    ch_counts = raw["Channel"].value_counts()
                    st.success(f"✅ {len(raw):,} rows | {raw['Order Date'].min()} → {raw['Order Date'].max()}")
                    st.info(f"🏪 National: {ch_counts.get('National',0):,} | 🛍️ Shopsy: {ch_counts.get('Shopsy',0):,}")
                    if st.button("💾 Save to Google Sheets", type="primary"):
                        with st.spinner("Saving..."):
                            client = get_gsheet_client()
                            added, dupes = save_data(client, raw, spreadsheet_name)
                        st.success(f"✅ {added:,} new rows. {dupes:,} duplicates skipped.")
                        st.cache_data.clear()
            except Exception as e: st.error(f"Error: {e}")

        st.markdown("---")
        st.markdown("### 🔧 Database Tools")
        if st.button("➕ Add Channel column to Sheet"):
            with st.spinner("Updating..."):
                try:
                    client = get_gsheet_client()
                    sh = get_or_create_sheet(client, spreadsheet_name)
                    ws = sh.sheet1
                    data = ws.get_all_records()
                    if not data: st.warning("Sheet is empty.")
                    else:
                        df_m = pd.DataFrame(data)
                        df_m["Channel"] = df_m["Vertical"].apply(
                            lambda v: "Shopsy" if str(v).strip().lower().startswith("shopsy") else "National")
                        df_m = df_m.fillna("").replace(["nan","NaT"],"")
                        ws.clear()
                        ws.update([df_m.columns.tolist()] + df_m.astype(str).values.tolist())
                        ch = df_m["Channel"].value_counts()
                        st.success(f"✅ {len(df_m):,} rows updated. National:{ch.get('National',0):,} Shopsy:{ch.get('Shopsy',0):,}")
                        st.cache_data.clear()
                except Exception as e: st.error(f"Error: {e}")

        # ── LOAD DATA ─────────────────────────────────────────────────────────
        df_all = load_data(spreadsheet_name)
        if df_all.empty:
            st.info("No data yet. Upload a file above.")
            st.stop()

        st.markdown("---")
        st.markdown("### 🔍 Filters")

        df_all["Order Date"] = pd.to_datetime(df_all["Order Date"])
        min_date = df_all["Order Date"].min().date()
        max_date = df_all["Order Date"].max().date()
        date_range = st.date_input("📅 Date Range", value=(min_date, max_date),
                                   min_value=min_date, max_value=max_date)
        start_date = date_range[0] if isinstance(date_range,(list,tuple)) and len(date_range)==2 else min_date
        end_date   = date_range[1] if isinstance(date_range,(list,tuple)) and len(date_range)==2 else max_date

        channel_f = st.selectbox("📡 Channel", ["All","National","Shopsy"])
        brands = ["All"] + sorted(df_all["Brand"].dropna().unique().tolist())
        brand_f = st.selectbox("🏷️ Brand", brands)

        frag_kw = ["fragrance","perfume","deodorant","deo","edt","edp","attar","body mist","body spray"]
        if brand_f == "Bellavita":
            bv_cats = df_all[df_all["Brand"]=="Bellavita"]["Category"].dropna().unique().tolist()
            frag_cats   = [c for c in bv_cats if any(k in str(c).lower() for k in frag_kw)]
            nonfrag_cats= [c for c in bv_cats if c not in frag_cats]
            cat_f = st.selectbox("📦 Category", ["All","Fragrance","Non-Fragrance"])
        else:
            all_cats = ["All"] + sorted(df_all["Category"].dropna().unique().tolist())
            cat_f = st.selectbox("📦 Category", all_cats)
            frag_cats, nonfrag_cats = [], []

        st.markdown("---")
        st.markdown("### 📍 Jump To")
        nav_items = [("📊 Overview","overview"),("🏪 National Channel","national"),
                     ("🛍️ Shopsy Channel","shopsy"),("📅 DoD Analysis","dod"),
                     ("📆 WoW Analysis","wow"),("🗓️ MoM Analysis","mom"),
                     ("📉 Declining SKUs","declining"),("🎯 Action Points","actions")]
        if brand_f == "Bellavita": nav_items.insert(1,("🌸 Fragrance vs Non-Frag","fragrance"))
        excl_col = next((c for c in df_all.columns if "exclusive" in c.lower()), None)
        if excl_col: nav_items.append(("⭐ Exclusives","exclusives"))
        nav_html = ""
        for label, anchor in nav_items:
            nav_html += f"<a href='#{anchor}' style='display:block;padding:7px 12px;margin:3px 0;color:#C39BD3;text-decoration:none;font-size:13px;font-weight:500;border-radius:8px;background:rgba(108,52,131,0.08)'>{label}</a>"
        st.markdown(nav_html, unsafe_allow_html=True)
    
    # ── APPLY ALL FILTERS — THIS IS THE KEY FIX ───────────────────────────────
    # Step 1: date filter on master data
    df_all["Order Date"] = pd.to_datetime(df_all["Order Date"])
    df_dated = df_all[(df_all["Order Date"].dt.date >= start_date) & (df_all["Order Date"].dt.date <= end_date)].copy()

    # Step 2: full filtered df for ALL charts (channel + brand + category)
    df = df_dated.copy()
    if channel_f != "All": df = df[df["Channel"] == channel_f]
    if brand_f != "All":   df = df[df["Brand"] == brand_f]
    if cat_f != "All":
        if brand_f == "Bellavita":
            df = df[df["Category"].isin(frag_cats if cat_f=="Fragrance" else nonfrag_cats)]
        else:
            df = df[df["Category"] == cat_f]

    # Step 3: date-only df for channel-level and brand-level overview (not brand/cat filtered)

    dates = sorted(df["Order Date"].unique())
    has_wow = df["Order Date"].dt.to_period("W").nunique() >= 2
    has_mom = df["Order Date"].dt.to_period("M").nunique() >= 2

    # ── HEADER ────────────────────────────────────────────────────────────────
    ch_counts = df_dated["Channel"].value_counts()
    date_str = f"{dates[0].strftime('%d %b %Y')} → {dates[-1].strftime('%d %b %Y')}" if dates else "—"
    st.markdown(f"""
    <div style='background:linear-gradient(135deg,#1a0a2e,#0d1a3a,#0a1a20);border-radius:16px;
                padding:28px 32px;margin-bottom:24px;border:1px solid #2a2a4a;box-shadow:0 8px 32px rgba(0,0,0,0.4)'>
        <div style='display:flex;align-items:center;gap:12px;margin-bottom:8px'>
            <span style='font-size:32px'>🛒</span>
            <div>
                <div style='font-size:26px;font-weight:800;color:#fff;letter-spacing:-0.5px'>Flipkart Sales Dashboard</div>
                <div style='font-size:13px;color:#8888bb;margin-top:2px'>One Guardian · {brand_f} · {channel_f} · {cat_f}</div>
            </div>
        </div>
        <div style='display:flex;gap:12px;flex-wrap:wrap;margin-top:14px'>
            <div style='background:rgba(108,52,131,0.2);border:1px solid rgba(108,52,131,0.4);border-radius:8px;padding:5px 12px;font-size:12px;color:#D7BDE2;font-weight:600'>📅 {date_str}</div>
            <div style='background:rgba(46,134,193,0.2);border:1px solid rgba(46,134,193,0.4);border-radius:8px;padding:5px 12px;font-size:12px;color:#85C1E9;font-weight:600'>🏪 National: {ch_counts.get("National",0):,}</div>
            <div style='background:rgba(230,126,34,0.2);border:1px solid rgba(230,126,34,0.4);border-radius:8px;padding:5px 12px;font-size:12px;color:#F0B27A;font-weight:600'>🛍️ Shopsy: {ch_counts.get("Shopsy",0):,}</div>
            <div style='background:rgba(46,204,113,0.15);border:1px solid rgba(46,204,113,0.3);border-radius:8px;padding:5px 12px;font-size:12px;color:#82E0AA;font-weight:600'>📊 {len(df):,} rows filtered</div>
        </div>
    </div>""", unsafe_allow_html=True)

    # ════════════════════════════════════════════════════════════════════
    # 1. OVERVIEW — uses df (fully filtered)
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='overview'></div>", unsafe_allow_html=True)
    sec_hdr("📊 Overview","overview")

    if dates:
        td=df[df["Order Date"]==dates[-1]]
        yd=df[df["Order Date"]==dates[-2]] if len(dates)>=2 else pd.DataFrame()
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
        current_month = pd.Timestamp.now().to_period("M")
        df_cm = df[pd.to_datetime(df["Order Date"]).dt.to_period("M") == current_month]
        with c5: metric_card("Current Month Sale", df_cm["Final Sale Amount"].sum())

    # Channel split — uses df (fully filtered)
    st.markdown("<div style='background:linear-gradient(90deg,rgba(46,134,193,0.12),rgba(230,126,34,0.12));border:1px solid #2a2a4a;border-radius:12px;padding:11px 18px;margin:20px 0 8px 0'><span style='font-size:15px;font-weight:700;color:#D7BDE2'>📡 National vs Shopsy — Total Period</span></div>", unsafe_allow_html=True)
    ch_grp = df.groupby("Channel").agg(Final_Sale=("Final Sale Amount","sum"),
        Cancellation=("Cancellation Amount","sum"), Returns=("Return Amount","sum"),
        Units=("Final Sale Units","sum")).reset_index()
    ch_grp["Cancel Rate %"]=(ch_grp["Cancellation"]/(ch_grp["Final_Sale"]+ch_grp["Cancellation"]).replace(0,np.nan)*100).round(1)
    ca,cb=st.columns(2)
    with ca:
        fig_ch = px.bar(ch_grp,x="Channel",y=["Final_Sale","Cancellation","Returns"],
            barmode="group",template="plotly_dark",title="National vs Shopsy: Sale, Cancel, Returns",
            color_discrete_map={"Final_Sale":"#6C3483","Cancellation":"#e74c3c","Returns":"#e67e22"},
            labels={"value":"₹","variable":"Metric"})
        max_v = ch_grp[["Final_Sale","Cancellation","Returns"]].max().max() if not ch_grp.empty else 1
        
        fig_ch.update_yaxes(tickvals=[v for v in __import__("numpy").linspace(0,max_v,6)],
                            ticktext=["₹"+indian_fmt(v) for v in __import__("numpy").linspace(0,max_v,6)])
        fig_ch.update_traces(hovertemplate="<b>%{x}</b><br>₹%{y:,.0f}<extra></extra>")
        st.plotly_chart(fig_ch, use_container_width=True)
    with cb:
        st.plotly_chart(px.pie(ch_grp,values="Final_Sale",names="Channel",
            title="Sale Share: National vs Shopsy",template="plotly_dark",
            color_discrete_map={"National":"#2E86C1","Shopsy":"#E67E22"}), use_container_width=True)
    render_table(ch_grp.rename(columns={"Final_Sale":"Final Sale (₹)","Cancellation":"Cancel (₹)","Returns":"Returns (₹)","Units":"Units Sold"}),
                 {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Returns (₹)":"₹{:,.0f}","Units Sold":"{:,.0f}","Cancel Rate %":"{:.1f}%"})

    # Brand-wise — uses df (fully filtered)
    st.markdown("<div style='background:rgba(108,52,131,0.12);border:1px solid #2a2a4a;border-radius:12px;padding:11px 18px;margin:20px 0 8px 0'><span style='font-size:15px;font-weight:700;color:#D7BDE2'>🏷️ Brand-wise Performance</span></div>", unsafe_allow_html=True)
    bg=df.groupby(["Brand","Channel"]).agg(Final_Sale=("Final Sale Amount","sum")).reset_index()
    bg["Final_Sale_Fmt"] = bg["Final_Sale"].apply(indian_fmt)
    fig_bg = px.bar(bg, x="Brand", y="Final_Sale", color="Channel", barmode="group",
        template="plotly_dark", title="Brand-wise Final Sale by Channel",
        color_discrete_map=CHANNEL_COLORS,
        labels={"Final_Sale":"Final Sale (₹)"},
        custom_data=["Final_Sale_Fmt","Channel"])
    fig_bg.update_traces(hovertemplate="<b>%{customdata[1]}</b><br>₹%{customdata[0]}<extra></extra>")
    max_val = bg["Final_Sale"].max() if not bg.empty else 1
    ticks = [max_val * i / 5 for i in range(6)]
    fig_bg.update_yaxes(tickvals=ticks, ticktext=["₹"+indian_fmt(v) for v in ticks])
    st.plotly_chart(fig_bg, use_container_width=True)
    bg2=df.groupby("Brand").agg(Final_Sale=("Final Sale Amount","sum"),Cancellation=("Cancellation Amount","sum"),
        Returns=("Return Amount","sum"),Units=("Final Sale Units","sum")).reset_index().sort_values("Final_Sale",ascending=False)
    bg2["Cancel Rate %"]=(bg2["Cancellation"]/(bg2["Final_Sale"]+bg2["Cancellation"]).replace(0,np.nan)*100).round(1)
    render_table(bg2.rename(columns={"Final_Sale":"Final Sale (₹)","Cancellation":"Cancel (₹)","Returns":"Returns (₹)","Units":"Units Sold"}),
                 {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Returns (₹)":"₹{:,.0f}","Units Sold":"{:,.0f}","Cancel Rate %":"{:.1f}%"})

    # Daily trend — uses df (fully filtered)
    st.markdown("<div style='background:rgba(46,204,113,0.08);border:1px solid #2a2a4a;border-radius:12px;padding:11px 18px;margin:20px 0 8px 0'><span style='font-size:15px;font-weight:700;color:#D7BDE2'>📈 Daily Trend</span></div>", unsafe_allow_html=True)
    st.plotly_chart(combined_chart(daily_agg(df),"Order Date","Daily: Final Sale (Bar) | Cancel & Returns (Line)"), use_container_width=True)
    dt_ch = df.copy(); dt_ch["Order Date"]=pd.to_datetime(dt_ch["Order Date"])
    dt_ch = dt_ch.groupby(["Order Date","Channel"]).agg(Final_Sale=("Final Sale Amount","sum")).reset_index()
    st.plotly_chart(ind_px_line(dt_ch,x="Order Date",y="Final_Sale",color="Channel",
        title="Daily Final Sale: National vs Shopsy",template="plotly_dark",
        color_discrete_map=CHANNEL_COLORS,labels={"Final_Sale":"Final Sale (₹)"}), use_container_width=True)

    # ════════════════════════════════════════════════════════════════════
    # 2. CHANNEL SECTIONS — uses df (fully filtered)
    # ════════════════════════════════════════════════════════════════════
    render_channel_section(df, "National", "national")
    render_channel_section(df, "Shopsy", "shopsy")

    # ════════════════════════════════════════════════════════════════════
    # 3. BELLAVITA FRAG vs NON-FRAG
    # ════════════════════════════════════════════════════════════════════
    if brand_f == "Bellavita":
        st.markdown("<div id='fragrance'></div>", unsafe_allow_html=True)
        sec_hdr("🌸 Fragrance vs Non-Fragrance","fragrance")
        bv=df[df["Brand"]=="Bellavita"].copy()
        bv["Order Date"]=pd.to_datetime(bv["Order Date"])
        bv["Type"]=bv["Category"].apply(lambda c:"Fragrance" if any(k in str(c).lower() for k in frag_kw) else "Non-Fragrance")
        tg=bv.groupby("Type").agg(Final_Sale=("Final Sale Amount","sum"),Cancellation=("Cancellation Amount","sum"),Returns=("Return Amount","sum"),Units=("Final Sale Units","sum")).reset_index()
        c1,c2=st.columns(2)
        with c1:
            st.plotly_chart(ind_px_bar(tg,x="Type",y=["Final_Sale","Cancellation","Returns"],barmode="group",
                template="plotly_dark",title="Fragrance vs Non-Frag",
                color_discrete_map={"Final_Sale":"#6C3483","Cancellation":"#e74c3c","Returns":"#e67e22"}), use_container_width=True)
        with c2:
            st.plotly_chart(px.pie(tg,values="Final_Sale",names="Type",title="Sale Share",template="plotly_dark"), use_container_width=True)
        tg_ch=bv.groupby(["Type","Channel"]).agg(Final_Sale=("Final Sale Amount","sum")).reset_index()
        st.plotly_chart(ind_px_bar(tg_ch,x="Type",y="Final_Sale",color="Channel",barmode="group",
            template="plotly_dark",title="Fragrance vs Non-Frag by Channel",
            color_discrete_map=CHANNEL_COLORS,labels={"Final_Sale":"Final Sale (₹)"}), use_container_width=True)
        dt2=bv.groupby(["Order Date","Type"]).agg(Final_Sale=("Final Sale Amount","sum")).reset_index()
        st.plotly_chart(ind_px_line(dt2,x="Order Date",y="Final_Sale",color="Type",
            title="Daily: Fragrance vs Non-Fragrance",template="plotly_dark"), use_container_width=True)
        cg=bv.groupby(["Type","Category"]).agg(Final_Sale=("Final Sale Amount","sum"),Cancellation=("Cancellation Amount","sum")).reset_index().sort_values("Final_Sale",ascending=False)
        cg["Cancel Rate %"]=(cg["Cancellation"]/(cg["Final_Sale"]+cg["Cancellation"]).replace(0,np.nan)*100).round(1)
        render_table(cg.rename(columns={"Final_Sale":"Final Sale (₹)","Cancellation":"Cancel (₹)"}),
                     {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Cancel Rate %":"{:.1f}%"})

    # ════════════════════════════════════════════════════════════════════
    # 4-6. DOD / WOW / MOM — all use df (fully filtered)
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='dod'></div>", unsafe_allow_html=True)
    sec_hdr("📅 Day-on-Day (DoD) Analysis","dod")
    dod=dod_data(df)
    st.plotly_chart(combined_chart(dod,"Order Date","DoD: Final Sale (Bar) | Cancel & Returns (Line)"), use_container_width=True)
    dd=dod.copy(); dd["Order Date"]=dd["Order Date"].dt.strftime("%d %b %Y")
    dd=dd.rename(columns={"Final_Sale":"Final Sale (₹)","DoD_Sale_%":"DoD Sale %","Cancellation":"Cancel (₹)",
                           "DoD_Cancel_%":"DoD Cancel %","Returns":"Returns (₹)","DoD_Return_%":"DoD Return %","Sale_Units":"Units"})
    render_table(dd[["Order Date","Final Sale (₹)","DoD Sale %","Cancel (₹)","DoD Cancel %","Returns (₹)","DoD Return %","Units"]],
                 {"Final Sale (₹)":"₹{:,.0f}","DoD Sale %":"{:.1f}%","Cancel (₹)":"₹{:,.0f}",
                  "DoD Cancel %":"{:.1f}%","Returns (₹)":"₹{:,.0f}","DoD Return %":"{:.1f}%","Units":"{:,.0f}"},
                 pct_cols=["DoD Sale %","DoD Cancel %","DoD Return %"])

    st.markdown("<div id='wow'></div>", unsafe_allow_html=True)
    sec_hdr("📆 Week-on-Week (WoW) Analysis","wow")
    if not has_wow: st.info("Need at least 2 weeks of data.")
    else:
        wow=wow_data(df); wow["Week_Str"]=wow["Week"].dt.strftime("W/C %d %b")
        st.plotly_chart(combined_chart(wow,"Week_Str","WoW: Final Sale (Bar) | Cancel & Returns (Line)"), use_container_width=True)
        wd=wow.copy(); wd["Week"]=wd["Week"].dt.strftime("W/C %d %b %Y")
        wd=wd.rename(columns={"Final_Sale":"Final Sale (₹)","WoW_Sale_%":"WoW Sale %","Cancellation":"Cancel (₹)",
                               "WoW_Cancel_%":"WoW Cancel %","Returns":"Returns (₹)","WoW_Return_%":"WoW Return %"})
        render_table(wd[["Week","Final Sale (₹)","WoW Sale %","Cancel (₹)","WoW Cancel %","Returns (₹)","WoW Return %"]],
                     {"Final Sale (₹)":"₹{:,.0f}","WoW Sale %":"{:.1f}%","Cancel (₹)":"₹{:,.0f}",
                      "WoW Cancel %":"{:.1f}%","Returns (₹)":"₹{:,.0f}","WoW Return %":"{:.1f}%"},
                     pct_cols=["WoW Sale %","WoW Cancel %","WoW Return %"])

    st.markdown("<div id='mom'></div>", unsafe_allow_html=True)
    sec_hdr("🗓️ Month-on-Month (MoM) Analysis","mom")
    if not has_mom: st.info("📊 MoM activates with 2+ months of data.")
    else:
        mom=mom_data(df); mom["Month_Str"]=mom["Month"].dt.strftime("%b %Y")
        st.plotly_chart(combined_chart(mom,"Month_Str","MoM: Final Sale (Bar) | Cancel & Returns (Line)"), use_container_width=True)
        md=mom.copy(); md["Month"]=md["Month"].dt.strftime("%b %Y")
        md=md.rename(columns={"Final_Sale":"Final Sale (₹)","MoM_Sale_%":"MoM Sale %","Cancellation":"Cancel (₹)",
                               "MoM_Cancel_%":"MoM Cancel %","Returns":"Returns (₹)","MoM_Return_%":"MoM Return %"})
        render_table(md[["Month","Final Sale (₹)","MoM Sale %","Cancel (₹)","MoM Cancel %","Returns (₹)","MoM Return %"]],
                     {"Final Sale (₹)":"₹{:,.0f}","MoM Sale %":"{:.1f}%","Cancel (₹)":"₹{:,.0f}",
                      "MoM Cancel %":"{:.1f}%","Returns (₹)":"₹{:,.0f}","MoM Return %":"{:.1f}%"},
                     pct_cols=["MoM Sale %","MoM Cancel %","MoM Return %"])

    # ════════════════════════════════════════════════════════════════════
    # 7. DECLINING SKUs — uses df (fully filtered)
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='declining'></div>", unsafe_allow_html=True)
    sec_hdr("📉 Declining SKUs","declining")
    dec=declining_skus(df)
    if dec.empty: st.info("Need at least 2 weeks of data.")
    else:
        st.plotly_chart(px.bar(dec.head(15),x="SKU ID",y="Change %",color="Brand",template="plotly_dark",
                               title="Top Declining SKUs (WoW %)",color_discrete_map=BRAND_COLORS,
                               labels={"Change %":"WoW Change %"}), use_container_width=True)
        cols_show=[c for c in ["SKU ID","Brand","Category","Channel","Prev Week","Last Week","Change %"] if c in dec.columns]
        render_table(dec[cols_show],{"Prev Week":"₹{:,.0f}","Last Week":"₹{:,.0f}","Change %":"{:.1f}%"},pct_cols=["Change %"])

    # ════════════════════════════════════════════════════════════════════
    # 8. ACTION POINTS — uses df (fully filtered)
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='actions'></div>", unsafe_allow_html=True)
    sec_hdr("🎯 Action Points","actions")
    st.caption("Auto-generated based on filtered data. Updates with every upload.")
    for i,a in enumerate(action_points(df),1): st.markdown(f"**{i}.** {a}")
    st.markdown("#### Brand-wise Actions")
    for b in sorted(df["Brand"].unique()):
        bdf=df[df["Brand"]==b]
        bdates=sorted(pd.to_datetime(bdf["Order Date"]).unique())
        if bdates:
            td2=bdf[pd.to_datetime(bdf["Order Date"])==bdates[-1]]
            s2=td2["Final Sale Amount"].sum(); c2=td2["Cancellation Amount"].sum()
            with st.expander(f"**{b}** — ₹{s2:,.0f} sale | ₹{c2:,.0f} cancel"):
                for a in action_points(bdf): st.markdown(f"• {a}")

    # ════════════════════════════════════════════════════════════════════
    # 9. EXCLUSIVES
    # ════════════════════════════════════════════════════════════════════
    if excl_col:
        st.markdown("<div id='exclusives'></div>", unsafe_allow_html=True)
        sec_hdr("⭐ Exclusives Analysis","exclusives")
        ex=df.copy(); ex["Is_Excl"]=ex[excl_col].astype(str).str.lower().isin(["yes","true","1","y","exclusive"])
        eg=ex.groupby("Is_Excl").agg(Final_Sale=("Final Sale Amount","sum"),Cancellation=("Cancellation Amount","sum"),
                                      Returns=("Return Amount","sum"),Units=("Final Sale Units","sum")).reset_index()
        eg["Label"]=eg["Is_Excl"].map({True:"Exclusive",False:"Non-Exclusive"})
        c1,c2=st.columns(2)
        with c1: st.plotly_chart(px.bar(eg,x="Label",y=["Final_Sale","Cancellation","Returns"],barmode="group",template="plotly_dark",title="Exclusive vs Non-Exclusive"), use_container_width=True)
        with c2: st.plotly_chart(px.pie(eg,values="Final_Sale",names="Label",title="Sale Share",template="plotly_dark"), use_container_width=True)
        render_table(eg[["Label","Final_Sale","Cancellation","Returns","Units"]].rename(
            columns={"Label":"Type","Final_Sale":"Final Sale (₹)","Cancellation":"Cancel (₹)","Returns":"Returns (₹)","Units":"Units Sold"}),
            {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Returns (₹)":"₹{:,.0f}","Units Sold":"{:,.0f}"})

    # ════════════════════════════════════════════════════════════════════
    # 10. EXTRA NUMERIC COLUMNS
    # ════════════════════════════════════════════════════════════════════
    extra_num=[c for c in df.columns if c not in REQUIRED_COLS+["Order Date","Channel"]
               and pd.api.types.is_numeric_dtype(df[c])]
    if extra_num:
        st.markdown("---")
        st.markdown("#### 📌 Additional Metrics")
        cols=st.columns(min(len(extra_num),4))
        for i,col in enumerate(extra_num):
            with cols[i%4]: metric_card(col, df[col].sum())

if __name__ == "__main__":
    main()
