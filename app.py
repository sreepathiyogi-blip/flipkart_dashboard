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
BRAND_COLORS = {"Bellavita":"#9B59B6","Kenaz":"#3498DB","Embarouge":"#E74C3C","HipHop Skincare":"#2ECC71","Guzz":"#F39C12"}
PIE_COLORS = [
    "#9B59B6",  # vivid purple
    "#2ECC71",  # emerald green
    "#E74C3C",  # red
    "#3498DB",  # blue
    "#F39C12",  # amber
    "#1ABC9C",  # teal
    "#E91E63",  # hot pink
    "#FF5722",  # deep orange
    "#00BCD4",  # cyan
    "#8BC34A",  # lime green
    "#FF9800",  # orange
    "#673AB7",  # deep purple
]

FRAG_COLORS = {"Fragrance": "#C39BD3", "Non-Fragrance": "#2ECC71"}
FRAG_KW = ["fragrance","perfume","deodorant","deo","edt","edp","attar","body mist","body spray"]
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
    .main .block-container{padding-left:1rem!important;padding-right:1rem!important}
    [data-testid="stSidebar"][aria-expanded="false"] ~ .main .block-container{
        padding-left:3rem!important;
        padding-right:1rem!important;
        max-width:100%!important;
    }
    [data-testid="stSidebar"][aria-expanded="true"] ~ .main .block-container{
        max-width:calc(100% - 270px)!important;
    }

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
    header{visibility:visible!important;background:transparent!important;box-shadow:none!important}
    header button[data-testid="stBaseButton-header"]{display:none!important}
    header a{display:none!important}
    ::-webkit-scrollbar{width:6px;height:6px}
    ::-webkit-scrollbar-thumb{background:#2a2a4a;border-radius:3px}
    ::-webkit-scrollbar-thumb:hover{background:#6C3483}
    section[data-testid="stSidebar"] > div:first-child{
        overflow-y:auto!important;
        max-height:100vh!important;
    }
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

        if brand_f == "Bellavita":
            bv_cats = df_all[df_all["Brand"]=="Bellavita"]["Category"].dropna().unique().tolist()
            frag_cats   = [c for c in bv_cats if any(k in str(c).lower() for k in FRAG_KW)]
            nonfrag_cats= [c for c in bv_cats if c not in frag_cats]
            cat_f = st.selectbox("📦 Category", ["All","Fragrance","Non-Fragrance"])
        else:
            all_cats = ["All"] + sorted(df_all["Category"].dropna().unique().tolist())
            cat_f = st.selectbox("📦 Category", all_cats)
            frag_cats, nonfrag_cats = [], []

        st.markdown("---")
        st.markdown("### 📍 Jump To")
        nav_items = [
            ("📊 Overview","overview"),
            ("📊 Vertical Analysis","vertical"),
            ("🌸 Fragrance vs Non-Frag","fragrance"),
            ("🏪 National Channel","national"),
            ("🛍️ Shopsy Channel","shopsy"),
            ("💧 GMV Leakage Waterfall","waterfall"),
            ("❌ Cancellation Deep Dive","cancel_deep"),
            ("↩️ Return Rate Deep Dive","return_deep"),
            ("🏷️ Fulfillment Type Analysis","fulfillment"),
            ("🆕 New SKU Tracker","new_skus"),
            ("🏆 SKU Health Scorecard","sku_health"),
            ("📈 Growing vs Declining SKUs","sku_trends"),
            ("🗺️ Category Heatmap","cat_heatmap"),
            ("📍 Location Analysis","location"),
            ("🌱 Non-Frag Growth Tracker","nonfrag_tracker"),
            ("📊 Brand Contribution Trend","brand_trend"),
            ("📋 Weekly Brand Matrix","weekly_matrix"),
            ("📅 DoD Analysis","dod"),
            ("📆 WoW Analysis","wow"),
            ("🗓️ MoM Analysis","mom"),
            ("📉 Declining SKUs","declining"),
            ("🎯 Action Points","actions"),
        ]
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
    fig_daily_ch = px.area(dt_ch,x="Order Date",y="Final_Sale",color="Channel",
        title="Daily Final Sale: National vs Shopsy",template="plotly_dark",
        color_discrete_map={"National":"#2E86C1","Shopsy":"#E67E22"},
        labels={"Final_Sale":"Final Sale (₹)","Order Date":"Date"},
        line_group="Channel")
    fig_daily_ch.update_traces(opacity=0.7)
    max_v2 = dt_ch["Final_Sale"].max() if not dt_ch.empty else 1
    ticks2 = [max_v2*i/5 for i in range(6)]
    fig_daily_ch.update_yaxes(tickvals=ticks2, ticktext=["₹"+indian_fmt(v) for v in ticks2])
    st.plotly_chart(fig_daily_ch, use_container_width=True)

    # ════════════════════════════════════════════════════════════════════
    # 2. CHANNEL SECTIONS — uses df (fully filtered)
    # ════════════════════════════════════════════════════════════════════
    render_channel_section(df, "National", "national")
    render_channel_section(df, "Shopsy", "shopsy")

    # ════════════════════════════════════════════════════════════════════
    # 2.5 VERTICAL ANALYSIS — Overall + DoD + WoW + MoM
    # ════════════════════════════════════════════════════════════════════
    sec_hdr("📊 Vertical Analysis","vertical")

    vert_grp = df.groupby("Vertical").agg(
        Final_Sale=("Final Sale Amount","sum"),
        Cancellation=("Cancellation Amount","sum"),
        Returns=("Return Amount","sum"),
        Units=("Final Sale Units","sum")
    ).reset_index().sort_values("Final_Sale", ascending=False).head(15)
    vert_grp["Cancel Rate %"] = (vert_grp["Cancellation"]/(vert_grp["Final_Sale"]+vert_grp["Cancellation"]).replace(0,np.nan)*100).round(1)

    va, vb = st.columns(2)
    with va:
        fig_vert = px.bar(vert_grp, x="Vertical", y="Final_Sale", template="plotly_dark",
            title="Top Verticals by Final Sale", color="Vertical",
            color_discrete_sequence=PIE_COLORS,
            labels={"Final_Sale":"Final Sale (₹)"})
        max_vv = vert_grp["Final_Sale"].max() if not vert_grp.empty else 1
        ticks_vv = [max_vv*i/5 for i in range(6)]
        fig_vert.update_yaxes(tickvals=ticks_vv, ticktext=["₹"+indian_fmt(v) for v in ticks_vv])
        fig_vert.update_xaxes(tickangle=45)
        fig_vert.update_layout(showlegend=False)
        st.plotly_chart(fig_vert, use_container_width=True)
    with vb:
        st.plotly_chart(px.pie(vert_grp, values="Final_Sale", names="Vertical",
            title="Vertical Sale Share", template="plotly_dark",
            color_discrete_sequence=PIE_COLORS,
            hole=0.35), use_container_width=True)

    render_table(vert_grp.rename(columns={"Final_Sale":"Final Sale (₹)","Cancellation":"Cancel (₹)","Returns":"Returns (₹)","Units":"Units Sold"}),
        {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Returns (₹)":"₹{:,.0f}","Units Sold":"{:,.0f}","Cancel Rate %":"{:.1f}%"})

    # ── Vertical DoD ──────────────────────────────────────────────────
    with st.expander("📅 Vertical DoD — Which verticals grew or dropped yesterday?"):
        vert_dod = df.copy()
        vert_dod["Order Date"] = pd.to_datetime(vert_dod["Order Date"])
        vdates = sorted(vert_dod["Order Date"].unique())
        if len(vdates) >= 2:
            vt = vert_dod[vert_dod["Order Date"]==vdates[-1]].groupby("Vertical")["Final Sale Amount"].sum()
            vy = vert_dod[vert_dod["Order Date"]==vdates[-2]].groupby("Vertical")["Final Sale Amount"].sum()
            vdod = pd.DataFrame({"Today":vt, "Yesterday":vy}).fillna(0)
            vdod["DoD %"] = ((vdod["Today"]-vdod["Yesterday"])/vdod["Yesterday"].replace(0,np.nan)*100).round(1)
            vdod["Trend"] = vdod["DoD %"].apply(lambda x: "🟢 Growing" if x > 5 else ("🔴 Declining" if x < -5 else "🟡 Stable"))
            vdod = vdod.sort_values("Today", ascending=False).reset_index()
            render_table(vdod, {"Today":"₹{:,.0f}","Yesterday":"₹{:,.0f}","DoD %":"{:.1f}%"}, pct_cols=["DoD %"])

            # bar chart for DoD
            vdod_pos = vdod.copy()
            fig_vdod = px.bar(vdod_pos, x="Vertical", y="DoD %", color="DoD %",
                color_continuous_scale=["#e74c3c","#f39c12","#2ecc71"],
                template="plotly_dark", title="Vertical DoD % Change",
                labels={"DoD %":"DoD Change %"})
            fig_vdod.update_xaxes(tickangle=45)
            fig_vdod.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.4)
            st.plotly_chart(fig_vdod, use_container_width=True)
        else:
            st.info("Need at least 2 days of data for DoD.")

    # ── Vertical WoW ──────────────────────────────────────────────────
    with st.expander("📆 Vertical WoW — Week-on-week performance by vertical"):
        vert_wow = df.copy()
        vert_wow["Order Date"] = pd.to_datetime(vert_wow["Order Date"])
        vert_wow["Week"] = vert_wow["Order Date"].dt.to_period("W").apply(lambda r: r.start_time)
        vweeks = sorted(vert_wow["Week"].unique())
        if len(vweeks) >= 2:
            vw1 = vert_wow[vert_wow["Week"]==vweeks[-1]].groupby("Vertical")["Final Sale Amount"].sum()
            vw2 = vert_wow[vert_wow["Week"]==vweeks[-2]].groupby("Vertical")["Final Sale Amount"].sum()
            vwow = pd.DataFrame({"This Week":vw1, "Last Week":vw2}).fillna(0)
            vwow["WoW %"] = ((vwow["This Week"]-vwow["Last Week"])/vwow["Last Week"].replace(0,np.nan)*100).round(1)
            vwow["Trend"] = vwow["WoW %"].apply(lambda x: "🟢 Growing" if x > 5 else ("🔴 Declining" if x < -5 else "🟡 Stable"))
            vwow = vwow.sort_values("This Week", ascending=False).reset_index()
            render_table(vwow, {"This Week":"₹{:,.0f}","Last Week":"₹{:,.0f}","WoW %":"{:.1f}%"}, pct_cols=["WoW %"])

            fig_vwow = px.bar(vwow, x="Vertical", y="WoW %", color="WoW %",
                color_continuous_scale=["#e74c3c","#f39c12","#2ecc71"],
                template="plotly_dark", title="Vertical WoW % Change")
            fig_vwow.update_xaxes(tickangle=45)
            fig_vwow.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.4)
            st.plotly_chart(fig_vwow, use_container_width=True)

            # Grouped bar: This Week vs Last Week side by side
            vwow_melt = vwow[["Vertical","This Week","Last Week"]].melt(id_vars="Vertical", var_name="Period", value_name="Sale")
            fig_vwow2 = px.bar(vwow_melt, x="Vertical", y="Sale", color="Period", barmode="group",
                template="plotly_dark", title="Vertical: This Week vs Last Week",
                color_discrete_map={"This Week":"#9B59B6","Last Week":"#3498DB"})
            max_vw = vwow_melt["Sale"].max() if not vwow_melt.empty else 1
            ticks_vw = [max_vw*i/5 for i in range(6)]
            fig_vwow2.update_yaxes(tickvals=ticks_vw, ticktext=["₹"+indian_fmt(v) for v in ticks_vw])
            fig_vwow2.update_xaxes(tickangle=45)
            st.plotly_chart(fig_vwow2, use_container_width=True)
        else:
            st.info("Need at least 2 weeks of data for WoW.")

    # ── Vertical MoM ──────────────────────────────────────────────────
    with st.expander("🗓️ Vertical MoM — Month-on-month performance by vertical"):
        vert_mom = df.copy()
        vert_mom["Order Date"] = pd.to_datetime(vert_mom["Order Date"])
        vert_mom["Month"] = vert_mom["Order Date"].dt.to_period("M").apply(lambda r: r.start_time)
        vmonths = sorted(vert_mom["Month"].unique())
        if len(vmonths) >= 2:
            vm1 = vert_mom[vert_mom["Month"]==vmonths[-1]].groupby("Vertical")["Final Sale Amount"].sum()
            vm2 = vert_mom[vert_mom["Month"]==vmonths[-2]].groupby("Vertical")["Final Sale Amount"].sum()
            vmom = pd.DataFrame({"This Month":vm1, "Last Month":vm2}).fillna(0)
            vmom["MoM %"] = ((vmom["This Month"]-vmom["Last Month"])/vmom["Last Month"].replace(0,np.nan)*100).round(1)
            vmom["Trend"] = vmom["MoM %"].apply(lambda x: "🟢 Growing" if x > 5 else ("🔴 Declining" if x < -5 else "🟡 Stable"))
            vmom = vmom.sort_values("This Month", ascending=False).reset_index()
            render_table(vmom, {"This Month":"₹{:,.0f}","Last Month":"₹{:,.0f}","MoM %":"{:.1f}%"}, pct_cols=["MoM %"])

            fig_vmom = px.bar(vmom, x="Vertical", y="MoM %", color="MoM %",
                color_continuous_scale=["#e74c3c","#f39c12","#2ecc71"],
                template="plotly_dark", title="Vertical MoM % Change")
            fig_vmom.update_xaxes(tickangle=45)
            fig_vmom.add_hline(y=0, line_dash="dash", line_color="white", opacity=0.4)
            st.plotly_chart(fig_vmom, use_container_width=True)

            vmom_melt = vmom[["Vertical","This Month","Last Month"]].melt(id_vars="Vertical", var_name="Period", value_name="Sale")
            fig_vmom2 = px.bar(vmom_melt, x="Vertical", y="Sale", color="Period", barmode="group",
                template="plotly_dark", title="Vertical: This Month vs Last Month",
                color_discrete_map={"This Month":"#9B59B6","Last Month":"#3498DB"})
            max_vm = vmom_melt["Sale"].max() if not vmom_melt.empty else 1
            ticks_vm = [max_vm*i/5 for i in range(6)]
            fig_vmom2.update_yaxes(tickvals=ticks_vm, ticktext=["₹"+indian_fmt(v) for v in ticks_vm])
            fig_vmom2.update_xaxes(tickangle=45)
            st.plotly_chart(fig_vmom2, use_container_width=True)
        else:
            st.info("Need at least 2 months of data for MoM.")
    # ════════════════════════════════════════════════════════════════════
    # ════════════════════════════════════════════════════════════════════
    # 3. FRAGRANCE vs NON-FRAGRANCE — Always visible, key growth tracker
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='fragrance'></div>", unsafe_allow_html=True)
    sec_hdr("🌸 Fragrance vs Non-Fragrance Analysis","fragrance")

    frag_df = df.copy()
    frag_df["Order Date"] = pd.to_datetime(frag_df["Order Date"])
    frag_df["Type"] = frag_df["Category"].apply(
        lambda c: "Fragrance" if any(k in str(c).lower() for k in FRAG_KW) else "Non-Fragrance"
    )

    tg = frag_df.groupby("Type").agg(
        Final_Sale=("Final Sale Amount","sum"),
        Cancellation=("Cancellation Amount","sum"),
        Returns=("Return Amount","sum"),
        Units=("Final Sale Units","sum")
    ).reset_index()
    tg["Cancel Rate %"] = (tg["Cancellation"]/(tg["Final_Sale"]+tg["Cancellation"]).replace(0,np.nan)*100).round(1)
    total_frag_sale = tg["Final_Sale"].sum()
    tg["Share %"] = (tg["Final_Sale"]/total_frag_sale*100).round(1) if total_frag_sale > 0 else 0

    # Summary KPI cards
    f_row = tg[tg["Type"]=="Fragrance"].iloc[0] if "Fragrance" in tg["Type"].values else None
    nf_row = tg[tg["Type"]=="Non-Fragrance"].iloc[0] if "Non-Fragrance" in tg["Type"].values else None
    k1, k2, k3, k4 = st.columns(4)
    with k1: metric_card("Fragrance Sale", f_row["Final_Sale"] if f_row is not None else 0)
    with k2: metric_card("Non-Frag Sale", nf_row["Final_Sale"] if nf_row is not None else 0)
    with k3: metric_card("Fragrance Share", f_row["Share %"] if f_row is not None else 0, prefix="", suffix="%")
    with k4: metric_card("Non-Frag Share", nf_row["Share %"] if nf_row is not None else 0, prefix="", suffix="%")

    st.markdown("""<div style='background:rgba(46,204,113,0.1);border:1px solid rgba(46,204,113,0.3);
        border-radius:10px;padding:10px 16px;margin:10px 0'>
        <span style='color:#2ecc71;font-weight:700'>🎯 Growth Focus:</span>
        <span style='color:#aaa;font-size:13px'> Non-Fragrance is the next growth lever — 
        track its share weekly and push category expansion via exclusives, new SKUs, and targeted ads.</span>
    </div>""", unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        fig_tg = px.bar(tg, x="Type", y=["Final_Sale","Cancellation","Returns"], barmode="group",
            template="plotly_dark", title="Fragrance vs Non-Frag: Sale, Cancel, Returns",
            color_discrete_map={"Final_Sale":"#C39BD3","Cancellation":"#e74c3c","Returns":"#e67e22"},
            labels={"value":"₹","variable":"Metric"})
        max_tg = tg[["Final_Sale","Cancellation","Returns"]].max().max() if not tg.empty else 1
        ticks_tg = [max_tg*i/5 for i in range(6)]
        fig_tg.update_yaxes(tickvals=ticks_tg, ticktext=["₹"+indian_fmt(v) for v in ticks_tg])
        st.plotly_chart(fig_tg, use_container_width=True)
    with c2:
        st.plotly_chart(px.pie(tg, values="Final_Sale", names="Type",
            title="Sale Share: Frag vs Non-Frag", template="plotly_dark",
            color_discrete_map=FRAG_COLORS, hole=0.4), use_container_width=True)

    # By Brand — Frag vs Non-Frag breakdown
    tg_brand = frag_df.groupby(["Brand","Type"]).agg(Final_Sale=("Final Sale Amount","sum")).reset_index()
    fig_tgb = px.bar(tg_brand, x="Brand", y="Final_Sale", color="Type", barmode="group",
        template="plotly_dark", title="Brand-wise: Fragrance vs Non-Fragrance Split",
        color_discrete_map=FRAG_COLORS, labels={"Final_Sale":"Final Sale (₹)"})
    max_tgb = tg_brand["Final_Sale"].max() if not tg_brand.empty else 1
    ticks_tgb = [max_tgb*i/5 for i in range(6)]
    fig_tgb.update_yaxes(tickvals=ticks_tgb, ticktext=["₹"+indian_fmt(v) for v in ticks_tgb])
    st.plotly_chart(fig_tgb, use_container_width=True)

    # By Channel — Frag vs Non-Frag
    tg_ch = frag_df.groupby(["Channel","Type"]).agg(Final_Sale=("Final Sale Amount","sum")).reset_index()
    fig_tgch = px.bar(tg_ch, x="Channel", y="Final_Sale", color="Type", barmode="group",
        template="plotly_dark", title="Channel-wise: Fragrance vs Non-Fragrance",
        color_discrete_map=FRAG_COLORS, labels={"Final_Sale":"Final Sale (₹)"})
    max_tgch = tg_ch["Final_Sale"].max() if not tg_ch.empty else 1
    ticks_tgch = [max_tgch*i/5 for i in range(6)]
    fig_tgch.update_yaxes(tickvals=ticks_tgch, ticktext=["₹"+indian_fmt(v) for v in ticks_tgch])
    st.plotly_chart(fig_tgch, use_container_width=True)

    # Daily trend — Frag vs Non-Frag
    dt_frag = frag_df.groupby(["Order Date","Type"]).agg(Final_Sale=("Final Sale Amount","sum")).reset_index()
    fig_dtf = px.line(dt_frag, x="Order Date", y="Final_Sale", color="Type",
        title="Daily Trend: Fragrance vs Non-Fragrance", template="plotly_dark",
        color_discrete_map=FRAG_COLORS, labels={"Final_Sale":"Final Sale (₹)","Order Date":"Date"},
        markers=True)
    max_dtf = dt_frag["Final_Sale"].max() if not dt_frag.empty else 1
    ticks_dtf = [max_dtf*i/5 for i in range(6)]
    fig_dtf.update_yaxes(tickvals=ticks_dtf, ticktext=["₹"+indian_fmt(v) for v in ticks_dtf])
    st.plotly_chart(fig_dtf, use_container_width=True)

    # DoD Frag vs Non-Frag
    with st.expander("📅 Frag vs Non-Frag — DoD Change"):
        frag_dates = sorted(frag_df["Order Date"].unique())
        if len(frag_dates) >= 2:
            ft = frag_df[frag_df["Order Date"]==frag_dates[-1]].groupby("Type")["Final Sale Amount"].sum()
            fy = frag_df[frag_df["Order Date"]==frag_dates[-2]].groupby("Type")["Final Sale Amount"].sum()
            fdod = pd.DataFrame({"Today":ft,"Yesterday":fy}).fillna(0)
            fdod["DoD %"] = ((fdod["Today"]-fdod["Yesterday"])/fdod["Yesterday"].replace(0,np.nan)*100).round(1)
            fdod["Signal"] = fdod.apply(lambda r:
                "🟢 Non-Frag Growing — keep pushing!" if (r.name=="Non-Fragrance" and r["DoD %"]>0)
                else ("🔴 Non-Frag declining — investigate" if (r.name=="Non-Fragrance" and r["DoD %"]<0)
                else ""), axis=1)
            render_table(fdod.reset_index().rename(columns={"index":"Type"}),
                {"Today":"₹{:,.0f}","Yesterday":"₹{:,.0f}","DoD %":"{:.1f}%"}, pct_cols=["DoD %"])
        else:
            st.info("Need 2+ days of data.")

    # WoW Frag vs Non-Frag
    with st.expander("📆 Frag vs Non-Frag — WoW Change"):
        frag_df["Week"] = frag_df["Order Date"].dt.to_period("W").apply(lambda r: r.start_time)
        fweeks = sorted(frag_df["Week"].unique())
        if len(fweeks) >= 2:
            fw1 = frag_df[frag_df["Week"]==fweeks[-1]].groupby("Type")["Final Sale Amount"].sum()
            fw2 = frag_df[frag_df["Week"]==fweeks[-2]].groupby("Type")["Final Sale Amount"].sum()
            fwow = pd.DataFrame({"This Week":fw1,"Last Week":fw2}).fillna(0)
            fwow["WoW %"] = ((fwow["This Week"]-fwow["Last Week"])/fwow["Last Week"].replace(0,np.nan)*100).round(1)
            fwow["Share This Week %"] = (fwow["This Week"]/fwow["This Week"].sum()*100).round(1)
            fwow["Share Last Week %"] = (fwow["Last Week"]/fwow["Last Week"].sum()*100).round(1)
            fwow["Share Shift"] = (fwow["Share This Week %"] - fwow["Share Last Week %"]).round(1)
            render_table(fwow.reset_index().rename(columns={"index":"Type"}),
                {"This Week":"₹{:,.0f}","Last Week":"₹{:,.0f}","WoW %":"{:.1f}%",
                 "Share This Week %":"{:.1f}%","Share Last Week %":"{:.1f}%","Share Shift":"{:.1f}%"},
                pct_cols=["WoW %","Share Shift"])
        else:
            st.info("Need 2+ weeks of data.")

    # MoM Frag vs Non-Frag
    with st.expander("🗓️ Frag vs Non-Frag — MoM Change"):
        frag_df["Month"] = frag_df["Order Date"].dt.to_period("M").apply(lambda r: r.start_time)
        fmonths = sorted(frag_df["Month"].unique())
        if len(fmonths) >= 2:
            fm1 = frag_df[frag_df["Month"]==fmonths[-1]].groupby("Type")["Final Sale Amount"].sum()
            fm2 = frag_df[frag_df["Month"]==fmonths[-2]].groupby("Type")["Final Sale Amount"].sum()
            fmom = pd.DataFrame({"This Month":fm1,"Last Month":fm2}).fillna(0)
            fmom["MoM %"] = ((fmom["This Month"]-fmom["Last Month"])/fmom["Last Month"].replace(0,np.nan)*100).round(1)
            fmom["Share This Month %"] = (fmom["This Month"]/fmom["This Month"].sum()*100).round(1)
            fmom["Share Last Month %"] = (fmom["Last Month"]/fmom["Last Month"].sum()*100).round(1)
            fmom["Share Shift"] = (fmom["Share This Month %"] - fmom["Share Last Month %"]).round(1)
            render_table(fmom.reset_index().rename(columns={"index":"Type"}),
                {"This Month":"₹{:,.0f}","Last Month":"₹{:,.0f}","MoM %":"{:.1f}%",
                 "Share This Month %":"{:.1f}%","Share Last Month %":"{:.1f}%","Share Shift":"{:.1f}%"},
                pct_cols=["MoM %","Share Shift"])

            # Non-Frag category breakdown for current month
            st.markdown("#### 🔍 Non-Frag Categories — This Month")
            nf_cats = frag_df[(frag_df["Month"]==fmonths[-1]) & (frag_df["Type"]=="Non-Fragrance")]
            nf_cat_grp = nf_cats.groupby("Category").agg(
                Final_Sale=("Final Sale Amount","sum"),
                Units=("Final Sale Units","sum"),
                Cancellation=("Cancellation Amount","sum")
            ).reset_index().sort_values("Final_Sale", ascending=False)
            nf_cat_grp["Cancel Rate %"] = (nf_cat_grp["Cancellation"]/(nf_cat_grp["Final_Sale"]+nf_cat_grp["Cancellation"]).replace(0,np.nan)*100).round(1)
            render_table(nf_cat_grp.rename(columns={"Final_Sale":"Final Sale (₹)","Cancellation":"Cancel (₹)","Units":"Units Sold"}),
                {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Units Sold":"{:,.0f}","Cancel Rate %":"{:.1f}%"})
        else:
            st.info("Need 2+ months of data.")

            # ════════════════════════════════════════════════════════════════════
    # A. GMV LEAKAGE WATERFALL
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='waterfall'></div>", unsafe_allow_html=True)
    sec_hdr("💧 GMV Leakage Waterfall","waterfall")

    gross_gmv   = df["GMV"].sum()
    cancel_amt  = df["Cancellation Amount"].sum()
    return_amt  = df["Return Amount"].sum()
    final_sale  = df["Final Sale Amount"].sum()
    leakage_pct = (cancel_amt + return_amt) / gross_gmv * 100 if gross_gmv > 0 else 0

    wf1, wf2, wf3, wf4 = st.columns(4)
    with wf1: metric_card("Gross GMV", gross_gmv)
    with wf2: metric_card("Lost to Cancellations", cancel_amt, prefix="₹", suffix="")
    with wf3: metric_card("Lost to Returns", return_amt, prefix="₹", suffix="")
    with wf4: metric_card("Total Leakage %", round(leakage_pct, 1), prefix="", suffix="%")

    wf_fig = go.Figure(go.Waterfall(
        name="GMV Flow",
        orientation="v",
        measure=["absolute","relative","relative","total"],
        x=["Gross GMV","Cancellations","Returns","Final Sale"],
        y=[gross_gmv, -cancel_amt, -return_amt, final_sale],
        text=["₹"+indian_fmt(gross_gmv), "-₹"+indian_fmt(cancel_amt),
              "-₹"+indian_fmt(return_amt), "₹"+indian_fmt(final_sale)],
        textposition="outside",
        decreasing=dict(marker_color="#e74c3c"),
        increasing=dict(marker_color="#2ecc71"),
        totals=dict(marker_color="#9B59B6"),
        connector=dict(line=dict(color="#444466", width=1.5, dash="dot")),
    ))
    wf_fig.update_layout(template="plotly_dark", title="GMV → Final Sale Waterfall",
                         height=420, showlegend=False)
    max_wf = gross_gmv * 1.05
    ticks_wf = [max_wf * i / 5 for i in range(6)]
    wf_fig.update_yaxes(tickvals=ticks_wf, ticktext=["₹"+indian_fmt(v) for v in ticks_wf])
    st.plotly_chart(wf_fig, use_container_width=True)

    # Waterfall by Brand
    wf_brand = df.groupby("Brand").agg(
        GMV=("GMV","sum"),
        Cancel=("Cancellation Amount","sum"),
        Returns=("Return Amount","sum"),
        Final=("Final Sale Amount","sum")
    ).reset_index().sort_values("GMV", ascending=False)
    wf_brand["Leakage %"] = ((wf_brand["Cancel"]+wf_brand["Returns"])/wf_brand["GMV"].replace(0,np.nan)*100).round(1)
    wf_brand["Recovery %"] = (wf_brand["Final"]/wf_brand["GMV"].replace(0,np.nan)*100).round(1)
    render_table(wf_brand.rename(columns={"GMV":"Gross GMV (₹)","Cancel":"Cancellations (₹)",
        "Returns":"Returns (₹)","Final":"Final Sale (₹)"}),
        {"Gross GMV (₹)":"₹{:,.0f}","Cancellations (₹)":"₹{:,.0f}",
         "Returns (₹)":"₹{:,.0f}","Final Sale (₹)":"₹{:,.0f}",
         "Leakage %":"{:.1f}%","Recovery %":"{:.1f}%"})

    # ════════════════════════════════════════════════════════════════════
    # B. CANCELLATION DEEP DIVE
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='cancel_deep'></div>", unsafe_allow_html=True)
    sec_hdr("❌ Cancellation Deep Dive","cancel_deep")

    # Cancel rate by Brand
    can_brand = df.groupby("Brand").agg(
        Sale=("Final Sale Amount","sum"),
        Cancel=("Cancellation Amount","sum"),
        Cancel_Units=("Cancellation Units","sum"),
        Gross_Units=("Gross Units","sum")
    ).reset_index()
    can_brand["Cancel Rate %"] = (can_brand["Cancel"]/(can_brand["Sale"]+can_brand["Cancel"]).replace(0,np.nan)*100).round(1)
    can_brand["Unit Cancel Rate %"] = (can_brand["Cancel_Units"]/can_brand["Gross_Units"].replace(0,np.nan)*100).round(1)
    can_brand = can_brand.sort_values("Cancel Rate %", ascending=False)

    cd1, cd2 = st.columns(2)
    with cd1:
        fig_can_brand = px.bar(can_brand, x="Brand", y="Cancel Rate %",
            color="Cancel Rate %", color_continuous_scale=["#2ecc71","#f39c12","#e74c3c"],
            template="plotly_dark", title="Cancel Rate % by Brand",
            text="Cancel Rate %")
        fig_can_brand.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_can_brand.add_hline(y=15, line_dash="dash", line_color="#e74c3c",
                                 annotation_text="15% threshold", annotation_position="top right")
        st.plotly_chart(fig_can_brand, use_container_width=True)
    with cd2:
        fig_can_amt = px.bar(can_brand, x="Brand", y="Cancel",
            color="Brand", template="plotly_dark", title="Cancellation Amount by Brand",
            color_discrete_map=BRAND_COLORS, labels={"Cancel":"Cancel Amount (₹)"})
        max_ca = can_brand["Cancel"].max() if not can_brand.empty else 1
        ticks_ca = [max_ca*i/5 for i in range(6)]
        fig_can_amt.update_yaxes(tickvals=ticks_ca, ticktext=["₹"+indian_fmt(v) for v in ticks_ca])
        st.plotly_chart(fig_can_amt, use_container_width=True)

    render_table(can_brand.rename(columns={"Sale":"Final Sale (₹)","Cancel":"Cancel Amt (₹)",
        "Cancel_Units":"Cancel Units","Gross_Units":"Gross Units"}),
        {"Final Sale (₹)":"₹{:,.0f}","Cancel Amt (₹)":"₹{:,.0f}",
         "Cancel Units":"{:,.0f}","Gross Units":"{:,.0f}",
         "Cancel Rate %":"{:.1f}%","Unit Cancel Rate %":"{:.1f}%"})

    # Cancel rate by Vertical
    with st.expander("📊 Cancel Rate by Vertical"):
        can_vert = df.groupby("Vertical").agg(
            Sale=("Final Sale Amount","sum"),
            Cancel=("Cancellation Amount","sum")
        ).reset_index()
        can_vert["Cancel Rate %"] = (can_vert["Cancel"]/(can_vert["Sale"]+can_vert["Cancel"]).replace(0,np.nan)*100).round(1)
        can_vert = can_vert[can_vert["Sale"]>0].sort_values("Cancel Rate %", ascending=False).head(20)
        fig_cv = px.bar(can_vert, x="Vertical", y="Cancel Rate %",
            color="Cancel Rate %", color_continuous_scale=["#2ecc71","#f39c12","#e74c3c"],
            template="plotly_dark", title="Cancel Rate % by Vertical (Top 20)")
        fig_cv.update_xaxes(tickangle=45)
        fig_cv.add_hline(y=15, line_dash="dash", line_color="#e74c3c")
        st.plotly_chart(fig_cv, use_container_width=True)
        render_table(can_vert.rename(columns={"Sale":"Final Sale (₹)","Cancel":"Cancel (₹)"}),
            {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Cancel Rate %":"{:.1f}%"})

    # Top SKUs by cancel rate
    with st.expander("🔍 Top SKUs by Cancellation Rate (min ₹1K sale)"):
        can_sku = df.groupby(["SKU ID","Brand","Category"]).agg(
            Sale=("Final Sale Amount","sum"),
            Cancel=("Cancellation Amount","sum")
        ).reset_index()
        can_sku["Cancel Rate %"] = (can_sku["Cancel"]/(can_sku["Sale"]+can_sku["Cancel"]).replace(0,np.nan)*100).round(1)
        can_sku = can_sku[can_sku["Sale"]>=1000].sort_values("Cancel Rate %", ascending=False).head(20)
        render_table(can_sku.rename(columns={"Sale":"Final Sale (₹)","Cancel":"Cancel (₹)"}),
            {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Cancel Rate %":"{:.1f}%"})

    # Cancel trend over time
    with st.expander("📈 Cancellation Rate Trend Over Time"):
        can_trend = df.copy()
        can_trend["Order Date"] = pd.to_datetime(can_trend["Order Date"])
        can_trend["Week"] = can_trend["Order Date"].dt.to_period("W").apply(lambda r: r.start_time)
        ct = can_trend.groupby("Week").agg(
            Sale=("Final Sale Amount","sum"), Cancel=("Cancellation Amount","sum")
        ).reset_index()
        ct["Cancel Rate %"] = (ct["Cancel"]/(ct["Sale"]+ct["Cancel"]).replace(0,np.nan)*100).round(1)
        fig_ct = px.line(ct, x="Week", y="Cancel Rate %", markers=True,
            template="plotly_dark", title="Weekly Cancellation Rate Trend",
            labels={"Cancel Rate %":"Cancel Rate %","Week":"Week"})
        fig_ct.add_hline(y=15, line_dash="dash", line_color="#e74c3c",
                          annotation_text="15% alert threshold")
        fig_ct.update_traces(line_color="#e74c3c", line_width=2.5)
        st.plotly_chart(fig_ct, use_container_width=True)

    # ════════════════════════════════════════════════════════════════════
    # C. RETURN RATE DEEP DIVE
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='return_deep'></div>", unsafe_allow_html=True)
    sec_hdr("↩️ Return Rate Deep Dive","return_deep")

    ret_brand = df.groupby("Brand").agg(
        Sale=("Final Sale Amount","sum"),
        Returns=("Return Amount","sum"),
        Return_Units=("Return Units","sum"),
        Final_Units=("Final Sale Units","sum")
    ).reset_index()
    ret_brand["Return Rate %"] = (ret_brand["Returns"]/(ret_brand["Sale"]+ret_brand["Returns"]).replace(0,np.nan)*100).round(1)
    ret_brand = ret_brand.sort_values("Return Rate %", ascending=False)

    rb1, rb2 = st.columns(2)
    with rb1:
        fig_rb = px.bar(ret_brand, x="Brand", y="Return Rate %",
            color="Return Rate %", color_continuous_scale=["#2ecc71","#f39c12","#e74c3c"],
            template="plotly_dark", title="Return Rate % by Brand", text="Return Rate %")
        fig_rb.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_rb.add_hline(y=10, line_dash="dash", line_color="#e67e22",
                          annotation_text="10% threshold")
        st.plotly_chart(fig_rb, use_container_width=True)
    with rb2:
        fig_rb2 = px.bar(ret_brand, x="Brand", y="Returns",
            color="Brand", template="plotly_dark", title="Return Amount by Brand",
            color_discrete_map=BRAND_COLORS, labels={"Returns":"Returns (₹)"})
        max_rb = ret_brand["Returns"].max() if not ret_brand.empty else 1
        ticks_rb = [max_rb*i/5 for i in range(6)]
        fig_rb2.update_yaxes(tickvals=ticks_rb, ticktext=["₹"+indian_fmt(v) for v in ticks_rb])
        st.plotly_chart(fig_rb2, use_container_width=True)

    render_table(ret_brand.rename(columns={"Sale":"Final Sale (₹)","Returns":"Returns (₹)",
        "Return_Units":"Return Units","Final_Units":"Final Sale Units"}),
        {"Final Sale (₹)":"₹{:,.0f}","Returns (₹)":"₹{:,.0f}",
         "Return Units":"{:,.0f}","Final Sale Units":"{:,.0f}","Return Rate %":"{:.1f}%"})

    with st.expander("📦 Return Rate by Category"):
        ret_cat = df.groupby(["Category","Brand"]).agg(
            Sale=("Final Sale Amount","sum"), Returns=("Return Amount","sum")
        ).reset_index()
        ret_cat["Return Rate %"] = (ret_cat["Returns"]/(ret_cat["Sale"]+ret_cat["Returns"]).replace(0,np.nan)*100).round(1)
        ret_cat = ret_cat[ret_cat["Sale"]>=1000].sort_values("Return Rate %", ascending=False).head(20)
        render_table(ret_cat.rename(columns={"Sale":"Final Sale (₹)","Returns":"Returns (₹)"}),
            {"Final Sale (₹)":"₹{:,.0f}","Returns (₹)":"₹{:,.0f}","Return Rate %":"{:.1f}%"})

    # ════════════════════════════════════════════════════════════════════
    # D. FULFILLMENT TYPE ANALYSIS
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='fulfillment'></div>", unsafe_allow_html=True)
    sec_hdr("🏷️ Fulfillment Type Analysis","fulfillment")

    ft_grp = df.groupby("Fulfillment Type").agg(
        Final_Sale=("Final Sale Amount","sum"),
        Cancel=("Cancellation Amount","sum"),
        Returns=("Return Amount","sum"),
        Units=("Final Sale Units","sum"),
        GMV=("GMV","sum")
    ).reset_index()
    ft_grp["Cancel Rate %"] = (ft_grp["Cancel"]/(ft_grp["Final_Sale"]+ft_grp["Cancel"]).replace(0,np.nan)*100).round(1)
    ft_grp["Return Rate %"] = (ft_grp["Returns"]/(ft_grp["Final_Sale"]+ft_grp["Returns"]).replace(0,np.nan)*100).round(1)
    ft_grp["Recovery %"] = (ft_grp["Final_Sale"]/ft_grp["GMV"].replace(0,np.nan)*100).round(1)
    ft_grp = ft_grp.sort_values("Final_Sale", ascending=False)

    ff1, ff2, ff3 = st.columns(3)
    with ff1:
        st.plotly_chart(px.pie(ft_grp, values="Final_Sale", names="Fulfillment Type",
            title="Sale Share by Fulfillment", template="plotly_dark",
            color_discrete_sequence=PIE_COLORS, hole=0.35), use_container_width=True)
    with ff2:
        fig_ff2 = px.bar(ft_grp, x="Fulfillment Type", y="Cancel Rate %",
            color="Cancel Rate %", color_continuous_scale=["#2ecc71","#f39c12","#e74c3c"],
            template="plotly_dark", title="Cancel Rate by Fulfillment Type", text="Cancel Rate %")
        fig_ff2.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        st.plotly_chart(fig_ff2, use_container_width=True)
    with ff3:
        fig_ff3 = px.bar(ft_grp, x="Fulfillment Type", y="Return Rate %",
            color="Return Rate %", color_continuous_scale=["#2ecc71","#f39c12","#e74c3c"],
            template="plotly_dark", title="Return Rate by Fulfillment Type", text="Return Rate %")
        fig_ff3.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        st.plotly_chart(fig_ff3, use_container_width=True)

    render_table(ft_grp.rename(columns={"Final_Sale":"Final Sale (₹)","Cancel":"Cancel (₹)",
        "Returns":"Returns (₹)","Units":"Units Sold","GMV":"Gross GMV (₹)"}),
        {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Returns (₹)":"₹{:,.0f}",
         "Units Sold":"{:,.0f}","Gross GMV (₹)":"₹{:,.0f}",
         "Cancel Rate %":"{:.1f}%","Return Rate %":"{:.1f}%","Recovery %":"{:.1f}%"})

    # ════════════════════════════════════════════════════════════════════
    # E. NEW SKU TRACKER
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='new_skus'></div>", unsafe_allow_html=True)
    sec_hdr("🆕 New SKU Tracker","new_skus")

    new_sku_df = df.copy()
    new_sku_df["Order Date"] = pd.to_datetime(new_sku_df["Order Date"])
    max_dt = new_sku_df["Order Date"].max()

    ns1, ns2 = st.columns(2)
    with ns1:
        window_days = st.selectbox("New SKU window", [7, 14, 30], index=0, key="new_sku_window")
    cutoff = max_dt - pd.Timedelta(days=window_days)

    recent_skus = set(new_sku_df[new_sku_df["Order Date"] >= cutoff]["SKU ID"].astype(str).unique())
    older_skus  = set(new_sku_df[new_sku_df["Order Date"] <  cutoff]["SKU ID"].astype(str).unique())
    truly_new   = recent_skus - older_skus

    st.markdown(f"<div style='background:rgba(46,204,113,0.1);border:1px solid rgba(46,204,113,0.3);border-radius:10px;padding:10px 16px;margin:8px 0'>"
                f"<span style='color:#2ecc71;font-weight:700'>🆕 {len(truly_new)} new SKUs</span>"
                f"<span style='color:#aaa;font-size:13px'> appeared in last {window_days} days that had zero sales before</span></div>",
                unsafe_allow_html=True)

    if truly_new:
        new_sku_data = new_sku_df[
            (new_sku_df["Order Date"] >= cutoff) &
            (new_sku_df["SKU ID"].astype(str).isin(truly_new))
        ].groupby(["SKU ID","Brand","Category","Fulfillment Type"]).agg(
            Final_Sale=("Final Sale Amount","sum"),
            Units=("Final Sale Units","sum"),
            Cancel=("Cancellation Amount","sum"),
            Days_Active=("Order Date", lambda x: x.nunique())
        ).reset_index().sort_values("Final_Sale", ascending=False)
        new_sku_data["Cancel Rate %"] = (new_sku_data["Cancel"]/(new_sku_data["Final_Sale"]+new_sku_data["Cancel"]).replace(0,np.nan)*100).round(1)
        new_sku_data["Daily Avg (₹)"] = (new_sku_data["Final_Sale"]/new_sku_data["Days_Active"].replace(0,np.nan)).round(0)

        fig_ns = px.bar(new_sku_data.head(20), x="SKU ID", y="Final_Sale",
            color="Brand", template="plotly_dark",
            title=f"New SKUs — Final Sale (Last {window_days} Days)",
            color_discrete_map=BRAND_COLORS, labels={"Final_Sale":"Final Sale (₹)"})
        max_ns = new_sku_data["Final_Sale"].max() if not new_sku_data.empty else 1
        ticks_ns = [max_ns*i/5 for i in range(6)]
        fig_ns.update_yaxes(tickvals=ticks_ns, ticktext=["₹"+indian_fmt(v) for v in ticks_ns])
        fig_ns.update_xaxes(tickangle=45)
        st.plotly_chart(fig_ns, use_container_width=True)

        render_table(new_sku_data.rename(columns={"Final_Sale":"Final Sale (₹)","Cancel":"Cancel (₹)",
            "Units":"Units","Days_Active":"Days Active"}),
            {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Units":"{:,.0f}",
             "Days Active":"{:,.0f}","Cancel Rate %":"{:.1f}%","Daily Avg (₹)":"₹{:,.0f}"})
    else:
        st.info(f"No brand-new SKUs in the last {window_days} days.")

    # ════════════════════════════════════════════════════════════════════
    # F. SKU HEALTH SCORECARD
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='sku_health'></div>", unsafe_allow_html=True)
    sec_hdr("🏆 SKU Health Scorecard","sku_health")

    sku_health = df.copy()
    sku_health["Order Date"] = pd.to_datetime(sku_health["Order Date"])
    sku_health["Week"] = sku_health["Order Date"].dt.to_period("W").apply(lambda r: r.start_time)
    weeks_h = sorted(sku_health["Week"].unique())

    sh = sku_health.groupby(["SKU ID","Brand","Category"]).agg(
        Sale=("Final Sale Amount","sum"),
        Cancel=("Cancellation Amount","sum"),
        Returns=("Return Amount","sum"),
        Units=("Final Sale Units","sum"),
        GMV=("GMV","sum")
    ).reset_index()
    sh["Cancel Rate %"] = (sh["Cancel"]/(sh["Sale"]+sh["Cancel"]).replace(0,np.nan)*100).round(1)
    sh["Return Rate %"] = (sh["Returns"]/(sh["Sale"]+sh["Returns"]).replace(0,np.nan)*100).round(1)

    # WoW trend
    if len(weeks_h) >= 2:
        lw_h = sku_health[sku_health["Week"]==weeks_h[-1]].groupby("SKU ID")["Final Sale Amount"].sum()
        pw_h = sku_health[sku_health["Week"]==weeks_h[-2]].groupby("SKU ID")["Final Sale Amount"].sum()
        lw_h.index = lw_h.index.astype(str)
        pw_h.index = pw_h.index.astype(str)
        sh["SKU ID"] = sh["SKU ID"].astype(str)
        sh["WoW %"] = sh["SKU ID"].map(
            ((lw_h - pw_h) / pw_h.replace(0, np.nan) * 100).round(1)
        )

    sh = sh[sh["Sale"] >= 500].copy()

    # Composite health score (0-100)
    # Higher sale rank = better, lower cancel = better, lower return = better, positive WoW = better
    sh["Sale_Score"]   = pd.qcut(sh["Sale"].rank(method="first"), 5, labels=[1,2,3,4,5]).astype(float)
    sh["Cancel_Score"] = pd.qcut(sh["Cancel Rate %"].rank(method="first", ascending=False), 5, labels=[1,2,3,4,5]).astype(float)
    sh["Return_Score"] = pd.qcut(sh["Return Rate %"].rank(method="first", ascending=False), 5, labels=[1,2,3,4,5]).astype(float)
    if "WoW %" in sh.columns:
        sh["WoW_Score"] = sh["WoW %"].apply(lambda x: 5 if x > 20 else (4 if x > 5 else (3 if x >= 0 else (2 if x > -20 else 1))) if pd.notna(x) else 3)
    else:
        sh["WoW_Score"] = 3

    sh["Health Score"] = ((sh["Sale_Score"]*0.4 + sh["Cancel_Score"]*0.3 +
                           sh["Return_Score"]*0.2 + sh["WoW_Score"]*0.1) * 20).round(0).astype(int)
    sh["Status"] = sh["Health Score"].apply(
        lambda x: "🟢 Healthy" if x >= 70 else ("🟡 Watch" if x >= 45 else "🔴 Critical"))
    sh = sh.sort_values("Health Score", ascending=False)

    # Summary
    hc1, hc2, hc3 = st.columns(3)
    with hc1: metric_card("🟢 Healthy SKUs", len(sh[sh["Status"]=="🟢 Healthy"]), prefix="", suffix="")
    with hc2: metric_card("🟡 Watch SKUs",   len(sh[sh["Status"]=="🟡 Watch"]),   prefix="", suffix="")
    with hc3: metric_card("🔴 Critical SKUs",len(sh[sh["Status"]=="🔴 Critical"]),prefix="", suffix="")

    status_filter = st.selectbox("Filter by Status", ["All","🟢 Healthy","🟡 Watch","🔴 Critical"], key="sku_health_filter")
    sh_show = sh if status_filter == "All" else sh[sh["Status"]==status_filter]

    cols_sh = ["SKU ID","Brand","Category","Sale","Cancel Rate %","Return Rate %","Health Score","Status"]
    if "WoW %" in sh.columns: cols_sh.insert(6, "WoW %")
    render_table(sh_show[cols_sh].rename(columns={"Sale":"Final Sale (₹)"}),
        {"Final Sale (₹)":"₹{:,.0f}","Cancel Rate %":"{:.1f}%",
         "Return Rate %":"{:.1f}%","Health Score":"{:.0f}"},
        pct_cols=["WoW %"] if "WoW %" in sh_show.columns else [])

    # ════════════════════════════════════════════════════════════════════
    # G. TOP GROWING vs DECLINING SKUs
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='sku_trends'></div>", unsafe_allow_html=True)
    sec_hdr("📈 Top Growing vs Declining SKUs","sku_trends")

    sku_trend_df = df.copy()
    sku_trend_df["Order Date"] = pd.to_datetime(sku_trend_df["Order Date"])
    sku_trend_df["Week"] = sku_trend_df["Order Date"].dt.to_period("W").apply(lambda r: r.start_time)
    weeks_st = sorted(sku_trend_df["Week"].unique())

    if len(weeks_st) >= 2:
        lw_st = sku_trend_df[sku_trend_df["Week"]==weeks_st[-1]].groupby(["SKU ID","Brand","Category"])["Final Sale Amount"].sum().reset_index()
        pw_st = sku_trend_df[sku_trend_df["Week"]==weeks_st[-2]].groupby("SKU ID")["Final Sale Amount"].sum().reset_index()
        lw_st.columns = ["SKU ID","Brand","Category","This Week"]
        pw_st.columns = ["SKU ID","Last Week"]
        lw_st["SKU ID"] = lw_st["SKU ID"].astype(str)
        pw_st["SKU ID"] = pw_st["SKU ID"].astype(str)
        merged_st = lw_st.merge(pw_st, on="SKU ID", how="outer").fillna(0)
        merged_st["WoW %"] = ((merged_st["This Week"]-merged_st["Last Week"])/merged_st["Last Week"].replace(0,np.nan)*100).round(1)
        merged_st = merged_st[merged_st["Last Week"] >= 500]

        growing  = merged_st[merged_st["WoW %"] > 0].sort_values("WoW %", ascending=False).head(10)
        declining= merged_st[merged_st["WoW %"] < 0].sort_values("WoW %").head(10)

        gt1, gt2 = st.columns(2)
        with gt1:
            st.markdown("<div style='background:rgba(46,204,113,0.1);border:1px solid rgba(46,204,113,0.3);border-radius:8px;padding:8px 14px;margin-bottom:8px'><span style='color:#2ecc71;font-weight:700'>📈 Top 10 Growing SKUs (WoW)</span></div>", unsafe_allow_html=True)
            if not growing.empty:
                fig_grow = px.bar(growing, x="SKU ID", y="WoW %", color="Brand",
                    template="plotly_dark", color_discrete_map=BRAND_COLORS,
                    title="Top Growing SKUs", labels={"WoW %":"WoW Growth %"})
                fig_grow.update_xaxes(tickangle=45)
                st.plotly_chart(fig_grow, use_container_width=True)
                render_table(growing, {"This Week":"₹{:,.0f}","Last Week":"₹{:,.0f}","WoW %":"{:.1f}%"}, pct_cols=["WoW %"])
            else:
                st.info("No growing SKUs this week.")

        with gt2:
            st.markdown("<div style='background:rgba(231,76,60,0.1);border:1px solid rgba(231,76,60,0.3);border-radius:8px;padding:8px 14px;margin-bottom:8px'><span style='color:#e74c3c;font-weight:700'>📉 Top 10 Declining SKUs (WoW)</span></div>", unsafe_allow_html=True)
            if not declining.empty:
                fig_dec = px.bar(declining, x="SKU ID", y="WoW %", color="Brand",
                    template="plotly_dark", color_discrete_map=BRAND_COLORS,
                    title="Top Declining SKUs", labels={"WoW %":"WoW Change %"})
                fig_dec.update_xaxes(tickangle=45)
                st.plotly_chart(fig_dec, use_container_width=True)
                render_table(declining, {"This Week":"₹{:,.0f}","Last Week":"₹{:,.0f}","WoW %":"{:.1f}%"}, pct_cols=["WoW %"])
            else:
                st.info("No declining SKUs this week.")
    else:
        st.info("Need at least 2 weeks of data.")

    # ════════════════════════════════════════════════════════════════════
    # H. CATEGORY PERFORMANCE HEATMAP
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='cat_heatmap'></div>", unsafe_allow_html=True)
    sec_hdr("🗺️ Category Performance Heatmap","cat_heatmap")

    heat_df = df.groupby(["Brand","Category"])["Final Sale Amount"].sum().reset_index()
    heat_pivot = heat_df.pivot(index="Brand", columns="Category", values="Final Sale Amount").fillna(0)

    if not heat_pivot.empty:
        # Limit to top 15 categories by total sale
        top_cats = heat_df.groupby("Category")["Final Sale Amount"].sum().nlargest(15).index
        heat_pivot = heat_pivot[[c for c in top_cats if c in heat_pivot.columns]]

        fig_heat = px.imshow(heat_pivot,
            color_continuous_scale=["#0a0a14","#2a1a4a","#6C3483","#9B59B6","#D7BDE2"],
            template="plotly_dark",
            title="Brand × Category Final Sale Heatmap (₹)",
            text_auto=False,
            aspect="auto")
        # Add text annotations
        annotations = []
        for i, brand in enumerate(heat_pivot.index):
            for j, cat in enumerate(heat_pivot.columns):
                val = heat_pivot.loc[brand, cat]
                if val > 0:
                    annotations.append(dict(
                        x=j, y=i,
                        text="₹" + indian_fmt(val),
                        showarrow=False,
                        font=dict(size=9, color="white")
                    ))
        fig_heat.update_layout(annotations=annotations, height=max(300, len(heat_pivot)*60))
        fig_heat.update_xaxes(tickangle=45)
        st.plotly_chart(fig_heat, use_container_width=True)

        st.caption("💡 White spaces = growth opportunities. Dark cells = zero/low contribution.")

    # ════════════════════════════════════════════════════════════════════
    # I. LOCATION ANALYSIS
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='location'></div>", unsafe_allow_html=True)
    sec_hdr("📍 Location Analysis","location")

    loc_grp = df.groupby("Location Id").agg(
        Final_Sale=("Final Sale Amount","sum"),
        Cancel=("Cancellation Amount","sum"),
        Returns=("Return Amount","sum"),
        Units=("Final Sale Units","sum"),
        GMV=("GMV","sum")
    ).reset_index()
    loc_grp["Cancel Rate %"] = (loc_grp["Cancel"]/(loc_grp["Final_Sale"]+loc_grp["Cancel"]).replace(0,np.nan)*100).round(1)
    loc_grp["Return Rate %"] = (loc_grp["Returns"]/(loc_grp["Final_Sale"]+loc_grp["Returns"]).replace(0,np.nan)*100).round(1)
    loc_grp = loc_grp[loc_grp["Final_Sale"] > 0].sort_values("Final_Sale", ascending=False)

    la1, la2 = st.columns(2)
    with la1:
        top_locs = loc_grp.head(15)
        fig_loc = px.bar(top_locs, x="Location Id", y="Final_Sale",
            color="Cancel Rate %", color_continuous_scale=["#2ecc71","#f39c12","#e74c3c"],
            template="plotly_dark", title="Top 15 Locations by Sale (color = Cancel Rate)",
            labels={"Final_Sale":"Final Sale (₹)","Location Id":"Location"})
        max_loc = top_locs["Final_Sale"].max() if not top_locs.empty else 1
        ticks_loc = [max_loc*i/5 for i in range(6)]
        fig_loc.update_yaxes(tickvals=ticks_loc, ticktext=["₹"+indian_fmt(v) for v in ticks_loc])
        fig_loc.update_xaxes(tickangle=45)
        st.plotly_chart(fig_loc, use_container_width=True)
    with la2:
        high_cancel_locs = loc_grp[loc_grp["Final_Sale"] >= loc_grp["Final_Sale"].quantile(0.25)].sort_values("Cancel Rate %", ascending=False).head(15)
        fig_loc2 = px.bar(high_cancel_locs, x="Location Id", y="Cancel Rate %",
            color="Cancel Rate %", color_continuous_scale=["#2ecc71","#f39c12","#e74c3c"],
            template="plotly_dark", title="High Cancel Rate Locations",
            text="Cancel Rate %")
        fig_loc2.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_loc2.update_xaxes(tickangle=45)
        st.plotly_chart(fig_loc2, use_container_width=True)

    render_table(loc_grp.head(30).rename(columns={"Final_Sale":"Final Sale (₹)","Cancel":"Cancel (₹)",
        "Returns":"Returns (₹)","Units":"Units Sold","GMV":"Gross GMV (₹)","Location Id":"Location"}),
        {"Final Sale (₹)":"₹{:,.0f}","Cancel (₹)":"₹{:,.0f}","Returns (₹)":"₹{:,.0f}",
         "Units Sold":"{:,.0f}","Gross GMV (₹)":"₹{:,.0f}",
         "Cancel Rate %":"{:.1f}%","Return Rate %":"{:.1f}%"})

    # ════════════════════════════════════════════════════════════════════
    # J. NON-FRAG GROWTH TRACKER
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='nonfrag_tracker'></div>", unsafe_allow_html=True)
    sec_hdr("🌱 Non-Frag Growth Tracker","nonfrag_tracker")

    nfg = df.copy()
    nfg["Order Date"] = pd.to_datetime(nfg["Order Date"])
    nfg["Type"] = nfg["Category"].apply(
        lambda c: "Fragrance" if any(k in str(c).lower() for k in FRAG_KW) else "Non-Fragrance"
    )
    nfg["Week"] = nfg["Order Date"].dt.to_period("W").apply(lambda r: r.start_time)

    nfg_weekly = nfg.groupby(["Week","Type"])["Final Sale Amount"].sum().reset_index()
    nfg_total  = nfg_weekly.groupby("Week")["Final Sale Amount"].sum().reset_index().rename(columns={"Final Sale Amount":"Total"})
    nfg_weekly = nfg_weekly.merge(nfg_total, on="Week")
    nfg_weekly["Share %"] = (nfg_weekly["Final Sale Amount"]/nfg_weekly["Total"]*100).round(1)
    nfg_nf = nfg_weekly[nfg_weekly["Type"]=="Non-Fragrance"].copy()

    ngt1, ngt2 = st.columns(2)
    nf_target = st.sidebar.selectbox("🌱 Non-Frag Share Target (%)", [20,25,30,35,40,45,50], index=2, key="nf_target")

    with ngt1:
        fig_nfg = px.line(nfg_nf, x="Week", y="Share %", markers=True,
            template="plotly_dark", title="Non-Fragrance Share % — Weekly Trend",
            labels={"Share %":"Non-Frag Share %","Week":"Week"})
        fig_nfg.add_hline(y=nf_target, line_dash="dash", line_color="#2ecc71",
                           annotation_text=f"Target: {nf_target}%", annotation_position="top right")
        fig_nfg.update_traces(line_color="#2ecc71", line_width=2.5,
                               marker=dict(size=8, color="#2ecc71"))
        fig_nfg.update_yaxes(range=[0, max(nfg_nf["Share %"].max()*1.3, nf_target*1.2) if not nfg_nf.empty else 50])
        st.plotly_chart(fig_nfg, use_container_width=True)

    with ngt2:
        fig_nfg2 = px.bar(nfg_weekly, x="Week", y="Final Sale Amount", color="Type",
            template="plotly_dark", title="Weekly Sale: Frag vs Non-Frag",
            color_discrete_map=FRAG_COLORS, labels={"Final Sale Amount":"Final Sale (₹)"})
        max_nfg2 = nfg_weekly["Final Sale Amount"].max() if not nfg_weekly.empty else 1
        ticks_nfg2 = [max_nfg2*i/5 for i in range(6)]
        fig_nfg2.update_yaxes(tickvals=ticks_nfg2, ticktext=["₹"+indian_fmt(v) for v in ticks_nfg2])
        st.plotly_chart(fig_nfg2, use_container_width=True)

    if not nfg_nf.empty:
        latest_nf_share = nfg_nf.iloc[-1]["Share %"]
        gap = nf_target - latest_nf_share
        color = "#2ecc71" if gap <= 0 else "#e74c3c"
        st.markdown(f"<div style='background:rgba(46,204,113,0.08);border:1px solid #2a2a4a;border-radius:10px;padding:12px 18px;margin:8px 0'>"
                    f"<span style='color:#D7BDE2;font-weight:700'>Current Non-Frag Share: </span>"
                    f"<span style='color:{color};font-size:20px;font-weight:800'>{latest_nf_share:.1f}%</span>"
                    f"<span style='color:#aaa;font-size:13px'> &nbsp;|&nbsp; Gap to {nf_target}% target: "
                    f"<span style='color:{color};font-weight:700'>{gap:+.1f}pp</span></span></div>",
                    unsafe_allow_html=True)

    # Non-frag by category — top contributors
    nfg_cats = nfg[nfg["Type"]=="Non-Fragrance"].groupby("Category").agg(
        Final_Sale=("Final Sale Amount","sum"), Units=("Final Sale Units","sum")
    ).reset_index().sort_values("Final_Sale", ascending=False).head(10)
    fig_nfg_cats = px.bar(nfg_cats, x="Category", y="Final_Sale",
        color="Final_Sale", color_continuous_scale=["#1a4a1a","#2ecc71"],
        template="plotly_dark", title="Top Non-Frag Categories",
        labels={"Final_Sale":"Final Sale (₹)"})
    max_nfc = nfg_cats["Final_Sale"].max() if not nfg_cats.empty else 1
    ticks_nfc = [max_nfc*i/5 for i in range(6)]
    fig_nfg_cats.update_yaxes(tickvals=ticks_nfc, ticktext=["₹"+indian_fmt(v) for v in ticks_nfc])
    fig_nfg_cats.update_xaxes(tickangle=45)
    st.plotly_chart(fig_nfg_cats, use_container_width=True)

    # ════════════════════════════════════════════════════════════════════
    # K. BRAND CONTRIBUTION TREND
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='brand_trend'></div>", unsafe_allow_html=True)
    sec_hdr("📊 Brand Contribution Trend","brand_trend")

    bct = df.copy()
    bct["Order Date"] = pd.to_datetime(bct["Order Date"])
    bct["Week"] = bct["Order Date"].dt.to_period("W").apply(lambda r: r.start_time)
    bct_grp = bct.groupby(["Week","Brand"])["Final Sale Amount"].sum().reset_index()
    bct_total = bct_grp.groupby("Week")["Final Sale Amount"].sum().reset_index().rename(columns={"Final Sale Amount":"Total"})
    bct_grp = bct_grp.merge(bct_total, on="Week")
    bct_grp["Share %"] = (bct_grp["Final Sale Amount"]/bct_grp["Total"]*100).round(1)
    bct_grp["Week_Str"] = bct_grp["Week"].dt.strftime("W/C %d %b")

    bc1, bc2 = st.columns(2)
    with bc1:
        fig_bct = px.area(bct_grp, x="Week_Str", y="Final Sale Amount", color="Brand",
            template="plotly_dark", title="Brand Revenue — Weekly Stacked Area",
            color_discrete_map=BRAND_COLORS, labels={"Final Sale Amount":"Final Sale (₹)","Week_Str":"Week"})
        max_bct = bct_grp.groupby("Week_Str")["Final Sale Amount"].sum().max() if not bct_grp.empty else 1
        ticks_bct = [max_bct*i/5 for i in range(6)]
        fig_bct.update_yaxes(tickvals=ticks_bct, ticktext=["₹"+indian_fmt(v) for v in ticks_bct])
        fig_bct.update_xaxes(tickangle=45)
        st.plotly_chart(fig_bct, use_container_width=True)

    with bc2:
        fig_bct2 = px.line(bct_grp, x="Week_Str", y="Share %", color="Brand",
            template="plotly_dark", title="Brand Share % — Weekly Trend",
            color_discrete_map=BRAND_COLORS, markers=True,
            labels={"Share %":"Revenue Share %","Week_Str":"Week"})
        fig_bct2.update_xaxes(tickangle=45)
        st.plotly_chart(fig_bct2, use_container_width=True)

    # ════════════════════════════════════════════════════════════════════
    # L. WEEKLY BRAND PERFORMANCE MATRIX
    # ════════════════════════════════════════════════════════════════════
    st.markdown("<div id='weekly_matrix'></div>", unsafe_allow_html=True)
    sec_hdr("📋 Weekly Brand Performance Matrix","weekly_matrix")

    wbm = df.copy()
    wbm["Order Date"] = pd.to_datetime(wbm["Order Date"])
    wbm["Week"] = wbm["Order Date"].dt.to_period("W").apply(lambda r: r.start_time).dt.strftime("W/C %d %b")

    metric_choice = st.selectbox("Matrix Metric", ["Final Sale Amount","Cancellation Amount","Return Amount","Final Sale Units"], key="matrix_metric")

    wbm_pivot = wbm.groupby(["Brand","Week"])[metric_choice].sum().reset_index()
    wbm_pivot = wbm_pivot.pivot(index="Brand", columns="Week", values=metric_choice).fillna(0)

    if not wbm_pivot.empty:
        fig_wbm = px.imshow(wbm_pivot,
            color_continuous_scale=["#0a0a14","#2a1a4a","#6C3483","#9B59B6","#D7BDE2"],
            template="plotly_dark",
            title=f"Brand × Week: {metric_choice}",
            aspect="auto")
        # Add value annotations
        wbm_annotations = []
        for i, brand in enumerate(wbm_pivot.index):
            for j, week in enumerate(wbm_pivot.columns):
                val = wbm_pivot.loc[brand, week]
                wbm_annotations.append(dict(
                    x=j, y=i,
                    text="₹"+indian_fmt(val) if "Amount" in metric_choice else indian_fmt(val),
                    showarrow=False,
                    font=dict(size=9, color="white")
                ))
        fig_wbm.update_layout(annotations=wbm_annotations, height=max(250, len(wbm_pivot)*60))
        st.plotly_chart(fig_wbm, use_container_width=True)

        st.caption("💡 Scan left to right to spot which brands are growing week over week. Dark = low, bright = high.")

        # WoW change matrix
        with st.expander("📊 WoW % Change Matrix"):
            wbm_cols = wbm_pivot.columns.tolist()
            if len(wbm_cols) >= 2:
                wbm_pct = wbm_pivot.copy()
                for i in range(len(wbm_cols)-1, 0, -1):
                    wbm_pct[wbm_cols[i]] = ((wbm_pivot[wbm_cols[i]] - wbm_pivot[wbm_cols[i-1]]) /
                                             wbm_pivot[wbm_cols[i-1]].replace(0, np.nan) * 100).round(1)
                wbm_pct = wbm_pct[wbm_cols[1:]]
                fig_wbm_pct = px.imshow(wbm_pct,
                    color_continuous_scale=["#e74c3c","#f39c12","#2ecc71"],
                    color_continuous_midpoint=0,
                    template="plotly_dark",
                    title="WoW % Change by Brand × Week",
                    aspect="auto")
                pct_annotations = []
                for i, brand in enumerate(wbm_pct.index):
                    for j, week in enumerate(wbm_pct.columns):
                        val = wbm_pct.loc[brand, week]
                        if pd.notna(val):
                            pct_annotations.append(dict(
                                x=j, y=i,
                                text=f"{val:+.1f}%",
                                showarrow=False,
                                font=dict(size=9, color="white")
                            ))
                fig_wbm_pct.update_layout(annotations=pct_annotations, height=max(250, len(wbm_pct)*60))
                st.plotly_chart(fig_wbm_pct, use_container_width=True)
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
