import streamlit as st
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import warnings
warnings.filterwarnings("ignore")

st.set_page_config(
    page_title="Flipkart Sales Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── CONSTANTS ────────────────────────────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
REQUIRED_COLS = [
    "Product Id", "SKU ID", "Category", "Brand", "Vertical",
    "Order Date", "Fulfillment Type", "Location Id",
    "Gross Units", "GMV",
    "Cancellation Units", "Cancellation Amount",
    "Return Units", "Return Amount",
    "Final Sale Units", "Final Sale Amount"
]

BRAND_COLORS = {
    "BELLAVITA": "#6C3483",
    "Bella vita organic": "#A569BD",
    "Kenaz": "#1A5276",
    "Embarouge": "#C0392B",
    "HipHop Skincare": "#117A65",
}

# ─── GOOGLE SHEETS AUTH ───────────────────────────────────────────────────────
@st.cache_resource
def get_gsheet_client():
    creds_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

def get_or_create_sheet(client, spreadsheet_name="Flipkart_Sales_DB"):
    try:
        sh = client.open(spreadsheet_name)
    except gspread.SpreadsheetNotFound:
        sh = client.create(spreadsheet_name)
        sh.share(st.secrets["gcp_service_account"]["client_email"], perm_type="user", role="writer")
    return sh

def load_data_from_gsheet(client, spreadsheet_name="Flipkart_Sales_DB"):
    try:
        sh = get_or_create_sheet(client, spreadsheet_name)
        ws = sh.sheet1
        data = ws.get_all_records()
        if not data:
            return pd.DataFrame(columns=REQUIRED_COLS)
        df = pd.DataFrame(data)
        df["Order Date"] = pd.to_datetime(df["Order Date"])
        for col in ["GMV", "Cancellation Amount", "Return Amount", "Final Sale Amount",
                    "Gross Units", "Cancellation Units", "Return Units", "Final Sale Units"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        return df
    except Exception as e:
        st.error(f"Error loading from Google Sheets: {e}")
        return pd.DataFrame(columns=REQUIRED_COLS)

def append_to_gsheet(client, new_df, spreadsheet_name="Flipkart_Sales_DB"):
    sh = get_or_create_sheet(client, spreadsheet_name)
    ws = sh.sheet1

    existing_data = ws.get_all_records()
    if not existing_data:
        # First upload — write headers + data
        ws.update([new_df.columns.tolist()] + new_df.astype(str).values.tolist())
        return len(new_df), 0

    existing_df = pd.DataFrame(existing_data)
    existing_df["Order Date"] = pd.to_datetime(existing_df["Order Date"])
    new_df["Order Date"] = pd.to_datetime(new_df["Order Date"])

    # Deduplicate on Product Id + SKU ID + Order Date + Brand
    merge_keys = ["Product Id", "SKU ID", "Order Date", "Brand"]
    existing_keys = existing_df[merge_keys].astype(str).apply(lambda r: "_".join(r), axis=1)
    new_keys = new_df[merge_keys].astype(str).apply(lambda r: "_".join(r), axis=1)
    truly_new = new_df[~new_keys.isin(existing_keys)]

    if len(truly_new) == 0:
        return 0, len(new_df)

    combined = pd.concat([existing_df, truly_new], ignore_index=True)
    combined["Order Date"] = combined["Order Date"].dt.strftime("%Y-%m-%d")
    ws.clear()
    ws.update([combined.columns.tolist()] + combined.astype(str).values.tolist())
    return len(truly_new), len(new_df) - len(truly_new)

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def safe_pct(new, old):
    if old == 0:
        return None
    return round((new - old) / old * 100, 1)

def arrow(pct, inverse=False):
    if pct is None:
        return "—"
    good = pct >= 0
    if inverse:
        good = not good
    color = "green" if good else "red"
    symbol = "▲" if pct >= 0 else "▼"
    return f"<span style='color:{color}'>{symbol} {abs(pct):.1f}%</span>"

def metric_card(label, value, delta_html="", prefix="₹"):
    val_str = f"{prefix}{value:,.0f}" if prefix else f"{value:,.0f}"
    st.markdown(f"""
    <div style='background:#1e1e2f;padding:16px 20px;border-radius:12px;border-left:4px solid #6C3483;'>
        <div style='color:#aaa;font-size:13px;margin-bottom:4px;'>{label}</div>
        <div style='color:#fff;font-size:22px;font-weight:700;'>{val_str}</div>
        <div style='font-size:12px;margin-top:4px;'>{delta_html}</div>
    </div>
    """, unsafe_allow_html=True)

# ─── ANALYSIS FUNCTIONS ───────────────────────────────────────────────────────
def dod_analysis(df, brand=None):
    d = df.copy()
    if brand and brand != "All":
        d = d[d["Brand"] == brand]
    d["Order Date"] = pd.to_datetime(d["Order Date"])
    daily = d.groupby("Order Date").agg(
        Final_Sale=("Final Sale Amount", "sum"),
        Cancellation=("Cancellation Amount", "sum"),
        Returns=("Return Amount", "sum"),
        Sale_Units=("Final Sale Units", "sum"),
        Cancel_Units=("Cancellation Units", "sum"),
        Return_Units=("Return Units", "sum"),
    ).reset_index().sort_values("Order Date")
    daily["DoD_Sale_%"] = daily["Final_Sale"].pct_change() * 100
    daily["DoD_Cancel_%"] = daily["Cancellation"].pct_change() * 100
    daily["DoD_Return_%"] = daily["Returns"].pct_change() * 100
    return daily

def wow_analysis(df, brand=None):
    d = df.copy()
    if brand and brand != "All":
        d = d[d["Brand"] == brand]
    d["Order Date"] = pd.to_datetime(d["Order Date"])
    d["Week"] = d["Order Date"].dt.to_period("W").apply(lambda r: r.start_time)
    weekly = d.groupby("Week").agg(
        Final_Sale=("Final Sale Amount", "sum"),
        Cancellation=("Cancellation Amount", "sum"),
        Returns=("Return Amount", "sum"),
    ).reset_index().sort_values("Week")
    weekly["WoW_Sale_%"] = weekly["Final_Sale"].pct_change() * 100
    weekly["WoW_Cancel_%"] = weekly["Cancellation"].pct_change() * 100
    weekly["WoW_Return_%"] = weekly["Returns"].pct_change() * 100
    return weekly

def mom_analysis(df, brand=None):
    d = df.copy()
    if brand and brand != "All":
        d = d[d["Brand"] == brand]
    d["Order Date"] = pd.to_datetime(d["Order Date"])
    d["Month"] = d["Order Date"].dt.to_period("M").apply(lambda r: r.start_time)
    monthly = d.groupby("Month").agg(
        Final_Sale=("Final Sale Amount", "sum"),
        Cancellation=("Cancellation Amount", "sum"),
        Returns=("Return Amount", "sum"),
    ).reset_index().sort_values("Month")
    monthly["MoM_Sale_%"] = monthly["Final_Sale"].pct_change() * 100
    monthly["MoM_Cancel_%"] = monthly["Cancellation"].pct_change() * 100
    monthly["MoM_Return_%"] = monthly["Returns"].pct_change() * 100
    return monthly

def declining_skus(df, brand=None, top_n=15):
    d = df.copy()
    if brand and brand != "All":
        d = d[d["Brand"] == brand]
    d["Order Date"] = pd.to_datetime(d["Order Date"])
    d["Week"] = d["Order Date"].dt.to_period("W").apply(lambda r: r.start_time)
    weeks = sorted(d["Week"].unique())
    if len(weeks) < 2:
        return pd.DataFrame()
    last_week = d[d["Week"] == weeks[-1]].groupby("SKU ID")["Final Sale Amount"].sum()
    prev_week = d[d["Week"] == weeks[-2]].groupby("SKU ID")["Final Sale Amount"].sum()
    cmp = pd.DataFrame({"Last Week": last_week, "Prev Week": prev_week}).fillna(0)
    cmp["Change %"] = ((cmp["Last Week"] - cmp["Prev Week"]) / cmp["Prev Week"].replace(0, np.nan) * 100).round(1)
    cmp = cmp[cmp["Prev Week"] > 0].sort_values("Change %")
    cmp = cmp[cmp["Change %"] < 0].head(top_n).reset_index()
    # add brand and category
    meta = df[["SKU ID", "Brand", "Category"]].drop_duplicates("SKU ID")
    cmp = cmp.merge(meta, on="SKU ID", how="left")
    return cmp

def generate_action_points(df, brand=None):
    actions = []
    d = df.copy()
    if brand and brand != "All":
        d = d[d["Brand"] == brand]
    d["Order Date"] = pd.to_datetime(d["Order Date"])

    dates = sorted(d["Order Date"].unique())
    if len(dates) >= 2:
        today_data = d[d["Order Date"] == dates[-1]]
        yest_data = d[d["Order Date"] == dates[-2]]
        today_sale = today_data["Final Sale Amount"].sum()
        yest_sale = yest_data["Final Sale Amount"].sum()
        pct = safe_pct(today_sale, yest_sale)
        if pct is not None and pct < -15:
            actions.append(f"🔴 **Sales dropped {abs(pct):.1f}% DoD** (₹{yest_sale:,.0f} → ₹{today_sale:,.0f}). Investigate top-selling SKUs for stock-out or listing issues.")
        today_cancel = today_data["Cancellation Amount"].sum()
        yest_cancel = yest_data["Cancellation Amount"].sum()
        if yest_cancel > 0:
            cp = safe_pct(today_cancel, yest_cancel)
            if cp and cp > 20:
                actions.append(f"🔴 **Cancellations spiked {cp:.1f}% DoD** (₹{yest_cancel:,.0f} → ₹{today_cancel:,.0f}). Check pricing, promise dates, and inventory availability.")
        today_ret = today_data["Return Amount"].sum()
        yest_ret = yest_data["Return Amount"].sum()
        if yest_ret > 0:
            rp = safe_pct(today_ret, yest_ret)
            if rp and rp > 20:
                actions.append(f"🟡 **Returns increased {rp:.1f}% DoD**. Review return reasons for quality or description mismatch issues.")

    # Cancel rate today
    if len(dates) >= 1:
        td = d[d["Order Date"] == dates[-1]]
        gmv = td["GMV"].sum()
        cancel = td["Cancellation Amount"].sum()
        if gmv > 0:
            cr = cancel / gmv * 100
            if cr > 15:
                actions.append(f"🔴 **High cancellation rate {cr:.1f}%** on latest day. Investigate top cancelled SKUs and seller fill rate.")

    # Declining SKUs
    dec = declining_skus(df, brand, top_n=5)
    if not dec.empty:
        sku_list = ", ".join(dec["SKU ID"].head(3).tolist())
        actions.append(f"📉 **Top declining SKUs this week:** {sku_list}. Run ads boost or price correction.")

    # Low sale with high cancellation SKUs
    sku_grp = d.groupby("SKU ID").agg(
        sale=("Final Sale Amount", "sum"),
        cancel=("Cancellation Amount", "sum")
    )
    sku_grp["cancel_rate"] = sku_grp["cancel"] / (sku_grp["sale"] + sku_grp["cancel"]).replace(0, np.nan)
    bad_skus = sku_grp[(sku_grp["cancel_rate"] > 0.3) & (sku_grp["sale"] > 1000)].index.tolist()
    if bad_skus:
        actions.append(f"⚠️ **{len(bad_skus)} SKUs with >30% cancellation rate** but significant GMV. Fix inventory or pricing: {', '.join(bad_skus[:3])}")

    # MoM if 2+ months
    months = sorted(d["Order Date"].dt.to_period("M").unique())
    if len(months) >= 2:
        m1 = d[d["Order Date"].dt.to_period("M") == months[-1]]["Final Sale Amount"].sum()
        m0 = d[d["Order Date"].dt.to_period("M") == months[-2]]["Final Sale Amount"].sum()
        mp = safe_pct(m1, m0)
        if mp and mp < -10:
            actions.append(f"📉 **MoM sales declined {abs(mp):.1f}%**. Review brand-level contribution and focus on scaling exclusives or promotions.")

    if not actions:
        actions.append("✅ All key metrics look healthy. Keep monitoring DoD and push for exclusives scale-up.")

    return actions

# ─── UI ───────────────────────────────────────────────────────────────────────
def main():
    st.markdown("""
    <style>
    .main {background-color: #0f0f1a;}
    .stApp {background-color: #0f0f1a; color: white;}
    .block-container {padding-top: 1rem;}
    h1, h2, h3 {color: #D7BDE2;}
    .stSelectbox label, .stMultiSelect label, .stDateInput label {color: #ccc !important;}
    .upload-section {background:#1a1a2e;padding:20px;border-radius:12px;margin-bottom:20px;}
    </style>
    """, unsafe_allow_html=True)

    st.title("🛒 Flipkart Sales Dashboard — One Guardian")
    st.markdown("**Brands:** Bellavita · Kenaz · Embarouge · Guzz · HipHop Skincare")
    st.markdown("---")

    # ── Sidebar ──────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("⚙️ Settings")
        spreadsheet_name = st.text_input("Google Sheet Name", value="Flipkart_Sales_DB")
        st.markdown("---")
        st.subheader("📤 Upload Daily Data")
        uploaded = st.file_uploader("Upload Flipkart Excel Report", type=["xlsx", "xls"])

        if uploaded:
            try:
                raw = pd.read_excel(uploaded)
                missing = [c for c in REQUIRED_COLS if c not in raw.columns]
                if missing:
                    st.error(f"Missing columns: {missing}")
                else:
                    raw["Order Date"] = pd.to_datetime(raw["Order Date"]).dt.strftime("%Y-%m-%d")
                    for col in ["GMV", "Cancellation Amount", "Return Amount", "Final Sale Amount",
                                "Gross Units", "Cancellation Units", "Return Units", "Final Sale Units"]:
                        raw[col] = pd.to_numeric(raw[col], errors="coerce").fillna(0)

                    st.success(f"✅ File read: {len(raw)} rows | Dates: {raw['Order Date'].min()} → {raw['Order Date'].max()}")
                    if st.button("💾 Save to Google Sheets"):
                        with st.spinner("Uploading..."):
                            client = get_gsheet_client()
                            added, dupes = append_to_gsheet(client, raw, spreadsheet_name)
                        st.success(f"Saved! {added} new rows added. {dupes} duplicates skipped.")
                        st.cache_data.clear()
            except Exception as e:
                st.error(f"Upload error: {e}")

        st.markdown("---")
        st.subheader("🔍 Filters")
        brand_filter = st.selectbox("Brand", ["All"] + list(BRAND_COLORS.keys()) + ["Bella vita organic"])
        analysis_tab = st.radio("View", ["Overview", "DoD Analysis", "WoW Analysis", "MoM Analysis", "Declining SKUs", "Action Points"])

    # ── Load Data ──────────────────────────────────────────────────────────────
    with st.spinner("Loading data from Google Sheets..."):
        try:
            client = get_gsheet_client()
            df = load_data_from_gsheet(client, spreadsheet_name)
        except Exception as e:
            st.error(f"Could not connect to Google Sheets: {e}\n\nMake sure your `secrets.toml` has `[gcp_service_account]` configured.")
            st.stop()

    if df.empty:
        st.info("No data yet. Upload your first Flipkart report using the sidebar.")
        st.stop()

    df["Order Date"] = pd.to_datetime(df["Order Date"])
    data = df[df["Brand"] == brand_filter] if brand_filter != "All" else df.copy()

    has_mom = df["Order Date"].dt.to_period("M").nunique() >= 2

    # ── Overview ──────────────────────────────────────────────────────────────
    if analysis_tab == "Overview":
        st.subheader("📊 All-Time Summary")

        dates = sorted(df["Order Date"].unique())
        col1, col2, col3, col4 = st.columns(4)

        today = dates[-1] if len(dates) >= 1 else None
        yesterday = dates[-2] if len(dates) >= 2 else None

        def get_metrics(subset, date):
            r = subset[subset["Order Date"] == date]
            return r["Final Sale Amount"].sum(), r["Cancellation Amount"].sum(), r["Return Amount"].sum()

        if today:
            ts, tc, tr = get_metrics(data, today)
            ys, yc, yr = get_metrics(data, yesterday) if yesterday else (0, 0, 0)
            with col1:
                metric_card("Today's Final Sale", ts, arrow(safe_pct(ts, ys)))
            with col2:
                metric_card("Today's Cancellation", tc, arrow(safe_pct(tc, yc), inverse=True), prefix="₹")
            with col3:
                metric_card("Today's Returns", tr, arrow(safe_pct(tr, yr), inverse=True), prefix="₹")
            with col4:
                cancel_rate = tc / (ts + tc) * 100 if (ts + tc) > 0 else 0
                metric_card("Cancel Rate Today", cancel_rate, "", prefix="")
                st.markdown("<div style='margin-top:-10px;color:#aaa;font-size:11px;text-align:center;'>%</div>", unsafe_allow_html=True)

        st.markdown("---")
        st.subheader("📈 Brand-wise Performance")

        brand_grp = df.groupby("Brand").agg(
            Final_Sale=("Final Sale Amount", "sum"),
            Cancellation=("Cancellation Amount", "sum"),
            Returns=("Return Amount", "sum"),
        ).reset_index().sort_values("Final_Sale", ascending=False)

        fig = px.bar(brand_grp, x="Brand", y=["Final_Sale", "Cancellation", "Returns"],
                     barmode="group",
                     color_discrete_map={"Final_Sale": "#6C3483", "Cancellation": "#C0392B", "Returns": "#E67E22"},
                     title="Brand-wise: Final Sale vs Cancellation vs Returns",
                     labels={"value": "Amount (₹)", "variable": "Metric"},
                     template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)

        col_a, col_b = st.columns(2)
        with col_a:
            fig2 = px.pie(brand_grp, values="Final_Sale", names="Brand",
                          title="Final Sale Share by Brand",
                          color_discrete_sequence=px.colors.sequential.Purples_r,
                          template="plotly_dark")
            st.plotly_chart(fig2, use_container_width=True)
        with col_b:
            fig3 = px.bar(brand_grp, x="Brand", y="Cancellation",
                          title="Cancellation Amount by Brand",
                          color="Cancellation", color_continuous_scale="Reds",
                          template="plotly_dark")
            st.plotly_chart(fig3, use_container_width=True)

        st.markdown("---")
        st.subheader("📆 Daily Trend (All Brands)")
        daily_all = df.groupby("Order Date").agg(
            Final_Sale=("Final Sale Amount", "sum"),
            Cancellation=("Cancellation Amount", "sum"),
            Returns=("Return Amount", "sum"),
        ).reset_index()
        fig4 = px.line(daily_all, x="Order Date", y=["Final_Sale", "Cancellation", "Returns"],
                       title="Daily Sales Trend",
                       labels={"value": "₹", "variable": "Metric"},
                       template="plotly_dark",
                       color_discrete_map={"Final_Sale": "#A569BD", "Cancellation": "#C0392B", "Returns": "#E67E22"})
        st.plotly_chart(fig4, use_container_width=True)

    # ── DoD ───────────────────────────────────────────────────────────────────
    elif analysis_tab == "DoD Analysis":
        st.subheader(f"📅 Day-on-Day Analysis — {brand_filter}")
        dod = dod_analysis(df, brand_filter)

        fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                            subplot_titles=("Final Sale Amount (₹)", "Cancellation Amount (₹)", "Return Amount (₹)"),
                            vertical_spacing=0.08)
        fig.add_trace(go.Bar(x=dod["Order Date"], y=dod["Final_Sale"], name="Final Sale", marker_color="#6C3483"), row=1, col=1)
        fig.add_trace(go.Bar(x=dod["Order Date"], y=dod["Cancellation"], name="Cancellation", marker_color="#C0392B"), row=2, col=1)
        fig.add_trace(go.Bar(x=dod["Order Date"], y=dod["Returns"], name="Returns", marker_color="#E67E22"), row=3, col=1)
        fig.update_layout(template="plotly_dark", height=650, showlegend=False, title="DoD: Sale, Cancellation & Return")
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("DoD % Change")
        disp = dod[["Order Date", "Final_Sale", "DoD_Sale_%", "Cancellation", "DoD_Cancel_%", "Returns", "DoD_Return_%"]].copy()
        disp["Order Date"] = disp["Order Date"].dt.strftime("%d %b %Y")
        disp = disp.rename(columns={
            "Final_Sale": "Final Sale (₹)", "DoD_Sale_%": "DoD Sale %",
            "Cancellation": "Cancel (₹)", "DoD_Cancel_%": "DoD Cancel %",
            "Returns": "Returns (₹)", "DoD_Return_%": "DoD Return %"
        })
        st.dataframe(
            disp.style.format({
                "Final Sale (₹)": "₹{:,.0f}", "DoD Sale %": "{:.1f}%",
                "Cancel (₹)": "₹{:,.0f}", "DoD Cancel %": "{:.1f}%",
                "Returns (₹)": "₹{:,.0f}", "DoD Return %": "{:.1f}%",
            }).background_gradient(subset=["DoD Sale %"], cmap="RdYlGn"),
            use_container_width=True, hide_index=True
        )

    # ── WoW ───────────────────────────────────────────────────────────────────
    elif analysis_tab == "WoW Analysis":
        st.subheader(f"📅 Week-on-Week Analysis — {brand_filter}")
        wow = wow_analysis(df, brand_filter)

        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            subplot_titles=("Weekly Final Sale (₹)", "Weekly Cancellation (₹)"),
                            vertical_spacing=0.12)
        fig.add_trace(go.Bar(x=wow["Week"].astype(str), y=wow["Final_Sale"], name="Final Sale", marker_color="#6C3483"), row=1, col=1)
        fig.add_trace(go.Bar(x=wow["Week"].astype(str), y=wow["Cancellation"], name="Cancellation", marker_color="#C0392B"), row=2, col=1)
        fig.update_layout(template="plotly_dark", height=550, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

        disp = wow.copy()
        disp["Week"] = disp["Week"].dt.strftime("W/C %d %b %Y")
        disp = disp.rename(columns={
            "Final_Sale": "Final Sale (₹)", "WoW_Sale_%": "WoW Sale %",
            "Cancellation": "Cancel (₹)", "WoW_Cancel_%": "WoW Cancel %",
            "Returns": "Returns (₹)", "WoW_Return_%": "WoW Return %"
        })
        st.dataframe(
            disp.style.format({
                "Final Sale (₹)": "₹{:,.0f}", "WoW Sale %": "{:.1f}%",
                "Cancel (₹)": "₹{:,.0f}", "WoW Cancel %": "{:.1f}%",
                "Returns (₹)": "₹{:,.0f}", "WoW Return %": "{:.1f}%",
            }).background_gradient(subset=["WoW Sale %"], cmap="RdYlGn"),
            use_container_width=True, hide_index=True
        )

    # ── MoM ───────────────────────────────────────────────────────────────────
    elif analysis_tab == "MoM Analysis":
        if not has_mom:
            st.info("📊 MoM analysis requires at least 2 months of data. Upload more historical data via the sidebar.")
        else:
            st.subheader(f"📅 Month-on-Month Analysis — {brand_filter}")
            mom = mom_analysis(df, brand_filter)

            fig = px.bar(mom, x=mom["Month"].dt.strftime("%b %Y"), y=["Final_Sale", "Cancellation", "Returns"],
                         barmode="group",
                         title="MoM: Final Sale vs Cancellation vs Returns",
                         labels={"value": "Amount (₹)", "variable": "Metric"},
                         color_discrete_map={"Final_Sale": "#6C3483", "Cancellation": "#C0392B", "Returns": "#E67E22"},
                         template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

            disp = mom.copy()
            disp["Month"] = disp["Month"].dt.strftime("%b %Y")
            disp = disp.rename(columns={
                "Final_Sale": "Final Sale (₹)", "MoM_Sale_%": "MoM Sale %",
                "Cancellation": "Cancel (₹)", "MoM_Cancel_%": "MoM Cancel %",
                "Returns": "Returns (₹)", "MoM_Return_%": "MoM Return %"
            })
            st.dataframe(
                disp.style.format({
                    "Final Sale (₹)": "₹{:,.0f}", "MoM Sale %": "{:.1f}%",
                    "Cancel (₹)": "₹{:,.0f}", "MoM Cancel %": "{:.1f}%",
                    "Returns (₹)": "₹{:,.0f}", "MoM Return %": "{:.1f}%",
                }).background_gradient(subset=["MoM Sale %"], cmap="RdYlGn"),
                use_container_width=True, hide_index=True
            )

    # ── Declining SKUs ────────────────────────────────────────────────────────
    elif analysis_tab == "Declining SKUs":
        st.subheader(f"📉 Declining SKUs (WoW) — {brand_filter}")
        dec = declining_skus(df, brand_filter)
        if dec.empty:
            st.info("Not enough weekly data to compute SKU-level decline. Upload at least 2 weeks of data.")
        else:
            fig = px.bar(dec.head(15), x="SKU ID", y="Change %",
                         color="Brand", title="Top Declining SKUs (WoW %)",
                         template="plotly_dark",
                         labels={"Change %": "WoW Change %"},
                         color_discrete_map=BRAND_COLORS)
            fig.update_traces(marker_line_width=0)
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(
                dec[["SKU ID", "Brand", "Category", "Prev Week", "Last Week", "Change %"]].style.format({
                    "Prev Week": "₹{:,.0f}", "Last Week": "₹{:,.0f}", "Change %": "{:.1f}%"
                }).background_gradient(subset=["Change %"], cmap="RdYlGn"),
                use_container_width=True, hide_index=True
            )

    # ── Action Points ─────────────────────────────────────────────────────────
    elif analysis_tab == "Action Points":
        st.subheader(f"🎯 Dynamic Action Points — {brand_filter}")
        st.caption("Auto-generated based on your uploaded data. Updated every time new data is added.")
        actions = generate_action_points(df, brand_filter)
        for i, a in enumerate(actions, 1):
            st.markdown(f"**{i}.** {a}")

        st.markdown("---")
        st.subheader("📋 Brand-wise Quick Snapshot")
        brands = df["Brand"].unique()
        for b in brands:
            bdf = df[df["Brand"] == b]
            dates = sorted(bdf["Order Date"].unique())
            if len(dates) >= 1:
                td = bdf[bdf["Order Date"] == dates[-1]]
                sale = td["Final Sale Amount"].sum()
                cancel = td["Cancellation Amount"].sum()
                ret = td["Return Amount"].sum()
                with st.expander(f"**{b}** — Latest Day: ₹{sale:,.0f} sale | ₹{cancel:,.0f} cancel | ₹{ret:,.0f} returns"):
                    ba = generate_action_points(bdf, b)
                    for a in ba:
                        st.markdown(f"• {a}")

if __name__ == "__main__":
    main()
