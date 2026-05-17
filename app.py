"""
sku_master_section.py
─────────────────────
Drop this file next to your main app.py.

Integration (3 lines in app.py):
─────────────────────────────────
  1. At the top of app.py, after your existing imports:
        from sku_master_section import render_sku_sidebar, render_sku_section

  2. Inside `with st.sidebar:`, after the first `st.markdown("---")`:
        render_sku_sidebar()

  3. Inside main(), after the Exclusives block:
        render_sku_section()

  4. Add to your nav_html loop (optional):
        ("📦 SKU Master & Inventory", "sku_master"),
"""

import numpy as np
import sys

# Module-level client function — set via set_client_fn() from app.py
_CLIENT_FN = None

def set_client_fn(fn):
    global _CLIENT_FN
    _CLIENT_FN = fn
import pandas as pd
import streamlit as st
import plotly.express as px

# ── Constants ─────────────────────────────────────────────────────────────────
MASTER_GSHEET    = "Flipkart_SKU_Master_DB"
INVENTORY_GSHEET = "Flipkart_Inventory_DB"
LISTING_GSHEET   = "Flipkart_Listing_DB"

BELLAVITA_ALL = [
    "BELLAVITA", "Bella vita organic", "Bellavita", "bella vita",
    "BELLA VITA ORGANIC", "bellavita", "Bella Vita Organic",
]

PIE_COLORS = [
    "#9B59B6","#2ECC71","#E74C3C","#3498DB","#F39C12",
    "#1ABC9C","#E91E63","#FF5722","#00BCD4","#8BC34A",
    "#FF9800","#673AB7",
]

BRAND_COLORS = {
    "Bellavita": "#9B59B6", "Kenaz": "#3498DB",
    "Embarouge": "#E74C3C", "HipHop Skincare": "#2ECC71", "Guzz": "#F39C12",
}

HEALTH_COLORS = {
    "🔴 OOS":          "#e74c3c",
    "🟡 Low (<7d)":    "#f39c12",
    "🟠 Medium (7-14d)":"#e67e22",
    "🟢 Healthy":      "#2ecc71",
}


# ── Shared helpers (re-declared locally so the module is self-contained) ──────
def _indian_fmt(n):
    try:
        n = int(round(float(n)))
        s = str(abs(n))
        if len(s) <= 3:
            return ("-" if n < 0 else "") + s
        last3 = s[-3:]
        rest = s[:-3]
        groups = []
        while len(rest) > 2:
            groups.append(rest[-2:])
            rest = rest[:-2]
        if rest:
            groups.append(rest)
        result = ",".join(reversed(groups)) + "," + last3
        return ("-" if n < 0 else "") + result
    except Exception:
        return str(n)


def _metric_card(label, val, prefix="", suffix=""):
    try:
        vs = f"{prefix}{_indian_fmt(val)}{suffix}"
    except Exception:
        vs = str(val)
    st.markdown(
        f"""<div style='background:linear-gradient(135deg,#13132a,#1a1a35);
            padding:16px 18px;border-radius:14px;border:1px solid #2a2a4a;
            border-left:4px solid #6C3483;margin-bottom:8px;
            box-shadow:0 4px 20px rgba(0,0,0,0.3)'>
            <div style='color:#8888aa;font-size:11px;font-weight:500;
                letter-spacing:0.5px;text-transform:uppercase;margin-bottom:5px'>{label}</div>
            <div style='color:#fff;font-size:20px;font-weight:800'>{vs}</div>
        </div>""",
        unsafe_allow_html=True,
    )


def _render_table(df, fmt=None, pct_cols=None):
    fmt = fmt or {}
    pct_cols = pct_cols or []
    new_fmt = {}
    for col, f in fmt.items():
        if "₹" in str(f):
            new_fmt[col] = lambda v, _f=f: (
                "₹" + _indian_fmt(v) if pd.notna(v) and v != "" else "—"
            )
        else:
            new_fmt[col] = f
    styled = df.style.format(new_fmt, na_rep="—")
    for col in pct_cols:
        if col in df.columns:
            fn = getattr(styled, "map", getattr(styled, "applymap", None))
            def _color(v):
                try:
                    fv = float(str(v).replace("%", ""))
                    if fv > 0:
                        return "color:#2ecc71;font-weight:600"
                    if fv < 0:
                        return "color:#e74c3c;font-weight:600"
                except Exception:
                    pass
                return ""
            styled = fn(_color, subset=[col])
    st.dataframe(styled, use_container_width=True, hide_index=True)


# ── GSheet helpers ─────────────────────────────────────────────────────────────
def _get_client():
    """Use the client function registered via set_client_fn()."""
    if _CLIENT_FN is None:
        raise RuntimeError("Call set_client_fn(get_gsheet_client) in app.py after importing this module.")
    return _CLIENT_FN()


def _get_or_create(client, name):
    import gspread
    try:
        return client.open(name)
    except gspread.SpreadsheetNotFound:
        sh = client.create(name)
        import streamlit as st
        sh.share(st.secrets["gcp_service_account"]["client_email"],
                 perm_type="user", role="writer")
        return sh


def _load_sheet(client, name):
    try:
        ws = _get_or_create(client, name).sheet1
        data = ws.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"[{name}] load error: {e}")
        return pd.DataFrame()


def _overwrite_sheet(client, df, name):
    ws = _get_or_create(client, name).sheet1
    df = df.copy().fillna("").astype(str)
    ws.clear()
    ws.update([df.columns.tolist()] + df.values.tolist())
    return len(df)


def _upsert_master(client, new_df, name):
    ws = _get_or_create(client, name).sheet1
    existing = ws.get_all_records()
    new_df = _norm_brands(new_df.copy()).fillna("").astype(str)
    if not existing:
        ws.update([new_df.columns.tolist()] + new_df.values.tolist())
        return len(new_df), 0
    ex = pd.DataFrame(existing)
    ex_fsns = set(ex["FSN"].astype(str))
    truly_new = new_df[~new_df["FSN"].astype(str).isin(ex_fsns)]
    combined = pd.concat([ex, truly_new], ignore_index=True).fillna("").astype(str)
    ws.clear()
    ws.update([combined.columns.tolist()] + combined.values.tolist())
    return len(truly_new), len(new_df) - len(truly_new)


# ── Data helpers ───────────────────────────────────────────────────────────────
def _norm_brands(df):
    df = df.copy()
    df["Brand"] = df["Brand"].astype(str).str.strip().apply(
        lambda x: "Bellavita" if x in BELLAVITA_ALL else x
    )
    return df


def _agg_inventory(df_inv):
    num_cols = [
        "Live on Website", "Sales 7D", "Sales 14D", "Sales 30D",
        "Sales 60D", "Sales 90D", "Flipkart Selling Price",
        "Orders to Dispatch", "Damaged", "Returns Processing",
    ]
    for c in num_cols:
        if c in df_inv.columns:
            df_inv[c] = pd.to_numeric(df_inv[c], errors="coerce").fillna(0)

    return df_inv.groupby("FSN").agg(
        Sellable_Stock=("Live on Website", "sum"),
        Sales_7D=("Sales 7D", "sum"),
        Sales_14D=("Sales 14D", "sum"),
        Sales_30D=("Sales 30D", "sum"),
        Inv_Price=("Flipkart Selling Price", "first"),
        Orders_Pending=("Orders to Dispatch", "sum"),
        Damaged=("Damaged", "sum"),
        Returns_Processing=("Returns Processing", "sum"),
        Warehouses=("Warehouse Id", "nunique"),
        Inv_Fulfillment=("Fulfilment Type",
                         lambda x: " | ".join(x.dropna().astype(str).unique())),
        F_Assured=("F Assured Badge", "first"),
    ).reset_index()


def _prep_listing(df_l):
    if df_l.empty:
        return pd.DataFrame()
    df_l = df_l.copy()
    # Handle both raw upload (verbose col names) and GSheet reload (short names)
    rename_map = {
        "Flipkart Serial Number": "FSN",
        "Listing Status": "Listing_Status",
        "Inactive Reason": "Inactive_Reason",
        "Your Selling Price": "Listed_Price",
        "System Stock count": "System_Stock",
        "Fulfillment By": "Fulfillment_By",
        "MRP": "MRP",
    }
    df_l = df_l.rename(columns={k: v for k, v in rename_map.items() if k in df_l.columns})
    keep = [c for c in ["FSN", "Listing_Status", "Inactive_Reason",
                         "Listed_Price", "System_Stock", "Fulfillment_By", "MRP"]
            if c in df_l.columns]
    df_l = df_l[keep].copy()
    for c in ["Listed_Price", "System_Stock", "MRP"]:
        if c in df_l.columns:
            df_l[c] = pd.to_numeric(df_l[c], errors="coerce").fillna(0)
    df_l["FSN"] = df_l["FSN"].astype(str)
    return df_l.drop_duplicates("FSN")


def _add_health(merged):
    if "Sellable_Stock" not in merged.columns:
        return merged
    merged["Days_Cover"] = (
        merged["Sellable_Stock"] / merged["Sales_7D"].replace(0, np.nan) * 7
    ).round(0)

    def _h(r):
        s = r["Sellable_Stock"]
        d = r["Days_Cover"]
        if pd.isna(s) or s <= 0:
            return "🔴 OOS"
        if pd.notna(d) and d < 7:
            return "🟡 Low (<7d)"
        if pd.notna(d) and d < 14:
            return "🟠 Medium (7-14d)"
        return "🟢 Healthy"

    merged["Stock_Health"] = merged.apply(_h, axis=1)
    return merged


@st.cache_data(ttl=300)
def _build_merged(_master_name, _inv_name, _listing_name):
    client = _get_client()
    df_m = _load_sheet(client, _master_name)
    df_i = _load_sheet(client, _inv_name)
    df_l = _load_sheet(client, _listing_name)

    if df_m.empty:
        return pd.DataFrame()

    df_m.columns = df_m.columns.str.strip()
    df_m = df_m.loc[:, ~df_m.columns.str.startswith("Unnamed")]
    df_m = _norm_brands(df_m)
    df_m["FSN"] = df_m["FSN"].astype(str)

    merged = df_m.copy()

    if not df_i.empty:
        inv_agg = _agg_inventory(df_i)
        inv_agg["FSN"] = inv_agg["FSN"].astype(str)
        merged = merged.merge(inv_agg, on="FSN", how="left")

    lst = _prep_listing(df_l)
    if not lst.empty:
        merged = merged.merge(lst, on="FSN", how="left")

    merged = _add_health(merged)
    return merged


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC — SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
def render_sku_sidebar():
    """Call this inside `with st.sidebar:` in app.py."""
    st.markdown("---")
    st.markdown("### 📦 SKU Master & Inventory")

    # ── SKU Master ────────────────────────────────────────────────────────────
    master_up = st.file_uploader(
        "SKU Master (.xlsx / .csv)", type=["xlsx", "xls", "csv"], key="sb_master"
    )
    if master_up:
        try:
            raw_m = (pd.read_csv(master_up) if master_up.name.endswith(".csv")
                     else pd.read_excel(master_up))
            raw_m.columns = raw_m.columns.str.strip()
            raw_m = raw_m.loc[:, ~raw_m.columns.str.startswith("Unnamed")]
            st.info(f"📋 {len(raw_m):,} FSNs ready")
            if st.button("💾 Save SKU Master", key="sb_save_master"):
                with st.spinner("Saving…"):
                    cli = _get_client()
                    added, dupes = _upsert_master(cli, raw_m, MASTER_GSHEET)
                st.success(f"✅ {added} new | {dupes} existing skipped")
                st.cache_data.clear()
        except Exception as e:
            st.error(f"Master error: {e}")

    # ── Inventory ─────────────────────────────────────────────────────────────
    inv_up = st.file_uploader(
        "Current Inventory (.csv / .xlsx)", type=["csv", "xlsx", "xls"], key="sb_inv"
    )
    if inv_up:
        try:
            raw_inv = (pd.read_csv(inv_up) if inv_up.name.endswith(".csv")
                       else pd.read_excel(inv_up))
            raw_inv.columns = raw_inv.columns.str.strip()
            st.info(f"📦 {len(raw_inv):,} rows ready")
            if st.button("💾 Save Inventory (replaces snapshot)", key="sb_save_inv"):
                with st.spinner("Saving…"):
                    cli = _get_client()
                    n = _overwrite_sheet(cli, raw_inv, INVENTORY_GSHEET)
                st.success(f"✅ {n:,} rows saved")
                st.cache_data.clear()
        except Exception as e:
            st.error(f"Inventory error: {e}")

    # ── Listing ───────────────────────────────────────────────────────────────
    lst_up = st.file_uploader(
        "Listing Report (.xls / .xlsx / .csv)", type=["xls", "xlsx", "csv"], key="sb_lst"
    )
    if lst_up:
        try:
            if lst_up.name.endswith(".csv"):
                raw_lst = pd.read_csv(lst_up)
            elif lst_up.name.endswith(".xls"):
                raw_lst = pd.read_excel(lst_up, engine="xlrd")
                # Flipkart puts a description row at index 0 — drop it
                if str(raw_lst.iloc[0].get("Flipkart Serial Number", "")).startswith("Flipkart"):
                    raw_lst = raw_lst.iloc[1:].reset_index(drop=True)
            else:
                raw_lst = pd.read_excel(lst_up)
            raw_lst.columns = raw_lst.columns.str.strip()
            st.info(f"🏷️ {len(raw_lst):,} listings ready")
            if st.button("💾 Save Listing (replaces snapshot)", key="sb_save_lst"):
                with st.spinner("Saving…"):
                    cli = _get_client()
                    n = _overwrite_sheet(cli, raw_lst, LISTING_GSHEET)
                st.success(f"✅ {n:,} listings saved")
                st.cache_data.clear()
        except Exception as e:
            st.error(f"Listing error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC — MAIN SECTION
# ══════════════════════════════════════════════════════════════════════════════
def render_sku_section():
    """Call this inside main() in app.py, after the Exclusives block."""

    st.markdown("<div id='sku_master'></div>", unsafe_allow_html=True)
    st.markdown(
        "<div style='margin-top:40px;margin-bottom:20px'>"
        "<h2 style='color:#D7BDE2;font-size:22px;font-weight:700;margin:0;"
        "padding-bottom:10px;border-bottom:2px solid;"
        "border-image:linear-gradient(90deg,#6C3483,#2E86C1) 1;"
        "letter-spacing:-0.3px'>📦 SKU Master & Inventory</h2></div>",
        unsafe_allow_html=True,
    )

    dm = _build_merged(MASTER_GSHEET, INVENTORY_GSHEET, LISTING_GSHEET)

    if dm.empty:
        st.info("Upload SKU Master via the sidebar to activate this section.")
        return

    has_inv = "Sellable_Stock" in dm.columns
    has_lst = "Listing_Status" in dm.columns

    # ── Global filters ─────────────────────────────────────────────────────────
    gf1, gf2, gf3, gf4 = st.columns(4)
    with gf1:
        sm_brand = st.selectbox(
            "Brand", ["All"] + sorted(dm["Brand"].dropna().unique().tolist()), key="sm_brand"
        )
    with gf2:
        sm_cat = st.selectbox(
            "Category", ["All"] + sorted(dm["Category"].dropna().unique().tolist()), key="sm_cat"
        )
    with gf3:
        sm_range = st.selectbox(
            "Range", ["All"] + sorted(dm["Range"].dropna().unique().tolist()), key="sm_range"
        )
    with gf4:
        sm_status = st.selectbox(
            "Status", ["All", "ACTIVE", "INACTIVE"], key="sm_status"
        )

    if sm_brand  != "All": dm = dm[dm["Brand"]  == sm_brand]
    if sm_cat    != "All": dm = dm[dm["Category"] == sm_cat]
    if sm_range  != "All": dm = dm[dm["Range"]   == sm_range]
    if sm_status != "All": dm = dm[dm["Active/Discontinued"] == sm_status]

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tab_cat, tab_range, tab_inv, tab_full = st.tabs([
        "🗂️ Category View",
        "🎯 Range View",
        "📦 Inventory Health",
        "📋 Full SKU Table",
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — CATEGORY VIEW
    # ══════════════════════════════════════════════════════════════════════════
    with tab_cat:
        ck1, ck2, ck3, ck4 = st.columns(4)
        with ck1: _metric_card("Total FSNs", len(dm))
        with ck2: _metric_card("Active", (dm["Active/Discontinued"] == "ACTIVE").sum())
        with ck3: _metric_card("Categories", dm["Category"].nunique())
        with ck4: _metric_card("Brands", dm["Brand"].nunique())

        ca1, ca2 = st.columns(2)
        with ca1:
            cb_cnt = (
                dm.groupby(["Category", "Brand"]).size()
                  .reset_index(name="FSN Count")
            )
            st.plotly_chart(
                px.bar(cb_cnt, x="Category", y="FSN Count", color="Brand",
                       template="plotly_dark",
                       title="FSN Count — Category × Brand",
                       barmode="stack",
                       color_discrete_map=BRAND_COLORS),
                use_container_width=True,
            )
        with ca2:
            cat_tot = dm.groupby("Category").size().reset_index(name="FSN Count")
            st.plotly_chart(
                px.pie(cat_tot, values="FSN Count", names="Category",
                       title="Category Share", template="plotly_dark",
                       color_discrete_sequence=PIE_COLORS, hole=0.4),
                use_container_width=True,
            )

        # Sub-category bar
        top_sub = (
            dm.groupby(["Sub-category", "Category"]).size()
              .reset_index(name="FSNs")
              .sort_values("FSNs", ascending=False).head(20)
        )
        fig_sub = px.bar(
            top_sub, x="Sub-category", y="FSNs", color="Category",
            template="plotly_dark",
            title="Top 20 Sub-categories by FSN Count",
            color_discrete_sequence=PIE_COLORS,
        )
        fig_sub.update_xaxes(tickangle=45)
        st.plotly_chart(fig_sub, use_container_width=True)

        # Category → Sub-category table
        cat_sub = (
            dm.groupby(["Category", "Sub-category"]).size()
              .reset_index(name="FSNs")
              .sort_values(["Category", "FSNs"], ascending=[True, False])
        )
        if has_inv:
            inv_sub = dm.groupby(["Category", "Sub-category"]).agg(
                OOS=("Stock_Health", lambda x: (x == "🔴 OOS").sum()),
                Sellable=("Sellable_Stock", "sum"),
                Sales_30D=("Sales_30D", "sum"),
            ).reset_index()
            cat_sub = cat_sub.merge(inv_sub, on=["Category", "Sub-category"], how="left")

        fmt_cs = {"FSNs": "{:,.0f}"}
        if has_inv:
            fmt_cs.update({"OOS": "{:,.0f}", "Sellable": "{:,.0f}", "Sales_30D": "{:,.0f}"})
        _render_table(cat_sub.reset_index(drop=True), fmt_cs)

        # Active vs Inactive
        act_cat = dm.groupby(["Category", "Active/Discontinued"]).size().reset_index(name="Count")
        st.plotly_chart(
            px.bar(act_cat, x="Category", y="Count",
                   color="Active/Discontinued",
                   color_discrete_map={"ACTIVE": "#2ecc71", "INACTIVE": "#e74c3c"},
                   template="plotly_dark",
                   title="Active vs Inactive — by Category",
                   barmode="group"),
            use_container_width=True,
        )

        # NPD / EPD / Exclusive
        if "NPD /EPD/ Exclusive" in dm.columns:
            npd = dm.groupby(["Category", "NPD /EPD/ Exclusive"]).size().reset_index(name="FSNs")
            st.plotly_chart(
                px.bar(npd, x="Category", y="FSNs",
                       color="NPD /EPD/ Exclusive",
                       template="plotly_dark",
                       title="EPD vs Exclusives — by Category",
                       barmode="stack",
                       color_discrete_sequence=PIE_COLORS),
                use_container_width=True,
            )

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — RANGE VIEW
    # ══════════════════════════════════════════════════════════════════════════
    with tab_range:
        rk1, rk2, rk3 = st.columns(3)
        with rk1: _metric_card("Ranges", dm["Range"].nunique())
        with rk2: _metric_card("Avg SKUs / Range", round(len(dm) / max(dm["Range"].nunique(), 1), 1))
        with rk3:
            if has_inv:
                _metric_card("Ranges with OOS",
                             dm[dm["Stock_Health"] == "🔴 OOS"]["Range"].nunique())
            else:
                _metric_card("Total SKUs", len(dm))

        range_total = (
            dm.groupby("Range").size().reset_index(name="FSNs")
              .sort_values("FSNs", ascending=False)
        )
        top_rng = range_total["Range"].head(25).tolist()

        ra1, ra2 = st.columns(2)
        with ra1:
            fig_range = px.bar(
                range_total.head(25), x="Range", y="FSNs",
                color="FSNs",
                color_continuous_scale=["#2a1a4a", "#9B59B6"],
                template="plotly_dark",
                title="Top 25 Ranges by FSN Count",
            )
            fig_range.update_xaxes(tickangle=45)
            fig_range.update_layout(showlegend=False)
            st.plotly_chart(fig_range, use_container_width=True)

        with ra2:
            rng_cat = dm.groupby(["Range", "Category"]).size().reset_index(name="FSNs")
            rng_pivot = (
                rng_cat.pivot(index="Range", columns="Category", values="FSNs")
                       .fillna(0)
            )
            rng_pivot = rng_pivot.loc[[r for r in top_rng if r in rng_pivot.index]].head(20)
            fig_heat = px.imshow(
                rng_pivot,
                color_continuous_scale=["#0a0a14", "#2a1a4a", "#6C3483", "#9B59B6", "#D7BDE2"],
                template="plotly_dark",
                title="Range × Category Heatmap (FSN count)",
                aspect="auto",
            )
            ann = []
            for i, rng in enumerate(rng_pivot.index):
                for j, cat in enumerate(rng_pivot.columns):
                    v = int(rng_pivot.loc[rng, cat])
                    if v > 0:
                        ann.append(dict(x=j, y=i, text=str(v),
                                        showarrow=False,
                                        font=dict(size=9, color="white")))
            fig_heat.update_layout(annotations=ann,
                                   height=max(300, len(rng_pivot) * 28))
            st.plotly_chart(fig_heat, use_container_width=True)

        # Range × Size/Qty
        rng_size = dm.groupby(["Range", "Size/Qty"]).size().reset_index(name="FSNs")
        rng_size = rng_size[rng_size["Range"].isin(top_rng)]
        fig_rsize = px.bar(
            rng_size, x="Range", y="FSNs", color="Size/Qty",
            template="plotly_dark",
            title="Range — Size/Qty split (Top 25 Ranges)",
            barmode="stack",
            color_discrete_sequence=PIE_COLORS,
        )
        fig_rsize.update_xaxes(tickangle=45)
        st.plotly_chart(fig_rsize, use_container_width=True)

        # Range × Brand
        rng_brand = dm.groupby(["Range", "Brand"]).size().reset_index(name="FSNs")
        rng_brand = rng_brand[rng_brand["Range"].isin(top_rng)]
        fig_rb = px.bar(
            rng_brand, x="Range", y="FSNs", color="Brand",
            template="plotly_dark",
            title="Range — Brand split (Top 25 Ranges)",
            barmode="stack",
            color_discrete_map=BRAND_COLORS,
        )
        fig_rb.update_xaxes(tickangle=45)
        st.plotly_chart(fig_rb, use_container_width=True)

        # Full range table
        range_agg = dm.groupby(["Range", "Brand", "Category"]).agg(
            FSNs=("FSN", "count"),
            Sizes=("Size/Qty", "nunique"),
        ).reset_index()
        if has_inv:
            range_inv = dm.groupby(["Range", "Brand", "Category"]).agg(
                Sellable=("Sellable_Stock", "sum"),
                Sales_7D=("Sales_7D", "sum"),
                Sales_30D=("Sales_30D", "sum"),
                OOS=("Stock_Health", lambda x: (x == "🔴 OOS").sum()),
            ).reset_index()
            range_agg = range_agg.merge(range_inv, on=["Range", "Brand", "Category"], how="left")

        rng_search = st.text_input("🔍 Filter Range table", key="rng_search")
        ra_show = range_agg.copy()
        if rng_search:
            mask = ra_show.apply(
                lambda r: rng_search.lower() in " ".join(r.astype(str).values).lower(), axis=1
            )
            ra_show = ra_show[mask]

        fmt_range = {"FSNs": "{:,.0f}", "Sizes": "{:,.0f}"}
        if has_inv:
            fmt_range.update({
                "Sellable": "{:,.0f}", "Sales_7D": "{:,.0f}",
                "Sales_30D": "{:,.0f}", "OOS": "{:,.0f}",
            })
        _render_table(ra_show.sort_values("FSNs", ascending=False).reset_index(drop=True), fmt_range)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — INVENTORY HEALTH
    # ══════════════════════════════════════════════════════════════════════════
    with tab_inv:
        if not has_inv:
            st.info("Upload Current Inventory via the sidebar to see stock health.")
        else:
            ik1, ik2, ik3, ik4, ik5 = st.columns(5)
            with ik1: _metric_card("Total FSNs", len(dm))
            with ik2: _metric_card("🔴 OOS", (dm["Stock_Health"] == "🔴 OOS").sum())
            with ik3: _metric_card("🟡 Low (<7d)", (dm["Stock_Health"] == "🟡 Low (<7d)").sum())
            with ik4: _metric_card("Sellable Units", int(dm["Sellable_Stock"].fillna(0).sum()), suffix=" u")
            with ik5: _metric_card("Sales 7D", int(dm["Sales_7D"].fillna(0).sum()), suffix=" u")

            ia1, ia2 = st.columns(2)
            with ia1:
                hcnt = dm["Stock_Health"].value_counts().reset_index()
                hcnt.columns = ["Status", "Count"]
                st.plotly_chart(
                    px.pie(hcnt, values="Count", names="Status",
                           title="Stock Health Distribution",
                           template="plotly_dark",
                           color="Status",
                           color_discrete_map=HEALTH_COLORS,
                           hole=0.45),
                    use_container_width=True,
                )
            with ia2:
                bh = dm.groupby(["Brand", "Stock_Health"]).size().reset_index(name="FSNs")
                st.plotly_chart(
                    px.bar(bh, x="Brand", y="FSNs", color="Stock_Health",
                           template="plotly_dark",
                           title="Stock Health by Brand",
                           barmode="stack",
                           color_discrete_map=HEALTH_COLORS),
                    use_container_width=True,
                )

            # Category × health
            ch_grp = dm.groupby(["Category", "Stock_Health"]).size().reset_index(name="FSNs")
            st.plotly_chart(
                px.bar(ch_grp, x="Category", y="FSNs", color="Stock_Health",
                       barmode="stack", template="plotly_dark",
                       title="Stock Health by Category",
                       color_discrete_map=HEALTH_COLORS),
                use_container_width=True,
            )

            # Range OOS exposure
            rng_oos = (
                dm[dm["Stock_Health"] == "🔴 OOS"]
                .groupby("Range").size().reset_index(name="OOS FSNs")
                .sort_values("OOS FSNs", ascending=False).head(20)
            )
            if not rng_oos.empty:
                fig_roos = px.bar(
                    rng_oos, x="Range", y="OOS FSNs",
                    color="OOS FSNs",
                    color_continuous_scale=["#441a1a", "#e74c3c"],
                    template="plotly_dark",
                    title="Top Ranges with OOS FSNs",
                )
                fig_roos.update_xaxes(tickangle=45)
                st.plotly_chart(fig_roos, use_container_width=True)

            # OOS table — sorted by 30D sales (act on high-velocity OOS first)
            with st.expander("🔴 OOS FSNs — sorted by 30D sales velocity"):
                oos_df = dm[dm["Stock_Health"] == "🔴 OOS"].sort_values(
                    "Sales_30D", ascending=False
                )
                oos_cols = [c for c in [
                    "FSN", "Brand", "Category", "Range", "Size/Qty",
                    "Sellable_Stock", "Sales_7D", "Sales_30D",
                    "Orders_Pending", "Listing_Status", "Active/Discontinued",
                ] if c in oos_df.columns]
                _render_table(
                    oos_df[oos_cols].reset_index(drop=True),
                    {"Sellable_Stock": "{:,.0f}", "Sales_7D": "{:,.0f}",
                     "Sales_30D": "{:,.0f}", "Orders_Pending": "{:,.0f}"},
                )

            # Low stock table
            with st.expander("🟡 Low Stock FSNs (<7 days cover)"):
                low_df = dm[dm["Stock_Health"] == "🟡 Low (<7d)"].sort_values("Days_Cover")
                low_cols = [c for c in [
                    "FSN", "Brand", "Category", "Range", "Size/Qty",
                    "Sellable_Stock", "Days_Cover", "Sales_7D", "Sales_30D",
                    "Inv_Fulfillment", "Active/Discontinued",
                ] if c in low_df.columns]
                _render_table(
                    low_df[low_cols].reset_index(drop=True),
                    {"Sellable_Stock": "{:,.0f}", "Days_Cover": "{:.0f}",
                     "Sales_7D": "{:,.0f}", "Sales_30D": "{:,.0f}"},
                )

            # Velocity scatter
            sc_df = dm[dm["Sales_7D"].fillna(0) > 0].copy()
            if not sc_df.empty:
                st.plotly_chart(
                    px.scatter(
                        sc_df.head(300),
                        x="Sellable_Stock", y="Sales_7D",
                        color="Stock_Health", size="Sales_30D",
                        hover_data=["FSN", "Brand", "Range", "Category"],
                        template="plotly_dark",
                        title="Sellable Stock vs 7D Velocity",
                        color_discrete_map=HEALTH_COLORS,
                        labels={"Sellable_Stock": "Sellable (units)",
                                "Sales_7D": "Sales 7D (units)"},
                    ),
                    use_container_width=True,
                )

            # OOS but still ACTIVE listing — mismatch alert
            if has_lst:
                mismatch = dm[
                    (dm["Stock_Health"] == "🔴 OOS") &
                    (dm["Listing_Status"].astype(str).str.upper() == "ACTIVE")
                ]
                if not mismatch.empty:
                    st.warning(
                        f"⚠️ {len(mismatch)} FSNs are OOS in inventory but "
                        f"still **ACTIVE** on listing — check pricing / stock."
                    )
                    m_cols = [c for c in [
                        "FSN", "Brand", "Range", "Category",
                        "Sellable_Stock", "Listing_Status", "Inactive_Reason",
                    ] if c in mismatch.columns]
                    _render_table(mismatch[m_cols].reset_index(drop=True),
                                  {"Sellable_Stock": "{:,.0f}"})
                else:
                    st.success("✅ No OOS + Active listing mismatches.")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4 — FULL SKU TABLE
    # ══════════════════════════════════════════════════════════════════════════
    with tab_full:
        all_cols = [c for c in [
            "FSN", "Title", "Brand", "Category", "Sub-category",
            "Range", "Size/Qty", "Vertical", "FULFILMENT TYPE",
            "MRP_Actual", "Master Category", "Gender",
            "NPD /EPD/ Exclusive", "Short Form", "EAN", "SKU ID",
            "Active/Discontinued", "Channel",
            "Sellable_Stock", "Sales_7D", "Sales_14D", "Sales_30D",
            "Days_Cover", "Stock_Health", "Orders_Pending", "Damaged",
            "Inv_Price", "Warehouses", "Inv_Fulfillment", "F_Assured",
            "Listing_Status", "Inactive_Reason", "Listed_Price",
            "System_Stock", "Fulfillment_By",
        ] if c in dm.columns]

        default_cols = [c for c in [
            "FSN", "Brand", "Category", "Range", "Size/Qty",
            "Active/Discontinued", "Sellable_Stock", "Sales_7D",
            "Sales_30D", "Days_Cover", "Stock_Health", "Listing_Status",
        ] if c in dm.columns]

        chosen = st.multiselect(
            "Columns to display", options=all_cols, default=default_cols, key="full_sku_cols"
        )
        ft_search = st.text_input("🔍 Search FSN / Title / Range / Brand", key="full_sku_search")

        dm_show = dm[chosen].copy() if chosen else dm.copy()
        if ft_search:
            mask = dm_show.apply(
                lambda r: ft_search.lower() in " ".join(r.astype(str).values).lower(), axis=1
            )
            dm_show = dm_show[mask]

        fmt_full = {}
        for c in ["Sellable_Stock", "Sales_7D", "Sales_14D", "Sales_30D",
                   "Orders_Pending", "Damaged", "System_Stock", "Warehouses"]:
            if c in dm_show.columns:
                fmt_full[c] = "{:,.0f}"
        for c in ["Days_Cover"]:
            if c in dm_show.columns:
                fmt_full[c] = "{:.0f}"
        for c in ["Inv_Price", "Listed_Price", "MRP_Actual", "MRP"]:
            if c in dm_show.columns:
                fmt_full[c] = "₹{:,.0f}"

        _render_table(dm_show.reset_index(drop=True), fmt_full)
        st.caption(f"{len(dm_show):,} SKUs shown")
