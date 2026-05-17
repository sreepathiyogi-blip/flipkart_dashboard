"""
sku_master_section.py
─────────────────────
Drop this file next to your main app.py.

Integration (3 lines in app.py):
─────────────────────────────────
1. At the top of app.py, after your existing imports:
       from sku_master_section import render_sku_sidebar, render_sku_section, set_client_fn

2. After get_gsheet_client() function definition:
       set_client_fn(get_gsheet_client)

3. Inside with st.sidebar: (after earn more uploaders):
       render_sku_sidebar()

4. Inside main(), before if __name__ == "__main__":
       render_sku_section()
"""

import sys
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ── Module-level client function — injected by app.py ──────────────────────
_CLIENT_FN = None

def set_client_fn(fn):
    global _CLIENT_FN
    _CLIENT_FN = fn

def _get_client():
    if _CLIENT_FN is None:
        raise RuntimeError("Call set_client_fn(get_gsheet_client) in app.py first.")
    return _CLIENT_FN()

# ── Sheet names ─────────────────────────────────────────────────────────────
MASTER_SHEET  = "Flipkart_SKU_Master_DB"
INV_SHEET     = "Flipkart_Inventory_DB"
LISTING_SHEET = "Flipkart_Listing_DB"

# ── Brand normalisation ─────────────────────────────────────────────────────
_BV = {"BELLAVITA","Bella vita organic","Bellavita","bella vita",
       "BELLA VITA ORGANIC","bellavita","Bella Vita Organic"}

def _norm_brand(df):
    if "Brand" not in df.columns:
        return df
    df = df.copy()
    df["Brand"] = df["Brand"].astype(str).str.strip()
    df["Brand"] = df["Brand"].apply(lambda x: "Bellavita" if x in _BV else x)
    return df

# ── GSheet helpers ──────────────────────────────────────────────────────────
def _get_or_create(client, name):
    try:
        return client.open(name)
    except Exception:
        sh = client.create(name)
        sh.share(None, perm_type="anyone", role="writer")
        return sh

def _load(client, sheet_name):
    try:
        sh = _get_or_create(client, sheet_name)
        data = sh.sheet1.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"Load error ({sheet_name}): {e}")
        return pd.DataFrame()

def _overwrite(client, sheet_name, df):
    sh = _get_or_create(client, sheet_name)
    ws = sh.sheet1
    df = df.copy().fillna("").astype(str)
    ws.clear()
    ws.update([df.columns.tolist()] + df.values.tolist())
    return len(df)

# ── SKU Master — upsert by FSN ──────────────────────────────────────────────
def _save_master(client, new_df):
    sh = _get_or_create(client, MASTER_SHEET)
    ws = sh.sheet1
    new_df = _norm_brand(new_df.copy()).fillna("").astype(str)
    existing = ws.get_all_records()
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

# ── Cached loaders (5-min TTL) ──────────────────────────────────────────────
@st.cache_data(ttl=300)
def _cached_master():
    return _norm_brand(_load(_get_client(), MASTER_SHEET))

@st.cache_data(ttl=300)
def _cached_inv():
    df = _load(_get_client(), INV_SHEET)
    num_cols = ["Live on Website","Sales 7D","Sales 14D","Sales 30D",
                "Sales 60D","Sales 90D","Flipkart Selling Price",
                "Reserved for Orders and Recalls","Orders to Dispatch",
                "Returns Processing","Damaged"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df

@st.cache_data(ttl=300)
def _cached_listing():
    return _load(_get_client(), LISTING_SHEET)

# ── Build merged master ─────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def _build_merged():
    df_m = _cached_master()
    df_i = _cached_inv()
    df_l = _cached_listing()

    if df_m.empty:
        return pd.DataFrame()

    # Aggregate inventory to FSN level
    if not df_i.empty and "FSN" in df_i.columns:
        agg = df_i.groupby("FSN").agg(
            Sellable_Stock   =("Live on Website",  "sum"),
            Sales_7D         =("Sales 7D",         "sum"),
            Sales_14D        =("Sales 14D",        "sum"),
            Sales_30D        =("Sales 30D",        "sum"),
            Orders_Pending   =("Orders to Dispatch","sum"),
            Damaged          =("Damaged",           "sum"),
            Selling_Price    =("Flipkart Selling Price","first"),
            Warehouses       =("Warehouse Id",     "nunique"),
            Fulfillment      =("Fulfilment Type",  lambda x: " | ".join(x.dropna().unique())),
        ).reset_index()
        merged = df_m.merge(agg, on="FSN", how="left")
    else:
        merged = df_m.copy()

    # Merge listing
    if not df_l.empty:
        fsn_col = "Flipkart Serial Number" if "Flipkart Serial Number" in df_l.columns else None
        if fsn_col:
            dl = df_l.rename(columns={
                fsn_col:           "FSN",
                "Listing Status":  "Listing_Status",
                "Inactive Reason": "Inactive_Reason",
                "Your Selling Price": "Listed_Price",
                "System Stock count": "System_Stock",
                "Fulfillment By":  "Fulfillment_Listing",
            })
            keep = [c for c in ["FSN","Listing_Status","Inactive_Reason",
                                  "Listed_Price","System_Stock","Fulfillment_Listing"]
                    if c in dl.columns]
            merged = merged.merge(dl[keep], on="FSN", how="left")

    # Derived fields
    if "Sellable_Stock" in merged.columns and "Sales_7D" in merged.columns:
        merged["Sellable_Stock"] = pd.to_numeric(merged["Sellable_Stock"], errors="coerce").fillna(0)
        merged["Sales_7D"]       = pd.to_numeric(merged["Sales_7D"],       errors="coerce").fillna(0)
        merged["Days_Cover"] = (
            merged["Sellable_Stock"] / merged["Sales_7D"].replace(0, np.nan) * 7
        ).round(0)
        merged["Stock_Health"] = merged.apply(
            lambda r: "🔴 OOS"    if r["Sellable_Stock"] <= 0
            else ("🟡 Low"        if pd.notna(r["Days_Cover"]) and r["Days_Cover"] < 7
            else ("🟠 Medium"     if pd.notna(r["Days_Cover"]) and r["Days_Cover"] < 14
            else "🟢 Healthy")), axis=1
        )
    return merged

# ═══════════════════════════════════════════════════════════════════════════
# PUBLIC — Sidebar uploaders
# ═══════════════════════════════════════════════════════════════════════════
def render_sku_sidebar():
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📦 SKU Master & Inventory")

    # ── SKU Master upload ──────────────────────────────────────────────────
    master_file = st.sidebar.file_uploader(
        "Upload SKU Master (.xlsx / .csv)", type=["xlsx","xls","csv"],
        key="sku_master_upload"
    )
    if master_file:
        try:
            raw = (pd.read_csv(master_file) if master_file.name.endswith(".csv")
                   else pd.read_excel(master_file))
            raw.columns = raw.columns.str.strip()
            raw = raw.loc[:, ~raw.columns.str.startswith("Unnamed")]
            st.sidebar.success(f"✅ {len(raw):,} rows ready")
            if st.sidebar.button("💾 Save SKU Master", key="btn_save_master"):
                with st.spinner("Saving SKU Master…"):
                    added, dupes = _save_master(_get_client(), raw)
                    _cached_master.clear(); _build_merged.clear()
                st.sidebar.success(f"✅ {added:,} new | {dupes:,} dupes skipped")
        except Exception as e:
            st.sidebar.error(f"SKU Master error: {e}")

    # ── Inventory upload ───────────────────────────────────────────────────
    inv_file = st.sidebar.file_uploader(
        "Upload Inventory snapshot (.csv / .xlsx)", type=["csv","xlsx","xls"],
        key="inv_upload"
    )
    if inv_file:
        try:
            raw_i = (pd.read_csv(inv_file) if inv_file.name.endswith(".csv")
                     else pd.read_excel(inv_file))
            raw_i.columns = raw_i.columns.str.strip()
            st.sidebar.success(f"✅ {len(raw_i):,} rows ready")
            if st.sidebar.button("💾 Save Inventory", key="btn_save_inv"):
                with st.spinner("Saving Inventory…"):
                    n = _overwrite(_get_client(), INV_SHEET, raw_i)
                    _cached_inv.clear(); _build_merged.clear()
                st.sidebar.success(f"✅ {n:,} rows saved (snapshot replaced)")
        except Exception as e:
            st.sidebar.error(f"Inventory error: {e}")

    # ── Listing upload ─────────────────────────────────────────────────────
    lst_file = st.sidebar.file_uploader(
        "Upload Listing report (.xls / .xlsx / .csv)", type=["xls","xlsx","csv"],
        key="listing_upload"
    )
    if lst_file:
        try:
            if lst_file.name.endswith(".csv"):
                raw_l = pd.read_csv(lst_file)
            elif lst_file.name.endswith(".xls"):
                raw_l = pd.read_excel(lst_file, engine="xlrd")
            else:
                raw_l = pd.read_excel(lst_file)
            raw_l.columns = raw_l.columns.str.strip()
            st.sidebar.success(f"✅ {len(raw_l):,} listings ready")
            if st.sidebar.button("💾 Save Listing", key="btn_save_listing"):
                with st.spinner("Saving Listing…"):
                    n = _overwrite(_get_client(), LISTING_SHEET, raw_l)
                    _cached_listing.clear(); _build_merged.clear()
                st.sidebar.success(f"✅ {n:,} listings saved")
        except Exception as e:
            st.sidebar.error(f"Listing error: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# PUBLIC — Main section (4 tabs)
# ═══════════════════════════════════════════════════════════════════════════
def render_sku_section():
    st.markdown("<div id='sku_master'></div>", unsafe_allow_html=True)
    st.markdown("## 📦 SKU Master & Inventory")

    df = _build_merged()

    if df.empty:
        st.info("No SKU Master data yet — upload via sidebar.")
        return

    # ── Global filters ─────────────────────────────────────────────────────
    f1, f2, f3, f4 = st.columns(4)
    with f1:
        brands = ["All"] + sorted(df["Brand"].dropna().unique().tolist()) if "Brand" in df.columns else ["All"]
        sel_brand = st.selectbox("Brand", brands, key="sku_brand")
    with f2:
        cats = ["All"] + sorted(df["Category"].dropna().unique().tolist()) if "Category" in df.columns else ["All"]
        sel_cat = st.selectbox("Category", cats, key="sku_cat")
    with f3:
        ranges = ["All"] + sorted(df["Range"].dropna().unique().tolist()) if "Range" in df.columns else ["All"]
        sel_range = st.selectbox("Range", ranges, key="sku_range")
    with f4:
        if "Active/Discontinued" in df.columns:
            statuses = ["All"] + sorted(df["Active/Discontinued"].dropna().unique().tolist())
            sel_status = st.selectbox("Status", statuses, key="sku_status")
        else:
            sel_status = "All"

    dff = df.copy()
    if sel_brand  != "All" and "Brand"              in dff.columns: dff = dff[dff["Brand"]              == sel_brand]
    if sel_cat    != "All" and "Category"           in dff.columns: dff = dff[dff["Category"]           == sel_cat]
    if sel_range  != "All" and "Range"              in dff.columns: dff = dff[dff["Range"]              == sel_range]
    if sel_status != "All" and "Active/Discontinued" in dff.columns: dff = dff[dff["Active/Discontinued"] == sel_status]

    # ── Tabs ───────────────────────────────────────────────────────────────
    tab_cat, tab_range, tab_inv, tab_full = st.tabs([
        "🗂️ Category View", "🎯 Range View", "📦 Inventory Health", "📋 Full SKU Table"
    ])

    # ── TAB 1: Category View ───────────────────────────────────────────────
    with tab_cat:
        if "Category" not in dff.columns:
            st.info("No Category column found in SKU Master.")
        else:
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total FSNs", f"{len(dff):,}")
            active_n = len(dff[dff["Active/Discontinued"] == "ACTIVE"]) if "Active/Discontinued" in dff.columns else len(dff)
            k2.metric("Active", f"{active_n:,}")
            k3.metric("Inactive", f"{len(dff) - active_n:,}")
            k4.metric("Categories", dff["Category"].nunique())

            # FSN count by Category × Brand
            cat_brand = dff.groupby(["Category","Brand"]).size().reset_index(name="FSNs")
            fig1 = px.bar(cat_brand, x="Category", y="FSNs", color="Brand",
                          title="FSN Count by Category & Brand",
                          template="plotly_dark", barmode="stack")
            fig1.update_layout(xaxis_tickangle=-40)
            st.plotly_chart(fig1, use_container_width=True)

            # Active vs Inactive by Category
            if "Active/Discontinued" in dff.columns:
                ca1, ca2 = st.columns(2)
                with ca1:
                    act_cat = dff.groupby(["Category","Active/Discontinued"]).size().reset_index(name="Count")
                    st.plotly_chart(px.bar(act_cat, x="Category", y="Count",
                        color="Active/Discontinued",
                        color_discrete_map={"ACTIVE":"#2ecc71","INACTIVE":"#e74c3c"},
                        template="plotly_dark", title="Active vs Inactive by Category",
                        barmode="stack"), use_container_width=True)
                with ca2:
                    if "Sub-category" in dff.columns:
                        sub = dff.groupby(["Category","Sub-category"]).size().reset_index(name="FSNs")
                        st.plotly_chart(px.bar(sub, x="Sub-category", y="FSNs",
                            color="Category", template="plotly_dark",
                            title="Sub-category Breakdown"), use_container_width=True)

            # NPD / EPD / Exclusive split
            for col in ["NPD /EPD/ Exclusive Range", "NPD/EPD/Exclusive Range"]:
                if col in dff.columns:
                    npd = dff.groupby(col).size().reset_index(name="Count")
                    st.plotly_chart(px.pie(npd, values="Count", names=col,
                        title="NPD / EPD / Exclusive Split",
                        template="plotly_dark", hole=0.4), use_container_width=True)
                    break

            # Sub-category table
            st.markdown("#### Sub-category Summary")
            sub_cols = ["Category","Sub-category","Brand"]
            sub_cols = [c for c in sub_cols if c in dff.columns]
            if sub_cols:
                sub_tbl = dff.groupby(sub_cols).agg(
                    FSNs=("FSN","count"),
                    **({} if "Sellable_Stock" not in dff.columns else
                       {"Sellable_Stock":("Sellable_Stock","sum"),
                        "Sales_30D":("Sales_30D","sum")})
                ).reset_index().sort_values("FSNs", ascending=False)
                st.dataframe(sub_tbl, use_container_width=True, hide_index=True)

    # ── TAB 2: Range View ──────────────────────────────────────────────────
    with tab_range:
        if "Range" not in dff.columns:
            st.info("No Range column found in SKU Master.")
        else:
            # Top 25 ranges by FSN count
            top_ranges = dff["Range"].value_counts().head(25).reset_index()
            top_ranges.columns = ["Range","FSNs"]
            fig_r = px.bar(top_ranges, x="Range", y="FSNs",
                           title="Top 25 Ranges by FSN Count",
                           template="plotly_dark",
                           color="FSNs", color_continuous_scale=["#2a1a4a","#9B59B6"])
            fig_r.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_r, use_container_width=True)

            # Range × Category heatmap
            if "Category" in dff.columns:
                rc1, rc2 = st.columns(2)
                with rc1:
                    heat = dff.groupby(["Range","Category"]).size().reset_index(name="FSNs")
                    top_r_list = top_ranges["Range"].head(15).tolist()
                    heat = heat[heat["Range"].isin(top_r_list)]
                    pivot = heat.pivot(index="Range", columns="Category", values="FSNs").fillna(0)
                    fig_heat = px.imshow(pivot, title="Range × Category Heatmap",
                                         template="plotly_dark",
                                         color_continuous_scale="Purples", aspect="auto")
                    st.plotly_chart(fig_heat, use_container_width=True)

                with rc2:
                    # Range × Brand stacked
                    if "Brand" in dff.columns:
                        rb = dff[dff["Range"].isin(top_r_list)].groupby(["Range","Brand"]).size().reset_index(name="FSNs")
                        st.plotly_chart(px.bar(rb, x="Range", y="FSNs", color="Brand",
                            title="Range × Brand", template="plotly_dark",
                            barmode="stack"), use_container_width=True)

            # Size / Qty breakdown
            for size_col in ["Size","Qty","Pack Size","Size/Qty"]:
                if size_col in dff.columns:
                    rs = dff[dff["Range"].isin(top_ranges["Range"].head(15))].groupby(
                        ["Range", size_col]).size().reset_index(name="FSNs")
                    st.plotly_chart(px.bar(rs, x="Range", y="FSNs", color=size_col,
                        title=f"Range × {size_col}", template="plotly_dark",
                        barmode="stack"), use_container_width=True)
                    break

            # Searchable range table
            st.markdown("#### Range Detail Table")
            rng_search = st.text_input("🔍 Search Range", key="rng_search")
            range_tbl_cols = ["Range","Category","Brand","FSN"]
            range_tbl_cols = [c for c in range_tbl_cols if c in dff.columns]
            agg_dict = {"FSNs": ("FSN", "count")}
            if "Sellable_Stock" in dff.columns:
                agg_dict["Sellable_Stock"] = ("Sellable_Stock", "sum")
                agg_dict["OOS_FSNs"] = ("Stock_Health", lambda x: (x == "🔴 OOS").sum())
            if "Sales_30D" in dff.columns:
                agg_dict["Sales_30D"] = ("Sales_30D", "sum")

            grp_cols = [c for c in ["Range","Category","Brand"] if c in dff.columns]
            rng_tbl = dff.groupby(grp_cols).agg(**agg_dict).reset_index().sort_values("FSNs", ascending=False)

            if rng_search:
                mask = rng_tbl.apply(lambda r: rng_search.lower() in " ".join(r.astype(str)).lower(), axis=1)
                rng_tbl = rng_tbl[mask]
            st.dataframe(rng_tbl, use_container_width=True, hide_index=True)

    # ── TAB 3: Inventory Health ────────────────────────────────────────────
    with tab_inv:
        if "Sellable_Stock" not in dff.columns:
            st.info("Upload inventory snapshot via sidebar first.")
        else:
            oos   = dff[dff["Stock_Health"] == "🔴 OOS"]
            low   = dff[dff["Stock_Health"] == "🟡 Low"]
            med   = dff[dff["Stock_Health"] == "🟠 Medium"]
            hlthy = dff[dff["Stock_Health"] == "🟢 Healthy"]

            i1, i2, i3, i4, i5 = st.columns(5)
            i1.metric("Total FSNs",  f"{len(dff):,}")
            i2.metric("🔴 OOS",      f"{len(oos):,}")
            i3.metric("🟡 Low Stock",f"{len(low):,}")
            i4.metric("🟠 Medium",   f"{len(med):,}")
            i5.metric("🟢 Healthy",  f"{len(hlthy):,}")

            inv_c1, inv_c2 = st.columns(2)
            with inv_c1:
                status_cnt = dff["Stock_Health"].value_counts().reset_index()
                status_cnt.columns = ["Status","Count"]
                color_map = {"🔴 OOS":"#e74c3c","🟡 Low":"#f39c12",
                             "🟠 Medium":"#e67e22","🟢 Healthy":"#2ecc71"}
                st.plotly_chart(px.pie(status_cnt, values="Count", names="Status",
                    title="Stock Health Distribution", template="plotly_dark",
                    color="Status", color_discrete_map=color_map, hole=0.4),
                    use_container_width=True)

            with inv_c2:
                if "Brand" in dff.columns:
                    bh = dff.groupby(["Brand","Stock_Health"]).size().reset_index(name="Count")
                    st.plotly_chart(px.bar(bh, x="Brand", y="Count",
                        color="Stock_Health", color_discrete_map=color_map,
                        template="plotly_dark", title="Stock Health by Brand",
                        barmode="stack"), use_container_width=True)

            # Category × health
            if "Category" in dff.columns:
                ch = dff.groupby(["Category","Stock_Health"]).size().reset_index(name="Count")
                st.plotly_chart(px.bar(ch, x="Category", y="Count",
                    color="Stock_Health", color_discrete_map=color_map,
                    template="plotly_dark", title="Stock Health by Category",
                    barmode="stack"), use_container_width=True)

            # Range OOS exposure
            if "Range" in dff.columns:
                rng_oos = dff.groupby("Range").agg(
                    Total=("FSN","count"),
                    OOS=("Stock_Health", lambda x: (x=="🔴 OOS").sum())
                ).reset_index()
                rng_oos["OOS_%"] = (rng_oos["OOS"] / rng_oos["Total"] * 100).round(1)
                rng_oos = rng_oos[rng_oos["OOS"] > 0].sort_values("OOS_%", ascending=False).head(20)
                if not rng_oos.empty:
                    st.plotly_chart(px.bar(rng_oos, x="Range", y="OOS_%",
                        color="OOS_%", color_continuous_scale=["#f39c12","#e74c3c"],
                        template="plotly_dark", title="Range OOS Exposure (%)"),
                        use_container_width=True)

            # OOS drill-down
            with st.expander(f"🔴 OOS FSNs ({len(oos)}) — sort by 30D velocity"):
                if oos.empty:
                    st.success("No OOS FSNs!")
                else:
                    oos_show_cols = [c for c in ["FSN","Brand","Title","Category","Range",
                        "Sales_7D","Sales_30D","Orders_Pending","Fulfillment",
                        "Listing_Status"] if c in oos.columns]
                    oos_sorted = oos[oos_show_cols].sort_values(
                        "Sales_30D", ascending=False) if "Sales_30D" in oos.columns else oos[oos_show_cols]
                    st.dataframe(oos_sorted.reset_index(drop=True), use_container_width=True, hide_index=True)

            # Low stock drill-down
            with st.expander(f"🟡 Low Stock FSNs ({len(low)}) — <7 days cover"):
                if low.empty:
                    st.success("No low-stock FSNs!")
                else:
                    low_cols = [c for c in ["FSN","Brand","Title","Category","Range",
                        "Sellable_Stock","Sales_7D","Days_Cover","Fulfillment"] if c in low.columns]
                    st.dataframe(low[low_cols].sort_values("Days_Cover").reset_index(drop=True),
                                 use_container_width=True, hide_index=True)

            # OOS but listing Active — mismatch alert
            if "Listing_Status" in dff.columns:
                mismatch = dff[(dff["Stock_Health"] == "🔴 OOS") &
                               (dff["Listing_Status"].astype(str).str.upper() == "ACTIVE")]
                if not mismatch.empty:
                    with st.expander(f"⚠️ OOS but still ACTIVE on listing ({len(mismatch)} FSNs)"):
                        mc = [c for c in ["FSN","Brand","Title","Category","Listing_Status",
                            "Sales_30D","Orders_Pending"] if c in mismatch.columns]
                        st.dataframe(mismatch[mc].reset_index(drop=True),
                                     use_container_width=True, hide_index=True)

            # Velocity scatter
            if "Sales_30D" in dff.columns and "Sellable_Stock" in dff.columns:
                scatter_df = dff[dff["Sales_30D"] > 0].copy()
                if not scatter_df.empty:
                    hover = [c for c in ["FSN","Brand","Title","Category"] if c in scatter_df.columns]
                    st.plotly_chart(px.scatter(scatter_df, x="Sales_30D", y="Sellable_Stock",
                        color="Stock_Health", color_discrete_map=color_map,
                        hover_data=hover, template="plotly_dark",
                        title="Sales Velocity vs Sellable Stock",
                        labels={"Sales_30D":"30D Sales","Sellable_Stock":"Sellable Stock"}),
                        use_container_width=True)

    # ── TAB 4: Full SKU Table ──────────────────────────────────────────────
    with tab_full:
        all_cols = dff.columns.tolist()
        default_cols = [c for c in ["FSN","Brand","Title","Category","Sub-category",
            "Range","Active/Discontinued","Sellable_Stock","Sales_7D","Sales_30D",
            "Days_Cover","Stock_Health","Listing_Status","Selling_Price","Fulfillment"]
            if c in all_cols]
        sel_cols = st.multiselect("Columns to show", all_cols,
                                   default=default_cols, key="full_cols")
        full_search = st.text_input("🔍 Search any field", key="full_search")

        show = dff[sel_cols].copy() if sel_cols else dff.copy()
        if full_search:
            mask = show.apply(lambda r: full_search.lower() in " ".join(r.astype(str)).lower(), axis=1)
            show = show[mask]

        st.dataframe(show.reset_index(drop=True), use_container_width=True, hide_index=True)
        st.caption(f"{len(show):,} rows shown")

        # Download button
        csv_bytes = show.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download CSV", csv_bytes,
                           file_name="sku_master_export.csv", mime="text/csv")
