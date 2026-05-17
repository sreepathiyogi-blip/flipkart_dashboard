# ══════════════════════════════════════════════════════════════════════════════
# HOW TO INTEGRATE
# ──────────────────────────────────────────────────────────────────────────────
# 1. Add the SIDEBAR UPLOADERS block inside your `with st.sidebar:` block,
#    just after the existing "Upload Data" section.
#
# 2. Add the CONSTANTS + HELPERS block at module level (outside main()),
#    just after your existing BRAND_COLORS / FRAG_KW constants.
#
# 3. Add the MAIN SECTION block inside main(), after your last existing section
#    (after the Exclusives block, before the extra_num columns block).
#
# 4. Add the nav link to your existing nav_html loop:
#    ("📦 SKU Master & Inventory", "sku_master")
# ══════════════════════════════════════════════════════════════════════════════


# ─────────────────────────────────────────────────────────────────────────────
# BLOCK 1 — CONSTANTS & HELPERS  (paste at module level, after FRAG_KW)
# ─────────────────────────────────────────────────────────────────────────────

MASTER_GSHEET   = "Flipkart_SKU_Master_DB"
INVENTORY_GSHEET = "Flipkart_Inventory_DB"
LISTING_GSHEET  = "Flipkart_Listing_DB"

BELLAVITA_ALL = [
    "BELLAVITA", "Bella vita organic", "Bellavita", "bella vita",
    "BELLA VITA ORGANIC", "bellavita", "Bella Vita Organic",
]

def _norm_brands_master(df):
    df = df.copy()
    df["Brand"] = df["Brand"].astype(str).str.strip().apply(
        lambda x: "Bellavita" if x in BELLAVITA_ALL else x
    )
    return df


def _load_gsheet(client, name):
    try:
        sh = get_or_create_sheet(client, name)
        data = sh.sheet1.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"Load error [{name}]: {e}")
        return pd.DataFrame()


def _save_gsheet_overwrite(client, df, name):
    """Overwrite the entire sheet — used for inventory/listing snapshots."""
    sh = get_or_create_sheet(client, name)
    ws = sh.sheet1
    df = df.copy().fillna("").astype(str)
    ws.clear()
    ws.update([df.columns.tolist()] + df.values.tolist())
    return len(df)


def _save_master_upsert(client, new_df, name):
    """FSN-keyed upsert for SKU Master (append new, skip existing)."""
    sh = get_or_create_sheet(client, name)
    ws = sh.sheet1
    existing = ws.get_all_records()
    new_df = _norm_brands_master(new_df.copy()).fillna("").astype(str)
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


@st.cache_data(ttl=300)
def _build_merged_master(master_name, inv_name, listing_name):
    """
    Joins SKU Master + Inventory + Listing on FSN.
    Returns a single wide DataFrame used by all sub-views.
    """
    client = get_gsheet_client()

    df_m = _load_gsheet(client, master_name)
    df_i = _load_gsheet(client, inv_name)
    df_l = _load_gsheet(client, listing_name)

    if df_m.empty:
        return pd.DataFrame()

    df_m.columns = df_m.columns.str.strip()
    df_m = df_m.loc[:, ~df_m.columns.str.startswith("Unnamed")]
    df_m = _norm_brands_master(df_m)

    # ── Aggregate inventory per FSN ──────────────────────────────────────────
    if not df_i.empty:
        num_inv = ["Live on Website", "Sales 7D", "Sales 14D", "Sales 30D",
                   "Sales 60D", "Sales 90D", "Flipkart Selling Price",
                   "Orders to Dispatch", "Damaged", "Returns Processing",
                   "Reserved for Orders and Recalls"]
        for c in num_inv:
            if c in df_i.columns:
                df_i[c] = pd.to_numeric(df_i[c], errors="coerce").fillna(0)

        inv_agg = df_i.groupby("FSN").agg(
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
        inv_agg["FSN"] = inv_agg["FSN"].astype(str)
    else:
        inv_agg = pd.DataFrame()

    # ── Listing (first data row = idx 1 because row 0 is descriptions) ───────
    if not df_l.empty:
        # When loaded from GSheets the header is already correct
        lst = df_l.copy()
        lst["FSN"] = lst.get("Flipkart Serial Number", pd.Series(dtype=str)).astype(str)
        lst = lst.rename(columns={
            "Listing Status": "Listing_Status",
            "Inactive Reason": "Inactive_Reason",
            "Your Selling Price": "Listed_Price",
            "System Stock count": "System_Stock",
            "Fulfillment By": "Fulfillment_By",
        })
        lst_cols = [c for c in ["FSN", "Listing_Status", "Inactive_Reason",
                                 "Listed_Price", "System_Stock", "Fulfillment_By"] if c in lst.columns]
        lst = lst[lst_cols].drop_duplicates("FSN")
        for c in ["Listed_Price", "System_Stock"]:
            if c in lst.columns:
                lst[c] = pd.to_numeric(lst[c], errors="coerce").fillna(0)
    else:
        lst = pd.DataFrame()

    # ── Merge ────────────────────────────────────────────────────────────────
    df_m["FSN"] = df_m["FSN"].astype(str)
    merged = df_m.copy()
    if not inv_agg.empty:
        merged = merged.merge(inv_agg, on="FSN", how="left")
    if not lst.empty:
        merged = merged.merge(lst, on="FSN", how="left")

    # ── Derived ──────────────────────────────────────────────────────────────
    if "Sellable_Stock" in merged.columns:
        merged["Days_Cover"] = (
            merged["Sellable_Stock"] / merged["Sales_7D"].replace(0, np.nan) * 7
        ).round(0)

        def _health(r):
            s = r.get("Sellable_Stock", np.nan)
            d = r.get("Days_Cover", np.nan)
            if pd.isna(s) or s <= 0:
                return "🔴 OOS"
            if pd.notna(d) and d < 7:
                return "🟡 Low (<7d)"
            if pd.notna(d) and d < 14:
                return "🟠 Medium (7-14d)"
            return "🟢 Healthy"

        merged["Stock_Health"] = merged.apply(_health, axis=1)

    return merged


# ─────────────────────────────────────────────────────────────────────────────
# BLOCK 2 — SIDEBAR UPLOADERS  (paste inside `with st.sidebar:` after "---")
# ─────────────────────────────────────────────────────────────────────────────

"""
Paste this inside your existing `with st.sidebar:` block,
right after the first `st.markdown("---")`:

    st.markdown("### 📦 SKU Master & Inventory")

    # ── SKU Master ────────────────────────────────────────
    master_up = st.file_uploader(
        "SKU Master (.xlsx)", type=["xlsx","xls","csv"], key="sb_master"
    )
    if master_up:
        try:
            raw_m = (pd.read_csv(master_up) if master_up.name.endswith(".csv")
                     else pd.read_excel(master_up))
            raw_m.columns = raw_m.columns.str.strip()
            raw_m = raw_m.loc[:, ~raw_m.columns.str.startswith("Unnamed")]
            st.info(f"📋 {len(raw_m):,} FSNs ready")
            if st.button("💾 Save SKU Master", key="sb_save_master"):
                with st.spinner("Saving..."):
                    cli = get_gsheet_client()
                    added, dupes = _save_master_upsert(cli, raw_m, MASTER_GSHEET)
                st.success(f"✅ {added} new | {dupes} existing skipped")
                st.cache_data.clear()
        except Exception as e:
            st.error(f"Master error: {e}")

    # ── Inventory ─────────────────────────────────────────
    inv_up = st.file_uploader(
        "Current Inventory (.csv/.xlsx)", type=["csv","xlsx","xls"], key="sb_inv"
    )
    if inv_up:
        try:
            raw_inv = (pd.read_csv(inv_up) if inv_up.name.endswith(".csv")
                       else pd.read_excel(inv_up))
            raw_inv.columns = raw_inv.columns.str.strip()
            st.info(f"📦 {len(raw_inv):,} rows ready")
            if st.button("💾 Save Inventory", key="sb_save_inv"):
                with st.spinner("Saving..."):
                    cli = get_gsheet_client()
                    n = _save_gsheet_overwrite(cli, raw_inv, INVENTORY_GSHEET)
                st.success(f"✅ {n:,} rows saved (snapshot replaced)")
                st.cache_data.clear()
        except Exception as e:
            st.error(f"Inventory error: {e}")

    # ── Listing ───────────────────────────────────────────
    lst_up = st.file_uploader(
        "Listing Report (.xls/.xlsx/.csv)", type=["xls","xlsx","csv"], key="sb_lst"
    )
    if lst_up:
        try:
            if lst_up.name.endswith(".csv"):
                raw_lst = pd.read_csv(lst_up)
            elif lst_up.name.endswith(".xls"):
                raw_lst = pd.read_excel(lst_up, engine="xlrd")
                # Drop Flipkart's description row (row 0 after header)
                if raw_lst.iloc[0]["Flipkart Serial Number"] in [np.nan, "Flipkart's Identifier of the product", None]:
                    raw_lst = raw_lst.iloc[1:].reset_index(drop=True)
            else:
                raw_lst = pd.read_excel(lst_up)
            raw_lst.columns = raw_lst.columns.str.strip()
            st.info(f"🏷️ {len(raw_lst):,} listings ready")
            if st.button("💾 Save Listing", key="sb_save_lst"):
                with st.spinner("Saving..."):
                    cli = get_gsheet_client()
                    n = _save_gsheet_overwrite(cli, raw_lst, LISTING_GSHEET)
                st.success(f"✅ {n:,} listings saved (snapshot replaced)")
                st.cache_data.clear()
        except Exception as e:
            st.error(f"Listing error: {e}")

    st.markdown("---")
"""


# ─────────────────────────────────────────────────────────────────────────────
# BLOCK 3 — MAIN SECTION  (paste inside main(), after the Exclusives section)
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("<div id='sku_master'></div>", unsafe_allow_html=True)
sec_hdr("📦 SKU Master & Inventory", "sku_master")

# ── Load merged data ──────────────────────────────────────────────────────────
df_all_skus = _build_merged_master(MASTER_GSHEET, INVENTORY_GSHEET, LISTING_GSHEET)

if df_all_skus.empty:
    st.info("Upload SKU Master (and optionally Inventory + Listing) using the sidebar uploaders above.")
else:
    has_inv = "Sellable_Stock" in df_all_skus.columns
    has_lst = "Listing_Status" in df_all_skus.columns

    # ── Global filters ────────────────────────────────────────────────────────
    gf1, gf2, gf3, gf4 = st.columns(4)
    with gf1:
        sm_brand = st.selectbox(
            "Brand", ["All"] + sorted(df_all_skus["Brand"].dropna().unique().tolist()),
            key="sm_brand"
        )
    with gf2:
        sm_cat = st.selectbox(
            "Category", ["All"] + sorted(df_all_skus["Category"].dropna().unique().tolist()),
            key="sm_cat"
        )
    with gf3:
        all_ranges = sorted(df_all_skus["Range"].dropna().unique().tolist())
        sm_range = st.selectbox("Range", ["All"] + all_ranges, key="sm_range")
    with gf4:
        sm_status = st.selectbox(
            "Active/Discontinued", ["All", "ACTIVE", "INACTIVE"], key="sm_status"
        )

    dm = df_all_skus.copy()
    if sm_brand  != "All": dm = dm[dm["Brand"]  == sm_brand]
    if sm_cat    != "All": dm = dm[dm["Category"] == sm_cat]
    if sm_range  != "All": dm = dm[dm["Range"]   == sm_range]
    if sm_status != "All": dm = dm[dm["Active/Discontinued"] == sm_status]

    # ══════════════════════════════════════════════════════════════════════════
    # TAB LAYOUT
    # ══════════════════════════════════════════════════════════════════════════
    tab_cat, tab_range, tab_inv, tab_full = st.tabs([
        "🗂️ Category View",
        "🎯 Range View",
        "📦 Inventory Health",
        "📋 Full SKU Table",
    ])

    # ──────────────────────────────────────────────────────────────────────────
    # TAB 1 — CATEGORY VIEW
    # ──────────────────────────────────────────────────────────────────────────
    with tab_cat:
        sec_label = "<div style='background:rgba(108,52,131,0.12);border:1px solid #2a2a4a;border-radius:10px;padding:9px 16px;margin:10px 0'><span style='font-size:14px;font-weight:700;color:#D7BDE2'>Category × Brand bifurcation</span></div>"
        st.markdown(sec_label, unsafe_allow_html=True)

        # KPI row
        ck1, ck2, ck3, ck4 = st.columns(4)
        with ck1: metric_card("Total FSNs", len(dm), prefix="", suffix="")
        with ck2: metric_card("Active", len(dm[dm["Active/Discontinued"] == "ACTIVE"]), prefix="", suffix="")
        with ck3: metric_card("Categories", dm["Category"].nunique(), prefix="", suffix="")
        with ck4: metric_card("Brands", dm["Brand"].nunique(), prefix="", suffix="")

        ca1, ca2 = st.columns(2)

        # Category × Brand stacked bar
        with ca1:
            cb_cnt = dm.groupby(["Category", "Brand"]).size().reset_index(name="FSN Count")
            fig_cb = px.bar(
                cb_cnt, x="Category", y="FSN Count", color="Brand",
                template="plotly_dark", title="FSN Count — Category × Brand",
                barmode="stack", color_discrete_map=BRAND_COLORS,
                labels={"FSN Count": "No. of FSNs"},
            )
            st.plotly_chart(fig_cb, use_container_width=True)

        # Category share pie
        with ca2:
            cat_tot = dm.groupby("Category").size().reset_index(name="FSN Count")
            st.plotly_chart(
                px.pie(cat_tot, values="FSN Count", names="Category",
                       title="Category Share (FSN count)", template="plotly_dark",
                       color_discrete_sequence=PIE_COLORS, hole=0.4),
                use_container_width=True,
            )

        # Category × Sub-category drill
        st.markdown("#### Category → Sub-category Breakdown")
        cat_sub = dm.groupby(["Category", "Sub-category"]).agg(
            FSNs=("FSN", "count"),
        ).reset_index().sort_values(["Category", "FSNs"], ascending=[True, False])

        if has_inv:
            cat_sub_inv = dm.groupby(["Category", "Sub-category"]).agg(
                OOS=("Stock_Health", lambda x: (x == "🔴 OOS").sum()),
                Sellable=("Sellable_Stock", "sum"),
                Sales_30D=("Sales_30D", "sum"),
            ).reset_index()
            cat_sub = cat_sub.merge(cat_sub_inv, on=["Category", "Sub-category"], how="left")

        # Grouped bar - top sub-cats
        top_sub = dm.groupby(["Sub-category", "Category"]).size().reset_index(name="FSNs") \
                    .sort_values("FSNs", ascending=False).head(20)
        fig_sub = px.bar(
            top_sub, x="Sub-category", y="FSNs", color="Category",
            template="plotly_dark", title="Top 20 Sub-categories by FSN Count",
            color_discrete_sequence=PIE_COLORS,
        )
        fig_sub.update_xaxes(tickangle=45)
        st.plotly_chart(fig_sub, use_container_width=True)

        fmt_cat_sub = {"FSNs": "{:,.0f}"}
        if has_inv:
            fmt_cat_sub.update({"OOS": "{:,.0f}", "Sellable": "{:,.0f}", "Sales_30D": "{:,.0f}"})
        render_table(cat_sub.reset_index(drop=True), fmt_cat_sub)

        # Active vs Inactive by Category
        st.markdown("#### Active vs Inactive by Category")
        act_cat = dm.groupby(["Category", "Active/Discontinued"]).size().reset_index(name="Count")
        st.plotly_chart(
            px.bar(act_cat, x="Category", y="Count",
                   color="Active/Discontinued",
                   color_discrete_map={"ACTIVE": "#2ecc71", "INACTIVE": "#e74c3c"},
                   template="plotly_dark",
                   title="Active vs Inactive — Category wise",
                   barmode="group"),
            use_container_width=True,
        )

        # NPD / EPD / Exclusive breakdown
        if "NPD /EPD/ Exclusive" in dm.columns:
            st.markdown("#### NPD / EPD / Exclusive by Category")
            npd_cat = dm.groupby(["Category", "NPD /EPD/ Exclusive"]).size().reset_index(name="FSNs")
            st.plotly_chart(
                px.bar(npd_cat, x="Category", y="FSNs",
                       color="NPD /EPD/ Exclusive",
                       template="plotly_dark",
                       title="EPD vs Exclusives — Category wise",
                       barmode="stack",
                       color_discrete_sequence=PIE_COLORS),
                use_container_width=True,
            )

    # ──────────────────────────────────────────────────────────────────────────
    # TAB 2 — RANGE VIEW
    # ──────────────────────────────────────────────────────────────────────────
    with tab_range:
        st.markdown(
            "<div style='background:rgba(46,204,113,0.08);border:1px solid #2a2a4a;border-radius:10px;"
            "padding:9px 16px;margin:10px 0'>"
            "<span style='font-size:14px;font-weight:700;color:#D7BDE2'>"
            "Range × Category × Brand deep dive</span></div>",
            unsafe_allow_html=True,
        )

        rk1, rk2, rk3 = st.columns(3)
        with rk1: metric_card("Ranges", dm["Range"].nunique(), prefix="", suffix="")
        with rk2: metric_card("Avg SKUs/Range",
                               round(len(dm) / max(dm["Range"].nunique(), 1), 1),
                               prefix="", suffix="")
        with rk3:
            if has_inv:
                oos_ranges = dm[dm["Stock_Health"] == "🔴 OOS"]["Range"].nunique()
                metric_card("Ranges with OOS", oos_ranges, prefix="", suffix="")
            else:
                metric_card("Total SKUs", len(dm), prefix="", suffix="")

        # Range summary table
        range_grp_cols = ["Range", "Brand", "Category"]
        range_agg = dm.groupby(range_grp_cols).agg(
            FSNs=("FSN", "count"),
            Sizes=("Size/Qty", "nunique"),
        ).reset_index()

        if has_inv:
            range_inv = dm.groupby(range_grp_cols).agg(
                Sellable=("Sellable_Stock", "sum"),
                Sales_7D=("Sales_7D", "sum"),
                Sales_30D=("Sales_30D", "sum"),
                OOS=("Stock_Health", lambda x: (x == "🔴 OOS").sum()),
            ).reset_index()
            range_agg = range_agg.merge(range_inv, on=range_grp_cols, how="left")

        # Top ranges bubble / bar
        range_total = (
            dm.groupby("Range").size().reset_index(name="FSNs")
              .sort_values("FSNs", ascending=False).head(25)
        )

        ra1, ra2 = st.columns(2)
        with ra1:
            fig_range = px.bar(
                range_total, x="Range", y="FSNs",
                color="FSNs", color_continuous_scale=["#2a1a4a", "#9B59B6"],
                template="plotly_dark", title="Top Ranges by FSN Count",
            )
            fig_range.update_xaxes(tickangle=45)
            fig_range.update_layout(showlegend=False)
            st.plotly_chart(fig_range, use_container_width=True)

        with ra2:
            # Range × Category heatmap
            rng_cat = dm.groupby(["Range", "Category"]).size().reset_index(name="FSNs")
            rng_pivot = rng_cat.pivot(index="Range", columns="Category", values="FSNs").fillna(0)
            # Limit to top 20 ranges
            top_rng = range_total["Range"].head(20).tolist()
            rng_pivot = rng_pivot.loc[[r for r in top_rng if r in rng_pivot.index]]
            fig_heat_rng = px.imshow(
                rng_pivot,
                color_continuous_scale=["#0a0a14", "#2a1a4a", "#6C3483", "#9B59B6", "#D7BDE2"],
                template="plotly_dark",
                title="Range × Category Heatmap",
                aspect="auto",
            )
            ann = []
            for i, rng in enumerate(rng_pivot.index):
                for j, cat in enumerate(rng_pivot.columns):
                    v = rng_pivot.loc[rng, cat]
                    if v > 0:
                        ann.append(dict(x=j, y=i, text=str(int(v)),
                                        showarrow=False,
                                        font=dict(size=9, color="white")))
            fig_heat_rng.update_layout(annotations=ann,
                                        height=max(300, len(rng_pivot) * 28))
            st.plotly_chart(fig_heat_rng, use_container_width=True)

        # Range × Size/Qty breakdown
        st.markdown("#### Range × Size/Qty breakdown")
        rng_size = dm.groupby(["Range", "Size/Qty"]).size().reset_index(name="FSNs")
        rng_size_top = rng_size[rng_size["Range"].isin(top_rng)]
        fig_rng_size = px.bar(
            rng_size_top, x="Range", y="FSNs", color="Size/Qty",
            template="plotly_dark",
            title="Range — Size/Qty split (Top 20 Ranges)",
            barmode="stack",
            color_discrete_sequence=PIE_COLORS,
        )
        fig_rng_size.update_xaxes(tickangle=45)
        st.plotly_chart(fig_rng_size, use_container_width=True)

        # Full range table with search
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
        render_table(ra_show.sort_values("FSNs", ascending=False).reset_index(drop=True), fmt_range)

    # ──────────────────────────────────────────────────────────────────────────
    # TAB 3 — INVENTORY HEALTH
    # ──────────────────────────────────────────────────────────────────────────
    with tab_inv:
        if not has_inv:
            st.info("Upload Inventory via the sidebar to see stock health. SKU Master is loaded — inventory snapshot missing.")
        else:
            st.markdown(
                "<div style='background:rgba(231,76,60,0.08);border:1px solid #2a2a4a;"
                "border-radius:10px;padding:9px 16px;margin:10px 0'>"
                "<span style='font-size:14px;font-weight:700;color:#D7BDE2'>"
                "Live stock health mapped to SKU Master</span></div>",
                unsafe_allow_html=True,
            )

            # KPIs
            ik1, ik2, ik3, ik4, ik5 = st.columns(5)
            with ik1: metric_card("Total FSNs", len(dm), prefix="", suffix="")
            with ik2: metric_card("🔴 OOS",
                                   len(dm[dm["Stock_Health"] == "🔴 OOS"]),
                                   prefix="", suffix="")
            with ik3: metric_card("🟡 Low (<7d)",
                                   len(dm[dm["Stock_Health"] == "🟡 Low (<7d)"]),
                                   prefix="", suffix="")
            with ik4: metric_card("Sellable Units",
                                   int(dm["Sellable_Stock"].fillna(0).sum()),
                                   prefix="", suffix=" units")
            with ik5: metric_card("Sales 7D",
                                   int(dm["Sales_7D"].fillna(0).sum()),
                                   prefix="", suffix=" units")

            # Health distribution
            ia1, ia2 = st.columns(2)
            with ia1:
                health_cnt = dm["Stock_Health"].value_counts().reset_index()
                health_cnt.columns = ["Status", "Count"]
                cmap = {
                    "🔴 OOS": "#e74c3c",
                    "🟡 Low (<7d)": "#f39c12",
                    "🟠 Medium (7-14d)": "#e67e22",
                    "🟢 Healthy": "#2ecc71",
                }
                st.plotly_chart(
                    px.pie(health_cnt, values="Count", names="Status",
                           title="Stock Health Distribution",
                           template="plotly_dark",
                           color="Status", color_discrete_map=cmap, hole=0.45),
                    use_container_width=True,
                )

            with ia2:
                # Brand × health stacked bar
                bh = dm.groupby(["Brand", "Stock_Health"]).size().reset_index(name="FSNs")
                fig_bh = px.bar(
                    bh, x="Brand", y="FSNs", color="Stock_Health",
                    template="plotly_dark",
                    title="Stock Health by Brand",
                    barmode="stack",
                    color_discrete_map=cmap,
                )
                st.plotly_chart(fig_bh, use_container_width=True)

            # Category × health
            ch_grp = dm.groupby(["Category", "Stock_Health"]).size().reset_index(name="FSNs")
            st.plotly_chart(
                px.bar(ch_grp, x="Category", y="FSNs", color="Stock_Health",
                       barmode="stack", template="plotly_dark",
                       title="Stock Health by Category",
                       color_discrete_map=cmap),
                use_container_width=True,
            )

            # Range × OOS heatmap
            st.markdown("#### Range-wise OOS exposure")
            rng_health = dm.groupby(["Range", "Stock_Health"]).size().reset_index(name="FSNs")
            rng_oos = rng_health[rng_health["Stock_Health"] == "🔴 OOS"].sort_values("FSNs", ascending=False).head(20)
            if not rng_oos.empty:
                fig_rng_oos = px.bar(
                    rng_oos, x="Range", y="FSNs",
                    color="FSNs", color_continuous_scale=["#441a1a", "#e74c3c"],
                    template="plotly_dark",
                    title="Top Ranges with OOS FSNs",
                )
                fig_rng_oos.update_xaxes(tickangle=45)
                st.plotly_chart(fig_rng_oos, use_container_width=True)
            else:
                st.success("No OOS FSNs in selected filters 🎉")

            # OOS drill table — sorted by Sales_30D desc (highest-selling OOS first)
            with st.expander("🔴 OOS FSNs — sorted by 30D sales (act fast on these)"):
                oos_df = dm[dm["Stock_Health"] == "🔴 OOS"].sort_values(
                    "Sales_30D", ascending=False
                )
                oos_cols = [c for c in [
                    "FSN", "Brand", "Category", "Range", "Size/Qty",
                    "Sellable_Stock", "Sales_7D", "Sales_30D",
                    "Orders_Pending", "Listing_Status", "Active/Discontinued"
                ] if c in oos_df.columns]
                render_table(
                    oos_df[oos_cols].reset_index(drop=True),
                    {"Sellable_Stock": "{:,.0f}", "Sales_7D": "{:,.0f}",
                     "Sales_30D": "{:,.0f}", "Orders_Pending": "{:,.0f}"},
                )

            # Low stock drill table
            with st.expander("🟡 Low Stock FSNs (<7 days cover)"):
                low_df = dm[dm["Stock_Health"] == "🟡 Low (<7d)"].sort_values("Days_Cover")
                low_cols = [c for c in [
                    "FSN", "Brand", "Category", "Range", "Size/Qty",
                    "Sellable_Stock", "Days_Cover", "Sales_7D", "Sales_30D",
                    "Inv_Fulfillment", "Active/Discontinued"
                ] if c in low_df.columns]
                render_table(
                    low_df[low_cols].reset_index(drop=True),
                    {"Sellable_Stock": "{:,.0f}", "Days_Cover": "{:.0f}",
                     "Sales_7D": "{:,.0f}", "Sales_30D": "{:,.0f}"},
                )

            # Velocity vs stock scatter
            st.markdown("#### Velocity vs Stock (bubble = Sales 30D)")
            scatter_df = dm[
                dm["Sales_7D"].fillna(0) > 0
            ].copy()
            scatter_df["Sellable_Stock"] = scatter_df["Sellable_Stock"].fillna(0)
            if not scatter_df.empty:
                fig_sc = px.scatter(
                    scatter_df.head(200),
                    x="Sellable_Stock", y="Sales_7D",
                    color="Stock_Health",
                    size="Sales_30D",
                    hover_data=["FSN", "Brand", "Range", "Category"],
                    template="plotly_dark",
                    title="Sellable Stock vs 7D Sales Velocity",
                    color_discrete_map=cmap,
                    labels={"Sellable_Stock": "Sellable Stock (units)", "Sales_7D": "Sales 7D (units)"},
                )
                fig_sc.add_vline(x=0, line_dash="dash", line_color="#e74c3c",
                                  annotation_text="OOS boundary")
                st.plotly_chart(fig_sc, use_container_width=True)

            # Listing status alignment check
            if has_lst:
                st.markdown("#### ⚠️ Inventory vs Listing mismatch")
                mismatch = dm[
                    (dm["Stock_Health"] == "🔴 OOS") &
                    (dm["Listing_Status"].astype(str).str.upper() == "ACTIVE")
                ]
                if not mismatch.empty:
                    st.warning(f"{len(mismatch)} FSNs are OOS in inventory but still ACTIVE on listing!")
                    m_cols = [c for c in [
                        "FSN", "Brand", "Range", "Category",
                        "Sellable_Stock", "Listing_Status", "Inactive_Reason"
                    ] if c in mismatch.columns]
                    render_table(mismatch[m_cols].reset_index(drop=True),
                                 {"Sellable_Stock": "{:,.0f}"})
                else:
                    st.success("No OOS + Active listing mismatch detected ✅")

    # ──────────────────────────────────────────────────────────────────────────
    # TAB 4 — FULL SKU TABLE
    # ──────────────────────────────────────────────────────────────────────────
    with tab_full:
        st.markdown("#### 🔍 Full SKU Table — All master fields + live inventory")

        # Column selector
        all_possible = [c for c in [
            "FSN", "Title", "Brand", "Category", "Sub-category",
            "Range", "Size/Qty", "Vertical", "FULFILMENT TYPE",
            "MRP_Actual", "Master Category", "Gender",
            "NPD /EPD/ Exclusive", "Short Form", "EAN", "SKU ID",
            "Active/Discontinued", "Channel",
            # Inventory
            "Sellable_Stock", "Sales_7D", "Sales_14D", "Sales_30D",
            "Days_Cover", "Stock_Health", "Orders_Pending", "Damaged",
            "Inv_Price", "Warehouses", "Inv_Fulfillment", "F_Assured",
            # Listing
            "Listing_Status", "Inactive_Reason", "Listed_Price",
            "System_Stock", "Fulfillment_By",
        ] if c in dm.columns]

        default_cols = [c for c in [
            "FSN", "Brand", "Category", "Range", "Size/Qty",
            "Active/Discontinued", "Sellable_Stock", "Sales_7D",
            "Sales_30D", "Days_Cover", "Stock_Health", "Listing_Status",
        ] if c in dm.columns]

        chosen_cols = st.multiselect(
            "Choose columns to display",
            options=all_possible,
            default=default_cols,
            key="full_sku_cols",
        )

        # Search
        ft_search = st.text_input("🔍 Search FSN / Title / Range / Brand", key="full_sku_search")
        dm_show = dm[chosen_cols].copy() if chosen_cols else dm.copy()
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
        for c in ["Inv_Price", "Listed_Price", "MRP_Actual"]:
            if c in dm_show.columns:
                fmt_full[c] = "₹{:,.0f}"

        render_table(dm_show.reset_index(drop=True), fmt_full)
        st.caption(f"{len(dm_show):,} SKUs shown")
