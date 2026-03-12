"""
app.py — Streamlit dashboard for Pakistani Supermarket Price Comparison Pipeline.
Run:  streamlit run app.py
"""

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────
BASE = Path(__file__).parent
RAW_DIR = BASE / "data" / "raw"
PROC_DIR = BASE / "data" / "processed"
MATCH_DIR = BASE / "data" / "matched"
CHARTS_DIR = BASE / "analysis" / "charts"

# ── Page config ────────────────────────────────────────────────
st.set_page_config(
    page_title="PK Supermarket Price Comparison",
    page_icon="🛒",
    layout="wide",
)

# ── Load data (cached) ────────────────────────────────────────
@st.cache_data
def load_raw():
    return pd.read_parquet(RAW_DIR / "all_raw_data.parquet")

@st.cache_data
def load_processed():
    return pd.read_parquet(PROC_DIR / "all_processed_data.parquet")

@st.cache_data
def load_matched():
    return pd.read_parquet(MATCH_DIR / "matched_products.parquet")

@st.cache_data
def load_summary():
    return pd.read_parquet(MATCH_DIR / "match_summary.parquet")

raw_df = load_raw()
proc_df = load_processed()
matched_df = load_matched()
summary_df = load_summary()

# ── Sidebar ────────────────────────────────────────────────────
st.sidebar.title("🛒 PK Price Pipeline")
page = st.sidebar.radio("Navigate", [
    "📊 Overview",
    "🔍 Product Search",
    "⚖️ Price Comparison",
    "📈 Analysis Charts",
    "🗂️ Raw Data Explorer",
])

# ── Helper ─────────────────────────────────────────────────────
STORE_COLORS = {
    "metro": "#E63946",
    "alfatah": "#457B9D",
    "naheed": "#2A9D8F",
    "daraz": "#F4A261",
}

# ================================================================
#  PAGE: Overview
# ================================================================
if page == "📊 Overview":
    st.title("Pakistani Supermarket Price Comparison Dashboard")
    st.markdown("Real-time price intelligence across **4 major Pakistani retail chains** and **6 cities**.")

    # KPI row
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Products", f"{len(raw_df):,}")
    c2.metric("Stores", raw_df["store_key"].nunique())
    c3.metric("Cities", raw_df["city"].nunique())
    c4.metric("Match Groups", f"{len(summary_df):,}")
    c5.metric("Matched Rows", f"{len(matched_df):,}")

    st.divider()

    # Store breakdown
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Products by Store")
        store_counts = raw_df.groupby("store_key").size().reset_index(name="rows")
        fig, ax = plt.subplots(figsize=(6, 4))
        bars = ax.bar(store_counts["store_key"], store_counts["rows"],
                      color=[STORE_COLORS.get(s, "#888") for s in store_counts["store_key"]])
        ax.set_ylabel("Number of Products")
        ax.set_title("Raw Products per Store")
        for bar, val in zip(bars, store_counts["rows"]):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 500,
                    f"{val:,}", ha="center", va="bottom", fontsize=9)
        plt.tight_layout()
        st.pyplot(fig)

    with col2:
        st.subheader("Products by City")
        city_counts = raw_df.groupby("city").size().reset_index(name="rows")
        fig2, ax2 = plt.subplots(figsize=(6, 4))
        ax2.barh(city_counts["city"], city_counts["rows"], color="#2A9D8F")
        ax2.set_xlabel("Number of Products")
        ax2.set_title("Products per City")
        plt.tight_layout()
        st.pyplot(fig2)

    # Store-city heatmap
    st.subheader("Store × City Coverage")
    pivot = raw_df.groupby(["store_key", "city"]).size().unstack(fill_value=0)
    fig3, ax3 = plt.subplots(figsize=(10, 4))
    sns.heatmap(pivot, annot=True, fmt=",", cmap="YlGnBu", ax=ax3)
    ax3.set_title("Product Count: Store × City")
    plt.tight_layout()
    st.pyplot(fig3)

    # Matching summary
    st.subheader("Cross-Store Matching Summary")
    st.dataframe(
        summary_df[["product_name", "stores", "min_price", "max_price", "price_range", "num_stores"]]
        .sort_values("price_range", ascending=False)
        .head(20)
        .reset_index(drop=True),
        use_container_width=True,
    )

# ================================================================
#  PAGE: Product Search
# ================================================================
elif page == "🔍 Product Search":
    st.title("🔍 Product Search")
    query = st.text_input("Search product name", placeholder="e.g. Tapal Danedar, Rice, Shampoo...")

    if query:
        mask = proc_df["product_name_clean"].str.contains(query, case=False, na=False)
        results = proc_df[mask][["product_name_clean", "brand", "price", "store_key", "city", "category"]].copy()
        results = results.sort_values("price")

        st.write(f"**{len(results):,}** results found")

        # Filters
        col1, col2 = st.columns(2)
        with col1:
            sel_stores = st.multiselect("Filter by store", results["store_key"].unique(), default=results["store_key"].unique())
        with col2:
            sel_cities = st.multiselect("Filter by city", results["city"].unique(), default=results["city"].unique())

        filtered = results[(results["store_key"].isin(sel_stores)) & (results["city"].isin(sel_cities))]
        st.dataframe(filtered.head(200).reset_index(drop=True), use_container_width=True)

        # Price comparison chart
        if len(filtered) > 0:
            st.subheader("Price by Store")
            fig, ax = plt.subplots(figsize=(8, 4))
            store_avg = filtered.groupby("store_key")["price"].agg(["mean", "min", "max"]).reset_index()
            ax.bar(store_avg["store_key"], store_avg["mean"],
                   color=[STORE_COLORS.get(s, "#888") for s in store_avg["store_key"]])
            ax.set_ylabel("Average Price (Rs.)")
            ax.set_title(f"Average Price for '{query}' by Store")
            plt.tight_layout()
            st.pyplot(fig)

# ================================================================
#  PAGE: Price Comparison
# ================================================================
elif page == "⚖️ Price Comparison":
    st.title("⚖️ Cross-Store Price Comparison")
    st.markdown("Products matched across 2+ stores with price differences.")

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        min_range = st.slider("Minimum price range (Rs.)", 0, int(summary_df["price_range"].max()), 0)
    with col2:
        min_stores = st.selectbox("Minimum stores", [2, 3, 4], index=0)

    filtered_summary = summary_df[
        (summary_df["price_range"] >= min_range) &
        (summary_df["num_stores"] >= min_stores)
    ].sort_values("price_range", ascending=False)

    st.write(f"**{len(filtered_summary):,}** matched products")
    st.dataframe(
        filtered_summary[["product_name", "brand", "stores", "min_price", "max_price", "price_range", "avg_price"]]
        .reset_index(drop=True)
        .head(100),
        use_container_width=True,
    )

    # Top price differences chart
    st.subheader("Top 15 Biggest Price Differences")
    top15 = filtered_summary.head(15)
    if len(top15) > 0:
        fig, ax = plt.subplots(figsize=(10, 6))
        names = [n[:40] for n in top15["product_name"]]
        ax.barh(names, top15["price_range"], color="#E63946")
        ax.set_xlabel("Price Range (Rs.)")
        ax.set_title("Top 15 Products with Highest Price Disparity")
        ax.invert_yaxis()
        plt.tight_layout()
        st.pyplot(fig)

    # Store price leadership
    st.subheader("Store Price Leadership")
    st.markdown("Which store offers the lowest price most often?")

    cheapest_counts = {}
    for _, row in matched_df.iterrows():
        grp = row.get("match_group")
        if pd.isna(grp):
            continue
        grp_data = matched_df[matched_df["match_group"] == grp]
        cheapest_store = grp_data.loc[grp_data["price"].idxmin(), "store_key"]
        cheapest_counts[cheapest_store] = cheapest_counts.get(cheapest_store, 0) + 1

    if cheapest_counts:
        fig2, ax2 = plt.subplots(figsize=(6, 4))
        stores = list(cheapest_counts.keys())
        counts = list(cheapest_counts.values())
        ax2.bar(stores, counts, color=[STORE_COLORS.get(s, "#888") for s in stores])
        ax2.set_ylabel("Times Cheapest")
        ax2.set_title("How Often Each Store Has the Lowest Price")
        plt.tight_layout()
        st.pyplot(fig2)

# ================================================================
#  PAGE: Analysis Charts
# ================================================================
elif page == "📈 Analysis Charts":
    st.title("📈 Analysis Charts (Sections 3.1–3.4)")

    chart_files = sorted(CHARTS_DIR.glob("*.png"))
    if not chart_files:
        st.warning("No charts found. Run the analysis pipeline first.")
    else:
        # Group charts by section
        sections = {
            "3.1 — Price Dispersion": ["cv_distribution", "price_spread_ratio", "category_cv"],
            "3.2 — Price Leadership": ["price_leadership_pie", "ldi_comparison", "category_ldi_heatmap"],
            "3.3 — City Comparison": ["city_avg_price", "store_city_price_index_heatmap"],
            "3.4 — Correlation Analysis": ["city_price_correlation", "cross_store_sync", "corr_brand_tier", "corr_competitors", "corr_size"],
        }

        for section_name, keywords in sections.items():
            st.subheader(section_name)
            cols = st.columns(min(len(keywords), 3))
            col_idx = 0
            for chart_path in chart_files:
                if any(kw in chart_path.stem for kw in keywords):
                    with cols[col_idx % len(cols)]:
                        st.image(str(chart_path), caption=chart_path.stem.replace("_", " ").title(), use_container_width=True)
                    col_idx += 1

# ================================================================
#  PAGE: Raw Data Explorer
# ================================================================
elif page == "🗂️ Raw Data Explorer":
    st.title("🗂️ Raw Data Explorer")

    tab1, tab2, tab3 = st.tabs(["Raw Data", "Processed Data", "Matched Data"])

    with tab1:
        st.write(f"**{len(raw_df):,}** rows | **{raw_df.columns.size}** columns")
        sel_store = st.selectbox("Store", ["All"] + sorted(raw_df["store_key"].unique()), key="raw_store")
        display = raw_df if sel_store == "All" else raw_df[raw_df["store_key"] == sel_store]
        st.dataframe(display.head(500).reset_index(drop=True), use_container_width=True)

    with tab2:
        st.write(f"**{len(proc_df):,}** rows")
        st.dataframe(proc_df.head(500).reset_index(drop=True), use_container_width=True)

    with tab3:
        st.write(f"**{len(matched_df):,}** rows | **{matched_df['match_group'].nunique()}** match groups")
        st.dataframe(matched_df.head(500).reset_index(drop=True), use_container_width=True)

# ── Footer ────────────────────────────────────────
st.sidebar.divider()
st.sidebar.caption("Built with Streamlit | Data scraped from Metro, Al-Fatah, Naheed & Daraz")
