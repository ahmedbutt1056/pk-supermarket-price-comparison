"""
price_analysis.py — Phase 7: Complete price dispersion, market structure,
                    competition, and correlation analysis.

Implements ALL required analyses from the assignment:
  3.1  Price Dispersion Metrics (per matched product)
  3.2  Store-Level Aggregated Metrics
  3.3  Leader Dominance Index (LDI)
  3.4  Correlation & Competition Analysis

Run from project root:
    python analysis/price_analysis.py
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from utils.logger_setup import get_logger

log = get_logger("analysis")

# output folders
CHARTS_DIR = config.BASE_DIR / "analysis" / "charts"
CHARTS_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR = config.BASE_DIR / "analysis" / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# ==============================================================
# 1. LOAD DATA
# ==============================================================

def load_data():
    """Load matched products and full processed data."""
    matched_path = config.MATCHED_DIR / "matched_products.parquet"
    full_path = config.PROCESSED_DIR / "all_processed_data.parquet"

    if not matched_path.exists():
        log.error("Matched data not found! Run matching first.")
        return None, None

    matched = pd.read_parquet(matched_path, engine="pyarrow")
    full = pd.read_parquet(full_path, engine="pyarrow") if full_path.exists() else None

    log.info(f"Loaded {len(matched)} matched rows")
    if full is not None:
        log.info(f"Loaded {len(full)} total processed rows")

    return matched, full


# ================================================================
# 3.1  PRICE DISPERSION METRICS (per matched product group)
# ================================================================

def price_dispersion_metrics(matched):
    """
    For each matched product across stores and cities, compute:
      - Mean Price
      - Median Price
      - Standard Deviation
      - Coefficient of Variation (CV = std/mean)
      - Price Range (max - min)
      - Interquartile Range (IQR)
      - Price Spread Ratio (max / min)
      - Relative Price Position Index per store (store_price / category_mean)
    """
    log.info("=" * 60)
    log.info("3.1  PRICE DISPERSION METRICS")
    log.info("=" * 60)

    groups = matched.groupby("match_group").agg(
        product=("product_name_clean", "first"),
        brand=("brand", "first"),
        category=("category", "first"),
        num_stores=("store_key", "nunique"),
        num_cities=("city", "nunique"),
        num_obs=("price", "count"),
        mean_price=("price", "mean"),
        median_price=("price", "median"),
        std_price=("price", "std"),
        min_price=("price", "min"),
        max_price=("price", "max"),
    ).reset_index()

    # Compute quantiles separately (much faster than lambda in agg)
    q25 = matched.groupby("match_group")["price"].quantile(0.25).rename("q25")
    q75 = matched.groupby("match_group")["price"].quantile(0.75).rename("q75")
    groups = groups.merge(q25, on="match_group").merge(q75, on="match_group")

    # Coefficient of Variation (CV = std / mean * 100)
    groups["cv"] = (groups["std_price"] / groups["mean_price"] * 100).round(2)

    # Price Range (max - min)
    groups["price_range"] = (groups["max_price"] - groups["min_price"]).round(2)

    # Interquartile Range (IQR)
    groups["iqr"] = (groups["q75"] - groups["q25"]).round(2)

    # Price Spread Ratio (max / min)
    groups["price_spread_ratio"] = np.where(
        groups["min_price"] > 0,
        (groups["max_price"] / groups["min_price"]).round(3),
        np.nan
    )

    # Save
    path = RESULTS_DIR / "price_dispersion.xlsx"
    groups.to_excel(path, index=False, engine="openpyxl")
    log.info(f"Saved price dispersion: {path} ({len(groups)} groups)")

    log.info(f"\n  === PRICE DISPERSION SUMMARY ===")
    log.info(f"  Total matched groups:        {len(groups)}")
    log.info(f"  Mean CV:                     {groups['cv'].mean():.2f}%")
    log.info(f"  Median CV:                   {groups['cv'].median():.2f}%")
    log.info(f"  Mean Price Range:            Rs. {groups['price_range'].mean():.0f}")
    log.info(f"  Max Price Range:             Rs. {groups['price_range'].max():.0f}")
    log.info(f"  Mean IQR:                    Rs. {groups['iqr'].mean():.0f}")
    log.info(f"  Mean Price Spread Ratio:     {groups['price_spread_ratio'].mean():.3f}")
    log.info(f"  Groups with CV > 10%:        {(groups['cv'] > 10).sum()}")
    log.info(f"  Groups with CV > 20%:        {(groups['cv'] > 20).sum()}")

    # --- Relative Price Position Index (RPPI) per store ---
    log.info("\n  Computing Relative Price Position Index (RPPI) per store...")
    cat_means = matched.groupby("category")["price"].mean().rename("category_mean")
    rppi_df = matched.merge(cat_means, on="category", how="left")
    rppi_df["rppi"] = (rppi_df["price"] / rppi_df["category_mean"]).round(4)

    rppi_summary = rppi_df.groupby("store_key")["rppi"].agg(
        ["mean", "median", "std"]
    ).round(4).reset_index()
    rppi_summary.columns = ["store_key", "rppi_mean", "rppi_median", "rppi_std"]

    rppi_path = RESULTS_DIR / "relative_price_position_index.xlsx"
    rppi_summary.to_excel(rppi_path, index=False, engine="openpyxl")
    log.info(f"  Saved RPPI: {rppi_path}")
    for _, row in rppi_summary.iterrows():
        log.info(f"    {row['store_key']:12s}: RPPI mean={row['rppi_mean']:.4f}, "
                 f"median={row['rppi_median']:.4f}")

    # --- Charts ---
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(groups["cv"].dropna(), bins=50, color="#42A5F5", edgecolor="white")
    ax.set_title("Distribution of Price Dispersion (CV%)", fontsize=14)
    ax.set_xlabel("Coefficient of Variation (%)")
    ax.set_ylabel("Number of Products")
    ax.axvline(groups["cv"].mean(), color="red", linestyle="--",
               label=f"Mean CV = {groups['cv'].mean():.1f}%")
    ax.legend()
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "cv_distribution.png", dpi=150)
    plt.close()

    fig, ax = plt.subplots(figsize=(10, 5))
    spread = groups["price_spread_ratio"].dropna()
    if len(spread) > 0:
        ax.hist(spread[spread < spread.quantile(0.99)], bins=50,
                color="#66BB6A", edgecolor="white")
        ax.set_title("Distribution of Price Spread Ratio (max/min)", fontsize=14)
        ax.set_xlabel("Price Spread Ratio")
        ax.set_ylabel("Number of Products")
        ax.axvline(spread.mean(), color="red", linestyle="--",
                   label=f"Mean = {spread.mean():.2f}")
        ax.legend()
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "price_spread_ratio_dist.png", dpi=150)
    plt.close()

    log.info("  Charts saved: cv_distribution.png, price_spread_ratio_dist.png")

    return groups, rppi_df


# ================================================================
# 3.2  STORE-LEVEL AGGREGATED METRICS
# ================================================================

def store_level_metrics(matched, dispersion_df):
    """
    For each store and city compute:
      - Average category price index
      - Median price deviation from market average
      - Price Volatility Score (average CV across products)
      - Price Leadership Frequency (% of times store has lowest price)
    """
    log.info("\n" + "=" * 60)
    log.info("3.2  STORE-LEVEL AGGREGATED METRICS")
    log.info("=" * 60)

    results = []

    # market average by category
    cat_market_avg = matched.groupby("category")["price"].mean().rename("market_avg")

    # Precompute cheapest store per match_group for leadership frequency
    cheapest_by_group = matched.loc[matched.groupby("match_group")["price"].idxmin()]

    stores_cities = matched.groupby(["store_key", "city"])

    for (store, city), grp in stores_cities:
        # Average category price index
        store_cat_avg = grp.groupby("category")["price"].mean()
        merged = store_cat_avg.to_frame("store_avg").join(cat_market_avg, how="inner")
        merged["price_index"] = merged["store_avg"] / merged["market_avg"]
        avg_cat_price_index = merged["price_index"].mean()

        # Median price deviation from market average
        grp_with_mkt = grp.merge(cat_market_avg, on="category", how="left")
        grp_with_mkt["deviation"] = grp_with_mkt["price"] - grp_with_mkt["market_avg"]
        median_price_deviation = grp_with_mkt["deviation"].median()

        # Price Volatility Score: average CV of matched products this store sells
        store_groups = grp["match_group"].dropna().unique()
        store_cv = dispersion_df[dispersion_df["match_group"].isin(store_groups)]["cv"]
        volatility_score = store_cv.mean() if len(store_cv) > 0 else 0

        # Price Leadership Frequency (precomputed)
        lowest_count = cheapest_by_group[
            (cheapest_by_group["store_key"] == store) &
            (cheapest_by_group["match_group"].isin(store_groups))
        ].shape[0]
        total_groups = len(store_groups)
        leadership_freq = (lowest_count / total_groups * 100) if total_groups > 0 else 0

        results.append({
            "store_key": store,
            "city": city,
            "avg_category_price_index": round(avg_cat_price_index, 4),
            "median_price_deviation": round(median_price_deviation, 2),
            "price_volatility_score": round(volatility_score, 2),
            "price_leadership_frequency_pct": round(leadership_freq, 2),
            "products_count": len(grp),
        })

    store_metrics = pd.DataFrame(results)

    path = RESULTS_DIR / "store_level_metrics.xlsx"
    store_metrics.to_excel(path, index=False, engine="openpyxl")
    log.info(f"Saved store-level metrics: {path}")

    for _, r in store_metrics.iterrows():
        log.info(f"  {r['store_key']:12s} / {r['city']:12s}: "
                 f"PriceIdx={r['avg_category_price_index']:.3f}, "
                 f"Dev=Rs.{r['median_price_deviation']:.0f}, "
                 f"Vol={r['price_volatility_score']:.1f}%, "
                 f"Lead={r['price_leadership_frequency_pct']:.1f}%")

    # --- Charts ---
    store_avg = matched.groupby("store_key").agg(
        avg_price=("price", "mean"),
        median_price=("price", "median"),
        total_products=("price", "count"),
    ).reset_index()

    fig, ax = plt.subplots(figsize=(10, 5))
    colors = sns.color_palette("Set2", len(store_avg))
    bars = ax.bar(store_avg["store_key"], store_avg["avg_price"], color=colors)
    ax.set_title("Average Price by Store (Matched Products)", fontsize=14)
    ax.set_ylabel("Average Price (PKR)")
    ax.set_xlabel("Store")
    for bar, val in zip(bars, store_avg["avg_price"]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5,
                f"Rs.{val:.0f}", ha="center", fontsize=9)
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "store_avg_price.png", dpi=150)
    plt.close()

    city_avg = matched.groupby("city").agg(
        avg_price=("price", "mean"),
        median_price=("price", "median"),
        total_products=("price", "count"),
    ).reset_index()

    fig, ax = plt.subplots(figsize=(10, 5))
    colors = sns.color_palette("Pastel1", len(city_avg))
    bars = ax.bar(city_avg["city"], city_avg["avg_price"], color=colors)
    ax.set_title("Average Price by City", fontsize=14)
    ax.set_ylabel("Average Price (PKR)")
    ax.set_xlabel("City")
    for bar, val in zip(bars, city_avg["avg_price"]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5,
                f"Rs.{val:.0f}", ha="center", fontsize=9)
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "city_avg_price.png", dpi=150)
    plt.close()

    cat_stats = dispersion_df.groupby("category").agg(
        avg_cv=("cv", "mean"),
        avg_range=("price_range", "mean"),
        num_products=("product", "count"),
    ).reset_index().sort_values("avg_cv", ascending=False)

    cat_path = RESULTS_DIR / "category_dispersion.xlsx"
    cat_stats.to_excel(cat_path, index=False, engine="openpyxl")

    fig, ax = plt.subplots(figsize=(12, max(6, len(cat_stats) * 0.35)))
    ax.barh(cat_stats["category"], cat_stats["avg_cv"], color="#FF7043")
    ax.set_title("Average Price CV% by Category", fontsize=14)
    ax.set_xlabel("Coefficient of Variation (%)")
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "category_cv.png", dpi=150)
    plt.close()

    # store-city heatmap
    pivot = store_metrics.pivot_table(
        index="store_key", columns="city",
        values="avg_category_price_index", aggfunc="first"
    )
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.heatmap(pivot, annot=True, fmt=".3f", cmap="RdYlGn_r", ax=ax)
    ax.set_title("Category Price Index by Store x City", fontsize=14)
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "store_city_price_index_heatmap.png", dpi=150)
    plt.close()

    log.info("  Charts saved: store_avg_price, city_avg_price, category_cv, "
             "store_city_price_index_heatmap")

    return store_metrics


# ================================================================
# 3.3  LEADER DOMINANCE INDEX (LDI)
# ================================================================

def leader_dominance_index(matched):
    """
    LDI_store = (Number of products where store has lowest price)
              / (Total matched products)

    Also compute:
      - Weighted LDI (weighted by category size)
      - Category-wise LDI
    """
    log.info("\n" + "=" * 60)
    log.info("3.3  LEADER DOMINANCE INDEX (LDI)")
    log.info("=" * 60)

    # --- Basic LDI ---
    cheapest = matched.loc[matched.groupby("match_group")["price"].idxmin()]
    total_groups = matched["match_group"].nunique()

    leader_counts = cheapest["store_key"].value_counts()
    ldi_basic = (leader_counts / total_groups).round(4)

    ldi_df = pd.DataFrame({
        "store_key": ldi_basic.index,
        "lowest_price_count": leader_counts.values,
        "total_matched_groups": total_groups,
        "LDI": ldi_basic.values,
    })

    log.info(f"\n  === BASIC LDI ===")
    log.info(f"  Formula: LDI_store = (products where store has lowest price) "
             f"/ (total matched products)")
    log.info(f"  Total matched product groups: {total_groups}")
    for _, row in ldi_df.iterrows():
        log.info(f"    {row['store_key']:12s}: LDI = {row['LDI']:.4f} "
                 f"(cheapest in {row['lowest_price_count']} / {total_groups})")

    # --- Weighted LDI (weighted by category size) ---
    log.info("\n  Computing Weighted LDI (weighted by category size)...")
    # Precompute category of each match_group
    group_cat = matched.groupby("match_group")["category"].first()
    cheapest_with_cat = cheapest.copy()
    cheapest_with_cat["category"] = cheapest_with_cat["match_group"].map(group_cat)
    
    cat_sizes = cheapest_with_cat["category"].value_counts()
    total_weighted = cat_sizes.sum()
    
    cat_store_counts = cheapest_with_cat.groupby(["category", "store_key"]).size().reset_index(name="count")
    cat_store_counts["cat_total"] = cat_store_counts["category"].map(cat_sizes)
    cat_store_counts["weight"] = cat_store_counts["cat_total"] / total_weighted
    cat_store_counts["weighted_contrib"] = (cat_store_counts["count"] / cat_store_counts["cat_total"]) * cat_store_counts["weight"]
    
    weighted_ldi = cat_store_counts.groupby("store_key")["weighted_contrib"].sum().round(4).to_dict()

    ldi_df["Weighted_LDI"] = ldi_df["store_key"].map(weighted_ldi)

    log.info("  Weighted LDI:")
    for store, wldi in sorted(weighted_ldi.items(), key=lambda x: -x[1]):
        log.info(f"    {store:12s}: Weighted LDI = {wldi:.4f}")

    # --- Category-wise LDI ---
    log.info("\n  Computing Category-wise LDI...")
    # Use precomputed cheapest_with_cat
    cat_store_ldi = cheapest_with_cat.groupby(["category", "store_key"]).size().reset_index(name="cheapest_count")
    cat_totals = cheapest_with_cat.groupby("category").size().rename("total_in_category")
    cat_store_ldi = cat_store_ldi.merge(cat_totals, on="category")
    cat_store_ldi["category_LDI"] = (cat_store_ldi["cheapest_count"] / cat_store_ldi["total_in_category"]).round(4)

    # Add missing store-category combinations with 0
    all_stores = matched["store_key"].unique()
    all_cats = matched["category"].unique()
    full_index = pd.MultiIndex.from_product([all_cats, all_stores], names=["category", "store_key"])
    cat_ldi_df = cat_store_ldi.set_index(["category", "store_key"]).reindex(full_index, fill_value=0).reset_index()
    if "total_in_category" not in cat_ldi_df.columns or cat_ldi_df["total_in_category"].isna().any():
        cat_ldi_df = cat_ldi_df.merge(cat_totals, on="category", how="left", suffixes=("_old", ""))
        if "total_in_category_old" in cat_ldi_df.columns:
            cat_ldi_df["total_in_category"] = cat_ldi_df["total_in_category"].fillna(cat_ldi_df["total_in_category_old"])
            cat_ldi_df.drop(columns=["total_in_category_old"], inplace=True)

    # Save
    path = RESULTS_DIR / "leader_dominance_index.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        ldi_df.to_excel(writer, sheet_name="Basic_LDI", index=False)
        cat_ldi_df.to_excel(writer, sheet_name="Category_LDI", index=False)
    log.info(f"  Saved LDI: {path}")

    # --- Charts ---
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    colors = sns.color_palette("Set2", len(ldi_df))

    axes[0].bar(ldi_df["store_key"], ldi_df["LDI"], color=colors)
    axes[0].set_title("Leader Dominance Index (LDI) by Store", fontsize=13)
    axes[0].set_ylabel("LDI")
    axes[0].set_xlabel("Store")
    for i, val in enumerate(ldi_df["LDI"]):
        axes[0].text(i, val + 0.005, f"{val:.3f}", ha="center", fontsize=9)

    w_sorted = ldi_df.sort_values("Weighted_LDI", ascending=False)
    axes[1].bar(w_sorted["store_key"], w_sorted["Weighted_LDI"], color=colors)
    axes[1].set_title("Weighted LDI (by Category Size)", fontsize=13)
    axes[1].set_ylabel("Weighted LDI")
    axes[1].set_xlabel("Store")
    for i, (_, row) in enumerate(w_sorted.iterrows()):
        axes[1].text(i, row["Weighted_LDI"] + 0.005,
                     f"{row['Weighted_LDI']:.3f}", ha="center", fontsize=9)

    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "ldi_comparison.png", dpi=150)
    plt.close()

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.pie(ldi_df["lowest_price_count"], labels=ldi_df["store_key"],
           autopct="%1.1f%%", colors=sns.color_palette("Set2", len(ldi_df)),
           startangle=90)
    ax.set_title("Price Leadership — Which Store is Cheapest Most Often?", fontsize=13)
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "price_leadership_pie.png", dpi=150)
    plt.close()

    cat_ldi_pivot = cat_ldi_df.pivot_table(
        index="category", columns="store_key", values="category_LDI"
    ).fillna(0)

    fig, ax = plt.subplots(figsize=(12, max(8, len(cat_ldi_pivot) * 0.4)))
    sns.heatmap(cat_ldi_pivot, annot=True, fmt=".2f", cmap="YlOrRd", ax=ax)
    ax.set_title("Category-wise LDI (Price Leadership by Category)", fontsize=14)
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / "category_ldi_heatmap.png", dpi=150)
    plt.close()

    log.info("  Charts: ldi_comparison, price_leadership_pie, category_ldi_heatmap")

    return ldi_df, cat_ldi_df


# ================================================================
# 3.4  CORRELATION & COMPETITION ANALYSIS
# ================================================================

def correlation_analysis(matched, dispersion_df):
    """
    1. Correlation between product size and price dispersion
    2. Correlation between number of competitors and price spread
    3. Correlation between brand tier (premium vs economy) and price volatility
    4. City-wise price correlation matrix (Pearson and Spearman)
    5. Cross-store price synchronization score
    """
    log.info("\n" + "=" * 60)
    log.info("3.4  CORRELATION & COMPETITION ANALYSIS")
    log.info("=" * 60)

    corr_results = {}

    # ---- 1. Product size vs price dispersion ----
    log.info("\n  1) Correlation: Product Size vs Price Dispersion")
    qty_info = matched.groupby("match_group").agg(
        qty_normalized=("qty_normalized", "first"),
    ).reset_index()
    size_disp = dispersion_df.merge(qty_info, on="match_group", how="left")
    valid = size_disp.dropna(subset=["qty_normalized", "cv"])

    if len(valid) > 10:
        r_p, p_p = stats.pearsonr(valid["qty_normalized"], valid["cv"])
        r_s, p_s = stats.spearmanr(valid["qty_normalized"], valid["cv"])
        corr_results["size_vs_cv_pearson"] = round(r_p, 4)
        corr_results["size_vs_cv_spearman"] = round(r_s, 4)
        log.info(f"    Pearson r  = {r_p:.4f} (p={p_p:.4e})")
        log.info(f"    Spearman r = {r_s:.4f} (p={p_s:.4e})")
        if abs(r_p) < 0.1:
            log.info("    -> No significant correlation between size and dispersion")
        elif r_p < 0:
            log.info("    -> Larger products tend to have LOWER price dispersion")
        else:
            log.info("    -> Larger products tend to have HIGHER price dispersion")

        fig, ax = plt.subplots(figsize=(8, 5))
        ax.scatter(valid["qty_normalized"], valid["cv"], alpha=0.3, s=10)
        ax.set_xlabel("Product Size (normalized)")
        ax.set_ylabel("Coefficient of Variation (%)")
        ax.set_title(f"Product Size vs Price Dispersion (r={r_p:.3f})")
        plt.tight_layout()
        plt.savefig(CHARTS_DIR / "corr_size_vs_cv.png", dpi=150)
        plt.close()
    else:
        log.warning("    Insufficient data for size-dispersion correlation")

    # ---- 2. Number of competitors vs price spread ----
    log.info("\n  2) Correlation: Number of Competitors vs Price Spread")
    comp = dispersion_df[["match_group", "num_stores", "price_range", "cv",
                          "price_spread_ratio"]].dropna()
    if len(comp) > 10:
        r_p, p_p = stats.pearsonr(comp["num_stores"], comp["price_range"])
        r_s, p_s = stats.spearmanr(comp["num_stores"], comp["price_range"])
        corr_results["competitors_vs_spread_pearson"] = round(r_p, 4)
        corr_results["competitors_vs_spread_spearman"] = round(r_s, 4)
        log.info(f"    Pearson r  = {r_p:.4f} (p={p_p:.4e})")
        log.info(f"    Spearman r = {r_s:.4f} (p={p_s:.4e})")
        if r_p > 0.1:
            log.info("    -> More competitors = wider price spread")
        elif r_p < -0.1:
            log.info("    -> More competitors = narrower spread (convergence)")
        else:
            log.info("    -> No significant correlation")

        fig, ax = plt.subplots(figsize=(8, 5))
        sns.boxplot(x="num_stores", y="price_range", data=comp, ax=ax)
        ax.set_title(f"Competitors vs Price Range (r={r_p:.3f})")
        ax.set_xlabel("Number of Stores Selling Product")
        ax.set_ylabel("Price Range (PKR)")
        plt.tight_layout()
        plt.savefig(CHARTS_DIR / "corr_competitors_vs_spread.png", dpi=150)
        plt.close()
    else:
        log.warning("    Insufficient data")

    # ---- 3. Brand tier vs price volatility ----
    log.info("\n  3) Correlation: Brand Tier vs Price Volatility")
    brand_avg = matched.groupby("brand")["price"].mean()
    q33 = brand_avg.quantile(0.33)
    q66 = brand_avg.quantile(0.66)

    def brand_tier(brand):
        avg = brand_avg.get(brand)
        if avg is None:
            return "unknown"
        return "economy" if avg <= q33 else ("mid-range" if avg <= q66 else "premium")

    tier_disp = dispersion_df.copy()
    tier_disp["brand_tier"] = tier_disp["brand"].apply(brand_tier)
    tier_valid = tier_disp[tier_disp["brand_tier"] != "unknown"].copy()

    if len(tier_valid) > 10:
        tier_map = {"economy": 1, "mid-range": 2, "premium": 3}
        tier_valid["tier_numeric"] = tier_valid["brand_tier"].map(tier_map)
        r_p, p_p = stats.pearsonr(tier_valid["tier_numeric"], tier_valid["cv"])
        r_s, p_s = stats.spearmanr(tier_valid["tier_numeric"], tier_valid["cv"])
        corr_results["brand_tier_vs_cv_pearson"] = round(r_p, 4)
        corr_results["brand_tier_vs_cv_spearman"] = round(r_s, 4)
        log.info(f"    Pearson r  = {r_p:.4f} (p={p_p:.4e})")
        log.info(f"    Spearman r = {r_s:.4f} (p={p_s:.4e})")

        tier_stats = tier_valid.groupby("brand_tier")["cv"].agg(
            ["mean", "median", "std"]).round(2)
        log.info(f"    Tier stats:\n{tier_stats}")

        fig, ax = plt.subplots(figsize=(8, 5))
        sns.boxplot(x="brand_tier", y="cv", data=tier_valid,
                    order=["economy", "mid-range", "premium"], ax=ax)
        ax.set_title(f"Brand Tier vs Price Volatility (r={r_p:.3f})")
        ax.set_xlabel("Brand Tier")
        ax.set_ylabel("Coefficient of Variation (%)")
        plt.tight_layout()
        plt.savefig(CHARTS_DIR / "corr_brand_tier_vs_cv.png", dpi=150)
        plt.close()
    else:
        log.warning("    Insufficient data")

    # ---- 4. City-wise price correlation matrix ----
    log.info("\n  4) City-wise Price Correlation Matrix")
    city_pivot = matched.pivot_table(
        index="match_group", columns="city", values="price", aggfunc="mean"
    ).dropna(thresh=2)

    if len(city_pivot) > 10 and city_pivot.shape[1] >= 2:
        pearson_corr = city_pivot.corr(method="pearson").round(4)
        spearman_corr = city_pivot.corr(method="spearman").round(4)
        log.info(f"    Pearson:\n{pearson_corr}")
        log.info(f"    Spearman:\n{spearman_corr}")

        corr_path = RESULTS_DIR / "city_price_correlation.xlsx"
        with pd.ExcelWriter(corr_path, engine="openpyxl") as writer:
            pearson_corr.to_excel(writer, sheet_name="Pearson")
            spearman_corr.to_excel(writer, sheet_name="Spearman")

        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        sns.heatmap(pearson_corr, annot=True, fmt=".3f", cmap="coolwarm",
                    vmin=-1, vmax=1, ax=axes[0])
        axes[0].set_title("City Price Correlation (Pearson)")
        sns.heatmap(spearman_corr, annot=True, fmt=".3f", cmap="coolwarm",
                    vmin=-1, vmax=1, ax=axes[1])
        axes[1].set_title("City Price Correlation (Spearman)")
        plt.tight_layout()
        plt.savefig(CHARTS_DIR / "city_price_correlation_matrix.png", dpi=150)
        plt.close()
    else:
        log.warning("    Insufficient cross-city data")

    # ---- 5. Cross-store price synchronization score ----
    log.info("\n  5) Cross-Store Price Synchronization Score")
    store_pivot = matched.pivot_table(
        index="match_group", columns="store_key", values="price", aggfunc="mean"
    ).dropna(thresh=2)

    if len(store_pivot) > 10 and store_pivot.shape[1] >= 2:
        store_corr = store_pivot.corr(method="pearson").round(4)
        log.info(f"    Store correlation:\n{store_corr}")

        upper_tri = store_corr.where(
            np.triu(np.ones(store_corr.shape), k=1).astype(bool)
        )
        sync_score = upper_tri.stack().mean()
        corr_results["cross_store_sync_score"] = round(sync_score, 4)
        log.info(f"    SYNCHRONIZATION SCORE = {sync_score:.4f}")
        log.info(f"    (1.0 = perfectly synchronized, 0.0 = no correlation)")

        for i in range(len(store_corr)):
            for j in range(i + 1, len(store_corr)):
                s1, s2 = store_corr.index[i], store_corr.columns[j]
                log.info(f"    {s1} <-> {s2}: r = {store_corr.iloc[i, j]:.4f}")

        sync_path = RESULTS_DIR / "cross_store_synchronization.xlsx"
        store_corr.to_excel(sync_path, engine="openpyxl")

        fig, ax = plt.subplots(figsize=(8, 6))
        sns.heatmap(store_corr, annot=True, fmt=".3f", cmap="coolwarm",
                    vmin=-1, vmax=1, ax=ax)
        ax.set_title(f"Cross-Store Price Synchronization (Score={sync_score:.3f})")
        plt.tight_layout()
        plt.savefig(CHARTS_DIR / "cross_store_sync_heatmap.png", dpi=150)
        plt.close()
    else:
        log.warning("    Insufficient cross-store data")

    # Save correlation summary
    corr_summary = pd.DataFrame([corr_results])
    corr_summary.to_excel(RESULTS_DIR / "correlation_summary.xlsx",
                          index=False, engine="openpyxl")

    return corr_results


# ================================================================
# EXTRA: Top price differences
# ================================================================

def top_price_diffs(dispersion_df):
    """Top 20 products with biggest price differences."""
    log.info("\n  Top price differences...")
    top = dispersion_df.nlargest(20, "price_range")[[
        "product", "brand", "category", "num_stores",
        "min_price", "max_price", "price_range", "cv",
        "iqr", "price_spread_ratio"
    ]]
    path = RESULTS_DIR / "top_price_differences.xlsx"
    top.to_excel(path, index=False, engine="openpyxl")
    log.info(f"  Saved: {path}")
    for _, row in top.head(10).iterrows():
        log.info(f"    {row['product'][:40]:40s} | Range=Rs.{row['price_range']:.0f} "
                 f"| CV={row['cv']:.1f}% | Spread={row['price_spread_ratio']:.2f}x")
    return top


# ==============================================================
# MAIN
# ==============================================================

def main():
    log.info("=" * 60)
    log.info("STARTING COMPREHENSIVE PRICE ANALYSIS")
    log.info("  Sections: 3.1, 3.2, 3.3, 3.4")
    log.info("=" * 60)

    matched, full = load_data()
    if matched is None:
        return

    # 3.1 Price Dispersion Metrics
    dispersion, rppi_df = price_dispersion_metrics(matched)

    # 3.2 Store-Level Aggregated Metrics
    store_metrics = store_level_metrics(matched, dispersion)

    # 3.3 Leader Dominance Index
    ldi_df, cat_ldi_df = leader_dominance_index(matched)

    # 3.4 Correlation & Competition Analysis
    corr_results = correlation_analysis(matched, dispersion)

    # Extra: Top differences
    top_price_diffs(dispersion)

    log.info("\n" + "=" * 60)
    log.info("ANALYSIS COMPLETE — All sections 3.1-3.4 computed")
    log.info(f"  Results: {RESULTS_DIR}")
    log.info(f"  Charts:  {CHARTS_DIR}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
