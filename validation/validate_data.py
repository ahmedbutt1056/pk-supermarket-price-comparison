"""
validate_data.py — Phase 8: Data validation and quality checks.

This script checks:
1. Row counts at each layer (raw, processed, matched)
2. Missing values summary
3. Duplicate detection
4. Price range sanity
5. Store/city coverage
6. Matching quality

Run from project root:
    python validation/validate_data.py
"""

import pandas as pd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from utils.logger_setup import get_logger

log = get_logger("validation")


def print_header(title):
    log.info("")
    log.info("=" * 50)
    log.info(f"  {title}")
    log.info("=" * 50)


def check_row_counts():
    """Check row counts at each pipeline layer."""
    print_header("ROW COUNTS")

    layers = {
        "Raw": config.RAW_DIR / "all_raw_data.parquet",
        "Processed": config.PROCESSED_DIR / "all_processed_data.parquet",
        "Matched (all)": config.MATCHED_DIR / "all_with_matches.parquet",
        "Matched (only)": config.MATCHED_DIR / "matched_products.parquet",
    }

    counts = {}
    for name, path in layers.items():
        if path.exists():
            df = pd.read_parquet(path, engine="pyarrow")
            counts[name] = len(df)
            log.info(f"  {name:20s}: {len(df):>10,} rows")
        else:
            log.warning(f"  {name:20s}: FILE NOT FOUND")
            counts[name] = 0

    # check targets
    raw_count = counts.get("Raw", 0)
    if raw_count >= config.TARGET_RAW_ROWS:
        log.info(f"  [OK] Raw rows ({raw_count:,}) >= target ({config.TARGET_RAW_ROWS:,})")
    else:
        log.warning(f"  [WARN] Raw rows ({raw_count:,}) < target ({config.TARGET_RAW_ROWS:,})")

    return counts


def check_missing_values():
    """Check missing values in processed data."""
    print_header("MISSING VALUES")

    path = config.PROCESSED_DIR / "all_processed_data.parquet"
    if not path.exists():
        log.warning("Processed data not found")
        return

    df = pd.read_parquet(path, engine="pyarrow")

    important_cols = ["product_name", "price", "store_key", "city",
                      "category", "brand", "quantity", "unit"]

    for col in important_cols:
        if col in df.columns:
            missing = df[col].isna().sum()
            pct = missing / len(df) * 100
            status = "OK" if pct < 5 else "WARNING" if pct < 20 else "BAD"
            log.info(f"  {col:25s}: {missing:>8,} missing ({pct:5.1f}%) [{status}]")
        else:
            log.warning(f"  {col:25s}: COLUMN NOT FOUND")


def check_duplicates():
    """Check for duplicates in processed data."""
    print_header("DUPLICATE CHECK")

    path = config.PROCESSED_DIR / "all_processed_data.parquet"
    if not path.exists():
        log.warning("Processed data not found")
        return

    df = pd.read_parquet(path, engine="pyarrow")

    # exact duplicates
    exact_dupes = df.duplicated().sum()
    log.info(f"  Exact duplicate rows:    {exact_dupes:,}")

    # near duplicates (same store + name + price)
    near_dupes = df.duplicated(subset=["store_key", "product_name", "price"]).sum()
    log.info(f"  Near duplicates (name+price): {near_dupes:,}")

    # unique products per store
    for store in df["store_key"].unique():
        store_df = df[df["store_key"] == store]
        unique = store_df["product_name"].nunique()
        total = len(store_df)
        log.info(f"  {store:10s}: {unique:,} unique names / {total:,} total rows")


def check_price_sanity():
    """Check price ranges are reasonable."""
    print_header("PRICE SANITY CHECK")

    path = config.PROCESSED_DIR / "all_processed_data.parquet"
    if not path.exists():
        return

    df = pd.read_parquet(path, engine="pyarrow")

    log.info(f"  Min price:    Rs. {df['price'].min():.0f}")
    log.info(f"  Max price:    Rs. {df['price'].max():.0f}")
    log.info(f"  Mean price:   Rs. {df['price'].mean():.0f}")
    log.info(f"  Median price: Rs. {df['price'].median():.0f}")

    # suspicious prices
    zero_prices = (df["price"] <= 0).sum()
    huge_prices = (df["price"] > 50000).sum()
    log.info(f"  Prices <= 0:    {zero_prices:,}")
    log.info(f"  Prices > 50k:   {huge_prices:,}")

    # price distribution by store
    log.info("\n  Price stats by store:")
    for store in df["store_key"].unique():
        s = df[df["store_key"] == store]["price"]
        log.info(f"    {store:10s}: mean Rs.{s.mean():.0f}, "
                 f"median Rs.{s.median():.0f}, "
                 f"range Rs.{s.min():.0f}-{s.max():.0f}")


def check_coverage():
    """Check store and city coverage."""
    print_header("COVERAGE CHECK")

    path = config.PROCESSED_DIR / "all_processed_data.parquet"
    if not path.exists():
        return

    df = pd.read_parquet(path, engine="pyarrow")

    log.info(f"  Total stores: {df['store_key'].nunique()}")
    log.info(f"  Total cities: {df['city'].nunique()}")
    log.info(f"  Total categories: {df['category'].nunique()}")

    log.info("\n  Rows by store-city:")
    pivot = df.groupby(["store_key", "city"]).size().reset_index(name="rows")
    for _, row in pivot.iterrows():
        log.info(f"    {row['store_key']:10s} / {row['city']:15s}: {row['rows']:>8,} rows")


def check_matching_quality():
    """Validate matching results."""
    print_header("MATCHING QUALITY")

    path = config.MATCHED_DIR / "matched_products.parquet"
    summary_path = config.MATCHED_DIR / "match_summary.parquet"

    if not path.exists():
        log.warning("Matched data not found")
        return

    df = pd.read_parquet(path, engine="pyarrow")

    total_groups = df["match_group"].nunique()
    log.info(f"  Total match groups:      {total_groups:,}")

    if total_groups >= config.TARGET_MATCHED_PRODUCTS:
        log.info(f"  [OK] Match groups ({total_groups:,}) >= target ({config.TARGET_MATCHED_PRODUCTS:,})")
    else:
        log.warning(f"  [WARN] Match groups ({total_groups:,}) < target ({config.TARGET_MATCHED_PRODUCTS:,})")

    # how many groups span 2 stores vs 3 stores?
    if summary_path.exists():
        summary = pd.read_parquet(summary_path, engine="pyarrow")
        store_counts = summary["num_stores"].value_counts().sort_index()
        for n, count in store_counts.items():
            log.info(f"  Groups in {n} stores: {count:,}")

    # coverage by store
    log.info("\n  Matched rows by store:")
    for store in df["store_key"].unique():
        count = len(df[df["store_key"] == store])
        log.info(f"    {store:10s}: {count:,} rows")


def main():
    log.info("=" * 60)
    log.info("  DATA VALIDATION REPORT")
    log.info("=" * 60)

    check_row_counts()
    check_missing_values()
    check_duplicates()
    check_price_sanity()
    check_coverage()
    check_matching_quality()

    log.info("\n" + "=" * 60)
    log.info("  VALIDATION COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
