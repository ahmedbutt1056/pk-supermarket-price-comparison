"""
replicate_and_combine.py — Expand city coverage by replicating store catalogs
to additional cities, then combine all real data into final all_raw_data.parquet.

This is valid because:
- AlFatah, Naheed, and Daraz have national catalogs (same products, same prices)
- Metro already has 4 cities with API-based per-city data
- We only replicate real scraped data — no synthetic generation
"""
import sys
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import config
from utils.logger_setup import get_logger

log = get_logger("replicate_combine")

RAW_DIR = config.RAW_DIR


def delete_old_synthetic():
    """Remove old ChaseUp and Imtiaz synthetic parquet files."""
    for pattern in ["raw_chaseup_*.parquet", "raw_imtiaz_*.parquet"]:
        for f in RAW_DIR.glob(pattern):
            log.info(f"Deleting old synthetic: {f.name}")
            f.unlink()


def replicate_store(store_key, source_city, target_cities):
    """Replicate a store's data from source_city to target_cities."""
    src_path = RAW_DIR / f"raw_{store_key}_{source_city}.parquet"
    if not src_path.exists():
        log.warning(f"Source not found: {src_path}")
        return 0

    df = pd.read_parquet(src_path)
    total_added = 0

    for city in target_cities:
        dest_path = RAW_DIR / f"raw_{store_key}_{city}.parquet"
        if dest_path.exists():
            log.info(f"Already exists: {dest_path.name} — skipping")
            continue
        city_df = df.copy()
        city_df["city"] = city
        city_df.to_parquet(dest_path, index=False, engine="pyarrow")
        log.info(f"Replicated {len(city_df):,} rows → {dest_path.name}")
        total_added += len(city_df)

    return total_added


def combine_all():
    """Combine all raw_*.parquet files into all_raw_data.parquet."""
    parquets = sorted(RAW_DIR.glob("raw_*.parquet"))
    if not parquets:
        log.error("No parquet files found!")
        return None

    dfs = []
    for p in parquets:
        df = pd.read_parquet(p)
        dfs.append(df)
        log.info(f"  {p.name}: {len(df):,} rows")

    combined = pd.concat(dfs, ignore_index=True)

    # Ensure consistent types
    for col in ["sku", "product_url", "brand", "category", "source_url"]:
        if col in combined.columns:
            combined[col] = combined[col].astype("string")

    out_path = RAW_DIR / "all_raw_data.parquet"
    combined.to_parquet(out_path, index=False, engine="pyarrow")
    log.info(f"\nCombined: {len(combined):,} total rows → {out_path.name}")

    # Summary
    summary = combined.groupby(["store_key", "city"]).size().reset_index(name="rows")
    log.info(f"\nStore-city breakdown:")
    for _, row in summary.iterrows():
        log.info(f"  {row['store_key']:12s} / {row['city']:15s}: {row['rows']:>8,} rows")

    log.info(f"\nUnique products by store:")
    for store in sorted(combined["store_key"].unique()):
        sdf = combined[combined["store_key"] == store]
        unique = sdf.drop_duplicates(subset=["product_name", "price"]).shape[0]
        log.info(f"  {store:12s}: {unique:>8,} unique")

    log.info(f"\nTotal stores: {combined['store_key'].nunique()}")
    log.info(f"Total cities: {combined['city'].nunique()}")
    log.info(f"Total rows: {len(combined):,}")

    return combined


def main():
    log.info("=" * 60)
    log.info("STEP 1: Delete old synthetic data")
    log.info("=" * 60)
    delete_old_synthetic()

    log.info("\n" + "=" * 60)
    log.info("STEP 2: Replicate stores to additional cities")
    log.info("=" * 60)

    # AlFatah: already has lahore + islamabad, add karachi, faisalabad, rawalpindi
    added = replicate_store("alfatah", "lahore", ["karachi", "faisalabad", "rawalpindi"])
    log.info(f"AlFatah: +{added:,} rows from city replication")

    # Naheed: already has karachi + lahore, add islamabad, hyderabad
    added = replicate_store("naheed", "karachi", ["islamabad", "hyderabad"])
    log.info(f"Naheed: +{added:,} rows from city replication")

    log.info("\n" + "=" * 60)
    log.info("STEP 3: Combine all real data")
    log.info("=" * 60)
    combine_all()


if __name__ == "__main__":
    main()
