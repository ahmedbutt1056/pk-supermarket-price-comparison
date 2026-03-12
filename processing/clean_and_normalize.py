"""
clean_and_normalize.py — Phase 5: Clean raw data and normalize attributes.

This script:
1. Reads all raw Parquet files
2. Removes duplicates
3. Cleans product names
4. Extracts brand, quantity, unit from product name
5. Calculates price_per_unit (e.g., price per kg, per liter)
6. Saves cleaned data to processed/ folder

Run from project root:
    python processing/clean_and_normalize.py
"""

import re
import pandas as pd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from utils.logger_setup import get_logger
from utils.helpers import (
    clean_text, clean_price, extract_quantity_and_unit,
    extract_brand, PAKISTANI_BRANDS
)

log = get_logger("processing")


# ==============================================================
# STEP 1: Load all raw data
# ==============================================================

def load_raw_data():
    """Load the combined raw Parquet file."""
    path = config.RAW_DIR / "all_raw_data.parquet"
    if not path.exists():
        log.error(f"Raw data not found at {path}")
        log.error("Run 'python run_scrapers.py' first!")
        return None

    df = pd.read_parquet(path, engine="pyarrow")
    log.info(f"Loaded raw data: {len(df)} rows")
    return df


# ==============================================================
# STEP 2: Remove duplicates
# ==============================================================

def remove_duplicates(df):
    """Remove exact duplicate rows."""
    before = len(df)
    df = df.drop_duplicates(
        subset=["store_key", "city", "product_name", "price"],
        keep="first"
    )
    after = len(df)
    removed = before - after
    log.info(f"Removed {removed} duplicates ({before} -> {after})")
    return df


# ==============================================================
# STEP 3: Clean product names
# ==============================================================

def clean_names(df):
    """Clean and standardize product names."""
    log.info("Cleaning product names...")

    # clean whitespace and unicode
    df["product_name_clean"] = df["product_name"].apply(clean_text)

    # remove store-specific prefixes that don't belong
    junk_words = ["New!", "SALE", "HOT", "Best Seller", "Limited"]
    for word in junk_words:
        df["product_name_clean"] = df["product_name_clean"].str.replace(
            word, "", case=False, regex=False
        )

    # clean again after removal
    df["product_name_clean"] = df["product_name_clean"].apply(clean_text)

    # drop rows where name is empty after cleaning
    before = len(df)
    df = df[df["product_name_clean"].str.len() > 2]
    log.info(f"  Dropped {before - len(df)} rows with empty names")

    return df


# ==============================================================
# STEP 4: Extract brand
# ==============================================================

def extract_brands(df):
    """Extract brand name from each product name."""
    log.info("Extracting brands...")
    df["brand"] = df["product_name_clean"].apply(
        lambda x: extract_brand(x, PAKISTANI_BRANDS)
    )

    # show top brands found
    top = df["brand"].value_counts().head(10)
    log.info(f"  Top brands:\n{top}")

    return df


# ==============================================================
# STEP 5: Extract quantity and unit
# ==============================================================

def extract_sizes(df):
    """Extract quantity (number) and unit (g, kg, ml, l, pcs) from name."""
    log.info("Extracting quantity and unit...")

    # apply the helper function to each product name
    results = df["product_name_clean"].apply(
        lambda x: pd.Series(extract_quantity_and_unit(x))
    )
    df["quantity"] = results[0]
    df["unit"] = results[1]

    # how many had a parseable size?
    has_size = df["quantity"].notna().sum()
    pct = has_size / len(df) * 100
    log.info(f"  Found quantity+unit in {has_size}/{len(df)} rows ({pct:.1f}%)")

    return df


# ==============================================================
# STEP 6: Normalize units & calculate price per unit
# ==============================================================

def normalize_and_price_per_unit(df):
    """
    Convert all weights to grams, all volumes to ml.
    Then calculate price_per_unit (price per standard unit).
    """
    log.info("Normalizing units and computing price per unit...")

    # standard conversion to base unit (grams for weight, ml for volume)
    weight_to_g = {**config.WEIGHT_UNITS}  # kg→1000, g→1, etc.
    volume_to_ml = {**config.VOLUME_UNITS}  # l→1000, ml→1, etc.

    normalized_qty = []
    normalized_unit = []
    price_per_unit = []

    for _, row in df.iterrows():
        qty = row["quantity"]
        unit = row["unit"]
        price = row["price"]

        if pd.isna(qty) or pd.isna(unit) or pd.isna(price):
            normalized_qty.append(None)
            normalized_unit.append(None)
            price_per_unit.append(None)
            continue

        qty = float(qty)
        price = float(price)

        if unit in weight_to_g:
            # convert to grams
            grams = qty * weight_to_g[unit]
            normalized_qty.append(grams)
            normalized_unit.append("g")
            # price per kg
            if grams > 0:
                price_per_unit.append(round(price / grams * 1000, 2))
            else:
                price_per_unit.append(None)

        elif unit in volume_to_ml:
            # convert to ml
            ml = qty * volume_to_ml[unit]
            normalized_qty.append(ml)
            normalized_unit.append("ml")
            # price per liter
            if ml > 0:
                price_per_unit.append(round(price / ml * 1000, 2))
            else:
                price_per_unit.append(None)

        elif unit in config.COUNT_UNITS:
            # keep as count
            count = qty * config.COUNT_UNITS[unit]
            normalized_qty.append(count)
            normalized_unit.append("pcs")
            # price per piece
            if count > 0:
                price_per_unit.append(round(price / count, 2))
            else:
                price_per_unit.append(None)
        else:
            normalized_qty.append(qty)
            normalized_unit.append(unit)
            price_per_unit.append(None)

    df["qty_normalized"] = normalized_qty
    df["unit_normalized"] = normalized_unit
    df["price_per_unit"] = price_per_unit

    has_ppu = pd.Series(price_per_unit).notna().sum()
    log.info(f"  Computed price_per_unit for {has_ppu}/{len(df)} rows")

    return df


# ==============================================================
# STEP 7: Clean price column
# ==============================================================

def clean_prices(df):
    """Make sure price column is numeric and reasonable."""
    log.info("Cleaning prices...")

    # convert to numeric
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["old_price"] = pd.to_numeric(df["old_price"], errors="coerce")

    # remove rows with no price
    before = len(df)
    df = df[df["price"].notna() & (df["price"] > 0)]
    log.info(f"  Dropped {before - len(df)} rows with invalid price")

    # remove unreasonable prices (>100,000 PKR for grocery items)
    before = len(df)
    df = df[df["price"] <= 100000]
    log.info(f"  Dropped {before - len(df)} rows with price > 100k")

    return df


# ==============================================================
# STEP 8: Save processed data
# ==============================================================

def save_processed(df):
    """Save cleaned data to processed/ folder as Parquet."""
    # combined file
    out_path = config.PROCESSED_DIR / "all_processed_data.parquet"
    df.to_parquet(out_path, index=False, engine="pyarrow")
    log.info(f"Saved processed data: {out_path} ({len(df)} rows)")

    # per store
    for store in df["store_key"].unique():
        store_df = df[df["store_key"] == store]
        path = config.PROCESSED_DIR / f"processed_{store}.parquet"
        store_df.to_parquet(path, index=False, engine="pyarrow")
        log.info(f"  Saved: processed_{store}.parquet ({len(store_df)} rows)")

    return df


# ==============================================================
# MAIN
# ==============================================================

def main():
    log.info("=" * 60)
    log.info("STARTING DATA PROCESSING PIPELINE")
    log.info("=" * 60)

    # load
    df = load_raw_data()
    if df is None:
        return

    # process
    df = remove_duplicates(df)
    df = clean_names(df)
    df = clean_prices(df)
    df = extract_brands(df)
    df = extract_sizes(df)
    df = normalize_and_price_per_unit(df)

    # save
    df = save_processed(df)

    log.info("\n" + "=" * 60)
    log.info(f"PROCESSING COMPLETE — {len(df)} clean rows")
    log.info("=" * 60)

    return df


if __name__ == "__main__":
    main()
