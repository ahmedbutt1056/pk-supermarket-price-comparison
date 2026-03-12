"""
run_scrapers.py — Phase 4: Run all scrapers and save raw data.

This script:
1. Tries to scrape real data from each store website
2. If real data is too small, generates synthetic data to fill the gap
3. Saves raw Parquet files: one per store per city
4. Also saves one combined raw Parquet

Run from project root:
    python run_scrapers.py
"""

import pandas as pd
from datetime import datetime

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import config
from utils.logger_setup import get_logger
from scrapers.imtiaz_scraper import ImtiazScraper
from scrapers.metro_scraper import MetroScraper
from scrapers.alfatah_scraper import AlFatahScraper
from scrapers.chaseup_scraper import ChaseUpScraper
from scrapers.naheed_scraper import NaheedScraper
from scrapers.data_generator import generate_data

log = get_logger("pipeline")


def run_live_scrapers():
    """Try to scrape real data from all 5 stores. Returns list of dicts."""
    all_data = []

    scrapers = [
        ("imtiaz", ImtiazScraper()),
        ("metro", MetroScraper()),
        ("alfatah", AlFatahScraper()),
        ("chaseup", ChaseUpScraper()),
        ("naheed", NaheedScraper()),
    ]

    for store_name, scraper in scrapers:
        log.info(f"--- Starting live scrape: {store_name} ---")
        try:
            data = scraper.scrape()
            log.info(f"  {store_name}: got {len(data)} products from live scraping")
            all_data.extend(data)
        except Exception as e:
            log.error(f"  {store_name} scraper failed: {e}")
            log.info(f"  Continuing with other scrapers...")

    return all_data


def save_raw_data(all_data):
    """Save raw data as Parquet files — one per store-city and one combined."""
    if not all_data:
        log.warning("No data to save!")
        return

    df = pd.DataFrame(all_data)
    log.info(f"Total raw rows: {len(df)}")

    # save combined file
    combined_path = config.RAW_DIR / "all_raw_data.parquet"
    df.to_parquet(combined_path, index=False, engine="pyarrow")
    log.info(f"Saved combined: {combined_path} ({len(df)} rows)")

    # save per store per city
    for store_key in df["store_key"].unique():
        store_df = df[df["store_key"] == store_key]
        for city in store_df["city"].unique():
            city_df = store_df[store_df["city"] == city]
            filename = f"raw_{store_key}_{city}.parquet"
            filepath = config.RAW_DIR / filename
            city_df.to_parquet(filepath, index=False, engine="pyarrow")
            log.info(f"  Saved: {filename} ({len(city_df)} rows)")

    return df


def main():
    """Main pipeline: scrape + generate + save."""
    log.info("=" * 60)
    log.info("STARTING DATA COLLECTION PIPELINE")
    log.info(f"Time: {datetime.now().isoformat()}")
    log.info("=" * 60)

    # Step 1: Try live scraping
    log.info("\n--- STEP 1: Live Scraping ---")
    live_data = run_live_scrapers()
    log.info(f"Live scraping total: {len(live_data)} rows")

    # Step 2: Check if we need synthetic data
    all_data = list(live_data)

    if config.USE_DATA_GENERATOR:
        current = len(all_data)
        target = config.TARGET_RAW_ROWS
        shortfall = target - current

        if shortfall > 0:
            log.info(f"\n--- STEP 2: Generating Synthetic Data ---")
            log.info(f"  Current rows: {current}")
            log.info(f"  Target rows:  {target}")
            log.info(f"  Need to generate: {shortfall}")

            # calculate how many per store-city
            # 3 stores, avg 2.67 cities = ~8 store-city combos
            store_cities = sum(len(s["cities"]) for s in config.STORES.values())
            per_combo = (shortfall // store_cities) + 1

            generated = generate_data(rows_per_store_city=per_combo)
            all_data.extend(generated)
            log.info(f"  After generation: {len(all_data)} total rows")
        else:
            log.info(f"  Already have {current} rows, no generation needed")

    # Step 3: Save everything
    log.info(f"\n--- STEP 3: Saving Raw Data ---")
    df = save_raw_data(all_data)

    log.info("\n" + "=" * 60)
    log.info("DATA COLLECTION COMPLETE")
    log.info(f"Total rows saved: {len(df) if df is not None else 0}")
    log.info("=" * 60)

    return df


if __name__ == "__main__":
    main()
