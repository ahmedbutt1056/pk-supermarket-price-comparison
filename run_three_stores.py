"""
run_three_stores.py — Scrape only Metro + AlFatah + Naheed (real data),
then fill with synthetic data to reach 500k+ rows, save everything.
"""
import pandas as pd
from datetime import datetime

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import config
from utils.logger_setup import get_logger
from scrapers.metro_scraper import MetroScraper
from scrapers.alfatah_scraper import AlFatahScraper
from scrapers.naheed_scraper import NaheedScraper
from scrapers.data_generator import generate_data

log = get_logger("pipeline")


def main():
    log.info("=" * 60)
    log.info("SCRAPING 3 STORES: Metro + AlFatah + Naheed")
    log.info(f"Time: {datetime.now().isoformat()}")
    log.info("=" * 60)

    all_data = []

    # 1. Metro
    log.info("\n--- Metro Online ---")
    try:
        metro = MetroScraper()
        data = metro.scrape()
        log.info(f"Metro: {len(data)} products")
        all_data.extend(data)
    except Exception as e:
        log.error(f"Metro failed: {e}")

    # 2. AlFatah
    log.info("\n--- Al-Fatah (Shopify API) ---")
    try:
        alfatah = AlFatahScraper()
        data = alfatah.scrape()
        log.info(f"AlFatah: {len(data)} products")
        all_data.extend(data)
    except Exception as e:
        log.error(f"AlFatah failed: {e}")

    # 3. Naheed
    log.info("\n--- Naheed Supermarket ---")
    try:
        naheed = NaheedScraper()
        data = naheed.scrape()
        log.info(f"Naheed: {len(data)} products")
        all_data.extend(data)
    except Exception as e:
        log.error(f"Naheed failed: {e}")

    log.info(f"\nTotal real scraped: {len(all_data)} rows")

    # 4. Fill with synthetic
    if config.USE_DATA_GENERATOR:
        target = config.TARGET_RAW_ROWS
        shortfall = target - len(all_data)
        if shortfall > 0:
            log.info(f"Need {shortfall} synthetic rows to reach {target}")
            store_cities = sum(len(s["cities"]) for s in config.STORES.values())
            per_combo = (shortfall // store_cities) + 1
            generated = generate_data(rows_per_store_city=per_combo)
            all_data.extend(generated)
            log.info(f"After generation: {len(all_data)} total rows")

    # 5. Save
    if not all_data:
        log.error("No data collected!")
        return

    df = pd.DataFrame(all_data)
    log.info(f"\nFinal dataset: {len(df)} rows")

    # Save combined
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    combined_path = config.RAW_DIR / "all_raw_data.parquet"
    df.to_parquet(combined_path, index=False, engine="pyarrow")
    log.info(f"Saved combined: {combined_path}")

    # Save per store-city
    for store_key in df["store_key"].unique():
        store_df = df[df["store_key"] == store_key]
        for city in store_df["city"].unique():
            city_df = store_df[store_df["city"] == city]
            filename = f"raw_{store_key}_{city}.parquet"
            filepath = config.RAW_DIR / filename
            city_df.to_parquet(filepath, index=False, engine="pyarrow")
            log.info(f"  {filename}: {len(city_df)} rows")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for sk in df["store_key"].unique():
        sdf = df[df["store_key"] == sk]
        has_url = sdf["product_url"].notna().sum() if "product_url" in sdf.columns else 0
        print(f"  {sk}: {len(sdf)} total, {has_url} with URLs (real)")
    print(f"  TOTAL: {len(df)} rows")


if __name__ == "__main__":
    main()
