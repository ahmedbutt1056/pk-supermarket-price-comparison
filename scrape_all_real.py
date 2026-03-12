"""
scrape_all_real.py — Run ALL scrapers to collect maximum real data.

Runs: Metro (all categories) + AlFatah + Naheed + Daraz
Saves each store-city parquet for transparency.
"""
import sys
import time
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from utils.logger_setup import get_logger
import config

log = get_logger("scrape_all_real")


def run_metro():
    """Scrape Metro — all categories, 4 cities."""
    from scrapers.metro_scraper import MetroScraper
    log.info("=" * 60)
    log.info("METRO — Scraping ALL categories (food + non-food)")
    log.info("=" * 60)
    s = MetroScraper()
    return s.scrape()


def run_alfatah():
    """Scrape AlFatah — Shopify API, 2 cities."""
    from scrapers.alfatah_scraper import AlFatahScraper
    log.info("=" * 60)
    log.info("ALFATAH — Scraping via Shopify API")
    log.info("=" * 60)
    s = AlFatahScraper()
    return s.scrape()


def run_naheed():
    """Scrape Naheed — HTML scraping, 2 cities."""
    from scrapers.naheed_scraper import NaheedScraper
    log.info("=" * 60)
    log.info("NAHEED — Scraping via HTML")
    log.info("=" * 60)
    s = NaheedScraper()
    return s.scrape()


def run_daraz():
    """Scrape Daraz — catalog search API, 3 cities."""
    from scrapers.daraz_scraper import DarazScraper
    log.info("=" * 60)
    log.info("DARAZ — Scraping via search API")
    log.info("=" * 60)
    s = DarazScraper()
    return s.scrape()


def save_data(all_rows, store_key):
    """Save scraped data to per-store-city parquets."""
    df = pd.DataFrame(all_rows)
    if df.empty:
        log.warning(f"  {store_key}: no data to save")
        return df

    config.RAW_DIR.mkdir(parents=True, exist_ok=True)

    for (sk, city), grp in df.groupby(["store_key", "city"]):
        path = config.RAW_DIR / f"raw_{sk}_{city}.parquet"
        grp.to_parquet(path, index=False, engine="pyarrow")
        log.info(f"  Saved {len(grp):,} rows → {path.name}")

    return df


def main():
    log.info("Starting FULL REAL DATA scraping...")
    start = time.time()

    all_data = []

    # 1. Metro (API, fast)
    try:
        metro = run_metro()
        all_data.extend(metro)
        save_data(metro, "metro")
        log.info(f"Metro: {len(metro):,} rows collected")
    except Exception as e:
        log.error(f"Metro failed: {e}")

    # 2. AlFatah (Shopify API, fast)
    try:
        alfatah = run_alfatah()
        all_data.extend(alfatah)
        save_data(alfatah, "alfatah")
        log.info(f"AlFatah: {len(alfatah):,} rows collected")
    except Exception as e:
        log.error(f"AlFatah failed: {e}")

    # 3. Naheed (HTML, slower)
    try:
        naheed = run_naheed()
        all_data.extend(naheed)
        save_data(naheed, "naheed")
        log.info(f"Naheed: {len(naheed):,} rows collected")
    except Exception as e:
        log.error(f"Naheed failed: {e}")

    # 4. Daraz (search API)
    try:
        daraz = run_daraz()
        all_data.extend(daraz)
        save_data(daraz, "daraz")
        log.info(f"Daraz: {len(daraz):,} rows collected")
    except Exception as e:
        log.error(f"Daraz failed: {e}")

    # Save combined raw
    df = pd.DataFrame(all_data)
    if not df.empty:
        combined_path = config.RAW_DIR / "all_raw_data.parquet"
        # Ensure consistent string types for mixed columns
        for col in ["sku", "product_url", "brand", "category"]:
            if col in df.columns:
                df[col] = df[col].astype("string")
        df.to_parquet(combined_path, index=False, engine="pyarrow")
        log.info(f"\nCombined: {len(df):,} total rows → {combined_path.name}")

    elapsed = time.time() - start
    log.info(f"\n{'='*60}")
    log.info(f"SCRAPING COMPLETE — {len(all_data):,} total rows in {elapsed/60:.1f} minutes")
    log.info(f"{'='*60}")

    # Summary
    if not df.empty:
        summary = df.groupby(["store_key", "city"]).size().reset_index(name="rows")
        log.info(f"\nStore-city breakdown:")
        for _, row in summary.iterrows():
            log.info(f"  {row['store_key']:12s} / {row['city']:15s}: {row['rows']:>8,} rows")
        log.info(f"\n  Total unique products by store:")
        for store in df["store_key"].unique():
            sdf = df[df["store_key"] == store]
            unique = sdf.drop_duplicates(subset=["product_name", "price"]).shape[0]
            log.info(f"    {store:12s}: {unique:>8,} unique")

    return df


if __name__ == "__main__":
    main()
