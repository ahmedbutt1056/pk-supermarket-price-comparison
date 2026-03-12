"""Quick re-scrape of Daraz with improved anti-blocking measures."""
import sys
import time
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from utils.logger_setup import get_logger
import config

log = get_logger("rescrape_daraz")


def main():
    from scrapers.daraz_scraper import DarazScraper

    log.info("=" * 60)
    log.info("RE-SCRAPING DARAZ with improved anti-blocking")
    log.info("=" * 60)

    s = DarazScraper()
    rows = s.scrape()

    if not rows:
        log.error("No data scraped from Daraz")
        return

    df = pd.DataFrame(rows)
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)

    for (sk, city), grp in df.groupby(["store_key", "city"]):
        path = config.RAW_DIR / f"raw_{sk}_{city}.parquet"
        grp.to_parquet(path, index=False, engine="pyarrow")
        log.info(f"  Saved {len(grp):,} rows -> {path.name}")

    log.info(f"\nDaraz total: {len(df):,} rows, {df['product_name'].nunique():,} unique products")


if __name__ == "__main__":
    main()
