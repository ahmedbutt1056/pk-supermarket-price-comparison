"""
run_all.py — Master script that runs the ENTIRE pipeline end-to-end.

Steps:
1. Scrape data (live + synthetic)        -> data/raw/
2. Clean and normalize                    -> data/processed/
3. Match products across stores           -> data/matched/
4. Run price analysis                     -> analysis/results/ + charts/
5. Validate everything                    -> prints report

Run from project root:
    python run_all.py
"""

import sys
from pathlib import Path

# make sure we can import from project root
sys.path.insert(0, str(Path(__file__).parent))

from utils.logger_setup import get_logger

log = get_logger("run_all")


def main():
    log.info("=" * 70)
    log.info("   SUPERMARKET PRICE PIPELINE — FULL RUN")
    log.info("=" * 70)

    # STEP 1: Scrape / Generate raw data
    try:
        log.info("\n>>> STEP 1/5: Collecting raw data...")
        from run_scrapers import main as run_scrapers
        run_scrapers()
    except Exception as e:
        log.error(f"Step 1 failed: {e}")
        log.info("Continuing to next step...")

    # STEP 2: Clean and normalize
    try:
        log.info("\n>>> STEP 2/5: Cleaning and normalizing...")
        from processing.clean_and_normalize import main as run_processing
        run_processing()
    except Exception as e:
        log.error(f"Step 2 failed: {e}")
        log.info("Continuing to next step...")

    # STEP 3: Match products across stores
    try:
        log.info("\n>>> STEP 3/5: Matching products...")
        from matching.product_matching import main as run_matching
        run_matching()
    except Exception as e:
        log.error(f"Step 3 failed: {e}")
        log.info("Continuing to next step...")

    # STEP 4: Price analysis
    try:
        log.info("\n>>> STEP 4/5: Running price analysis...")
        from analysis.price_analysis import main as run_analysis
        run_analysis()
    except Exception as e:
        log.error(f"Step 4 failed: {e}")
        log.info("Continuing to next step...")

    # STEP 5: Validate
    try:
        log.info("\n>>> STEP 5/5: Validating data...")
        from validation.validate_data import main as run_validation
        run_validation()
    except Exception as e:
        log.error(f"Step 5 failed: {e}")

    log.info("\n" + "=" * 70)
    log.info("   ALL DONE! Check the data/ and analysis/ folders.")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
