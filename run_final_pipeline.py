"""
run_final_pipeline.py — Load all real scraped data, 
run complete pipeline (clean → match → analyze → validate).
"""
import pandas as pd
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

import config
from utils.logger_setup import get_logger

log = get_logger("final_pipeline")


def gather_real_data():
    """Load all per-city parquet files with real scraped data."""
    raw_dir = config.RAW_DIR
    real_frames = []

    # Load every raw_STORE_CITY.parquet file
    for f in sorted(raw_dir.glob("raw_*_*.parquet")):
        try:
            df = pd.read_parquet(f, engine="pyarrow")
            if len(df) > 0:
                real_frames.append(df)
                log.info(f"  {f.name}: {len(df):,} rows")
        except Exception as e:
            log.warning(f"  {f.name}: skip ({e})")

    if real_frames:
        combined = pd.concat(real_frames, ignore_index=True)
        # Drop any duplicates from overlapping parquet files
        dedup_cols = ["store_key", "city", "product_name", "price"]
        available = [c for c in dedup_cols if c in combined.columns]
        combined = combined.drop_duplicates(subset=available, keep="first")
        return combined
    return pd.DataFrame()


def main():
    log.info("=" * 60)
    log.info("FINAL PIPELINE — Merge + Fill + Clean + Match + Analyze")
    log.info(f"Time: {datetime.now().isoformat()}")
    log.info("=" * 60)

    # ─── Step 1: Gather real data ───
    log.info("\n[STEP 1] Gathering real scraped data...")
    real_df = gather_real_data()
    real_count = len(real_df)
    log.info(f"Total real scraped rows: {real_count:,}")
    
    if real_count == 0:
        log.error("No real data found! Run scrape_all_real.py first.")
        return
    
    # Show breakdown
    for sk in sorted(real_df["store_key"].unique()):
        sdf = real_df[real_df["store_key"] == sk]
        cities = sdf["city"].nunique()
        log.info(f"  {sk}: {len(sdf):,} rows across {cities} cities")

    # ─── Step 2: Save raw data ───
    log.info("\n[STEP 2] Saving combined raw data...")
    df = real_df.copy()
    # Ensure mixed-type columns are consistent strings
    for col in ["sku", "product_url", "brand", "category", "source_url"]:
        if col in df.columns:
            df[col] = df[col].astype("string")
    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    
    combined_path = config.RAW_DIR / "all_raw_data.parquet"
    df.to_parquet(combined_path, index=False, engine="pyarrow")
    log.info(f"Saved: {combined_path} ({len(df):,} rows)")
    
    # Summary
    log.info("\nRaw data summary:")
    for sk in sorted(df["store_key"].unique()):
        sdf = df[df["store_key"] == sk]
        has_url = sdf["product_url"].notna().sum() if "product_url" in sdf.columns else 0
        log.info(f"  {sk}: {len(sdf):,} total, {has_url:,} with product_url")
    log.info(f"  TOTAL: {len(df):,} rows")

    # ─── Step 3: Clean & Normalize ───
    log.info("\n[STEP 3] Running clean_and_normalize...")
    try:
        from processing.clean_and_normalize import main as clean_main
        clean_main()
        log.info("Clean & Normalize DONE")
    except Exception as e:
        log.error(f"Clean failed: {e}")
        import traceback; traceback.print_exc()
        return

    # ─── Step 4: Product Matching ───
    log.info("\n[STEP 4] Running product_matching...")
    try:
        from matching.product_matching import main as match_main
        match_main()
        log.info("Product Matching DONE")
    except Exception as e:
        log.error(f"Matching failed: {e}")
        import traceback; traceback.print_exc()
        return

    # ─── Step 5: Price Analysis ───
    log.info("\n[STEP 5] Running price_analysis...")
    try:
        from analysis.price_analysis import main as analysis_main
        analysis_main()
        log.info("Price Analysis DONE")
    except Exception as e:
        log.error(f"Analysis failed: {e}")
        import traceback; traceback.print_exc()
        return

    # ─── Step 6: Validation ───
    log.info("\n[STEP 6] Running validation...")
    try:
        from validation.validate_data import main as validate_main
        validate_main()
        log.info("Validation DONE")
    except Exception as e:
        log.error(f"Validation failed: {e}")
        import traceback; traceback.print_exc()

    log.info("\n" + "=" * 60)
    log.info("PIPELINE COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
