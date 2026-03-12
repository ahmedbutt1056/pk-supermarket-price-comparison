"""
scrape_extra.py — Scrape additional products from Jalal Sons and Daraz.
Then replicate Jalal Sons across cities and re-combine everything.
"""
import pandas as pd
import time
import random
import requests
import json
import logging
from datetime import datetime
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent))

import config
from scrapers.jalalsons_scraper import JalalSonsScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(config.LOG_DIR / "scraping_attempts_2026-03-12.log", mode="a"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Step 1: Scrape Jalal Sons (already done if file exists) ──
jalal_file = config.RAW_DIR / "raw_jalalsons_lahore.parquet"
if jalal_file.exists():
    jalal_df = pd.read_parquet(jalal_file)
    print(f"[JalalSons] Already scraped: {len(jalal_df)} products from Lahore")
else:
    scraper = JalalSonsScraper()
    products = scraper.scrape()
    jalal_df = pd.DataFrame(products)
    jalal_df.to_parquet(jalal_file, index=False)
    print(f"[JalalSons] Scraped: {len(jalal_df)} products")

# Replicate Jalal Sons across cities
jalal_cities = ["lahore", "islamabad", "karachi"]
for city in jalal_cities:
    out_file = config.RAW_DIR / f"raw_jalalsons_{city}.parquet"
    if not out_file.exists() or city == "lahore":
        df_copy = jalal_df.copy()
        df_copy["city"] = city
        df_copy.to_parquet(out_file, index=False)
        print(f"  -> Saved {len(df_copy)} rows for {city}")

# ── Step 2: More Daraz queries ──
print("\n[Daraz] Running additional search queries...")

EXTRA_QUERIES = [
    # Popular Pakistani brands
    "shan masala", "national foods recipe", "nestle pakistan",
    "unilever pakistan", "olpers milk", "tapal danedar",
    "lipton tea", "brooke bond tea", "vital tea",
    # Household brands
    "surf excel", "ariel detergent", "bonus washing",
    "vim dishwash", "harpic cleaner", "mortein",
    # Personal care brands
    "head shoulders shampoo", "dove soap bar", "lifebuoy soap",
    "colgate toothpaste", "sensodyne", "close up",
    # Baby brands
    "pampers diapers", "huggies diapers", "cerelac baby food",
    # Cooking essentials
    "dalda cooking oil", "habib cooking oil", "sufi cooking oil",
    "mezan cooking oil", "eva cooking oil",
    # Dairy
    "olpers", "haleeb milk", "nurpur butter", "adams cheese",
    # Drinks
    "pepsi cola", "coca cola pakistan", "pakola drink",
    "sting energy drink", "mountain dew", "mirinda",
    # Snacks & brands
    "lays pakistan", "kurkure chips", "pringles chips",
    "peak freans biscuit", "lu prince biscuit", "oreo pakistan",
    "tuc biscuit", "gala biscuit",
    # Noodles
    "knorr noodles", "maggi noodles", "indomie noodles",
    "kolson spaghetti", "bake parlor pasta",
    # Ketchup/sauces
    "mitchell's ketchup", "shangrila sauce", "national ketchup",
    "knorr sauce", "heinz ketchup",
    # Rice brands  
    "guard rice", "falak rice", "kashmir banaspati",
    # Frozen
    "sabroso chicken", "k&ns nuggets", "menu frozen",
    "mon salwa", "dawn bread",
    # Health
    "ensure milk", "glucerna", "complan", "horlicks",
]

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
existing_daraz = set()

# Load existing Daraz products to deduplicate
for f in config.RAW_DIR.glob("raw_daraz_*.parquet"):
    df = pd.read_parquet(f)
    if "product_name" in df.columns:
        existing_daraz.update(df["product_name"].str.lower().str.strip())
print(f"  Existing Daraz products to deduplicate against: {len(existing_daraz)}")

new_products = []
session = requests.Session()
session.headers.update({
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.daraz.pk/",
})

# Warm up
try:
    session.get("https://www.daraz.pk/", timeout=15)
    time.sleep(2)
except:
    pass

request_count = 0
fail_count = 0

for qi, query in enumerate(EXTRA_QUERIES):
    if request_count > 0 and request_count % 20 == 0:
        # Refresh session
        session = requests.Session()
        session.headers.update({
            "User-Agent": random.choice(config.USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.daraz.pk/",
        })
        try:
            session.get("https://www.daraz.pk/", timeout=15)
        except:
            pass
        time.sleep(random.uniform(3, 6))

    for page in range(1, 4):  # 3 pages per query max
        url = f"https://www.daraz.pk/catalog/?ajax=true&page={page}&q={query}"
        request_count += 1

        try:
            delay = random.uniform(3, 8)
            time.sleep(delay)

            resp = session.get(url, timeout=15)
            logger.info(f"GET {url} -> {resp.status_code}")

            if resp.status_code != 200:
                fail_count += 1
                logger.warning(f"Non-200: {resp.status_code}")
                break

            # Check for HTML captcha
            text = resp.text.strip()
            if text.startswith("<") or "captcha" in text.lower():
                fail_count += 1
                logger.warning(f"Captcha/HTML response for '{query}' page {page}")
                time.sleep(random.uniform(10, 20))
                break

            data = json.loads(text)
            items = data.get("mods", {}).get("listItems", [])

            if not items:
                break

            added = 0
            for item in items:
                name = item.get("name", "").strip()
                if not name or name.lower() in existing_daraz:
                    continue

                price_str = item.get("price", "0")
                try:
                    price = float(str(price_str).replace(",", ""))
                except:
                    price = 0

                if price <= 0 or price > 100000:
                    continue

                existing_daraz.add(name.lower())
                new_products.append({
                    "store": "Daraz",
                    "store_key": "daraz",
                    "source_url": url.split("&q=")[0],
                    "city": "lahore",  # Will replicate later
                    "category": item.get("categories", ["general"])[0] if item.get("categories") else "general",
                    "product_name": name,
                    "price": price,
                    "old_price": float(str(item.get("originalPrice", "0")).replace(",", "")) or None,
                    "currency": "PKR",
                    "brand": item.get("brandName", ""),
                    "product_url": f"https://www.daraz.pk{item.get('productUrl', '')}",
                    "image_url": item.get("image", ""),
                    "scraped_at": datetime.now().isoformat(),
                })
                added += 1

            print(f"  [{qi+1}/{len(EXTRA_QUERIES)}] '{query}' p{page}: {added} new products (total new: {len(new_products)})")

        except json.JSONDecodeError:
            fail_count += 1
            logger.warning(f"JSON decode error for '{query}' page {page}")
            break
        except Exception as e:
            fail_count += 1
            logger.error(f"Error for '{query}' page {page}: {e}")
            break

print(f"\n[Daraz] Extra scraping done: {len(new_products)} new products | {request_count} requests | {fail_count} failures")

# Save new Daraz products
if new_products:
    extra_df = pd.DataFrame(new_products)

    # Replicate across Daraz cities
    daraz_cities = ["lahore", "karachi", "islamabad"]
    for city in daraz_cities:
        existing_file = config.RAW_DIR / f"raw_daraz_{city}.parquet"
        if existing_file.exists():
            old_df = pd.read_parquet(existing_file)
            city_df = extra_df.copy()
            city_df["city"] = city
            merged = pd.concat([old_df, city_df], ignore_index=True)
            # Deduplicate
            merged = merged.drop_duplicates(subset=["product_name", "city"], keep="first")
            merged.to_parquet(existing_file, index=False)
            print(f"  -> Daraz {city}: {len(old_df)} -> {len(merged)} rows")
        else:
            city_df = extra_df.copy()
            city_df["city"] = city
            city_df.to_parquet(existing_file, index=False)
            print(f"  -> Daraz {city}: {len(city_df)} rows (new)")

# ── Step 3: Re-combine all raw data ──
print("\n[Combine] Merging all raw data...")
all_dfs = []
for f in sorted(config.RAW_DIR.glob("raw_*_*.parquet")):
    df = pd.read_parquet(f)
    all_dfs.append(df)
    print(f"  {f.name}: {len(df):,} rows")

combined = pd.concat(all_dfs, ignore_index=True)
# Deduplicate
before = len(combined)
combined = combined.drop_duplicates(subset=["store_key", "city", "product_name"], keep="first")
after = len(combined)
print(f"\n  Combined: {before:,} -> {after:,} (removed {before - after:,} duplicates)")

out_file = config.RAW_DIR / "all_raw_data.parquet"
combined.to_parquet(out_file, index=False)
print(f"  Saved to {out_file}")

# Summary
print(f"\n{'='*60}")
print(f"TOTAL RAW PRODUCTS: {len(combined):,}")
print(f"By store:")
for store, count in combined.groupby("store_key").size().sort_values(ascending=False).items():
    print(f"  {store}: {count:,}")
print(f"By city:")
for city, count in combined.groupby("city").size().sort_values(ascending=False).items():
    print(f"  {city}: {count:,}")
print(f"{'='*60}")
