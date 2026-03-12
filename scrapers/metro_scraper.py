"""
metro_scraper.py — Scraper for Metro Online (metro-online.pk)

How it works:
1. Fetches all categories from Metro's JSON API
2. Filters to Food categories only
3. For each leaf category, paginates through products via API
4. Maps products to each city Metro operates in
5. Random delays between API calls for politeness

API endpoint: https://stagging.metro-online.pk/api/read/Products
"""

import requests
import time
import random
from datetime import datetime

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from scrapers.base_scraper import BaseScraper


# Metro's backend API (discovered from their JS bundle)
METRO_API = "https://stagging.metro-online.pk"
METRO_STORE_ID = 10
BATCH_SIZE = 200


class MetroScraper(BaseScraper):
    """Scraper for metro-online.pk using their JSON API."""

    def __init__(self):
        super().__init__("metro")
        self.info = config.STORES["metro"]
        self.base = self.info["base_url"]
        self.api_session = requests.Session()
        self.api_session.headers.update({
            "User-Agent": random.choice(config.USER_AGENTS),
            "Accept": "application/json",
        })

    # ----------------------------------------------------------
    # API helpers
    # ----------------------------------------------------------

    def _api_get(self, path, retries=3):
        """Make a GET request to Metro's API with retry. Every attempt is logged."""
        url = f"{METRO_API}{path}"
        for attempt in range(1, retries + 1):
            req_start = time.time()
            try:
                self.log.debug(f"API GET (try {attempt}): {path[:120]}")
                resp = self.api_session.get(url, timeout=20)
                resp.raise_for_status()
                elapsed_ms = (time.time() - req_start) * 1000
                self.attempt_log.info(
                    f"metro        | GET | {url[:120]} | try={attempt} | "
                    f"status={resp.status_code} | {elapsed_ms:.0f}ms | SUCCESS"
                )
                return resp.json()
            except Exception as e:
                elapsed_ms = (time.time() - req_start) * 1000
                self.attempt_log.warning(
                    f"metro        | GET | {url[:120]} | try={attempt} | "
                    f"status=--- | {elapsed_ms:.0f}ms | FAIL | {e}"
                )
                self.log.warning(f"API error try {attempt}: {e}")
                if attempt < retries:
                    time.sleep(3 * attempt + random.uniform(1, 3))
        return None

    def fetch_categories(self):
        """Fetch ALL active leaf categories from Metro API (food + non-food)."""
        data = self._api_get(f"/api/read/Categories?filter=storeId&filterValue={METRO_STORE_ID}")
        if not data or "data" not in data:
            self.log.error("Failed to fetch categories")
            return []

        cats = data["data"]
        self.log.info(f"Fetched {len(cats)} total categories")

        # Build parent -> children map
        children_of = {}
        for c in cats:
            pid = c.get("parentId")
            if pid:
                children_of.setdefault(pid, []).append(c)

        parent_ids = set(children_of.keys())

        # ALL leaf categories (not just food)
        leaves = []
        for c in cats:
            if not c.get("enable", True):
                continue
            if c["id"] not in parent_ids:
                leaves.append(c)

        self.log.info(f"Found {len(leaves)} leaf categories (all departments)")
        return leaves

    def get_categories(self, city=None):
        """Required by BaseScraper — delegates to fetch_categories."""
        cats = self.fetch_categories()
        return [{"id": c["id"], "name": c["category_name"]} for c in cats]

    def parse_products(self, html, category, city):
        """Not used — Metro uses JSON API, not HTML parsing."""
        return []

    def fetch_products_for_category(self, cat_id, cat_name, city):
        """Fetch all products for a category via API with pagination."""
        items = []
        offset = 0

        while True:
            path = (
                f"/api/read/Products"
                f"?type=Products_nd_associated_Brands"
                f"&filter=||tier3Id&filterValue=||{cat_id}"
                f"&filter=storeId&filterValue={METRO_STORE_ID}"
                f"&filter=active&filterValue=true"
                f"&filter=!url&filterValue=!null"
                f"&offset={offset}&limit={BATCH_SIZE}"
                f"&order=product_scoring__DESC"
            )
            data = self._api_get(path)

            if not data or "data" not in data:
                break

            products = data["data"]
            if not products:
                break

            for p in products:
                name = p.get("product_name")
                if not name or len(name) < 3:
                    continue

                price = p.get("price")
                sale_price = p.get("sale_price")
                sell_price = p.get("sell_price")

                # Use sale_price as current if it's a real discount
                current_price = price
                old_price = None
                if sale_price and price and float(sale_price) < float(price):
                    current_price = sale_price
                    old_price = price

                # Category hierarchy from API
                cat_hierarchy = " > ".join(filter(None, [
                    p.get("teir1Name"),
                    p.get("tier2Name"),
                    p.get("tier3Name"),
                    p.get("tier4Name"),
                ]))

                img_url = p.get("url")  # product image URL
                product_url = f"{self.base}/product/{p.get('product_code_app', '')}" if p.get("product_code_app") else None

                row = {
                    "store": "Metro Online",
                    "store_key": "metro",
                    "source_url": "https://www.metro-online.pk",
                    "city": city,
                    "category": cat_name,
                    "category_hierarchy": cat_hierarchy,
                    "product_name": name,
                    "price": float(current_price) if current_price else None,
                    "old_price": float(old_price) if old_price else None,
                    "currency": "PKR",
                    "brand": p.get("brand_name"),
                    "weight": p.get("weight"),
                    "unit_type": p.get("unit_type"),
                    "sku": p.get("product_code_app"),
                    "product_url": product_url,
                    "image_url": img_url,
                    "in_stock": (p.get("available_stock") or 0) > 0,
                    "scraped_at": datetime.now().isoformat(),
                }
                items.append(row)

            self.log.debug(f"    offset={offset}: got {len(products)} raw, {len(items)} total")

            if len(products) < BATCH_SIZE:
                break  # last page

            offset += BATCH_SIZE
            # Small delay between pages
            time.sleep(random.uniform(0.5, 1.5))

        return items

    def scrape(self, city=None):
        """Main scrape — fetch all products from Metro API for all cities."""
        self.started_at = datetime.now()
        all_items = []

        # Fetch categories once (same for all cities)
        categories = self.fetch_categories()
        if not categories:
            self.log.error("No categories found, aborting")
            self.show_stats()
            return all_items

        cities = [city] if city else self.info["cities"]

        # Fetch all unique products once
        self.log.info(f"Fetching products from {len(categories)} categories (all departments)...")
        product_pool = []
        for cat in categories:
            cat_id = cat["id"]
            cat_name = cat["category_name"]

            products = self.fetch_products_for_category(cat_id, cat_name, cities[0])
            if products:
                product_pool.extend(products)
                self.log.info(f"  {cat_name}: {len(products)} products")

            # Polite delay between categories
            time.sleep(random.uniform(1.0, 3.0))

        self.log.info(f"Total unique products fetched: {len(product_pool)}")

        # Replicate for each city (Metro serves same catalog across cities)
        for cur_city in cities:
            self.log.info(f"==> Metro — city: {cur_city}")
            for p in product_pool:
                row = dict(p)
                row["city"] = cur_city
                all_items.append(row)
            self.found += len(product_pool)

        self.show_stats()
        return all_items


if __name__ == "__main__":
    s = MetroScraper()
    data = s.scrape()
    print(f"Total: {len(data)} products")
    if data:
        print("Sample:", data[0])
