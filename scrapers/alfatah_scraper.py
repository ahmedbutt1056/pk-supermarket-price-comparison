"""
alfatah_scraper.py — Scraper for Al-Fatah (alfatah.pk) via Shopify JSON API.

How it works:
1. Fetches products from alfatah.pk/products.json (Shopify API)
2. Paginates through all pages (250 products per page)
3. Maps product_type and tags for category classification
4. Replicates product catalog for each city
5. No HTML parsing needed — pure JSON API

API: https://alfatah.pk/products.json?limit=250&page=N
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


ALFATAH_API = "https://alfatah.pk"
BATCH_SIZE = 250  # Shopify max per page


class AlFatahScraper(BaseScraper):
    """Scraper for alfatah.pk using Shopify JSON API."""

    def __init__(self):
        super().__init__("alfatah")
        self.info = config.STORES["alfatah"]
        self.base = ALFATAH_API
        self.api_session = requests.Session()
        self.api_session.headers.update({
            "User-Agent": random.choice(config.USER_AGENTS),
            "Accept": "application/json",
        })

    def _api_get(self, url, retries=3):
        """GET request to Shopify API with retry + UA rotation. Every attempt is logged."""
        for attempt in range(1, retries + 1):
            req_start = time.time()
            try:
                # Rotate user-agent each request to avoid fingerprinting
                self.api_session.headers["User-Agent"] = random.choice(config.USER_AGENTS)
                self.log.debug(f"API GET (try {attempt}): {url}")
                resp = self.api_session.get(url, timeout=20)
                resp.raise_for_status()
                elapsed_ms = (time.time() - req_start) * 1000
                self.attempt_log.info(
                    f"alfatah      | GET | {url[:120]} | try={attempt} | "
                    f"status={resp.status_code} | {elapsed_ms:.0f}ms | SUCCESS"
                )
                return resp.json()
            except Exception as e:
                elapsed_ms = (time.time() - req_start) * 1000
                self.attempt_log.warning(
                    f"alfatah      | GET | {url[:120]} | try={attempt} | "
                    f"status=--- | {elapsed_ms:.0f}ms | FAIL | {e}"
                )
                self.log.warning(f"API error try {attempt}: {e}")
                if attempt < retries:
                    time.sleep(3 * attempt + random.uniform(2, 5))
        return None

    def _classify_category(self, product):
        """Classify product into a grocery category based on product_type and tags."""
        ptype = (product.get("product_type") or "").strip().lower()
        raw_tags = product.get("tags") or ""
        tags = ", ".join(raw_tags).lower() if isinstance(raw_tags, list) else str(raw_tags).lower()
        title = (product.get("title") or "").lower()

        # Map based on product_type first
        type_map = {
            "fruits & vegitables": "fruits-vegetables",
            "fruits & vegetables": "fruits-vegetables",
            "snacks & beverages": "snacks-beverages",
            "staples": "staples",
            "daily essentials": "daily-essentials",
            "dairy & fresh bakery": "dairy-bakery",
            "dairy creams": "dairy",
            "packaged food": "packaged-food",
            "drinking water": "beverages",
            "tea & cofee": "tea-coffee",
            "tea & coffee": "tea-coffee",
            "flour": "flour-atta",
            "oil & ghee": "cooking-oil-ghee",
            "baby care": "baby-care",
            "cat food": "pet-care",
            "yogurt": "dairy",
            "chicken": "meat-poultry",
            "chips and nimko": "snacks",
            "spices": "spices",
            "bread": "bakery",
            "flavoured milk": "dairy",
            "biscuits": "snacks",
            "chocolates": "chocolates",
            "chocolate": "chocolates",
            "frozen fries": "frozen-food",
            "cereals": "breakfast",
            "baking items": "baking",
            "dry fruit & dates": "dry-fruits",
            "salt": "staples",
            "candies & bubble gums": "confectionery",
        }

        if ptype in type_map:
            return type_map[ptype]

        # Fallback: check tags and title for keywords
        keywords = {
            "fruit": "fruits-vegetables", "vegetable": "fruits-vegetables",
            "meat": "meat-poultry", "chicken": "meat-poultry", "beef": "meat-poultry",
            "milk": "dairy", "cheese": "dairy", "butter": "dairy", "yogurt": "dairy",
            "rice": "staples", "flour": "flour-atta", "atta": "flour-atta",
            "oil": "cooking-oil-ghee", "ghee": "cooking-oil-ghee",
            "tea": "tea-coffee", "coffee": "tea-coffee",
            "juice": "beverages", "water": "beverages", "drink": "beverages",
            "snack": "snacks", "chip": "snacks", "biscuit": "snacks",
            "chocolate": "chocolates", "candy": "confectionery",
            "frozen": "frozen-food", "ice cream": "frozen-food",
            "spice": "spices", "masala": "spices",
            "soap": "personal-care", "shampoo": "personal-care",
            "detergent": "household", "cleaner": "household",
            "baby": "baby-care", "diaper": "baby-care",
            "pet": "pet-care", "cat food": "pet-care", "dog food": "pet-care",
        }
        combined = f"{ptype} {tags} {title}"
        for kw, cat in keywords.items():
            if kw in combined:
                return cat

        return "general"

    def get_categories(self, city=None):
        """Not used — AlFatah fetches all products from Shopify API."""
        return []

    def parse_products(self, html, category, city):
        """Not used — AlFatah uses JSON API."""
        return []

    def fetch_all_products(self):
        """Fetch ALL products from Shopify API with pagination."""
        all_products = []
        page = 1

        while True:
            url = f"{ALFATAH_API}/products.json?limit={BATCH_SIZE}&page={page}"
            data = self._api_get(url)

            if not data or "products" not in data:
                break

            products = data["products"]
            if not products:
                break

            for p in products:
                title = (p.get("title") or "").strip()
                if not title or len(title) < 2:
                    continue

                variants = p.get("variants", [])
                if not variants:
                    continue

                # Use first variant for pricing
                v = variants[0]
                price = v.get("price")
                compare_price = v.get("compare_at_price")

                try:
                    price_val = float(price) if price else None
                except (ValueError, TypeError):
                    price_val = None

                try:
                    old_price_val = float(compare_price) if compare_price else None
                except (ValueError, TypeError):
                    old_price_val = None

                if not price_val:
                    continue

                category = self._classify_category(p)

                weight = v.get("weight")
                weight_unit = v.get("weight_unit", "")
                weight_str = f"{weight} {weight_unit}".strip() if weight else None

                row = {
                    "store": "Al-Fatah",
                    "store_key": "alfatah",
                    "source_url": "https://alfatah.pk",
                    "city": None,
                    "category": category,
                    "category_hierarchy": p.get("product_type", ""),
                    "product_name": title,
                    "price": price_val,
                    "old_price": old_price_val,
                    "currency": "PKR",
                    "brand": p.get("vendor", ""),
                    "weight": weight_str,
                    "unit_type": weight_unit,
                    "sku": v.get("sku", ""),
                    "product_url": f"{ALFATAH_API}/products/{p.get('handle', '')}",
                    "image_url": (p.get("images", [{}])[0].get("src") if p.get("images") else None),
                    "in_stock": v.get("available", False),
                    "scraped_at": datetime.now().isoformat(),
                }
                all_products.append(row)

            self.log.info(f"  Page {page}: {len(products)} products (total: {len(all_products)})")
            page += 1

            # Random delay between pages (1-3s) to avoid rate limiting
            time.sleep(random.uniform(1.0, 3.0))

            if page > 100:
                break

        return all_products

    def scrape(self, city=None):
        """Main scrape — fetch all products then replicate per city."""
        self.started_at = datetime.now()
        all_items = []

        cities = [city] if city else self.info["cities"]

        self.log.info("Fetching all products from AlFatah Shopify API...")
        product_pool = self.fetch_all_products()
        self.log.info(f"Total unique products fetched: {len(product_pool)}")

        if not product_pool:
            self.log.error("No products fetched, aborting")
            self.show_stats()
            return all_items

        for cur_city in cities:
            self.log.info(f"==> AlFatah — city: {cur_city}")
            for p in product_pool:
                row = dict(p)
                row["city"] = cur_city
                all_items.append(row)

        self.found = len(all_items)
        self.show_stats()
        return all_items


if __name__ == "__main__":
    s = AlFatahScraper()
    data = s.scrape()
    print(f"Total: {len(data)} products")
    if data:
        print("Sample:", data[0])
