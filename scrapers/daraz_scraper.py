"""
daraz_scraper.py — Scraper for Daraz.pk (Pakistan's largest e-commerce)

How it works:
1. Uses Daraz's catalog search JSON API (/catalog/?ajax=true)
2. Searches across 60+ grocery/household/FMCG keywords
3. Paginates through results (40 items per page, up to 102 pages per query)
4. Extracts real product names, prices, brands, categories, URLs
5. Deduplicates across overlapping search results

API endpoint: https://www.daraz.pk/catalog/?ajax=true&page=N&q=KEYWORD
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


# Daraz returns max 40 items/page, max ~102 pages per query
ITEMS_PER_PAGE = 40
MAX_PAGES = 100

# Search queries covering grocery/household/FMCG categories
SEARCH_QUERIES = [
    # Staples & pantry
    "rice basmati", "flour atta", "sugar", "salt", "daal lentils",
    "cooking oil", "ghee", "spices masala", "biryani masala",
    "pickle achar", "chutney", "vinegar",
    # Beverages
    "tea", "green tea", "coffee", "milk", "juice", "mineral water",
    "cold drink", "energy drink", "tang", "rooh afza",
    # Dairy
    "yogurt", "cheese", "butter", "cream", "eggs",
    # Snacks
    "chips", "biscuits", "cookies", "chocolate", "candy sweets",
    "dry fruits", "nuts", "namkeen",
    # Sauces & condiments
    "ketchup", "mayonnaise", "soy sauce", "hot sauce",
    # Baby care
    "diapers", "baby milk formula", "baby food cereal", "baby wipes",
    # Personal care
    "shampoo", "soap", "toothpaste", "face wash",
    "body lotion", "deodorant", "hair oil",
    # Household cleaning
    "detergent washing powder", "dish wash", "floor cleaner",
    "toilet cleaner", "bleach", "tissue paper",
    # Frozen food
    "frozen chicken nuggets", "frozen paratha", "frozen samosa",
    # Breakfast
    "cornflakes cereal", "oats", "honey", "jam", "peanut butter",
    # Noodles & instant
    "noodles", "soup", "pasta", "macaroni", "vermicelli",
    # Canned food
    "canned beans", "canned tuna", "canned corn", "tomato paste",
    # Health & wellness
    "vitamins", "hand sanitizer", "antiseptic",
    # Pet care
    "cat food", "dog food", "pet supplies",
    # Kitchen items
    "aluminium foil", "cling wrap", "garbage bags", "paper plates",
]


class DarazScraper(BaseScraper):
    """Scraper for daraz.pk using their catalog search JSON API."""

    def __init__(self):
        super().__init__("daraz")
        self.seen_ids = set()  # dedup across queries
        self._request_count = 0
        self._refresh_session()

    def _refresh_session(self):
        """Create a fresh session with new UA and headers (mimics browser)."""
        self.api_session = requests.Session()
        ua = random.choice(config.USER_AGENTS)
        self.api_session.headers.update({
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://www.daraz.pk/",
        })
        # Warm up session — visit homepage first to get cookies
        try:
            self.api_session.get("https://www.daraz.pk/", timeout=15)
            time.sleep(random.uniform(1.0, 2.0))
        except Exception:
            pass
        self.log.info(f"Session refreshed (UA: {ua[:50]}...)")

    def get_categories(self, city=None):
        return [{"name": q} for q in SEARCH_QUERIES]

    def parse_products(self, html, category, city):
        return []

    def _search_page(self, query, page):
        """Fetch one page of search results. Every attempt is logged."""
        url = f"https://www.daraz.pk/catalog/?ajax=true&page={page}&q={query}"
        for attempt in range(1, 4):
            self._request_count += 1
            self.total_hits += 1  # count in base class stats too
            # Refresh session every 20 requests to avoid detection
            if self._request_count % 20 == 0:
                self.log.info(f"  Rotating session after {self._request_count} requests")
                self._refresh_session()
                time.sleep(random.uniform(3.0, 6.0))

            req_start = time.time()
            try:
                self.log.debug(f"  Daraz search: q={query}, page={page} (try {attempt})")
                resp = self.api_session.get(url, timeout=20)
                elapsed_ms = (time.time() - req_start) * 1000

                if resp.status_code == 429:
                    self.fails += 1
                    self.attempt_log.warning(
                        f"daraz        | GET | {url[:120]} | try={attempt} | "
                        f"status=429 | {elapsed_ms:.0f}ms | FAIL | Rate limited"
                    )
                    wait = 30 + random.uniform(10, 30)
                    self.log.warning(f"  Rate limited, waiting {wait:.0f}s")
                    self._refresh_session()
                    time.sleep(wait)
                    continue
                resp.raise_for_status()

                # Check if response is actually JSON (not an HTML block page)
                content_type = resp.headers.get("Content-Type", "")
                if "json" not in content_type and "javascript" not in content_type:
                    # Likely HTML captcha/block page
                    self.fails += 1
                    self.attempt_log.warning(
                        f"daraz        | GET | {url[:120]} | try={attempt} | "
                        f"status={resp.status_code} | {elapsed_ms:.0f}ms | FAIL | Non-JSON response ({content_type[:50]})"
                    )
                    self.log.warning(f"  Non-JSON response (blocked?), refreshing session...")
                    self._refresh_session()
                    wait = 15 + random.uniform(5, 15) * attempt
                    time.sleep(wait)
                    continue

                data = resp.json()
                self.attempt_log.info(
                    f"daraz        | GET | {url[:120]} | try={attempt} | "
                    f"status={resp.status_code} | {elapsed_ms:.0f}ms | SUCCESS"
                )
                return data
            except Exception as e:
                elapsed_ms = (time.time() - req_start) * 1000
                self.attempt_log.warning(
                    f"daraz        | GET | {url[:120]} | try={attempt} | "
                    f"status=--- | {elapsed_ms:.0f}ms | FAIL | {e}"
                )
                self.fails += 1
                self.log.warning(f"  Search error try {attempt}: {e}")
                if attempt < 3:
                    # On JSON decode errors, refresh session and wait longer
                    if "Expecting value" in str(e) or "JSONDecode" in str(e):
                        self._refresh_session()
                        wait = 20 + random.uniform(10, 20) * attempt
                    else:
                        wait = 5 * attempt + random.uniform(1, 3)
                    time.sleep(wait)
        return None

    def _parse_item(self, item, city):
        """Parse one item from Daraz search results."""
        item_id = item.get("itemId") or item.get("nid")
        if not item_id or item_id in self.seen_ids:
            return None
        self.seen_ids.add(item_id)

        name = item.get("name", "").strip()
        if not name or len(name) < 3:
            return None

        price = item.get("price")
        original_price = item.get("originalPrice")
        old_price = None
        if original_price and price:
            try:
                if float(original_price) > float(price):
                    old_price = float(original_price)
            except (ValueError, TypeError):
                pass

        try:
            price = float(price) if price else None
        except (ValueError, TypeError):
            price = None

        if not price or price <= 0:
            return None

        # Build product URL
        item_url = item.get("itemUrl", "")
        if item_url and not item_url.startswith("http"):
            item_url = f"https:{item_url}" if item_url.startswith("//") else f"https://www.daraz.pk{item_url}"

        # Image
        image = item.get("image", "")
        if image and not image.startswith("http"):
            image = f"https:{image}" if image.startswith("//") else image

        # Categories from Daraz
        cats = item.get("categories", [])
        cat_name = " > ".join(str(c) for c in cats) if isinstance(cats, list) and cats else ""

        # Try to get a readable category from the item URL path
        readable_cat = ""
        if item_url:
            parts = item_url.replace("https://www.daraz.pk/", "").split("/")
            if parts and parts[0]:
                readable_cat = parts[0].replace("-", " ").title()

        return {
            "store": "Daraz",
            "store_key": "daraz",
            "source_url": "https://www.daraz.pk",
            "city": city,
            "category": readable_cat or "General",
            "category_hierarchy": cat_name,
            "product_name": name,
            "price": price,
            "old_price": old_price,
            "currency": "PKR",
            "brand": item.get("brandName"),
            "weight": None,
            "unit_type": None,
            "sku": str(item_id),
            "product_url": item_url,
            "image_url": image,
            "in_stock": item.get("inStock", True),
            "scraped_at": datetime.now().isoformat(),
        }

    def scrape(self, city=None):
        """Scrape Daraz across all search queries for all cities."""
        self.started_at = datetime.now()
        cities = [city] if city else ["lahore", "karachi", "islamabad"]
        all_items = []

        # First pass: collect unique products
        self.log.info(f"Scraping Daraz with {len(SEARCH_QUERIES)} search queries...")
        unique_products = []

        for qi, query in enumerate(SEARCH_QUERIES):
            self.log.info(f"[{qi+1}/{len(SEARCH_QUERIES)}] Searching: '{query}'")
            before = len(self.seen_ids)

            page = 1
            consecutive_empty = 0
            while page <= MAX_PAGES:
                data = self._search_page(query, page)
                if not data:
                    break

                items = data.get("mods", {}).get("listItems", [])
                if not items:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        break
                    page += 1
                    continue

                consecutive_empty = 0
                new_this_page = 0
                for item in items:
                    parsed = self._parse_item(item, cities[0])
                    if parsed:
                        unique_products.append(parsed)
                        new_this_page += 1

                if new_this_page == 0:
                    # All items on this page were already seen
                    break

                page += 1
                # Polite delay between pages
                time.sleep(random.uniform(3.0, 6.0))

            added = len(self.seen_ids) - before
            self.log.info(f"  '{query}': +{added} new products (total unique: {len(self.seen_ids)})")

            # Longer delay between queries to avoid detection
            time.sleep(random.uniform(5.0, 10.0))

        self.log.info(f"Total unique Daraz products: {len(unique_products)}")

        # Replicate across cities (Daraz ships nationwide, same catalog)
        for cur_city in cities:
            self.log.info(f"==> Daraz — city: {cur_city}")
            for p in unique_products:
                row = dict(p)
                row["city"] = cur_city
                all_items.append(row)
            self.found += len(unique_products)

        self.show_stats()
        return all_items


if __name__ == "__main__":
    s = DarazScraper()
    data = s.scrape()
    print(f"Total: {len(data)} products")
    if data:
        print(f"Unique: {len(s.seen_ids)} products")
        print("Sample:", data[0])
