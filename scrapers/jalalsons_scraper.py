"""
jalalsons_scraper.py — Scrapes products from Jalal Sons (jalalsons.com.pk).

Jalal Sons is a Lahore-based grocery, bakery, and restaurant chain.
Their website uses a custom platform (tossdown) with server-rendered HTML.
Products are paginated at 100 per page via ?page_no=N parameter.
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import random
import logging
from datetime import datetime

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

logger = logging.getLogger(__name__)


class JalalSonsScraper:
    """Scrape products from jalalsons.com.pk."""

    BASE_URL = "https://jalalsons.com.pk"
    SHOP_URL = f"{BASE_URL}/shop"
    CITIES = ["lahore"]  # Jalal Sons is Lahore-based

    def __init__(self):
        self.session = requests.Session()
        self.total_products = 0
        self.total_requests = 0
        self.fails = 0

    def _get_headers(self):
        return {
            "User-Agent": random.choice(config.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": self.BASE_URL,
        }

    def _fetch_page(self, page_no):
        """Fetch a single page of products."""
        url = f"{self.SHOP_URL}?page_no={page_no}"
        self.total_requests += 1
        try:
            resp = self.session.get(url, headers=self._get_headers(), timeout=config.REQUEST_TIMEOUT)
            logger.info(f"GET {url} -> {resp.status_code} ({len(resp.text)} bytes)")
            if resp.status_code == 200:
                return resp.text
            else:
                self.fails += 1
                logger.warning(f"Non-200 status: {resp.status_code} for {url}")
                return None
        except Exception as e:
            self.fails += 1
            logger.error(f"Request failed for {url}: {e}")
            return None

    def _parse_products(self, html, city):
        """Parse product cards from HTML."""
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select("[class*='single_product_theme']")
        products = []

        for card in cards:
            try:
                name_el = card.select_one(".product_name_theme")
                link_el = card.select_one("a[href*='/product/']")
                img_el = card.select_one("img")

                if not name_el:
                    continue

                name = name_el.get_text(strip=True)
                if not name:
                    continue

                # Extract price from card text
                all_text = card.get_text()
                price_match = re.search(r"Rs\.?\s*([\d,]+)", all_text)
                price = float(price_match.group(1).replace(",", "")) if price_match else None

                # Extract old/compare price if exists
                old_price = None
                old_match = re.findall(r"Rs\.?\s*([\d,]+)", all_text)
                if len(old_match) >= 2:
                    p1 = float(old_match[0].replace(",", ""))
                    p2 = float(old_match[1].replace(",", ""))
                    if p1 > p2:
                        old_price = p1
                        price = p2
                    elif p2 > p1:
                        old_price = p2

                href = link_el.get("href", "") if link_el else ""
                product_url = f"{self.BASE_URL}{href}" if href and not href.startswith("http") else href
                image_url = img_el.get("src", "") if img_el else None

                # Try to extract category from URL or card
                category = "general"
                cat_el = card.select_one("[class*='category'], [class*='cat']")
                if cat_el:
                    category = cat_el.get_text(strip=True).lower()

                products.append({
                    "store": "Jalal Sons",
                    "store_key": "jalalsons",
                    "source_url": f"{self.SHOP_URL}",
                    "city": city,
                    "category": category,
                    "product_name": name,
                    "price": price,
                    "old_price": old_price,
                    "currency": "PKR",
                    "product_url": product_url,
                    "image_url": image_url,
                    "scraped_at": datetime.now().isoformat(),
                })
            except Exception as e:
                logger.debug(f"Error parsing card: {e}")
                continue

        return products

    def _get_total_items(self, html):
        """Extract total item count from the pagination script."""
        match = re.search(r"totalItems\s*=\s*(\d+)", html)
        if match:
            return int(match.group(1))
        return None

    def scrape(self):
        """Scrape all products from Jalal Sons."""
        all_products = []

        for city in self.CITIES:
            logger.info(f"Scraping Jalal Sons for city: {city}")
            print(f"\n[JalalSons] Scraping {city}...")

            # Fetch first page to get total count
            html = self._fetch_page(1)
            if not html:
                print(f"  Failed to fetch page 1")
                continue

            total_items = self._get_total_items(html)
            items_per_page = 100
            total_pages = (total_items // items_per_page) + 1 if total_items else 25

            print(f"  Total items reported: {total_items} | Pages: {total_pages}")

            # Parse first page
            products = self._parse_products(html, city)
            all_products.extend(products)
            print(f"  Page 1: {len(products)} products (total: {len(all_products)})")

            # Fetch remaining pages
            for page in range(2, total_pages + 1):
                delay = random.uniform(config.MIN_DELAY, config.MAX_DELAY)
                time.sleep(delay)

                html = self._fetch_page(page)
                if not html:
                    print(f"  Page {page}: FAILED")
                    continue

                products = self._parse_products(html, city)
                if not products:
                    print(f"  Page {page}: 0 products (stopping)")
                    break

                all_products.extend(products)
                self.total_products = len(all_products)
                print(f"  Page {page}: {len(products)} products (total: {len(all_products)})")

                # Rotate session periodically
                if self.total_requests % config.REQUESTS_BEFORE_NEW_SESSION == 0:
                    self.session = requests.Session()

        self.total_products = len(all_products)
        return all_products

    def show_stats(self):
        print(f"\n[JalalSons] Stats: {self.total_products} products | "
              f"{self.total_requests} requests | {self.fails} failures")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    scraper = JalalSonsScraper()
    products = scraper.scrape()
    scraper.show_stats()

    if products:
        import pandas as pd
        df = pd.DataFrame(products)
        out = config.RAW_DIR / "raw_jalalsons_lahore.parquet"
        df.to_parquet(out, index=False)
        print(f"Saved {len(df)} products to {out}")
        print(f"Price range: Rs.{df['price'].min():.0f} - Rs.{df['price'].max():.0f}")
        print(f"Sample:\n{df[['product_name','price','category']].head(10)}")
