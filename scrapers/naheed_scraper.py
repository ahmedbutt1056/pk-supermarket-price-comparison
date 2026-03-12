"""
naheed_scraper.py — Scraper for Naheed Supermarket (naheed.pk)

Naheed is a popular Pakistani supermarket chain with an online grocery
store serving Karachi and Lahore. Built on Magento (server-side rendered).

How it works:
1. Loop through each city
2. For each city, iterate through every product category
3. Paginate category pages (?p=1, ?p=2 ...)
4. Parse product cards from HTML using confirmed CSS selectors
5. Stop when page has no products (0 items on page)
6. Random delays + rotating UA for anti-blocking

Confirmed selectors (Magento SSR):
  - Product cards: li.product-item
  - Product name: .product-item-link
  - Price: [data-price-amount] attribute
  - Old price: .old-price .price
  - Image: img (data-src or src)
  - Pagination: ?p=N (32 items/page)
"""

from bs4 import BeautifulSoup
from datetime import datetime
import random
import re

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from scrapers.base_scraper import BaseScraper
from utils.helpers import clean_text, clean_price


class NaheedScraper(BaseScraper):
    """Scraper for naheed.pk (Magento SSR)"""

    def __init__(self):
        super().__init__("naheed")
        self.info = config.STORES["naheed"]
        self.base = self.info["base_url"]

    def get_categories(self, city=None):
        """Build list of category URLs for Naheed."""
        cats = self.info["categories"]
        result = []
        for c in cats:
            link = f"{self.base}/{c}"
            # Extract readable name from path
            name = c.split("/")[-1].replace("-", " ").title()
            result.append({"url": link, "name": name})
        return result

    def parse_products(self, html, category, city):
        """
        Parse one page of Naheed product listings.
        Uses confirmed Magento CSS selectors from real site probing.
        """
        items = []
        soup = BeautifulSoup(html, "lxml")

        # Confirmed working selector: li.product-item
        cards = soup.select("li.product-item")
        if not cards:
            # Fallback
            cards = soup.select(".product-item")

        for card in cards:
            try:
                # --- product name (confirmed: .product-item-link) ---
                name_tag = card.select_one(
                    ".product-item-link, a.product-item-link, "
                    ".product-item-name a"
                )
                name = clean_text(name_tag.get_text()) if name_tag else None
                if not name or len(name) < 3:
                    continue

                # --- price (confirmed: data-price-amount attribute) ---
                price = None
                price_el = card.select_one("[data-price-amount]")
                if price_el:
                    try:
                        price = float(price_el["data-price-amount"])
                    except (ValueError, KeyError):
                        pass

                # Fallback: parse from text
                if price is None:
                    price_tag = card.select_one(
                        ".special-price .price, .price-box .price, .price"
                    )
                    if price_tag:
                        price = clean_price(clean_text(price_tag.get_text()))

                # --- old price (confirmed: .old-price .price) ---
                old_price = None
                old_tag = card.select_one(".old-price .price")
                if old_tag:
                    old_price = clean_price(clean_text(old_tag.get_text()))

                # --- product link ---
                a_tag = (name_tag if name_tag and name_tag.name == "a"
                         else card.select_one("a[href]"))
                link = a_tag.get("href") if a_tag else None
                if link and not link.startswith("http"):
                    link = self.base + link

                # --- image ---
                img_tag = card.select_one("img")
                img = None
                if img_tag:
                    img = (img_tag.get("data-lazy") or
                           img_tag.get("data-src") or
                           img_tag.get("src"))

                row = {
                    "store": "Naheed Supermarket",
                    "store_key": "naheed",
                    "source_url": "https://www.naheed.pk",
                    "city": city,
                    "category": category,
                    "product_name": name,
                    "price": price,
                    "old_price": old_price,
                    "currency": "PKR",
                    "product_url": link,
                    "image_url": img,
                    "scraped_at": datetime.now().isoformat(),
                }
                items.append(row)

            except Exception as e:
                self.log.debug(f"Skipped a card: {e}")
                continue

        return items

    def scrape(self, city=None):
        """Main method — scrape all cities and categories."""
        self.started_at = datetime.now()
        all_items = []

        cities = [city] if city else self.info["cities"]

        for cur_city in cities:
            self.log.info(f"==> Naheed — city: {cur_city}")
            cats = self.get_categories(cur_city)

            # shuffle categories to avoid predictable pattern
            random.shuffle(cats)

            for cat in cats:
                cat_url = cat["url"]
                cat_name = cat["name"]
                page_num = 1

                self.log.info(f"  Category: {cat_name}")

                while page_num <= config.MAX_PAGES_PER_CATEGORY:
                    url = f"{cat_url}?p={page_num}"

                    resp = self.get_page(url)
                    if not resp:
                        break

                    products = self.parse_products(resp.text, cat_name, cur_city)

                    if not products:
                        self.log.debug(f"    Page {page_num}: empty, moving on")
                        break

                    all_items.extend(products)
                    self.found += len(products)
                    self.log.info(f"    Page {page_num}: got {len(products)} products")

                    page_num += 1

                # pause between categories
                if not self.site_is_down:
                    self._long_wait()
                else:
                    self.log.info(f"  Site is down, skipping remaining categories")
                    break

            # pause between cities
            if len(cities) > 1 and not self.site_is_down:
                self.log.info(f"Finished city {cur_city}, taking a long break...")
                self._very_long_wait()

        self.show_stats()
        return all_items


if __name__ == "__main__":
    s = NaheedScraper()
    data = s.scrape()
    print(f"Total: {len(data)} products")
    if data:
        print("Sample:", data[0])
