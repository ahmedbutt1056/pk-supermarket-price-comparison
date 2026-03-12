"""
imtiaz_scraper.py — Scraper for Imtiaz Super Market (imtiaz.com.pk)

How it works:
1. Loop through each city
2. For each city, go through every category
3. Paginate category pages (page=1, page=2 ...)
4. On each page, find all product cards
5. Pull out: name, price, category, link
6. Stop when page has no products
7. Random delays + rotating UA keep us from getting blocked
"""

from bs4 import BeautifulSoup
from datetime import datetime
import random

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from scrapers.base_scraper import BaseScraper
from utils.helpers import clean_text, clean_price


class ImtiazScraper(BaseScraper):
    """Scraper for imtiaz.com.pk"""

    def __init__(self):
        super().__init__("imtiaz")
        self.info = config.STORES["imtiaz"]
        self.base = self.info["base_url"]

    def get_categories(self, city=None):
        """Build list of category URLs."""
        cats = self.info["categories"]
        result = []
        for c in cats:
            link = f"{self.base}/category/{c}"
            result.append({"url": link, "name": c})
        return result

    def parse_products(self, html, category, city):
        """
        Parse one page of Imtiaz product listings.
        We try many CSS selectors because site structure can vary.
        """
        items = []
        soup = BeautifulSoup(html, "lxml")

        # try to find product cards with various selectors
        cards = soup.select(
            ".product-card, .product-item, .product-box, "
            ".card-product, .product, .item-product, "
            "[class*='product'], .col-product"
        )
        if not cards:
            cards = soup.select("li.product, div.product, article.product")

        for card in cards:
            try:
                # --- get product name ---
                name_tag = card.select_one(
                    "h2, h3, h4, .product-name, .product-title, "
                    ".card-title, [class*='name'], [class*='title']"
                )
                name = clean_text(name_tag.get_text()) if name_tag else None
                if not name:
                    continue

                # --- get price ---
                price_tag = card.select_one(
                    ".price, .product-price, .current-price, "
                    "[class*='price'], .amount, span.woocommerce-Price-amount"
                )
                price_txt = clean_text(price_tag.get_text()) if price_tag else None
                price = clean_price(price_txt)

                # --- get old price (before discount) ---
                old_tag = card.select_one(
                    ".old-price, .original-price, del .amount, [class*='old-price']"
                )
                old_price = clean_price(clean_text(old_tag.get_text())) if old_tag else None

                # --- get product link ---
                a_tag = card.select_one("a[href]")
                link = a_tag["href"] if a_tag else None
                if link and not link.startswith("http"):
                    link = self.base + link

                # --- get image ---
                img_tag = card.select_one("img[src], img[data-src]")
                img = None
                if img_tag:
                    img = img_tag.get("data-src") or img_tag.get("src")

                # build one row
                row = {
                    "store": "Imtiaz Super Market",
                    "store_key": "imtiaz",
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
        """
        Main method — scrape all cities and categories.
        Returns big list of product dicts.
        """
        self.started_at = datetime.now()
        all_items = []

        cities = [city] if city else self.info["cities"]

        for cur_city in cities:
            self.log.info(f"==> Imtiaz — city: {cur_city}")
            cats = self.get_categories(cur_city)

            # shuffle categories so request pattern is unpredictable
            random.shuffle(cats)

            for cat in cats:
                cat_url = cat["url"]
                cat_name = cat["name"]
                page_num = 1

                self.log.info(f"  Category: {cat_name}")

                while page_num <= config.MAX_PAGES_PER_CATEGORY:
                    # build page URL
                    url = f"{cat_url}?page={page_num}"

                    # fetch with retries + random delay (all in base class)
                    resp = self.get_page(url)
                    if not resp:
                        break

                    # parse HTML for products
                    products = self.parse_products(resp.text, cat_name, cur_city)

                    if not products:
                        self.log.debug(f"    Page {page_num}: empty, moving on")
                        break

                    all_items.extend(products)
                    self.found += len(products)
                    self.log.info(f"    Page {page_num}: got {len(products)} products")

                    page_num += 1

                # big pause between categories (15-40s)
                if not self.site_is_down:
                    self._long_wait()
                else:
                    self.log.info(f"  Site is down, skipping remaining categories")
                    break

            # very big pause between cities
            if len(cities) > 1 and not self.site_is_down:
                self.log.info(f"Finished city {cur_city}, taking a long break...")
                self._very_long_wait()

        self.show_stats()
        return all_items


# run this file directly to test
if __name__ == "__main__":
    s = ImtiazScraper()
    data = s.scrape()
    print(f"Total: {len(data)} products")
    if data:
        print("Sample:", data[0])
