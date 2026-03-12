"""
chaseup_scraper.py — Scraper for Chase Up (chaseup.com.pk)

Chase Up is one of the largest supermarket chains in Pakistan operating
in Karachi, Lahore, and Islamabad with a full online grocery store.

How it works:
1. Loop through each city
2. For each city, iterate through every product category
3. Paginate category pages (?page=1, ?page=2 ...)
4. Parse product cards from HTML
5. Stop when page has no products
6. Random delays + rotating UA keep us from getting blocked
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


class ChaseUpScraper(BaseScraper):
    """Scraper for chaseup.com.pk"""

    def __init__(self):
        super().__init__("chaseup")
        self.info = config.STORES["chaseup"]
        self.base = self.info["base_url"]

    def get_categories(self, city=None):
        """Build list of category URLs for Chase Up."""
        cats = self.info["categories"]
        result = []
        for c in cats:
            link = f"{self.base}/groceries/{c}.html"
            result.append({"url": link, "name": c})
        return result

    def parse_products(self, html, category, city):
        """
        Parse one page of Chase Up product listings.
        Chase Up uses Magento-style HTML with product-item divs.
        """
        items = []
        soup = BeautifulSoup(html, "lxml")

        # Chase Up / Magento style product cards
        cards = soup.select(
            ".product-item, .product-card, .product-box, "
            ".item.product, .product-items .product-item, "
            "[class*='product-item'], li.product-item, "
            "div.product, .products-grid .item"
        )
        if not cards:
            cards = soup.select("[class*='product']")

        for card in cards:
            try:
                # --- product name ---
                name_tag = card.select_one(
                    ".product-item-link, .product-name, .product-title, "
                    "h2 a, h3 a, h4 a, a.product-item-link, "
                    "[class*='name'], [class*='title']"
                )
                name = clean_text(name_tag.get_text()) if name_tag else None
                if not name or len(name) < 3:
                    continue

                # --- price ---
                price_tag = card.select_one(
                    ".price, .special-price .price, .price-box .price, "
                    "[class*='price'] .price, [data-price-amount], "
                    "span.price, .product-price"
                )
                price_txt = clean_text(price_tag.get_text()) if price_tag else None
                price = clean_price(price_txt)

                # try data attribute
                if price is None and price_tag:
                    amt = price_tag.get("data-price-amount")
                    if amt:
                        try:
                            price = float(amt)
                        except ValueError:
                            pass

                # --- old price ---
                old_tag = card.select_one(
                    ".old-price .price, del .price, .regular-price .price, "
                    "[class*='old-price'], [class*='was-price']"
                )
                old_price = clean_price(clean_text(old_tag.get_text())) if old_tag else None

                # --- product link ---
                a_tag = (name_tag if name_tag and name_tag.name == "a"
                         else card.select_one("a[href]"))
                link = a_tag.get("href") if a_tag else None
                if link and not link.startswith("http"):
                    link = self.base + link

                # --- image ---
                img_tag = card.select_one("img[src], img[data-src], img[data-lazy]")
                img = None
                if img_tag:
                    img = (img_tag.get("data-lazy") or
                           img_tag.get("data-src") or
                           img_tag.get("src"))

                row = {
                    "store": "Chase Up",
                    "store_key": "chaseup",
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
            self.log.info(f"==> Chase Up — city: {cur_city}")
            cats = self.get_categories(cur_city)

            # shuffle categories so pattern is unpredictable
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
    s = ChaseUpScraper()
    data = s.scrape()
    print(f"Total: {len(data)} products")
    if data:
        print("Sample:", data[0])
