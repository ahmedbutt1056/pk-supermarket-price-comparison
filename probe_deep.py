"""Deeper probe: Daraz API + Metro total count."""
import requests
import json
import re

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# ============================================================
# DARAZ — has JSON API at /catalog/?ajax=true
# ============================================================
print("=" * 60)
print("DARAZ.PK — Exploring API")
print("=" * 60)

headers_daraz = {
    "User-Agent": UA,
    "Accept": "application/json",
    "X-Requested-With": "XMLHttpRequest",
}

# Search grocery
r = requests.get(
    "https://www.daraz.pk/catalog/?ajax=true&page=1&q=grocery",
    headers=headers_daraz, timeout=15
)
d = r.json()
items = d.get("mods", {}).get("listItems", [])
print(f"Search 'grocery': {len(items)} items on page 1")
if items:
    first = items[0]
    print(f"Item keys: {list(first.keys())}")
    print(f"Sample: name={first.get('name','?')[:60]}, price={first.get('price','?')}, brand={first.get('brandName','?')}")

# Try grocery category
categories_to_try = [
    ("Groceries & Pet Supplies", "https://www.daraz.pk/catalog/?ajax=true&page=1&spm=a2a0e.home.cateList.1&q=All-Products&from=input&catId=10002287"),
    ("Grocery direct", "https://www.daraz.pk/groceries-pet-supplies/?ajax=true&page=1"),
    ("Food staples", "https://www.daraz.pk/food-staples/?ajax=true&page=1"),
    ("Beverages", "https://www.daraz.pk/beverages/?ajax=true&page=1"),
    ("Laundry", "https://www.daraz.pk/laundry-household/?ajax=true&page=1"),
    ("Health", "https://www.daraz.pk/health-beauty/?ajax=true&page=1"),
    ("Baby", "https://www.daraz.pk/mother-baby/?ajax=true&page=1"),
    ("Home Living", "https://www.daraz.pk/home-living/?ajax=true&page=1"),
    ("Snacks", "https://www.daraz.pk/snacks-confectionery/?ajax=true&page=1"),
]

total_daraz = 0
for name, url in categories_to_try:
    try:
        r = requests.get(url, headers=headers_daraz, timeout=15)
        if r.status_code == 200:
            d = r.json()
            items = d.get("mods", {}).get("listItems", [])
            main_info = d.get("mainInfo", {})
            total_results = main_info.get("totalResults", "?")
            print(f"  {name}: {len(items)} items/page, totalResults={total_results}")
            total_daraz += int(total_results) if str(total_results).isdigit() else 0
        else:
            print(f"  {name}: HTTP {r.status_code}")
    except Exception as e:
        print(f"  {name}: ERROR {e}")

print(f"\nDaraz estimated total across categories: {total_daraz:,}")

# Show item structure in detail
print("\n--- Daraz Item Structure ---")
r = requests.get("https://www.daraz.pk/food-staples/?ajax=true&page=1", headers=headers_daraz, timeout=15)
if r.status_code == 200:
    d = r.json()
    items = d.get("mods", {}).get("listItems", [])
    if items:
        item = items[0]
        for k, v in item.items():
            print(f"  {k}: {str(v)[:80]}")

# How many pages?
print("\n--- Daraz Pagination ---")
for page in [1, 2, 10, 50, 100]:
    r = requests.get(f"https://www.daraz.pk/food-staples/?ajax=true&page={page}", headers=headers_daraz, timeout=15)
    if r.status_code == 200:
        d = r.json()
        items = d.get("mods", {}).get("listItems", [])
        print(f"  Page {page}: {len(items)} items")
    else:
        print(f"  Page {page}: HTTP {r.status_code}")


# ============================================================
# METRO — get total_count from API
# ============================================================
print("\n" + "=" * 60)
print("METRO — Total products (food + non-food)")
print("=" * 60)

headers_metro = {"Accept": "application/json", "User-Agent": UA}

# The first response showed total_count key
r = requests.get(
    "https://stagging.metro-online.pk/api/read/Products?type=Products_nd_associated_Brands&filter=storeId&filterValue=10&filter=active&filterValue=true&offset=0&limit=1",
    headers=headers_metro, timeout=15
)
d = r.json()
print(f"total_count: {d.get('total_count', '?')}")
print(f"Categories count: {len(d.get('categories', []))}")
print(f"Brands count: {len(d.get('brands', []))}")

# Try different offsets to find how many products total
for offset in [0, 1000, 2000, 3000, 4000, 4500, 4800, 4900, 4950, 5000]:
    r = requests.get(
        f"https://stagging.metro-online.pk/api/read/Products?type=Products_nd_associated_Brands&filter=storeId&filterValue=10&filter=active&filterValue=true&offset={offset}&limit=50",
        headers=headers_metro, timeout=15
    )
    d = r.json()
    count = len(d.get("data", []))
    tc = d.get("total_count", "?")
    print(f"  offset={offset}: got {count} items, total_count={tc}")

print("\nDONE!")
