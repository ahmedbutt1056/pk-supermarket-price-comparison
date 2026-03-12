"""Probe multiple Pakistani stores for scrapability."""
import requests
import re
import json

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
headers = {"User-Agent": UA, "Accept": "text/html,application/json"}

def probe(name, url, **kwargs):
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"  {url}")
    print(f"{'='*60}")
    try:
        r = requests.get(url, headers=headers, timeout=15, **kwargs)
        print(f"Status: {r.status_code}  Length: {len(r.text):,}")
        ct = r.headers.get("Content-Type", "")
        print(f"Content-Type: {ct}")
        if "json" in ct or r.text.strip().startswith("{"):
            try:
                d = r.json()
                print(f"JSON keys: {list(d.keys())[:15]}")
                if "mods" in d:
                    print(f"  mods keys: {list(d['mods'].keys())[:15]}")
                if "data" in d:
                    if isinstance(d["data"], list):
                        print(f"  data[]: {len(d['data'])} items")
                        if d["data"]:
                            print(f"  first item keys: {list(d['data'][0].keys())[:15]}")
                    elif isinstance(d["data"], dict):
                        print(f"  data{{}}: {list(d['data'].keys())[:15]}")
                if "products" in d:
                    print(f"  products: {len(d['products'])} items")
                if "items" in d:
                    print(f"  items: {len(d['items'])} items")
                if "totalResults" in str(d)[:5000]:
                    tr = re.findall(r'"totalResults":\s*(\d+)', json.dumps(d)[:5000])
                    if tr:
                        print(f"  totalResults: {tr[0]}")
            except:
                print(f"  JSON parse failed, first 500: {r.text[:500]}")
        else:
            # HTML — look for product counts or embedded JSON
            total_matches = re.findall(r'(\d[\d,]+)\s*(?:products?|items?|results?)', r.text[:20000], re.I)
            if total_matches:
                print(f"  Product count mentions: {total_matches[:5]}")
            next_data = re.findall(r'__NEXT_DATA__.*?({.*?})\s*</script', r.text[:100000], re.S)
            if next_data:
                print(f"  Found __NEXT_DATA__ embed ({len(next_data[0])} chars)")
            product_cards = len(re.findall(r'product-card|product-item|productCard', r.text))
            print(f"  Product card patterns found: {product_cards}")
            print(f"  First 600 chars: {r.text[:600]}")
        return r
    except Exception as e:
        print(f"ERROR: {e}")
        return None

# 1. Daraz grocery category page
probe("Daraz.pk Grocery", "https://www.daraz.pk/groceries-pet-supplies/")

# 2. Daraz API-style search
probe("Daraz.pk API Search", "https://www.daraz.pk/catalog/?ajax=true&page=1&q=grocery")

# 3. GrocerApp 
probe("GrocerApp.pk", "https://www.grocerapp.pk/")

# 4. GrocerApp API
probe("GrocerApp API", "https://api.grocerapp.pk/v2/products")

# 5. Carrefour Pakistan
probe("Carrefour.pk", "https://www.carrefour.pk/")

# 6. Carrefour API
probe("Carrefour API", "https://www.carrefour.pk/api/v1/categories?lang=en&storeId=mafpak")

# 7. PriceOye (electronics but has kitchen/home)
probe("PriceOye.pk", "https://priceoye.pk/grocery")

# 8. iShopping.pk
probe("iShopping.pk", "https://www.ishopping.pk/grocery")

# 9. Bagallery
probe("Bagallery.com", "https://bagallery.com/collections/grocery")

# 10. Metro non-food (we already have food - get everything)
probe("Metro ALL Products", "https://stagging.metro-online.pk/api/read/Products?type=Products_nd_associated_Brands&filter=storeId&filterValue=10&filter=active&filterValue=true&offset=0&limit=5")

# 11. How many total Metro products?
print("\n=== Metro Total Count ===")
r = requests.get(
    "https://stagging.metro-online.pk/api/read/Products?type=Products_nd_associated_Brands&filter=storeId&filterValue=10&filter=active&filterValue=true&offset=0&limit=1",
    headers={"Accept": "application/json", "User-Agent": UA}, timeout=15
)
if r.status_code == 200:
    d = r.json()
    print(f"Total available: {len(d.get('data',[]))} in this batch")
    # Try getting count by going to high offset
    for offset in [5000, 10000, 15000, 20000]:
        r2 = requests.get(
            f"https://stagging.metro-online.pk/api/read/Products?type=Products_nd_associated_Brands&filter=storeId&filterValue=10&filter=active&filterValue=true&offset={offset}&limit=5",
            headers={"Accept": "application/json", "User-Agent": UA}, timeout=15
        )
        d2 = r2.json()
        count = len(d2.get("data", []))
        print(f"  offset={offset}: got {count} products")
        if count == 0:
            print(f"  -> Total Metro products is between {offset-5000} and {offset}")
            break

print("\n\nDONE probing!")
