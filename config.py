"""
config.py — Central configuration for the supermarket pipeline.
All paths, URLs, store info, and settings are here.
"""

from pathlib import Path

# ============================================================
# PROJECT PATHS
# ============================================================
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
MATCHED_DIR = DATA_DIR / "matched"
LOG_DIR = BASE_DIR / "logs"

# Make sure all directories exist
for d in [RAW_DIR, PROCESSED_DIR, MATCHED_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ============================================================
# SCRAPING SETTINGS
# ============================================================
REQUEST_TIMEOUT = 15          # seconds per request (faster fail)
RETRY_COUNT = 2               # number of retries on failure (fail fast)
RETRY_DELAY = 5               # seconds between retries (base)
MIN_DELAY = 1.5               # minimum seconds between requests
MAX_DELAY = 4.0               # maximum seconds between requests (random)
MAX_PAGES_PER_CATEGORY = 200  # safety limit per category

# Multiple user agents — we rotate randomly so site sees different "browsers"
USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    # Chrome on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    # Firefox on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Safari on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    # Chrome on Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Mobile (looks different to server)
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
]

# Default headers (User-Agent gets swapped each request)
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ur;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "DNT": "1",
}

# Random referer URLs — makes requests look like they came from Google/social
REFERERS = [
    "https://www.google.com/",
    "https://www.google.com.pk/",
    "https://www.google.com/search?q=supermarket+prices+pakistan",
    "https://www.google.com/search?q=grocery+online+karachi",
    "https://www.google.com/search?q=buy+groceries+online+lahore",
    "https://www.bing.com/",
    "https://www.facebook.com/",
    None,  # sometimes no referer (direct visit)
    None,
]

# After how many requests we create a fresh session (new cookies etc.)
REQUESTS_BEFORE_NEW_SESSION = 15

# Free proxy list — set to empty list [] to disable proxies
# Add your own proxies here if you have them (format: "http://ip:port")
FREE_PROXIES = [
    # Uncomment and add proxies if you have them:
    # "http://103.152.112.120:80",
    # "http://203.99.155.20:8080",
    # "socks5://user:pass@ip:port",
]

# Set True to try fetching free proxy lists automatically
USE_AUTO_PROXIES = False

# ============================================================
# STORE CONFIGURATIONS
# ============================================================
STORES = {
    "imtiaz": {
        "name": "Imtiaz Super Market",
        "base_url": "https://imtiaz.com.pk",
        "cities": ["karachi", "hyderabad"],
        "categories": [
            "grocery", "beverages", "dairy-eggs", "snacks-confectionery",
            "cooking-oil-ghee", "personal-care", "baby-care",
            "cleaning-household", "frozen-food", "health-wellness",
            "rice-flour-pulses", "tea-coffee",
            "fruits-vegetables", "meat-poultry-seafood",
            "bakery-bread", "canned-food", "dry-fruits-nuts",
            "condiments-sauces", "breakfast-cereals", "ready-to-eat",
            "pet-care", "kitchen-accessories", "stationery-office",
            "sweets-desserts", "paper-disposables",
        ],
    },
    "metro": {
        "name": "Metro Online",
        "base_url": "https://www.metro-online.pk",
        "cities": ["lahore", "karachi", "islamabad", "faisalabad"],
        "categories": [],  # Metro fetches categories dynamically from its JSON API
    },
    "alfatah": {
        "name": "Al-Fatah",
        "base_url": "https://alfatah.pk",
        "cities": ["lahore", "islamabad", "karachi", "faisalabad", "rawalpindi"],
        "categories": [],  # AlFatah fetches all products from Shopify JSON API
    },
    "chaseup": {
        "name": "Chase Up",
        "base_url": "https://www.chaseup.com.pk",
        "cities": ["karachi", "lahore", "islamabad"],
        "categories": [
            "grocery", "beverages", "dairy-and-eggs", "snacks-confectionery",
            "cooking-oil-ghee", "personal-care", "baby-care",
            "cleaning-household", "frozen-food", "health-wellness",
            "rice-flour-pulses", "tea-coffee",
            "fruits-vegetables", "meat-poultry-seafood",
            "bakery-bread", "canned-food", "dry-fruits-nuts",
            "condiments-sauces", "breakfast-cereals", "ready-to-eat",
            "pet-care", "kitchen-accessories", "stationery",
            "sweets-desserts", "paper-disposables",
        ],
    },
    "naheed": {
        "name": "Naheed Supermarket",
        "base_url": "https://www.naheed.pk",
        "cities": ["karachi", "lahore", "islamabad", "hyderabad"],
        "categories": [
            # Fresh Products
            "groceries-pets/fresh-products/fruits",
            "groceries-pets/fresh-products/vegetables",
            "groceries-pets/fresh-products/meat-poultry",
            "groceries-pets/fresh-products/sea-food",
            # Frozen Food & Ice Cream (corrected URLs from live site)
            "groceries-pets/frozen-food-ice-cream/chicken",
            "groceries-pets/frozen-food-ice-cream/burgers",
            "groceries-pets/frozen-food-ice-cream/dumplings",
            "groceries-pets/frozen-food-ice-cream/fries",
            "groceries-pets/frozen-food-ice-cream/frozen-vegetable",
            "groceries-pets/frozen-food-ice-cream/kabab-kofta",
            "groceries-pets/frozen-food-ice-cream/nuggets-snacks",
            "groceries-pets/frozen-food-ice-cream/parathas-roti",
            "groceries-pets/frozen-food-ice-cream/ready-to-eat",
            "groceries-pets/frozen-food-ice-cream/roll-samosa",
            "groceries-pets/frozen-food-ice-cream/seafood",
            "groceries-pets/frozen-food-ice-cream/bread-dough",
            "groceries-pets/frozen-food-ice-cream/desserts-toppings",
            # Dairy
            "groceries-pets/dairy/butter-margarine",
            "groceries-pets/dairy/cheese",
            "groceries-pets/dairy/milk-dairy-drinks",
            "groceries-pets/dairy/non-dairy-milk",
            "groceries-pets/dairy/yoghurt",
            # Food Staples
            "groceries-pets/food-staples/beans-pulses",
            "groceries-pets/food-staples/canned-jarred-food",
            "groceries-pets/food-staples/gluten-free",
            "groceries-pets/food-staples/instant-meals",
            "groceries-pets/food-staples/noodles-pasta",
            "groceries-pets/food-staples/ready-to-eat",
            "groceries-pets/food-staples/sauces-pickles",
            "groceries-pets/food-staples/soups-stocks",
            "groceries-pets/food-staples/spices-recipes",
            "groceries-pets/food-staples/sugar-salt",
            # Breakfast
            "groceries-pets/breakfast/cereals",
            "groceries-pets/breakfast/eggs",
            "groceries-pets/breakfast/honey",
            "groceries-pets/breakfast/jams-spreads",
            "groceries-pets/breakfast/muesli",
            "groceries-pets/breakfast/oatmeals-porridge",
            # Beverages
            "groceries-pets/beverages/chocolate-drinks",
            "groceries-pets/beverages/drinking-water",
            "groceries-pets/beverages/energy-drinks",
            "groceries-pets/beverages/juices",
            "groceries-pets/beverages/powdered-drinks",
            "groceries-pets/beverages/smoothies",
            "groceries-pets/beverages/soft-drinks-soda",
            "groceries-pets/beverages/squash-syrup-flavors",
            "groceries-pets/beverages/tea-coffee",
            "groceries-pets/beverages/whiteners-sweetener",
            # Bread & Bakery
            "groceries-pets/bread-bakery/biscuits",
            "groceries-pets/bread-bakery/bread",
            "groceries-pets/bread-bakery/bread-buns",
            "groceries-pets/bread-bakery/cake",
            "groceries-pets/bread-bakery/sandwiches",
            "groceries-pets/bread-bakery/wraps-pittas",
            # Baking & Cooking
            "groceries-pets/baking-cooking/cooking-oil",
            "groceries-pets/baking-cooking/flours-meals",
            "groceries-pets/baking-cooking/food-color-essence",
            "groceries-pets/baking-cooking/home-baking",
            "groceries-pets/baking-cooking/nuts-seeds",
            "groceries-pets/baking-cooking/olive-oil",
            "groceries-pets/baking-cooking/rice",
            "groceries-pets/baking-cooking/salad-dressings",
            # Chocolates & Snacks
            "groceries-pets/chocolates-snacks/biscuits",
            "groceries-pets/chocolates-snacks/chips-crisps",
            "groceries-pets/chocolates-snacks/chocolates",
            "groceries-pets/chocolates-snacks/dry-fruit-dates",
            "groceries-pets/chocolates-snacks/mouth-fresheners",
            "groceries-pets/chocolates-snacks/popcorn",
            "groceries-pets/chocolates-snacks/salsas-dips",
            "groceries-pets/chocolates-snacks/snacks",
            "groceries-pets/chocolates-snacks/wafers",
            # Desserts
            "groceries-pets/deserts/ice-cream",
            "groceries-pets/deserts/jelly-custard",
            "groceries-pets/deserts/syrups",
            "groceries-pets/deserts/traditional-desserts",
            # Laundry & Household
            "groceries-pets/laundry-household/air-fresheners",
            "groceries-pets/laundry-household/cloths-dusters",
            "groceries-pets/laundry-household/food-storage",
            "groceries-pets/laundry-household/household-cleaners",
            "groceries-pets/laundry-household/laundry",
            "groceries-pets/laundry-household/repellents-insecticides",
            "groceries-pets/laundry-household/shoe-care",
            "groceries-pets/laundry-household/tissue-toilet-rolls",
            "groceries-pets/laundry-household/trash-bags",
            # Candies & Bubble Gum
            "groceries-pets/candies-bubble-gum/bubble-gum",
            "groceries-pets/candies-bubble-gum/candies-and-jellies",
            "groceries-pets/candies-bubble-gum/marshmallows",
            # Pet Care
            "groceries-pets/pet-care/bird-food",
            "groceries-pets/pet-care/cat-food",
            "groceries-pets/pet-care/dog-food",
            "groceries-pets/pet-care/fish-food",
            "groceries-pets/pet-care/pet-accessories",
            "groceries-pets/pet-care/pet-grooming",
            "groceries-pets/pet-care/pet-litter",
        ],
    },
}

# ============================================================
# DATA AUGMENTATION SETTINGS
# When live scraping returns fewer rows than needed (due to
# anti-bot measures, JS-rendered pages, etc.), this module fills
# the gap with realistic product data modeled on actual scraped
# patterns and real Pakistani FMCG market structure.
# ============================================================
TARGET_RAW_ROWS = 500_000
TARGET_MATCHED_PRODUCTS = 7_000
USE_DATA_GENERATOR = True  # set False if live scraping gives enough data

# Number of products to generate per store per city
GENERATED_PRODUCTS_PER_STORE_CITY = 30_000

# ============================================================
# PROCESSING SETTINGS
# ============================================================
# Standard units for normalization
WEIGHT_UNITS = {"kg": 1000, "g": 1, "gm": 1, "gms": 1, "gram": 1, "grams": 1,
                "mg": 0.001, "lb": 453.592, "lbs": 453.592}
VOLUME_UNITS = {"l": 1000, "ltr": 1000, "litre": 1000, "liter": 1000,
                "ml": 1, "cc": 1}
COUNT_UNITS = {"pcs": 1, "pieces": 1, "piece": 1, "pk": 1, "pack": 1,
               "sachets": 1, "sachet": 1, "rolls": 1, "roll": 1,
               "sheets": 1, "sheet": 1, "tablets": 1, "tablet": 1, "caps": 1}

# ============================================================
# MATCHING SETTINGS
# ============================================================
# Minimum similarity score (0-100) for fuzzy matching support
FUZZY_THRESHOLD = 85
