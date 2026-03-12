"""
helpers.py — Small helper functions used across the pipeline.
"""

import re
import unicodedata


def clean_text(text):
    """Remove extra whitespace, newlines, and strip a string."""
    if not text:
        return ""
    # Normalize unicode characters
    text = unicodedata.normalize("NFKD", str(text))
    # Replace multiple spaces/newlines with single space
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_price(price_text):
    """
    Extract numeric price from text like 'Rs. 1,250.00' or 'PKR 999'.
    Returns float or None.
    """
    if not price_text:
        return None
    # Remove currency symbols and text
    price_text = str(price_text)
    price_text = re.sub(r"[Rr][Ss]\.?|PKR|pkr|/-|,", "", price_text)
    price_text = price_text.strip()
    # Find the first number (with optional decimal)
    match = re.search(r"(\d+\.?\d*)", price_text)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None


def extract_quantity_and_unit(text):
    """
    Extract quantity and unit from product name.
    Examples:
        'Tapal Danedar 950g'   -> (950.0, 'g')
        'Olpers Milk 1.5 Ltr'  -> (1.5, 'l')
        'Surf Excel 2kg'       -> (2.0, 'kg')
        'Colgate 150ml'        -> (150.0, 'ml')
        'Lays Classic 40g x12' -> (40.0, 'g')   (pack multiplier ignored for now)
    Returns:
        (quantity, unit) tuple. Both can be None if not found.
    """
    if not text:
        return None, None
    
    text_lower = str(text).lower()
    
    # Pattern: number followed by unit (with optional space)
    # Examples: 950g, 1.5 ltr, 2 kg, 150ml
    pattern = r"(\d+\.?\d*)\s*(kg|g|gm|gms|gram|grams|mg|l|ltr|litre|liter|ml|cc|pcs|pieces|pack|pk|rolls|sachets|tablets|sheets)\b"
    
    match = re.search(pattern, text_lower)
    if match:
        quantity = float(match.group(1))
        unit = match.group(2)
        # Standardize common unit names
        unit_map = {
            "gm": "g", "gms": "g", "gram": "g", "grams": "g",
            "ltr": "l", "litre": "l", "liter": "l",
            "pcs": "pcs", "pieces": "pcs", "pack": "pack", "pk": "pack",
            "rolls": "roll", "sachets": "sachet", "tablets": "tablet",
            "sheets": "sheet",
        }
        unit = unit_map.get(unit, unit)
        return quantity, unit
    
    return None, None


def extract_brand(product_name, known_brands=None):
    """
    Extract brand name from product title.
    Simple approach: the first word (or first two words) is usually the brand.
    If a list of known_brands is provided, try to match against it.
    """
    if not product_name:
        return "Unknown"
    
    words = str(product_name).strip().split()
    if not words:
        return "Unknown"
    
    # If we have known brands, check if product starts with any
    if known_brands:
        name_lower = product_name.lower()
        for brand in known_brands:
            if name_lower.startswith(brand.lower()):
                return brand.title()
    
    # Default: first word as brand
    brand = words[0]
    # Clean up the brand name
    brand = re.sub(r"[^a-zA-Z0-9\s\-]", "", brand)
    return brand.strip().title() if brand.strip() else "Unknown"


def normalize_product_key(name, brand, quantity, unit):
    """
    Create a normalized key for product matching.
    This key is used to match the same product across stores.
    
    Example: 'tapal_danedar_950_g' 
    """
    parts = []
    
    # Clean and lowercase the name
    if name:
        clean = re.sub(r"[^a-z0-9\s]", "", str(name).lower())
        # Remove extra spaces
        clean = re.sub(r"\s+", " ", clean).strip()
        parts.append(clean.replace(" ", "_"))
    
    # Add quantity and unit
    if quantity is not None and unit:
        parts.append(f"{quantity}_{unit}")
    
    key = "__".join(parts) if parts else "unknown"
    return key


# List of common Pakistani FMCG brands (used for brand extraction)
PAKISTANI_BRANDS = [
    # Tea & Coffee
    "Tapal", "Lipton", "Vital", "Supreme", "Tetley", "Nescafe",
    # Dairy
    "Olpers", "Milkpak", "Nurpur", "Haleeb", "Good Milk", "Dairy Queen",
    "Nestle", "Adams",
    # Cooking Oil / Ghee
    "Dalda", "Habib", "Sufi", "Mezan", "Eva", "Seasons", "Soya Supreme",
    # Rice / Flour
    "Guard", "Matco", "Falak", "Sunridge", "Reem",
    # Beverages
    "Pepsi", "Coca Cola", "7Up", "Sprite", "Fanta", "Dew", "Pakola",
    "Sting", "Red Bull", "Tang", "Rooh Afza",
    # Snacks
    "Lays", "Kurkure", "Super Crisp", "Kolson", "Peek Freans", "LU",
    "Bisconni", "Oreo", "Prince",
    # Personal Care
    "Safeguard", "Lifebuoy", "Lux", "Dove", "Pantene", "Head Shoulders",
    "Sunsilk", "Clear", "Colgate", "Sensodyne", "Close Up", "Pepsodent",
    # Cleaning
    "Surf Excel", "Ariel", "Bonus", "Brite", "Express Power",
    "Harpic", "Vim", "Dettol",
    # Baby
    "Pampers", "Huggies", "Canbebe", "Baby Dee",
    # Spices
    "National", "Shan", "Mehran",
    # Sauces
    "Mitchell", "Shangrila", "National", "Heinz", "Knorr",
]
