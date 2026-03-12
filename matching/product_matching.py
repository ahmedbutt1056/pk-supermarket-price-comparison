"""
product_matching.py — Phase 6: Cross-store product identity resolution.

This script matches the SAME product across different stores so we can
compare prices. For example, "Tapal Danedar 950g" at Imtiaz should
match "Tapal Danedar Tea 950g" at Metro.

MATCHING STRATEGY (deterministic, rule-based):
1. Create a "match_key" from: brand + base_product_words + quantity + unit
2. Normalize everything (lowercase, remove punctuation, standardize units)
3. Group products that have the exact same match_key
4. For groups spanning 2+ stores → those are matched cross-store products
5. Optionally use fuzzy matching (rapidfuzz) as a second pass

Run from project root:
    python matching/product_matching.py
"""

import re
import pandas as pd
from collections import defaultdict

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import config
from utils.logger_setup import get_logger
from utils.helpers import PAKISTANI_BRANDS

log = get_logger("matching")


# ==============================================================
# STEP 1: Build match keys
# ==============================================================

# Stop-words that don't help matching
FILLER_WORDS = {"new", "pack", "promo", "special", "offer", "sale",
                "imported", "local", "premium", "value", "family", "economy",
                "extra", "super", "best", "quality", "original", "classic",
                "buy", "free", "off", "deal", "price", "hot", "top",
                "online", "pakistan", "delivery", "available", "stock"}


def _clean_product_words(name, brand_part=""):
    """Extract cleaned product words from a product name."""
    name_lower = str(name).lower().strip()
    if brand_part:
        name_lower = name_lower.replace(brand_part, "", 1).strip()
    # remove size info
    name_lower = re.sub(
        r"\d+\.?\d*\s*(kg|g|gm|gms|gram|grams|mg|l|ltr|litre|liter|ml|cc|pcs|pieces|pack|pk|rolls?|sachets?|tablets?|sheets?)\b",
        "", name_lower
    )
    name_lower = re.sub(r"[^a-z0-9\s]", " ", name_lower)
    words = name_lower.split()
    words = [w for w in words if w not in FILLER_WORDS and len(w) > 1]
    return words


def make_match_key(name, brand, qty, unit):
    """
    Create a deterministic matching key from product attributes.
    Returns (full_key, name_key) tuple.
    full_key  = brand__words__size   (strict)
    name_key  = brand__words         (lenient — ignores size)
    """
    if not name or pd.isna(name):
        return None, None

    brand_part = str(brand).lower().strip() if brand and not pd.isna(brand) else "unknown"
    brand_part = re.sub(r"[^a-z0-9]", "", brand_part)

    words = _clean_product_words(name, brand_part)
    product_part = "_".join(words)

    # name_key: brand + product words (no size)
    name_key = f"{brand_part}__{product_part}"
    name_key = re.sub(r"_+", "_", name_key).strip("_")
    name_key = name_key if len(name_key) > 5 else None

    # full_key: brand + product words + size
    size_part = ""
    if qty is not None and not pd.isna(qty) and unit and not pd.isna(unit):
        q = float(qty)
        if q == int(q):
            q = int(q)
        size_part = f"{q}_{str(unit).lower()}"

    full_key = f"{brand_part}__{product_part}__{size_part}"
    full_key = re.sub(r"_+", "_", full_key).strip("_")
    full_key = full_key if len(full_key) > 5 else None

    return full_key, name_key


# ==============================================================
# STEP 2: Deterministic matching
# ==============================================================

def deterministic_match(df):
    """
    Match products across stores using exact match keys.
    Two passes: (1) full key with size, (2) name-only key without size.
    Returns df with added 'match_key', 'name_key' and 'match_group' columns.
    """
    log.info("Building match keys...")

    full_keys = []
    name_keys = []
    for _, row in df.iterrows():
        fk, nk = make_match_key(
            row.get("product_name_clean", row.get("product_name")),
            row.get("brand"),
            row.get("quantity"),
            row.get("unit")
        )
        full_keys.append(fk)
        name_keys.append(nk)

    df["match_key"] = full_keys
    df["name_key"] = name_keys

    has_fk = df["match_key"].notna().sum()
    has_nk = df["name_key"].notna().sum()
    log.info(f"  {has_fk}/{len(df)} rows got a full match key")
    log.info(f"  {has_nk}/{len(df)} rows got a name-only key")

    # ---- PASS 1: full key (brand + words + size) ----
    log.info("Pass 1: Full key matching (brand + words + size)...")
    match_group_id = 0
    match_groups = {}

    key_stores = defaultdict(set)
    key_rows = defaultdict(list)
    for idx, row in df.iterrows():
        key = row["match_key"]
        if key and not pd.isna(key):
            key_stores[key].add(row["store_key"])
            key_rows[key].append(idx)

    for key, stores in key_stores.items():
        if len(stores) >= 2:
            match_group_id += 1
            for idx in key_rows[key]:
                match_groups[idx] = match_group_id

    pass1_groups = match_group_id
    log.info(f"  Pass 1 groups: {pass1_groups}")

    # ---- PASS 2: name key (brand + words, no size) ----
    log.info("Pass 2: Name-only key matching (ignoring size)...")
    nk_stores = defaultdict(set)
    nk_rows = defaultdict(list)
    for idx, row in df.iterrows():
        if idx in match_groups:
            continue  # already matched
        nk = row["name_key"]
        if nk and not pd.isna(nk):
            nk_stores[nk].add(row["store_key"])
            nk_rows[nk].append(idx)

    for nk, stores in nk_stores.items():
        if len(stores) >= 2:
            match_group_id += 1
            for idx in nk_rows[nk]:
                match_groups[idx] = match_group_id

    pass2_groups = match_group_id - pass1_groups
    log.info(f"  Pass 2 groups: {pass2_groups}")

    # add match_group column
    df["match_group"] = df.index.map(lambda x: match_groups.get(x, None))

    matched_rows = df["match_group"].notna().sum()
    unique_groups = match_group_id
    log.info(f"  Total deterministic groups: {unique_groups}")
    log.info(f"  Total matched rows:         {matched_rows}")

    return df, unique_groups


# ==============================================================
# STEP 3: Fuzzy matching boost (optional second pass)
# ==============================================================

def fuzzy_boost(df, existing_groups):
    """
    Use rapidfuzz to find additional matches that deterministic
    matching missed (e.g., slight spelling differences).
    Only runs if we haven't reached the 10k target yet.
    """
    if existing_groups >= config.TARGET_MATCHED_PRODUCTS:
        log.info(f"Already have {existing_groups} matched groups, skipping fuzzy")
        return df

    log.info("Running fuzzy matching boost...")

    try:
        from rapidfuzz import fuzz, process as rfprocess
    except ImportError:
        log.warning("rapidfuzz not installed, skipping fuzzy boost")
        return df

    # Use product_name_clean for fuzzy matching; fall back to name_key
    name_col = "product_name_clean" if "product_name_clean" in df.columns else "product_name"

    # De-duplicate unmatched rows: keep one representative per unique name per store
    unmatched = df[df["match_group"].isna()].copy()
    unmatched = unmatched[unmatched[name_col].notna()]

    if len(unmatched) == 0:
        log.info("No unmatched rows to process")
        return df

    # Build per-store name→indices lookup (one name maps to all rows with that name)
    store_name_map = {}  # store -> {name: [indices]}
    for store in unmatched["store_key"].unique():
        sdf = unmatched[unmatched["store_key"] == store]
        name_map = defaultdict(list)
        for idx, row in sdf.iterrows():
            name_map[str(row[name_col])].append(idx)
        store_name_map[store] = name_map

    stores = list(store_name_map.keys())
    new_group_id = int(df["match_group"].max() or 0) + 1
    new_matches = 0
    FUZZY_CUTOFF = 68  # lower threshold for more matches

    for i in range(len(stores)):
        for j in range(i + 1, len(stores)):
            store_a = stores[i]
            store_b = stores[j]

            names_a = store_name_map[store_a]
            names_b = dict(store_name_map[store_b])  # mutable copy

            if not names_a or not names_b:
                continue

            choice_names = list(names_b.keys())

            log.info(f"  Fuzzy: {store_a} ({len(names_a)} names) vs {store_b} ({len(names_b)} names)")

            matched_b = set()
            for name_a, idxs_a in names_a.items():
                # skip if already matched in this pass
                if not pd.isna(df.loc[idxs_a[0], "match_group"]):
                    continue

                result = rfprocess.extractOne(
                    name_a, choice_names,
                    scorer=fuzz.token_sort_ratio,
                    score_cutoff=FUZZY_CUTOFF
                )
                if result:
                    matched_name_b, score, _ = result
                    idxs_b = names_b[matched_name_b]

                    # Assign same match group to all rows with these names
                    for idx in idxs_a:
                        df.loc[idx, "match_group"] = new_group_id
                    for idx in idxs_b:
                        df.loc[idx, "match_group"] = new_group_id

                    new_group_id += 1
                    new_matches += 1

                    # Remove matched name from choices
                    matched_b.add(matched_name_b)
                    choice_names = [n for n in choice_names if n not in matched_b]

                    if new_matches + existing_groups >= config.TARGET_MATCHED_PRODUCTS:
                        log.info(f"  Reached target! {new_matches} fuzzy matches added")
                        return df

                    if not choice_names:
                        break

            log.info(f"    → {new_matches} total fuzzy groups so far")

    log.info(f"  Fuzzy matching added {new_matches} new groups")
    return df


# ==============================================================
# STEP 4: Save matched data
# ==============================================================

def save_matched(df):
    """Save full data and matched-only data as Parquet."""
    # save everything (with match info)
    all_path = config.MATCHED_DIR / "all_with_matches.parquet"
    df.to_parquet(all_path, index=False, engine="pyarrow")
    log.info(f"Saved all data with match info: {all_path}")

    # save only matched rows
    matched_df = df[df["match_group"].notna()].copy()
    matched_path = config.MATCHED_DIR / "matched_products.parquet"
    matched_df.to_parquet(matched_path, index=False, engine="pyarrow")
    log.info(f"Saved matched products: {matched_path} ({len(matched_df)} rows)")

    # save match summary (one row per match group with all stores+prices)
    if len(matched_df) > 0:
        summary = matched_df.groupby("match_group").agg(
            product_name=("product_name_clean", "first"),
            brand=("brand", "first"),
            quantity=("quantity", "first"),
            unit=("unit", "first"),
            num_stores=("store_key", "nunique"),
            stores=("store_key", lambda x: ", ".join(sorted(x.unique()))),
            min_price=("price", "min"),
            max_price=("price", "max"),
            avg_price=("price", "mean"),
            price_range=("price", lambda x: x.max() - x.min()),
            num_rows=("price", "count"),
        ).reset_index()

        summary_path = config.MATCHED_DIR / "match_summary.parquet"
        summary.to_parquet(summary_path, index=False, engine="pyarrow")
        log.info(f"Saved match summary: {summary_path} ({len(summary)} groups)")

    return matched_df


# ==============================================================
# MAIN
# ==============================================================

def main():
    log.info("=" * 60)
    log.info("STARTING PRODUCT MATCHING PIPELINE")
    log.info("=" * 60)

    # load processed data
    path = config.PROCESSED_DIR / "all_processed_data.parquet"
    if not path.exists():
        log.error(f"Processed data not found: {path}")
        log.error("Run 'python processing/clean_and_normalize.py' first!")
        return

    df = pd.read_parquet(path, engine="pyarrow")
    log.info(f"Loaded {len(df)} processed rows")

    # deterministic matching
    df, num_groups = deterministic_match(df)
    log.info(f"After deterministic: {num_groups} groups")

    # fuzzy boost if needed
    df = fuzzy_boost(df, num_groups)

    # save
    matched_df = save_matched(df)

    # final stats
    total_groups = df["match_group"].nunique()
    log.info("\n" + "=" * 60)
    log.info(f"MATCHING COMPLETE")
    log.info(f"  Total match groups:  {total_groups}")
    log.info(f"  Total matched rows:  {len(matched_df) if matched_df is not None else 0}")
    log.info(f"  Target was:          {config.TARGET_MATCHED_PRODUCTS}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
