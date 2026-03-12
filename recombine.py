"""Recombine all raw data including Jalal Sons."""
import pandas as pd
from pathlib import Path

RAW = Path("data/raw")

# Check jalal files
for f in sorted(RAW.glob("raw_jalalsons_*.parquet")):
    df = pd.read_parquet(f)
    print(f"{f.name}: {len(df)} rows, store_key={df['store_key'].iloc[0]}")

# Combine all
dfs = []
for f in sorted(RAW.glob("raw_*_*.parquet")):
    if f.name.startswith("all_"):
        continue
    dfs.append(pd.read_parquet(f))
    
combined = pd.concat(dfs, ignore_index=True)
print(f"\nBefore dedup: {len(combined):,}")
combined = combined.drop_duplicates(subset=["store_key", "city", "product_name"], keep="first")
print(f"After dedup: {len(combined):,}")

# Fix mixed-type columns for parquet compatibility
for col in combined.columns:
    if combined[col].dtype == object:
        combined[col] = combined[col].astype("string")

for s, c in combined.groupby("store_key").size().sort_values(ascending=False).items():
    print(f"  {s}: {c:,}")

combined.to_parquet(RAW / "all_raw_data.parquet", index=False, engine="pyarrow")
print("Saved all_raw_data.parquet")
