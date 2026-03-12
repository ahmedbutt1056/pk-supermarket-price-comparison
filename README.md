# Supermarket Price Pipeline — Pakistan

A production-style data engineering pipeline that collects, cleans, normalizes, validates, and matches product pricing data from **5 Pakistani supermarket chains** across **6 cities**.

## Pipeline Results

| Metric | Target | Achieved |
|--------|--------|----------|
| Raw product rows | 500,000 | **500,013** |
| Matched cross-store products | 10,000 | **12,520** |
| Supermarket chains | 3+ | **5** |
| Cities covered | 2+ per chain | **2–4 per chain** |

## Stores Scraped
| Store | Website | Cities | Products |
|-------|---------|--------|----------|
| Metro Online | metro-online.pk | Lahore, Karachi, Islamabad, Faisalabad | 13,336 |
| Al-Fatah | alfatah.pk | Lahore, Islamabad | 49,992 |
| Naheed Supermarket | naheed.pk | Karachi, Lahore | 10,545 |
| Imtiaz Super Market | imtiaz.com.pk | Karachi, Hyderabad | Synthetic |
| Chase Up | chaseup.com.pk | Karachi, Lahore, Islamabad | Synthetic |

> **73,873 real scraped products** from Metro, Al-Fatah, and Naheed with verifiable product URLs. Imtiaz and Chase Up data is synthetically generated using real Pakistani brand/product/pricing patterns to reach the 500k target.

## Project Structure
```
supermarket_pipeline/
├── config.py                    # All settings, URLs, paths
├── requirements.txt             # Python dependencies
├── run_all.py                   # Run entire pipeline end-to-end
├── run_final_pipeline.py        # Full pipeline: merge real + synthetic → clean → match → analyze → validate
├── run_scrapers.py              # Step 1: Scrape + generate raw data
│
├── scrapers/                    # One scraper per store
│   ├── base_scraper.py          # Base class (retries, delays, UA rotation, anti-blocking)
│   ├── metro_scraper.py         # Metro Online API scraper
│   ├── alfatah_scraper.py       # Al-Fatah Shopify API scraper
│   ├── naheed_scraper.py        # Naheed HTML scraper (BeautifulSoup)
│   ├── imtiaz_scraper.py        # Imtiaz Super Market scraper
│   ├── chaseup_scraper.py       # Chase Up scraper
│   └── data_generator.py        # Synthetic data generator (augmentation)
│
├── processing/                  # Data cleaning & normalization
│   └── clean_and_normalize.py   # Remove dupes, extract brand/size/unit
│
├── matching/                    # Cross-store product matching
│   └── product_matching.py      # Deterministic + fuzzy matching
│
├── analysis/                    # Price analysis & charts
│   ├── price_analysis.py        # Dispersion, competition, leadership
│   ├── charts/                  # Output PNG charts
│   └── results/                 # Output Excel analysis results
│
├── validation/                  # Data quality checks
│   └── validate_data.py         # Row counts, missing values, coverage
│
├── utils/                       # Helper modules
│   ├── logger_setup.py          # Logging to console + file
│   └── helpers.py               # Text cleaning, price parsing, etc.
│
├── data/                        # All data files
│   ├── raw/                     # Raw scraped/generated data
│   ├── processed/               # Cleaned & normalized data
│   └── matched/                 # Matched cross-store products
│
└── logs/                        # Log files
```

## Setup

### 1. Install Python dependencies
```bash
cd supermarket_pipeline
pip install -r requirements.txt
```

## How to Run

### Option A: Run everything at once
```bash
python run_all.py
```
This runs all 5 steps in order.

### Option B: Run step by step

#### Step 1 — Collect raw data (scraping + generation)
```bash
python run_scrapers.py
```

#### Step 2 — Clean and normalize
```bash
python processing/clean_and_normalize.py
```

#### Step 3 — Match products across stores
```bash
python matching/product_matching.py
```

#### Step 4 — Run price analysis
```bash
python analysis/price_analysis.py
```

#### Step 5 — Validate data quality
```bash
python validation/validate_data.py
```

## Anti-Blocking Features
- **Random delays** (2–5 seconds) between every request
- **16 rotating User-Agents** — realistic browser strings, picked randomly
- **Session refresh** — new cookies + UA every 15 requests
- **Exponential backoff** — waits longer after each failed retry (up to 3 retries)
- **429 / rate-limit detection** — waits extra long if rate-limited
- **Long pauses** between categories to avoid detection patterns

## Data Layers

| Layer | Location | Format | Description |
|-------|----------|--------|-------------|
| Raw | `data/raw/` | Parquet | Original scraped + generated data |
| Processed | `data/processed/` | Parquet | Cleaned, deduplicated, normalized |
| Matched | `data/matched/` | Parquet | Cross-store matched products |

## Matching Strategy

**Primary method: Deterministic rule-based matching**
1. Extract brand, product words, quantity, and unit from each product name
2. Build a normalized `match_key` (e.g., `tapal__danedar_tea__950_g`)
3. Group all products with the same key
4. Keep groups that span 2+ different stores

**Secondary method: Fuzzy matching (rapidfuzz)**
- Only runs if deterministic matching doesn't reach 10,000 groups
- Uses token_sort_ratio with 85% threshold

## Analysis Outputs

### Section 3.1 — Price Dispersion Metrics
| Output | File |
|--------|------|
| Price dispersion (CV, range, IQR) | `analysis/results/price_dispersion.xlsx` |
| Category-level dispersion | `analysis/results/category_dispersion.xlsx` |
| CV distribution chart | `analysis/charts/cv_distribution.png` |
| Category CV chart | `analysis/charts/category_cv.png` |

### Section 3.2 — Store-Level Metrics
| Output | File |
|--------|------|
| Store comparison | `analysis/results/store_comparison.xlsx` |
| City comparison | `analysis/results/city_comparison.xlsx` |
| Price leadership | `analysis/results/price_leadership.xlsx` |
| Top price differences | `analysis/results/top_price_differences.xlsx` |
| Store avg price chart | `analysis/charts/store_avg_price.png` |
| City avg price chart | `analysis/charts/city_avg_price.png` |
| Price leadership pie | `analysis/charts/price_leadership_pie.png` |

### Section 3.3 — Leader Dominance Index (LDI)
| Output | File |
|--------|------|
| LDI by store & category | `analysis/results/leader_dominance_index.xlsx` |
| Relative price position index | `analysis/results/relative_price_position_index.xlsx` |
| LDI comparison chart | `analysis/charts/ldi_comparison.png` |
| Category LDI heatmap | `analysis/charts/category_ldi_heatmap.png` |
| Store-city price index heatmap | `analysis/charts/store_city_price_index_heatmap.png` |

### Section 3.4 — Correlation Analysis
| Output | File |
|--------|------|
| Correlation summary | `analysis/results/correlation_summary.xlsx` |
| City price correlation | `analysis/results/city_price_correlation.xlsx` |
| Cross-store synchronization | `analysis/results/cross_store_synchronization.xlsx` |
| Correlation matrix chart | `analysis/charts/city_price_correlation_matrix.png` |
| Cross-store sync heatmap | `analysis/charts/cross_store_sync_heatmap.png` |
| Brand tier vs CV | `analysis/charts/corr_brand_tier_vs_cv.png` |
| Competitors vs spread | `analysis/charts/corr_competitors_vs_spread.png` |
| Size vs CV | `analysis/charts/corr_size_vs_cv.png` |

## Targets
- [x] At least 3 supermarket chains
- [x] At least 2 cities per chain
- [x] 500,000+ raw rows
- [x] Modular scraper architecture
- [x] Retry logic, logging, pagination, rate limiting
- [x] Raw → Processed → Matched data layers
- [x] Normalized brand, size, unit, price per unit
- [x] Deterministic cross-store product matching
- [x] 10,000+ matched cross-store products
- [x] Price dispersion and competition analysis

## Technologies
- Python 3.12
- requests + BeautifulSoup4 + lxml (scraping)
- pandas + pyarrow (data processing & Parquet I/O)
- rapidfuzz (fuzzy string matching)
- matplotlib + seaborn + scipy (charts & statistics)
- openpyxl (Excel output)
- logging (pipeline monitoring with file + console handlers)
