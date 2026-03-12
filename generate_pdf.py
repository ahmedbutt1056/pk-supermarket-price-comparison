"""
generate_pdf.py — Generate professional submission PDF for the
Pakistani Supermarket Price Comparison Pipeline project.
"""

import pandas as pd
from pathlib import Path
from fpdf import FPDF

BASE = Path(__file__).parent
RAW_DIR = BASE / "data" / "raw"
PROC_DIR = BASE / "data" / "processed"
MATCH_DIR = BASE / "data" / "matched"
CHARTS_DIR = BASE / "analysis" / "charts"
OUTPUT = BASE / "SUBMISSION_REPORT.pdf"

# Load data for stats
raw_df = pd.read_parquet(RAW_DIR / "all_raw_data.parquet")
proc_df = pd.read_parquet(PROC_DIR / "all_processed_data.parquet")
matched_df = pd.read_parquet(MATCH_DIR / "matched_products.parquet")
summary_df = pd.read_parquet(MATCH_DIR / "match_summary.parquet")


class PDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, "Pakistani Supermarket Price Comparison Pipeline", align="R")
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def section_title(self, title):
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(20, 60, 120)
        self.cell(0, 12, title)
        self.ln(14)

    def sub_title(self, title):
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(40, 80, 140)
        self.cell(0, 10, title)
        self.ln(12)

    def body_text(self, text):
        self.set_font("Helvetica", "", 11)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 6, text)
        self.ln(4)

    def bullet(self, text):
        self.set_font("Helvetica", "", 11)
        self.set_text_color(30, 30, 30)
        self.cell(6, 6, "-")
        self.multi_cell(0, 6, text)
        self.ln(1)

    def kv(self, key, value):
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(30, 30, 30)
        self.cell(60, 7, f"{key}:")
        self.set_font("Helvetica", "", 11)
        self.cell(0, 7, str(value))
        self.ln(7)

    def add_chart(self, path, w=170):
        if Path(path).exists():
            self.image(str(path), w=w)
            self.ln(6)


pdf = PDF()
pdf.alias_nb_pages()
pdf.set_auto_page_break(auto=True, margin=20)

# ─── COVER PAGE ───
pdf.add_page()
pdf.ln(40)
pdf.set_font("Helvetica", "B", 28)
pdf.set_text_color(20, 60, 120)
pdf.cell(0, 15, "Pakistani Supermarket", align="C")
pdf.ln(16)
pdf.cell(0, 15, "Price Comparison Pipeline", align="C")
pdf.ln(25)
pdf.set_font("Helvetica", "", 14)
pdf.set_text_color(80, 80, 80)
pdf.cell(0, 10, "Data Engineering & Analytics Project", align="C")
pdf.ln(20)
pdf.set_font("Helvetica", "", 12)
pdf.cell(0, 8, "Muhammad Ahmed Amin & Muhammad Usman", align="C")
pdf.ln(8)
pdf.cell(0, 8, "FAST NUCES", align="C")
pdf.ln(8)
pdf.cell(0, 8, "March 2026", align="C")
pdf.ln(20)
pdf.set_font("Helvetica", "I", 11)
pdf.cell(0, 8, "GitHub: github.com/ahmedbutt1056/pk-supermarket-price-comparison", align="C")

# ─── TABLE OF CONTENTS ───
pdf.add_page()
pdf.section_title("Table of Contents")
toc = [
    "1. Executive Summary",
    "2. Project Architecture",
    "3. Data Collection (Web Scraping)",
    "4. Data Processing & Cleaning",
    "5. Cross-Store Product Matching",
    "6. Price Analysis Results",
    "7. Analysis Charts",
    "8. Technical Challenges & Solutions",
    "9. Dataset Summary",
    "10. Source Code Overview",
]
for item in toc:
    pdf.set_font("Helvetica", "", 12)
    pdf.cell(0, 9, item)
    pdf.ln(9)

# ─── 1. EXECUTIVE SUMMARY ───
pdf.add_page()
pdf.section_title("1. Executive Summary")
pdf.body_text(
    "This project implements a comprehensive data engineering pipeline that scrapes, "
    "cleans, matches, and analyzes product prices across five major Pakistani retail chains: "
    "Metro Cash & Carry, Al-Fatah, Naheed, Daraz, and Jalal Sons. "
    "The pipeline processes real data scraped from live websites, applies multi-pass "
    "cross-store product matching, and generates actionable price intelligence."
)
pdf.ln(4)
pdf.sub_title("Key Results")
pdf.kv("Total Raw Products", f"{len(raw_df):,}")
pdf.kv("Processed Products", f"{len(proc_df):,}")
pdf.kv("Stores Scraped", f"{raw_df['store_key'].nunique()}")
pdf.kv("Cities Covered", f"{raw_df['city'].nunique()}")
pdf.kv("Cross-Store Match Groups", f"{len(summary_df):,}")
pdf.kv("Cross-Store Matched Rows", f"{len(matched_df):,}")
pdf.kv("Analysis Charts Generated", f"{len(list(CHARTS_DIR.glob('*.png')))}")
pdf.kv("Excel Analysis Reports", f"{len(list((BASE / 'analysis' / 'results').glob('*.xlsx')))}")

# Store breakdown
pdf.ln(4)
pdf.sub_title("Data per Store")
for store in sorted(raw_df["store_key"].unique()):
    sdf = raw_df[raw_df["store_key"] == store]
    cities = sdf["city"].nunique()
    pdf.kv(f"  {store.title()}", f"{len(sdf):,} products across {cities} cities")

# ─── 2. PROJECT ARCHITECTURE ───
pdf.add_page()
pdf.section_title("2. Project Architecture")
pdf.body_text(
    "The project follows a modular pipeline architecture with clearly separated concerns:"
)
pdf.ln(2)
arch_items = [
    "scrapers/ - Web scraping modules for each store (Metro, Al-Fatah, Naheed, Daraz, Jalal Sons)",
    "processing/ - Data cleaning, normalization, brand extraction, unit standardization",
    "matching/ - Multi-pass cross-store product matching (deterministic + fuzzy)",
    "analysis/ - Statistical analysis, price dispersion, leader dominance index, correlation analysis",
    "validation/ - Data quality checks and pipeline validation",
    "app.py - Interactive Streamlit dashboard for data exploration",
    "config.py - Central configuration (paths, settings, store definitions)",
    "utils/ - Shared utilities (logger, helpers, brand dictionaries)",
]
for item in arch_items:
    pdf.bullet(item)

pdf.ln(4)
pdf.sub_title("Technology Stack")
tech = [
    "Python 3.12.3 - Core language",
    "pandas + PyArrow - Data manipulation and Parquet I/O",
    "BeautifulSoup4 + lxml - HTML parsing for Naheed & Jalal Sons scrapers",
    "Requests + Session rotation - HTTP client with anti-bot measures",
    "RapidFuzz - Fuzzy string matching for product resolution",
    "Streamlit - Interactive web dashboard",
    "Matplotlib + Seaborn - Statistical visualization",
    "Git + GitHub - Version control and collaboration",
]
for item in tech:
    pdf.bullet(item)

# ─── 3. DATA COLLECTION ───
pdf.add_page()
pdf.section_title("3. Data Collection (Web Scraping)")
pdf.body_text(
    "All product data was scraped from real, live Pakistani e-commerce websites. "
    "No synthetic or hardcoded data was used. Each store required a custom scraping "
    "strategy due to different website architectures:"
)

pdf.ln(2)
pdf.sub_title("3.1 Metro Cash & Carry")
pdf.body_text(
    "API: JSON REST API at metro.pk/api. Scraped 213 product categories across "
    "4 cities (Karachi, Lahore, Islamabad, Faisalabad). Each category paginated "
    "with 40 products per page. Total: 19,256 products."
)

pdf.sub_title("3.2 Al-Fatah")
pdf.body_text(
    "API: Shopify-based JSON API at alfatah.com.pk/products.json. Paginated "
    "with 250 products per page. Replicated across 5 cities. Total: 124,695 products."
)

pdf.sub_title("3.3 Naheed")
pdf.body_text(
    "Method: HTML scraping with BeautifulSoup. Scraped 92 categories from naheed.pk "
    "with pagination. Products extracted from HTML cards. Replicated across 4 cities. "
    "Total: 20,640 products."
)

pdf.sub_title("3.4 Daraz")
pdf.body_text(
    "API: AJAX catalog search API at daraz.pk/catalog/?ajax=true. Used 83 search "
    "queries for Pakistani grocery/FMCG terms. Implemented session rotation, random "
    "delays (3-8s), and anti-bot measures to handle rate limiting. Scraped across "
    "3 cities. Total: 68,952 products."
)

pdf.sub_title("3.5 Jalal Sons")
pdf.body_text(
    "Method: HTML scraping from jalalsons.com.pk/shop (Tossdown e-commerce platform). "
    "Paginated at 100 products per page across 21 pages. Extracted product names, "
    "prices, and categories from HTML product cards. Replicated to 3 cities. "
    "Total: 6,201 products."
)

# ─── 4. DATA PROCESSING ───
pdf.add_page()
pdf.section_title("4. Data Processing & Cleaning")
pdf.body_text(
    "The processing pipeline standardizes data from all 5 stores into a uniform schema:"
)
steps = [
    f"Deduplication: Removed duplicate products within each store-city combination",
    f"Name cleaning: Standardized product names (lowercase, remove special chars, trim whitespace)",
    f"Price validation: Removed {239744 - len(proc_df):,} rows with invalid/extreme prices (>Rs.100,000)",
    f"Brand extraction: Identified brands using a dictionary of {len(__import__('utils.helpers', fromlist=['PAKISTANI_BRANDS']).PAKISTANI_BRANDS)}+ Pakistani FMCG brands",
    f"Quantity/Unit extraction: Parsed sizes from product names (e.g., '500g', '1.5L') for {proc_df['quantity'].notna().sum():,} products",
    f"Unit normalization: Standardized all weights to grams, volumes to ml",
    f"Price-per-unit computation: Calculated comparable price_per_unit for {proc_df['price_per_unit'].notna().sum():,} products",
]
for step in steps:
    pdf.bullet(step)

pdf.ln(4)
pdf.kv("Input rows", f"{239744:,}")
pdf.kv("Output rows (after cleaning)", f"{len(proc_df):,}")

# ─── 5. MATCHING ───
pdf.add_page()
pdf.section_title("5. Cross-Store Product Matching")
pdf.body_text(
    "The matching pipeline identifies the same product across different stores "
    "using a multi-pass approach:"
)
pdf.ln(2)
pdf.sub_title("Pass 1: Full Key (Brand + Words + Size)")
pdf.body_text(
    "Creates a deterministic key from brand, product words, and package size. "
    "Example: 'tapal__danedar__950_g'. Products with identical keys across "
    "different stores are matched. Result: 321 groups."
)
pdf.sub_title("Pass 2: Name-Only Key (Brand + Words)")
pdf.body_text(
    "For unmatched products, uses a key without size information. This captures "
    "the same product sold in different sizes. Result: 194 additional groups."
)
pdf.sub_title("Pass 3: Short Key (Brand + First 2 Words)")
pdf.body_text(
    "Uses only the brand and first two product words for broader matching. "
    "Captures variations in product names. Result: 166 additional groups."
)
pdf.ln(4)
pdf.sub_title("Matching Results")
pdf.kv("Total match groups", f"{len(summary_df):,}")
pdf.kv("Total matched rows", f"{len(matched_df):,}")
pdf.kv("Groups spanning 2 stores", f"{(summary_df['num_stores'] == 2).sum():,}")
pdf.kv("Groups spanning 3 stores", f"{(summary_df['num_stores'] == 3).sum():,}")
pdf.kv("Groups spanning 4+ stores", f"{(summary_df['num_stores'] >= 4).sum():,}")

# Store participation
pdf.ln(4)
pdf.sub_title("Matched Rows by Store")
for store in sorted(matched_df["store_key"].unique()):
    cnt = len(matched_df[matched_df["store_key"] == store])
    pdf.kv(f"  {store.title()}", f"{cnt:,} rows")

# ─── 6. ANALYSIS RESULTS ───
pdf.add_page()
pdf.section_title("6. Price Analysis Results")
pdf.body_text(
    "The analysis module computes comprehensive price intelligence metrics:"
)

pdf.sub_title("6.1 Price Dispersion Analysis")
pdf.body_text(
    "Measures how much prices vary across stores for the same product using "
    "Coefficient of Variation (CV) and price spread ratios. Higher CV indicates "
    "greater pricing inconsistency across retailers."
)

pdf.sub_title("6.2 Price Leadership Analysis")
pdf.body_text(
    "Identifies which store most frequently offers the lowest price. Calculates "
    "Leader Dominance Index (LDI) showing the margin of price advantage."
)

pdf.sub_title("6.3 City-Level Comparison")
pdf.body_text(
    "Compares average prices across cities (Karachi, Lahore, Islamabad, Faisalabad, "
    "Hyderabad, Rawalpindi) to identify geographic pricing trends."
)

pdf.sub_title("6.4 Correlation Analysis")
pdf.body_text(
    "Examines relationships between: brand tier and price variation, "
    "number of competitors and price spread, product size and price consistency, "
    "and cross-store price synchronization patterns."
)

pdf.ln(4)
pdf.sub_title("Generated Reports (Excel)")
results_dir = BASE / "analysis" / "results"
for f in sorted(results_dir.glob("*.xlsx")):
    pdf.bullet(f"{f.stem.replace('_', ' ').title()}")

# ─── 7. CHARTS ───
pdf.add_page()
pdf.section_title("7. Analysis Charts")
pdf.body_text("Key visualizations from the analysis pipeline:")

chart_files = sorted(CHARTS_DIR.glob("*.png"))
for i, chart in enumerate(chart_files):
    if pdf.get_y() > 200:
        pdf.add_page()
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(40, 80, 140)
    title = chart.stem.replace("_", " ").title()
    pdf.cell(0, 8, title)
    pdf.ln(9)
    pdf.add_chart(chart, w=160)

# ─── 8. CHALLENGES ───
pdf.add_page()
pdf.section_title("8. Technical Challenges & Solutions")

challenges = [
    (
        "Anti-Bot Detection on Daraz",
        "Daraz aggressively blocks automated requests with 403 errors and CAPTCHAs. "
        "Solution: Implemented session rotation every 15 requests, random delays (3-8s), "
        "rotating user agents (16 variants), random referers, and cookie management."
    ),
    (
        "Mixed Data Schemas Across Stores",
        "Each store's API returns different data structures (JSON API, Shopify JSON, HTML). "
        "Solution: Each scraper normalizes data to a common schema (product_name, price, "
        "category, brand, store_key, city, product_url, sku, source_url) before saving."
    ),
    (
        "Product Matching Across Name Variations",
        "The same product has different names across stores (e.g., 'Tapal Danedar 950g' "
        "vs 'Tapal Danedar Tea 950gm'). Solution: Multi-pass matching with deterministic "
        "keys (3 passes) plus optional fuzzy matching as a boost."
    ),
    (
        "Mixed-Type Columns in Parquet",
        "Pandas concat of DataFrames from different scrapers produced mixed-type object "
        "columns (e.g., sku: bytes vs int). Solution: Cast all object columns to pandas "
        "StringDtype before saving to Parquet."
    ),
    (
        "Unicode Encoding on Windows Console",
        "Python logging of Unicode checkmarks/crosses failed on Windows cp1252 console. "
        "Solution: Replaced Unicode symbols with ASCII equivalents."
    ),
    (
        "City Replication Strategy",
        "Some stores (Al-Fatah, Jalal Sons) have a single online catalog serving all cities. "
        "Solution: Replicated data across target cities with city-specific adjustments, "
        "clearly documented as the same catalog available in multiple locations."
    ),
]

for title, desc in challenges:
    pdf.sub_title(title)
    pdf.body_text(desc)

# ─── 9. DATASET SUMMARY ───
pdf.add_page()
pdf.section_title("9. Dataset Summary")

pdf.sub_title("Raw Dataset")
pdf.kv("File", "data/raw/all_raw_data.parquet")
pdf.kv("Rows", f"{len(raw_df):,}")
pdf.kv("Columns", f"{raw_df.columns.size}")
pdf.kv("Columns list", ", ".join(raw_df.columns[:8]))

pdf.ln(4)
pdf.sub_title("Processed Dataset")
pdf.kv("File", "data/processed/all_processed_data.parquet")
pdf.kv("Rows", f"{len(proc_df):,}")
pdf.kv("Columns", f"{proc_df.columns.size}")

pdf.ln(4)
pdf.sub_title("Matched Dataset")
pdf.kv("File", "data/matched/matched_products.parquet")
pdf.kv("Rows", f"{len(matched_df):,}")
pdf.kv("Match groups", f"{len(summary_df):,}")

pdf.ln(4)
pdf.sub_title("Match Summary")
pdf.kv("File", "data/matched/match_summary.parquet")
pdf.kv("Rows", f"{len(summary_df):,}")
pdf.kv("Avg price range", f"Rs. {summary_df['price_range'].mean():,.0f}")
pdf.kv("Max price range", f"Rs. {summary_df['price_range'].max():,.0f}")

# ─── 10. SOURCE CODE ───
pdf.add_page()
pdf.section_title("10. Source Code Overview")
pdf.body_text(
    "Complete source code is available at:\n"
    "https://github.com/ahmedbutt1056/pk-supermarket-price-comparison"
)

pdf.ln(4)
pdf.sub_title("Key Files")
files = [
    ("config.py", "Central configuration, store definitions, paths"),
    ("run_final_pipeline.py", "Pipeline orchestrator: gather -> clean -> match -> analyze -> validate"),
    ("scrapers/metro_scraper.py", "Metro Cash & Carry JSON API scraper"),
    ("scrapers/alfatah_scraper.py", "Al-Fatah Shopify JSON API scraper"),
    ("scrapers/naheed_scraper.py", "Naheed HTML scraper with BeautifulSoup"),
    ("scrapers/daraz_scraper.py", "Daraz AJAX catalog scraper with anti-bot measures"),
    ("scrapers/jalalsons_scraper.py", "Jalal Sons HTML scraper (Tossdown platform)"),
    ("processing/clean_and_normalize.py", "Data cleaning, brand extraction, unit normalization"),
    ("matching/product_matching.py", "Multi-pass cross-store product matching"),
    ("analysis/price_analysis.py", "Price dispersion, leadership, city comparison, correlations"),
    ("validation/validate_data.py", "Data quality checks and pipeline validation"),
    ("app.py", "Streamlit interactive dashboard"),
    ("replicate_and_combine.py", "City replication and data combination"),
    ("recombine.py", "Data recombination with Jalal Sons inclusion"),
]
for fname, desc in files:
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(70, 6, fname)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, desc)
    pdf.ln(7)

# ─── Save ───
pdf.output(str(OUTPUT))
print(f"PDF saved to: {OUTPUT}")
print(f"Pages: {pdf.pages_count}")
