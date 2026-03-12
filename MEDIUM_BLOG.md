# Building a Real-Time Price Comparison Pipeline for Pakistani Supermarkets: Scraping 170,000+ Products Across 4 Major Chains

*How we built an end-to-end data engineering pipeline that scrapes, cleans, matches, and analyzes grocery prices from Metro, Al-Fatah, Naheed, and Daraz — and what we found about price disparities in Pakistan.*

---

## The Problem: Why Can't Pakistanis Easily Compare Grocery Prices?

Pakistan's retail grocery market is fragmented. A consumer shopping for everyday items — rice, cooking oil, tea, diapers — has no easy way to compare prices across supermarket chains. Unlike mature markets with established price comparison tools, Pakistani shoppers rely on word of mouth or physically visiting multiple stores.

We asked a simple question: **How much does the same product actually cost at different stores?** The answer required building an automated data pipeline from scratch.

---

## The Architecture: From Raw HTML to Actionable Insights

Our pipeline follows a classic ETL (Extract, Transform, Load) pattern with four distinct phases:

```
Scraping → Cleaning & Normalization → Cross-Store Matching → Statistical Analysis
```

### Phase 1: Data Collection (The Hard Part)

We targeted four major Pakistani retail chains, each requiring a completely different scraping strategy:

| Store | Approach | Challenge |
|-------|----------|-----------|
| **Metro Online** | REST JSON API | 213 category endpoints to discover and paginate |
| **Al-Fatah** | Shopify JSON API | 25,000 products across 100 pages |
| **Naheed** | HTML Scraping (BeautifulSoup) | 92 categories, aggressive session timeouts |
| **Daraz** | AJAX Catalog Search API | Severe anti-bot measures, IP-level blocking |

#### Anti-Bot Warfare

The most technically challenging aspect was evading anti-scraping measures. Daraz, in particular, employs aggressive bot detection that blocks IPs after just 15–20 requests. Our countermeasures included:

- **Session rotation**: Fresh `requests.Session()` objects with new User-Agent strings every 20 requests
- **16 browser fingerprints**: Rotating across Chrome, Firefox, Safari, and Edge user-agent strings (desktop + mobile)
- **Exponential backoff**: 3–30 second delays between requests, with longer cooldowns on failures
- **Content-type validation**: Detecting HTML CAPTCHA pages masquerading as JSON responses

Despite these measures, Daraz blocked us intermittently. Our scraping log recorded **1,453 HTTP requests to Daraz alone**, of which **446 failed** due to anti-bot responses — a 30% failure rate that we handled gracefully.

Every single HTTP request was logged with timestamp, URL, attempt number, HTTP status, response time, and result — producing a 2,825-line audit trail.

### Phase 2: Cleaning & Normalization

Raw scraped data is messy. Product names contain inconsistent formatting, promotional text, and encoding artifacts. Our cleaning pipeline:

1. **Deduplication**: Removed exact duplicates across overlapping scraping runs
2. **Name normalization**: Stripped promotional suffixes, standardized casing, removed special characters
3. **Price validation**: Filtered outliers (prices > Rs.100,000) and zero/negative values
4. **Brand extraction**: Pattern-matched 200+ Pakistani FMCG brands from product names
5. **Unit extraction**: Parsed weight/volume from strings like "950g", "1.5L", "6 Pack"

Result: **170,909 raw rows → 168,453 clean rows** (1.4% rejection rate).

### Phase 3: Cross-Store Product Matching

This is the core intellectual challenge. "Tapal Danedar 950g" at Metro is the same product as "Tapal Danedar Tea 950gm" at Naheed — but a naive string match fails.

Our three-pass matching strategy:

1. **Deterministic full-key match**: `brand + cleaned_words + quantity + unit` → catches exact matches
2. **Name-only key match**: `brand + cleaned_words` ignoring size → catches size-variant listings
3. **Fuzzy matching**: RapidFuzz library with `token_sort_ratio` scorer at 68% threshold → catches spelling variations

This yielded **674 cross-store match groups** covering **5,574 product rows** across all four stores. The majority (541 groups) were Metro–Naheed matches, which makes sense — both are grocery-focused chains with significant product catalog overlap.

### Phase 4: Statistical Analysis

With matched products in hand, we computed four categories of analysis:

**3.1 — Price Dispersion**: The average matched product had a **Rs.490 price range** across stores. The highest dispersion was found in rice products — Guard Super Kernel Sella Basmati Rice 1Kg varied from Rs.414 (Daraz) to Rs.11,849 (Metro).

**3.2 — Price Leadership**: Naheed emerged as the most consistently affordable store (avg Rs.779 for matched products), while Al-Fatah was the most expensive (avg Rs.2,001). We computed a **Leader Dominance Index (LDI)** per category showing which store "wins" most often.

**3.3 — City Price Comparison**: Prices across 6 cities (Lahore, Karachi, Islamabad, Faisalabad, Rawalpindi, Hyderabad) were compared using a relative price position index and correlation analysis.

**3.4 — Correlation Analysis**: We examined relationships between brand tier and price variability, number of competing stores and price spread, and cross-store price synchronization.

---

## Technical Stack

- **Python 3.12** with virtual environment
- **requests** + **BeautifulSoup4** + **lxml** for scraping
- **pandas** + **PyArrow** for data processing (Parquet format for efficient storage)
- **RapidFuzz** for fuzzy string matching
- **Matplotlib** + **Seaborn** for visualization
- **Streamlit** for the interactive frontend dashboard
- **SciPy** for statistical tests

---

## Key Findings

1. **Same product, vastly different prices**: Pakistani consumers can save 15–40% on common household items by choosing the right store.

2. **Naheed is the price leader for groceries**: Across matched products, Naheed consistently offered the lowest prices, followed by Metro.

3. **Anti-bot measures are real obstacles**: Daraz's anti-scraping technology is sophisticated enough to make large-scale automated price monitoring genuinely difficult.

4. **Product catalogs barely overlap**: Despite all being "supermarkets," our four stores have fundamentally different product focuses — Al-Fatah is heavily non-grocery, while Naheed and Metro are grocery-first.

5. **Data quality varies wildly**: Metro's API returns clean, structured data. Naheed's HTML requires heavy parsing. Daraz's JSON is inconsistent across product categories.

---

## Lessons Learned

- **Start with API probing**: Before writing scrapers, spend time with browser DevTools to find hidden JSON APIs. We found three of four stores had JSON endpoints that were far more reliable than HTML scraping.
- **Log everything**: Our scraping attempts log was invaluable for debugging failures and proving data provenance.
- **Fuzzy matching needs tuning**: A threshold too high misses real matches; too low creates false positives. We iteratively tuned from 85% down to 68%.
- **Real-world data volumes are limited by anti-bot measures**, not by code limitations. Planning for this reality is essential.

---

## What's Next

- Scheduled daily scraping to track price trends over time
- Adding more stores (Chase Up, Imtiaz) once their anti-bot measures are mapped
- Building a consumer-facing price alert system
- Expanding to non-grocery categories (electronics, fashion)

---

*This project was developed as part of our Data Science coursework. The complete source code, including all scrapers, processing pipeline, and analysis modules, is available on GitHub.*

**Tech Stack**: Python, BeautifulSoup4, Pandas, RapidFuzz, Streamlit, Matplotlib, PyArrow

**Data**: 170,909 products | 4 stores | 6 cities | 674 cross-store matches | 2,757+ logged HTTP requests

---

*If you're interested in web scraping, data engineering, or Pakistani e-commerce, feel free to connect — we'd love to discuss our approach and findings.*
