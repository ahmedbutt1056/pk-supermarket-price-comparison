🚀 Excited to share our latest Data Science project — a full-stack price comparison pipeline for Pakistani supermarkets!

We built an automated system that scrapes real product data from 4 major Pakistani retail chains (Metro Online, Al-Fatah, Naheed, and Daraz), processes 170,000+ products across 6 cities, and performs cross-store price analysis.

🔍 What we built:
• Web scrapers handling 4 different data sources (REST APIs, Shopify JSON, HTML parsing, AJAX search endpoints)
• Anti-bot evasion with session rotation, 16 browser fingerprints, exponential backoff
• A 3-pass product matching engine (deterministic keys + fuzzy matching via RapidFuzz)
• Statistical analysis covering price dispersion, price leadership, city comparisons, and correlation studies
• Interactive Streamlit dashboard for exploring the data
• Complete scraping audit trail — 2,825 logged HTTP requests

📊 Key findings:
• Consumers can save 15-40% on everyday items by picking the right store
• Naheed is the most affordable for groceries (avg Rs.779 on matched products)
• The same product can vary by Rs.13,000+ across stores
• Only ~2% of products overlap across all 4 stores — they serve very different markets

💡 Biggest technical challenge? Daraz's anti-bot system blocked us after every 15-20 requests, resulting in a 30% request failure rate. Despite this, we gathered 1,813 unique Daraz products through persistent session rotation and backoff strategies.

Tech stack: Python | BeautifulSoup4 | Pandas | PyArrow | RapidFuzz | Streamlit | Matplotlib | Seaborn | SciPy

📝 Full technical deep-dive on Medium: [LINK]
💻 Source code on GitHub: [LINK]

#DataScience #WebScraping #Python #DataEngineering #PriceComparison #Pakistan #ETL #MachineLearning #Analytics
