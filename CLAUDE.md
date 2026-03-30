# PSA 10 Pokemon Card Dashboard — Claude Code Instructions

## Project Overview

Build a dashboard that tracks **PSA 10 vs raw price ratios** for Pokemon cards and identifies cards gaining momentum. The system combines three free data sources:

1. **130point.com** — scrape recent eBay sold listings (PSA 10 and raw NM prices)
2. **PokemonPriceTracker API** (free tier) — historical price snapshots and PSA data
3. **Reddit** (PRAW, free) — sentiment analysis from r/PokemonTCG and related subreddits

---

## Tech Stack

- **Backend**: Python 3.11+
- **Scraping**: `playwright` or `httpx` + `BeautifulSoup4` for 130point
- **Reddit**: `praw` library
- **Sentiment**: `transformers` pipeline via Hugging Face (`cardiffnlp/twitter-roberta-base-sentiment-latest`) — free inference
- **Database**: SQLite (local dev) or PostgreSQL (production)
- **Scheduler**: `APScheduler` or simple cron for daily jobs
- **Frontend**: Next.js + Recharts, or a simpler FastAPI + Jinja2 + Chart.js stack
- **Environment**: `.env` file for all secrets (never hardcode)

---

## Project Structure

```
pokemon-psa-dashboard/
├── CLAUDE.md                  # This file
├── .env.example               # Template for secrets
├── requirements.txt
├── db/
│   └── schema.sql             # SQLite/Postgres schema
├── scrapers/
│   ├── __init__.py
│   ├── point130_scraper.py    # 130point.com scraper
│   └── reddit_scraper.py      # Reddit PRAW scraper
├── api_clients/
│   ├── __init__.py
│   └── pokemon_price_tracker.py  # PokemonPriceTracker API client
├── analysis/
│   ├── __init__.py
│   ├── sentiment.py           # Hugging Face sentiment scoring
│   └── ratio_calculator.py    # PSA10/raw ratio logic + momentum signals
├── jobs/
│   ├── __init__.py
│   └── daily_job.py           # Orchestrates all scrapers + analysis daily
├── dashboard/
│   ├── app.py                 # FastAPI or Flask app
│   └── templates/             # HTML templates
└── tests/
    └── test_scrapers.py
```

---

## Environment Variables (`.env`)

```env
# PokemonPriceTracker
POKEMON_PRICE_TRACKER_API_KEY=your_key_here

# Reddit (PRAW) — register at reddit.com/prefs/apps
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=psa10-dashboard/1.0 by /u/your_username

# Database
DATABASE_URL=sqlite:///./dashboard.db

# Optional: Hugging Face token (not required for public models)
HF_TOKEN=
```

---

## Data Sources

### 1. 130point.com Scraper (`scrapers/point130_scraper.py`)

**Purpose**: Get real eBay sold prices including "Best Offer Accepted" prices that eBay hides.

**How it works**:
- 130point.com uses eBay's API + smart scraping to reveal true sale prices
- Search URL pattern: `https://130point.com/sales/?query=CARD_NAME+PSA+10`
- Parse the results table for: card name, grade, sale price, sale date, listing title

**Implementation notes**:
- Use `playwright` (headless Chromium) since 130point renders via JavaScript
- Add 2–5 second random delays between requests to avoid rate limiting
- Respect robots.txt — only scrape public search results, not user data
- Cache results locally; don't re-scrape the same card within 24 hours
- Filter results: only accept listings where title contains "PSA 10" or "PSA10" for graded, and "NM" or "Near Mint" or "Raw" for ungraded
- Store raw listing titles so you can manually audit edge cases

**Key fields to extract**:
```python
{
    "card_name": str,        # e.g. "Charizard Base Set"
    "set_name": str,         # e.g. "Base Set"
    "card_number": str,      # e.g. "4/102"
    "grade": str,            # "PSA 10" or "RAW"
    "sale_price": float,
    "sale_date": date,
    "listing_title": str,    # full title for auditing
    "source": "130point"
}
```

**Rate limiting**: Add `time.sleep(random.uniform(2, 5))` between every request. Do not run more than 100 requests per session.

**Watch out for**:
- Lot sales (multiple cards) — detect by titles containing "lot" or "bundle" and skip
- Damaged cards sold as NM — statistical outliers; filter with IQR method
- BGS/CGC listings appearing in PSA searches — filter strictly on "PSA" in title

---

### 2. PokemonPriceTracker API (`api_clients/pokemon_price_tracker.py`)

**Base URL**: `https://www.pokemonpricetracker.com/api/v2`

**Free tier limits**:
- 100 credits/day
- 3 days of price history only
- No PSA graded data on free tier (PSA data is paid-only)
- Rate: 2 requests/minute to be safe

**What to use it for on free tier**:
- Raw NM card prices (TCGPlayer market price) — updated daily
- Card metadata: name, set, card number, image URL, rarity
- Set listings to build your card watchlist

**Key endpoints**:
```
GET /api/v2/cards?search=charizard&setId=base1
GET /api/v2/cards?tcgPlayerId=123
GET /api/v2/sets
```

**Authentication**:
```python
headers = {"Authorization": "Bearer YOUR_API_KEY"}
```

**Strategy**: Since free tier only gives 3 days of history, use this API **only for raw NM prices and card metadata**. Use 130point scraping for PSA 10 prices. Store everything you fetch in your local DB — this builds your own historical record over time.

**Credit budget** (100/day):
- Reserve 60 credits for card price updates on your watchlist (~60 cards max at free tier)
- Reserve 30 credits for new card discovery / set browsing
- Reserve 10 credits as buffer

**Error handling**:
- 429: back off for 60 seconds, retry once
- 401: API key invalid, log and alert
- Always check `response.status_code` before parsing

---

### 3. Reddit Scraper (`scrapers/reddit_scraper.py`)

**Purpose**: Capture community sentiment as a leading indicator before price moves.

**Target subreddits**:
- `r/PokemonTCG` — largest general community
- `r/pkmntcgdeals` — deal-focused, good for price awareness
- `r/PokemonCardValue` — valuation discussions
- `r/pokemoncardcollectors` — collector sentiment

**PRAW setup**:
```python
import praw

reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT"),
)
```

**What to collect**:
- New/hot posts mentioning card names from your watchlist
- Comment counts, upvote ratio, score — as engagement signals
- Post titles and body text for sentiment scoring
- Top comments on high-engagement posts

**Rate limits**: Free tier allows 100 QPM. PRAW handles rate limiting automatically but add `time.sleep(1)` between subreddit fetches anyway.

**Card name matching**:
- Build a dictionary of card aliases: `{"zard": "Charizard", "ex": ...}`
- Use fuzzy matching (`fuzzywuzzy` or `rapidfuzz`) to catch informal names like "base zard" or "shadowless char"
- Match against your watchlist of tracked cards

**Fields to store**:
```python
{
    "card_name": str,          # matched card name
    "subreddit": str,
    "post_id": str,            # Reddit post ID (for dedup)
    "post_title": str,
    "post_body": str,
    "score": int,              # upvotes
    "upvote_ratio": float,
    "num_comments": int,
    "created_utc": datetime,
    "sentiment_label": str,    # "positive" / "neutral" / "negative"
    "sentiment_score": float   # confidence 0.0–1.0
}
```

---

## Sentiment Analysis (`analysis/sentiment.py`)

**Model**: `cardiffnlp/twitter-roberta-base-sentiment-latest`
- Free via Hugging Face `transformers`
- Trained on social media text — better than generic models for Reddit/Twitter content
- Returns: LABEL_0 (negative), LABEL_1 (neutral), LABEL_2 (positive)

**Setup**:
```python
from transformers import pipeline

sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment-latest",
    return_all_scores=True
)
```

**Text preprocessing before scoring**:
1. Strip URLs
2. Strip Reddit markdown (`**bold**`, `>quote`, etc.)
3. Truncate to 512 tokens (model limit)
4. Combine post title + first 200 chars of body

**Aggregating daily sentiment per card**:
- Collect all posts mentioning a card in last 24h
- Weight by `score` (upvotes): higher-upvoted posts count more
- Formula: `weighted_sentiment = sum(sentiment_score * post_score) / sum(post_score)`
- Store as a single daily sentiment value per card: -1.0 (very negative) to +1.0 (very positive)

**Sentiment momentum signal**:
- If 7-day average sentiment rises by > 0.15 points → flag as "rising sentiment"
- Combine with price ratio momentum for a composite score

---

## Database Schema (`db/schema.sql`)

```sql
-- Cards watchlist
CREATE TABLE cards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    set_name TEXT,
    card_number TEXT,
    tcgplayer_id TEXT UNIQUE,
    image_url TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw NM prices (from PokemonPriceTracker)
CREATE TABLE raw_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id INTEGER REFERENCES cards(id),
    price REAL NOT NULL,
    source TEXT DEFAULT 'pokemonpricetracker',
    recorded_date DATE NOT NULL,
    UNIQUE(card_id, recorded_date)
);

-- PSA 10 sold listings (from 130point)
CREATE TABLE psa10_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id INTEGER REFERENCES cards(id),
    sale_price REAL NOT NULL,
    sale_date DATE NOT NULL,
    listing_title TEXT,
    source TEXT DEFAULT '130point',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily aggregated PSA 10 price (rolling 30-day avg of sales)
CREATE TABLE psa10_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id INTEGER REFERENCES cards(id),
    avg_price REAL,
    min_price REAL,
    max_price REAL,
    sale_count INTEGER,
    recorded_date DATE NOT NULL,
    UNIQUE(card_id, recorded_date)
);

-- Daily ratio calculation
CREATE TABLE price_ratios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id INTEGER REFERENCES cards(id),
    psa10_price REAL,
    raw_price REAL,
    ratio REAL,           -- psa10_price / raw_price
    ratio_7d_change REAL, -- ratio vs 7 days ago
    ratio_30d_change REAL,
    recorded_date DATE NOT NULL,
    UNIQUE(card_id, recorded_date)
);

-- Reddit sentiment
CREATE TABLE reddit_mentions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id INTEGER REFERENCES cards(id),
    post_id TEXT UNIQUE,
    subreddit TEXT,
    post_title TEXT,
    score INTEGER,
    upvote_ratio REAL,
    num_comments INTEGER,
    sentiment_label TEXT,
    sentiment_score REAL,
    created_utc TIMESTAMP,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily aggregated sentiment per card
CREATE TABLE daily_sentiment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id INTEGER REFERENCES cards(id),
    weighted_sentiment REAL,  -- -1.0 to +1.0
    mention_count INTEGER,
    recorded_date DATE NOT NULL,
    UNIQUE(card_id, recorded_date)
);
```

---

## Ratio & Momentum Logic (`analysis/ratio_calculator.py`)

### Core Ratio
```python
ratio = psa10_price / raw_price
```

A ratio of 3.0 means the PSA 10 costs 3x the raw NM price.

### Momentum Signals

**Price Ratio Momentum** (primary signal):
- `ratio_7d_change > 0.3` → ratio rising fast (graded premium expanding)
- `ratio_30d_change > 0.5` → sustained upward trend

**Sales Velocity** (secondary signal):
- Compare 30-day sale count vs prior 30 days from 130point data
- `velocity_change > 50%` → significantly more PSA 10s selling

**Sentiment Momentum** (leading indicator):
- `sentiment_7d_avg > 0.3` AND rising → community excitement growing

### Composite Score (0–100)
```python
momentum_score = (
    ratio_momentum_score * 0.40 +   # 40% weight
    sales_velocity_score * 0.35 +   # 35% weight
    sentiment_score * 0.25          # 25% weight
)
```

### "Moving Up" Classification
- Score ≥ 70: 🔥 Strong momentum
- Score 50–69: 📈 Moderate momentum  
- Score 30–49: 👀 Watch list
- Score < 30: ➡️ Neutral / cooling

---

## Daily Job Orchestration (`jobs/daily_job.py`)

Run once per day (suggest: 6am UTC via cron or APScheduler):

```
1. For each active card in watchlist:
   a. Scrape 130point for recent PSA 10 sales (last 7 days)
   b. Scrape 130point for recent raw NM sales (last 7 days)
   c. Fetch raw NM price from PokemonPriceTracker (1 credit)
   d. Scrape Reddit for mentions in last 24h
   e. Run sentiment analysis on new Reddit posts
   
2. Aggregate:
   a. Calculate 30-day rolling avg PSA 10 price from sales
   b. Update price_ratios table
   c. Update daily_sentiment table
   d. Calculate momentum scores

3. Flag cards with score ≥ 50 for dashboard alerts
```

**Rate limit compliance**:
- Space 130point requests: `time.sleep(random.uniform(3, 6))` between cards
- PokemonPriceTracker: max 60 requests/day, 1 per second
- Reddit PRAW: let library manage, add 1s sleep between subreddits
- Total runtime estimate: ~45 min for 50-card watchlist

---

## Dashboard Features

### Views to Build

1. **Leaderboard** — all tracked cards sorted by momentum score, updated daily
2. **Momentum Movers** — cards where ratio grew most in last 7 / 30 days
3. **Sentiment Rising** — cards with improving Reddit sentiment score
4. **Card Detail Page**:
   - PSA 10 price history chart (line)
   - Raw NM price history chart (line)
   - Ratio over time chart (the key metric)
   - Reddit mention volume + sentiment bar chart
   - Recent 130point sales table

### Key Metrics to Display Per Card
- Current ratio (e.g., 4.2x)
- Ratio 7d change (e.g., +0.3)
- Ratio 30d change
- PSA 10 avg price (30-day)
- Raw NM price
- Reddit mentions (7d)
- Sentiment score (7d avg)
- Momentum score badge

---

## Initial Watchlist (Seed Data)

Start with these high-interest cards to seed the database:

```python
SEED_CARDS = [
    # Vintage holos
    {"name": "Charizard", "set": "Base Set", "number": "4/102"},
    {"name": "Charizard", "set": "Base Set 2", "number": "4/130"},
    {"name": "Charizard", "set": "Team Rocket Returns", "number": "4/109"},
    {"name": "Blastoise", "set": "Base Set", "number": "2/102"},
    {"name": "Venusaur", "set": "Base Set", "number": "15/102"},
    {"name": "Lugia", "set": "Neo Genesis", "number": "9/111"},
    {"name": "Ho-Oh", "set": "Neo Revelation", "number": "7/64"},
    # Modern high-demand
    {"name": "Charizard VMAX", "set": "Champions Path", "number": "74/73"},
    {"name": "Pikachu VMAX", "set": "Vivid Voltage", "number": "188/185"},
    {"name": "Umbreon VMAX", "set": "Evolving Skies", "number": "215/203"},
    {"name": "Rayquaza VMAX", "set": "Evolving Skies", "number": "218/203"},
    {"name": "Charizard ex", "set": "Paldean Fates", "number": "247/091"},
]
```

---

## Error Handling & Resilience

- **130point down / blocked**: Skip and log; don't crash the daily job. Alert if blocked 3 days in a row.
- **API rate limit hit**: Exponential backoff: 60s, 120s, 240s. Log all 429s.
- **Sentiment model slow**: Load model once at startup, reuse pipeline object. Use batching.
- **Sparse PSA 10 data**: If fewer than 3 sales in 30 days, mark data as `low_confidence = True` and surface that in the UI.
- **Card matching ambiguity**: Prefer exact card number match over name-only match. Log all fuzzy matches for review.

---

## Legal & Ethical Notes

- 130point.com: Only scrape publicly visible search results. Add `User-Agent` identifying your project. Do not scrape at high volume. Respect `Retry-After` headers.
- Reddit: Use official PRAW library with OAuth. Non-commercial personal use only. Never store deleted content. Respect Reddit's API ToS.
- PokemonPriceTracker: Stay within free tier limits (100 credits/day). Do not cache and redistribute their data commercially.
- This project is for personal/research use only.

---

## Getting Started (Claude Code Steps)

1. `pip install praw playwright beautifulsoup4 httpx transformers torch python-dotenv apscheduler fastapi`
2. `playwright install chromium`
3. Copy `.env.example` to `.env` and fill in credentials
4. `python db/schema.sql` — initialize database
5. `python jobs/seed_cards.py` — insert initial watchlist
6. `python jobs/daily_job.py --backfill 7` — run first scrape with 7-day lookback
7. `python dashboard/app.py` — start the dashboard

---

## Future Enhancements (Out of Scope for MVP)

- PSA population report scraping (PSA website, unofficial)
- YouTube video sentiment (YouTube Data API v3, free)
- Price alerts via email/Discord webhook when momentum score crosses threshold
- Automatic watchlist expansion: detect newly hyped cards from Reddit before they're in watchlist
- Grading ROI calculator: raw price + grading fee vs expected PSA 10 value
