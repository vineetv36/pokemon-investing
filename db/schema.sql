-- Cards watchlist
CREATE TABLE IF NOT EXISTS cards (
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
CREATE TABLE IF NOT EXISTS raw_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id INTEGER REFERENCES cards(id),
    price REAL NOT NULL,
    source TEXT DEFAULT 'pokemonpricetracker',
    recorded_date DATE NOT NULL,
    UNIQUE(card_id, recorded_date)
);

-- PSA 10 sold listings (from 130point)
CREATE TABLE IF NOT EXISTS psa10_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id INTEGER REFERENCES cards(id),
    sale_price REAL NOT NULL,
    sale_date DATE NOT NULL,
    listing_title TEXT,
    source TEXT DEFAULT '130point',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily aggregated PSA 10 price (rolling 30-day avg of sales)
CREATE TABLE IF NOT EXISTS psa10_prices (
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
CREATE TABLE IF NOT EXISTS price_ratios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id INTEGER REFERENCES cards(id),
    psa10_price REAL,
    raw_price REAL,
    ratio REAL,
    ratio_7d_change REAL,
    ratio_30d_change REAL,
    recorded_date DATE NOT NULL,
    UNIQUE(card_id, recorded_date)
);

-- Reddit sentiment
CREATE TABLE IF NOT EXISTS reddit_mentions (
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
CREATE TABLE IF NOT EXISTS daily_sentiment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    card_id INTEGER REFERENCES cards(id),
    weighted_sentiment REAL,
    mention_count INTEGER,
    recorded_date DATE NOT NULL,
    UNIQUE(card_id, recorded_date)
);
