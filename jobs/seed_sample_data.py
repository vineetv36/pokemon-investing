"""Seed the database with realistic sample price data for dashboard development.

This generates 60 days of realistic PSA 10 sales, raw prices, and sentiment data
so the dashboard can be tested with meaningful charts. When real scraping is enabled,
this data can be cleared and replaced.
"""

import sys
import os
import random
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection, init_db

# Realistic price ranges for each card (PSA 10 avg, raw NM avg)
CARD_PRICES = {
    "Charizard|Base Set|4/102": {"psa10": (42000, 55000), "raw": (800, 1200), "trend": 0.003},
    "Charizard|Base Set 2|4/130": {"psa10": (8000, 12000), "raw": (200, 350), "trend": 0.002},
    "Charizard|Team Rocket Returns|4/109": {"psa10": (3500, 5500), "raw": (150, 250), "trend": 0.004},
    "Blastoise|Base Set|2/102": {"psa10": (8000, 12000), "raw": (150, 300), "trend": 0.001},
    "Venusaur|Base Set|15/102": {"psa10": (5000, 8000), "raw": (100, 200), "trend": 0.001},
    "Lugia|Neo Genesis|9/111": {"psa10": (15000, 22000), "raw": (300, 500), "trend": 0.005},
    "Ho-Oh|Neo Revelation|7/64": {"psa10": (4000, 7000), "raw": (80, 150), "trend": 0.002},
    "Charizard VMAX|Champions Path|74/73": {"psa10": (600, 900), "raw": (120, 200), "trend": 0.006},
    "Pikachu VMAX|Vivid Voltage|188/185": {"psa10": (400, 650), "raw": (80, 140), "trend": 0.003},
    "Umbreon VMAX|Evolving Skies|215/203": {"psa10": (700, 1100), "raw": (150, 250), "trend": 0.008},
    "Rayquaza VMAX|Evolving Skies|218/203": {"psa10": (350, 550), "raw": (70, 120), "trend": 0.004},
    "Charizard ex|Paldean Fates|247/091": {"psa10": (200, 350), "raw": (40, 80), "trend": 0.007},
}

SAMPLE_TITLES_PSA10 = [
    "Pokemon {name} {set} #{number} PSA 10 GEM MINT",
    "PSA 10 {name} Holo {set} #{number} Gem Mint",
    "{name} {set} #{number} PSA 10 Gem Mint Pokemon Card",
    "PSA10 Pokemon TCG {name} {set} #{number}",
]

SAMPLE_TITLES_RAW = [
    "Pokemon {name} {set} #{number} NM Near Mint",
    "{name} Holo {set} #{number} Near Mint",
    "{name} {set} #{number} NM/M Raw Pokemon Card",
    "Pokemon TCG {name} {set} #{number} NM",
]

SUBREDDITS = ["PokemonTCG", "pkmntcgdeals", "PokemonCardValue", "pokemoncardcollectors"]

REDDIT_TITLES = [
    "Just pulled a {name} from {set}!",
    "Is {name} from {set} a good investment right now?",
    "PSA 10 {name} {set} price check",
    "{name} prices are going crazy lately",
    "Got my {name} back from PSA - 10!",
    "Should I grade my {name} {set}?",
    "Market analysis: {name} {set} trend",
    "Picked up a raw {name} {set} - deal or no?",
]


def generate_price(base_range, day_offset, trend, noise=0.08):
    """Generate a realistic price with trend and noise."""
    low, high = base_range
    base = (low + high) / 2
    # Add upward trend over time (day_offset is negative for past days)
    trended = base * (1 + trend * day_offset)
    # Add random noise
    noisy = trended * (1 + random.gauss(0, noise))
    return round(max(low * 0.7, noisy), 2)


def seed_sample_data(days=60):
    """Populate database with realistic sample data."""
    init_db()
    conn = get_connection()
    cursor = conn.cursor()

    # Get all cards
    cursor.execute("SELECT id, name, set_name, card_number FROM cards WHERE is_active = 1")
    cards = [dict(row) for row in cursor.fetchall()]

    if not cards:
        print("No cards in database. Run seed_cards.py first.")
        return

    today = date.today()
    total_psa10_sales = 0
    total_raw_prices = 0

    for card in cards:
        key = f"{card['name']}|{card['set_name']}|{card['card_number']}"
        prices = CARD_PRICES.get(key)
        if not prices:
            print(f"  No price data configured for {key}, skipping")
            continue

        card_id = card["id"]
        print(f"Seeding data for {card['name']} ({card['set_name']})...")

        # Generate PSA 10 sales (2-5 per week, scattered)
        for day_offset in range(days):
            d = today - timedelta(days=day_offset)

            # PSA 10 sales: ~3 per week on average (not every day)
            if random.random() < 0.45:  # ~45% chance per day
                num_sales = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
                for _ in range(num_sales):
                    price = generate_price(prices["psa10"], -day_offset, prices["trend"])
                    title_template = random.choice(SAMPLE_TITLES_PSA10)
                    title = title_template.format(
                        name=card["name"], set=card["set_name"], number=card["card_number"]
                    )
                    try:
                        cursor.execute(
                            """INSERT OR IGNORE INTO psa10_sales
                               (card_id, sale_price, sale_date, listing_title, source)
                               VALUES (?, ?, ?, ?, 'ebay')""",
                            (card_id, price, d.isoformat(), title),
                        )
                        total_psa10_sales += cursor.rowcount
                    except Exception as e:
                        print(f"    Error: {e}")

            # Raw price: one per day (market price)
            raw_price = generate_price(prices["raw"], -day_offset, prices["trend"], noise=0.05)
            try:
                cursor.execute(
                    """INSERT OR IGNORE INTO raw_prices
                       (card_id, price, source, recorded_date)
                       VALUES (?, ?, 'ebay', ?)""",
                    (card_id, raw_price, d.isoformat()),
                )
                total_raw_prices += cursor.rowcount
            except Exception as e:
                print(f"    Error: {e}")

        # Generate some Reddit mentions (1-3 per day for popular cards, less for others)
        popularity = 1.0
        if "Charizard" in card["name"]:
            popularity = 2.0
        elif card["name"] in ("Umbreon VMAX", "Lugia"):
            popularity = 1.5

        for day_offset in range(min(days, 30)):  # only 30 days of Reddit data
            d = today - timedelta(days=day_offset)
            if random.random() < 0.3 * popularity:
                num_posts = random.choices([1, 2, 3], weights=[50, 35, 15])[0]
                for i in range(num_posts):
                    title_template = random.choice(REDDIT_TITLES)
                    post_title = title_template.format(
                        name=card["name"], set=card["set_name"]
                    )
                    subreddit = random.choice(SUBREDDITS)
                    score = random.randint(5, 500)
                    upvote_ratio = round(random.uniform(0.7, 0.98), 2)
                    num_comments = random.randint(2, 80)
                    sentiment_label = random.choices(
                        ["positive", "neutral", "negative"],
                        weights=[50, 35, 15]
                    )[0]
                    sentiment_score = round(random.uniform(0.5, 0.95), 3)
                    post_id = f"sample_{card_id}_{day_offset}_{i}"
                    created_utc = f"{d.isoformat()} {random.randint(8,23):02d}:{random.randint(0,59):02d}:00"

                    try:
                        cursor.execute(
                            """INSERT OR IGNORE INTO reddit_mentions
                               (card_id, post_id, subreddit, post_title, score,
                                upvote_ratio, num_comments, sentiment_label,
                                sentiment_score, created_utc)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (card_id, post_id, subreddit, post_title, score,
                             upvote_ratio, num_comments, sentiment_label,
                             sentiment_score, created_utc),
                        )
                    except Exception as e:
                        print(f"    Error: {e}")

    conn.commit()
    print(f"\nInserted {total_psa10_sales} PSA 10 sales, {total_raw_prices} raw prices")

    # Now compute aggregates
    print("\nComputing daily aggregates...")
    conn.close()
    compute_all_aggregates(days)


def compute_all_aggregates(days=60):
    """Compute psa10_prices, price_ratios, and daily_sentiment for each day."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM cards WHERE is_active = 1")
    cards = cursor.fetchall()
    today = date.today()

    for card in cards:
        card_id = card["id"]
        card_name = card["name"]

        for day_offset in range(days - 1, -1, -1):  # oldest first so 7d/30d lookbacks work
            d = today - timedelta(days=day_offset)
            d_str = d.isoformat()
            start = (d - timedelta(days=30)).isoformat()

            # PSA 10 rolling average
            cursor.execute(
                """SELECT AVG(sale_price) as avg_price, MIN(sale_price) as min_price,
                          MAX(sale_price) as max_price, COUNT(*) as cnt
                   FROM psa10_sales
                   WHERE card_id = ? AND sale_date >= ? AND sale_date <= ?""",
                (card_id, start, d_str),
            )
            row = cursor.fetchone()
            if row and row["cnt"] > 0:
                cursor.execute(
                    """INSERT OR REPLACE INTO psa10_prices
                       (card_id, avg_price, min_price, max_price, sale_count, recorded_date)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (card_id, round(row["avg_price"], 2), round(row["min_price"], 2),
                     round(row["max_price"], 2), row["cnt"], d_str),
                )

            # Get raw price for this day
            cursor.execute(
                "SELECT price FROM raw_prices WHERE card_id = ? AND recorded_date = ?",
                (card_id, d_str),
            )
            raw_row = cursor.fetchone()

            # Calculate ratio
            if row and row["cnt"] > 0 and raw_row and raw_row["price"] > 0:
                ratio = round(row["avg_price"] / raw_row["price"], 2)

                # Get 7-day-ago ratio
                cursor.execute(
                    "SELECT ratio FROM price_ratios WHERE card_id = ? AND recorded_date = ?",
                    (card_id, (d - timedelta(days=7)).isoformat()),
                )
                r7 = cursor.fetchone()
                ratio_7d = round(ratio - r7["ratio"], 2) if r7 and r7["ratio"] else None

                cursor.execute(
                    "SELECT ratio FROM price_ratios WHERE card_id = ? AND recorded_date = ?",
                    (card_id, (d - timedelta(days=30)).isoformat()),
                )
                r30 = cursor.fetchone()
                ratio_30d = round(ratio - r30["ratio"], 2) if r30 and r30["ratio"] else None

                cursor.execute(
                    """INSERT OR REPLACE INTO price_ratios
                       (card_id, psa10_price, raw_price, ratio, ratio_7d_change, ratio_30d_change, recorded_date)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (card_id, round(row["avg_price"], 2), raw_row["price"],
                     ratio, ratio_7d, ratio_30d, d_str),
                )

            # Daily sentiment aggregate
            cursor.execute(
                """SELECT
                       SUM(CASE WHEN sentiment_label='positive' THEN sentiment_score * score
                                WHEN sentiment_label='negative' THEN -sentiment_score * score
                                ELSE 0 END) as weighted_sum,
                       SUM(score) as total_score,
                       COUNT(*) as cnt
                   FROM reddit_mentions
                   WHERE card_id = ? AND date(created_utc) = ?""",
                (card_id, d_str),
            )
            s_row = cursor.fetchone()
            if s_row and s_row["cnt"] > 0 and s_row["total_score"] > 0:
                weighted_sentiment = round(s_row["weighted_sum"] / s_row["total_score"], 4)
                # Clamp to [-1, 1]
                weighted_sentiment = max(-1.0, min(1.0, weighted_sentiment))
                cursor.execute(
                    """INSERT OR REPLACE INTO daily_sentiment
                       (card_id, weighted_sentiment, mention_count, recorded_date)
                       VALUES (?, ?, ?, ?)""",
                    (card_id, weighted_sentiment, s_row["cnt"], d_str),
                )

        conn.commit()
        print(f"  Aggregates computed for {card_name}")

    conn.close()
    print("All aggregates computed.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=60, help="Days of data to generate")
    parser.add_argument("--clear", action="store_true", help="Clear existing data first")
    args = parser.parse_args()

    if args.clear:
        conn = get_connection()
        for table in ["psa10_sales", "raw_prices", "psa10_prices", "price_ratios",
                       "reddit_mentions", "daily_sentiment"]:
            conn.execute(f"DELETE FROM {table}")
        conn.commit()
        conn.close()
        print("Cleared all existing data.")

    seed_sample_data(args.days)
