"""Daily job orchestrator — runs all scrapers, analysis, and scoring."""

import argparse
import logging
import sys
import os
import time
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection, init_db
from scrapers.point130_scraper import scrape_card_sales, store_psa10_sales
from scrapers.reddit_scraper import scrape_all_subreddits, store_reddit_mentions
from api_clients.pokemon_price_tracker import fetch_and_store_raw_price, get_credits_remaining
from analysis.sentiment import analyze_posts, compute_daily_sentiment
from analysis.ratio_calculator import calculate_momentum_score

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def get_active_cards():
    """Fetch all active cards from the watchlist."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, set_name, card_number FROM cards WHERE is_active = 1")
    cards = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return cards


def scrape_130point_for_card(card, days_back=7):
    """Scrape 130point for both PSA 10 and raw sales of a card."""
    card_id = card["id"]

    # PSA 10 sales
    psa10_sales = scrape_card_sales(
        card["name"], card["set_name"], card["card_number"],
        grade="PSA 10", days_back=days_back,
    )
    if psa10_sales:
        store_psa10_sales(card_id, psa10_sales)
        logger.info("Card %s: %d PSA 10 sales found", card["name"], len(psa10_sales))

    # Raw NM sales
    raw_sales = scrape_card_sales(
        card["name"], card["set_name"], card["card_number"],
        grade="RAW", days_back=days_back,
    )
    if raw_sales:
        # Store raw sales as price data points
        conn = get_connection()
        cursor = conn.cursor()
        for sale in raw_sales:
            try:
                cursor.execute(
                    """INSERT OR IGNORE INTO raw_prices (card_id, price, source, recorded_date)
                       VALUES (?, ?, '130point', ?)""",
                    (card_id, sale["sale_price"], sale["sale_date"].isoformat()),
                )
            except Exception as e:
                logger.error("Error storing raw sale: %s", e)
        conn.commit()
        conn.close()
        logger.info("Card %s: %d raw sales found", card["name"], len(raw_sales))


def run_daily_job(days_back=7):
    """Run the full daily job pipeline."""
    logger.info("=== Starting daily job (lookback: %d days) ===", days_back)
    start_time = time.time()

    init_db()
    cards = get_active_cards()
    if not cards:
        logger.warning("No active cards in watchlist. Run seed_cards.py first.")
        return

    logger.info("Processing %d active cards...", len(cards))

    # Step 1: Scrape 130point for each card (uses Playwright)
    logger.info("--- Step 1: Scraping 130point.com ---")
    for card in cards:
        try:
            scrape_130point_for_card(card, days_back)
        except Exception as e:
            logger.error("Error scraping 130point for %s: %s", card["name"], e)

    # Step 2: Fetch raw prices from PokemonPriceTracker
    # TODO: Uncomment when API rate limiting is resolved
    # logger.info("--- Step 2: Fetching prices from PokemonPriceTracker ---")
    # for card in cards:
    #     if get_credits_remaining() <= 10:
    #         logger.warning("Low on API credits. Stopping price fetches.")
    #         break
    #     try:
    #         fetch_and_store_raw_price(card["id"], card["name"], card["set_name"])
    #     except Exception as e:
    #         logger.error("Error fetching price for %s: %s", card["name"], e)
    logger.info("--- Step 2: Skipping PokemonPriceTracker (disabled) ---")

    # Step 3: Scrape Reddit
    logger.info("--- Step 3: Scraping Reddit ---")
    try:
        posts = scrape_all_subreddits(limit_per_sub=50)
        if posts:
            # Step 4: Run sentiment analysis
            logger.info("--- Step 4: Analyzing sentiment (%d posts) ---", len(posts))
            posts = analyze_posts(posts)
            store_reddit_mentions(posts)

            # Compute daily sentiment per card
            for card in cards:
                compute_daily_sentiment(card["id"])
    except Exception as e:
        logger.error("Error in Reddit scraping/sentiment: %s", e)

    # Step 5: Calculate momentum scores
    logger.info("--- Step 5: Calculating momentum scores ---")
    results = []
    for card in cards:
        try:
            score = calculate_momentum_score(card["id"])
            results.append({"card": card, "momentum": score})
            logger.info(
                "Card %s: momentum=%.1f (%s)",
                card["name"], score["momentum_score"], score["badge"],
            )
        except Exception as e:
            logger.error("Error calculating momentum for %s: %s", card["name"], e)

    # Step 6: Flag high-momentum cards
    alerts = [r for r in results if r["momentum"]["momentum_score"] >= 50]
    if alerts:
        logger.info("=== ALERTS: %d cards with momentum >= 50 ===", len(alerts))
        for a in alerts:
            logger.info(
                "  %s: %.1f (%s)",
                a["card"]["name"], a["momentum"]["momentum_score"], a["momentum"]["badge"],
            )

    elapsed = time.time() - start_time
    logger.info("=== Daily job complete in %.1f seconds ===", elapsed)


def main():
    parser = argparse.ArgumentParser(description="Run the daily PSA 10 dashboard job")
    parser.add_argument(
        "--backfill", type=int, default=7,
        help="Number of days to look back (default: 7)",
    )
    args = parser.parse_args()
    run_daily_job(days_back=args.backfill)


if __name__ == "__main__":
    main()
