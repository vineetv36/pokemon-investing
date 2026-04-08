"""Backfill historical prices from PokemonPriceTracker API.

Reads from data/watchlist.parquet (or data/cards_catalog.parquet) and fetches
up to 6 months of daily price history for each card. Automatically syncs new
cards into the SQLite cards table as it goes.

Usage:
    python3 jobs/backfill_history.py                          # watchlist, 6 months
    python3 jobs/backfill_history.py --days 90                # 3 months
    python3 jobs/backfill_history.py --min-price 20           # only cards >= $20
    python3 jobs/backfill_history.py --source catalog         # full catalog instead of watchlist
    python3 jobs/backfill_history.py --resume                 # skip cards already in raw_prices
"""

import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from db import get_connection, init_db
from api_clients.pokemon_price_tracker import fetch_and_store_history, get_credits_remaining

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
WATCHLIST_FILE = os.path.join(DATA_DIR, "watchlist.parquet")
CATALOG_FILE = os.path.join(DATA_DIR, "cards_catalog.parquet")


def _load_cards(source: str, min_price: float) -> pd.DataFrame:
    """Load cards from Parquet file and filter by price."""
    if source == "watchlist":
        path = WATCHLIST_FILE
        if not os.path.exists(path):
            logger.error("No watchlist found at %s", path)
            logger.error("Run: python3 jobs/filter_catalog.py --min-price %.0f", min_price)
            sys.exit(1)
    else:
        path = CATALOG_FILE
        if not os.path.exists(path):
            logger.error("No catalog found at %s", path)
            logger.error("Run: python3 jobs/import_cards.py")
            sys.exit(1)

    df = pd.read_parquet(path)
    logger.info("Loaded %d cards from %s", len(df), path)

    if min_price > 0:
        df = df[df["price_market"].notna() & (df["price_market"] >= min_price)]
        logger.info("Filtered to %d cards with market price >= $%.2f", len(df), min_price)

    # Sort by price descending — most valuable cards first
    df = df.sort_values("price_market", ascending=False, na_position="last")
    return df


def _ensure_card_in_db(name: str, set_name: str, number: str, image_url: str) -> int:
    """Ensure card exists in SQLite cards table, return its ID."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id FROM cards WHERE name = ? AND set_name = ? AND card_number = ?",
        (name, set_name, number),
    )
    row = cursor.fetchone()
    if row:
        conn.close()
        return row["id"]

    cursor.execute(
        """INSERT OR IGNORE INTO cards (name, set_name, card_number, image_url)
           VALUES (?, ?, ?, ?)""",
        (name, set_name, number, image_url),
    )
    conn.commit()
    card_id = cursor.lastrowid
    conn.close()
    return card_id


def _get_cards_with_history() -> set:
    """Get set of (name, set_name, card_number) that already have history."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT c.name, c.set_name, c.card_number
        FROM raw_prices rp
        JOIN cards c ON c.id = rp.card_id
        WHERE rp.source = 'pokemonpricetracker'
    """)
    result = {(r["name"], r["set_name"], r["card_number"]) for r in cursor.fetchall()}
    conn.close()
    return result


def backfill(days: int = 180, source: str = "watchlist",
             min_price: float = 5.0, resume: bool = False):
    """Backfill historical prices for cards in the Parquet catalog."""
    init_db()

    df = _load_cards(source, min_price)
    if df.empty:
        logger.error("No cards to process.")
        return

    # Skip cards that already have history
    skip_set = set()
    if resume:
        skip_set = _get_cards_with_history()
        logger.info("Resume mode: skipping %d cards that already have history", len(skip_set))

    total_cards = len(df)
    total_stored = 0
    processed = 0
    skipped = 0

    logger.info("Backfilling %d days of history for up to %d cards...", days, total_cards)
    logger.info("Credits available: %d", get_credits_remaining())

    for i, (_, row) in enumerate(df.iterrows()):
        credits = get_credits_remaining()
        if credits < 100:
            logger.warning("Low on credits (%d). Stopping at card %d/%d.",
                           credits, i + 1, total_cards)
            break

        name = row["name"]
        set_name = row["set_name"]
        number = row["number"]
        image_url = row.get("image_small", "")

        if resume and (name, set_name, number) in skip_set:
            skipped += 1
            continue

        # Ensure card is in SQLite
        card_id = _ensure_card_in_db(name, set_name, number, image_url)

        logger.info("[%d/%d] %s (%s #%s) — $%.2f — %d credits left",
                    i + 1, total_cards, name, set_name, number,
                    row.get("price_market", 0) or 0, credits)

        stored = fetch_and_store_history(card_id, name, set_name, days=days)
        total_stored += stored
        processed += 1

    logger.info("=" * 60)
    logger.info("Backfill complete:")
    logger.info("  Cards processed: %d", processed)
    logger.info("  Cards skipped (already had history): %d", skipped)
    logger.info("  Total price points stored: %d", total_stored)
    logger.info("  Credits remaining: %d", get_credits_remaining())


def main():
    parser = argparse.ArgumentParser(
        description="Backfill historical prices from PokemonPriceTracker for all cards in catalog")
    parser.add_argument("--days", type=int, default=180,
                        help="Days of history to fetch (default: 180)")
    parser.add_argument("--source", type=str, default="watchlist", choices=["watchlist", "catalog"],
                        help="Card source: 'watchlist' (filtered) or 'catalog' (all) (default: watchlist)")
    parser.add_argument("--min-price", type=float, default=5.0,
                        help="Minimum market price to include (default: $5)")
    parser.add_argument("--resume", action="store_true",
                        help="Skip cards that already have history in the database")
    args = parser.parse_args()
    backfill(days=args.days, source=args.source,
             min_price=args.min_price, resume=args.resume)


if __name__ == "__main__":
    main()
