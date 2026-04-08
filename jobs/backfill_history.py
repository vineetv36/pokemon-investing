"""Backfill historical prices from PokemonPriceTracker API.

Fetches up to 6 months of daily price history for all active cards
and stores in the raw_prices table. Run once after upgrading to paid tier.

Usage:
    python3 jobs/backfill_history.py                # 6 months for all cards
    python3 jobs/backfill_history.py --period 3m    # 3 months
    python3 jobs/backfill_history.py --card-id 1    # Single card
"""

import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection, init_db
from api_clients.pokemon_price_tracker import fetch_and_store_history, get_credits_remaining

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def backfill(period: str = "6m", card_id: int = None):
    """Backfill historical prices for active cards."""
    init_db()
    conn = get_connection()

    if card_id:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, set_name FROM cards WHERE id = ?", (card_id,))
        cards = [dict(row) for row in cursor.fetchall()]
    else:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, set_name FROM cards WHERE is_active = 1")
        cards = [dict(row) for row in cursor.fetchall()]

    conn.close()

    if not cards:
        logger.error("No cards found. Run seed_cards.py first.")
        return

    logger.info("Backfilling %s of history for %d cards...", period, len(cards))
    total_stored = 0

    for i, card in enumerate(cards):
        credits = get_credits_remaining()
        if credits < 100:
            logger.warning("Low on credits (%d). Stopping.", credits)
            break

        logger.info("[%d/%d] %s (%s) — %d credits remaining",
                    i + 1, len(cards), card["name"], card["set_name"], credits)

        stored = fetch_and_store_history(card["id"], card["name"], period)
        total_stored += stored

    logger.info("Backfill complete: %d total price points stored", total_stored)


def main():
    parser = argparse.ArgumentParser(description="Backfill historical prices from PokemonPriceTracker")
    parser.add_argument("--period", type=str, default="6m",
                        help="History period: 3d, 1m, 3m, 6m (default: 6m)")
    parser.add_argument("--card-id", type=int, default=None,
                        help="Backfill a single card by ID")
    args = parser.parse_args()
    backfill(period=args.period, card_id=args.card_id)


if __name__ == "__main__":
    main()
