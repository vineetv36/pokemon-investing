"""Backfill historical prices from PokemonPriceTracker API.

Fetches by SET (not by card) to minimize API calls. One call per set
returns all cards in that set with history, instead of 1 call per card.

3,964 cards across ~150 sets = ~150 API calls instead of ~3,964.

Usage:
    python3 jobs/backfill_history.py                          # watchlist, 6 months
    python3 jobs/backfill_history.py --days 90                # 3 months
    python3 jobs/backfill_history.py --min-price 20           # only cards >= $20
    python3 jobs/backfill_history.py --source catalog         # full catalog instead of watchlist
    python3 jobs/backfill_history.py --resume                 # skip sets already fetched
"""

import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time

import pandas as pd

from db import get_connection, init_db
from api_clients.pokemon_price_tracker import (
    get_all_cards_in_set, get_credits_remaining, _extract_cards_list, _extract_prices,
    fetch_and_store_history, search_card, store_raw_price, store_psa10_price,
)

# API client enforces 2s between requests. No extra delay needed here.

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


def _get_completed_sets() -> set:
    """Get set names that already have backfilled history."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT c.set_name
        FROM raw_prices rp
        JOIN cards c ON c.id = rp.card_id
        WHERE rp.source = 'pokemonpricetracker'
        GROUP BY c.set_name
        HAVING COUNT(DISTINCT rp.recorded_date) > 7
    """)
    result = {r["set_name"] for r in cursor.fetchall()}
    conn.close()
    return result


def _store_history_batch(card_id: int, history: list):
    """Store price history entries for a card."""
    conn = get_connection()
    cursor = conn.cursor()
    stored = 0

    for point in history:
        d = point.get("date")
        p = point.get("price")
        if d and p:
            try:
                cursor.execute(
                    """INSERT OR IGNORE INTO raw_prices (card_id, price, source, recorded_date)
                       VALUES (?, ?, 'pokemonpricetracker', ?)""",
                    (card_id, float(p), d),
                )
                stored += cursor.rowcount
            except Exception as e:
                logger.error("Error storing history: %s", e)

        psa10_h = point.get("psa10")
        if d and psa10_h:
            try:
                cursor.execute(
                    """INSERT OR IGNORE INTO psa10_sales
                       (card_id, sale_price, sale_date, listing_title, source)
                       VALUES (?, ?, ?, 'PokemonPriceTracker PSA 10 history', 'pokemonpricetracker')""",
                    (card_id, float(psa10_h), d),
                )
                stored += cursor.rowcount
            except Exception as e:
                logger.error("Error storing PSA 10 history: %s", e)

    conn.commit()
    conn.close()
    return stored


def _build_api_set_map() -> dict:
    """Fetch set names from PokemonPriceTracker API and build a lookup.

    Returns dict mapping lowercase set name -> API's exact set name.
    """
    from api_clients.pokemon_price_tracker import get_sets
    api_set_map = {}
    data = get_sets()
    if not data:
        logger.warning("Could not fetch sets from API — will use raw set names")
        return api_set_map

    # Handle both list and dict-with-data formats
    sets_list = data if isinstance(data, list) else data.get("data", data.get("sets", []))
    if isinstance(sets_list, dict):
        sets_list = list(sets_list.values()) if sets_list else []

    for s in sets_list:
        if isinstance(s, dict):
            name = s.get("name", "") or s.get("setName", "")
            if name:
                api_set_map[name.lower().strip()] = name
        elif isinstance(s, str):
            api_set_map[s.lower().strip()] = s

    logger.info("Loaded %d set names from API", len(api_set_map))
    return api_set_map


def _resolve_set_name(our_name: str, api_set_map: dict) -> str:
    """Find the best API set name for our pokemontcg.io set name.

    Tries: exact match, then partial/substring match, then shortened variants.
    """
    lower = our_name.lower().strip()

    # Exact match
    if lower in api_set_map:
        return api_set_map[lower]

    # Substring match: find API sets that contain our name or vice versa
    for api_lower, api_name in api_set_map.items():
        if lower in api_lower or api_lower in lower:
            return api_name

    # Return original — the API does partial matching so it may still work
    return our_name


def _get_set_name_variants(set_name: str) -> list:
    """Generate alternative set name strings to try if the primary fails.

    E.g., "Expedition Base Set" -> ["Expedition Base", "Expedition"]
    """
    variants = []
    words = set_name.split()
    # Drop trailing words one at a time (e.g., "Expedition Base Set" -> "Expedition Base" -> "Expedition")
    for i in range(len(words) - 1, 0, -1):
        variant = " ".join(words[:i])
        if len(variant) >= 3:
            variants.append(variant)
    return variants


def backfill(days: int = 180, source: str = "watchlist",
             min_price: float = 5.0, resume: bool = False):
    """Backfill historical prices by fetching entire sets at once."""
    init_db()

    df = _load_cards(source, min_price)
    if df.empty:
        logger.error("No cards to process.")
        return

    # Fetch API set names for better matching
    api_set_map = _build_api_set_map()

    # Build a lookup of cards we want per set
    sets = {}
    for _, row in df.iterrows():
        set_id = row.get("set_id", "")
        set_name = row["set_name"]
        if not set_id and not set_name:
            continue
        key = set_id or set_name
        if key not in sets:
            # Resolve the set name to what the API expects
            api_name = _resolve_set_name(set_name, api_set_map) if api_set_map else set_name
            sets[key] = {"set_name": set_name, "api_name": api_name, "set_id": set_id, "cards": []}
        sets[key]["cards"].append({
            "name": row["name"],
            "set_name": set_name,
            "number": row["number"],
            "image_url": row.get("image_small", ""),
            "price_market": row.get("price_market", 0) or 0,
        })

    # Sort sets by total value (most valuable first)
    sorted_sets = sorted(sets.items(),
                         key=lambda x: sum(c["price_market"] for c in x[1]["cards"]),
                         reverse=True)

    # Skip completed sets if resuming
    skip_sets = set()
    if resume:
        skip_sets = _get_completed_sets()
        logger.info("Resume mode: skipping %d sets that already have history", len(skip_sets))

    total_sets = len(sorted_sets)
    total_cards = sum(len(s["cards"]) for _, s in sorted_sets)
    total_stored = 0
    sets_processed = 0
    cards_processed = 0

    logger.info("Backfilling %d days of history for %d cards across %d sets",
                days, total_cards, total_sets)
    logger.info("~%d API calls needed (1 per set) instead of %d (1 per card)",
                total_sets, total_cards)
    logger.info("Credits available: %d", get_credits_remaining())

    for i, (set_key, set_info) in enumerate(sorted_sets):
        credits = get_credits_remaining()
        if credits < 100:
            logger.warning("Low on credits (%d). Stopping at set %d/%d.",
                           credits, i + 1, total_sets)
            break

        set_name = set_info["set_name"]
        num_cards = len(set_info["cards"])

        if resume and set_name in skip_sets:
            cards_processed += num_cards
            logger.info("[%d/%d] Skipping %s (%d cards) — already has history",
                        i + 1, total_sets, set_name, num_cards)
            continue

        api_name = set_info.get("api_name", set_name)
        logger.info("[%d/%d] Fetching set '%s' (api: '%s', %d cards) — %d credits left",
                    i + 1, total_sets, set_name, api_name, num_cards, credits)

        # Try set-based bulk fetch: first with resolved API name, then with name variants
        set_stored = 0
        bulk_success = False
        bulk_cards = []

        # Try the resolved API name first
        names_to_try = [api_name]
        if api_name != set_name:
            names_to_try.append(set_name)
        # Add shortened variants as fallbacks
        names_to_try.extend(_get_set_name_variants(set_name))
        # Deduplicate while preserving order
        seen = set()
        unique_names = []
        for n in names_to_try:
            if n.lower() not in seen:
                seen.add(n.lower())
                unique_names.append(n)

        for try_name in unique_names:
            bulk_data = get_all_cards_in_set(
                try_name, include_history=True, days=days, include_ebay=True)
            if bulk_data:
                bulk_cards = _extract_cards_list(bulk_data)
                if bulk_cards:
                    logger.info("  Bulk fetch returned %d cards for '%s' (tried: '%s')",
                                len(bulk_cards), set_name, try_name)
                    bulk_success = True
                    break
            logger.info("  Bulk fetch empty with name '%s', trying next variant...", try_name)

        if bulk_success:
            # Build lookup by card name+number for matching
            wanted = {}
            for card_info in set_info["cards"]:
                key = (card_info["name"].lower(), card_info["number"])
                wanted[key] = card_info

            for api_card in bulk_cards:
                card_api_name = (api_card.get("name", "") or "").lower()
                api_number = api_card.get("number", "") or api_card.get("cardNumber", "")
                match_key = (card_api_name, api_number)

                card_info = wanted.get(match_key)
                if not card_info:
                    # Try matching by name only
                    for k, v in wanted.items():
                        if k[0] == card_api_name:
                            card_info = v
                            break

                if not card_info:
                    continue

                card_id = _ensure_card_in_db(
                    card_info["name"], card_info["set_name"],
                    card_info["number"], card_info["image_url"],
                )

                prices = _extract_prices(api_card)

                if "raw_price" in prices:
                    store_raw_price(card_id, prices["raw_price"])
                if "psa10_price" in prices:
                    store_psa10_price(card_id, prices["psa10_price"])

                history = prices.get("history", [])
                if history:
                    stored = _store_history_batch(card_id, history)
                    set_stored += stored

                cards_processed += 1

        # Fallback: per-card fetch if ALL name variants returned nothing
        if not bulk_success:
            logger.warning("  All bulk attempts failed for '%s' — falling back to per-card (%d cards)",
                           set_name, num_cards)
            for card_info in set_info["cards"]:
                if get_credits_remaining() < 100:
                    break

                card_id = _ensure_card_in_db(
                    card_info["name"], card_info["set_name"],
                    card_info["number"], card_info["image_url"],
                )

                stored = fetch_and_store_history(
                    card_id, card_info["name"], card_info["set_name"], days=days)
                set_stored += stored
                cards_processed += 1

        total_stored += set_stored
        sets_processed += 1
        logger.info("  Stored %d price points for %s", set_stored, set_name)

    logger.info("=" * 60)
    logger.info("Backfill complete:")
    logger.info("  Sets processed: %d / %d", sets_processed, total_sets)
    logger.info("  Cards processed: %d / %d", cards_processed, total_cards)
    logger.info("  Total price points stored: %d", total_stored)
    logger.info("  Credits remaining: %d", get_credits_remaining())


def main():
    parser = argparse.ArgumentParser(
        description="Backfill historical prices from PokemonPriceTracker (fetches by set)")
    parser.add_argument("--days", type=int, default=180,
                        help="Days of history to fetch (default: 180)")
    parser.add_argument("--source", type=str, default="watchlist", choices=["watchlist", "catalog"],
                        help="Card source: 'watchlist' (filtered) or 'catalog' (all) (default: watchlist)")
    parser.add_argument("--min-price", type=float, default=5.0,
                        help="Minimum market price to include (default: $5)")
    parser.add_argument("--resume", action="store_true",
                        help="Skip sets that already have history in the database")
    args = parser.parse_args()
    backfill(days=args.days, source=args.source,
             min_price=args.min_price, resume=args.resume)


if __name__ == "__main__":
    main()
