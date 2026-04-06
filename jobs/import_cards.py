"""Import all Pokemon cards from pokemontcg.io API and store as Parquet.

Downloads every card ever printed with metadata (name, set, number, rarity,
images, types, etc.) and saves to data/cards_catalog.parquet.

This is a one-time import that takes ~10-15 minutes. Re-run to update.

Usage:
    pip install pokemontcgsdk pyarrow pandas
    python3 jobs/import_cards.py                  # Full import
    python3 jobs/import_cards.py --sets-only       # Just list sets
    python3 jobs/import_cards.py --set sv4pt5      # Import one set
"""

import argparse
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BASE_URL = "https://api.pokemontcg.io/v2"
API_KEY = os.getenv("POKEMONTCG_API_KEY", "")  # optional, raises rate limit
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
CARDS_FILE = os.path.join(DATA_DIR, "cards_catalog.parquet")
SETS_FILE = os.path.join(DATA_DIR, "sets_catalog.parquet")

# Rate limiting: 30/min without key, much higher with key
REQUEST_DELAY = 0.5 if API_KEY else 2.0


def _headers():
    h = {"Accept": "application/json"}
    if API_KEY:
        h["X-Api-Key"] = API_KEY
    return h


def _get(endpoint, params=None, retries=3):
    """Make a rate-limited GET request with retries."""
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(retries):
        try:
            r = httpx.get(url, headers=_headers(), params=params,
                          timeout=30, follow_redirects=True)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 60))
                logger.warning("Rate limited. Waiting %ds...", wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:
            logger.error("HTTP %d for %s: %s", e.response.status_code, url, e)
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
        except httpx.RequestError as e:
            logger.error("Request error for %s: %s", url, e)
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
    return None


def fetch_all_sets():
    """Fetch all Pokemon TCG sets."""
    logger.info("Fetching all sets...")
    data = _get("sets", params={"pageSize": 250, "orderBy": "-releaseDate"})
    if not data:
        logger.error("Failed to fetch sets")
        return []

    sets = data["data"]
    logger.info("Found %d sets (total: %d)", len(sets), data.get("totalCount", 0))

    # Paginate if needed
    total = data.get("totalCount", len(sets))
    page = 2
    while len(sets) < total:
        time.sleep(REQUEST_DELAY)
        data = _get("sets", params={"pageSize": 250, "page": page, "orderBy": "-releaseDate"})
        if not data or not data["data"]:
            break
        sets.extend(data["data"])
        page += 1

    return sets


def fetch_cards_for_set(set_id, set_name):
    """Fetch all cards for a specific set."""
    cards = []
    page = 1
    page_size = 250

    while True:
        time.sleep(REQUEST_DELAY)
        data = _get("cards", params={
            "q": f"set.id:{set_id}",
            "pageSize": page_size,
            "page": page,
        })
        if not data or not data["data"]:
            break

        cards.extend(data["data"])
        total = data.get("totalCount", 0)

        if len(cards) >= total:
            break
        page += 1

    return cards


def flatten_card(card):
    """Flatten nested card JSON into a flat dict for Parquet storage."""
    set_data = card.get("set", {})
    images = card.get("images", {})
    tcgplayer = card.get("tcgplayer", {})
    prices = tcgplayer.get("prices", {})

    # Extract the best available price data
    # TCGPlayer prices come in variants: normal, holofoil, reverseHolofoil, etc.
    price_market = None
    price_low = None
    price_variant = None
    for variant in ["holofoil", "normal", "reverseHolofoil", "1stEditionHolofoil",
                     "1stEditionNormal", "unlimitedHolofoil"]:
        if variant in prices:
            vp = prices[variant]
            price_market = vp.get("market")
            price_low = vp.get("low")
            price_variant = variant
            break

    return {
        "card_id": card.get("id", ""),
        "name": card.get("name", ""),
        "supertype": card.get("supertype", ""),
        "subtypes": "|".join(card.get("subtypes", [])),
        "hp": card.get("hp", ""),
        "types": "|".join(card.get("types", [])),
        "set_id": set_data.get("id", ""),
        "set_name": set_data.get("name", ""),
        "set_series": set_data.get("series", ""),
        "number": card.get("number", ""),
        "rarity": card.get("rarity", ""),
        "artist": card.get("artist", ""),
        "image_small": images.get("small", ""),
        "image_large": images.get("large", ""),
        "tcgplayer_url": tcgplayer.get("url", ""),
        "tcgplayer_updated": tcgplayer.get("updatedAt", ""),
        "price_variant": price_variant or "",
        "price_market": price_market,
        "price_low": price_low,
        "release_date": set_data.get("releaseDate", ""),
    }


def import_all_cards():
    """Import all cards from every set and save to Parquet."""
    os.makedirs(DATA_DIR, exist_ok=True)

    # Step 1: Fetch all sets
    sets = fetch_all_sets()
    if not sets:
        return

    # Save sets catalog
    sets_flat = []
    for s in sets:
        sets_flat.append({
            "set_id": s.get("id", ""),
            "name": s.get("name", ""),
            "series": s.get("series", ""),
            "printed_total": s.get("printedTotal", 0),
            "total": s.get("total", 0),
            "release_date": s.get("releaseDate", ""),
            "symbol": s.get("images", {}).get("symbol", ""),
            "logo": s.get("images", {}).get("logo", ""),
        })
    sets_df = pd.DataFrame(sets_flat)
    sets_df.to_parquet(SETS_FILE, compression="zstd", index=False)
    logger.info("Saved %d sets to %s", len(sets_df), SETS_FILE)

    # Step 2: Fetch cards for each set
    all_cards = []
    total_sets = len(sets)

    # Check for existing partial import to resume
    existing_set_ids = set()
    if os.path.exists(CARDS_FILE):
        try:
            existing = pd.read_parquet(CARDS_FILE)
            existing_set_ids = set(existing["set_id"].unique())
            all_cards = existing.to_dict("records")
            logger.info("Resuming import — %d cards from %d sets already imported",
                        len(all_cards), len(existing_set_ids))
        except Exception:
            pass

    for i, s in enumerate(sets):
        set_id = s["id"]
        set_name = s["name"]
        set_total = s.get("total", 0)

        if set_id in existing_set_ids:
            logger.info("[%d/%d] Skipping %s (%s) — already imported",
                        i + 1, total_sets, set_name, set_id)
            continue

        logger.info("[%d/%d] Fetching %s (%s) — %d cards...",
                     i + 1, total_sets, set_name, set_id, set_total)

        cards = fetch_cards_for_set(set_id, set_name)
        if cards:
            flat = [flatten_card(c) for c in cards]
            all_cards.extend(flat)
            logger.info("  Got %d cards (running total: %d)", len(flat), len(all_cards))

            # Save checkpoint every 10 sets
            if (i + 1) % 10 == 0:
                _save_parquet(all_cards)

    # Final save
    _save_parquet(all_cards)
    logger.info("Import complete: %d total cards from %d sets", len(all_cards), total_sets)


def _save_parquet(cards_list):
    """Save cards list to Parquet."""
    df = pd.DataFrame(cards_list)
    df.to_parquet(CARDS_FILE, compression="zstd", index=False)
    size_mb = os.path.getsize(CARDS_FILE) / (1024 * 1024)
    logger.info("Saved %d cards to %s (%.1f MB)", len(df), CARDS_FILE, size_mb)


def import_single_set(set_id):
    """Import cards from a single set."""
    os.makedirs(DATA_DIR, exist_ok=True)
    logger.info("Fetching cards for set: %s", set_id)
    cards = fetch_cards_for_set(set_id, set_id)
    if not cards:
        logger.error("No cards found for set %s", set_id)
        return

    flat = [flatten_card(c) for c in cards]
    df = pd.DataFrame(flat)

    # Append to existing catalog if it exists
    if os.path.exists(CARDS_FILE):
        existing = pd.read_parquet(CARDS_FILE)
        # Remove old entries for this set, then append new
        existing = existing[existing["set_id"] != set_id]
        df = pd.concat([existing, df], ignore_index=True)

    df.to_parquet(CARDS_FILE, compression="zstd", index=False)
    logger.info("Saved %d cards for set %s (catalog total: %d)", len(flat), set_id, len(df))


def list_sets():
    """List all sets to stdout."""
    sets = fetch_all_sets()
    if not sets:
        return
    print(f"\n{'ID':<20} {'Name':<40} {'Series':<25} {'Cards':<8} {'Released'}")
    print("-" * 110)
    for s in sets:
        print(f"{s['id']:<20} {s['name']:<40} {s.get('series',''):<25} {s.get('total',0):<8} {s.get('releaseDate','')}")
    print(f"\nTotal: {len(sets)} sets")


def load_catalog():
    """Load the card catalog from Parquet. Returns a DataFrame."""
    if not os.path.exists(CARDS_FILE):
        logger.error("No catalog found at %s. Run import first.", CARDS_FILE)
        return None
    df = pd.read_parquet(CARDS_FILE)
    logger.info("Loaded %d cards from catalog", len(df))
    return df


def sync_catalog_to_db(min_price=None):
    """Sync cards from Parquet catalog into the SQLite cards table.

    Args:
        min_price: Only import cards with a TCGPlayer market price >= this value.
                   None means import all.
    """
    df = load_catalog()
    if df is None:
        return

    if min_price is not None:
        df = df[df["price_market"].notna() & (df["price_market"] >= min_price)]
        logger.info("Filtered to %d cards with market price >= $%.2f", len(df), min_price)

    from db import get_connection, init_db
    init_db()
    conn = get_connection()
    cursor = conn.cursor()

    inserted = 0
    for _, row in df.iterrows():
        try:
            cursor.execute(
                """INSERT OR IGNORE INTO cards (name, set_name, card_number, image_url)
                   VALUES (?, ?, ?, ?)""",
                (row["name"], row["set_name"], row["number"], row["image_small"]),
            )
            inserted += cursor.rowcount
        except Exception as e:
            logger.error("Error inserting %s: %s", row["name"], e)

    conn.commit()
    conn.close()
    logger.info("Synced %d new cards to SQLite (of %d candidates)", inserted, len(df))


def main():
    parser = argparse.ArgumentParser(description="Import Pokemon cards from pokemontcg.io")
    parser.add_argument("--sets-only", action="store_true", help="Just list all sets")
    parser.add_argument("--set", type=str, help="Import a single set by ID (e.g. 'base1', 'sv4pt5')")
    parser.add_argument("--sync-db", action="store_true",
                        help="Sync Parquet catalog into SQLite cards table")
    parser.add_argument("--min-price", type=float, default=None,
                        help="Only sync cards with market price >= this (use with --sync-db)")
    args = parser.parse_args()

    if args.sets_only:
        list_sets()
    elif args.set:
        import_single_set(args.set)
    elif args.sync_db:
        sync_catalog_to_db(min_price=args.min_price)
    else:
        import_all_cards()


if __name__ == "__main__":
    main()
