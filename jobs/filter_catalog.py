"""Analyze and filter the downloaded card catalog.

Reads data/cards_catalog.parquet and prints breakdown by price tier and rarity,
then exports a filtered watchlist of cards worth tracking for PSA 10 momentum.

Usage:
    python3 jobs/filter_catalog.py                    # Show stats, export >= $5
    python3 jobs/filter_catalog.py --min-price 10     # Export cards >= $10
    python3 jobs/filter_catalog.py --min-price 0 --rarity-only  # Filter by rarity only
    python3 jobs/filter_catalog.py --sync-db           # Also push filtered cards to SQLite
"""

import argparse
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
CATALOG_FILE = os.path.join(DATA_DIR, "cards_catalog.parquet")
WATCHLIST_FILE = os.path.join(DATA_DIR, "watchlist.parquet")

# Rarities worth tracking for PSA 10 momentum (skip commons/uncommons/bulk)
TRACKABLE_RARITIES = {
    # Vintage
    "Rare Holo",
    "Rare Holo EX",
    "Rare Holo GX",
    "Rare Holo V",
    "Rare Holo VMAX",
    "Rare Holo VSTAR",
    # Modern ultra rares
    "Rare Ultra",
    "Rare Secret",
    "Rare Rainbow",
    "Rare Shiny",
    "Rare Shiny GX",
    "Rare BREAK",
    "Rare Prime",
    "Rare Prism Star",
    "Rare ACE",
    # Illustration rares (modern sets)
    "Illustration Rare",
    "Special Illustration Rare",
    "Hyper Rare",
    "Double Rare",
    "Ultra Rare",
    # ex/EX era
    "Rare Holo ex",
    "Rare ex",
    # LEGEND
    "LEGEND",
    # Promos worth tracking
    "Classic Collection",
    "Amazing Rare",
    "Radiant Rare",
    "Shiny Rare",
    "Shiny Ultra Rare",
    # SV era
    "ACE SPEC Rare",
    "Trainer Gallery Rare Holo",
}


def load_catalog():
    """Load the full card catalog."""
    if not os.path.exists(CATALOG_FILE):
        logger.error("Catalog not found at %s", CATALOG_FILE)
        logger.error("Run: python3 jobs/import_cards.py")
        sys.exit(1)
    return pd.read_parquet(CATALOG_FILE)


def print_stats(df):
    """Print catalog stats."""
    print(f"\n{'=' * 60}")
    print(f"CARD CATALOG STATS")
    print(f"{'=' * 60}")
    print(f"Total cards:           {len(df):>8}")
    has_price = df["price_market"].notna()
    print(f"With market price:     {has_price.sum():>8}")
    print(f"Without market price:  {(~has_price).sum():>8}")

    print(f"\n--- By Price Tier ---")
    for thresh in [1, 5, 10, 20, 50, 100, 250, 500, 1000]:
        count = (df["price_market"] >= thresh).sum()
        print(f"  >= ${thresh:>6}:  {count:>6} cards")

    print(f"\n--- By Rarity (top 20) ---")
    rarity_counts = df["rarity"].value_counts().head(20)
    for rarity, count in rarity_counts.items():
        trackable = "  *" if rarity in TRACKABLE_RARITIES else ""
        print(f"  {rarity:<35} {count:>6}{trackable}")
    print("  (* = included in trackable rarities)")

    print(f"\n--- By Set Series (top 10) ---")
    series_counts = df["set_series"].value_counts().head(10)
    for series, count in series_counts.items():
        avg_price = df.loc[df["set_series"] == series, "price_market"].mean()
        avg_str = f"${avg_price:.2f}" if pd.notna(avg_price) else "N/A"
        print(f"  {series:<30} {count:>6}   avg: {avg_str}")

    print(f"\n--- Top 20 Most Valuable ---")
    top = df.nlargest(20, "price_market")[["name", "set_name", "number", "rarity", "price_market"]]
    for _, r in top.iterrows():
        print(f"  ${r['price_market']:>10.2f}  {r['name']} ({r['set_name']} #{r['number']}) [{r['rarity']}]")


def filter_watchlist(df, min_price=5.0, rarity_only=False):
    """Filter catalog to cards worth tracking.

    Strategy:
    1. Cards with market price >= min_price
    2. OR cards with trackable rarity (even if price is missing)
    """
    price_mask = df["price_market"].notna() & (df["price_market"] >= min_price)
    rarity_mask = df["rarity"].isin(TRACKABLE_RARITIES)

    if rarity_only:
        mask = rarity_mask
    else:
        # Include cards that meet price threshold OR have trackable rarity with any price
        mask = price_mask | (rarity_mask & df["price_market"].notna())

    filtered = df[mask].copy()

    # Sort by market price descending, nulls last
    filtered = filtered.sort_values("price_market", ascending=False, na_position="last")

    return filtered


def sync_to_db(df):
    """Push filtered watchlist into SQLite cards table."""
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
    logger.info("Synced %d new cards to SQLite (of %d in watchlist)", inserted, len(df))


def main():
    parser = argparse.ArgumentParser(description="Filter card catalog for PSA 10 momentum tracking")
    parser.add_argument("--min-price", type=float, default=5.0,
                        help="Minimum market price to include (default: $5)")
    parser.add_argument("--rarity-only", action="store_true",
                        help="Filter by rarity only, ignore price threshold")
    parser.add_argument("--sync-db", action="store_true",
                        help="Push filtered cards into SQLite cards table")
    parser.add_argument("--stats-only", action="store_true",
                        help="Just print stats, don't export")
    args = parser.parse_args()

    df = load_catalog()
    print_stats(df)

    if args.stats_only:
        return

    filtered = filter_watchlist(df, min_price=args.min_price, rarity_only=args.rarity_only)

    print(f"\n{'=' * 60}")
    print(f"FILTERED WATCHLIST")
    print(f"{'=' * 60}")
    print(f"Cards selected:  {len(filtered)}")
    if len(filtered) > 0 and filtered["price_market"].notna().any():
        print(f"Price range:     ${filtered['price_market'].min():.2f} - ${filtered['price_market'].max():.2f}")
        print(f"Median price:    ${filtered['price_market'].median():.2f}")

    print(f"\neBay API calls needed: {len(filtered) * 2} (2 per card: PSA 10 + raw)")
    days_needed = (len(filtered) * 2) / 5000
    print(f"Days at 5k calls/day:  {days_needed:.1f}")
    print(f"Days at no-limit scrape: <1 (with rate limiting)")

    # Save filtered watchlist
    filtered.to_parquet(WATCHLIST_FILE, compression="zstd", index=False)
    size_kb = os.path.getsize(WATCHLIST_FILE) / 1024
    print(f"\nSaved to {WATCHLIST_FILE} ({size_kb:.0f} KB)")

    if args.sync_db:
        sync_to_db(filtered)


if __name__ == "__main__":
    main()
