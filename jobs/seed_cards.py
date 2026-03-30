"""Seed the database with initial watchlist cards."""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection, init_db

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


def seed():
    """Insert seed cards into the database."""
    init_db()
    conn = get_connection()
    cursor = conn.cursor()

    inserted = 0
    for card in SEED_CARDS:
        try:
            cursor.execute(
                """INSERT OR IGNORE INTO cards (name, set_name, card_number)
                   VALUES (?, ?, ?)""",
                (card["name"], card["set"], card["number"]),
            )
            if cursor.rowcount > 0:
                inserted += 1
                print(f"  Added: {card['name']} ({card['set']} #{card['number']})")
            else:
                print(f"  Already exists: {card['name']} ({card['set']} #{card['number']})")
        except Exception as e:
            print(f"  Error adding {card['name']}: {e}")

    conn.commit()
    conn.close()
    print(f"\nSeeded {inserted} new cards ({len(SEED_CARDS)} total in watchlist).")


if __name__ == "__main__":
    seed()
