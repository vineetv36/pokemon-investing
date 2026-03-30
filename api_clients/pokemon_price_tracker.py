"""Client for the PokemonPriceTracker API (free tier)."""

import logging
import os
import time
from datetime import date
from typing import Optional

import httpx
from dotenv import load_dotenv

from db import get_connection

load_dotenv()
logger = logging.getLogger(__name__)

BASE_URL = "https://www.pokemonpricetracker.com/api/v2"
API_KEY = os.getenv("POKEMON_PRICE_TRACKER_API_KEY", "")

# Free tier: 100 credits/day, 2 requests/minute
DAILY_CREDIT_LIMIT = 100
_credits_used = 0
_last_request_time = 0.0


def _get_headers() -> dict:
    return {"Authorization": f"Bearer {API_KEY}"}


def _rate_limit():
    """Enforce at least 60 seconds between requests (1 req/min to stay safe)."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < 60.0:
        wait = 60.0 - elapsed
        logger.info("Rate limit: waiting %.0fs before next API call...", wait)
        time.sleep(wait)
    _last_request_time = time.time()


def _make_request(endpoint: str, params: Optional[dict] = None) -> Optional[dict]:
    """Make an authenticated GET request with rate limiting and error handling."""
    global _credits_used

    if _credits_used >= DAILY_CREDIT_LIMIT:
        logger.warning("Daily credit limit reached (%d). Stopping API calls.", DAILY_CREDIT_LIMIT)
        return None

    _rate_limit()

    url = f"{BASE_URL}{endpoint}"
    try:
        response = httpx.get(url, headers=_get_headers(), params=params, timeout=30)

        if response.status_code == 429:
            for backoff in [60, 120, 240]:
                logger.warning("Rate limited (429). Backing off %ds...", backoff)
                time.sleep(backoff)
                response = httpx.get(url, headers=_get_headers(), params=params, timeout=30)
                if response.status_code != 429:
                    break
            if response.status_code == 429:
                logger.error("Still rate limited after exponential backoff. Giving up.")
                return None

        if response.status_code == 401:
            logger.error("API key invalid (401). Check POKEMON_PRICE_TRACKER_API_KEY.")
            return None

        response.raise_for_status()
        _credits_used += 1
        return response.json()

    except httpx.HTTPStatusError as e:
        logger.error("HTTP error from PokemonPriceTracker: %s", e)
        return None
    except httpx.RequestError as e:
        logger.error("Request error: %s", e)
        return None


def search_card(name: str, set_id: Optional[str] = None) -> Optional[dict]:
    """Search for a card by name and optionally set ID."""
    params = {"search": name}
    if set_id:
        params["setId"] = set_id
    return _make_request("/cards", params)


def get_card_by_tcgplayer_id(tcgplayer_id: str) -> Optional[dict]:
    """Fetch card data by TCGPlayer ID."""
    return _make_request("/cards", {"tcgPlayerId": tcgplayer_id})


def get_sets() -> Optional[dict]:
    """Fetch all available sets."""
    return _make_request("/sets")


def fetch_and_store_raw_price(card_id: int, card_name: str, set_name: str) -> Optional[float]:
    """Fetch current raw NM price for a card and store it."""
    data = search_card(card_name, set_name)
    if not data:
        return None

    # Extract price from response - structure depends on API
    cards = data if isinstance(data, list) else data.get("data", data.get("cards", []))
    if not cards:
        logger.warning("No results found for %s (%s)", card_name, set_name)
        return None

    card_data = cards[0] if isinstance(cards, list) else cards
    price = (
        card_data.get("market_price")
        or card_data.get("tcgplayer_price")
        or card_data.get("price")
    )

    if price is None:
        logger.warning("No price found for %s", card_name)
        return None

    price = float(price)

    # Store in database
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """INSERT OR REPLACE INTO raw_prices (card_id, price, source, recorded_date)
               VALUES (?, ?, 'pokemonpricetracker', ?)""",
            (card_id, price, date.today().isoformat()),
        )
        conn.commit()
        logger.info("Stored raw price $%.2f for card %d (%s)", price, card_id, card_name)
    except Exception as e:
        logger.error("Error storing raw price for card %d: %s", card_id, e)
    finally:
        conn.close()

    return price


def get_credits_remaining() -> int:
    """Return estimated credits remaining for today."""
    return max(0, DAILY_CREDIT_LIMIT - _credits_used)
