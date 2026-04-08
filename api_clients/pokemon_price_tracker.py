"""Client for PokemonPriceTracker API (paid tier).

Endpoint: GET /api/v2/cards
  Params: search, setId, tcgPlayerId, includeHistory, days, maxDataPoints,
          fetchAllInSet, limit, sortBy, includeEbay

History is returned inline via includeHistory=true&days=180, NOT a separate endpoint.

Paid tier: ~20,000 credits/day, 60 req/min, 6 months history.
"""

import logging
import os
import ssl
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

# Paid tier limits
DAILY_CREDIT_LIMIT = 20000
REQUESTS_PER_MINUTE = 60
_credits_used = 0
_request_times = []  # sliding window for rate limiting

# SSL context (needed in some environments)
_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE


def _get_headers() -> dict:
    return {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }


def _rate_limit():
    """Enforce 60 requests/minute sliding window."""
    global _request_times
    now = time.time()
    _request_times = [t for t in _request_times if now - t < 60]
    if len(_request_times) >= REQUESTS_PER_MINUTE:
        wait = 60 - (now - _request_times[0]) + 0.1
        logger.info("Rate limit: waiting %.1fs...", wait)
        time.sleep(wait)
    _request_times.append(time.time())


def _make_request(endpoint: str, params: Optional[dict] = None,
                  credits: int = 1) -> Optional[dict]:
    """Make an authenticated GET request with rate limiting and retries."""
    global _credits_used

    if _credits_used + credits > DAILY_CREDIT_LIMIT:
        logger.warning("Daily credit limit approaching (%d/%d). Stopping.",
                       _credits_used, DAILY_CREDIT_LIMIT)
        return None

    _rate_limit()

    url = f"{BASE_URL}{endpoint}"
    logger.info("HTTP Request: GET %s params=%s", url, params)

    try:
        response = httpx.get(url, headers=_get_headers(), params=params,
                             timeout=30, verify=_ssl_ctx)

        if response.status_code == 429:
            for backoff in [60, 120, 240]:
                logger.warning("Rate limited (429). Backing off %ds...", backoff)
                time.sleep(backoff)
                response = httpx.get(url, headers=_get_headers(), params=params,
                                     timeout=30, verify=_ssl_ctx)
                if response.status_code != 429:
                    break
            if response.status_code == 429:
                logger.error("Still rate limited after backoff. Giving up.")
                return None

        if response.status_code == 401:
            logger.error("API key invalid (401). Check POKEMON_PRICE_TRACKER_API_KEY.")
            return None

        if response.status_code == 404:
            logger.warning("404 for %s — card not found", url)
            return None

        response.raise_for_status()
        _credits_used += credits

        # Parse response — guard against HTML responses
        content_type = response.headers.get("content-type", "")
        if "application/json" not in content_type and "text/json" not in content_type:
            logger.error("Non-JSON response (content-type: %s): %s",
                         content_type, response.text[:200])
            return None

        return response.json()

    except httpx.HTTPStatusError as e:
        logger.error("HTTP error: %d %s", e.response.status_code, e.response.text[:200])
        return None
    except httpx.RequestError as e:
        logger.error("Request error: %s", e)
        return None


# --- Card Lookup ---

def search_card(name: str, set_name: Optional[str] = None,
                include_history: bool = False, days: int = 0) -> Optional[dict]:
    """Search for a card by name, optionally with price history.

    GET /api/v2/cards?search=charizard&setId=base1&includeHistory=true&days=180
    """
    params = {"search": name}
    if set_name:
        params["setId"] = set_name
    if include_history and days > 0:
        params["includeHistory"] = "true"
        params["days"] = days
    return _make_request("/cards", params)


def get_card_by_tcgplayer_id(tcgplayer_id: str, include_history: bool = False,
                              days: int = 0) -> Optional[dict]:
    """Fetch card by TCGPlayer ID, optionally with history."""
    params = {"tcgPlayerId": tcgplayer_id}
    if include_history and days > 0:
        params["includeHistory"] = "true"
        params["days"] = days
    return _make_request("/cards", params)


def get_sets() -> Optional[dict]:
    """Fetch all available sets."""
    return _make_request("/sets")


def get_all_cards_in_set(set_id: str, include_history: bool = False,
                          days: int = 0) -> Optional[dict]:
    """Fetch all cards in a set (1 credit per card returned)."""
    params = {"set": set_id, "fetchAllInSet": "true"}
    if include_history and days > 0:
        params["includeHistory"] = "true"
        params["days"] = days
    return _make_request("/cards", params, credits=0)


# --- Storage ---

def store_raw_price(card_id: int, price: float, recorded_date: Optional[date] = None):
    """Store a raw NM price in the database."""
    if recorded_date is None:
        recorded_date = date.today()
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """INSERT OR REPLACE INTO raw_prices (card_id, price, source, recorded_date)
               VALUES (?, ?, 'pokemonpricetracker', ?)""",
            (card_id, price, recorded_date.isoformat()),
        )
        conn.commit()
    except Exception as e:
        logger.error("Error storing raw price: %s", e)
    finally:
        conn.close()


def store_psa10_price(card_id: int, price: float, recorded_date: Optional[date] = None):
    """Store a PSA 10 price data point."""
    if recorded_date is None:
        recorded_date = date.today()
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """INSERT OR IGNORE INTO psa10_sales
               (card_id, sale_price, sale_date, listing_title, source)
               VALUES (?, ?, ?, 'PokemonPriceTracker PSA 10 market price', 'pokemonpricetracker')""",
            (card_id, price, recorded_date.isoformat()),
        )
        conn.commit()
    except Exception as e:
        logger.error("Error storing PSA 10 price: %s", e)
    finally:
        conn.close()


def _extract_cards_list(data: dict) -> list:
    """Extract list of card dicts from API response (handles multiple formats)."""
    if isinstance(data, list):
        return data
    if "data" in data:
        return data["data"] if isinstance(data["data"], list) else [data["data"]]
    if "cards" in data:
        return data["cards"] if isinstance(data["cards"], list) else [data["cards"]]
    return [data]


def _extract_prices(card_data: dict) -> dict:
    """Extract raw, PSA 10, and history from a card response."""
    result = {}

    # Raw NM price — try multiple field names
    prices = card_data.get("prices", {})
    raw_price = (
        prices.get("market")
        or card_data.get("marketPrice")
        or card_data.get("market_price")
        or card_data.get("tcgplayer_price")
        or card_data.get("price")
    )
    if raw_price is not None:
        result["raw_price"] = float(raw_price)

    # PSA 10 price — try multiple locations
    graded = card_data.get("gradedPrices", {})
    ebay = card_data.get("ebay", {})
    psa10 = ebay.get("psa10", {})

    psa10_price = (
        graded.get("psa10")
        or psa10.get("avg")
        or psa10.get("market_price")
        or psa10.get("last_sold")
    )
    if psa10_price is not None:
        result["psa10_price"] = float(psa10_price)

    # Price history — multiple possible formats:
    #   1. Dict: {"2026-01-15": 125.50, ...} (date keys, price values)
    #   2. List of dicts: [{"date": "...", "price": ...}, ...]
    #   3. List of lists: [["2026-01-15", 125.50], ...]
    #   4. List of dicts with nested: [{"date": "...", "tcgplayer": {"market": ...}}, ...]
    history = card_data.get("priceHistory", {})
    if history:
        result["history"] = []

        if isinstance(history, dict):
            # Format 1: {date_string: price_value, ...}
            for d, val in history.items():
                if isinstance(val, (int, float)):
                    result["history"].append({"date": d, "price": float(val)})
                elif isinstance(val, dict):
                    p = val.get("market") or val.get("price")
                    if p is not None:
                        entry = {"date": d, "price": float(p)}
                        psa10_h = val.get("psa10")
                        if psa10_h is not None:
                            entry["psa10"] = float(psa10_h)
                        result["history"].append(entry)

        elif isinstance(history, list):
            # Log first element to understand format
            if history and not isinstance(history[0], dict):
                logger.info("priceHistory format sample: %s (type: %s)",
                            repr(history[0])[:100], type(history[0]).__name__)

            for point in history:
                if isinstance(point, dict):
                    # Format 2 or 4: {"date": ..., "price": ...}
                    d = point.get("date")
                    tcg = point.get("tcgplayer", {}) if isinstance(point.get("tcgplayer"), dict) else {}
                    p = tcg.get("market") or point.get("price") or point.get("market")
                    psa10_h = point.get("psa10")
                    if d and p:
                        entry = {"date": d, "price": float(p)}
                        if psa10_h is not None:
                            entry["psa10"] = float(psa10_h)
                        result["history"].append(entry)
                elif isinstance(point, (list, tuple)) and len(point) >= 2:
                    # Format 3: [date, price]
                    result["history"].append({"date": str(point[0]), "price": float(point[1])})
                # Skip strings or other unexpected types

    return result


def fetch_and_store_card_prices(card_id: int, card_name: str,
                                 set_name: str) -> Optional[dict]:
    """Fetch and store both raw NM and PSA 10 prices for a card (current only).

    This is the main function for daily_job.py.
    """
    data = search_card(card_name, set_name)
    if not data:
        return None

    cards = _extract_cards_list(data)
    if not cards:
        logger.warning("No results for %s (%s)", card_name, set_name)
        return None

    prices = _extract_prices(cards[0])

    if "raw_price" in prices:
        store_raw_price(card_id, prices["raw_price"])
        logger.info("Card %s: raw NM = $%.2f", card_name, prices["raw_price"])

    if "psa10_price" in prices:
        store_psa10_price(card_id, prices["psa10_price"])
        logger.info("Card %s: PSA 10 = $%.2f", card_name, prices["psa10_price"])

    return prices if prices else None


def fetch_and_store_history(card_id: int, card_name: str,
                            set_name: Optional[str] = None,
                            days: int = 180) -> int:
    """Fetch historical prices and store daily entries.

    Uses: GET /api/v2/cards?search=name&setId=set&includeHistory=true&days=180

    Stores raw prices from priceHistory array into raw_prices table,
    and PSA 10 prices into psa10_sales table.
    """
    data = search_card(card_name, set_name, include_history=True, days=days)
    if not data:
        return 0

    cards = _extract_cards_list(data)
    if not cards:
        logger.warning("No results for %s", card_name)
        return 0

    prices = _extract_prices(cards[0])

    # Store current price
    if "raw_price" in prices:
        store_raw_price(card_id, prices["raw_price"])
    if "psa10_price" in prices:
        store_psa10_price(card_id, prices["psa10_price"])

    # Store historical prices
    history = prices.get("history", [])
    if not history:
        logger.warning("No history data for %s", card_name)
        return 0

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
                logger.error("Error storing history point: %s", e)

        # Store PSA 10 history if available
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
                logger.error("Error storing PSA 10 history point: %s", e)

    conn.commit()
    conn.close()
    logger.info("Stored %d historical price points for card %d (%s)", stored, card_id, card_name)
    return stored


def get_credits_remaining() -> int:
    """Return estimated credits remaining for today."""
    return max(0, DAILY_CREDIT_LIMIT - _credits_used)
