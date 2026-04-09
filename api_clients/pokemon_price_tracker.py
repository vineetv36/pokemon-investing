"""Client for PokemonPriceTracker API (paid tier).

Endpoint: GET /api/v2/cards
  Params: search, set (partial name match), setId (TCGPlayer GroupId),
          tcgPlayerId, includeHistory, includeEbay, includeBoth,
          days, maxDataPoints, fetchAllInSet, limit, sortBy

History format (includeHistory=true):
  priceHistory.conditions["Near Mint"].history[{date, market, volume}, ...]

PSA 10 format (includeEbay=true):
  ebay.salesByGrade.psa10.{averagePrice, medianPrice, marketPrice7Day, ...}

Credit costs: cards_returned × (1 + includeHistory + includeEbay)
  includeBoth=true is shorthand for includeHistory + includeEbay (3 credits/card)

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
_last_request_time = 0.0

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
    """Enforce minimum 3 seconds between every request (20 req/min max)."""
    global _last_request_time
    now = time.time()
    elapsed = now - _last_request_time
    if elapsed < 3.0:
        time.sleep(3.0 - elapsed)
    _last_request_time = time.time()


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
    except httpx.RequestError as e:
        logger.error("Request error: %s", e)
        return None

    if response.status_code == 429:
        for backoff in [60, 120, 240]:
            logger.warning("Rate limited (429). Backing off %ds...", backoff)
            time.sleep(backoff)
            _last_request_time = time.time()
            try:
                response = httpx.get(url, headers=_get_headers(), params=params,
                                     timeout=30, verify=_ssl_ctx)
            except httpx.RequestError as e:
                logger.error("Request error during retry: %s", e)
                return None
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

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.error("HTTP error: %d %s", e.response.status_code, e.response.text[:200])
        return None

    _credits_used += credits

    # Parse response — guard against HTML responses
    content_type = response.headers.get("content-type", "")
    if "application/json" not in content_type and "text/json" not in content_type:
        logger.error("Non-JSON response (content-type: %s): %s",
                     content_type, response.text[:200])
        return None

    return response.json()


# --- Card Lookup ---

def search_card(name: str, set_name: Optional[str] = None,
                include_history: bool = False, days: int = 0,
                include_ebay: bool = False) -> Optional[dict]:
    """Search for a card by name, optionally with price history and eBay data.

    GET /api/v2/cards?search=charizard&set=Base Set&includeHistory=true&days=180
    Note: 'set' param accepts partial name match (e.g., "base set").
    """
    params = {"search": name}
    if set_name:
        # Use 'set' param (partial name match), NOT 'setId' (numeric TCGPlayer GroupId)
        params["set"] = set_name
    if include_history and include_ebay:
        params["includeBoth"] = "true"
        if days > 0:
            params["days"] = days
    elif include_history:
        params["includeHistory"] = "true"
        if days > 0:
            params["days"] = days
    elif include_ebay:
        params["includeEbay"] = "true"

    # Credit cost: 1 base + 1 for history + 1 for ebay per card returned
    credits = 1
    if include_history:
        credits += 1
    if include_ebay:
        credits += 1
    return _make_request("/cards", params, credits=credits)


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


def get_all_cards_in_set(set_name: str, include_history: bool = False,
                          days: int = 0, include_ebay: bool = False) -> Optional[dict]:
    """Fetch all cards in a set using human-readable set name.

    Uses 'set' param (partial name match, e.g., "Base Set", "Neo Genesis").
    With fetchAllInSet=true, limit increases to 200 (100 with history, 50 with both).

    Credit cost: cards_returned × (1 + includeHistory + includeEbay).
    """
    params = {"set": set_name, "fetchAllInSet": "true"}
    if include_history and include_ebay:
        params["includeBoth"] = "true"
        if days > 0:
            params["days"] = days
    elif include_history:
        params["includeHistory"] = "true"
        if days > 0:
            params["days"] = days
    elif include_ebay:
        params["includeEbay"] = "true"
    # Credits are per-card-returned; pass 0 here and track manually if needed
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
    """Extract raw NM price, PSA 10 price, and history from API response.

    Handles the actual PokemonPriceTracker API v2 response format:

    Top-level price fields:
      marketPrice, lowPrice, midPrice, highPrice, directLowPrice

    priceHistory (when includeHistory=true):
      {
        "conditions": {
          "Near Mint": {
            "history": [{"date": "2024-01-15T00:00:00Z", "market": 420, "volume": 12}, ...],
            "latestPrice": 420,
            "priceRange": {"min": 380, "max": 450}
          }
        }
      }

    ebay (when includeEbay=true):
      {
        "salesByGrade": {
          "psa10": {
            "averagePrice": 15000,
            "medianPrice": 14500,
            "marketPrice7Day": 14800,
            "smartMarketPrice": {...},
            "salesCount": 25,
            "lastSoldDate": "...",
            "lastSoldPrice": 14200
          }
        }
      }
    """
    result = {}

    # --- Raw NM price ---
    raw_price = (
        card_data.get("marketPrice")
        or card_data.get("lowPrice")
        or card_data.get("midPrice")
        or card_data.get("price")
    )
    # Also check nested "prices" dict as fallback
    if raw_price is None:
        prices = card_data.get("prices", {})
        if isinstance(prices, dict):
            raw_price = prices.get("market") or prices.get("low") or prices.get("mid")
    if raw_price is not None:
        result["raw_price"] = float(raw_price)

    # --- PSA 10 price from eBay graded sales ---
    ebay = card_data.get("ebay", {})
    if isinstance(ebay, dict):
        sales_by_grade = ebay.get("salesByGrade", {})
        if isinstance(sales_by_grade, dict):
            psa10 = sales_by_grade.get("psa10", {})
            if isinstance(psa10, dict):
                psa10_price = (
                    psa10.get("marketPrice7Day")
                    or psa10.get("medianPrice")
                    or psa10.get("averagePrice")
                    or psa10.get("lastSoldPrice")
                )
                if psa10_price is not None:
                    result["psa10_price"] = float(psa10_price)
                    result["psa10_sales_count"] = psa10.get("salesCount", 0)
                    result["psa10_last_sold_date"] = psa10.get("lastSoldDate")

    # --- Price history (nested under conditions) ---
    price_history = card_data.get("priceHistory", {})
    if isinstance(price_history, dict):
        conditions = price_history.get("conditions", {})
        if isinstance(conditions, dict):
            # Prefer "Near Mint", fall back to first available condition
            nm_data = (
                conditions.get("Near Mint")
                or conditions.get("Holofoil")
                or conditions.get("Normal")
                or (next(iter(conditions.values())) if conditions else None)
            )
            if isinstance(nm_data, dict):
                history_list = nm_data.get("history", [])
                if isinstance(history_list, list) and history_list:
                    result["history"] = []
                    for point in history_list:
                        if not isinstance(point, dict):
                            continue
                        d = point.get("date")
                        p = point.get("market") or point.get("price")
                        if d and p is not None:
                            entry = {"date": d, "price": float(p)}
                            vol = point.get("volume")
                            if vol is not None:
                                entry["volume"] = int(vol)
                            result["history"].append(entry)

        # Fallback: if conditions is empty but priceHistory is a flat list
        if "history" not in result and isinstance(price_history, list):
            result["history"] = []
            for point in price_history:
                if isinstance(point, dict):
                    d = point.get("date")
                    p = point.get("market") or point.get("price")
                    if d and p is not None:
                        result["history"].append({"date": d, "price": float(p)})

    # Fallback: priceHistory is directly a list (older format)
    if "history" not in result and isinstance(price_history, list):
        result["history"] = []
        for point in price_history:
            if isinstance(point, dict):
                d = point.get("date")
                p = point.get("market") or point.get("price")
                if d and p is not None:
                    result["history"].append({"date": d, "price": float(p)})

    return result


def fetch_and_store_card_prices(card_id: int, card_name: str,
                                 set_name: str) -> Optional[dict]:
    """Fetch and store both raw NM and PSA 10 prices for a card (current only).

    This is the main function for daily_job.py.
    Uses includeEbay=true to get PSA 10 graded sales data.
    """
    data = search_card(card_name, set_name, include_ebay=True)
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

    Uses: GET /api/v2/cards?search=name&set=<set name>&includeBoth=true&days=180

    Stores raw NM prices from priceHistory.conditions into raw_prices table,
    and PSA 10 prices from ebay.salesByGrade into psa10_sales table.
    """
    data = search_card(card_name, set_name, include_history=True,
                       include_ebay=True, days=days)
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
