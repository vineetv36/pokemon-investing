"""Client for PokemonPriceTracker API (paid tier).

Supports:
- Single card lookup with PSA graded prices
- Bulk fetch (up to 100 cards per request)
- 6-month historical price data (daily granularity)
- PSA 8/9/10 pricing from eBay completed listings

Paid tier: ~20,000 credits/day, 60 req/min, 6 months history.
"""

import logging
import os
import ssl
import time
from datetime import date, timedelta
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
    # Remove requests older than 60 seconds
    _request_times = [t for t in _request_times if now - t < 60]
    if len(_request_times) >= REQUESTS_PER_MINUTE:
        wait = 60 - (now - _request_times[0]) + 0.1
        logger.info("Rate limit: waiting %.1fs...", wait)
        time.sleep(wait)
    _request_times.append(time.time())


def _make_request(method: str, endpoint: str, params: Optional[dict] = None,
                  json_body: Optional[dict] = None, credits: int = 1) -> Optional[dict]:
    """Make an authenticated request with rate limiting and retries."""
    global _credits_used

    if _credits_used + credits > DAILY_CREDIT_LIMIT:
        logger.warning("Daily credit limit approaching (%d/%d). Stopping.",
                       _credits_used, DAILY_CREDIT_LIMIT)
        return None

    _rate_limit()

    url = f"{BASE_URL}{endpoint}" if endpoint.startswith("/") else f"https://www.pokemonpricetracker.com{endpoint}"

    try:
        if method == "GET":
            response = httpx.get(url, headers=_get_headers(), params=params,
                                 timeout=30, verify=_ssl_ctx)
        else:
            response = httpx.post(url, headers=_get_headers(), json=json_body,
                                  timeout=30, verify=_ssl_ctx)

        if response.status_code == 429:
            for backoff in [60, 120, 240]:
                logger.warning("Rate limited (429). Backing off %ds...", backoff)
                time.sleep(backoff)
                if method == "GET":
                    response = httpx.get(url, headers=_get_headers(), params=params,
                                         timeout=30, verify=_ssl_ctx)
                else:
                    response = httpx.post(url, headers=_get_headers(), json=json_body,
                                          timeout=30, verify=_ssl_ctx)
                if response.status_code != 429:
                    break
            if response.status_code == 429:
                logger.error("Still rate limited after backoff. Giving up.")
                return None

        if response.status_code == 401:
            logger.error("API key invalid (401). Check POKEMON_PRICE_TRACKER_API_KEY.")
            return None

        response.raise_for_status()
        _credits_used += credits
        return response.json()

    except httpx.HTTPStatusError as e:
        logger.error("HTTP error: %d %s", e.response.status_code, e.response.text[:200])
        return None
    except httpx.RequestError as e:
        logger.error("Request error: %s", e)
        return None


# --- Card Lookup ---

def search_card(name: str, set_id: Optional[str] = None) -> Optional[dict]:
    """Search for a card by name and optionally set ID."""
    params = {"search": name}
    if set_id:
        params["setId"] = set_id
    return _make_request("GET", "/cards", params)


def get_card(tcgplayer_id: str, include_ebay: bool = True) -> Optional[dict]:
    """Fetch card data by TCGPlayer ID with optional eBay price data."""
    params = {"tcgPlayerId": tcgplayer_id}
    if include_ebay:
        params["includeEbay"] = "true"
    return _make_request("GET", "/cards", params)


def get_sets() -> Optional[dict]:
    """Fetch all available sets."""
    return _make_request("GET", "/sets")


def get_all_cards_in_set(set_id: str) -> Optional[dict]:
    """Fetch all cards in a set using bulk fetch (1 credit per card)."""
    params = {"setId": set_id, "fetchAllInSet": "true"}
    return _make_request("GET", "/cards", params, credits=0)  # credits counted per card returned


# --- Bulk Operations ---

def bulk_fetch_cards(card_ids: list) -> Optional[list]:
    """Fetch up to 100 cards in a single request.

    Args:
        card_ids: List of card IDs (up to 100)

    Returns:
        List of card data dicts, or None on error.
    """
    if len(card_ids) > 100:
        logger.warning("Bulk fetch limited to 100 cards. Truncating.")
        card_ids = card_ids[:100]

    # Credits = 1 per card, minute calls = ceil(cards/10) capped at 30
    import math
    credits = len(card_ids)
    minute_calls = min(math.ceil(len(card_ids) / 10), 30)

    data = _make_request("POST", "/api/cards/bulk", json_body={"ids": card_ids},
                         credits=credits)
    return data


# --- PSA Graded Prices ---

def get_psa_pricing(card_id: str) -> Optional[dict]:
    """Fetch PSA graded pricing for a card (all grades PSA 1-10).

    Returns dict with psa_1 through psa_10, each containing
    market_price and last_sold.
    """
    return _make_request("GET", f"/api/psa/pricing/{card_id}")


# --- Historical Data ---

def get_price_history(card_id: str, period: str = "6m") -> Optional[dict]:
    """Fetch historical price data for a card.

    Args:
        card_id: Card identifier
        period: Time period - "3d" (free), "6m" (paid), "1y" (business)

    Returns:
        Dict with data_points (date, price, volume) and statistics.
    """
    params = {"period": period}
    return _make_request("GET", f"/cards/{card_id}/history", params)


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


def fetch_and_store_card_prices(card_id: int, card_name: str, set_name: str) -> Optional[dict]:
    """Fetch and store both raw NM and PSA 10 prices for a card.

    This is the main function to call from the daily job.
    Returns dict with raw_price and psa10_price, or None.
    """
    data = search_card(card_name, set_name)
    if not data:
        return None

    # Parse response - handle various response formats
    cards = data if isinstance(data, list) else data.get("data", data.get("cards", []))
    if not cards:
        logger.warning("No results for %s (%s)", card_name, set_name)
        return None

    card_data = cards[0] if isinstance(cards, list) else cards
    result = {}

    # Raw NM price
    raw_price = (
        card_data.get("market_price")
        or card_data.get("tcgplayer_price")
        or card_data.get("price")
    )
    if raw_price is not None:
        raw_price = float(raw_price)
        store_raw_price(card_id, raw_price)
        result["raw_price"] = raw_price
        logger.info("Card %s: raw NM = $%.2f", card_name, raw_price)

    # PSA 10 price from eBay data
    ebay_data = card_data.get("ebay", {})
    psa10_data = ebay_data.get("psa10", {})
    psa10_price = psa10_data.get("avg") or psa10_data.get("market_price") or psa10_data.get("last_sold")

    # Also check gradedPrices field
    if not psa10_price:
        graded = card_data.get("gradedPrices", {})
        psa10_price = graded.get("psa10")

    if psa10_price is not None:
        psa10_price = float(psa10_price)
        store_psa10_price(card_id, psa10_price)
        result["psa10_price"] = psa10_price
        logger.info("Card %s: PSA 10 = $%.2f", card_name, psa10_price)

    return result if result else None


def fetch_and_store_history(card_id: int, api_card_id: str, period: str = "6m"):
    """Fetch 6-month historical prices and store daily entries.

    This backfills raw_prices with historical data points.
    """
    data = get_price_history(api_card_id, period)
    if not data:
        return 0

    data_points = data.get("data_points", data.get("data", []))
    if not data_points:
        logger.warning("No history data for card %s", api_card_id)
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    stored = 0

    for point in data_points:
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

    conn.commit()
    conn.close()
    logger.info("Stored %d historical price points for card %d", stored, card_id)
    return stored


def get_credits_remaining() -> int:
    """Return estimated credits remaining for today."""
    return max(0, DAILY_CREDIT_LIMIT - _credits_used)
