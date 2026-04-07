"""eBay Browse API client for fetching active listing prices.

NOTE: The Browse API only returns ACTIVE listings, not sold/completed items.
For sold prices, use the Playwright scraper (scrapers/point130_scraper.py).

This client fetches current asking prices and listing counts, useful for:
- Current market price estimates (median asking price)
- Supply signals (how many listings exist)
- Price spread analysis (gap between low and high asks)

Usage:
    # Set up credentials in .env:
    #   EBAY_CLIENT_ID=your_app_id
    #   EBAY_CLIENT_SECRET=your_cert_id

    from api_clients.ebay_browse import EbayBrowseClient
    client = EbayBrowseClient()
    results = client.search_cards("Charizard Base Set PSA 10", limit=50)
"""

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

EBAY_AUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_BROWSE_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"

# eBay category IDs
POKEMON_CATEGORY = "183454"  # Collectible Card Games > Pokémon TCG

# Rate limiting: 5000 calls/day = ~3.5/min sustained, but can burst
REQUEST_DELAY = 0.5  # seconds between requests
MAX_DAILY_CALLS = 5000


class EbayBrowseClient:
    def __init__(self):
        self.client_id = os.getenv("EBAY_CLIENT_ID", "")
        self.client_secret = os.getenv("EBAY_CLIENT_SECRET", "")
        self._token = None
        self._token_expires = None
        self._daily_calls = 0
        self._last_request = 0.0

        if not self.client_id or not self.client_secret:
            logger.warning("EBAY_CLIENT_ID / EBAY_CLIENT_SECRET not set in .env")

    def _get_token(self) -> str:
        """Get OAuth2 access token using client credentials flow."""
        if self._token and self._token_expires and datetime.now() < self._token_expires:
            return self._token

        logger.info("Requesting eBay OAuth token...")
        r = httpx.post(
            EBAY_AUTH_URL,
            auth=(self.client_id, self.client_secret),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "scope": "https://api.ebay.com/oauth/api_scope",
            },
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()

        self._token = data["access_token"]
        # Token typically valid for 2 hours; refresh at 90 min
        expires_in = data.get("expires_in", 7200)
        self._token_expires = datetime.now() + timedelta(seconds=expires_in - 300)

        logger.info("eBay OAuth token acquired (expires in %ds)", expires_in)
        return self._token

    def _rate_limit(self):
        """Enforce rate limiting."""
        if self._daily_calls >= MAX_DAILY_CALLS:
            raise RuntimeError(f"eBay daily API limit reached ({MAX_DAILY_CALLS} calls)")

        elapsed = time.time() - self._last_request
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)

        self._last_request = time.time()
        self._daily_calls += 1

    def search_cards(self, query: str, limit: int = 50,
                     min_price: Optional[float] = None,
                     max_price: Optional[float] = None,
                     condition: Optional[str] = None) -> list:
        """Search active eBay listings for Pokemon cards.

        Args:
            query: Search keywords (e.g. "Charizard Base Set PSA 10")
            limit: Max results (up to 200 per call)
            min_price: Minimum price filter
            max_price: Maximum price filter
            condition: "NEW", "USED", or None for all

        Returns:
            List of dicts with listing data
        """
        self._rate_limit()
        token = self._get_token()

        params = {
            "q": query,
            "category_ids": POKEMON_CATEGORY,
            "limit": min(limit, 200),
            "sort": "price",
        }

        # Build filter string
        filters = []
        if min_price is not None:
            filters.append(f"price:[{min_price}..],priceCurrency:USD")
        if max_price is not None:
            filters.append(f"price:[..{max_price}],priceCurrency:USD")
        if condition:
            cond_map = {"NEW": "1000", "USED": "3000"}
            if condition.upper() in cond_map:
                filters.append(f"conditionIds:{{{cond_map[condition.upper()]}}}")

        if filters:
            params["filter"] = ",".join(filters)

        headers = {
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
            "Content-Type": "application/json",
        }

        try:
            r = httpx.get(EBAY_BROWSE_URL, headers=headers, params=params, timeout=15)

            if r.status_code == 429:
                logger.warning("eBay rate limit hit. Waiting 60s...")
                time.sleep(60)
                return self.search_cards(query, limit, min_price, max_price, condition)

            r.raise_for_status()
            data = r.json()

        except httpx.HTTPStatusError as e:
            logger.error("eBay API error %d: %s", e.response.status_code, e.response.text[:200])
            return []
        except httpx.RequestError as e:
            logger.error("eBay request error: %s", e)
            return []

        items = data.get("itemSummaries", [])
        total = data.get("total", 0)
        logger.info("eBay search '%s': %d results (of %d total)", query, len(items), total)

        return [self._parse_item(item) for item in items]

    def search_all_pages(self, query: str, max_results: int = 500, **kwargs) -> list:
        """Search with pagination to get more results.

        Args:
            query: Search keywords
            max_results: Maximum total results to fetch
            **kwargs: Passed to search_cards
        """
        all_items = []
        offset = 0
        page_size = 200

        while offset < max_results:
            self._rate_limit()
            token = self._get_token()

            params = {
                "q": query,
                "category_ids": POKEMON_CATEGORY,
                "limit": min(page_size, max_results - offset),
                "offset": offset,
                "sort": "price",
            }

            headers = {
                "Authorization": f"Bearer {token}",
                "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
            }

            try:
                r = httpx.get(EBAY_BROWSE_URL, headers=headers, params=params, timeout=15)
                if r.status_code == 429:
                    time.sleep(60)
                    continue
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                logger.error("eBay pagination error at offset %d: %s", offset, e)
                break

            items = data.get("itemSummaries", [])
            if not items:
                break

            all_items.extend([self._parse_item(i) for i in items])
            total = data.get("total", 0)

            offset += len(items)
            if offset >= total:
                break

        logger.info("eBay paginated search '%s': %d total results", query, len(all_items))
        return all_items

    def _parse_item(self, item: dict) -> dict:
        """Parse an eBay item summary into a flat dict."""
        price_data = item.get("price", {})
        image = item.get("image", {})
        seller = item.get("seller", {})

        return {
            "item_id": item.get("itemId", ""),
            "title": item.get("title", ""),
            "price": float(price_data.get("value", 0)),
            "currency": price_data.get("currency", "USD"),
            "condition": item.get("condition", ""),
            "image_url": image.get("imageUrl", ""),
            "item_url": item.get("itemWebUrl", ""),
            "seller_username": seller.get("username", ""),
            "seller_feedback_pct": seller.get("feedbackPercentage", ""),
            "listing_type": item.get("buyingOptions", []),
            "category_id": item.get("categories", [{}])[0].get("categoryId", "") if item.get("categories") else "",
            "item_location": item.get("itemLocation", {}).get("country", ""),
        }

    def get_card_market_snapshot(self, card_name: str, set_name: str,
                                  card_number: str) -> dict:
        """Get a market snapshot for a specific card (active listings only).

        Returns median, low, high asking prices + listing count for both
        PSA 10 and raw versions.
        """
        snapshot = {"card_name": card_name, "set_name": set_name, "card_number": card_number}

        # PSA 10 listings
        psa_query = f"{card_name} {set_name} {card_number} PSA 10"
        psa_listings = self.search_cards(psa_query, limit=50)
        psa_prices = [l["price"] for l in psa_listings if l["price"] > 0]

        if psa_prices:
            psa_prices.sort()
            snapshot["psa10_listing_count"] = len(psa_prices)
            snapshot["psa10_ask_low"] = psa_prices[0]
            snapshot["psa10_ask_high"] = psa_prices[-1]
            snapshot["psa10_ask_median"] = psa_prices[len(psa_prices) // 2]
        else:
            snapshot["psa10_listing_count"] = 0
            snapshot["psa10_ask_low"] = None
            snapshot["psa10_ask_high"] = None
            snapshot["psa10_ask_median"] = None

        # Raw listings
        raw_query = f"{card_name} {set_name} {card_number} -PSA -BGS -CGC"
        raw_listings = self.search_cards(raw_query, limit=50)
        raw_prices = [l["price"] for l in raw_listings if l["price"] > 0]

        if raw_prices:
            raw_prices.sort()
            snapshot["raw_listing_count"] = len(raw_prices)
            snapshot["raw_ask_low"] = raw_prices[0]
            snapshot["raw_ask_high"] = raw_prices[-1]
            snapshot["raw_ask_median"] = raw_prices[len(raw_prices) // 2]
        else:
            snapshot["raw_listing_count"] = 0
            snapshot["raw_ask_low"] = None
            snapshot["raw_ask_high"] = None
            snapshot["raw_ask_median"] = None

        # Ratio from asking prices
        if snapshot["psa10_ask_median"] and snapshot["raw_ask_median"] and snapshot["raw_ask_median"] > 0:
            snapshot["ask_ratio"] = round(snapshot["psa10_ask_median"] / snapshot["raw_ask_median"], 2)
        else:
            snapshot["ask_ratio"] = None

        return snapshot

    @property
    def calls_remaining(self) -> int:
        return MAX_DAILY_CALLS - self._daily_calls


def fetch_watchlist_snapshots(min_price: float = 5.0):
    """Fetch market snapshots for all cards in the filtered watchlist.

    Reads from data/watchlist.parquet and stores snapshots to
    data/market_snapshots.parquet.
    """
    import pandas as pd

    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
    watchlist_file = os.path.join(data_dir, "watchlist.parquet")
    snapshots_file = os.path.join(data_dir, "market_snapshots.parquet")

    if not os.path.exists(watchlist_file):
        logger.error("No watchlist found. Run: python3 jobs/filter_catalog.py")
        return

    df = pd.read_parquet(watchlist_file)
    if min_price:
        df = df[df["price_market"].notna() & (df["price_market"] >= min_price)]

    logger.info("Fetching snapshots for %d cards (API calls needed: ~%d)", len(df), len(df) * 2)

    client = EbayBrowseClient()
    snapshots = []

    for i, (_, row) in enumerate(df.iterrows()):
        if client.calls_remaining < 10:
            logger.warning("Running low on API calls. Stopping.")
            break

        logger.info("[%d/%d] %s (%s #%s)", i + 1, len(df), row["name"], row["set_name"], row["number"])
        snap = client.get_card_market_snapshot(row["name"], row["set_name"], row["number"])
        snap["catalog_price_market"] = row.get("price_market")
        snap["rarity"] = row.get("rarity", "")
        snap["snapshot_date"] = datetime.now().isoformat()
        snapshots.append(snap)

    if snapshots:
        snap_df = pd.DataFrame(snapshots)
        # Append to existing if file exists
        if os.path.exists(snapshots_file):
            existing = pd.read_parquet(snapshots_file)
            snap_df = pd.concat([existing, snap_df], ignore_index=True)
        snap_df.to_parquet(snapshots_file, compression="zstd", index=False)
        logger.info("Saved %d snapshots to %s", len(snapshots), snapshots_file)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="eBay Browse API client")
    parser.add_argument("--search", type=str, help="Search query")
    parser.add_argument("--snapshot", type=str, help="Card name for market snapshot")
    parser.add_argument("--set", type=str, default="", help="Set name (for --snapshot)")
    parser.add_argument("--number", type=str, default="", help="Card number (for --snapshot)")
    parser.add_argument("--fetch-watchlist", action="store_true",
                        help="Fetch snapshots for entire filtered watchlist")
    parser.add_argument("--min-price", type=float, default=5.0)
    args = parser.parse_args()

    client = EbayBrowseClient()

    if args.search:
        results = client.search_cards(args.search)
        for r in results[:10]:
            print(f"  ${r['price']:>8.2f}  {r['title'][:70]}")
        print(f"\n{len(results)} results")

    elif args.snapshot:
        snap = client.get_card_market_snapshot(args.snapshot, args.set, args.number)
        for k, v in snap.items():
            print(f"  {k}: {v}")

    elif args.fetch_watchlist:
        fetch_watchlist_snapshots(min_price=args.min_price)

    else:
        print("Usage: --search 'query' | --snapshot 'card' --set 'set' | --fetch-watchlist")
