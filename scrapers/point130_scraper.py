"""Scraper for 130point.com — reveals true eBay sold prices including Best Offer."""

import logging
import random
import re
import time
from datetime import date, datetime, timedelta

from db import get_connection

logger = logging.getLogger(__name__)

MAX_REQUESTS_PER_SESSION = 100
_request_count = 0


def _should_skip_listing(title: str) -> bool:
    """Skip lot sales, damaged cards, and non-PSA graded listings."""
    title_lower = title.lower()
    # Skip lot/bundle sales
    if any(word in title_lower for word in ["lot", "bundle", "collection of"]):
        return True
    # Skip BGS/CGC listings that appear in PSA searches
    if any(word in title_lower for word in ["bgs", "cgc"]) and "psa" not in title_lower:
        return True
    return False


def _is_psa10_listing(title: str) -> bool:
    """Check if listing is a PSA 10 graded card."""
    title_lower = title.lower()
    return "psa 10" in title_lower or "psa10" in title_lower


def _is_raw_listing(title: str) -> bool:
    """Check if listing is a raw NM card."""
    title_lower = title.lower()
    return any(term in title_lower for term in ["nm", "near mint", "raw"])


def _filter_outliers_iqr(prices: list[float]) -> list[float]:
    """Remove statistical outliers using IQR method."""
    if len(prices) < 4:
        return prices
    sorted_prices = sorted(prices)
    q1 = sorted_prices[len(sorted_prices) // 4]
    q3 = sorted_prices[3 * len(sorted_prices) // 4]
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return [p for p in prices if lower <= p <= upper]


async def scrape_card_sales(card_name: str, set_name: str, card_number: str,
                            grade: str = "PSA 10", days_back: int = 7) -> list[dict]:
    """
    Scrape 130point.com for recent sold listings of a card.

    Args:
        card_name: Card name (e.g. "Charizard")
        set_name: Set name (e.g. "Base Set")
        card_number: Card number (e.g. "4/102")
        grade: "PSA 10" for graded or "RAW" for ungraded
        days_back: How many days back to search

    Returns:
        List of sale dicts with price, date, title, etc.
    """
    global _request_count

    if _request_count >= MAX_REQUESTS_PER_SESSION:
        logger.warning("Max requests per session reached (%d). Stopping.", MAX_REQUESTS_PER_SESSION)
        return []

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("playwright not installed. Run: pip install playwright && playwright install chromium")
        return []

    search_query = f"{card_name} {set_name} {card_number}"
    if grade == "PSA 10":
        search_query += " PSA 10"

    results = []
    cutoff_date = date.today() - timedelta(days=days_back)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(
            user_agent="psa10-dashboard/1.0 (personal research project)"
        )

        url = f"https://130point.com/sales/?query={search_query.replace(' ', '+')}"
        logger.info("Scraping 130point: %s", url)

        try:
            await page.goto(url, timeout=30000)
            await page.wait_for_selector("table", timeout=15000)

            rows = await page.query_selector_all("table tbody tr")

            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) < 4:
                    continue

                title = await cells[0].inner_text()
                price_text = await cells[1].inner_text()
                date_text = await cells[2].inner_text()

                if _should_skip_listing(title):
                    continue

                # Determine if this matches our requested grade
                if grade == "PSA 10" and not _is_psa10_listing(title):
                    continue
                if grade == "RAW" and not _is_raw_listing(title):
                    continue

                # Parse price
                price_match = re.search(r"\$?([\d,]+\.?\d*)", price_text)
                if not price_match:
                    continue
                sale_price = float(price_match.group(1).replace(",", ""))

                # Parse date
                try:
                    sale_date = datetime.strptime(date_text.strip(), "%m/%d/%Y").date()
                except ValueError:
                    try:
                        sale_date = datetime.strptime(date_text.strip(), "%Y-%m-%d").date()
                    except ValueError:
                        continue

                if sale_date < cutoff_date:
                    continue

                results.append({
                    "card_name": card_name,
                    "set_name": set_name,
                    "card_number": card_number,
                    "grade": grade,
                    "sale_price": sale_price,
                    "sale_date": sale_date,
                    "listing_title": title.strip(),
                    "source": "130point",
                })

        except Exception as e:
            logger.error("Error scraping 130point for %s: %s", card_name, e)
        finally:
            await browser.close()
            _request_count += 1

    # Delay between requests
    time.sleep(random.uniform(2, 5))

    return results


def store_psa10_sales(card_id: int, sales: list[dict]):
    """Store PSA 10 sales in the database."""
    conn = get_connection()
    cursor = conn.cursor()
    inserted = 0
    for sale in sales:
        try:
            cursor.execute(
                """INSERT OR IGNORE INTO psa10_sales (card_id, sale_price, sale_date, listing_title, source)
                   VALUES (?, ?, ?, ?, ?)""",
                (card_id, sale["sale_price"], sale["sale_date"].isoformat(),
                 sale["listing_title"], sale["source"]),
            )
            inserted += cursor.rowcount
        except Exception as e:
            logger.error("Error storing sale for card %d: %s", card_id, e)
    conn.commit()
    conn.close()
    logger.info("Stored %d new PSA 10 sales for card %d", inserted, card_id)
    return inserted


def get_cached_sales(card_id: int, since_date: date) -> list[dict]:
    """Check if we already have recent sales cached."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT COUNT(*) FROM psa10_sales WHERE card_id = ? AND sale_date >= ?",
        (card_id, since_date.isoformat()),
    )
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0
