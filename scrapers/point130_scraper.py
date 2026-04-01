"""Scraper for eBay sold/completed listings — PSA 10 and raw card prices."""

import asyncio
import logging
import random
import re
import time
from datetime import date, datetime, timedelta

from db import get_connection

logger = logging.getLogger(__name__)

MAX_REQUESTS_PER_SESSION = 100
_request_count = 0

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)


def _should_skip_listing(title: str) -> bool:
    """Skip lot sales, damaged cards, and non-PSA graded listings."""
    title_lower = title.lower()
    if any(word in title_lower for word in ["lot", "bundle", "collection of", "repack"]):
        return True
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
    # If it mentions PSA/BGS/CGC with a grade, it's not raw
    if re.search(r"(psa|bgs|cgc)\s*\d", title_lower):
        return False
    return True


def _filter_outliers_iqr(prices: list) -> list:
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


def _build_ebay_url(search_query: str) -> str:
    """Build eBay sold listings search URL."""
    # LH_Sold=1 & LH_Complete=1 filters to sold/completed items
    query = search_query.replace(" ", "+")
    return (
        f"https://www.ebay.com/sch/i.html?_nkw={query}"
        f"&LH_Sold=1&LH_Complete=1&_sop=13&rt=nc"
    )


async def _scrape_ebay(search_query, grade, cutoff_date, card_name, set_name, card_number):
    """Use Playwright to scrape eBay sold listings."""
    from playwright.async_api import async_playwright

    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox"],
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        page = await context.new_page()

        url = _build_ebay_url(search_query)
        logger.info("Scraping eBay sold listings: %s", url)

        try:
            await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            # Wait longer for JS to render all items
            await page.wait_for_timeout(5000)

            # Try multiple selectors - eBay changes class names
            items = await page.query_selector_all("ul.srp-results > li")
            if not items:
                items = await page.query_selector_all(".s-item")
            if not items:
                items = await page.query_selector_all("[class*='s-item']")
            if not items:
                # Last resort: get all list items inside results container
                container = await page.query_selector("#srp-river-results")
                if container:
                    items = await container.query_selector_all("li")

            if not items:
                logger.info("No eBay results for %s", card_name)
                await browser.close()
                return results

            logger.info("Found %d eBay listing elements for %s", len(items), card_name)

            for item in items:
                # Get all text from the item
                item_text = (await item.inner_text()).strip()
                if not item_text or len(item_text) < 20:
                    continue

                # Skip non-listing items
                item_lower = item_text.lower()
                if "shop on ebay" in item_lower or "results for" in item_lower:
                    continue

                # Try to get title from specific selector first
                title = ""
                for sel in [".s-item__title", "[class*='item__title']", "h3", "a span[role='heading']"]:
                    title_el = await item.query_selector(sel)
                    if title_el:
                        title = (await title_el.inner_text()).strip()
                        break
                if not title:
                    # Extract title from first line of text
                    lines = [l.strip() for l in item_text.split("\n") if l.strip()]
                    # Skip date line like "Sold Mar 30, 2026"
                    for line in lines:
                        if not line.startswith("Sold") and len(line) > 15:
                            title = line
                            break
                if not title:
                    continue
                if _should_skip_listing(title):
                    continue
                if grade == "PSA 10" and not _is_psa10_listing(title):
                    continue
                if grade == "RAW" and not _is_raw_listing(title):
                    continue

                # Get price from selector or text
                price_text = ""
                for sel in [".s-item__price", "[class*='item__price']", ".s-item__detail--price"]:
                    price_el = await item.query_selector(sel)
                    if price_el:
                        price_text = (await price_el.inner_text()).strip()
                        break
                if not price_text:
                    # Find price in full text
                    price_match_text = re.search(r"\$([\d,]+\.?\d*)", item_text)
                    if price_match_text:
                        price_text = price_match_text.group(0)

                price_match = re.search(r"\$?([\d,]+\.?\d*)", price_text)
                if not price_match:
                    continue
                sale_price = float(price_match.group(1).replace(",", ""))

                # Skip unreasonable prices
                if sale_price < 1.0 or sale_price > 500000:
                    continue

                # Get date from text — look for "Sold Mon DD, YYYY" pattern
                sale_date = None
                sold_match = re.search(
                    r"Sold\s+(\w{3}\s+\d{1,2},?\s+\d{4})", item_text
                )
                if sold_match:
                    date_str = sold_match.group(1).replace(",", "")
                    for fmt in ("%b %d %Y", "%b %d, %Y"):
                        try:
                            sale_date = datetime.strptime(date_str.strip(), fmt).date()
                            break
                        except ValueError:
                            continue

                if sale_date is None:
                    # Try selector-based date
                    for sel in [".s-item__title--tagblock .POSITIVE", "[class*='ended-date']", ".s-item__endedDate"]:
                        date_el = await item.query_selector(sel)
                        if date_el:
                            dt = (await date_el.inner_text()).strip()
                            dt = re.sub(r"^Sold\s+", "", dt)
                            for fmt in ("%b %d, %Y", "%b %d %Y", "%m/%d/%Y"):
                                try:
                                    sale_date = datetime.strptime(dt.strip(), fmt).date()
                                    break
                                except ValueError:
                                    continue
                            if sale_date:
                                break

                # Default to today if no date found
                if sale_date is None:
                    sale_date = date.today()

                if sale_date < cutoff_date:
                    continue

                results.append({
                    "card_name": card_name,
                    "set_name": set_name,
                    "card_number": card_number,
                    "grade": grade,
                    "sale_price": sale_price,
                    "sale_date": sale_date,
                    "listing_title": title,
                    "source": "ebay",
                })

        except Exception as e:
            logger.error("Error scraping eBay for %s: %s", card_name, e)
        finally:
            await browser.close()

    return results


def scrape_card_sales(card_name, set_name, card_number,
                      grade="PSA 10", days_back=7):
    """Scrape eBay sold listings for a card."""
    global _request_count

    if _request_count >= MAX_REQUESTS_PER_SESSION:
        logger.warning("Max requests per session reached (%d). Stopping.", MAX_REQUESTS_PER_SESSION)
        return []

    search_query = f"{card_name} {set_name} {card_number}"
    if grade == "PSA 10":
        search_query += " PSA 10"

    cutoff_date = date.today() - timedelta(days=days_back)

    results = asyncio.run(
        _scrape_ebay(search_query, grade, cutoff_date, card_name, set_name, card_number)
    )

    _request_count += 1
    time.sleep(random.uniform(2, 5))

    return results


def store_psa10_sales(card_id, sales):
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


def get_cached_sales(card_id, since_date):
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
