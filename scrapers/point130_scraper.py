"""Scraper for 130point.com — reveals true eBay sold prices including Best Offer."""

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
    if any(word in title_lower for word in ["lot", "bundle", "collection of"]):
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
    return any(term in title_lower for term in ["nm", "near mint", "raw"])


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


async def _wait_for_cloudflare(page, timeout=30000):
    """Wait for Cloudflare challenge to resolve."""
    start = asyncio.get_event_loop().time()
    while (asyncio.get_event_loop().time() - start) * 1000 < timeout:
        content = await page.content()
        # Check if Cloudflare challenge is gone
        if "Performing security verification" not in content and "challenge-platform" not in content:
            return True
        await page.wait_for_timeout(1000)
    return False


async def _scrape_with_playwright(search_query, grade, cutoff_date, card_name, set_name, card_number):
    """Use Playwright with stealth to render 130point.com and extract sales data."""
    from playwright.async_api import async_playwright
    from playwright_stealth import stealth_async

    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        page = await context.new_page()
        await stealth_async(page)

        url = f"https://130point.com/sales/?query={search_query.replace(' ', '+')}"
        logger.info("Scraping 130point (stealth): %s", url)

        try:
            await page.goto(url, timeout=30000, wait_until="domcontentloaded")

            # Wait for Cloudflare to pass
            cf_passed = await _wait_for_cloudflare(page, timeout=15000)
            if not cf_passed:
                logger.warning("Cloudflare challenge did not resolve for %s", card_name)
                await browser.close()
                return results

            # Wait for content to load after Cloudflare
            await page.wait_for_timeout(5000)

            # Try to find sales data - check multiple possible selectors
            # First dump what we see for debugging
            body_text = await page.inner_text("body")
            if "No results" in body_text or len(body_text.strip()) < 100:
                logger.info("No results found on 130point for %s", card_name)
                await browser.close()
                return results

            # Look for table rows
            rows = await page.query_selector_all("table tbody tr")
            if not rows:
                rows = await page.query_selector_all("table tr")
            if not rows:
                # Try div-based layout
                rows = await page.query_selector_all(".sale-row, .sold-item, .result-row, [class*='sale'], [class*='sold']")

            if not rows:
                # Log page content for debugging
                logger.warning(
                    "No parseable rows found for %s. Page text: %.500s",
                    card_name, body_text,
                )
                await browser.close()
                return results

            logger.info("Found %d rows for %s", len(rows), card_name)

            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) < 3:
                    # Try getting all text and parsing it
                    text = await row.inner_text()
                    continue

                title = (await cells[0].inner_text()).strip()
                price_text = (await cells[1].inner_text()).strip()
                date_text = (await cells[2].inner_text()).strip()

                if not title or not price_text:
                    continue
                if _should_skip_listing(title):
                    continue
                if grade == "PSA 10" and not _is_psa10_listing(title):
                    continue
                if grade == "RAW" and not _is_raw_listing(title):
                    continue

                price_match = re.search(r"\$?([\d,]+\.?\d*)", price_text)
                if not price_match:
                    continue
                sale_price = float(price_match.group(1).replace(",", ""))

                sale_date = None
                for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%b %d, %Y"):
                    try:
                        sale_date = datetime.strptime(date_text.strip(), fmt).date()
                        break
                    except ValueError:
                        continue

                if sale_date is None or sale_date < cutoff_date:
                    continue

                results.append({
                    "card_name": card_name,
                    "set_name": set_name,
                    "card_number": card_number,
                    "grade": grade,
                    "sale_price": sale_price,
                    "sale_date": sale_date,
                    "listing_title": title,
                    "source": "130point",
                })

        except Exception as e:
            logger.error("Error scraping 130point for %s: %s", card_name, e)
        finally:
            await browser.close()

    return results


def scrape_card_sales(card_name, set_name, card_number,
                      grade="PSA 10", days_back=7):
    """Scrape 130point.com for recent sold listings using Playwright stealth."""
    global _request_count

    if _request_count >= MAX_REQUESTS_PER_SESSION:
        logger.warning("Max requests per session reached (%d). Stopping.", MAX_REQUESTS_PER_SESSION)
        return []

    search_query = f"{card_name} {set_name} {card_number}"
    if grade == "PSA 10":
        search_query += " PSA 10"

    cutoff_date = date.today() - timedelta(days=days_back)

    results = asyncio.run(
        _scrape_with_playwright(search_query, grade, cutoff_date, card_name, set_name, card_number)
    )

    _request_count += 1
    time.sleep(random.uniform(3, 6))

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
