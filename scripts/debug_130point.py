"""Debug script to inspect 130point.com page structure with stealth."""

import asyncio
from playwright.async_api import async_playwright
from playwright_stealth import stealth

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)


async def check():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        page = await context.new_page()
        await stealth(page)

        url = "https://130point.com/sales/?query=Charizard+Base+Set+PSA+10"
        print(f"Navigating to: {url}")
        await page.goto(url, timeout=30000, wait_until="domcontentloaded")

        # Wait for Cloudflare
        print("Waiting for Cloudflare challenge...")
        for i in range(20):
            content = await page.content()
            if "Performing security verification" not in content and "challenge-platform" not in content:
                print(f"Cloudflare passed after {i+1}s")
                break
            await page.wait_for_timeout(1000)
        else:
            print("Cloudflare challenge did NOT resolve after 20s")

        # Wait for page content
        await page.wait_for_timeout(5000)

        # Dump body text
        text = await page.inner_text("body")
        print(f"\n--- PAGE TEXT (first 3000 chars) ---\n{text[:3000]}")

        # Check tables
        print("\n--- TABLES ---")
        tables = await page.query_selector_all("table")
        print(f"Found {len(tables)} tables")
        for i, t in enumerate(tables):
            rows = await t.query_selector_all("tr")
            print(f"\nTable {i}: {len(rows)} rows")
            for r in rows[:5]:
                print("  ROW:", (await r.inner_text())[:200])

        # Check for div-based results
        print("\n--- DIV SEARCH ---")
        for selector in [".sale-row", ".sold-item", ".result-row", "[class*='sale']", "[class*='sold']", "[class*='item']", "[class*='result']"]:
            els = await page.query_selector_all(selector)
            if els:
                print(f"{selector}: {len(els)} elements")
                for el in els[:3]:
                    print("  ", (await el.inner_text())[:200])

        # Save screenshot for visual inspection
        await page.screenshot(path="debug_130point.png", full_page=True)
        print("\nScreenshot saved to debug_130point.png")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(check())
