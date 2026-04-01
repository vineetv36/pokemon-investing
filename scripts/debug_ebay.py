"""Debug script to inspect eBay sold listings page structure."""

import asyncio
from playwright.async_api import async_playwright

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)


async def check():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        page = await context.new_page()

        url = "https://www.ebay.com/sch/i.html?_nkw=Charizard+Base+Set+4%2F102+PSA+10&LH_Sold=1&LH_Complete=1&_sop=13&rt=nc"
        print(f"Navigating to: {url}")
        await page.goto(url, timeout=30000, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)

        # Check page title
        title = await page.title()
        print(f"Page title: {title}")

        # Body text preview
        text = await page.inner_text("body")
        print(f"\n--- PAGE TEXT (first 2000 chars) ---\n{text[:2000]}")

        # Check common eBay selectors
        print("\n--- SELECTOR SEARCH ---")
        for selector in [".s-item", ".srp-results .s-item", "li.s-item", "[class*='s-item']", ".srp-results", "#srp-river-results", "ul.srp-results"]:
            els = await page.query_selector_all(selector)
            print(f"  {selector}: {len(els)} elements")

        # Check all list items
        lis = await page.query_selector_all("li")
        print(f"\n  All <li>: {len(lis)} elements")
        for li in lis[:5]:
            cls = await li.get_attribute("class") or ""
            txt = (await li.inner_text())[:100]
            print(f"    class='{cls}' text='{txt}'")

        # Save screenshot
        await page.screenshot(path="debug_ebay.png", full_page=True)
        print("\nScreenshot saved to debug_ebay.png")

        # Save full HTML for inspection
        html = await page.content()
        with open("debug_ebay.html", "w") as f:
            f.write(html)
        print("Full HTML saved to debug_ebay.html")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(check())
