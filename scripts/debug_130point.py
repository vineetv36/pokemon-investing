"""Debug script to inspect 130point.com page structure."""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from playwright.async_api import async_playwright


async def check():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(
            "https://130point.com/sales/?query=Charizard+Base+Set+PSA+10",
            timeout=30000,
        )
        await page.wait_for_timeout(8000)

        # Dump page text
        text = await page.inner_text("body")
        print(text[:3000])

        print("\n---TABLES---")
        tables = await page.query_selector_all("table")
        print(f"Found {len(tables)} tables")
        for i, t in enumerate(tables):
            rows = await t.query_selector_all("tr")
            print(f"\nTable {i}: {len(rows)} rows")
            for r in rows[:5]:
                print("ROW:", (await r.inner_text())[:200])

        await browser.close()


if __name__ == "__main__":
    asyncio.run(check())
