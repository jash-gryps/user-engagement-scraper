"""
Targeted discovery: visits the Searches page and clicks each tab
to capture the raw individual search query endpoints + auth token.
"""

import asyncio
import json
import os
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

BASE_URL = "https://insights.gryps.io"
API_BASE = "https://kcdr22q3bd.execute-api.us-east-1.amazonaws.com"
EMAIL = os.getenv("GRYPS_EMAIL")
PASSWORD = os.getenv("GRYPS_PASSWORD")


async def main():
    responses = []
    auth_token = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        async def capture(response):
            nonlocal auth_token
            try:
                # Capture auth token from request headers
                if API_BASE in response.url:
                    headers = response.request.headers
                    if "authorization" in headers and not auth_token:
                        auth_token = headers["authorization"]
                        print(f"\n  AUTH TOKEN captured: {auth_token[:60]}...")

                if response.request.resource_type == "fetch":
                    try:
                        body = await response.json()
                    except Exception:
                        body = None
                    if API_BASE in response.url:
                        responses.append({
                            "url": response.url,
                            "status": response.status,
                            "body_preview": json.dumps(body)[:300] if body else None,
                        })
                        print(f"  API: {response.url.replace(API_BASE, '')}")
            except Exception:
                pass

        page.on("response", capture)

        # Login
        await page.goto(BASE_URL)
        await page.wait_for_load_state("domcontentloaded")
        await page.fill('input[type="email"], input[name="email"]', EMAIL)
        await page.fill('input[type="password"]', PASSWORD)
        await page.click('button:has-text("Sign In")')
        await page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(2)

        # Go to searches page
        print("\nNavigating to Searches page...")
        await page.goto(f"{BASE_URL}/#/searches")
        await page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(3)

        # Click each tab
        for tab_label in ["Q&A", "Keyword", "Top Search Users", "Charts"]:
            try:
                tab = page.get_by_role("tab", name=tab_label)
                if await tab.count() > 0:
                    print(f"\nClicking tab: {tab_label}")
                    await tab.click()
                    await asyncio.sleep(3)
                else:
                    # Try text match
                    tab = page.locator(f"text={tab_label}").first
                    print(f"\nClicking (text): {tab_label}")
                    await tab.click()
                    await asyncio.sleep(3)
            except Exception as e:
                print(f"  Could not click {tab_label}: {e}")

        # Also check search engagement page
        print("\nNavigating to Search Engagement page...")
        await page.goto(f"{BASE_URL}/#/search-engagement")
        await page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(4)

        await browser.close()

    print("\n" + "=" * 60)
    print("ALL API ENDPOINTS FOUND")
    print("=" * 60)
    seen = set()
    for r in responses:
        key = r["url"].split("?")[0]
        if key not in seen:
            seen.add(key)
            print(f"\n[{r['status']}] {r['url']}")
            if r["body_preview"]:
                print(f"  Preview: {r['body_preview'][:200]}")

    print("\n" + "=" * 60)
    print(f"AUTH TOKEN: {auth_token}")
    print("=" * 60)

    with open("searches_discovery.json", "w") as f:
        json.dump({"auth_token": auth_token, "endpoints": responses}, f, indent=2)
    print("\nSaved to searches_discovery.json")


if __name__ == "__main__":
    asyncio.run(main())
