"""
Logs into insights.gryps.io and intercepts all API calls across every page.
Run once to map out all available endpoints before building the full scraper.
"""

import asyncio
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

BASE_URL = "https://insights.gryps.io"
EMAIL = os.getenv("GRYPS_EMAIL")
PASSWORD = os.getenv("GRYPS_PASSWORD")

PAGES_TO_VISIT = [
    "/#/user-engagement",
    "/#/searches",
    "/#/search-engagement",
    "/#/agent-runs",
    "/#/users",
]

captured = []


def on_request(request):
    url = request.url
    if BASE_URL in url and any(
        skip not in url for skip in [".js", ".css", ".png", ".ico", ".woff"]
    ):
        if request.resource_type in ("fetch", "xhr"):
            captured.append({
                "type": "request",
                "method": request.method,
                "url": url,
                "headers": dict(request.headers),
            })


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        # Capture all fetch/xhr requests
        page.on("request", lambda req: (
            captured.append({
                "method": req.method,
                "url": req.url,
                "resource_type": req.resource_type,
            }) if req.resource_type in ("fetch", "xhr") and BASE_URL in req.url else None
        ))

        responses = []

        async def capture_response(response):
            try:
                rtype = response.request.resource_type
                print(f"  [{rtype}] {response.status} {response.url[:100]}")
                if rtype in ("fetch", "xhr"):
                    try:
                        body = await response.json()
                    except Exception:
                        body = None
                    responses.append({
                        "url": response.url,
                        "status": response.status,
                        "body_preview": json.dumps(body)[:500] if body else None,
                    })
            except Exception as e:
                print(f"  Error capturing: {e}")

        page.on("response", capture_response)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Navigating to login...")
        await page.goto(BASE_URL)
        await page.wait_for_load_state("networkidle")

        # Fill login form - adjust selectors if needed
        await page.fill('input[type="email"], input[name="email"], input[placeholder*="email" i]', EMAIL)
        await page.fill('input[type="password"]', PASSWORD)
        await page.click('button[type="submit"], button:has-text("Sign in"), button:has-text("Login")')
        await page.wait_for_load_state("networkidle")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Logged in. Current URL: {page.url}")

        # Visit each section
        for path in PAGES_TO_VISIT:
            url = f"{BASE_URL}{path}"
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Visiting {url}")
            try:
                await page.goto(url)
                await page.wait_for_load_state("domcontentloaded")
                await asyncio.sleep(4)  # let lazy-loaded API calls fire
            except Exception as e:
                print(f"  Error: {e}")

        await browser.close()

        # Deduplicate and print results
        seen = set()
        unique_responses = []
        for r in responses:
            key = r["url"].split("?")[0]
            if key not in seen:
                seen.add(key)
                unique_responses.append(r)

        print("\n" + "=" * 60)
        print("DISCOVERED API ENDPOINTS")
        print("=" * 60)
        for r in unique_responses:
            print(f"\n[{r['status']}] {r['url']}")
            if r["body_preview"]:
                print(f"  Response preview: {r['body_preview'][:200]}")

        # Save full results
        output_file = "api_discovery.json"
        with open(output_file, "w") as f:
            json.dump(unique_responses, f, indent=2)
        print(f"\nFull results saved to {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
