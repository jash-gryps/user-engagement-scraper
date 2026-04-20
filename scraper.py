"""
Gryps User Engagement Scraper
- Fetches raw search queries + dashboard usage for Massport, NEU, Zubatkin
- Diffs dashboard snapshots to reconstruct a per-event activity feed
- Stores all data as CSV on GitHub
- Creates GitHub Issues for user problem alerts (no email dependency)
"""

import asyncio
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = os.getenv("GRYPS_BASE_URL", "https://insights.gryps.io")
API_BASE = os.getenv("GRYPS_API_BASE", "https://kcdr22q3bd.execute-api.us-east-1.amazonaws.com/prod//ues")
TENANTS = ["massport", "northeastern-university", "zubatkin", "javits"]
TENANT_SLUG = {
    "massport": "massport",
    "northeastern-university": "neu",
    "zubatkin": "zubatkin",
    "javits": "javits",
}
DATA_DIR = Path(__file__).parent / "data"
GH_REPO = "jash-gryps/user-engagement-scraper"
GH_TOKEN = os.getenv("GH_PAT", os.getenv("GITHUB_TOKEN", ""))

SEARCH_FIELDS = ["id", "date_created", "tenant", "email", "search_type",
                 "question", "answer", "thumbs_up", "thumbs_down"]

DASHBOARD_SNAPSHOT_FIELDS = ["tenant", "user", "dashboard_name", "views", "last_viewed"]
TOKEN_CACHE = Path(__file__).parent / ".jwt_cache"
JWT_TTL_SECONDS = 3000  # Cognito tokens last 1hr; refresh at 50min to be safe
DASHBOARD_EVENT_FIELDS = ["event_time", "tenant", "user", "dashboard_name", "views_delta", "total_views"]


# ── Auth ──────────────────────────────────────────────────────────────────────

def load_cached_jwt() -> str | None:
    if not TOKEN_CACHE.exists():
        return None
    try:
        data = json.loads(TOKEN_CACHE.read_text())
        age = datetime.now(timezone.utc).timestamp() - data["cached_at"]
        if age < JWT_TTL_SECONDS:
            print(f"[auth] Using cached JWT ({int(age)}s old)")
            return data["token"]
    except Exception:
        pass
    return None


def save_jwt(token: str) -> None:
    TOKEN_CACHE.write_text(json.dumps({
        "token": token,
        "cached_at": datetime.now(timezone.utc).timestamp()
    }))


async def fetch_jwt_via_browser() -> str:
    token = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        async def capture(response):
            nonlocal token
            if not token and "execute-api" in response.url:
                auth = response.request.headers.get("authorization", "")
                if auth.startswith("Bearer "):
                    token = auth

        page.on("response", capture)
        await page.goto(BASE_URL)
        await page.wait_for_load_state("domcontentloaded")
        await page.fill('input[type="email"]', os.getenv("GRYPS_EMAIL"))
        await page.fill('input[type="password"]', os.getenv("GRYPS_PASSWORD"))
        await page.click('button:has-text("Sign In")')

        for path in ["/#/user-engagement", "/#/searches", "/#/search-engagement"]:
            if token:
                break
            await page.goto(f"{BASE_URL}{path}")
            await page.wait_for_load_state("domcontentloaded")
            await asyncio.sleep(4)

        await browser.close()

    if not token:
        raise RuntimeError("Failed to capture JWT")

    return token


async def get_jwt() -> str:
    token = load_cached_jwt()
    if token:
        return token
    print("[auth] Logging in via browser...")
    token = await fetch_jwt_via_browser()
    save_jwt(token)
    print("[auth] JWT acquired and cached")
    return token


# ── API Helpers ───────────────────────────────────────────────────────────────

def api_get(token: str, path: str, params: dict = None) -> list:
    r = requests.get(
        f"{API_BASE}{path}",
        params={"user_type": "Customer", **(params or {})},
        headers={"Authorization": token},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("data", [])


# ── Storage Helpers ───────────────────────────────────────────────────────────

def csv_path_searches(tenant: str) -> Path:
    slug = TENANT_SLUG.get(tenant, tenant)
    return DATA_DIR / slug / "searches.csv"


def csv_path_dashboard_events(tenant: str) -> Path:
    slug = TENANT_SLUG.get(tenant, tenant)
    return DATA_DIR / slug / "dashboard_events.csv"


def csv_path_dashboard_snapshot(tenant: str) -> Path:
    slug = TENANT_SLUG.get(tenant, tenant)
    return DATA_DIR / slug / "dashboard_snapshot.csv"


def load_csv(path: Path, key_field: str = None) -> list | dict:
    if not path.exists():
        return {} if key_field else []
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if key_field:
        return {r[key_field]: r for r in rows}
    return rows


def write_csv(path: Path, fields: list, rows: list, mode: str = "a") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not path.exists() or mode == "w"
    with open(path, mode, newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if is_new or mode == "w":
            w.writeheader()
        w.writerows(rows)


# ── Search Scraping ───────────────────────────────────────────────────────────

def normalize_search(r: dict) -> dict:
    return {
        "id": r.get("sk", ""),
        "date_created": r.get("date_created", ""),
        "tenant": r.get("tenant", ""),
        "email": r.get("email", ""),
        "search_type": r.get("search_type", ""),
        "question": (r.get("question", "") or "").replace("\n", " "),
        "answer": (r.get("answer", "") or "")[:1500].replace("\n", " "),
        "thumbs_up": r.get("thumbs_up", "false"),
        "thumbs_down": r.get("thumbs_down", "false"),
    }


def scrape_searches(token: str, tenant: str) -> int:
    records = api_get(token, "/searches", {"tenant": tenant})
    path = csv_path_searches(tenant)
    seen = {r["id"] for r in load_csv(path)} if path.exists() else set()
    new_rows = [normalize_search(r) for r in records if r.get("sk") not in seen]
    if new_rows:
        write_csv(path, SEARCH_FIELDS, new_rows)
        print(f"  [searches] +{len(new_rows)} new records → {path.relative_to(DATA_DIR.parent)}")
    return new_rows, new_rows


# ── Dashboard Usage Scraping ──────────────────────────────────────────────────

def scrape_dashboard_usage(token: str, tenant: str) -> list:
    """
    Fetches current dashboard usage snapshot, diffs against previous snapshot,
    records new view events, and saves updated snapshot.
    Returns list of new events (each event = a dashboard view detected).
    """
    records = api_get(token, "/dashboard-usage", {"tenant": tenant})

    snapshot_path = csv_path_dashboard_snapshot(tenant)
    prev = load_csv(snapshot_path, key_field=None)
    prev_lookup = {(r["user"], r["dashboard_name"]): r for r in prev}

    new_events = []
    new_snapshot = []

    for r in records:
        key = (r["user"], r["dashboard_name"])
        current_views = int(r.get("views", 0))
        current_last_viewed = r.get("last_viewed", "")

        snapshot_row = {
            "tenant": r.get("tenant", tenant),
            "user": r["user"],
            "dashboard_name": r["dashboard_name"],
            "views": str(current_views),
            "last_viewed": current_last_viewed,
        }
        new_snapshot.append(snapshot_row)

        prev_row = prev_lookup.get(key)
        if prev_row:
            prev_views = int(prev_row.get("views", 0))
            delta = current_views - prev_views
            if delta > 0 or current_last_viewed != prev_row.get("last_viewed", ""):
                new_events.append({
                    "event_time": current_last_viewed or now,
                    "tenant": r.get("tenant", tenant),
                    "user": r["user"],
                    "dashboard_name": r["dashboard_name"],
                    "views_delta": str(max(delta, 1)),
                    "total_views": str(current_views),
                })
        else:
            # First time seeing this user+dashboard — record as initial event
            new_events.append({
                "event_time": current_last_viewed or now,
                "tenant": r.get("tenant", tenant),
                "user": r["user"],
                "dashboard_name": r["dashboard_name"],
                "views_delta": str(current_views),
                "total_views": str(current_views),
            })

    # Overwrite snapshot with latest
    write_csv(snapshot_path, DASHBOARD_SNAPSHOT_FIELDS, new_snapshot, mode="w")

    if new_events:
        event_path = csv_path_dashboard_events(tenant)
        write_csv(event_path, DASHBOARD_EVENT_FIELDS, new_events)
        print(f"  [dashboard] +{len(new_events)} view events → {event_path.relative_to(DATA_DIR.parent)}")
    else:
        print(f"  [dashboard] no new views since last run")

    return new_events


# ── GitHub Issues Alerting ────────────────────────────────────────────────────

def create_github_issue(tenant: str, analysis: dict, new_search_count: int) -> None:
    if not analysis.get("has_problems") or not analysis.get("problems"):
        return
    if not GH_TOKEN:
        print("  [alert] No GH_PAT set — skipping issue creation")
        return

    problems = analysis["problems"]
    severity_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}

    body_lines = [
        f"**{new_search_count}** new searches analyzed — **{len(problems)}** users may be struggling.\n",
        f"> {analysis.get('summary', '')}\n",
        "---\n",
    ]
    for p in problems:
        emoji = severity_emoji.get(p.get("severity", "medium"), "🟡")
        queries = "\n".join(f"  - `{q}`" for q in p.get("queries", []))
        body_lines.append(
            f"### {emoji} {p['email']}\n"
            f"**Issue:** {p.get('issue', '')}\n"
            f"**Queries:**\n{queries}\n"
        )

    body_lines.append(f"\n---\n[View on Gryps]({BASE_URL}/#/searches)")

    r = requests.post(
        f"https://api.github.com/repos/{GH_REPO}/issues",
        headers={
            "Authorization": f"token {GH_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
        },
        json={
            "title": f"[{tenant.replace('-',' ').title()}] {len(problems)} user(s) struggling with Search",
            "body": "\n".join(body_lines),
            "labels": ["alert", tenant],
        },
    )
    if r.status_code == 201:
        print(f"  [alert] GitHub issue created: {r.json()['html_url']}")
    else:
        print(f"  [alert] Failed to create issue: {r.status_code} {r.text[:100]}")


def ensure_gh_labels() -> None:
    if not GH_TOKEN:
        return
    headers = {
        "Authorization": f"token {GH_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    existing = {l["name"] for l in requests.get(
        f"https://api.github.com/repos/{GH_REPO}/labels", headers=headers
    ).json() if isinstance(l, dict)}

    for label, color in [("alert", "d93f0b"), ("massport", "0075ca"),
                          ("northeastern-university", "e4e669"), ("zubatkin", "cfd3d7")]:
        if label not in existing:
            requests.post(
                f"https://api.github.com/repos/{GH_REPO}/labels",
                headers=headers,
                json={"name": label, "color": color},
            )


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    print(f"\n{'='*55}")
    print(f"Gryps Scraper — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*55}")

    token = await get_jwt()
    ensure_gh_labels()

    for tenant in TENANTS:
        print(f"\n[{tenant}]")
        try:
            # Searches
            new_searches, _ = scrape_searches(token, tenant)

            # Dashboard usage
            scrape_dashboard_usage(token, tenant)


        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            import traceback; traceback.print_exc()

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
