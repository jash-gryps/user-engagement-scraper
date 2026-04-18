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
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import anthropic
import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "https://insights.gryps.io"
API_BASE = "https://kcdr22q3bd.execute-api.us-east-1.amazonaws.com/prod//ues"
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

DASHBOARD_SNAPSHOT_FIELDS = ["scraped_at", "tenant", "user", "dashboard_name", "views", "last_viewed"]
DASHBOARD_EVENT_FIELDS = ["event_time", "tenant", "user", "dashboard_name", "views_delta", "total_views"]


# ── Auth ──────────────────────────────────────────────────────────────────────

async def get_jwt() -> str:
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

    print(f"[auth] JWT acquired")
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
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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
            "scraped_at": now,
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


# ── Claude Analysis ───────────────────────────────────────────────────────────

ANALYSIS_PROMPT = """You are analyzing user search activity on an internal knowledge-base platform used by facilities managers at {tenant}.

Recent search queries and their AI-generated answers are below. Identify users who may be struggling.

Flag a user as a PROBLEM if:
- thumbs_down is true (explicit negative feedback)
- Multiple similar/refined queries in quick succession (user not finding answer)
- Answer contains "not found", "no documents", "does not contain" (platform couldn't help)
- Query is vague, repeated, or shows frustration

Return ONLY valid JSON (no markdown):
{{
  "has_problems": true/false,
  "summary": "One sentence on overall search health",
  "problems": [
    {{
      "email": "user@example.com",
      "queries": ["question 1", "question 2"],
      "issue": "Why this is a problem",
      "severity": "high/medium/low"
    }}
  ]
}}

SEARCH DATA:
{data}"""


def analyze_searches(tenant: str, records: list) -> dict:
    if not records:
        return {"has_problems": False, "summary": "No new searches", "problems": []}

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    rows = [
        {
            "email": r["email"],
            "time": r["date_created"],
            "question": r["question"],
            "answer_snippet": r["answer"][:300],
            "thumbs_down": r["thumbs_down"],
        }
        for r in records[-100:]
    ]

    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        messages=[{"role": "user", "content": ANALYSIS_PROMPT.format(
            tenant=tenant, data=json.dumps(rows, indent=2)
        )}],
    )

    raw = msg.content[0].text.strip()
    # Strip markdown code fences if present
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except Exception:
                pass
        print(f"  [claude] Warning: could not parse response for {tenant}")
        return {"has_problems": False, "summary": "Parse error", "problems": []}


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

            # Analyze and alert on searches
            if new_searches:
                analysis = analyze_searches(tenant, new_searches)
                print(f"  [claude] {analysis.get('summary', '')}")
                if analysis.get("has_problems"):
                    create_github_issue(tenant, analysis, len(new_searches))

        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            import traceback; traceback.print_exc()

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
