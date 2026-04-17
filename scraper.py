"""
Gryps User Engagement Scraper
- Logs into insights.gryps.io via Playwright, grabs JWT
- Fetches raw search data for Massport, NEU, Zubatkin
- Deduplicates against existing CSV data
- Analyzes new records with Claude API
- Emails alerts for detected problems
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

from emails import EmailClient

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "https://insights.gryps.io"
API_BASE = "https://kcdr22q3bd.execute-api.us-east-1.amazonaws.com/prod//ues"
TENANTS = ["massport", "northeastern-university", "zubatkin"]
TENANT_SLUG = {
    "massport": "massport",
    "northeastern-university": "neu",
    "zubatkin": "zubatkin",
}
DATA_DIR = Path(__file__).parent / "data"
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "jashtailor18@gmail.com")
EMAIL_SENDER = "jash@gryps.io"

CSV_FIELDS = ["id", "date_created", "tenant", "email", "search_type",
              "question", "answer", "thumbs_up", "thumbs_down"]


# ── Auth ──────────────────────────────────────────────────────────────────────

async def get_jwt() -> str:
    """Login via Playwright and capture the Cognito JWT."""
    token = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        async def capture(response):
            nonlocal token
            if not token and API_BASE in response.url:
                auth = response.request.headers.get("authorization", "")
                if auth.startswith("Bearer "):
                    token = auth

        page.on("response", capture)

        await page.goto(BASE_URL)
        await page.wait_for_load_state("domcontentloaded")
        await page.fill('input[type="email"]', os.getenv("GRYPS_EMAIL"))
        await page.fill('input[type="password"]', os.getenv("GRYPS_PASSWORD"))
        await page.click('button:has-text("Sign In")')
        await page.wait_for_load_state("domcontentloaded")

        # Navigate to a page that triggers API calls
        await page.goto(f"{BASE_URL}/#/searches")
        await page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(3)

        await browser.close()

    if not token:
        raise RuntimeError("Failed to capture JWT — login may have failed")

    print(f"[auth] JWT acquired")
    return token


# ── Data Fetching ─────────────────────────────────────────────────────────────

def fetch_searches(token: str, tenant: str) -> list[dict]:
    """Fetch raw search records for a tenant."""
    r = requests.get(
        f"{API_BASE}/searches",
        params={"user_type": "Customer", "tenant": tenant},
        headers={"Authorization": token},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("data", [])


def normalize(record: dict) -> dict:
    """Flatten API record to CSV row."""
    return {
        "id": record.get("sk", ""),
        "date_created": record.get("date_created", ""),
        "tenant": record.get("tenant", ""),
        "email": record.get("email", ""),
        "search_type": record.get("search_type", ""),
        "question": record.get("question", "") or "",
        "answer": (record.get("answer", "") or "")[:2000],  # trim long answers
        "thumbs_up": record.get("thumbs_up", "false"),
        "thumbs_down": record.get("thumbs_down", "false"),
    }


# ── CSV Storage ───────────────────────────────────────────────────────────────

def csv_path(tenant: str) -> Path:
    slug = TENANT_SLUG.get(tenant, tenant)
    month = datetime.now(timezone.utc).strftime("%Y-%m")
    return DATA_DIR / slug / f"searches_{month}.csv"


def load_seen_ids(tenant: str) -> set[str]:
    path = csv_path(tenant)
    if not path.exists():
        return set()
    with open(path, newline="", encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f)}


def append_records(tenant: str, records: list[dict]) -> None:
    path = csv_path(tenant)
    path.parent.mkdir(parents=True, exist_ok=True)
    is_new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if is_new:
            w.writeheader()
        w.writerows(records)
    print(f"[storage] Wrote {len(records)} new records → {path}")


# ── Claude Analysis ───────────────────────────────────────────────────────────

ANALYSIS_PROMPT = """You are analyzing user search activity on an internal knowledge-base platform used by facilities managers at {tenant}.

Below are recent search queries and their AI-generated answers. Your job is to identify users who may be struggling.

Flag a user/session as a PROBLEM if you see:
- thumbs_down = true (explicit negative feedback)
- The question is vague or confusing (e.g. "not working", "can't find", single-word repeated attempts)
- The answer contains "not found", "no documents", "does not contain" — the platform couldn't help
- Multiple similar questions from the same user in quick succession (indicates they didn't find what they needed)

Return a JSON object with this exact structure:
{{
  "has_problems": true/false,
  "summary": "One sentence summary of overall search health",
  "problems": [
    {{
      "email": "user@example.com",
      "queries": ["their question 1", "their question 2"],
      "issue": "Why this looks like a problem",
      "severity": "high/medium/low"
    }}
  ]
}}

Return only valid JSON, no markdown.

SEARCH DATA:
{data}
"""


def analyze_with_claude(tenant: str, records: list[dict]) -> dict:
    if not records:
        return {"has_problems": False, "summary": "No new searches", "problems": []}

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

    # Summarize records for the prompt (keep it concise)
    rows = []
    for r in records[-100:]:  # last 100 new records
        rows.append({
            "email": r["email"],
            "time": r["date_created"],
            "question": r["question"],
            "answer_snippet": r["answer"][:300],
            "thumbs_down": r["thumbs_down"],
        })

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": ANALYSIS_PROMPT.format(
                tenant=tenant,
                data=json.dumps(rows, indent=2)
            )
        }]
    )

    raw = message.content[0].text.strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Try extracting JSON block
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            return json.loads(match.group())
        return {"has_problems": False, "summary": "Parse error", "problems": []}


# ── Email Alerting ────────────────────────────────────────────────────────────

def send_alert(tenant: str, analysis: dict, new_count: int) -> None:
    if not analysis.get("has_problems") or not analysis.get("problems"):
        return

    client = EmailClient(sender=EMAIL_SENDER)

    problems_html = ""
    for p in analysis["problems"]:
        severity_color = {"high": "#d32f2f", "medium": "#f57c00", "low": "#388e3c"}.get(
            p.get("severity", "medium"), "#f57c00"
        )
        queries = "<br>".join(f"• {q}" for q in p.get("queries", []))
        problems_html += f"""
        <tr>
            <td style="padding:8px;border-bottom:1px solid #eee;">
                <strong>{p.get('email')}</strong><br>
                <span style="color:{severity_color};font-weight:bold;">[{p.get('severity','?').upper()}]</span>
                {p.get('issue','')}<br>
                <small style="color:#666;">{queries}</small>
            </td>
        </tr>"""

    html_body = f"""
    <html><body style="font-family:sans-serif;max-width:600px;margin:auto;">
        <h2 style="color:#1a237e;">User Engagement Alert — {tenant.replace('-', ' ').title()}</h2>
        <p><strong>{new_count}</strong> new searches analyzed. <strong>{len(analysis['problems'])}</strong> users may be struggling.</p>
        <p style="color:#555;">{analysis.get('summary', '')}</p>
        <table width="100%" style="border-collapse:collapse;margin-top:16px;">
            <thead><tr style="background:#e8eaf6;">
                <th style="padding:8px;text-align:left;">User / Issue</th>
            </tr></thead>
            <tbody>{problems_html}</tbody>
        </table>
        <p style="margin-top:24px;color:#999;font-size:12px;">
            View full data: <a href="{BASE_URL}/#/searches">insights.gryps.io</a>
        </p>
    </body></html>"""

    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from configparser import ConfigParser

    config = ConfigParser()
    config.read(Path(__file__).parent / "config.ini")
    creds = config[EMAIL_SENDER]

    msg = MIMEMultipart("alternative")
    msg["From"] = EMAIL_SENDER
    msg["To"] = ALERT_EMAIL
    msg["Subject"] = f"[Gryps Alert] {len(analysis['problems'])} users struggling on {tenant.replace('-',' ').title()}"
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(creds["sender"], creds["password"])
        server.sendmail(EMAIL_SENDER, ALERT_EMAIL, msg.as_string())

    print(f"[email] Alert sent for {tenant} — {len(analysis['problems'])} problems")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    print(f"\n{'='*50}")
    print(f"Gryps Scraper — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*50}")

    token = await get_jwt()

    for tenant in TENANTS:
        print(f"\n[{tenant}] Fetching searches...")
        try:
            records = fetch_searches(token, tenant)
            print(f"[{tenant}] Got {len(records)} records from API")

            seen = load_seen_ids(tenant)
            new_records = [
                normalize(r) for r in records
                if r.get("sk") and r["sk"] not in seen
            ]
            print(f"[{tenant}] {len(new_records)} new records")

            if new_records:
                append_records(tenant, new_records)
                analysis = analyze_with_claude(tenant, new_records)
                print(f"[{tenant}] Analysis: {analysis.get('summary')}")
                if analysis.get("has_problems"):
                    print(f"[{tenant}] ⚠ {len(analysis['problems'])} problems detected")
                    send_alert(tenant, analysis, len(new_records))
            else:
                print(f"[{tenant}] No new data since last run")

        except Exception as e:
            print(f"[{tenant}] ERROR: {e}", file=sys.stderr)

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
